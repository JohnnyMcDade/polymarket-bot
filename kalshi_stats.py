"""Kalshi sports stats fetcher — runs once daily.

Single Claude call with the web_search tool to pull every stat the edge
agent will need for the day: MLB team records + leaders + pitcher ERAs,
NBA playoff bracket + per-game averages, NHL playoff bracket. Result
goes to stats_cache.json with a fetched_at timestamp.

The edge agent reads stats_cache.json on every cycle (free, no Claude
call) and refuses to run if the cache is older than 24h or doesn't
cover today/tomorrow's MLB slate (ET). This agent fires twice daily
(06:00 + 18:00 UTC by default) so afternoon/evening edge cycles see
day-of probable-pitcher reshuffles instead of yesterday's snapshot.

Cost target: ~$0.05–0.15 per day (one Sonnet call + up to ~10 web
searches at $10/1000).
"""

from __future__ import annotations

import json
import os
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

WEBHOOK_KALSHI_STATS = os.getenv("WEBHOOK_KALSHI_STATS", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
STATS_HOUR = int(os.getenv("KALSHI_STATS_HOUR", "6"))
STATS_HOUR_PM = int(os.getenv("KALSHI_STATS_HOUR_PM", "18"))
STATS_CACHE_PATH = Path(os.getenv("KALSHI_STATS_CACHE", "/app/data/stats_cache.json"))
ET = ZoneInfo("America/New_York")

# Macro refresher — Haiku call for gas/CPI/Fed (the only LLM use left in
# this module). Sports stats now come from direct statsapi.mlb.com and
# ESPN endpoints; BTC from Coinbase. Budget: < $0.02/macro call.
MACRO_MODEL = os.getenv("ANTHROPIC_MODEL_KALSHI_STATS_MACRO", "claude-haiku-4-5-20251001")
MACRO_INTERVAL_SECS = int(os.getenv("KALSHI_STATS_MACRO_INTERVAL", "3600"))
MACRO_MAX_SEARCHES = int(os.getenv("KALSHI_STATS_MACRO_MAX_SEARCHES", "3"))
MACRO_ENABLED = os.getenv("KALSHI_STATS_MACRO_ENABLED", "true").lower() in ("1", "true", "yes")

# Serializes read-modify-write between the daily fetch and the hourly
# macro Haiku fetch so one doesn't clobber the other.
_cache_lock = threading.Lock()

# (Removed: the old Sonnet+web_search system prompt for sports data.
# Sports stats now come from direct statsapi.mlb.com and ESPN endpoints
# below — web_search was returning structurally-correct JSON with every
# numeric field set to null, which made the edge agent SKIP every market
# for lack of an anchor stat.)
_MACRO_SYSTEM_PROMPT = """You are a macro data updater feeding a Kalshi prediction-market bot. Refresh ONLY the economic indicators below. Use web_search sparingly — ONE consolidated search is ideal; never more than 3.

REQUIRED COVERAGE
- US national average regular-grade gasoline price right now ($/gal, AAA)
- Most recently released CPI: month covered, headline YoY %, core YoY %, release date
- Federal funds target range (low %, high %) right now, next FOMC meeting date, market-implied probabilities (CME FedWatch) of hold / hike / cut at that next meeting
- breaking_news: one short string describing any Fed / CPI / macro news from the past 24 hours, or empty string if nothing material

OUTPUT FORMAT
Return ONE JSON object and nothing else — no prose before or after, no markdown fences. Schema:

{
  "gas_national_avg_usd_per_gal": <float>,
  "gas_source_date": "<YYYY-MM-DD>",
  "cpi": {"month": "<YYYY-MM>", "headline_yoy_pct": <float>, "core_yoy_pct": <float>, "release_date": "<YYYY-MM-DD>"},
  "fed": {"target_range_low_pct": <float>, "target_range_high_pct": <float>, "next_meeting_date": "<YYYY-MM-DD>", "next_meeting_hold_prob": <float>, "next_meeting_cut_prob": <float>, "next_meeting_hike_prob": <float>},
  "breaking_news": "<string or empty>"
}

(btc_spot_usd and btc_source_time_utc are filled in by the bot post-fetch from a direct Coinbase API call — do not include them.)

RULES
- If a stat is genuinely unknown after searching, use null — never invent.
- Keep the JSON valid — no trailing commas, no comments, no NaN/Infinity."""


def _is_cache_fresh() -> bool:
    """True if stats_cache.json was written less than 24h ago AND has
    the current schema AND covers today or tomorrow's MLB slate in ET.
    Treat caches missing mlb.team_scoring / mlb.upcoming_games as stale
    (schema migration) and caches whose upcoming_games window has rolled
    past as stale so late-evening edge cycles don't reason over
    yesterday's probable pitchers.
    """
    if not STATS_CACHE_PATH.exists():
        return False
    try:
        with STATS_CACHE_PATH.open() as f:
            cache = json.load(f)
        mlb = cache.get("mlb", {}) or {}
        if not mlb.get("team_scoring"):
            print("[stats] cache missing mlb.team_scoring — treating as stale (schema upgrade)", flush=True)
            return False
        if "upcoming_games" not in mlb:
            print("[stats] cache missing mlb.upcoming_games — treating as stale (schema upgrade)", flush=True)
            return False
        if "bullpens" not in mlb:
            print("[stats] cache missing mlb.bullpens — treating as stale (schema upgrade)", flush=True)
            return False
        if not cache.get("economic"):
            print("[stats] cache missing economic block — treating as stale (schema upgrade)", flush=True)
            return False
        fetched_at = cache.get("fetched_at", "")
        if not fetched_at:
            return False
        dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
        age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
        if age_hours >= 24:
            return False
        et_today = datetime.now(ET).date()
        expected = {et_today.isoformat(), (et_today + timedelta(days=1)).isoformat()}
        covered = {g.get("game_date") for g in mlb["upcoming_games"] if g.get("game_date")}
        if covered and not (expected & covered):
            print(
                f"[stats] upcoming_games covers {sorted(covered)}, need any of {sorted(expected)} — stale",
                flush=True,
            )
            return False
        return True
    except Exception as e:
        print(f"[WARN] stats_cache.json unreadable: {e}", flush=True)
        return False


def _extract_json(text: str) -> dict[str, Any] | None:
    """Pull the first {...} block out of Claude's response. Tolerates a
    stray ```json fence even though the prompt forbids it.
    """
    t = text.strip()
    if t.startswith("```"):
        t = t.split("```", 2)[1]
        if t.startswith("json"):
            t = t[4:]
        t = t.rsplit("```", 1)[0].strip()
    start = t.find("{")
    end = t.rfind("}")
    if start == -1 or end == -1 or end <= start:
        print(f"[WARN] No JSON object found in stats response. Head: {text[:200]!r}", flush=True)
        return None
    try:
        return json.loads(t[start : end + 1])
    except json.JSONDecodeError as e:
        print(f"[WARN] Stats JSON parse failed: {e}. Head: {text[:200]!r}", flush=True)
        return None


# ─── Direct sports-stat fetchers ────────────────────────────────────────
# Replaces a Sonnet+web_search call that was returning structurally-correct
# JSON with every numeric field set to null. MLB via statsapi.mlb.com,
# NBA/NHL via ESPN's site.api. Both APIs are unauthenticated.

_MLB_STATSAPI = "https://statsapi.mlb.com/api/v1"

# ─── KXMLBTOTAL context signals (park, ump, weather) ───────────────────
# Park factors normalized to ~1.0 = league average. >1.0 = more runs at
# this stadium than the league average; <1.0 = pitcher's park. Approximate
# 2026 values — refresh annually from FanGraphs/Baseball Savant if year-
# over-year shifts matter. Keyed by home-team abbreviation; missing teams
# default to 1.0 at the caller.
_PARK_FACTORS: dict[str, float] = {
    "COL": 1.32,  # Coors Field — altitude
    "CIN": 1.13,  # Great American Ball Park — short porches
    "NYY": 1.10,  # Yankee Stadium — short right field
    "BOS": 1.08,  # Fenway Park — Green Monster shallow LF
    "PHI": 1.05,  # Citizens Bank Park
    "CHC": 1.04,  # Wrigley Field (wind-dependent)
    "TEX": 1.04,  # Globe Life Field
    "TOR": 1.03,  # Rogers Centre
    "BAL": 1.02,  # Camden Yards
    "ATL": 1.00, "LAA": 1.00, "NYM": 1.00,
    "MIL": 0.99, "ARI": 0.99,
    "DET": 0.98, "MIN": 0.98,
    "WSH": 0.97,
    "STL": 0.96, "KC":  0.96, "HOU": 0.96,
    "CWS": 0.95, "CLE": 0.95,
    "TB":  0.94,
    "PIT": 0.93, "LAD": 0.93,
    "SEA": 0.92, "MIA": 0.92,
    "OAK": 0.90,  # Sutter Health Park (Sacramento) since 2025
    "SF":  0.87,  # Oracle Park
    "SD":  0.85,  # Petco Park
}

# Team → city for weather lookup. Use city, not stadium, so wttr.in's
# location parser handles it without ambiguity. Multi-team cities
# (NYC, Chicago) point to the same city — fine since stadium-specific
# weather variance is smaller than city-wide.
_TEAM_CITIES: dict[str, str] = {
    "ARI": "Phoenix", "ATL": "Atlanta", "BAL": "Baltimore",
    "BOS": "Boston", "CHC": "Chicago", "CWS": "Chicago",
    "CIN": "Cincinnati", "CLE": "Cleveland", "COL": "Denver",
    "DET": "Detroit", "HOU": "Houston", "KC": "Kansas City",
    "LAA": "Anaheim", "LAD": "Los Angeles", "MIA": "Miami",
    "MIL": "Milwaukee", "MIN": "Minneapolis",
    "NYM": "New York", "NYY": "New York",
    "OAK": "Sacramento",  # A's play in Sutter Health Park from 2025
    "PHI": "Philadelphia", "PIT": "Pittsburgh",
    "SD": "San Diego", "SEA": "Seattle", "SF": "San Francisco",
    "STL": "St. Louis", "TB": "St. Petersburg",
    "TEX": "Arlington", "TOR": "Toronto", "WSH": "Washington",
}
_ESPN_NBA_STANDINGS = "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings"
_ESPN_NHL_STANDINGS = "https://site.api.espn.com/apis/v2/sports/hockey/nhl/standings"
_ESPN_ATP_RANKINGS = "https://site.api.espn.com/apis/site/v2/sports/tennis/atp/rankings"
_ESPN_WTA_RANKINGS = "https://site.api.espn.com/apis/site/v2/sports/tennis/wta/rankings"
_ESPN_ATP_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard"
_ESPN_WTA_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/tennis/wta/scoreboard"
_HTTP_TIMEOUT = 15
_TENNIS_RANK_DEPTH = int(os.getenv("KALSHI_STATS_TENNIS_RANK_DEPTH", "200"))
_TENNIS_FORM_DAYS = int(os.getenv("KALSHI_STATS_TENNIS_FORM_DAYS", "10"))

# Stable division ID → name map (statsapi /divisions). Hardcoded because
# we'd otherwise need an extra request per fetch to resolve them.
_MLB_DIVISIONS = {
    200: "AL West", 201: "AL East", 202: "AL Central",
    203: "NL West", 204: "NL East", 205: "NL Central",
}


def _http_get_json(url: str) -> dict[str, Any] | None:
    try:
        r = requests.get(url, timeout=_HTTP_TIMEOUT)
        if r.status_code != 200:
            print(f"[WARN] GET {url[:90]} status={r.status_code}", flush=True)
            return None
        return r.json()
    except Exception as e:
        print(f"[WARN] GET {url[:90]} failed: {e}", flush=True)
        return None


def _to_float(v: Any) -> float | None:
    if v is None or v in ("-", ".---", "", "--"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_ip(s: Any) -> float:
    """MLB encodes innings pitched as a decimal where the tenths digit is
    outs recorded (5.1 = 5⅓ IP, 5.2 = 5⅔). Convert to real innings."""
    if s is None:
        return 0.0
    try:
        f = float(s)
    except (TypeError, ValueError):
        return 0.0
    whole = int(f)
    tenths = round((f - whole) * 10)
    return whole + tenths / 3.0


def _fetch_pitcher_gamelogs(pitcher_ids: set[int]) -> dict[int, list[dict]]:
    """For each pitcher id, fetch gameLog and return ordered list of
    starts: [{date, ip, er, runs, hits, walks, opp_id}, ...] sorted asc.
    One HTTP call per pitcher; called from the daily stats refresh so
    cost is bounded. Used to derive rolling ERA AND head-to-head stats
    against the day's opponent — fetching the log once and reusing it."""
    out: dict[int, list[dict]] = {}
    for pid in pitcher_ids:
        data = _http_get_json(
            f"{_MLB_STATSAPI}/people/{pid}/stats"
            f"?stats=gameLog&season=2026&group=pitching&sportId=1"
        )
        entries: list[dict] = []
        for sb in (data or {}).get("stats", []):
            for sp in sb.get("splits", []):
                d = sp.get("date")
                stat = sp.get("stat", {}) or {}
                opp = sp.get("opponent", {}) or {}
                ip_str = stat.get("inningsPitched")
                er = stat.get("earnedRuns")
                if not d or ip_str is None or er is None:
                    continue
                try:
                    entries.append({
                        "date": d,
                        "ip": _parse_ip(ip_str),
                        "er": int(er),
                        "runs": int(stat.get("runs") or er),
                        "hits": int(stat.get("hits") or 0),
                        "walks": int(stat.get("baseOnBalls") or 0),
                        "opp_id": opp.get("id"),
                    })
                except (TypeError, ValueError):
                    continue
        entries.sort(key=lambda e: e["date"])
        out[pid] = entries
    return out


def _rolling_era_last(entries: list[dict], window: int = 3) -> float | None:
    """IP-weighted ERA across the last `window` starts. None if fewer."""
    if len(entries) < window:
        return None
    recent = entries[-window:]
    total_ip = sum(e["ip"] for e in recent)
    total_er = sum(e["er"] for e in recent)
    return round((total_er * 9.0) / total_ip, 2) if total_ip > 0 else None


def _h2h_vs_opponent(entries: list[dict], opp_id: int | None) -> dict | None:
    """ERA / WHIP / avg-runs-last-3 against a specific opponent team id.
    Returns None when there are no prior starts vs that opponent so the
    edge agent treats absence as 'no signal' rather than zero."""
    if not opp_id:
        return None
    vs = [e for e in entries if e.get("opp_id") == opp_id]
    if not vs:
        return None
    total_ip = sum(e["ip"] for e in vs)
    total_er = sum(e["er"] for e in vs)
    total_h = sum(e["hits"] for e in vs)
    total_bb = sum(e["walks"] for e in vs)
    last3 = vs[-3:]
    avg_runs_last3 = sum(e["runs"] for e in last3) / len(last3)
    return {
        "starts": len(vs),
        "era_vs": round((total_er * 9.0) / total_ip, 2) if total_ip > 0 else None,
        "whip_vs": round((total_h + total_bb) / total_ip, 3) if total_ip > 0 else None,
        "avg_runs_last3_vs": round(avg_runs_last3, 2),
    }


def _fetch_mlb_teams_meta() -> dict[int, dict[str, str]]:
    """team_id → {abbr, name, division} for all active MLB teams."""
    data = _http_get_json(f"{_MLB_STATSAPI}/teams?sportId=1&season=2026&activeStatus=Y")
    out: dict[int, dict[str, str]] = {}
    for t in (data or {}).get("teams", []):
        tid = t.get("id")
        abbr = t.get("abbreviation", "")
        if not tid or not abbr:
            continue
        out[tid] = {
            "abbr": abbr,
            "name": t.get("name", ""),
            "division": (t.get("division") or {}).get("name", ""),
        }
    return out


def _fetch_mlb_standings(meta: dict[int, dict]) -> dict[str, dict]:
    """{team_abbr: {w, l, pct, gb, division}}."""
    data = _http_get_json(f"{_MLB_STATSAPI}/standings?leagueId=103,104&season=2026")
    out: dict[str, dict] = {}
    for rec in (data or {}).get("records", []):
        div_id = (rec.get("division") or {}).get("id")
        for tr in rec.get("teamRecords", []):
            tid = (tr.get("team") or {}).get("id")
            m = meta.get(tid)
            if not m:
                continue
            out[m["abbr"]] = {
                "w": tr.get("wins"),
                "l": tr.get("losses"),
                "pct": _to_float(tr.get("winningPercentage")),
                "gb": _to_float(tr.get("gamesBack")),
                "division": _MLB_DIVISIONS.get(div_id, m.get("division", "")),
            }
    return out


def _fetch_mlb_team_scoring(meta: dict[int, dict]) -> dict[str, dict]:
    """{team_abbr: {rs_per_game, ra_per_game, ...split nulls...}}. Season
    totals only; last7/home/away splits are left null (statsapi splits
    require per-team calls — not worth the extra latency for now)."""
    hit = _http_get_json(
        f"{_MLB_STATSAPI}/teams/stats?season=2026&stats=season&group=hitting&sportIds=1"
    )
    pit = _http_get_json(
        f"{_MLB_STATSAPI}/teams/stats?season=2026&stats=season&group=pitching&sportIds=1"
    )
    out: dict[str, dict] = {}

    def _populate(payload: dict | None, field: str) -> None:
        if not payload:
            return
        try:
            splits = payload["stats"][0]["splits"]
        except (KeyError, IndexError):
            return
        for sp in splits:
            tid = (sp.get("team") or {}).get("id")
            m = meta.get(tid)
            if not m:
                continue
            gp = sp.get("stat", {}).get("gamesPlayed") or 0
            runs = sp.get("stat", {}).get("runs") or 0
            out.setdefault(m["abbr"], {})[field] = round(runs / gp, 2) if gp else None

    _populate(hit, "rs_per_game")
    _populate(pit, "ra_per_game")
    for v in out.values():
        for k in ("rs_per_game_last7", "ra_per_game_last7",
                  "rs_per_game_home", "ra_per_game_home",
                  "rs_per_game_away", "ra_per_game_away"):
            v.setdefault(k, None)
    return out


def _fetch_weather(city: str) -> dict | None:
    """Fetch current weather from wttr.in for one MLB city.
    Returns {temp_f, wind_mph, wind_dir, condition} or None on failure.

    Used for KXMLBTOTAL context: temperature affects ball-flight distance
    (carry drops sharply below ~50°F and rises above ~75°F), wind speed
    + direction can shift a Wrigley/Yankee/Fenway game by 1-2 runs, and
    precipitation can lead to rain-shortened games or postponements.

    wttr.in is free and no-auth. Daily refresh at 06:00 UTC = ~30 calls
    well within their soft rate limit. On failure (network blip, parser
    error) we return None so the caller can omit the weather signal
    rather than block the rest of the MLB fetch."""
    if not city:
        return None
    try:
        url = f"https://wttr.in/{city.replace(' ', '+')}?format=j1"
        d = _http_get_json(url)
    except Exception:
        return None
    if not d:
        return None
    cur = (d.get("current_condition") or [{}])[0]
    if not cur:
        return None
    cond_list = cur.get("weatherDesc") or []
    return {
        "temp_f": _to_float(cur.get("temp_F")),
        "wind_mph": _to_float(cur.get("windspeedMiles")),
        "wind_dir": cur.get("winddir16Point", ""),
        "condition": (cond_list[0] or {}).get("value", "") if cond_list else "",
    }


def _fetch_mlb_todays_games(date_str: str, meta: dict[int, dict]) -> list[dict]:
    """Today's MLB schedule with probable pitchers and their season
    ERA/WHIP, plus KXMLBTOTAL context signals: park factor, home plate
    umpire, and current weather at the ballpark."""
    sched = _http_get_json(
        f"{_MLB_STATSAPI}/schedule?sportId=1&date={date_str}"
        f"&hydrate=probablePitcher,team,officials"
    )
    games_raw: list[dict] = []
    for d in (sched or {}).get("dates", []):
        games_raw.extend(d.get("games", []))
    if not games_raw:
        return []

    # Batch-fetch probable pitcher stats — one call instead of N round-trips.
    pitcher_ids: set[int] = set()
    for g in games_raw:
        for side in ("away", "home"):
            pp = (g.get("teams", {}).get(side, {}) or {}).get("probablePitcher") or {}
            if pp.get("id"):
                pitcher_ids.add(pp["id"])
    pitcher_stats: dict[int, dict] = {}
    if pitcher_ids:
        ids_csv = ",".join(str(i) for i in pitcher_ids)
        p_data = _http_get_json(
            f"{_MLB_STATSAPI}/people?personIds={ids_csv}"
            f"&hydrate=stats(type=season,season=2026,group=pitching)"
        )
        for p in (p_data or {}).get("people", []):
            pid = p.get("id")
            for sg in p.get("stats", []) or []:
                if (sg.get("group") or {}).get("displayName") != "pitching":
                    continue
                splits = sg.get("splits") or []
                if not splits:
                    continue
                st = splits[0].get("stat", {})
                pitcher_stats[pid] = {
                    "era": _to_float(st.get("era")),
                    "whip": _to_float(st.get("whip")),
                }
                break

    # Rolling last-3-starts ERA per starter — backtest showed this is a
    # materially sharper signal than season cumulative ERA. One gameLog
    # call per pitcher; ~30 pitchers per refresh, ~9s added latency. The
    # same gameLog feed also drives the new vs-opponent H2H stats below.
    gamelogs = _fetch_pitcher_gamelogs(pitcher_ids)

    # Weather fetch is deduped per city so doubleheaders + the two NYC
    # teams + the two Chicago teams don't double-call wttr.in.
    cities_needed: set[str] = set()
    for g in games_raw:
        ht = (g.get("teams", {}).get("home", {}) or {}).get("team", {}) or {}
        abbr = ht.get("abbreviation") or meta.get(ht.get("id"), {}).get("abbr", "")
        city = _TEAM_CITIES.get(abbr)
        if city:
            cities_needed.add(city)
    weather_by_city: dict[str, dict] = {}
    for city in sorted(cities_needed):
        w = _fetch_weather(city)
        if w:
            weather_by_city[city] = w

    out: list[dict] = []
    for g in games_raw:
        teams = g.get("teams", {})
        away_team = (teams.get("away", {}) or {}).get("team", {}) or {}
        home_team = (teams.get("home", {}) or {}).get("team", {}) or {}
        away_abbr = away_team.get("abbreviation") or meta.get(away_team.get("id"), {}).get("abbr", "")
        home_abbr = home_team.get("abbreviation") or meta.get(home_team.get("id"), {}).get("abbr", "")
        away_tid = away_team.get("id")
        home_tid = home_team.get("id")
        try:
            start_t = datetime.fromisoformat(g.get("gameDate", "").replace("Z", "+00:00")).strftime("%H:%M")
        except (TypeError, ValueError):
            start_t = "?"
        ap = (teams.get("away", {}) or {}).get("probablePitcher") or {}
        hp = (teams.get("home", {}) or {}).get("probablePitcher") or {}
        ap_log = gamelogs.get(ap.get("id"), [])
        hp_log = gamelogs.get(hp.get("id"), [])
        # Home plate umpire from the schedule hydrate=officials payload.
        # The officials array only populates on game-day (typically a
        # few hours pre-first-pitch); empty earlier in the day is fine.
        hp_umpire = ""
        for o in (g.get("officials") or []):
            if (o.get("officialType") or "") == "Home Plate":
                hp_umpire = (o.get("official") or {}).get("fullName", "")
                break
        park_factor = _PARK_FACTORS.get(home_abbr, 1.00)
        weather = weather_by_city.get(_TEAM_CITIES.get(home_abbr, ""))
        out.append({
            "away": away_abbr,
            "home": home_abbr,
            "start_time_utc": start_t,
            "park_factor": park_factor,
            "home_plate_umpire": hp_umpire,
            "weather": weather,
            "away_pitcher": {
                "player": ap.get("fullName", ""),
                "rolling_era_last3": _rolling_era_last(ap_log),
                "vs_opponent": _h2h_vs_opponent(ap_log, home_tid),
                **(pitcher_stats.get(ap.get("id")) or {"era": None, "whip": None}),
            },
            "home_pitcher": {
                "player": hp.get("fullName", ""),
                "rolling_era_last3": _rolling_era_last(hp_log),
                "vs_opponent": _h2h_vs_opponent(hp_log, away_tid),
                **(pitcher_stats.get(hp.get("id")) or {"era": None, "whip": None}),
            },
        })
    return out


def _fetch_team_bullpen(team_ids: set[int], meta: dict[int, dict]) -> dict[str, dict]:
    """Per-team bullpen stats over the last 15 days. Returns
    {team_abbr: {bullpen_era_15d, saves_15d, save_opportunities_15d,
    save_conversion_15d, blown_saves_15d}}.

    Aggregates from each team's active pitching roster, filtered to
    appearances where gamesStarted == 0 (relief appearances only). That's
    ~2 HTTP calls per team; we only call for teams playing in the
    upcoming_games slate so cost is bounded by the day's schedule, not
    the full 30-team league. statsapi's /teams/stats endpoint does NOT
    expose a starter/relief split — this per-pitcher aggregation is the
    cheapest path that's actually bullpen-only."""
    today = datetime.now(ET).date()
    start = (today - timedelta(days=15)).isoformat()
    end = today.isoformat()
    out: dict[str, dict] = {}
    for tid in team_ids:
        m = meta.get(tid)
        if not m:
            continue
        roster_data = _http_get_json(
            f"{_MLB_STATSAPI}/teams/{tid}/roster?rosterType=active&season=2026"
        )
        roster = (roster_data or {}).get("roster", []) or []
        pids = [
            p["person"]["id"] for p in roster
            if (p.get("position") or {}).get("abbreviation") == "P"
            and (p.get("person") or {}).get("id")
        ]
        if not pids:
            continue
        ids_csv = ",".join(str(p) for p in pids)
        data = _http_get_json(
            f"{_MLB_STATSAPI}/people?personIds={ids_csv}"
            f"&hydrate=stats(type=byDateRange,startDate={start},endDate={end},group=pitching)"
        )
        rel_er = 0
        rel_outs = 0
        saves = save_opps = blown = 0
        seen_pids: set[int] = set()
        for p in (data or {}).get("people", []) or []:
            pid = p.get("id")
            # statsapi sometimes returns the same pitcher twice (AL + MLB
            # league splits) — dedupe so we don't double-count outs.
            if pid in seen_pids:
                continue
            seen_pids.add(pid)
            for sg in p.get("stats", []) or []:
                if (sg.get("group") or {}).get("displayName") != "pitching":
                    continue
                splits = sg.get("splits") or []
                if not splits:
                    continue
                st = splits[0].get("stat", {}) or {}
                try:
                    gs = int(st.get("gamesStarted") or 0)
                    gp = int(st.get("gamesPitched") or 0)
                except (TypeError, ValueError):
                    break
                if gs > 0 or gp <= 0:
                    break
                try:
                    rel_er += int(st.get("earnedRuns") or 0)
                except (TypeError, ValueError):
                    pass
                ip_real = _parse_ip(st.get("inningsPitched"))
                rel_outs += int(round(ip_real * 3))
                try:
                    saves += int(st.get("saves") or 0)
                    save_opps += int(st.get("saveOpportunities") or 0)
                    blown += int(st.get("blownSaves") or 0)
                except (TypeError, ValueError):
                    pass
                break
        bullpen_era = round(rel_er * 27.0 / rel_outs, 2) if rel_outs > 0 else None
        save_conv = round(saves / save_opps, 3) if save_opps > 0 else None
        out[m["abbr"]] = {
            "bullpen_era_15d": bullpen_era,
            "saves_15d": saves,
            "save_opportunities_15d": save_opps,
            "save_conversion_15d": save_conv,
            "blown_saves_15d": blown,
        }
    return out


def _fetch_mlb_leaders() -> dict[str, dict[str, list[dict]]]:
    """Top-10 leaderboards for hitting and pitching."""
    def one(cat: str, group: str) -> list[dict]:
        data = _http_get_json(
            f"{_MLB_STATSAPI}/stats/leaders?leaderCategories={cat}"
            f"&season=2026&sportId=1&statGroup={group}&limit=10"
        )
        result: list[dict] = []
        for ll in (data or {}).get("leagueLeaders", []):
            for ldr in ll.get("leaders", [])[:10]:
                val = ldr.get("value")
                result.append({
                    "player": (ldr.get("person") or {}).get("fullName", ""),
                    "team": (ldr.get("team") or {}).get("abbreviation", ""),
                    "value": _to_float(val) if val is not None else None,
                })
            break  # first category (filter by name already)
        return result

    return {
        "hitting_leaders": {
            "hr": one("homeRuns", "hitting"),
            "rbi": one("runsBattedIn", "hitting"),
            "avg": one("battingAverage", "hitting"),
            "ops": one("onBasePlusSlugging", "hitting"),
            "sb": one("stolenBases", "hitting"),
        },
        "pitching_leaders": {
            "w": one("wins", "pitching"),
            "era": one("earnedRunAverage", "pitching"),
            "k": one("strikeouts", "pitching"),
            "whip": one("walksAndHitsPerInningPitched", "pitching"),
            "sv": one("saves", "pitching"),
        },
    }


def _fetch_mlb_block() -> dict[str, Any]:
    meta = _fetch_mlb_teams_meta()
    leaders = _fetch_mlb_leaders()
    # ET-anchored date window: MLB schedules by US Eastern game date, and
    # the 06:00 UTC refresh fires while ET is still "yesterday late". Pull
    # today + tomorrow (ET) and tag each entry so the edge agent can match
    # by game_date instead of guessing from team abbreviations alone.
    et_today = datetime.now(ET).date()
    dates = [et_today.isoformat(), (et_today + timedelta(days=1)).isoformat()]
    upcoming: list[dict] = []
    for d in dates:
        for g in _fetch_mlb_todays_games(d, meta):
            g["game_date"] = d
            upcoming.append(g)
    # Bullpen stats only for teams actually on the upcoming slate — the
    # /roster + /people calls cost ~2 HTTP requests per team, and we
    # only ever score markets for those teams.
    abbr_to_tid = {m["abbr"]: tid for tid, m in meta.items()}
    playing_tids: set[int] = set()
    for g in upcoming:
        for side in ("away", "home"):
            tid = abbr_to_tid.get(g.get(side, ""))
            if tid:
                playing_tids.add(tid)
    bullpens = _fetch_team_bullpen(playing_tids, meta)
    return {
        "standings": _fetch_mlb_standings(meta),
        "hitting_leaders": leaders["hitting_leaders"],
        "pitching_leaders": leaders["pitching_leaders"],
        "notable_players": [],
        "team_scoring": _fetch_mlb_team_scoring(meta),
        "upcoming_games": upcoming,
        "bullpens": bullpens,
    }


def _fetch_espn_standings(url: str) -> list[dict]:
    """Flatten ESPN's conference → standings.entries structure into a list
    of {team, w, l, pct, conference}. Returns [] on any failure so the
    caller can keep the cache shape intact."""
    data = _http_get_json(url)
    out: list[dict] = []
    for conf in (data or {}).get("children", []):
        entries = ((conf.get("standings") or {}).get("entries") or [])
        for e in entries:
            team = e.get("team") or {}
            abbr = team.get("abbreviation", "")
            if not abbr:
                continue
            stat_map = {st.get("name"): st.get("value") for st in (e.get("stats") or [])}
            wins = stat_map.get("wins")
            losses = stat_map.get("losses")
            out.append({
                "team": abbr,
                "name": team.get("displayName", ""),
                "w": int(wins) if wins is not None else None,
                "l": int(losses) if losses is not None else None,
                "pct": _to_float(stat_map.get("winPercent")),
                "conference": conf.get("name", ""),
            })
    return out


def _fetch_nba_block() -> dict[str, Any]:
    return {
        "standings": _fetch_espn_standings(_ESPN_NBA_STANDINGS),
        "playoff_results": [],
        "current_round": "?",
        "active_players": [],
    }


def _fetch_nhl_block() -> dict[str, Any]:
    return {
        "standings": _fetch_espn_standings(_ESPN_NHL_STANDINGS),
        "playoff_results": [],
        "current_round": "?",
        "top_scorers": [],
    }


# ─── Tennis (ATP / WTA) ────────────────────────────────────────────────
# ESPN's tennis endpoints expose current rankings (top 100+) and a
# scoreboard with completed matches. We pull top _TENNIS_RANK_DEPTH for
# each tour plus the last _TENNIS_FORM_DAYS days of results so the edge
# agent can ground its ATP/WTA match-winner picks in real ranking deltas
# and recent W/L form instead of asking Claude to remember the tour.

def _fetch_tennis_rankings(url: str) -> list[dict]:
    """Flatten ESPN's tennis rankings into {rank, player, country}.

    ESPN's current shape (2026): board.ranks[*].{current, athlete}, with
    athlete.displayName + athlete.flagAltText. Previous shape used
    board.athletes[*] and athlete.flag.alt — keep a defensive fallback
    in case ESPN flips back or serves both."""
    data = _http_get_json(url)
    out: list[dict] = []
    for board in (data or {}).get("rankings", []):
        entries = board.get("ranks") or board.get("athletes") or []
        for entry in entries:
            athlete = entry.get("athlete") or {}
            name = athlete.get("displayName") or athlete.get("fullName") or ""
            if not name:
                continue
            country = athlete.get("flagAltText") or ""
            if not country:
                flag = athlete.get("flag")
                if isinstance(flag, dict):
                    country = flag.get("alt", "") or flag.get("countryCode", "")
            rank = entry.get("current") or entry.get("rank")
            try:
                rank_i = int(rank) if rank is not None else None
            except (TypeError, ValueError):
                rank_i = None
            out.append({"rank": rank_i, "player": name, "country": country})
            if len(out) >= _TENNIS_RANK_DEPTH:
                return out
        if out:  # first board is the primary tour ranking
            return out
    return out


def _fetch_tennis_recent(url: str) -> list[dict]:
    """Recent completed matches across the last _TENNIS_FORM_DAYS days.
    One scoreboard call per day — ESPN's tennis scoreboard accepts a
    YYYYMMDD date param. Result rows: {date, winner, loser, score, event}.
    """
    out: list[dict] = []
    today = datetime.now(timezone.utc).date()
    for offset in range(_TENNIS_FORM_DAYS):
        d = today - timedelta(days=offset)
        data = _http_get_json(f"{url}?dates={d.strftime('%Y%m%d')}")
        for ev in (data or {}).get("events", []):
            event_name = (ev.get("league") or {}).get("name", "") or ev.get("name", "")
            # ESPN nests singles matches under ev.groupings[*].competitions
            # for tour-level / slam events. Older shape (and some smaller
            # events) still expose a flat ev.competitions — try both.
            competitions: list[dict] = []
            for grp in (ev.get("groupings") or []):
                competitions.extend(grp.get("competitions") or [])
            if not competitions:
                competitions = ev.get("competitions") or []
            for comp in competitions:
                status_t = ((comp.get("status") or {}).get("type") or {})
                if not status_t.get("completed"):
                    continue
                comps = comp.get("competitors") or []
                if len(comps) < 2:
                    continue
                winner = loser = None
                for c in comps:
                    ath = c.get("athlete") or {}
                    name = ath.get("displayName") or ath.get("fullName") or ""
                    if c.get("winner"):
                        winner = name
                    else:
                        loser = name
                if not (winner and loser):
                    continue
                score = ""
                for c in comps:
                    if c.get("winner"):
                        score = c.get("score") or ""
                        break
                out.append({
                    "date": d.isoformat(),
                    "winner": winner,
                    "loser": loser,
                    "score": score,
                    "event": event_name,
                })
    return out


def _fetch_tennis_block() -> dict[str, Any]:
    return {
        "atp_rankings": _fetch_tennis_rankings(_ESPN_ATP_RANKINGS),
        "wta_rankings": _fetch_tennis_rankings(_ESPN_WTA_RANKINGS),
        "atp_recent": _fetch_tennis_recent(_ESPN_ATP_SCOREBOARD),
        "wta_recent": _fetch_tennis_recent(_ESPN_WTA_SCOREBOARD),
    }


def save_cache(stats: dict[str, Any]) -> None:
    stats.setdefault("fetched_at", datetime.now(timezone.utc).isoformat())
    STATS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATS_CACHE_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(stats, f, indent=2)
    tmp.replace(STATS_CACHE_PATH)  # atomic on POSIX
    print(f"[stats] wrote {STATS_CACHE_PATH} ({STATS_CACHE_PATH.stat().st_size} bytes)", flush=True)


def _build_embed(stats: dict[str, Any]) -> dict[str, Any]:
    mlb = stats.get("mlb", {}) or {}
    nba = stats.get("nba", {}) or {}
    nhl = stats.get("nhl", {}) or {}
    tennis = stats.get("tennis", {}) or {}
    econ = stats.get("economic", {}) or {}
    n_teams = len(mlb.get("standings", {}) or {})
    n_team_scoring = len(mlb.get("team_scoring", {}) or {})
    n_upcoming_games = len(mlb.get("upcoming_games", []) or [])
    n_bullpens = len(mlb.get("bullpens", {}) or {})
    n_nba_results = len(nba.get("playoff_results", []) or [])
    n_nhl_results = len(nhl.get("playoff_results", []) or [])
    n_hitters = len((mlb.get("hitting_leaders", {}) or {}).get("hr", []) or [])
    n_atp = len(tennis.get("atp_rankings", []) or [])
    n_wta = len(tennis.get("wta_rankings", []) or [])
    n_atp_recent = len(tennis.get("atp_recent", []) or [])
    n_wta_recent = len(tennis.get("wta_recent", []) or [])
    gas = econ.get("gas_national_avg_usd_per_gal")
    btc = econ.get("btc_spot_usd")
    fed_range = econ.get("fed", {}) or {}
    fed_str = (
        f"{fed_range.get('target_range_low_pct','?')}-{fed_range.get('target_range_high_pct','?')}%"
        if fed_range else "?"
    )
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return {
        "title": "📊 KALSHI STATS — daily cache refreshed",
        "color": 0x3498DB,
        "fields": [
            {"name": "MLB teams", "value": str(n_teams), "inline": True},
            {"name": "MLB HR leaders", "value": str(n_hitters), "inline": True},
            {"name": "Team scoring", "value": str(n_team_scoring), "inline": True},
            {"name": "Upcoming games", "value": str(n_upcoming_games), "inline": True},
            {"name": "Bullpens (15d)", "value": str(n_bullpens), "inline": True},
            {"name": "NBA playoff results", "value": str(n_nba_results), "inline": True},
            {"name": "NHL playoff results", "value": str(n_nhl_results), "inline": True},
            {"name": "NBA round", "value": nba.get("current_round", "?"), "inline": True},
            {"name": "NHL round", "value": nhl.get("current_round", "?"), "inline": True},
            {"name": "ATP ranked", "value": str(n_atp), "inline": True},
            {"name": "WTA ranked", "value": str(n_wta), "inline": True},
            {"name": "Tennis recent (ATP/WTA)", "value": f"{n_atp_recent}/{n_wta_recent}", "inline": True},
            {"name": "Gas $/gal", "value": str(gas) if gas is not None else "?", "inline": True},
            {"name": "BTC $", "value": f"{btc:,.0f}" if isinstance(btc, (int, float)) else "?", "inline": True},
            {"name": "Fed range", "value": fed_str, "inline": True},
        ],
        "footer": {"text": f"PassivePoly Kalshi Stats  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_discord(embed: dict[str, Any]) -> None:
    if not WEBHOOK_KALSHI_STATS:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_STATS, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_STATS, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Discord send failed: {e}")


def _seconds_until_next_hour(target_hours: list[int]) -> float:
    """Seconds until the next occurrence of any hour in target_hours (UTC)."""
    now = datetime.now(timezone.utc)
    candidates: list[datetime] = []
    for h in target_hours:
        t = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if t <= now:
            t += timedelta(days=1)
        candidates.append(t)
    return (min(candidates) - now).total_seconds()


def _do_fetch_and_save() -> None:
    """Daily stats refresh. Sports blocks come from direct APIs
    (statsapi.mlb.com, ESPN); economic block from Haiku web_search; BTC
    from Coinbase. If any block fails its corresponding helper returns an
    empty shape so the cache is still written with whatever did succeed."""
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Fetching daily stats via direct APIs...", flush=True)
    t0 = time.time()
    stats: dict[str, Any] = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "season": "2026",
        "mlb": _fetch_mlb_block(),
        "nba": _fetch_nba_block(),
        "nhl": _fetch_nhl_block(),
        "tennis": _fetch_tennis_block(),
    }
    econ = fetch_economic_only() or {}
    btc = _fetch_btc_spot_coinbase()
    if btc:
        econ["btc_spot_usd"], econ["btc_source_time_utc"] = btc
    # KXBTC filter inputs — same fetchers as _refresh_macro(). Without
    # this the daily refresh nukes the F&G + 24h fields the KXBTC
    # filter (and the /dashboard BTC status card) depend on, until the
    # next hourly macro fires up to ~60 min later.
    btc_24h = _fetch_btc_24h_stats()
    if btc_24h:
        econ["btc_24h_open"] = btc_24h["open"]
        econ["btc_24h_last"] = btc_24h["last"]
        econ["btc_24h_momentum_pct"] = btc_24h["momentum_pct"]
    fng = _fetch_fear_greed()
    if fng:
        econ["crypto_fear_greed_value"] = fng["value"]
        econ["crypto_fear_greed_classification"] = fng["classification"]
    stats["economic"] = econ
    elapsed = time.time() - t0
    mlb_n = len(stats["mlb"].get("standings", {}))
    nba_n = len(stats["nba"].get("standings", []))
    nhl_n = len(stats["nhl"].get("standings", []))
    atp_n = len(stats["tennis"].get("atp_rankings", []))
    wta_n = len(stats["tennis"].get("wta_rankings", []))
    print(
        f"[stats] direct fetch done in {elapsed:.1f}s — mlb={mlb_n} nba={nba_n} "
        f"nhl={nhl_n} atp={atp_n} wta={wta_n} btc={econ.get('btc_spot_usd')}",
        flush=True,
    )
    with _cache_lock:
        save_cache(stats)
    send_discord(_build_embed(stats))


def fetch_economic_only() -> dict[str, Any] | None:
    """Cheap Haiku + web_search call that returns just the economic
    block. Caller is responsible for merging into stats_cache.json.
    """
    if not ANTHROPIC_API_KEY:
        print("[WARN] ANTHROPIC_API_KEY not set — skipping macro fetch", flush=True)
        return None

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    user_msg = (
        f"It is {today}. Refresh the macro/economic block per the schema in "
        "your system prompt and return the JSON object."
    )

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": MACRO_MODEL,
                "max_tokens": 2500,
                "system": [{"type": "text", "text": _MACRO_SYSTEM_PROMPT}],
                "tools": [
                    {
                        "type": "web_search_20250305",
                        "name": "web_search",
                        "max_uses": MACRO_MAX_SEARCHES,
                    }
                ],
                "messages": [{"role": "user", "content": user_msg}],
            },
            timeout=180,
        )
    except Exception as e:
        print(f"[WARN] macro Claude call failed: {e}", flush=True)
        return None

    if r.status_code != 200:
        print(f"[ERROR] macro Anthropic status={r.status_code} body: {r.text[:300]}", flush=True)
        return None

    body = r.json()
    usage = body.get("usage", {})
    if usage:
        print(
            f"[USAGE] in={usage.get('input_tokens', 0)} "
            f"out={usage.get('output_tokens', 0)} "
            f"cache_create={usage.get('cache_creation_input_tokens', 0)} "
            f"cache_read={usage.get('cache_read_input_tokens', 0)} "
            f"agent=macro model={MACRO_MODEL}",
            flush=True,
        )

    text_parts = [b.get("text", "") for b in body.get("content", []) if b.get("type") == "text"]
    if not text_parts:
        print(f"[WARN] No text blocks in macro response. Blocks={[b.get('type') for b in body.get('content', [])]}", flush=True)
        return None
    return _extract_json("\n".join(text_parts).strip())


def _fetch_btc_24h_stats() -> dict[str, float] | None:
    """Fetch 24h open/last/high/low for BTC from Coinbase exchange stats.
    Returns {open, last, momentum_pct} or None on failure.

    momentum_pct = (last - open) / open × 100. The KXBTC strategy uses
    this with the Fear & Greed reading to confirm direction: extreme
    fear + recent upward momentum = buy YES on "BTC up" markets;
    extreme greed + downward momentum = the opposite. Computed once
    per macro refresh so Claude always has a fresh 24h delta.
    """
    try:
        r = requests.get(
            "https://api.exchange.coinbase.com/products/BTC-USD/stats",
            timeout=10,
        )
        if r.status_code != 200:
            print(
                f"[WARN] Coinbase 24h stats status={r.status_code} "
                f"body: {r.text[:200]}",
                flush=True,
            )
            return None
        d = r.json() or {}
        o = _to_float(d.get("open"))
        last = _to_float(d.get("last"))
        if o is None or last is None or o == 0:
            return None
        return {
            "open": o,
            "last": last,
            "momentum_pct": round((last - o) / o * 100, 2),
        }
    except Exception as e:
        print(f"[WARN] Coinbase 24h stats fetch failed: {e}", flush=True)
        return None


def _fetch_fear_greed() -> dict[str, Any] | None:
    """Fetch the Crypto Fear & Greed Index from alternative.me.
    Returns {value: int (0-100), classification: str} or None on failure.

    0-25 = Extreme Fear (contrarian buy signal)
    26-45 = Fear
    46-55 = Neutral
    56-75 = Greed
    76-100 = Extreme Greed (contrarian sell signal)

    Free, no auth, updated once per day. Caching for an hour at the
    macro refresh cadence is fine — the index doesn't move intraday
    in their data.
    """
    try:
        r = requests.get(
            "https://api.alternative.me/fng/?limit=1",
            timeout=10,
        )
        if r.status_code != 200:
            print(
                f"[WARN] F&G status={r.status_code} body: {r.text[:200]}",
                flush=True,
            )
            return None
        d = (r.json() or {}).get("data") or []
        if not d:
            return None
        entry = d[0]
        val = entry.get("value")
        try:
            val_i = int(val)
        except (TypeError, ValueError):
            return None
        return {
            "value": val_i,
            "classification": entry.get("value_classification", ""),
        }
    except Exception as e:
        print(f"[WARN] F&G fetch failed: {e}", flush=True)
        return None


def _fetch_btc_spot_coinbase() -> tuple[float, str] | None:
    """Fetch BTC/USD spot directly from Coinbase. No auth needed.
    Returns (price_usd, "HH:MM" UTC) or None on failure. Used in
    preference to the LLM web_search path, which was returning null
    almost every hour.
    """
    try:
        r = requests.get(
            "https://api.coinbase.com/v2/prices/BTC-USD/spot",
            timeout=10,
        )
        if r.status_code != 200:
            print(f"[WARN] Coinbase BTC status={r.status_code} body: {r.text[:200]}", flush=True)
            return None
        amount = r.json().get("data", {}).get("amount")
        if amount is None:
            print("[WARN] Coinbase BTC response missing data.amount", flush=True)
            return None
        return float(amount), datetime.now(timezone.utc).strftime("%H:%M")
    except Exception as e:
        print(f"[WARN] Coinbase BTC fetch failed: {e}", flush=True)
        return None


def _refresh_macro() -> None:
    """Read current cache, merge a fresh economic block in, save atomically.
    Lock-protected so it can't race with the daily Sonnet fetch.
    """
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Refreshing macro block...", flush=True)
    econ = fetch_economic_only()
    if not econ:
        print("[macro] fetch returned nothing — keeping previous econ block", flush=True)
        return
    btc = _fetch_btc_spot_coinbase()
    if btc:
        econ["btc_spot_usd"], econ["btc_source_time_utc"] = btc
    # BTC 24h momentum + Fear & Greed — both feed the KXBTC filter rule.
    # F&G updates daily but the API is fine to hit hourly. 24h stats
    # come from Coinbase same as spot, separate endpoint.
    btc_24h = _fetch_btc_24h_stats()
    if btc_24h:
        econ["btc_24h_open"] = btc_24h["open"]
        econ["btc_24h_last"] = btc_24h["last"]
        econ["btc_24h_momentum_pct"] = btc_24h["momentum_pct"]
    fng = _fetch_fear_greed()
    if fng:
        econ["crypto_fear_greed_value"] = fng["value"]
        econ["crypto_fear_greed_classification"] = fng["classification"]
    with _cache_lock:
        cache: dict[str, Any] = {}
        if STATS_CACHE_PATH.exists():
            try:
                with STATS_CACHE_PATH.open() as f:
                    cache = json.load(f)
            except Exception as e:
                print(f"[WARN] macro read cache failed, starting from empty: {e}", flush=True)
        cache["economic"] = {**(cache.get("economic", {}) or {}), **econ}
        cache["economic_fetched_at"] = datetime.now(timezone.utc).isoformat()
        save_cache(cache)
    gas = econ.get("gas_national_avg_usd_per_gal")
    btc = econ.get("btc_spot_usd")
    btc_mom = econ.get("btc_24h_momentum_pct")
    fng_v = econ.get("crypto_fear_greed_value")
    fng_c = econ.get("crypto_fear_greed_classification", "")
    bn = (econ.get("breaking_news") or "").strip()
    print(
        f"[macro] refreshed: gas=${gas} btc=${btc} "
        f"btc_24h_mom={btc_mom}% f&g={fng_v}({fng_c}) "
        f"breaking={bn!r}",
        flush=True,
    )


def run_macro() -> None:
    if not MACRO_ENABLED:
        print("Kalshi Macro Refresh disabled via KALSHI_STATS_MACRO_ENABLED", flush=True)
        return
    print(
        f"Kalshi Macro Refresh starting — every {MACRO_INTERVAL_SECS}s, "
        f"model={MACRO_MODEL}, web_search_max={MACRO_MAX_SEARCHES}",
        flush=True,
    )
    # Skip startup prime — the daily Sonnet fetch covers initial state.
    # First macro refresh fires one interval after startup.
    while True:
        time.sleep(MACRO_INTERVAL_SECS)
        try:
            _refresh_macro()
        except Exception as e:
            print(f"[WARN] macro cycle crashed: {e}", flush=True)
            traceback.print_exc()
            time.sleep(60)


def run() -> None:
    targets = sorted({STATS_HOUR, STATS_HOUR_PM})
    print(
        f"Kalshi Stats Agent starting — direct APIs (statsapi+ESPN) + Haiku for econ, "
        f"refresh hours UTC={targets}"
    )
    # If the cache is missing or stale at startup, prime it immediately so
    # kalshi_edge can run before the next scheduled wakeup.
    if not _is_cache_fresh():
        print("[stats] cache missing or stale — priming on startup", flush=True)
        _do_fetch_and_save()

    while True:
        wait_s = _seconds_until_next_hour(targets)
        print(f"[stats] next fetch in {wait_s/3600:.1f}h", flush=True)
        time.sleep(wait_s)
        try:
            _do_fetch_and_save()
        except Exception as e:
            print(f"[WARN] stats cycle crashed: {e}", flush=True)
            # avoid tight-loop on persistent error
            time.sleep(60)


if __name__ == "__main__":
    run()
