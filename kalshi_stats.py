"""Kalshi sports stats fetcher — runs once daily.

Single Claude call with the web_search tool to pull every stat the edge
agent will need for the day: MLB team records + leaders + pitcher ERAs,
NBA playoff bracket + per-game averages, NHL playoff bracket. Result
goes to stats_cache.json with a fetched_at timestamp.

The edge agent reads stats_cache.json on every cycle (free, no Claude
call) and refuses to run if the cache is older than 24h. So this agent
firing once at 06:00 UTC is what keeps the rest of the pipeline alive.

Cost target: ~$0.05–0.15 per day (one Sonnet call + up to ~10 web
searches at $10/1000).
"""

from __future__ import annotations

import json
import os
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

WEBHOOK_KALSHI_STATS = os.getenv("WEBHOOK_KALSHI_STATS", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
STATS_HOUR = int(os.getenv("KALSHI_STATS_HOUR", "6"))
STATS_CACHE_PATH = Path(os.getenv("KALSHI_STATS_CACHE", "/app/data/stats_cache.json"))

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
    the current schema. Treat caches written before the schema added
    mlb.team_scoring / mlb.todays_games as stale so the edge agent
    doesn't keep SKIP'ing every MLBTOTAL/MLBSPREAD market.
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
        if not cache.get("economic"):
            print("[stats] cache missing economic block — treating as stale (schema upgrade)", flush=True)
            return False
        fetched_at = cache.get("fetched_at", "")
        if not fetched_at:
            return False
        dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
        age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
        return age_hours < 24
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
_ESPN_NBA_STANDINGS = "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings"
_ESPN_NHL_STANDINGS = "https://site.api.espn.com/apis/v2/sports/hockey/nhl/standings"
_HTTP_TIMEOUT = 15

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


def _fetch_mlb_todays_games(date_str: str, meta: dict[int, dict]) -> list[dict]:
    """Today's MLB schedule with probable pitchers and their season ERA/WHIP."""
    sched = _http_get_json(
        f"{_MLB_STATSAPI}/schedule?sportId=1&date={date_str}&hydrate=probablePitcher,team"
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

    out: list[dict] = []
    for g in games_raw:
        teams = g.get("teams", {})
        away_team = (teams.get("away", {}) or {}).get("team", {}) or {}
        home_team = (teams.get("home", {}) or {}).get("team", {}) or {}
        away_abbr = away_team.get("abbreviation") or meta.get(away_team.get("id"), {}).get("abbr", "")
        home_abbr = home_team.get("abbreviation") or meta.get(home_team.get("id"), {}).get("abbr", "")
        try:
            start_t = datetime.fromisoformat(g.get("gameDate", "").replace("Z", "+00:00")).strftime("%H:%M")
        except (TypeError, ValueError):
            start_t = "?"
        ap = (teams.get("away", {}) or {}).get("probablePitcher") or {}
        hp = (teams.get("home", {}) or {}).get("probablePitcher") or {}
        out.append({
            "away": away_abbr,
            "home": home_abbr,
            "start_time_utc": start_t,
            "away_pitcher": {
                "player": ap.get("fullName", ""),
                **(pitcher_stats.get(ap.get("id")) or {"era": None, "whip": None}),
            },
            "home_pitcher": {
                "player": hp.get("fullName", ""),
                **(pitcher_stats.get(hp.get("id")) or {"era": None, "whip": None}),
            },
        })
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
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {
        "standings": _fetch_mlb_standings(meta),
        "hitting_leaders": leaders["hitting_leaders"],
        "pitching_leaders": leaders["pitching_leaders"],
        "notable_players": [],
        "team_scoring": _fetch_mlb_team_scoring(meta),
        "todays_games": _fetch_mlb_todays_games(today, meta),
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
    econ = stats.get("economic", {}) or {}
    n_teams = len(mlb.get("standings", {}) or {})
    n_team_scoring = len(mlb.get("team_scoring", {}) or {})
    n_todays_games = len(mlb.get("todays_games", []) or [])
    n_nba_results = len(nba.get("playoff_results", []) or [])
    n_nhl_results = len(nhl.get("playoff_results", []) or [])
    n_hitters = len((mlb.get("hitting_leaders", {}) or {}).get("hr", []) or [])
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
            {"name": "Today's games", "value": str(n_todays_games), "inline": True},
            {"name": "NBA playoff results", "value": str(n_nba_results), "inline": True},
            {"name": "NHL playoff results", "value": str(n_nhl_results), "inline": True},
            {"name": "NBA round", "value": nba.get("current_round", "?"), "inline": True},
            {"name": "NHL round", "value": nhl.get("current_round", "?"), "inline": True},
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


def _seconds_until_next_hour(target_hour: int) -> float:
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


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
    }
    econ = fetch_economic_only() or {}
    btc = _fetch_btc_spot_coinbase()
    if btc:
        econ["btc_spot_usd"], econ["btc_source_time_utc"] = btc
    stats["economic"] = econ
    elapsed = time.time() - t0
    mlb_n = len(stats["mlb"].get("standings", {}))
    nba_n = len(stats["nba"].get("standings", []))
    nhl_n = len(stats["nhl"].get("standings", []))
    print(f"[stats] direct fetch done in {elapsed:.1f}s — mlb={mlb_n} nba={nba_n} nhl={nhl_n} btc={econ.get('btc_spot_usd')}", flush=True)
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
    bn = (econ.get("breaking_news") or "").strip()
    print(f"[macro] refreshed: gas=${gas} btc=${btc} breaking={bn!r}", flush=True)


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
    print(f"Kalshi Stats Agent starting — direct APIs (statsapi+ESPN) + Haiku for econ, hour={STATS_HOUR:02d}:00 UTC")
    # If the cache is missing or stale at startup, prime it immediately so
    # kalshi_edge can run before the next 06:00 wakeup.
    if not _is_cache_fresh():
        print("[stats] cache missing or >24h old — priming on startup", flush=True)
        _do_fetch_and_save()

    while True:
        wait_s = _seconds_until_next_hour(STATS_HOUR)
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
