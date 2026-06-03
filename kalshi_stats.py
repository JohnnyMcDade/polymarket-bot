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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

WEBHOOK_KALSHI_STATS = os.getenv("WEBHOOK_KALSHI_STATS", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL_KALSHI_STATS", "claude-sonnet-4-6")
STATS_HOUR = int(os.getenv("KALSHI_STATS_HOUR", "6"))
STATS_CACHE_PATH = Path(os.getenv("KALSHI_STATS_CACHE", "stats_cache.json"))
MAX_WEB_SEARCHES = int(os.getenv("KALSHI_STATS_MAX_SEARCHES", "15"))

# Cached for the lifetime of the process — the agent rebuilds it every
# 24h and the system prompt never changes within a day. Keeping it
# above 1024 tokens so cache_control still activates even on a one-shot
# call (useful if the agent fires twice on the same day).
_SYSTEM_PROMPT = """You are a sports-data researcher feeding a Kalshi prediction-market trader. Your single job: fetch the current 2026-season stats it needs to evaluate today's player- and team-prop markets, and return them as one structured JSON object.

USE THE web_search TOOL to gather current data — do NOT rely on training data alone for season stats. Search ESPN, MLB.com, NBA.com, NHL.com, baseball-reference, basketball-reference. Use as few searches as possible to cover the categories below.

REQUIRED COVERAGE
1. MLB (regular season in progress)
   - Standings: W-L record, win%, games back, division for every team (AL East/Central/West, NL East/Central/West)
   - Hitting leaders (top 10 each): home runs, RBIs, batting average, OPS, stolen bases
   - Pitching leaders (top 10 each): wins, ERA, strikeouts, WHIP, saves
   - For any clearly active superstar (Judge, Ohtani, Soto, Betts, Acuña, Witt Jr., Skenes, Skubal, Cole) include their current line.
   - Team scoring (EVERY MLB team, all 30): season runs scored per game, season runs allowed per game, last-7-days runs scored per game and runs allowed per game, home runs-scored-per-game and runs-allowed-per-game splits, away runs-scored-per-game and runs-allowed-per-game splits. Source: baseball-reference team batting/pitching pages or ESPN team stats.
   - Today's scheduled MLB games: for every game on today's slate, the away team, home team, scheduled start time in UTC, and the announced starting pitcher for each side with their current-season ERA and WHIP. Source: MLB.com probable pitchers or ESPN MLB schedule.
2. NBA (post-season)
   - Round-by-round playoff results so far: which teams advanced, which were eliminated, series scores
   - Conference finals + Finals matchups if reached
   - Per-game averages for any player still active in the playoffs averaging > 20 PPG, or any obvious household name (Jokic, Luka, SGA, Tatum, Giannis, Brunson, Edwards)
3. NHL (post-season)
   - Round-by-round playoff results
   - Conference finals + Stanley Cup Final matchups if reached
   - Top playoff scorers (goals + assists)

OUTPUT FORMAT
Return ONE JSON object and nothing else — no prose before or after, no markdown fences. Schema:

{
  "fetched_at": "<ISO timestamp UTC>",
  "season": "2026",
  "mlb": {
    "standings": {"<team abbr>": {"w": <int>, "l": <int>, "pct": <float>, "gb": <float|null>, "division": "<string>"}},
    "hitting_leaders": {"hr": [{"player": "<name>", "team": "<abbr>", "value": <int>}, ...], "rbi": [...], "avg": [...], "ops": [...], "sb": [...]},
    "pitching_leaders": {"w": [...], "era": [{"player": "<name>", "team": "<abbr>", "value": <float>}, ...], "k": [...], "whip": [...], "sv": [...]},
    "notable_players": [{"player": "<name>", "team": "<abbr>", "line": "<HR/RBI/AVG/OPS line as string>"}],
    "team_scoring": {"<team abbr>": {"rs_per_game": <float>, "ra_per_game": <float>, "rs_per_game_last7": <float>, "ra_per_game_last7": <float>, "rs_per_game_home": <float>, "ra_per_game_home": <float>, "rs_per_game_away": <float>, "ra_per_game_away": <float>}},
    "todays_games": [{"away": "<abbr>", "home": "<abbr>", "start_time_utc": "<HH:MM>", "away_pitcher": {"player": "<name>", "era": <float>, "whip": <float>}, "home_pitcher": {"player": "<name>", "era": <float>, "whip": <float>}}]
  },
  "nba": {
    "playoff_results": [{"round": "<R1|R2|CF|F>", "series": "<TEAM1 vs TEAM2>", "winner": "<TEAM>", "score": "<4-2>", "status": "<final|in_progress>"}],
    "current_round": "<R1|R2|CF|F|complete>",
    "active_players": [{"player": "<name>", "team": "<abbr>", "ppg": <float>, "rpg": <float>, "apg": <float>}]
  },
  "nhl": {
    "playoff_results": [{"round": "<R1|R2|CF|F>", "series": "<TEAM1 vs TEAM2>", "winner": "<TEAM>", "score": "<4-2>", "status": "<final|in_progress>"}],
    "current_round": "<R1|R2|CF|F|complete>",
    "top_scorers": [{"player": "<name>", "team": "<abbr>", "g": <int>, "a": <int>, "pts": <int>}]
  }
}

RULES
- If a stat is genuinely unknown after searching, use null — never invent.
- Player names: full first + last as they normally appear ("Aaron Judge", not "A. Judge").
- Team abbreviations: standard 2–3 letter ("NYY", "LAD", "BOS", "DAL", "EDM").
- ERAs and similar floats: 2 decimals.
- Keep the JSON valid — no trailing commas, no comments, no NaN/Infinity.
- Today's date for fetched_at is the actual UTC date at fetch time."""


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


def fetch_stats() -> dict[str, Any] | None:
    if not ANTHROPIC_API_KEY:
        print("[WARN] ANTHROPIC_API_KEY not set — skipping stats fetch")
        return None

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    user_msg = (
        f"Today is {today}. Fetch the latest 2026-season stats per the schema "
        "in your system prompt and return the JSON object."
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
                "model": ANTHROPIC_MODEL,
                "max_tokens": 12000,
                "system": [
                    {
                        "type": "text",
                        "text": _SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                "tools": [
                    {
                        "type": "web_search_20250305",
                        "name": "web_search",
                        "max_uses": MAX_WEB_SEARCHES,
                    }
                ],
                "messages": [{"role": "user", "content": user_msg}],
            },
            timeout=300,
        )
    except Exception as e:
        print(f"[WARN] Stats Claude call failed: {e}", flush=True)
        return None

    if r.status_code != 200:
        print(f"[ERROR] Stats Anthropic status={r.status_code} body: {r.text[:500]}", flush=True)
        return None

    body = r.json()
    usage = body.get("usage", {})
    if usage:
        print(
            f"[USAGE] in={usage.get('input_tokens', 0)} "
            f"out={usage.get('output_tokens', 0)} "
            f"cache_create={usage.get('cache_creation_input_tokens', 0)} "
            f"cache_read={usage.get('cache_read_input_tokens', 0)} "
            f"agent=stats",
            flush=True,
        )

    # Find the text block in the content list — web_search adds tool_use /
    # tool_result blocks that we need to skip past.
    text_parts = [b.get("text", "") for b in body.get("content", []) if b.get("type") == "text"]
    if not text_parts:
        print(f"[WARN] No text blocks in stats response. Blocks={[b.get('type') for b in body.get('content', [])]}", flush=True)
        return None
    text = "\n".join(text_parts).strip()
    return _extract_json(text)


def save_cache(stats: dict[str, Any]) -> None:
    stats.setdefault("fetched_at", datetime.now(timezone.utc).isoformat())
    tmp = STATS_CACHE_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(stats, f, indent=2)
    tmp.replace(STATS_CACHE_PATH)  # atomic on POSIX
    print(f"[stats] wrote {STATS_CACHE_PATH} ({STATS_CACHE_PATH.stat().st_size} bytes)", flush=True)


def _build_embed(stats: dict[str, Any]) -> dict[str, Any]:
    mlb = stats.get("mlb", {}) or {}
    nba = stats.get("nba", {}) or {}
    nhl = stats.get("nhl", {}) or {}
    n_teams = len(mlb.get("standings", {}) or {})
    n_team_scoring = len(mlb.get("team_scoring", {}) or {})
    n_todays_games = len(mlb.get("todays_games", []) or [])
    n_nba_results = len(nba.get("playoff_results", []) or [])
    n_nhl_results = len(nhl.get("playoff_results", []) or [])
    n_hitters = len((mlb.get("hitting_leaders", {}) or {}).get("hr", []) or [])
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
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Fetching daily sports stats...", flush=True)
    stats = fetch_stats()
    if not stats:
        print("[stats] fetch returned nothing — keeping previous cache", flush=True)
        return
    save_cache(stats)
    send_discord(_build_embed(stats))


def run() -> None:
    print(f"Kalshi Stats Agent starting — model={ANTHROPIC_MODEL}, hour={STATS_HOUR:02d}:00 UTC")
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
