"""Kalshi edge agent — batched Claude evaluator.

Fetches open Kalshi markets every KALSHI_EDGE_INTERVAL seconds, filters
out dead / illiquid / already-seen markets, then asks Claude (in
batches of EDGE_BATCH_SIZE) for a TRUE_PROBABILITY estimate using the
sports-stats JSON dumped by kalshi_stats.py as context.

Cost discipline (in priority order):
  1. NO Claude call if stats_cache.json is missing or > 24h old.
  2. NO Claude call if there are zero unseen markets in this cycle.
  3. Each call carries the full stats block + methodology in the system
     prompt with cache_control=ephemeral so back-to-back batches hit
     the prompt cache (cross-cycle hits are unlikely at 30-min cadence —
     don't count on them).
  4. Tiered BUY gate: HIGH at edge >= KALSHI_MIN_EDGE, or MEDIUM at the
     higher KALSHI_MEDIUM_MIN_EDGE floor. LOW never trades. Everything
     else gets dropped silently — no enqueue, no Discord.

Approved trades go to kalshi_queue stage "risk", which kalshi_trader
drains. Reuses the existing 4-stage queue rather than introducing a
new file — matches the prior pipeline's volatility profile.
"""

from __future__ import annotations

import json
import os
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

import kalshi_queue
import whale_signals
from kalshi_auth import KALSHI_BASE_URL, get_auth_headers

WEBHOOK_KALSHI_EDGE = os.getenv("WEBHOOK_KALSHI_EDGE", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL_KALSHI_EDGE", "claude-haiku-4-5-20251001")
CHECK_INTERVAL = int(os.getenv("KALSHI_EDGE_INTERVAL", "1800"))   # 30 min
MIN_EDGE = float(os.getenv("KALSHI_MIN_EDGE", "0.10"))             # 10%
# Sanity ceiling on claimed edge. >25% edge on liquid Kalshi markets is
# almost always Claude misreading the market (wrong bucket, in-progress
# game it can't see, etc.) — clamp the recorded edge and force SKIP.
MAX_EDGE = float(os.getenv("KALSHI_MAX_EDGE", "0.25"))             # 25%
# Tiered BUY gate: HIGH confidence trades at MIN_EDGE, MEDIUM only at the
# higher MEDIUM_MIN_EDGE. The extra edge buffer on MEDIUM compensates
# for the model's admitted uncertainty — without it, every borderline
# market the model isn't sure about would slip through.
MEDIUM_MIN_EDGE = float(os.getenv("KALSHI_MEDIUM_MIN_EDGE", "0.08"))  # 8%
BATCH_SIZE = int(os.getenv("KALSHI_EDGE_BATCH_SIZE", "10"))
MAX_MARKETS_PER_CYCLE = int(os.getenv("KALSHI_EDGE_MAX_MARKETS", "40"))
DEBUG_LOG = os.getenv("KALSHI_EDGE_DEBUG_LOG", "").lower() in ("1", "true", "yes")

# Whale-signal boost: bump confidence one level when a recent same-side
# whale trade backs our recommendation. Turn off with WHALE_BOOST_ENABLED=false.
WHALE_BOOST_ENABLED = os.getenv("WHALE_BOOST_ENABLED", "true").lower() in ("1", "true", "yes")
WHALE_BOOST_MIN_USD = float(os.getenv("WHALE_BOOST_MIN_USD", "1000"))
WHALE_BOOST_MAX_AGE_SECS = float(os.getenv("WHALE_BOOST_MAX_AGE_SECS", "3600"))
_CONFIDENCE_LADDER = {"LOW": "MEDIUM", "MEDIUM": "HIGH"}

# Backtest-validated boost (MLB game-winner only): bump MEDIUM → HIGH
# when the team we're betting has a rolling last-3-starts ERA <= X and
# the two teams' wpct differ by >= Y. Thresholds come from a 180-day
# v3 backtest in backtest_kalshi.py — the elite-rolling-ERA + meaningful-
# record-gap cohorts cleared 55%+ directional accuracy with Wilson 95%
# lower bound roughly around break-even. Lower confidence than the
# whale-boost (real-money flow signal), so keep it MLB-only and tunable.
BACKTEST_BOOST_ENABLED = os.getenv("KALSHI_BACKTEST_BOOST_ENABLED", "true").lower() in ("1", "true", "yes")
BACKTEST_BOOST_ROLLING_ERA_MAX = float(os.getenv("KALSHI_BACKTEST_BOOST_ROLLING_ERA_MAX", "2.50"))
BACKTEST_BOOST_WPCT_GAP_MIN = float(os.getenv("KALSHI_BACKTEST_BOOST_WPCT_GAP_MIN", "0.10"))
# Price-aware filter: don't boost if Kalshi's YES ask is already above the
# break-even price. The 2025 backtest showed HIGH-confidence picks (which
# is what we'd be upgrading into) clear conservative Wilson break-even at
# ~60c YES. Default 62c gives a thin profit margin without being so strict
# that the boost almost never fires. Set to 100 to disable price filtering.
BACKTEST_BOOST_MAX_ASK_CENTS = int(os.getenv("KALSHI_BACKTEST_BOOST_MAX_ASK_CENTS", "62"))

# Timing window. Pre-game stats only have an edge before the game starts,
# and odds move fast in the last few minutes — so we want close_time to be
# at least MIN_SECS_TO_CLOSE in the future but no more than MAX_SECS_TO_CLOSE.
# OPEN_AGE_IN_PROGRESS + CLOSE_SOON_FOR_IN_PROGRESS together flag "market
# opened hours ago and closes soon" as likely-in-progress and skip it.
MIN_SECS_TO_CLOSE = int(os.getenv("KALSHI_MIN_SECS_TO_CLOSE", "1800"))      # 30 min
MAX_SECS_TO_CLOSE = int(os.getenv("KALSHI_MAX_SECS_TO_CLOSE", "86400"))     # 24 h
OPEN_AGE_IN_PROGRESS = int(os.getenv("KALSHI_OPEN_AGE_IN_PROGRESS", "10800"))   # 3 h
CLOSE_SOON_FOR_IN_PROGRESS = int(os.getenv("KALSHI_CLOSE_SOON_IN_PROGRESS", "7200"))  # 2 h
STATS_CACHE_PATH = Path(os.getenv("KALSHI_STATS_CACHE", "/app/data/stats_cache.json"))
SEEN_CACHE_PATH = Path(os.getenv("KALSHI_EDGE_SEEN_CACHE", "/app/data/edge_seen.json"))
PRICE_HISTORY_PATH = Path(os.getenv("KALSHI_EDGE_PRICE_HISTORY", "/app/data/edge_price_history.json"))

# Line-movement guard. If a market's YES price has shifted more than this
# many cents since we last saw it, someone with information we don't have
# has been trading — skip the cycle for this ticker and let it stabilize.
MAX_LINE_MOVE_CENTS = int(os.getenv("KALSHI_MAX_LINE_MOVE_CENTS", "5"))
# Ignore stored prices older than this — a 3-day-old price isn't a
# "movement signal," it's just a different market state.
PRICE_HISTORY_MAX_AGE_SECS = int(os.getenv("KALSHI_PRICE_HISTORY_MAX_AGE", "86400"))

# Sports series we actually evaluate. Pulling /markets per series with a
# close-time window cuts the fetch from 5000 mostly-irrelevant rows to
# only in-window contracts the stats cache can actually score.
SERIES_TICKERS = [
    s.strip() for s in os.getenv(
        "KALSHI_EDGE_SERIES",
        "KXMLBGAME,KXMLBTOTAL,KXMLBSPREAD,KXNHLGAME,KXATPMATCH,KXWTAMATCH,"
        "KXNBAGAME,KXAAAGASD,KXCPI,KXFED,KXBTC",
    ).split(",") if s.strip()
]

# ─── Seen-cache: skip markets we've already evaluated ───────────────────

def _load_seen() -> set[str]:
    if not SEEN_CACHE_PATH.exists():
        return set()
    try:
        with SEEN_CACHE_PATH.open() as f:
            data = json.load(f)
        return set(data.get("tickers", []))
    except Exception as e:
        print(f"[WARN] edge_seen.json unreadable: {e}", flush=True)
        return set()


# KXBTC (and KXBTCD) hourly/daily "price range on <date>?" markets are
# contiguous narrow buckets — every ticker in the set is one bucket, and
# the `B`/`T` prefix is the bucket id, NOT a below/above threshold. The
# model has repeatedly read `B72750` as "below $72,750", taken YES at 1¢
# expecting +0.98 edge, and lost every time because the bucket itself is
# what resolves. Drop them before any Claude call. Binary BTC markets
# (KXBTC15M "BTC up in next 15 mins?") live in a different series and are
# unaffected. Suffix `[BT]<digits>` is the bucket signature; other
# suffixes (YES/NO, plain digits, etc.) pass through.
_BTC_BUCKET_SUFFIX_RE = re.compile(r"^[BT][\d.]+$")


def _is_btc_bucket_ticker(ticker: str) -> bool:
    if not ticker.startswith("KXBTC"):
        return False
    parts = ticker.split("-")
    if len(parts) < 3:
        return False
    return bool(_BTC_BUCKET_SUFFIX_RE.match(parts[-1]))


# ─── Price history: skip markets that moved sharply since last cycle ───
# Persisted so the signal survives a restart. Stored as {ticker: {price_cents, ts}}.

def _load_price_history() -> dict[str, dict[str, float]]:
    if not PRICE_HISTORY_PATH.exists():
        return {}
    try:
        with PRICE_HISTORY_PATH.open() as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"[WARN] edge_price_history.json unreadable: {e}", flush=True)
        return {}


def _save_price_history(history: dict[str, dict[str, float]]) -> None:
    PRICE_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = PRICE_HISTORY_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(history, f)
    tmp.replace(PRICE_HISTORY_PATH)


def _save_seen(seen: set[str]) -> None:
    if len(seen) > 20_000:
        # Cap memory + disk. Drop oldest by simple slicing; we have no
        # ordering signal in a set, so this is a coarse trim.
        seen = set(list(seen)[-10_000:])
    SEEN_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = SEEN_CACHE_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump({"tickers": sorted(seen)}, f)
    tmp.replace(SEEN_CACHE_PATH)


# ─── Stats cache ────────────────────────────────────────────────────────

def _load_stats_cache() -> dict[str, Any] | None:
    """Return stats dict if fresh (<24h), else None — caller skips the cycle."""
    if not STATS_CACHE_PATH.exists():
        print("[edge] stats_cache.json missing — skipping cycle", flush=True)
        return None
    try:
        with STATS_CACHE_PATH.open() as f:
            cache = json.load(f)
    except Exception as e:
        print(f"[edge] stats_cache.json unreadable: {e} — skipping cycle", flush=True)
        return None

    fetched_at = cache.get("fetched_at", "")
    if not fetched_at:
        print("[edge] stats_cache.json has no fetched_at — skipping cycle", flush=True)
        return None
    try:
        dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
    except ValueError:
        print(f"[edge] stats_cache.json fetched_at unparseable: {fetched_at!r}", flush=True)
        return None
    # Claude sometimes returns a date-only string ("2026-05-31"), which
    # fromisoformat parses to a NAIVE datetime. Subtracting that from an
    # aware datetime raises TypeError and crashes the whole cycle.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age_h = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    if age_h > 24:
        print(f"[edge] stats_cache is {age_h:.1f}h old (>24h) — skipping cycle", flush=True)
        return None
    return cache


# ─── Market fetch + filter ──────────────────────────────────────────────

def fetch_markets() -> list[dict[str, Any]]:
    path = "/trade-api/v2/markets"
    out: list[dict[str, Any]] = []
    now_ts = int(datetime.now(timezone.utc).timestamp())
    # Kalshi sports markets close 1-3 weeks AFTER the game itself
    # (settlement window) — close_time is NOT the game time. The
    # in-process timing check uses expected_expiration_time instead;
    # this 30-day bound is just a sanity ceiling for the API fetch.
    max_close_ts = now_ts + 30 * 86400
    for series in SERIES_TICKERS:
        cursor = None
        fetched = 0
        try:
            for _ in range(5):
                params: dict[str, Any] = {
                    "limit": 1000,
                    "status": "open",
                    "series_ticker": series,
                    "max_close_ts": max_close_ts,
                }
                if cursor:
                    params["cursor"] = cursor
                r = requests.get(
                    f"{KALSHI_BASE_URL}/markets",
                    headers=get_auth_headers("GET", path),
                    params=params,
                    timeout=15,
                )
                r.raise_for_status()
                data = r.json()
                markets = data.get("markets", []) or []
                out.extend(markets)
                fetched += len(markets)
                cursor = data.get("cursor")
                if not cursor:
                    break
        except Exception as e:
            print(f"[WARN] Kalshi fetch failed series={series}: {e}", flush=True)
            continue
        print(f"[edge] fetched series={series} count={fetched}", flush=True)
    return out


def _parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _check_timing(game_time_str: str, open_time_str: str,
                  now: datetime) -> tuple[bool, str, float]:
    """Return (ok, drop_reason, seconds_to_game).

    game_time_str should be the market's expected_expiration_time /
    occurrence_datetime (the game itself), NOT close_time (which is the
    post-game settlement window — often 1-3 weeks later for NHL/NBA).

    drop_reason is "" when ok=True, otherwise one of:
      no_close, ended, starting_soon, too_far, in_progress
    """
    close_dt = _parse_iso(game_time_str)
    if close_dt is None:
        return False, "no_close", 0.0
    secs = (close_dt - now).total_seconds()
    if secs <= 0:
        return False, "ended", secs
    if secs < MIN_SECS_TO_CLOSE:
        return False, "starting_soon", secs
    if secs > MAX_SECS_TO_CLOSE:
        return False, "too_far", secs
    open_dt = _parse_iso(open_time_str)
    if open_dt is not None:
        opened_ago = (now - open_dt).total_seconds()
        if opened_ago > OPEN_AGE_IN_PROGRESS and secs < CLOSE_SOON_FOR_IN_PROGRESS:
            return False, "in_progress", secs
    return True, "", secs


# Player / team name extraction. The stats cache JSON already names every
# relevant player and team, so the "parse" is really a substring match
# from the title against the cache — that's the safest signal we have
# without a full NER pass.

def _extract_entities(title: str, stats: dict[str, Any]) -> list[str]:
    found: list[str] = []
    title_l = title.lower()

    # MLB players from leader boards + notable list
    mlb = stats.get("mlb", {}) or {}
    candidates: set[str] = set()
    for board in (mlb.get("hitting_leaders", {}) or {}).values():
        for row in board or []:
            if row.get("player"):
                candidates.add(row["player"])
    for board in (mlb.get("pitching_leaders", {}) or {}).values():
        for row in board or []:
            if row.get("player"):
                candidates.add(row["player"])
    for row in mlb.get("notable_players", []) or []:
        if row.get("player"):
            candidates.add(row["player"])
    # NBA active
    for row in (stats.get("nba", {}) or {}).get("active_players", []) or []:
        if row.get("player"):
            candidates.add(row["player"])
    # NHL top scorers
    for row in (stats.get("nhl", {}) or {}).get("top_scorers", []) or []:
        if row.get("player"):
            candidates.add(row["player"])
    # ATP / WTA ranked players (rankings) + names from recent matches.
    # Including *_recent picks up qualifiers and lower-ranked players who
    # only appear when they actually play, so title-match still finds them.
    tennis = stats.get("tennis", {}) or {}
    for board_key in ("atp_rankings", "wta_rankings"):
        for row in tennis.get(board_key, []) or []:
            if row.get("player"):
                candidates.add(row["player"])
    for results_key in ("atp_recent", "wta_recent"):
        for row in tennis.get(results_key, []) or []:
            for side in ("winner", "loser"):
                name = row.get(side)
                if name:
                    candidates.add(name)
    # MLB team abbrevs from standings
    teams = set((mlb.get("standings", {}) or {}).keys())

    for name in candidates:
        # Match on last name (more reliable than full-name string match
        # because titles often abbreviate first names).
        last = name.split()[-1].lower() if name else ""
        if last and len(last) >= 4 and last in title_l:
            found.append(name)
    for abbr in teams:
        if abbr and re.search(rf"\b{re.escape(abbr)}\b", title, re.IGNORECASE):
            found.append(abbr)

    # Dedupe, preserve order
    seen: set[str] = set()
    out: list[str] = []
    for e in found:
        if e not in seen:
            seen.add(e)
            out.append(e)
    return out


_MLB_TICKER_MONTH = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
    "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}


def _mlb_game_meta_for(ticker: str, stats: dict[str, Any]) -> dict[str, Any] | None:
    """Match a KXMLBGAME ticker to its mlb.upcoming_games entry. Ticker
    body is YYMONDDhhmm<TEAMS>; we parse the date and find the upcoming
    games entry whose game_date matches and whose home or away abbr
    matches the contract team (the segment after the last hyphen).
    Returns the matched entry merged with contract_team, or None.
    """
    if not ticker.startswith("KXMLBGAME"):
        return None
    parts = ticker.split("-")
    if len(parts) < 3:
        return None
    body, contract_team = parts[1], parts[2]
    if len(body) < 7:
        return None
    yy, mon, dd = body[:2], body[2:5], body[5:7]
    mm = _MLB_TICKER_MONTH.get(mon.upper())
    if not mm:
        return None
    game_date = f"20{yy}-{mm}-{dd}"
    for g in (stats.get("mlb", {}) or {}).get("upcoming_games", []) or []:
        if g.get("game_date") != game_date:
            continue
        if g.get("home") == contract_team or g.get("away") == contract_team:
            return {**g, "contract_team": contract_team}
    return None


def _backtest_boost_applies(
    item: dict[str, Any], pred: dict[str, Any], stats: dict[str, Any]
) -> tuple[bool, dict[str, Any]]:
    """Return (True, info_dict) if the backtest-validated cohort matches:
      (1) MLB game-winner ticker resolvable to an upcoming_games entry
      (2) the starter for the team we're betting has rolling_era_last3
          <= BACKTEST_BOOST_ROLLING_ERA_MAX
      (3) |home_wpct - away_wpct| >= BACKTEST_BOOST_WPCT_GAP_MIN
    The edge agent always trades BUY YES (see payload construction), so
    the team we're betting = the contract team in the ticker. info_dict
    carries the values that feed the [BACKTEST-BOOST] / [BACKTEST-BOOST-SKIP]
    log lines: bet_team, starter, rolling_era, wpct_diff.
    """
    meta = _mlb_game_meta_for(item.get("ticker", ""), stats)
    if not meta:
        return False, {}
    bet_team = meta["contract_team"]
    if bet_team == meta.get("home"):
        starter = meta.get("home_pitcher") or {}
    elif bet_team == meta.get("away"):
        starter = meta.get("away_pitcher") or {}
    else:
        return False, {}
    rolling = starter.get("rolling_era_last3")
    if rolling is None or rolling > BACKTEST_BOOST_ROLLING_ERA_MAX:
        return False, {}
    standings = ((stats.get("mlb", {}) or {}).get("standings", {}) or {})
    home_pct = (standings.get(meta.get("home")) or {}).get("pct")
    away_pct = (standings.get(meta.get("away")) or {}).get("pct")
    if home_pct is None or away_pct is None:
        return False, {}
    wpct_diff = abs(float(home_pct) - float(away_pct))
    if wpct_diff < BACKTEST_BOOST_WPCT_GAP_MIN:
        return False, {}
    info = {
        "bet_team": bet_team,
        "starter": starter.get("player") or "?",
        "rolling_era": rolling,
        "wpct_diff": wpct_diff,
    }
    return True, info


def _filter_markets(markets: list[dict[str, Any]], stats: dict[str, Any],
                    seen: set[str],
                    price_history: dict[str, dict[str, float]]) -> list[dict[str, Any]]:
    drops = {"seen": 0, "dead": 0, "illiquid": 0, "btc_bucket": 0, "line_moved": 0}
    timing_drops = {"no_close": 0, "ended": 0, "starting_soon": 0,
                    "too_far": 0, "in_progress": 0}
    now = datetime.now(timezone.utc)
    now_ts = now.timestamp()
    kept: list[dict[str, Any]] = []
    for m in markets:
        ticker = m.get("ticker") or ""
        if not ticker:
            continue
        if ticker in seen:
            drops["seen"] += 1
            continue
        if _is_btc_bucket_ticker(ticker):
            drops["btc_bucket"] += 1
            seen.add(ticker)
            continue
        ya = float(m.get("yes_ask_dollars", 0) or 0)
        if ya <= 0.0:
            drops["illiquid"] += 1
            continue
        if ya >= 0.99:
            drops["dead"] += 1
            continue
        game_time = (
            m.get("expected_expiration_time")
            or m.get("occurrence_datetime")
            or m.get("close_time", "")
        )
        ok, reason, secs_to_close = _check_timing(
            game_time, m.get("open_time", ""), now
        )
        if not ok:
            timing_drops[reason] += 1
            continue

        yes_cents = int(round(ya * 100))

        # Line-movement guard. Use the persisted price for this ticker; if
        # the YES price has shifted more than MAX_LINE_MOVE_CENTS since we
        # last looked, someone with private information has been trading —
        # skip this cycle and update history so the next cycle can re-check
        # against the new baseline. New tickers (no prior entry) pass through.
        prior = price_history.get(ticker)
        if prior and (now_ts - float(prior.get("ts", 0))) <= PRICE_HISTORY_MAX_AGE_SECS:
            move = abs(yes_cents - float(prior.get("price_cents", yes_cents)))
            if move > MAX_LINE_MOVE_CENTS:
                drops["line_moved"] += 1
                print(
                    f"[edge] line-move skip {ticker}: "
                    f"{prior.get('price_cents')}¢ → {yes_cents}¢ "
                    f"(Δ{move:.0f}¢ > {MAX_LINE_MOVE_CENTS}¢)",
                    flush=True,
                )
                price_history[ticker] = {"price_cents": yes_cents, "ts": now_ts}
                continue
        price_history[ticker] = {"price_cents": yes_cents, "ts": now_ts}

        title = m.get("title", "") or ""
        entities = _extract_entities(title, stats)
        kept.append({
            "ticker": ticker,
            "title": title,
            "yes_ask_cents": yes_cents,
            "hours_left": round(secs_to_close / 3600, 1),
            "close_time": m.get("close_time", ""),
            "open_time": m.get("open_time", ""),
            "entities": entities,
        })
        if len(kept) >= MAX_MARKETS_PER_CYCLE:
            break

    print(
        f"[edge] filter: kept={len(kept)} "
        f"seen={drops['seen']} dead={drops['dead']} illiquid={drops['illiquid']} "
        f"btc_bucket={drops['btc_bucket']} line_moved={drops['line_moved']}",
        flush=True,
    )
    print(
        f"[TIMING] dropped={sum(timing_drops.values())} "
        f"ended={timing_drops['ended']} "
        f"starting_soon={timing_drops['starting_soon']} "
        f"in_progress={timing_drops['in_progress']} "
        f"too_far={timing_drops['too_far']} "
        f"no_close={timing_drops['no_close']} "
        f"(window: {MIN_SECS_TO_CLOSE//60}min–{MAX_SECS_TO_CLOSE//3600}h)",
        flush=True,
    )
    return kept


# ─── Claude call ────────────────────────────────────────────────────────

_METHODOLOGY = f"""You are a Kalshi prediction-market edge finder. For each market in the user message, estimate the TRUE probability of YES using the STATS CONTEXT block in this system prompt.

The STATS CONTEXT block has two halves:
- SPORTS STATS — team scoring, pitcher ERA/WHIP, standings, leaders, and an mlb.upcoming_games list of matchups tagged with `game_date` (YYYY-MM-DD, US Eastern) and probable pitchers. Each probable pitcher entry also carries `rolling_era_last3` (IP-weighted ERA over his last 3 starts) and `vs_opponent` — head-to-head stats versus today's opposing team: `{starts, era_vs, whip_vs, avg_runs_last3_vs}`. `vs_opponent` is null when the starter has no prior appearances against this opponent. For tennis, `tennis.atp_rankings` / `tennis.wta_rankings` carry the current top-ranked players, and `tennis.atp_recent` / `tennis.wta_recent` carry the last ~10 days of completed match results (winner, loser, score, event). Use these for MLB / NHL / NBA / ATP / WTA markets.
- ECONOMIC DATA — current national gas price, latest CPI, Fed funds target + next FOMC meeting expectations, BTC spot. Use these for KXAAAGASD / KXCPI / KXFED / KXBTC markets, combined with your own knowledge of macro trends, central-bank reaction functions, and recent price action.

EDGE = true_probability - market_implied_probability  (market price in cents / 100)

RECOMMENDATION RULES
- BUY only if you have specific, directly relevant data AND one of these tiers is met:
    - HIGH confidence with edge >= +{MIN_EDGE:.2f}
    - MEDIUM confidence with edge >= +{MEDIUM_MIN_EDGE:.2f}
  The MEDIUM tier requires a larger edge because MEDIUM means you are admitting uncertainty — the extra margin compensates for that uncertainty. Required data:
    - for sports: the named team or player appears in SPORTS STATS
    - for economic: the current value of the macro variable is in ECONOMIC DATA and resolution is close enough that the variable is unlikely to swing materially
- SKIP in every other case — including BUY_NO opportunities. We only act on positive-edge BUY_YES bets in this build.
- SKIP if the market's resolution depends on something not covered by either block (politics, weather, awards, esports, etc.).

CONFIDENCE GUIDANCE
- HIGH: data directly answers the question (e.g. "Will Judge hit 50 HR?" with HR count + pace in stats; "Will gas avg be < $3.50 on Jun 5?" with current gas at $3.42 and stable trend), market resolves within the next 24 hours, no obvious lurking-variable risk
- MEDIUM: data is relevant but partial (team standings inform a division winner but a lot can change; a CPI print is a week away and you have last month but not consensus)
- LOW: data is tangential or stale relative to the market

RESPONSE FORMAT
For each input market emit exactly one block in this format, separated by a line containing only three dashes:

TICKER: <ticker echoed from input>
TRUE_PROBABILITY: <float 0.0-1.0>
EDGE: <float -1.0-1.0>
CONFIDENCE: <LOW|MEDIUM|HIGH>
RECOMMENDATION: <BUY|SKIP>
REASONING: <one sentence pointing at the specific stat that drove the call>
---

EDGE SANITY CAP (GLOBAL OVERRIDE — APPLIES TO EVERY MARKET)
- Real edge on liquid Kalshi markets almost never exceeds 25%. If you computed an edge of +0.25 or higher, you are almost certainly misreading the market — wrong bucket, in-progress game whose live state you cannot see, resolution criteria you misunderstood, or a stale stat masquerading as current.
- When this happens, set RECOMMENDATION: SKIP and CONFIDENCE: LOW. Do not BUY at +25% claimed edge regardless of how obvious the reasoning feels.

KXBTC RANGE-BUCKET MARKETS (READ BEFORE EVALUATING ANY KXBTC TICKER)
- A KXBTC ticker shaped `KXBTC-<dateHour>-B<num>` or `-T<num>` (e.g. KXBTC-26JUN0501-B72750, KXBTC-26JUN0617-T57200) with title "Bitcoin price range on <date>?" is ONE bucket inside a contiguous set of narrow price buckets — NOT a "below <num>" or "above <num>" threshold.
- The `B` / `T` prefix is a bucket identifier, not "below" / "above". Each bucket's true probability of resolving YES is small (often 1–5%) regardless of where the bucket number sits relative to BTC spot.
- A market trading at 1–5¢ on a bucket far from spot is correctly priced, not mispriced. Buying YES on such a bucket because "BTC spot is far from <num>" is the systematic error pattern we are explicitly blocking — past trades of this shape lost 100% of the time.
- For any KXBTC ticker matching this shape, RECOMMENDATION must be SKIP regardless of computed edge. Set CONFIDENCE: LOW and REASONING: "KXBTC range-bucket ticker — narrow bucket, not a cumulative threshold; skipping per rule."
- Binary BTC markets in other series (e.g. KXBTC15M "BTC price up in next 15 mins?") are not affected by this rule.

GAS / THRESHOLD MARKETS (KXAAAGASD AND SIMILAR DAILY-AVERAGE TICKERS)
- These markets resolve on the AAA daily national-average gas price for a specific date. The threshold is in the ticker tail (e.g. KXAAAGASD-26JUN04-4.260 → threshold $4.260).
- If the current value of the underlying is within 0.5% of the threshold, treat the market as a coin flip. RECOMMENDATION: SKIP regardless of computed edge — daily settlement variance and rounding dominate any apparent edge from "we are already above/below by a tenth of a cent."
- Only BUY when the current value is at least 0.5% on the favored side of the threshold AND no plausible 1-day move closes that gap.

MLB GAME WINNER (KXMLBGAME) EDGE CEILING
- Real edge on KXMLBGAME (game winner) markets rarely exceeds 15% even with an elite starter. Bullpen depth, lineup matchups, weather, and umpire variance all compress edge fast — Cy Young pitchers still go 20-10, not 30-0.
- If you compute edge above +0.15 on a KXMLBGAME ticker, set RECOMMENDATION: SKIP regardless of computed edge. This ceiling does NOT apply to KXMLBSPREAD or KXMLBTOTAL — those have different variance profiles.

MLB PITCHER VS OPPONENT (HEAD-TO-HEAD)
- When `vs_opponent` is present with `starts >= 2` and `avg_runs_last3_vs <= 2.0` AND `era_vs <= 3.50`, treat it as a meaningful H2H favorite signal for the pitcher's team — but a soft one. Cap any H2H-driven upgrade at one confidence tier (LOW→MEDIUM, MEDIUM→HIGH) and do not let it raise computed edge above the +0.15 KXMLBGAME ceiling above.
- When `vs_opponent.avg_runs_last3_vs >= 5.0` OR `era_vs >= 6.00` across `starts >= 2`, the H2H signal points AGAINST the pitcher's team. Do not BUY YES on the pitcher's team in that case unless season ERA + rolling_era_last3 both clearly dominate the opponent's bats.
- For KXMLBTOTAL markets, weight `avg_runs_last3_vs` and `era_vs` from BOTH starters jointly — two pitchers with low H2H runs allowed argues UNDER, two with high argues OVER.
- `vs_opponent: null` means no prior matchup — do NOT treat absence as a positive or negative signal.

TENNIS MATCH WINNER (KXATPMATCH / KXWTAMATCH) RULES
- Both players named in the title must appear in `tennis.atp_rankings` (or `tennis.wta_rankings`) — match against the surname in `player`. If only one is ranked, set RECOMMENDATION: SKIP.
- A 30+ rank gap between two top-100 players is roughly a 65/35 favorite. A 50+ gap is roughly 75/25. Use these as anchors; do not claim >85% favorite probability for any match without lopsided recent form to back it.
- Recent form: weight the last ~10 days of `*_recent` matches for both players. Two recent wins by the underdog over comparably-ranked opponents should compress, not extend, the favorite edge.
- If either player has no recent matches in the cache, set RECOMMENDATION: SKIP — without recent form we cannot anchor the call.

MLB DATE MATCHING (READ BEFORE EVALUATING ANY MLB MARKET)
- MLB market tickers encode the game date as YYMMMDD followed by HHMM and the away+home team abbreviations, e.g. KXMLBSPREAD-26JUN041335CLENYY → 2026-06-04 13:35 CLE@NYY.
- For ANY MLB market, locate the entry in mlb.upcoming_games whose `game_date` matches the ticker date AND whose away/home abbreviations match. Use the pitchers and stats from THAT entry.
- If no upcoming_games entry matches the ticker date, SKIP. Do NOT fall back to team_scoring alone or to a different day's pitching matchup — yesterday's starter is rarely tomorrow's starter, and using the wrong pitcher silently breaks the edge calculation.

CRITICAL RULES
- Echo TICKER exactly so we can match outputs to inputs.
- Always emit --- after every block including the last.
- No prose before, between, or after the blocks. No markdown fences.
- One block per input market, no skipping, no extras."""


def _build_system_prompt(stats: dict[str, Any]) -> str:
    # Keep the stats JSON compact in the prompt — every extra token is
    # paid for on the first call of the day (cache write). Drop indentation.
    stats_json = json.dumps(stats, separators=(",", ":"))
    return (
        _METHODOLOGY
        + "\n\nSTATS CONTEXT (refresh date in fetched_at, includes both sports and economic blocks):\n"
        + stats_json
    )


def _build_user_message(items: list[dict[str, Any]]) -> str:
    blocks = []
    for i, it in enumerate(items, 1):
        ya = it["yes_ask_cents"]
        entities = ", ".join(it.get("entities") or []) or "(none matched)"
        blocks.append(
            f"=== MARKET {i} ===\n"
            f"TICKER: {it['ticker']}\n"
            f"TITLE: {it['title']}\n"
            f"HOURS UNTIL RESOLUTION: {it['hours_left']}\n"
            f"MARKET YES PRICE: {ya}¢ (implies {ya/100:.2%} YES)\n"
            f"STATS ENTITIES MATCHED: {entities}"
        )
    return "\n\n".join(blocks)


def _parse_response(text: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for block in (b.strip() for b in text.split("---")):
        if not block:
            continue
        fields: dict[str, str] = {}
        for line in block.splitlines():
            m = re.match(r"^\s*([A-Z_]+)\s*:\s*(.+)$", line)
            if m:
                fields[m.group(1).strip()] = m.group(2).strip()
        ticker = fields.get("TICKER", "").strip()
        if not ticker:
            continue
        try:
            true_prob = float(fields.get("TRUE_PROBABILITY", "nan"))
            edge = float(fields.get("EDGE", "nan"))
        except ValueError:
            print(f"[WARN] edge: unparseable numbers for {ticker}: {fields}", flush=True)
            continue
        if not (0 <= true_prob <= 1) or not (-1 <= edge <= 1):
            print(f"[WARN] edge: out-of-range for {ticker}: prob={true_prob} edge={edge}", flush=True)
            continue
        out[ticker] = {
            "true_probability": true_prob,
            "edge": edge,
            "confidence": fields.get("CONFIDENCE", "LOW").upper(),
            "recommendation": fields.get("RECOMMENDATION", "SKIP").upper(),
            "reasoning": fields.get("REASONING", ""),
        }
    return out


def _ask_claude(system_prompt: str, batch: list[dict[str, Any]],
                log_raw: bool = False) -> dict[str, dict[str, Any]]:
    if not ANTHROPIC_API_KEY or not batch:
        return {}
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
                "max_tokens": 200 * len(batch),
                "system": [
                    {
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                "messages": [{"role": "user", "content": _build_user_message(batch)}],
            },
            timeout=120,
        )
    except Exception as e:
        print(f"[WARN] edge Claude call failed: {e}", flush=True)
        return {}

    if r.status_code == 429:
        print("[WARN] edge Anthropic rate-limited — skipping batch", flush=True)
        return {}
    if r.status_code != 200:
        print(f"[ERROR] edge Anthropic status={r.status_code}: {r.text[:500]}", flush=True)
        return {}

    body = r.json()
    usage = body.get("usage", {})
    if usage:
        print(
            f"[USAGE] in={usage.get('input_tokens', 0)} "
            f"out={usage.get('output_tokens', 0)} "
            f"cache_create={usage.get('cache_creation_input_tokens', 0)} "
            f"cache_read={usage.get('cache_read_input_tokens', 0)} "
            f"agent=edge batch={len(batch)}",
            flush=True,
        )
    text_parts = [b.get("text", "") for b in body.get("content", []) if b.get("type") == "text"]
    raw_text = "\n".join(text_parts)
    if log_raw and DEBUG_LOG:
        print(f"[edge-debug] raw Claude response (batch_size={len(batch)}):\n{raw_text}\n[edge-debug] end raw response", flush=True)
    return _parse_response(raw_text)


# ─── Discord ────────────────────────────────────────────────────────────

def _build_embed(item: dict[str, Any], pred: dict[str, Any]) -> dict[str, Any]:
    title = item.get("title", item.get("ticker", "?"))
    ticker = item.get("ticker", "")
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    edge_pct = pred["edge"] * 100
    return {
        "title": f"🎯 KALSHI EDGE — {title[:80]}",
        "url": market_url,
        "color": 0x2ECC71,
        "fields": [
            {"name": "Market Price", "value": f"{item.get('yes_ask_cents', '?')}¢", "inline": True},
            {"name": "True Probability", "value": f"{pred['true_probability']:.1%}", "inline": True},
            {"name": "Edge", "value": f"{edge_pct:+.1f}%", "inline": True},
            {"name": "Confidence", "value": pred["confidence"], "inline": True},
            {"name": "Recommendation", "value": pred["recommendation"], "inline": True},
            {"name": "Hours Left", "value": str(item.get("hours_left", "?")), "inline": True},
            {"name": "Reasoning", "value": pred.get("reasoning", "")[:500] or "—", "inline": False},
            {"name": "Market", "value": f"[View on Kalshi]({market_url})", "inline": False},
        ],
        "footer": {"text": f"PassivePoly Kalshi Edge  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_discord(embed: dict[str, Any]) -> None:
    if not WEBHOOK_KALSHI_EDGE:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_EDGE, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_EDGE, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Discord send failed: {e}")


# ─── Main loop ──────────────────────────────────────────────────────────

def run() -> None:
    print(
        f"Kalshi Edge Agent starting — model={ANTHROPIC_MODEL}, "
        f"interval={CHECK_INTERVAL}s, batch={BATCH_SIZE}, "
        f"min_edge_high={MIN_EDGE:.0%}, min_edge_medium={MEDIUM_MIN_EDGE:.0%}, "
        f"max_edge={MAX_EDGE:.0%}, "
        f"debug_log={DEBUG_LOG}"
    )
    print(
        f"[edge] timing env: MIN_SECS_TO_CLOSE={MIN_SECS_TO_CLOSE} "
        f"MAX_SECS_TO_CLOSE={MAX_SECS_TO_CLOSE} "
        f"OPEN_AGE_IN_PROGRESS={OPEN_AGE_IN_PROGRESS} "
        f"CLOSE_SOON_FOR_IN_PROGRESS={CLOSE_SOON_FOR_IN_PROGRESS}",
        flush=True,
    )
    print(
        f"[edge] backtest-boost: enabled={BACKTEST_BOOST_ENABLED} "
        f"rolling_era_max={BACKTEST_BOOST_ROLLING_ERA_MAX} "
        f"wpct_gap_min={BACKTEST_BOOST_WPCT_GAP_MIN} "
        f"max_ask_cents={BACKTEST_BOOST_MAX_ASK_CENTS}",
        flush=True,
    )
    seen = _load_seen()
    price_history = _load_price_history()
    print(
        f"[edge] loaded {len(seen)} previously-seen tickers, "
        f"{len(price_history)} price-history entries",
        flush=True,
    )
    cycle = 0

    while True:
        cycle += 1
        cycle_start = time.time()
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(
            f"[edge-heartbeat] cycle={cycle} ts={ts} seen={len(seen)} "
            f"interval={CHECK_INTERVAL}s",
            flush=True,
        )
        try:
            stats = _load_stats_cache()
            if stats is None:
                print(f"[edge] cycle={cycle} no-op: stats unavailable", flush=True)
            else:
                markets = fetch_markets()
                print(
                    f"[edge] cycle={cycle} fetched {len(markets)} open markets",
                    flush=True,
                )
                candidates = _filter_markets(markets, stats, seen, price_history)
                _save_price_history(price_history)

                if not candidates:
                    print(
                        f"[edge] cycle={cycle} no candidates after filter — "
                        "skipping Claude call",
                        flush=True,
                    )
                else:
                    system_prompt = _build_system_prompt(stats)
                    approved = 0
                    skipped = 0
                    for start in range(0, len(candidates), BATCH_SIZE):
                        batch = candidates[start : start + BATCH_SIZE]
                        preds = _ask_claude(system_prompt, batch, log_raw=(start == 0))
                        for it in batch:
                            ticker = it["ticker"]
                            seen.add(ticker)
                            pred = preds.get(ticker)
                            if not pred:
                                skipped += 1
                                if DEBUG_LOG:
                                    print(f"[edge-debug] {ticker} no prediction parsed", flush=True)
                                continue
                            if DEBUG_LOG:
                                print(
                                    f"[edge-debug] {ticker} rec={pred['recommendation']} "
                                    f"edge={pred['edge']:+.3f} conf={pred['confidence']} "
                                    f"true_p={pred['true_probability']:.3f} "
                                    f"ask={it['yes_ask_cents']}c hrs={it['hours_left']} "
                                    f"why={pred.get('reasoning', '')[:200]!r}",
                                    flush=True,
                                )
                            # Whale boost: if we're about to BUY YES and a
                            # >= WHALE_BOOST_MIN_USD whale hit YES on the
                            # same ticker recently, bump conf one notch
                            # (LOW->MEDIUM, MEDIUM->HIGH). Direction must
                            # match — we don't BUY NO, and a NO whale is a
                            # countersignal, not a boost.
                            if (
                                WHALE_BOOST_ENABLED
                                and pred["recommendation"] == "BUY"
                                and pred["confidence"] in _CONFIDENCE_LADDER
                            ):
                                sig = whale_signals.get_signal(
                                    ticker, side="yes",
                                    min_value_usd=WHALE_BOOST_MIN_USD,
                                    max_age_secs=WHALE_BOOST_MAX_AGE_SECS,
                                )
                                if sig:
                                    old_conf = pred["confidence"]
                                    pred["confidence"] = _CONFIDENCE_LADDER[old_conf]
                                    age = time.time() - sig["ts"]
                                    pred["reasoning"] = (
                                        f"[whale-boost {old_conf}->{pred['confidence']}] "
                                        + pred.get("reasoning", "")
                                    )
                                    print(
                                        f"[WHALE-BOOST] {ticker} {old_conf}->{pred['confidence']} "
                                        f"whale=${sig['value_usd']:,.0f} YES age={age:.0f}s",
                                        flush=True,
                                    )
                            # Sanity cap: edges > MAX_EDGE are almost always
                            # model error (wrong bucket, in-progress game,
                            # misread resolution). Clamp the recorded edge
                            # AND force SKIP — under the tiered gate, just
                            # demoting confidence is no longer enough since
                            # MEDIUM also trades.
                            if pred["edge"] > MAX_EDGE:
                                print(
                                    f"[edge-cap] {ticker} raw_edge={pred['edge']:+.3f} "
                                    f"> {MAX_EDGE:.2f} cap — forcing SKIP",
                                    flush=True,
                                )
                                pred["edge"] = MAX_EDGE
                                pred["recommendation"] = "SKIP"
                            # Backtest-validated boost. Fires only on
                            # BUY-MEDIUM (we don't override Claude's
                            # SKIP / LOW) and runs AFTER the MAX_EDGE
                            # cap (so sanity SKIPs survive). Upgrades
                            # MEDIUM → HIGH on MLB game-winner markets
                            # where the favored team's starter has been
                            # elite over his last 3 starts AND the two
                            # teams' records differ meaningfully AND
                            # Kalshi's YES ask is below the break-even
                            # price (so the upgrade actually corresponds
                            # to a profitable bet, not just a directionally-
                            # correct one on an already-priced-in favorite).
                            if (
                                BACKTEST_BOOST_ENABLED
                                and pred["recommendation"] == "BUY"
                                and pred["confidence"] == "MEDIUM"
                            ):
                                applies, info = _backtest_boost_applies(it, pred, stats)
                                if applies:
                                    ask = int(it.get("yes_ask_cents") or 100)
                                    common = (
                                        f"era={info['rolling_era']:.2f} "
                                        f"gap={info['wpct_diff']:.3f} price={ask}c "
                                        f"bet={info['bet_team']} starter={info['starter']}"
                                    )
                                    if ask > BACKTEST_BOOST_MAX_ASK_CENTS:
                                        print(
                                            f"[BACKTEST-BOOST-SKIP] {ticker} "
                                            f"price={ask}c > {BACKTEST_BOOST_MAX_ASK_CENTS}c "
                                            f"break-even ({common})",
                                            flush=True,
                                        )
                                    else:
                                        pred["confidence"] = "HIGH"
                                        pred["reasoning"] = (
                                            "[backtest-boost MEDIUM->HIGH] "
                                            + pred.get("reasoning", "")
                                        )
                                        print(
                                            f"[BACKTEST-BOOST] {ticker} MEDIUM->HIGH "
                                            f"{common}",
                                            flush=True,
                                        )
                            high_ok = (
                                pred["confidence"] == "HIGH"
                                and pred["edge"] >= MIN_EDGE
                            )
                            medium_ok = (
                                pred["confidence"] == "MEDIUM"
                                and pred["edge"] >= MEDIUM_MIN_EDGE
                            )
                            if pred["recommendation"] != "BUY" or not (
                                high_ok or medium_ok
                            ):
                                skipped += 1
                                continue
                            approved += 1
                            payload = {
                                "ticker": ticker,
                                "title": it["title"],
                                "yes_ask": it["yes_ask_cents"],
                                "hours_left": it["hours_left"],
                                "close_time": it["close_time"],
                                "true_probability": pred["true_probability"],
                                "edge": pred["edge"],
                                "confidence": pred["confidence"],
                                "recommendation": "BUY_YES",  # rename for trader's side mapping
                                "reasoning": pred["reasoning"],
                                "side": "yes",
                                "price_for_order_cents": it["yes_ask_cents"],
                                # Forwarded so the trader can detect
                                # correlated bets (same team / same event)
                                # against today's existing trades.
                                "entities": it.get("entities") or [],
                            }
                            kalshi_queue.enqueue("risk", ticker, payload)
                            send_discord(_build_embed(it, pred))
                    _save_seen(seen)
                    print(
                        f"[edge] cycle={cycle} done: approved={approved} "
                        f"skipped={skipped}",
                        flush=True,
                    )
        except Exception as e:
            # Print the full traceback so we can pinpoint where the cycle
            # died — bare str(e) was hiding the real cause (e.g. naive vs
            # aware datetime mismatch from a date-only fetched_at).
            print(
                f"[WARN] edge cycle={cycle} crashed: {type(e).__name__}: {e}\n"
                f"{traceback.format_exc()}",
                flush=True,
            )

        elapsed = time.time() - cycle_start
        sleep_s = max(0.0, CHECK_INTERVAL - elapsed)
        next_wake = (datetime.now(timezone.utc)
                     + timedelta(seconds=sleep_s)).strftime("%H:%M:%S")
        print(
            f"[edge-sleep] cycle={cycle} elapsed={elapsed:.1f}s "
            f"sleeping={sleep_s:.0f}s next_wake={next_wake} UTC",
            flush=True,
        )
        time.sleep(sleep_s)


if __name__ == "__main__":
    run()
