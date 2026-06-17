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
# Sanity ceiling on claimed edge. >18% edge on liquid Kalshi markets is
# almost always Claude misreading the market (wrong bucket, in-progress
# game it can't see, etc.) — clamp the recorded edge and force SKIP.
# Lowered from 25% to 18% on 2026-06-14 after the 7-day calibration
# showed mean_pred=62% vs actual win_rate=29% (ECE 33%) — the model is
# systematically overconfident, and a tighter cap kills the worst
# offenders without affecting genuine edges (real edges on liquid
# Kalshi markets cluster well below 18%).
MAX_EDGE = float(os.getenv("KALSHI_MAX_EDGE", "0.18"))             # 18%
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

# Cohort filter for KXMLBTOTAL / KXMLBTEAMTOTAL. 180-day backtest showed
# real predictive lift on these series ONLY when the relevant starter(s)
# sit in the elite/good cohort (rolling_era_last3 <= 3.50). Outside that
# cohort, win-rate at the breakeven line tracked the trivial baseline —
# i.e. HIGH-confidence verdicts couldn't be trusted as edge. Cap to MEDIUM
# in that regime so we still trade (with a higher edge bar) but don't
# upweight what the data says is noise.
BACKTEST_FILTER_ENABLED = os.getenv("KALSHI_BACKTEST_FILTER_ENABLED", "true").lower() in ("1", "true", "yes")
BACKTEST_FILTER_ERA_CAP = float(os.getenv("KALSHI_BACKTEST_FILTER_ERA_CAP", "3.50"))

# Recalibration demote. When the live win-rate on the last N settled
# trades drops below RECALIB_WR_THRESHOLD, downgrade all HIGH→MEDIUM
# verdicts for RECALIB_DEMOTE_HOURS. Cools down model overconfidence
# during a losing streak: HIGH bets at MIN_EDGE (3%) become MEDIUM bets
# at the higher MEDIUM_MIN_EDGE (8%) floor, so the bar is higher and
# the position-size impact is smaller. State persists across restarts
# in /app/data/recalibration_demote.json so a redeploy mid-cooldown
# doesn't reset the timer.
RECALIB_DEMOTE_ENABLED = os.getenv("KALSHI_RECALIB_DEMOTE_ENABLED", "true").lower() in ("1", "true", "yes")
RECALIB_LOOKBACK_N = int(os.getenv("KALSHI_RECALIB_LOOKBACK_N", "10"))
RECALIB_WR_THRESHOLD = float(os.getenv("KALSHI_RECALIB_WR_THRESHOLD", "0.40"))
RECALIB_DEMOTE_HOURS = float(os.getenv("KALSHI_RECALIB_DEMOTE_HOURS", "24"))
RECALIB_STATE_PATH = Path(os.getenv(
    "KALSHI_RECALIB_STATE", "/app/data/recalibration_demote.json"
))
TRADES_LOG_PATH = Path(os.getenv(
    "KALSHI_TRADES_LOG", "/app/data/trades_log.json"
))

# Tennis (KXATPMATCH / KXWTAMATCH) market-price filter. The 180-day
# backtest (backtest_kalshi_tennis.py) showed the directional model has
# essentially no lift over "always pick the higher-ranked player" — that
# signal is already efficiently priced into rank-favorite Kalshi YES
# contracts. The only positive-EV tennis cohort was HIGH-confidence rank
# favorites available BELOW the break-even Wilson lower bound (~62¢). So
# instead of a directional filter, we run a mispricing filter: BUY only
# when the YES side IS the higher-ranked player AND the rank gap exceeds
# MIN_RANK_GAP AND the YES ask sits at-or-below MAX_ASK_CENTS. Everything
# else gets SKIP'd regardless of Claude's edge call.
TENNIS_FILTER_ENABLED = os.getenv("KALSHI_TENNIS_FILTER_ENABLED", "true").lower() in ("1", "true", "yes")
TENNIS_MAX_ASK_CENTS = int(os.getenv("KALSHI_TENNIS_MAX_ASK_CENTS", "62"))
TENNIS_MIN_RANK_GAP = int(os.getenv("KALSHI_TENNIS_MIN_RANK_GAP", "50"))

# Grass-court specialist filter. Layered on top of the tennis mispricing
# filter — only fires when the market title references a grass
# tournament. Requires YES side (the higher-ranked favorite) to have a
# career grass-vs-overall delta that exceeds the opponent's by at least
# GRASS_MIN_DELTA_DIFF_PP. 380-day backtest:
#   A's grass-delta − B's: ≥+5pp → A wins 73.5% (Wilson_lo 56.9%)
#   A's grass-delta − B's: ≤−5pp → A wins 53.7% (barely above 50%)
# Specialist data lives in data/grass_specialists.json (regen by
# re-running the backtest grass deep-dive — see backtest_kalshi_tennis.py).
# KXBTC strategy rebuild (2026-06-15). Prior history: 0W/5L because
# Claude was claiming 99% on bucket markets (KXBTC-...-B<n> tickers).
# The bucket filter still catches those. For the binary BTC direction
# markets (e.g., KXBTC15M "BTC up in 15 mins"), we now require all of:
#   - Fear & Greed Index in EXTREME zone (< BTC_FG_EXTREME_LOW or
#     > BTC_FG_EXTREME_HIGH) — calm markets are too efficient
#   - YES ask in BTC_PRICE_MIN_CENTS..BTC_PRICE_MAX_CENTS — avoids
#     bucket-like long-tail tickers
#   - Claude-claimed edge ≥ BTC_MIN_EDGE — much stricter than the
#     global MIN_EDGE because BTC is harder to predict than sports
# Direction agreement (F&G fear + positive 24h momentum = bounce; F&G
# greed + negative momentum = reversal) is enforced in the prompt
# methodology, not as a hard filter — Claude evaluates the direction.
BTC_FILTER_ENABLED = os.getenv("KALSHI_BTC_FILTER_ENABLED", "true").lower() in ("1", "true", "yes")
BTC_FG_EXTREME_LOW = int(os.getenv("KALSHI_BTC_FG_EXTREME_LOW", "20"))
BTC_FG_EXTREME_HIGH = int(os.getenv("KALSHI_BTC_FG_EXTREME_HIGH", "80"))
BTC_PRICE_MIN_CENTS = int(os.getenv("KALSHI_BTC_PRICE_MIN_CENTS", "30"))
BTC_PRICE_MAX_CENTS = int(os.getenv("KALSHI_BTC_PRICE_MAX_CENTS", "70"))
BTC_MIN_EDGE = float(os.getenv("KALSHI_BTC_MIN_EDGE", "0.10"))

GRASS_FILTER_ENABLED = os.getenv("KALSHI_GRASS_FILTER_ENABLED", "true").lower() in ("1", "true", "yes")
GRASS_MIN_DELTA_DIFF_PP = float(os.getenv("KALSHI_GRASS_MIN_DELTA_DIFF_PP", "5.0"))
GRASS_SPECIALISTS_PATH = Path(os.getenv(
    # Bundled resource under resources/ — NOT under /app/data/ which
    # is a Railway persistent volume that masks image files at runtime.
    "KALSHI_GRASS_SPECIALISTS_JSON", "resources/grass_specialists.json"
))
GRASS_TOURNAMENTS = (
    "wimbledon",
    # Halle: official name is "Terra Wortmann Open"; older title is "Gerry
    # Weber Open". Adding both because Kalshi titles may use either.
    "halle", "terra wortmann", "gerry weber",
    # Queens: "Cinch Championships" and historically "HSBC Championships";
    # Kalshi commonly says "Queen's" or "Queens".
    "boss open", "queen's club", "queens club", "queen's",
    "cinch championships", "hsbc championships",
    # s-Hertogenbosch: official "Libéma Open".
    "eastbourne", "s-hertogenbosch", "hertogenbosch", "libema", "libéma",
    "stuttgart open", "mallorca", "newport",
)

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
# T-2h seen-cache eviction for KXMLBTOTAL. Once Claude SKIPs a market,
# its ticker is permanently in seen until a redeploy clears state. For
# KXMLBTOTAL specifically that's a known throughput problem (life-of-
# log 2026-06-15: 4 trades vs 622 unique tickers evaluated = 0.6%
# conversion), because Kalshi lists the contracts 24+h before close
# and conditions change between morning-evaluation and game-time:
# starting lineups confirm, weather updates, late scratches. Re-
# evaluating in the final 2h gives Claude a second look with the
# market state that actually decides the bet. Other series stay
# sticky in seen (no evidence the same pattern helps them).
SEEN_EVICT_T_MINUS_SECS = int(os.getenv("KALSHI_SEEN_EVICT_T_MINUS_SECS", "7200"))
# T-30min-to-start eviction for KXMLBTOTAL. The close-time trigger above
# fires at game_end - 2h, which for ~3h games is ~game_start + 1h —
# i.e. AFTER first pitch on West Coast night games (e.g. BAL@SEA
# 2026-06-15: bot saw strikes at 20:14 UTC, game start 01:40 UTC, but
# close-trigger eviction wouldn't fire until ~02:30 UTC, 50 min past
# first pitch). Adding a start-time trigger gives one re-evaluation
# window with confirmed lineups while the pre-game model is still valid.
SEEN_EVICT_T_MINUS_START_SECS = int(
    os.getenv("KALSHI_SEEN_EVICT_T_MINUS_START_SECS", "1800")
)
# Seen-cache TTL. Entries older than this many seconds are filtered
# out at load AND re-filtered at the start of every cycle, so a market
# Claude SKIPped yesterday gets a fresh look today. Default 24h. The
# permanent-seen-cache was throttling KXMLBTOTAL throughput to ~0.6%
# conversion (4 trades / 622 tickers) because the same ticker would
# get SKIPped at listing time and never be reconsidered.
SEEN_EXPIRY_SECS = int(os.getenv("KALSHI_SEEN_EXPIRY_SECS", "86400"))
# Per-series tighter line-move thresholds. KXMLBTOTAL gets 3¢ (vs the
# global 5¢) because total-runs markets reprice sharply on lineup
# confirmations / weather updates that we can't see, and the few-cent
# moves carry more information per cent than other series. Sharp-money
# triggers > 3¢ on KXMLBTOTAL means someone has private data we don't.
SERIES_LINE_MOVE_CENTS: dict[str, int] = {
    "KXMLBTOTAL": int(os.getenv("KALSHI_KXMLBTOTAL_LINE_MOVE_CENTS", "3")),
}


def _line_move_threshold_for(ticker: str) -> int:
    """Return the line-move skip threshold (cents) for a ticker.
    Per-series override if defined, else global default."""
    for prefix, thresh in SERIES_LINE_MOVE_CENTS.items():
        if ticker.startswith(prefix):
            return thresh
    return MAX_LINE_MOVE_CENTS
# Ignore stored prices older than this — a 3-day-old price isn't a
# "movement signal," it's just a different market state.
PRICE_HISTORY_MAX_AGE_SECS = int(os.getenv("KALSHI_PRICE_HISTORY_MAX_AGE", "86400"))

# Sports series we actually evaluate. Pulling /markets per series with a
# close-time window cuts the fetch from 5000 mostly-irrelevant rows to
# only in-window contracts the stats cache can actually score.
SERIES_TICKERS = [
    s.strip() for s in os.getenv(
        "KALSHI_EDGE_SERIES",
        # Tennis (KXATPMATCH / KXWTAMATCH) re-enabled for the grass swing
        # and Wimbledon — gated by the mispricing-only TENNIS_FILTER below
        # (HIGH conf + gap > 50 + ask ≤ 62¢), so series re-add doesn't
        # broaden trading on its own. KXBTC re-enabled 2026-06-15 with
        # the new F&G + price-band + 10% edge gate (see _btc_filter_passes)
        # — bucket tickers continue to be filtered by _is_btc_bucket_ticker
        # before this filter is even consulted.
        "KXMLBTOTAL,KXMLBSPREAD,KXMLBTEAMTOTAL,KXATPMATCH,KXWTAMATCH,KXBTC",
    ).split(",") if s.strip()
]

# ─── Seen-cache: skip markets we've already evaluated ───────────────────

def _load_seen() -> dict[str, float]:
    """Load the seen-cache as {ticker: unix_timestamp_added}. Drops any
    entry older than SEEN_EXPIRY_SECS at load time.

    Backward-compat: the file used to store {"tickers": [list]}; if we
    detect that shape we migrate every entry with `now` as the timestamp
    so they expire 24h from now (graceful — no full re-evaluation
    avalanche, just a 24h drain). Schema-going-forward is
    {"tickers": {ticker: ts}}."""
    if not SEEN_CACHE_PATH.exists():
        return {}
    try:
        with SEEN_CACHE_PATH.open() as f:
            data = json.load(f)
    except Exception as e:
        print(f"[WARN] edge_seen.json unreadable: {e}", flush=True)
        return {}
    raw = data.get("tickers", {})
    now = time.time()
    if isinstance(raw, list):
        print(
            f"[edge] migrating edge_seen.json from list ({len(raw)} entries) "
            f"→ dict-of-timestamps; all entries given now as ts (24h drain)",
            flush=True,
        )
        return {t: now for t in raw}
    if not isinstance(raw, dict):
        return {}
    cutoff = now - SEEN_EXPIRY_SECS
    return {t: float(ts) for t, ts in raw.items() if float(ts) >= cutoff}


# _expire_seen() removed 2026-06-15. Previously it filtered the in-
# memory seen dict at cycle start, but this meant out-of-band evictions
# (operator running an SSH eviction script while the agent was alive)
# survived for less than one cycle — the agent's next _save_seen()
# would overwrite the disk with its still-bloated in-memory copy. The
# replacement pattern, implemented in run(), reloads the seen-cache
# fresh from disk at the start of every cycle. _load_seen() already
# handles TTL filtering, so the per-cycle reload covers both expiry
# AND out-of-band changes with one round-trip.


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


def _save_seen(seen: dict[str, float]) -> None:
    """Persist as {"tickers": {ticker: ts}}. Cap at 20k entries; drop
    the OLDEST by timestamp (now that we track them, the trim is no
    longer arbitrary)."""
    if len(seen) > 20_000:
        keep = sorted(seen.items(), key=lambda kv: -kv[1])[:10_000]
        seen = dict(keep)
    SEEN_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = SEEN_CACHE_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump({"tickers": seen}, f)
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
        # KXMLBTOTAL re-eval eviction. Two triggers, either can fire:
        #   1. T-2h-to-close (SEEN_EVICT_T_MINUS_SECS) — works for
        #      day/afternoon games where close-2h is still pre-pitch.
        #   2. T-30min-to-start (SEEN_EVICT_T_MINUS_START_SECS) — covers
        #      West Coast night games where T-2h-to-close lands after
        #      first pitch (see comment at SEEN_EVICT_T_MINUS_START_SECS).
        # Other series fall through unchanged.
        if ticker in seen and ticker.startswith("KXMLBTOTAL"):
            close_dt = _parse_iso(
                m.get("expected_expiration_time")
                or m.get("occurrence_datetime")
                or m.get("close_time", "")
            )
            start_dt = _parse_iso(m.get("occurrence_datetime", ""))
            evict_reason = None
            evict_secs = None
            if close_dt is not None:
                secs_to_close = (close_dt - now).total_seconds()
                if 0 < secs_to_close < SEEN_EVICT_T_MINUS_SECS:
                    evict_reason = "T-2h-close KXMLBTOTAL"
                    evict_secs = int(secs_to_close)
            if evict_reason is None and start_dt is not None:
                secs_to_start = (start_dt - now).total_seconds()
                if 0 < secs_to_start < SEEN_EVICT_T_MINUS_START_SECS:
                    evict_reason = "T-30min-start KXMLBTOTAL"
                    evict_secs = int(secs_to_start)
            if evict_reason is not None:
                seen.pop(ticker, None)
                print(
                    f"[SEEN-EVICT] {ticker} reason=\"{evict_reason}\" "
                    f"secs={evict_secs}",
                    flush=True,
                )
        if ticker in seen:
            drops["seen"] += 1
            continue
        if _is_btc_bucket_ticker(ticker):
            drops["btc_bucket"] += 1
            seen[ticker] = time.time()
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
        # NO ask is the price we pay for a BUY_NO. Fall back to the
        # 1-yes_ask proxy when Kalshi doesn't return no_ask_dollars (rare;
        # mirror field on the same response). The proxy ignores spread —
        # acceptable for the narrow KXMLBTOTAL -9/-10 BUY_NO rollout where
        # we only place at MEDIUM with a comfortable edge cushion.
        na = float(m.get("no_ask_dollars", 0) or 0)
        no_cents = int(round(na * 100)) if na > 0 else max(1, 100 - yes_cents)

        # Line-movement guard. Use the persisted price for this ticker;
        # if the YES price has shifted more than the per-series threshold
        # since we last looked, someone with private information has been
        # trading — skip this cycle and update history so the next cycle
        # can re-check against the new baseline. KXMLBTOTAL uses a tighter
        # 3¢ threshold via _line_move_threshold_for(); other series fall
        # back to MAX_LINE_MOVE_CENTS (default 5¢). New tickers (no prior
        # entry) pass through.
        threshold = _line_move_threshold_for(ticker)
        prior = price_history.get(ticker)
        if prior and (now_ts - float(prior.get("ts", 0))) <= PRICE_HISTORY_MAX_AGE_SECS:
            old_cents = int(float(prior.get("price_cents", yes_cents)))
            move = abs(yes_cents - old_cents)
            if move > threshold:
                drops["line_moved"] += 1
                print(
                    f"[LINE-MOVE-SKIP] {ticker} {old_cents}¢→{yes_cents}¢ "
                    f"(Δ{move}¢ > {threshold}¢ threshold)",
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
            "no_ask_cents": no_cents,
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
    # Merge timing_drops under a `timing_` prefix so the funnel line has a
    # single flat drops dict without key collisions.
    combined_drops = dict(drops)
    for k, v in timing_drops.items():
        combined_drops[f"timing_{k}"] = v
    return kept, combined_drops


# ─── Claude call ────────────────────────────────────────────────────────

_METHODOLOGY = f"""You are a Kalshi prediction-market edge finder. For each market in the user message, estimate the TRUE probability of YES using the STATS CONTEXT block in this system prompt.

The STATS CONTEXT block has two halves:
- SPORTS STATS — team scoring, pitcher ERA/WHIP, standings, leaders, and an mlb.upcoming_games list of matchups tagged with `game_date` (YYYY-MM-DD, US Eastern) and probable pitchers. Each probable pitcher entry also carries `rolling_era_last3` (IP-weighted ERA over his last 3 starts) and `vs_opponent` — head-to-head stats versus today's opposing team: `{{starts, era_vs, whip_vs, avg_runs_last3_vs}}`. `vs_opponent` is null when the starter has no prior appearances against this opponent. `mlb.bullpens` is a `{{team_abbr: {{bullpen_era_15d, saves_15d, save_opportunities_15d, save_conversion_15d, blown_saves_15d}}}}` map covering only the teams in upcoming_games — use it for KXMLBGAME, KXMLBTOTAL, and KXMLBTEAMTOTAL markets. For tennis, `tennis.atp_rankings` / `tennis.wta_rankings` carry the current top-ranked players, and `tennis.atp_recent` / `tennis.wta_recent` carry the last ~10 days of completed match results (winner, loser, score, event). Use these for MLB / NHL / NBA / ATP / WTA markets.
- ECONOMIC DATA — current national gas price, latest CPI, Fed funds target + next FOMC meeting expectations, BTC spot. Use these for KXAAAGASD / KXCPI / KXFED / KXBTC markets, combined with your own knowledge of macro trends, central-bank reaction functions, and recent price action.

EDGE = true_probability - market_implied_probability  (market price in cents / 100)

RECOMMENDATION RULES
- BUY only if you have specific, directly relevant data AND one of these tiers is met:
    - HIGH confidence with edge >= +{MIN_EDGE:.2f}
    - MEDIUM confidence with edge >= +{MEDIUM_MIN_EDGE:.2f}
  The MEDIUM tier requires a larger edge because MEDIUM means you are admitting uncertainty — the extra margin compensates for that uncertainty. Required data:
    - for sports: the named team or player appears in SPORTS STATS
    - for economic: the current value of the macro variable is in ECONOMIC DATA and resolution is close enough that the variable is unlikely to swing materially
- SKIP in every other case. BUY_NO is DISABLED in this build EXCEPT for a narrow KXMLBTOTAL cohort defined in the KXMLBTOTAL BUY_NO ELIGIBILITY section below — for every other ticker, emit BUY (= BUY_YES) or SKIP.
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
RECOMMENDATION: <BUY|BUY_NO|SKIP>   (BUY = BUY_YES; BUY_NO only on the narrow KXMLBTOTAL cohort below)
REASONING: <one sentence pointing at the specific stat that drove the call>
---

EDGE SANITY CAP (GLOBAL OVERRIDE — APPLIES TO EVERY MARKET)
- Real edge on liquid Kalshi markets almost never exceeds 18%. If you computed an edge of +0.18 or higher, you are almost certainly misreading the market — wrong bucket, in-progress game whose live state you cannot see, resolution criteria you misunderstood, or a stale stat masquerading as current.
- When this happens, set RECOMMENDATION: SKIP and CONFIDENCE: LOW. Do not BUY at +18% claimed edge regardless of how obvious the reasoning feels.

KXBTC RANGE-BUCKET MARKETS (READ BEFORE EVALUATING ANY KXBTC TICKER)
- A KXBTC ticker shaped `KXBTC-<dateHour>-B<num>` or `-T<num>` (e.g. KXBTC-26JUN0501-B72750, KXBTC-26JUN0617-T57200) with title "Bitcoin price range on <date>?" is ONE bucket inside a contiguous set of narrow price buckets — NOT a "below <num>" or "above <num>" threshold.
- The `B` / `T` prefix is a bucket identifier, not "below" / "above". Each bucket's true probability of resolving YES is small (often 1–5%) regardless of where the bucket number sits relative to BTC spot.
- A market trading at 1–5¢ on a bucket far from spot is correctly priced, not mispriced. Buying YES on such a bucket because "BTC spot is far from <num>" is the systematic error pattern we are explicitly blocking — past trades of this shape lost 100% of the time.
- For any KXBTC ticker matching this shape, RECOMMENDATION must be SKIP regardless of computed edge. Set CONFIDENCE: LOW and REASONING: "KXBTC range-bucket ticker — narrow bucket, not a cumulative threshold; skipping per rule."
- Binary BTC markets in other series (e.g. KXBTC15M "BTC price up in next 15 mins?") are not affected by this rule.

KXBTC BINARY-DIRECTION STRATEGY (2026-06-15 rebuild)
- Binary KXBTC tickers (KXBTC15M and other "BTC up/down in N mins/hours" forms) are evaluated via a strict three-gate filter implemented in code: Fear & Greed in EXTREME zone (< 20 or > 80), YES ask in 30-70¢ band, edge ≥ +10%. Predictions that don't clear all three get SKIP'd post-Claude.
- Your role: estimate the directional probability using `economic.crypto_fear_greed_value` (0-100), `economic.crypto_fear_greed_classification`, and `economic.btc_24h_momentum_pct` (percent change of last vs. 24h-ago Coinbase BTC spot). Apply the contrarian-extreme rule:
    - F&G < 20 (Extreme Fear) AND btc_24h_momentum_pct > 0  →  bounce signal; favor YES on "BTC up" markets.
    - F&G < 20 (Extreme Fear) AND btc_24h_momentum_pct < -2 → momentum-down + fear; ambiguous; SKIP unless edge is obvious.
    - F&G > 80 (Extreme Greed) AND btc_24h_momentum_pct < 0  →  reversal signal; favor YES on "BTC down" markets.
    - F&G > 80 (Extreme Greed) AND btc_24h_momentum_pct > +2 → continued melt-up; ambiguous; SKIP.
    - F&G in 20-80 (calm zone): SKIP regardless of momentum. The code filter will SKIP this too — your verdict here is informational.
- Edge ceiling: do NOT claim >25% edge on KXBTC binary markets. BTC short-horizon direction is genuinely uncertain — even strong contrarian setups historically clear ~58-62%. A claimed edge of +30% on a binary BTC market means you are misreading the time window or the market's resolution criteria.

GAS / THRESHOLD MARKETS (KXAAAGASD AND SIMILAR DAILY-AVERAGE TICKERS)
- These markets resolve on the AAA daily national-average gas price for a specific date. The threshold is in the ticker tail (e.g. KXAAAGASD-26JUN04-4.260 → threshold $4.260).
- If the current value of the underlying is within 0.5% of the threshold, treat the market as a coin flip. RECOMMENDATION: SKIP regardless of computed edge — daily settlement variance and rounding dominate any apparent edge from "we are already above/below by a tenth of a cent."
- Only BUY when the current value is at least 0.5% on the favored side of the threshold AND no plausible 1-day move closes that gap.

MLB GAME WINNER (KXMLBGAME) EDGE CEILING
- Real edge on KXMLBGAME (game winner) markets rarely exceeds 15% even with an elite starter. Bullpen depth, lineup matchups, weather, and umpire variance all compress edge fast — Cy Young pitchers still go 20-10, not 30-0.
- If you compute edge above +0.15 on a KXMLBGAME ticker, set RECOMMENDATION: SKIP regardless of computed edge. This ceiling does NOT apply to KXMLBSPREAD or KXMLBTOTAL — those have different variance profiles.

MLB BULLPEN (KXMLBGAME AND KXMLBTOTAL)
- For KXMLBGAME, when the favored team's `bullpens[abbr].bullpen_era_15d` is >= 5.00 AND its `save_conversion_15d` is <= 0.65 (with `save_opportunities_15d` >= 4), do NOT BUY YES on that team regardless of starter edge — a bullpen that loses ~1 in 3 leads erases a starter's 6-inning lead too often to ride. Drop one confidence tier (HIGH→MEDIUM, MEDIUM→SKIP) in that case.
- For KXMLBGAME, when the favored team's `bullpen_era_15d <= 3.00` AND `save_conversion_15d >= 0.85` (with `save_opportunities_15d >= 4`), treat the bullpen as a reinforcing-not-driving positive signal — do NOT raise confidence or edge above what the starter + standings already justify.
- For KXMLBTOTAL, when BOTH teams' `bullpen_era_15d` >= 5.00, lean OVER on tickets that look close; when BOTH are <= 3.00, lean UNDER. Do NOT use a single team's bullpen to call a total — totals depend on both pens equally.
- If either team's `bullpens` entry is missing or all the relevant fields are null, treat as no signal — do not infer.

MLB PITCHER VS OPPONENT (HEAD-TO-HEAD)
- When `vs_opponent` is present with `starts >= 2` and `avg_runs_last3_vs <= 2.0` AND `era_vs <= 3.50`, treat it as a meaningful H2H favorite signal for the pitcher's team — but a soft one. Cap any H2H-driven upgrade at one confidence tier (LOW→MEDIUM, MEDIUM→HIGH) and do not let it raise computed edge above the +0.15 KXMLBGAME ceiling above.
- When `vs_opponent.avg_runs_last3_vs >= 5.0` OR `era_vs >= 6.00` across `starts >= 2`, the H2H signal points AGAINST the pitcher's team. Do not BUY YES on the pitcher's team in that case unless season ERA + rolling_era_last3 both clearly dominate the opponent's bats.
- For KXMLBTOTAL markets, weight `avg_runs_last3_vs` and `era_vs` from BOTH starters jointly — two pitchers with low H2H runs allowed argues UNDER, two with high argues OVER.
- `vs_opponent: null` means no prior matchup — do NOT treat absence as a positive or negative signal.

MLB TEAM TOTAL (KXMLBTEAMTOTAL) — BATTING TEAM RUN PROJECTION
- KXMLBTEAMTOTAL resolves on ONE team's total runs hitting a threshold (e.g. KXMLBTEAMTOTAL-26JUN141215MIAPIT-PIT5 → "Will PIT score 5+?"). The ticker tail is `<batting_team_abbr><threshold>`. Score only the BATTING team's run-scoring ability — opponent runs do not resolve this market.
- Anchor: `team_scoring[batting_abbr].rs_per_game` against the opposing team's `bullpens[opposing_abbr].bullpen_era_15d`. High rs_per_game versus a high opposing bullpen ERA argues OVER; low rs_per_game versus a low opposing bullpen ERA argues UNDER.
- Opposing pitcher adjustment: factor in the OPPOSING probable starter's season ERA and WHIP. Shade DOWN when ERA < 3.50 AND WHIP < 1.20; shade UP when ERA > 4.50 OR WHIP > 1.40.
- Home-team boost: add +0.3 expected runs when the batting team is the home team (the trailing abbr in the matchup code).
- ELITE STARTER FLAG (BEARISH FOR TEAM TOTAL): if the opposing probable starter has `rolling_era_last3 < 2.50`, treat OVER on the batting team as bearish — an in-form elite arm compresses run scoring sharply. Drop one confidence tier (HIGH→MEDIUM, MEDIUM→SKIP) on any BUY YES tied to an OVER threshold against such a starter.
- THRESHOLD PREFERENCE (BACKTEST-VALIDATED 2026-06-14): the trailing integer N in the ticker tail (`-<TEAM>N`) is the contract's "N+ runs" threshold; a YES bet wins if the team scores N or more. backtest_kalshi.py over both 30-day and 180-day windows shows the only consistent positive lift on KXMLBTEAMTOTAL is at threshold N=4 (line=3.5 OVER) when predicted runs ≥ 4.25. At N=5+ (line ≥ 4.5), the elite-opposing-starter cohort lost its 180-day 6.5pp edge in the most recent 30-day window — no statistical separation from naive baseline. Rules:
    - `-<TEAM>4` markets (N=4): preferred. Allow BUY YES at normal tiered gates when predicted runs ≥ 4.25.
    - `-<TEAM>5` / `-<TEAM>6` / `-<TEAM>7` markets (N≥5): cap CONFIDENCE at MEDIUM regardless of computed edge. Require predicted runs to exceed N by at least 1.25 before BUY (so for N=5, predicted ≥ 6.25; for N=6, predicted ≥ 7.25). If that bar isn't cleared, SKIP — historical lift in the recent window is statistically indistinguishable from zero.
- Edge threshold matches KXMLBTOTAL — only the global +0.18 sanity cap applies; there is no series-specific ceiling.
- If `team_scoring[batting_abbr]` is missing rs_per_game, or the opposing probable starter is missing from the matched upcoming_games entry, SKIP.

TENNIS MATCH WINNER (KXATPMATCH / KXWTAMATCH) RULES
- Both players named in the title must appear in `tennis.atp_rankings` (or `tennis.wta_rankings`) — match against the surname in `player`. If only one is ranked, set RECOMMENDATION: SKIP.
- A 30+ rank gap between two top-100 players is roughly a 65/35 favorite. A 50+ gap is roughly 75/25. Use these as anchors; do not claim >85% favorite probability for any match without lopsided recent form to back it.
- Recent form: weight the last ~10 days of `*_recent` matches for both players. Two recent wins by the underdog over comparably-ranked opponents should compress, not extend, the favorite edge.
- If either player has no recent matches in the cache, set RECOMMENDATION: SKIP — without recent form we cannot anchor the call.
- SLAM / WIMBLEDON COHORT (BACKTEST-VALIDATED 2026-06-14): at Wimbledon specifically — and to a lesser extent the other three slams — higher-ranked favorites win at a meaningfully higher rate than they do at tour-level events. 380-day backtest: tour-grass naive baseline 65.0%, Wimbledon naive baseline 71.5% (+6.5pp). Strongest cell is gap=35-75 at Wimbledon where naive hits 77.5% (Wilson_lo 66.5%). Two adjustments when the market title or event context indicates Wimbledon (or any slam — Australian Open, French Open, US Open):
    - You may raise favorite probability up to 80% (vs the normal 75% ceiling) on a 50+ rank gap.
    - Treat HIGH confidence as appropriate at gap≥35 (the normal threshold is gap≥50) — slam BO5 reduces variance enough that mid-gap favorites become near-locks.
  Do NOT apply this adjustment at non-slam tour events (Halle, Queen's, Cincinnati, etc.) — only the four majors.

MLB DATE MATCHING (READ BEFORE EVALUATING ANY MLB MARKET)
- MLB market tickers encode the game date as YYMMMDD followed by HHMM and the away+home team abbreviations, e.g. KXMLBSPREAD-26JUN041335CLENYY → 2026-06-04 13:35 CLE@NYY.
- For ANY MLB market, locate the entry in mlb.upcoming_games whose `game_date` matches the ticker date AND whose away/home abbreviations match. Use the pitchers and stats from THAT entry.
- If no upcoming_games entry matches the ticker date, SKIP. Do NOT fall back to team_scoring alone or to a different day's pitching matchup — yesterday's starter is rarely tomorrow's starter, and using the wrong pitcher silently breaks the edge calculation.

MLB FACTS BLOCK (PER-MARKET INJECTION)
- For MLB markets, each user-message MARKET block may include a `FACTS:` section with the matched upcoming_games entry, both teams' rs_per_game, both probable pitchers (ERA, WHIP, rolling_era_last3, vs_opponent), both bullpens, and (when parseable) CONTRACT_TEAM. Three additional KXMLBTOTAL signals when available: PARK_FACTOR, HOME_PLATE_UMP, WEATHER.
- When FACTS is present, use it as the authoritative data source for that ticker — you do NOT need to scan STATS CONTEXT for the same matchup, and you should NOT emit "matching upcoming_games entry not found" or "team_scoring missing" when the FACTS block has the values.
- When FACTS is absent on an MLB ticker, the cache had no matching game — SKIP per the date-matching rule.

KXMLBTOTAL PARK / UMPIRE / WEATHER SIGNALS (NEW 2026-06-15)
- PARK_FACTOR is the home stadium's run-scoring index normalized to ~1.0 league average. >1.03 = high-run park, <0.97 = pitcher's park. Multiply your baseline expected total by the park factor when projecting runs for KXMLBTOTAL / KXMLBTEAMTOTAL: a baseline 8.5-run projection at Coors (1.32) becomes ~11.2, at Petco (0.85) becomes ~7.2. Park factor effect compounds with starter quality — an elite arm at Coors still allows more runs than an average arm at Petco.
- HOME_PLATE_UMP is the assigned home plate umpire's name when known (populates a few hours pre-game; absent at morning evaluation time). When known and the name matches a famously tight-strike-zone ump (e.g. Hunter Wendelstedt, Doug Eddings — small zones inflate K rates and walk rates simultaneously) or a famously generous zone (e.g. Joe West historically — though retired, illustrative), shade your expected total down 0.2-0.4 runs for tight zones and up 0.2-0.4 runs for generous zones. Do NOT apply this adjustment when the umpire field is absent or empty — never invent an umpire bias.
- WEATHER carries temp_f / wind / cond at the ballpark. Apply these rules:
    - Temp < 50°F → cold air kills carry; shade expected total DOWN 0.3-0.5 runs.
    - Temp > 80°F → warm air boosts carry; shade UP 0.2-0.4 runs.
    - Wind > 15mph blowing OUT (S/SW/W at most parks, but check by ballpark) → shade UP 0.5-0.8 runs. Wrigley Field is THE classic wind-aided park.
    - Wind > 15mph blowing IN → shade DOWN 0.4-0.6 runs.
    - Light rain or fog in cond → modest DOWN shade (0.2 runs) plus heightened postponement risk for very-near-close markets; if game_time is < 60min away and cond mentions rain, SKIP rather than guess at a delay.
- These three signals are SUPPLEMENTARY — they tune the prediction but never override the rolling-ERA + bullpen + H2H foundation. A 2.50 rolling-ERA elite arm at Coors with strong tailwind is still your best UNDER bet at the 11.5 line, not the OVER, because the starter quality dominates the park effect over 6 innings.

KXMLBTOTAL UNDER-LEAN ROUTING (NEW 2026-06-17)
- This build is YES-only (see RECOMMENDATION RULES above — BUY_NO is disabled). When your projected total comes in BELOW the line family of available tickers, the previously observed failure mode was to claim a "value tail" BUY YES on a high-line ticker (e.g. projected 8.7 runs, ticker -11, claim our_prob=0.42 vs ask=0.28 → BUY YES). 180-day directional backtest shows the predictor adds NO positive lift on high-line OVER picks (line 10.5 and 11.5 OVER lift is at or below zero), so these cheap-tail YES bets lose at a measurable rate.
- RULE 1 (per-ticker projected-vs-threshold floor): for any KXMLBTOTAL ticker `-N`, if your projected_total < (N - 0.5), set RECOMMENDATION: SKIP regardless of computed edge. The "value tail on a longshot" reasoning is the contradiction this rule blocks.
- RULE 2 (UNDER-favoring overlay): if your reasoning concludes the matchup is UNDER-favoring — any of: elite-duel (both starters rolling_era_last3 < 3.00), both bullpens bullpen_era_15d ≤ 3.00, both starters with vs_opponent.avg_runs_last3_vs ≤ 3.0 across starts ≥ 2, or projected_total at or below the lowest available threshold in the family — do NOT BUY YES on any -11 or higher ticker. Either BUY YES on the LOWEST available line N where projected_total ≥ N + 0.5, or SKIP every ticker in the family.
- Worked example: projected 8.7 runs, tickers -8, -9, -10, -11, -12 available. -8 passes (8.7 ≥ 8.5). -9, -10, -11, -12 all fail Rule 1 and Rule 2. → Evaluate BUY YES on -8 only (subject to the usual edge-tier gate); SKIP -9 through -12.

KXMLBTOTAL BUY_NO ELIGIBILITY (NEW 2026-06-17, narrow staged rollout)
- BUY_NO is allowed ONLY on KXMLBTOTAL tickers ending in `-9` or `-10` (the T=8.5 and T=9.5 lines). On every other KXMLBTOTAL ticker — and on every other series — the build is YES-only; emit BUY or SKIP, never BUY_NO. Out-of-cohort BUY_NO recommendations are dropped post-Claude by the code.
- CONFIDENCE must be MEDIUM for BUY_NO. Do NOT emit BUY_NO with CONFIDENCE: HIGH for the first 30 days of this rollout — emit BUY_NO with MEDIUM, or SKIP. HIGH BUY_NO is dropped post-Claude.
- PROJECTION CONDITION — emit BUY_NO on `-9` ONLY when your projected_total < 8.5 runs; on `-10` ONLY when projected_total < 9.5 runs. This is the mirror of Rule 1 above: your projection must sit cleanly on the NO side of the line, not a "value tail" claim.
- PITCHING CONDITION — both probable starters' season ERA must be < 3.50. Rationale: decent pitching → fewer runs → UNDER is the modal outcome and BUY_NO is the structurally correct read. Bad pitching (high ERA) → more runs → BUY_NO would be a longshot bet, which is the failure mode we are blocking. Only emit BUY_NO when both starters are credibly suppressing the run total.
- EDGE CONVENTION for BUY_NO — compute EDGE as (1 - TRUE_PROBABILITY) - no_market_implied, where no_market_implied = MARKET NO PRICE / 100. TRUE_PROBABILITY remains the YES probability (the same number you would emit for BUY_YES on this ticker). Must clear MEDIUM_MIN_EDGE.
- Backtest grounding: 180d directional backtest on KXMLBTOTAL shows UNDER bets at T=8.5 have +9pp Wilson-stable lift over the always-UNDER trivial baseline, replicating across both 60d and 180d windows; T=9.5 is borderline positive (+3pp). T≥10.5 UNDER bets just rediscover the trivial baseline (no real predictive signal), which is why this cohort is narrow.

CRITICAL RULES
- Echo TICKER exactly so we can match outputs to inputs.
- Always emit --- after every block including the last.
- No prose before, between, or after the blocks. No markdown fences.
- One block per input market, no skipping, no extras."""


def _build_system_prompt(stats: dict[str, Any]) -> str:
    # Keep the stats JSON compact in the prompt — every extra token is
    # paid for on the first call of the day (cache write). Drop indentation.
    # Trim tennis.*_recent before serializing. 2026-06-14: the raw cache
    # held 2,500 ATP + 3,525 WTA recent-match entries (kalshi_stats fetches
    # 10 days of scoreboard). At ~100 bytes per JSON entry, that's ~600KB
    # of context — enough to push the system prompt past Haiku 4.5's
    # 200k token limit (live failure: 202,676 tokens > 200,000). Last 500
    # per tour gives Claude plenty of form context without overflowing.
    trimmed = dict(stats)
    tennis = dict(trimmed.get("tennis") or {})
    for key in ("atp_recent", "wta_recent"):
        rows = tennis.get(key) or []
        if len(rows) > 500:
            tennis[key] = rows[-500:]
    if tennis:
        trimmed["tennis"] = tennis
    stats_json = json.dumps(trimmed, separators=(",", ":"))
    return (
        _METHODOLOGY
        + "\n\nSTATS CONTEXT (refresh date in fetched_at, includes both sports and economic blocks):\n"
        + stats_json
    )


_MLB_SERIES_PREFIXES = (
    "KXMLBGAME", "KXMLBTOTAL", "KXMLBSPREAD", "KXMLBTEAMTOTAL",
)


def _fmt_pitcher(p: dict[str, Any] | None) -> str:
    if not p:
        return "n/a"
    parts = [p.get("player") or "?"]
    for key, label in (
        ("era", "ERA"), ("whip", "WHIP"),
        ("rolling_era_last3", "rolling_era_last3"),
    ):
        v = p.get(key)
        if v is not None:
            parts.append(f"{label}={v}")
    vs = p.get("vs_opponent")
    if vs:
        parts.append(
            f"vs_opp(starts={vs.get('starts')},era_vs={vs.get('era_vs')},"
            f"whip_vs={vs.get('whip_vs')},avg_runs_last3_vs={vs.get('avg_runs_last3_vs')})"
        )
    return ", ".join(parts)


def _fmt_bullpen(b: dict[str, Any] | None) -> str:
    if not b:
        return "n/a"
    parts = []
    for k in ("bullpen_era_15d", "save_conversion_15d",
              "save_opportunities_15d", "blown_saves_15d"):
        v = b.get(k)
        if v is not None:
            parts.append(f"{k}={v}")
    return ", ".join(parts) or "n/a"


def _mlb_facts_for_ticker(ticker: str, stats: dict[str, Any]) -> str | None:
    """Inline the relevant upcoming_games entry + both teams' team_scoring
    and bullpens for an MLB ticker. Returns None for non-MLB tickers or
    when no matching game is in the cache. Removes the attention-drift
    failure mode where Claude couldn't find data already sitting in the
    big STATS CONTEXT blob."""
    if not any(ticker.startswith(p) for p in _MLB_SERIES_PREFIXES):
        return None
    parts = ticker.split("-")
    if len(parts) < 3:
        return None
    body = parts[1]
    if len(body) < 11:
        return None
    yy, mon, dd = body[:2], body[2:5], body[5:7]
    mm = _MLB_TICKER_MONTH.get(mon.upper())
    if not mm:
        return None
    game_date = f"20{yy}-{mm}-{dd}"
    matchup = body[11:]  # AWAY+HOME concatenated
    mlb = stats.get("mlb", {}) or {}
    game = None
    for g in mlb.get("upcoming_games", []) or []:
        if g.get("game_date") != game_date:
            continue
        if (g.get("away", "") + g.get("home", "")) == matchup:
            game = g
            break
    if not game:
        return None
    away = game.get("away", "?")
    home = game.get("home", "?")
    ts_map = mlb.get("team_scoring", {}) or {}
    bp_map = mlb.get("bullpens", {}) or {}
    away_rs = (ts_map.get(away) or {}).get("rs_per_game")
    home_rs = (ts_map.get(home) or {}).get("rs_per_game")
    contract_seg = parts[2]
    contract_team = None
    for abbr in (away, home):
        if abbr and contract_seg.startswith(abbr):
            if contract_team is None or len(abbr) > len(contract_team):
                contract_team = abbr
    lines = [
        f"GAME: {away} @ {home}, {game_date} {game.get('start_time_utc', '?')}Z",
        f"BATTING RS/G: {away}={away_rs} (away), {home}={home_rs} (home, +0.3 boost when batting at home)",
        f"AWAY PITCHER ({away}): {_fmt_pitcher(game.get('away_pitcher'))}",
        f"HOME PITCHER ({home}): {_fmt_pitcher(game.get('home_pitcher'))}",
        f"AWAY BULLPEN ({away}): {_fmt_bullpen(bp_map.get(away))}",
        f"HOME BULLPEN ({home}): {_fmt_bullpen(bp_map.get(home))}",
    ]
    # KXMLBTOTAL context signals — park / umpire / weather. Park factor
    # is always present (default 1.00 if home team is missing from the
    # bundled table); ump populates a few hours before game-time;
    # weather is None on fetch failure (caller may have hit a wttr.in
    # blip — Claude just omits the signal in that case).
    pf = game.get("park_factor")
    if pf is not None:
        lines.append(
            f"PARK_FACTOR ({home}): {pf:.2f}  "
            f"({'high-run' if pf > 1.03 else 'low-run' if pf < 0.97 else 'neutral'} venue)"
        )
    ump = game.get("home_plate_umpire") or ""
    if ump:
        lines.append(f"HOME_PLATE_UMP: {ump}")
    w = game.get("weather") or {}
    if w:
        wparts = []
        if w.get("temp_f") is not None:
            wparts.append(f"temp={w['temp_f']:.0f}°F")
        if w.get("wind_mph") is not None:
            wparts.append(f"wind={w['wind_mph']:.0f}mph {w.get('wind_dir','')}".strip())
        if w.get("condition"):
            wparts.append(f"cond={w['condition']}")
        if wparts:
            lines.append(f"WEATHER: {', '.join(wparts)}")
    if contract_team:
        lines.append(f"CONTRACT_TEAM: {contract_team}")
    return "\n".join(lines)


def _backtest_cohort_passes(
    ticker: str, stats: dict[str, Any]
) -> tuple[bool, str]:
    """For KXMLBTOTAL / KXMLBTEAMTOTAL, return (passes, reason).

    passes=True if the relevant starter(s) sit in the backtest-validated
    cohort (rolling_era_last3 <= BACKTEST_FILTER_ERA_CAP):
      - KXMLBTOTAL: BOTH starters must be in cohort
      - KXMLBTEAMTOTAL: the OPPOSING starter (vs the contract team) must
        be in cohort

    Other series return (True, "n/a") so they're untouched. Missing
    matchup or missing rolling_era_last3 returns False so we conservatively
    cap to MEDIUM rather than trusting an unvalidated HIGH."""
    if not (ticker.startswith("KXMLBTOTAL") or ticker.startswith("KXMLBTEAMTOTAL")):
        return True, "n/a"
    parts = ticker.split("-")
    if len(parts) < 3:
        return False, "ticker-parse"
    body = parts[1]
    if len(body) < 11:
        return False, "ticker-parse"
    yy, mon, dd = body[:2], body[2:5], body[5:7]
    mm = _MLB_TICKER_MONTH.get(mon.upper())
    if not mm:
        return False, "ticker-parse"
    game_date = f"20{yy}-{mm}-{dd}"
    matchup = body[11:]
    mlb = stats.get("mlb", {}) or {}
    game = None
    for g in mlb.get("upcoming_games", []) or []:
        if g.get("game_date") != game_date:
            continue
        if (g.get("away", "") + g.get("home", "")) == matchup:
            game = g
            break
    if not game:
        return False, "no-upcoming-match"
    away_abbr = game.get("away", "")
    home_abbr = game.get("home", "")
    away_era = (game.get("away_pitcher") or {}).get("rolling_era_last3")
    home_era = (game.get("home_pitcher") or {}).get("rolling_era_last3")

    if ticker.startswith("KXMLBTEAMTOTAL"):
        contract_seg = parts[2]
        batting = None
        for abbr in (away_abbr, home_abbr):
            if abbr and contract_seg.startswith(abbr):
                if batting is None or len(abbr) > len(batting):
                    batting = abbr
        if not batting:
            return False, "batting-team-parse"
        opp_era = away_era if batting == home_abbr else home_era
        if opp_era is None:
            return False, "opp-era-missing"
        if opp_era <= BACKTEST_FILTER_ERA_CAP:
            return True, f"opp-elite/good ({opp_era:.2f})"
        return False, f"opp-avg/bad ({opp_era:.2f})"

    # KXMLBTOTAL — match the 180-day backtest cohort, which used
    # era_tier(starter_avg_era), i.e. the AVERAGE of both starters'
    # rolling_era_last3 (not min/max individually). "Both individually
    # ≤ cap" was empirically too tight: live validation against the
    # full slate showed 0% pass rate vs ~33% expected, because a single
    # bad recent start can drag one rolling-3 above 3.50 even when the
    # pair's edge is real (e.g. Skenes 2.25 + Meyer 3.93 → avg 3.09).
    if away_era is None or home_era is None:
        return False, "era-missing"
    avg_era = (away_era + home_era) / 2.0
    if avg_era <= BACKTEST_FILTER_ERA_CAP:
        return True, f"avg-elite/good ({avg_era:.2f})"
    return False, f"avg-bad ({avg_era:.2f})"


_RECALIB_CACHE: dict[str, Any] = {
    "checked_at": 0.0,
    "demoting": False,
    "reason": "",
}
_RECALIB_CACHE_TTL = 300  # 5 minutes — avoid re-reading trades_log per call


def _check_recalibration_demote() -> tuple[bool, str]:
    """Return (is_demoting, reason).

    Three paths through the function:
      1. Recently checked (<5 min ago) → return cached answer.
      2. Persistent state says we're still inside a demote window → return
         demoting=True with the original trigger reason + expiry.
      3. Window expired or never set → load trades_log.json, compute win
         rate over the last RECALIB_LOOKBACK_N settled trades. If below
         RECALIB_WR_THRESHOLD, write a new demote state file with
         demote_until_ts = now + RECALIB_DEMOTE_HOURS hours.

    State file persists so a container restart mid-cooldown preserves the
    cool-down window. State + cache both keyed independently to keep the
    persistent decision separate from the in-process call rate-limit.
    """
    if not RECALIB_DEMOTE_ENABLED:
        return False, "disabled"
    now = time.time()
    if now - _RECALIB_CACHE["checked_at"] < _RECALIB_CACHE_TTL:
        return _RECALIB_CACHE["demoting"], _RECALIB_CACHE["reason"]

    # Path 2: existing demote window still active?
    state: dict[str, Any] = {}
    if RECALIB_STATE_PATH.exists():
        try:
            with RECALIB_STATE_PATH.open() as f:
                state = json.load(f)
        except Exception as e:
            print(f"[WARN] recalib state unreadable: {e}", flush=True)
    demote_until = float(state.get("demote_until_ts") or 0)
    if demote_until > now:
        hours_left = (demote_until - now) / 3600
        reason = (
            f"{state.get('trigger_reason','unknown')} "
            f"(active for {hours_left:.1f}h more)"
        )
        _RECALIB_CACHE.update(checked_at=now, demoting=True, reason=reason)
        return True, reason

    # Path 3: re-evaluate from trades_log
    if not TRADES_LOG_PATH.exists():
        _RECALIB_CACHE.update(
            checked_at=now, demoting=False, reason="no trades_log"
        )
        return False, "no trades_log"
    try:
        with TRADES_LOG_PATH.open() as f:
            data = json.load(f)
        trades = data if isinstance(data, list) else data.get("trades", [])
    except Exception as e:
        _RECALIB_CACHE.update(
            checked_at=now, demoting=False,
            reason=f"trades_log unreadable: {e}",
        )
        return False, _RECALIB_CACHE["reason"]
    settled = [t for t in trades if t.get("outcome") in ("won", "lost")]
    if len(settled) < RECALIB_LOOKBACK_N:
        msg = f"only {len(settled)} settled < {RECALIB_LOOKBACK_N}"
        _RECALIB_CACHE.update(checked_at=now, demoting=False, reason=msg)
        return False, msg
    last_n = settled[-RECALIB_LOOKBACK_N:]
    wins = sum(1 for t in last_n if t.get("outcome") == "won")
    wr = wins / len(last_n)
    if wr < RECALIB_WR_THRESHOLD:
        demote_until = now + RECALIB_DEMOTE_HOURS * 3600
        reason = (
            f"last{RECALIB_LOOKBACK_N} wr={wr:.1%} < "
            f"{RECALIB_WR_THRESHOLD:.0%} threshold"
        )
        new_state = {
            "triggered_at_ts": now,
            "triggered_at_iso": datetime.now(timezone.utc).isoformat(),
            "demote_until_ts": demote_until,
            "demote_until_iso": datetime.fromtimestamp(
                demote_until, tz=timezone.utc
            ).isoformat(),
            "trigger_reason": reason,
            "lookback_wr": wr,
            "lookback_n": len(last_n),
        }
        try:
            RECALIB_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = RECALIB_STATE_PATH.with_suffix(".tmp")
            with tmp.open("w") as f:
                json.dump(new_state, f, indent=2)
            tmp.replace(RECALIB_STATE_PATH)
        except Exception as e:
            print(f"[WARN] recalib state save failed: {e}", flush=True)
        print(
            f"[RECALIB-DEMOTE] TRIGGERED: {reason} — "
            f"demoting HIGH→MEDIUM for {RECALIB_DEMOTE_HOURS}h",
            flush=True,
        )
        _RECALIB_CACHE.update(checked_at=now, demoting=True, reason=reason)
        return True, reason

    msg = f"last{RECALIB_LOOKBACK_N} wr={wr:.1%} >= threshold (no demote)"
    _RECALIB_CACHE.update(checked_at=now, demoting=False, reason=msg)
    return False, msg


_GRASS_SPECIALISTS_CACHE: dict[str, float] | None = None


def _load_grass_specialists() -> dict[str, float]:
    """Return {player_name: delta_pp} where delta_pp = (career grass
    win rate) − (career overall win rate), in percentage points.

    Loaded once from data/grass_specialists.json; cached for the
    process lifetime. Empty dict on any failure (filter gracefully
    skips when data is unavailable rather than blocking trades)."""
    global _GRASS_SPECIALISTS_CACHE
    if _GRASS_SPECIALISTS_CACHE is not None:
        return _GRASS_SPECIALISTS_CACHE
    try:
        with GRASS_SPECIALISTS_PATH.open() as f:
            data = json.load(f)
        _GRASS_SPECIALISTS_CACHE = {
            name: float(info.get("delta_pp", 0.0))
            for name, info in (data.get("players") or {}).items()
        }
        print(
            f"[edge] loaded {len(_GRASS_SPECIALISTS_CACHE)} grass "
            f"specialists from {GRASS_SPECIALISTS_PATH}",
            flush=True,
        )
    except Exception as e:
        print(
            f"[WARN] grass_specialists.json unloadable: {e} — "
            f"grass filter will SKIP all grass markets",
            flush=True,
        )
        _GRASS_SPECIALISTS_CACHE = {}
    return _GRASS_SPECIALISTS_CACHE


def _is_grass_event(title: str) -> bool:
    """True if the market title matches a known grass-court tournament.
    Heuristic but conservative — only the four slams (just Wimbledon
    for grass) and the established summer-grass swing events qualify.
    Adding a tournament here triggers the grass-specialist gate; leave
    out events with unknown surface to fail safe (filter doesn't fire)."""
    t = (title or "").lower()
    return any(g in t for g in GRASS_TOURNAMENTS)


def _btc_filter_passes(
    item: dict[str, Any], pred: dict[str, Any], stats: dict[str, Any]
) -> tuple[bool, str]:
    """Strict gate for KXBTC binary-direction markets. Returns
    (passes, reason).

    PASS conditions (all required):
      - Crypto Fear & Greed Index in EXTREME zone (< BTC_FG_EXTREME_LOW
        or > BTC_FG_EXTREME_HIGH)
      - YES ask in [BTC_PRICE_MIN_CENTS, BTC_PRICE_MAX_CENTS] — avoids
        bucket-shaped long tails (which the bucket regex catches anyway,
        but this is belt-and-suspenders)
      - Claude-claimed edge ≥ BTC_MIN_EDGE (default +10%)

    Non-BTC tickers return (True, "n/a") so this is a no-op on other
    series. Direction agreement (fear+momentum-up = bounce, etc.) is
    enforced via prompt methodology, not as a hard filter, because the
    direction-mapping depends on each market's specific YES semantics
    (which Claude reads from the title)."""
    ticker = item.get("ticker", "")
    if not ticker.startswith("KXBTC"):
        return True, "n/a"
    # Bucket markets are killed by _is_btc_bucket_ticker upstream; defense-
    # in-depth here so a bucket that somehow slips through still SKIPs.
    if _is_btc_bucket_ticker(ticker):
        return False, "bucket ticker (should have been filtered earlier)"

    edge = float(pred.get("edge") or 0.0)
    if edge < BTC_MIN_EDGE:
        return False, f"edge={edge:+.2f} < +{BTC_MIN_EDGE:.2f} BTC floor"

    yes_ask = int(item.get("yes_ask_cents") or 0)
    if yes_ask < BTC_PRICE_MIN_CENTS or yes_ask > BTC_PRICE_MAX_CENTS:
        return False, (
            f"yes_ask={yes_ask}c outside [{BTC_PRICE_MIN_CENTS}, "
            f"{BTC_PRICE_MAX_CENTS}]¢ band"
        )

    econ = (stats.get("economic") or {})
    fng = econ.get("crypto_fear_greed_value")
    if fng is None:
        return False, "no Fear & Greed reading in stats cache"
    try:
        fng_i = int(fng)
    except (TypeError, ValueError):
        return False, f"F&G unparseable: {fng!r}"
    if BTC_FG_EXTREME_LOW <= fng_i <= BTC_FG_EXTREME_HIGH:
        return False, (
            f"F&G={fng_i} in calm zone "
            f"[{BTC_FG_EXTREME_LOW}, {BTC_FG_EXTREME_HIGH}] — "
            f"need <{BTC_FG_EXTREME_LOW} or >{BTC_FG_EXTREME_HIGH}"
        )

    momentum = econ.get("btc_24h_momentum_pct")
    classification = econ.get("crypto_fear_greed_classification", "")
    return True, (
        f"F&G={fng_i}({classification}) "
        f"yes_ask={yes_ask}c edge={edge:+.2f} "
        f"btc_24h={momentum}%"
    )


def _tennis_filter_passes(
    item: dict[str, Any], pred: dict[str, Any], stats: dict[str, Any]
) -> tuple[bool, str]:
    """Mispricing gate for KXATPMATCH / KXWTAMATCH. Returns (passes, reason).

    PASS conditions (all required):
      - Claude confidence is HIGH
      - YES ask ≤ TENNIS_MAX_ASK_CENTS
      - Both market entities are present in the corresponding rankings
        (atp_rankings for KXATPMATCH, wta_rankings for KXWTAMATCH)
      - YES side is the higher-ranked player (lower rank number) —
        identified by which surname appears first in the market title
      - (other_rank − yes_rank) > TENNIS_MIN_RANK_GAP

    Anything else returns (False, "<diagnostic>") so the caller can
    SKIP and log the reason. Non-tennis tickers return (True, "n/a")
    so this is a no-op on every other series.
    """
    ticker = item.get("ticker", "")
    is_atp = ticker.startswith("KXATPMATCH")
    is_wta = ticker.startswith("KXWTAMATCH")
    if not (is_atp or is_wta):
        return True, "n/a"

    if pred.get("confidence") != "HIGH":
        return False, f"conf={pred.get('confidence')} != HIGH"

    yes_ask = int(item.get("yes_ask_cents") or 100)
    if yes_ask > TENNIS_MAX_ASK_CENTS:
        return False, f"yes_ask={yes_ask}c > {TENNIS_MAX_ASK_CENTS}c"

    tennis = stats.get("tennis", {}) or {}
    board_key = "atp_rankings" if is_atp else "wta_rankings"
    rankings: dict[str, int] = {}
    for row in tennis.get(board_key, []) or []:
        name = row.get("player")
        rank = row.get("rank")
        if name and isinstance(rank, int):
            rankings[name] = rank

    matched = [e for e in (item.get("entities") or []) if e in rankings]
    if len(matched) < 2:
        return False, f"only {len(matched)} ranked players matched ({matched!r})"

    # Identify YES side = first surname appearance in title. Tennis market
    # titles read "<Player A> vs <Player B>" or "Will <A> beat <B>?" — in
    # both formats the YES side is the first-mentioned player.
    title_l = (item.get("title", "") or "").lower()

    def _first_pos(name: str) -> int:
        last = name.split()[-1].lower()
        if not last:
            return 10**9
        pos = title_l.find(last)
        return pos if pos >= 0 else 10**9

    by_pos = sorted(matched, key=_first_pos)
    yes_player = by_pos[0]
    if _first_pos(yes_player) >= 10**9:
        return False, "no player surname found in title"
    others = [p for p in matched if p != yes_player]
    if not others:
        return False, "could not isolate opponent"
    no_player = others[0]
    yes_rank = rankings[yes_player]
    no_rank = rankings[no_player]

    # Filter case is "favorite at a discount" — if YES is the underdog,
    # the price ≤ 62¢ is correctly priced down, not mispriced up.
    if yes_rank >= no_rank:
        return False, (
            f"YES={yes_player}(#{yes_rank}) is lower-ranked than "
            f"{no_player}(#{no_rank})"
        )

    gap = no_rank - yes_rank
    if gap <= TENNIS_MIN_RANK_GAP:
        return False, f"gap={gap} <= {TENNIS_MIN_RANK_GAP}"

    # Grass-event additional gate. 380-day backtest 2026-06-14 showed
    # that on grass tournaments, the higher-ranked player's win rate is
    # cleanly stratified by their grass-vs-overall career delta vs the
    # opponent. When A's grass delta exceeds B's by ≥5pp, the favorite
    # wins 73.5% (Wilson_lo 56.9%); when A is ≥5pp worse on grass, the
    # favorite drops to 53.7%. Without this gate the filter would buy
    # rank-favorites who are systematically worse on grass than their
    # ranking suggests — exactly the wrong side at Wimbledon.
    if GRASS_FILTER_ENABLED and _is_grass_event(item.get("title", "")):
        specialists = _load_grass_specialists()
        yes_delta = specialists.get(yes_player)
        no_delta = specialists.get(no_player)
        if yes_delta is None or no_delta is None:
            return False, (
                f"grass event but missing specialist data "
                f"(yes={yes_player!r}: {yes_delta}, "
                f"no={no_player!r}: {no_delta})"
            )
        diff = yes_delta - no_delta
        if diff < GRASS_MIN_DELTA_DIFF_PP:
            return False, (
                f"grass event but YES grass-delta vs opp is "
                f"{diff:+.1f}pp < {GRASS_MIN_DELTA_DIFF_PP:.1f}pp threshold "
                f"({yes_player}={yes_delta:+.1f}pp, "
                f"{no_player}={no_delta:+.1f}pp)"
            )
        return True, (
            f"YES={yes_player}(#{yes_rank}) vs {no_player}(#{no_rank}) "
            f"gap={gap} ask={yes_ask}c [grass-spec diff +{diff:.1f}pp]"
        )

    return True, (
        f"YES={yes_player}(#{yes_rank}) vs {no_player}(#{no_rank}) "
        f"gap={gap} ask={yes_ask}c"
    )


def _build_user_message(items: list[dict[str, Any]], stats: dict[str, Any]) -> str:
    blocks = []
    for i, it in enumerate(items, 1):
        ya = it["yes_ask_cents"]
        na = it.get("no_ask_cents", max(1, 100 - ya))
        entities = ", ".join(it.get("entities") or []) or "(none matched)"
        block = (
            f"=== MARKET {i} ===\n"
            f"TICKER: {it['ticker']}\n"
            f"TITLE: {it['title']}\n"
            f"HOURS UNTIL RESOLUTION: {it['hours_left']}\n"
            f"MARKET YES PRICE: {ya}¢ (implies {ya/100:.2%} YES)\n"
            f"MARKET NO PRICE: {na}¢ (implies {na/100:.2%} NO)\n"
            f"STATS ENTITIES MATCHED: {entities}"
        )
        facts = _mlb_facts_for_ticker(it["ticker"], stats)
        if facts:
            block += "\nFACTS:\n" + facts
        blocks.append(block)
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
                stats: dict[str, Any],
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
                # 400 tok/market (was 200): tennis predictions on
                # KXATPMATCH/KXWTAMATCH need rank-gap + recent-form
                # reasoning that frequently overran the prior cap, causing
                # the last few tickers in a batch to be silently truncated
                # — logs showed every tennis ticker hitting "no prediction
                # parsed" because Claude ran out of tokens before echoing
                # their TICKER block.
                "max_tokens": 400 * len(batch),
                "system": [
                    {
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                "messages": [{"role": "user", "content": _build_user_message(batch, stats)}],
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
    is_buy_no = pred.get("recommendation") == "BUY_NO"
    price_field = (
        f"{item.get('no_ask_cents', '?')}¢ (NO)"
        if is_buy_no
        else f"{item.get('yes_ask_cents', '?')}¢"
    )
    return {
        "title": f"🎯 KALSHI EDGE — {title[:80]}",
        "url": market_url,
        "color": 0xE67E22 if is_buy_no else 0x2ECC71,
        "fields": [
            {"name": "Market Price", "value": price_field, "inline": True},
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
    print(
        f"[edge] tennis-filter: enabled={TENNIS_FILTER_ENABLED} "
        f"max_ask_cents={TENNIS_MAX_ASK_CENTS} "
        f"min_rank_gap={TENNIS_MIN_RANK_GAP} "
        f"series={['KXATPMATCH','KXWTAMATCH']}",
        flush=True,
    )
    print(
        f"[edge] grass-filter: enabled={GRASS_FILTER_ENABLED} "
        f"min_delta_diff_pp={GRASS_MIN_DELTA_DIFF_PP} "
        f"specialists_loaded={len(_load_grass_specialists())} "
        f"tournaments={len(GRASS_TOURNAMENTS)}",
        flush=True,
    )
    print(
        f"[edge] btc-filter: enabled={BTC_FILTER_ENABLED} "
        f"f&g_extreme_zone=[<{BTC_FG_EXTREME_LOW},>{BTC_FG_EXTREME_HIGH}] "
        f"price_band=[{BTC_PRICE_MIN_CENTS},{BTC_PRICE_MAX_CENTS}]¢ "
        f"min_edge={BTC_MIN_EDGE:+.2f}",
        flush=True,
    )
    print(
        f"[edge] recalib-demote: enabled={RECALIB_DEMOTE_ENABLED} "
        f"lookback_n={RECALIB_LOOKBACK_N} "
        f"wr_threshold={RECALIB_WR_THRESHOLD:.0%} "
        f"demote_hours={RECALIB_DEMOTE_HOURS}",
        flush=True,
    )
    print(f"[edge] series active: {SERIES_TICKERS}", flush=True)
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
        # Drop seen-cache entries that crossed the SEEN_EXPIRY_SECS TTL
        # Reload seen-cache from disk EVERY cycle. _load_seen()
        # filters out entries past the SEEN_EXPIRY_SECS TTL, so the
        # reload covers both natural expiry AND any out-of-band
        # eviction someone ran via SSH between cycles. The delta
        # against the prior in-memory count is the [SEEN-EXPIRED]
        # signal — includes both TTL expiries and external evictions.
        prev_seen_count = len(seen)
        seen = _load_seen()
        delta = prev_seen_count - len(seen)
        if delta > 0:
            print(
                f"[SEEN-EXPIRED] count={delta} "
                f"ttl_hours={SEEN_EXPIRY_SECS/3600:.1f} "
                f"(TTL expiries + external evictions since last cycle)",
                flush=True,
            )
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
                candidates, pre_drops = _filter_markets(
                    markets, stats, seen, price_history
                )
                _save_price_history(price_history)

                # Funnel counters — populated at every SKIP / demote /
                # promote site below, then dumped in one [CYCLE-FUNNEL]
                # line at cycle end. Lets you grep one line per cycle to
                # see which gate did what, instead of cross-referencing
                # 8 different log patterns.
                post_drops: dict[str, int] = {}
                demoted: dict[str, int] = {}
                reached_claude = 0
                parsed = 0
                approved = 0
                skipped = 0

                if not candidates:
                    print(
                        f"[edge] cycle={cycle} no candidates after filter — "
                        "skipping Claude call",
                        flush=True,
                    )
                else:
                    system_prompt = _build_system_prompt(stats)
                    for start in range(0, len(candidates), BATCH_SIZE):
                        batch = candidates[start : start + BATCH_SIZE]
                        reached_claude += len(batch)
                        preds = _ask_claude(system_prompt, batch, stats, log_raw=(start == 0))
                        for it in batch:
                            ticker = it["ticker"]
                            seen[ticker] = time.time()
                            pred = preds.get(ticker)
                            if not pred:
                                skipped += 1
                                post_drops["no_prediction"] = (
                                    post_drops.get("no_prediction", 0) + 1
                                )
                                if DEBUG_LOG:
                                    print(f"[edge-debug] {ticker} no prediction parsed", flush=True)
                                continue
                            parsed += 1
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
                                    demoted["whale_boost"] = (
                                        demoted.get("whale_boost", 0) + 1
                                    )
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
                                post_drops["max_edge_cap"] = (
                                    post_drops.get("max_edge_cap", 0) + 1
                                )
                            # Backtest-validated cohort cap for KXMLBTOTAL /
                            # KXMLBTEAMTOTAL. 180-day backtest showed real
                            # lift over the trivial baseline only when the
                            # relevant starter(s) sit at rolling_era_last3
                            # <= 3.50. Outside that cohort, HIGH-confidence
                            # picks weren't statistically distinguishable
                            # from coin-flip → cap to MEDIUM so we still
                            # trade but at the higher MEDIUM_MIN_EDGE bar.
                            if (
                                BACKTEST_FILTER_ENABLED
                                and pred["confidence"] == "HIGH"
                                and pred["recommendation"] == "BUY"
                            ):
                                cohort_ok, reason = _backtest_cohort_passes(
                                    ticker, stats
                                )
                                if not cohort_ok:
                                    pred["confidence"] = "MEDIUM"
                                    demoted["backtest_filter"] = (
                                        demoted.get("backtest_filter", 0) + 1
                                    )
                                    pred["reasoning"] = (
                                        "[backtest-filter HIGH->MEDIUM] "
                                        + pred.get("reasoning", "")
                                    )
                                    print(
                                        f"[BACKTEST-FILTER] {ticker} "
                                        f"HIGH->MEDIUM reason={reason}",
                                        flush=True,
                                    )
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
                                        demoted["backtest_boost"] = (
                                            demoted.get("backtest_boost", 0) + 1
                                        )
                                        pred["reasoning"] = (
                                            "[backtest-boost MEDIUM->HIGH] "
                                            + pred.get("reasoning", "")
                                        )
                                        print(
                                            f"[BACKTEST-BOOST] {ticker} MEDIUM->HIGH "
                                            f"{common}",
                                            flush=True,
                                        )
                            # Recalibration demote. If the live win-rate on
                            # the last N settled trades is below threshold,
                            # cool off model overconfidence by capping HIGH
                            # at MEDIUM for RECALIB_DEMOTE_HOURS. Fires
                            # AFTER BACKTEST-BOOST so a boosted MEDIUM→HIGH
                            # gets demoted right back during a losing
                            # streak, and BEFORE TENNIS-FILTER so a demoted
                            # MEDIUM correctly fails the HIGH-only tennis
                            # gate. State persists across restarts in
                            # /app/data/recalibration_demote.json.
                            if (
                                RECALIB_DEMOTE_ENABLED
                                and pred["recommendation"] == "BUY"
                                and pred["confidence"] == "HIGH"
                            ):
                                demoting, reason = _check_recalibration_demote()
                                if demoting:
                                    pred["confidence"] = "MEDIUM"
                                    demoted["recalib_demote"] = (
                                        demoted.get("recalib_demote", 0) + 1
                                    )
                                    pred["reasoning"] = (
                                        f"[recalib-demote HIGH->MEDIUM {reason}] "
                                        + pred.get("reasoning", "")
                                    )
                                    print(
                                        f"[RECALIB-DEMOTE] {ticker} "
                                        f"HIGH->MEDIUM ({reason})",
                                        flush=True,
                                    )
                            # Tennis market-price filter. KXATPMATCH /
                            # KXWTAMATCH only: backtest showed no
                            # directional lift over "always pick higher-
                            # ranked", so we instead require a clear
                            # mispricing — HIGH conf + rank-favorite YES
                            # at ≤ 62¢ + gap > 50 spots. Everything else
                            # on these series gets SKIP'd, regardless of
                            # Claude's edge call. Non-tennis tickers pass
                            # through unchanged.
                            if (
                                TENNIS_FILTER_ENABLED
                                and pred["recommendation"] == "BUY"
                                and (
                                    ticker.startswith("KXATPMATCH")
                                    or ticker.startswith("KXWTAMATCH")
                                )
                            ):
                                ok, reason = _tennis_filter_passes(it, pred, stats)
                                if not ok:
                                    pred["recommendation"] = "SKIP"
                                    # Bucket grass-specialist SKIPs separately
                                    # from generic tennis-filter SKIPs so the
                                    # funnel surfaces which one is doing the work.
                                    bucket = (
                                        "tennis_grass_filter"
                                        if "grass" in reason.lower()
                                        else "tennis_filter"
                                    )
                                    post_drops[bucket] = post_drops.get(bucket, 0) + 1
                                    pred["reasoning"] = (
                                        f"[tennis-filter SKIP] {reason} | "
                                        + pred.get("reasoning", "")
                                    )
                                    print(
                                        f"[TENNIS-FILTER] {ticker} SKIP "
                                        f"reason={reason}",
                                        flush=True,
                                    )
                                else:
                                    print(
                                        f"[TENNIS-FILTER] {ticker} PASS {reason}",
                                        flush=True,
                                    )
                            # BTC mispricing filter. KXBTC binary
                            # markets only — bucket tickers already
                            # filtered by _is_btc_bucket_ticker. Need
                            # F&G in extreme zone + ask 30-70¢ + edge
                            # ≥ 10%. Non-KXBTC tickers pass through.
                            if (
                                BTC_FILTER_ENABLED
                                and pred["recommendation"] == "BUY"
                                and ticker.startswith("KXBTC")
                            ):
                                ok, reason = _btc_filter_passes(it, pred, stats)
                                if not ok:
                                    pred["recommendation"] = "SKIP"
                                    post_drops["btc_filter"] = (
                                        post_drops.get("btc_filter", 0) + 1
                                    )
                                    pred["reasoning"] = (
                                        f"[btc-filter SKIP] {reason} | "
                                        + pred.get("reasoning", "")
                                    )
                                    print(
                                        f"[BTC-FILTER] {ticker} SKIP "
                                        f"reason={reason}",
                                        flush=True,
                                    )
                                else:
                                    print(
                                        f"[BTC-FILTER] {ticker} PASS {reason}",
                                        flush=True,
                                    )
                            # BUY_NO eligibility — narrow KXMLBTOTAL -9/-10
                            # cohort, MEDIUM-only. Out-of-cohort or wrong-tier
                            # BUY_NO recommendations from Claude are forced to
                            # SKIP here so the prompt rule is enforced in code,
                            # not just trust.
                            rec = pred["recommendation"]
                            if rec == "BUY_NO":
                                tail = (
                                    ticker.rsplit("-", 1)[-1] if "-" in ticker else ""
                                )
                                eligible = (
                                    ticker.startswith("KXMLBTOTAL")
                                    and tail in ("9", "10")
                                    and pred["confidence"] == "MEDIUM"
                                )
                                if not eligible:
                                    post_drops["buy_no_ineligible"] = (
                                        post_drops.get("buy_no_ineligible", 0) + 1
                                    )
                                    print(
                                        f"[BUY-NO-INELIGIBLE] {ticker} "
                                        f"conf={pred['confidence']} tail={tail}",
                                        flush=True,
                                    )
                                    pred["recommendation"] = "SKIP"
                                    rec = "SKIP"

                            is_buy_yes = rec == "BUY"
                            is_buy_no = rec == "BUY_NO"
                            high_ok = (
                                pred["confidence"] == "HIGH"
                                and pred["edge"] >= MIN_EDGE
                                and not is_buy_no
                            )
                            medium_ok = (
                                pred["confidence"] == "MEDIUM"
                                and pred["edge"] >= MEDIUM_MIN_EDGE
                            )
                            if not (is_buy_yes or is_buy_no) or not (
                                high_ok or medium_ok
                            ):
                                skipped += 1
                                # Distinguish "Claude voted SKIP" from "Claude
                                # voted BUY/BUY_NO but tier-edge floor wasn't
                                # cleared" — different signals, different fixes.
                                if not (is_buy_yes or is_buy_no):
                                    post_drops["claude_skip"] = (
                                        post_drops.get("claude_skip", 0) + 1
                                    )
                                else:
                                    post_drops["low_edge"] = (
                                        post_drops.get("low_edge", 0) + 1
                                    )
                                continue
                            approved += 1
                            if is_buy_no:
                                payload = {
                                    "ticker": ticker,
                                    "title": it["title"],
                                    "yes_ask": it["yes_ask_cents"],
                                    "no_ask": it["no_ask_cents"],
                                    "hours_left": it["hours_left"],
                                    "close_time": it["close_time"],
                                    "true_probability": pred["true_probability"],
                                    "edge": pred["edge"],
                                    "confidence": pred["confidence"],
                                    "recommendation": "BUY_NO",
                                    "reasoning": pred["reasoning"],
                                    "side": "no",
                                    "price_for_order_cents": it["no_ask_cents"],
                                    "entities": it.get("entities") or [],
                                }
                            else:
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
                # ─── Cycle funnel summary ──────────────────────────────
                # One structured line per cycle so you can grep one
                # pattern and see exactly which gate dropped what.
                # Sums:
                #   fetched   = total markets pulled from Kalshi
                #   kept      = passed pre-Claude filter
                #   parsed    = Claude returned a parseable verdict
                #   approved  = made it to the queue
                # Drops (true rejections) are in `drops={...}`; demotions
                # / promotions (confidence-tier changes that did NOT drop
                # the trade) are in `demoted={...}` so you can see filter
                # activity even on markets that ultimately approved.
                pre_drop_total = sum(pre_drops.values())
                fetched_total = len(candidates) + pre_drop_total
                drops_str = ",".join(
                    f"{k}:{v}" for k, v in sorted(pre_drops.items()) if v
                )
                post_drops_str = ",".join(
                    f"{k}:{v}" for k, v in sorted(post_drops.items()) if v
                )
                demoted_str = ",".join(
                    f"{k}:{v}" for k, v in sorted(demoted.items()) if v
                )
                print(
                    f"[CYCLE-FUNNEL] cycle={cycle} "
                    f"fetched={fetched_total} kept={len(candidates)} "
                    f"reached_claude={reached_claude} parsed={parsed} "
                    f"approved={approved} skipped={skipped} "
                    f"pre_drops={{{drops_str}}} "
                    f"post_drops={{{post_drops_str}}} "
                    f"demoted={{{demoted_str}}}",
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
