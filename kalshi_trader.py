"""Kalshi trader — sizes, places, and tracks every approved trade.

Pulls approved trades off kalshi_queue stage "risk" (filled by
kalshi_edge), sizes each with half-Kelly capped at MAX_BET_PCT of
bankroll and the remaining daily-loss budget, then places the order.
Side is read from the payload — BUY_YES pays yes_ask, BUY_NO pays the
NO ask carried in `price_for_order_cents`. BUY_NO is only enabled by
kalshi_edge for the narrow KXMLBTOTAL -9/-10 cohort (2026-06-17).
PAPER_TRADING gates whether the order hits Kalshi or just gets logged.

Every placed order is appended to trades_log.json with status=pending.
Once an hour the same loop walks the pending entries and:
  - Fetches each ticker's current state via /markets/{ticker}
  - If the market is resolved (status=finalized / settled), flips the
    entry to won/lost and computes pnl in dollars

Zero Claude calls — this is pure plumbing + arithmetic.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

import kalshi_queue
from kalshi_auth import KALSHI_BASE_URL, get_auth_headers

WEBHOOK_KALSHI_TRADER = os.getenv("WEBHOOK_KALSHI_TRADER", "")
PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
# Per-series paper-trading override. When PAPER_TRADING=false (live
# mode), any ticker whose prefix matches an entry in this list STILL
# paper-trades. Designed for the auto-live flip: when calibration
# criteria pass and the system goes live, only backtest-validated
# series (default: KXMLBTOTAL via empty override) actually hit Kalshi
# with real money; experimental strategies (tennis, BTC) stay paper
# until they earn their way out. When PAPER_TRADING=true (global
# paper), this list is moot — everything is paper.
PAPER_SERIES: tuple[str, ...] = tuple(
    s.strip() for s in os.getenv(
        "KALSHI_PAPER_SERIES",
        "KXATPMATCH,KXWTAMATCH,KXBTC",
    ).split(",") if s.strip()
)


def _is_paper_for_ticker(ticker: str) -> bool:
    """Return True if the ticker should be paper-traded. Global
    PAPER_TRADING=true forces paper for everything; otherwise the
    ticker is paper iff its prefix matches any entry in PAPER_SERIES."""
    if PAPER_TRADING:
        return True
    return any(ticker.startswith(p) for p in PAPER_SERIES)
CHECK_INTERVAL = int(os.getenv("KALSHI_TRADER_INTERVAL", "300"))   # 5 min
SETTLEMENT_INTERVAL = int(os.getenv("KALSHI_SETTLEMENT_INTERVAL", "3600"))  # 1 h
BANKROLL = float(os.getenv("BANKROLL", "500"))
MAX_BET_PCT = float(os.getenv("MAX_BET_PCT", "0.05"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "0.10"))
MIN_BET_USD = float(os.getenv("KALSHI_MIN_BET_USD", "5"))
MIN_CONTRACTS = int(os.getenv("KALSHI_MIN_CONTRACTS", "1"))
TRADES_LOG_PATH = Path(os.getenv("KALSHI_TRADES_LOG", "/app/data/trades_log.json"))
DAILY_SPENT_PATH = Path(os.getenv("KALSHI_DAILY_SPENT", "/app/data/daily_spent.json"))
CALIBRATION_RUNNING_PATH = Path(os.getenv(
    "KALSHI_CALIBRATION_RUNNING", "/app/data/calibration_running.json"
))
# Per-ticker prediction-accuracy log. Written by _record_prediction_accuracy
# at settlement for KXMLBTOTAL trades only — every other series's run-total
# concept is undefined. Read by scripts/calibration_live.py.
PREDICTION_ACCURACY_PATH = Path(os.getenv(
    "KALSHI_PREDICTION_ACCURACY", "/app/data/prediction_accuracy.json"
))
# Anything within ±this of actual counts as a directionally-correct
# projection. 1.0 run is a generous-but-defensible threshold for "the
# model essentially got the total right"; tightening to 0.5 would push
# almost every prediction into OVER/UNDER given run totals are integers.
ACCURACY_TOLERANCE_RUNS = 1.0
# Broad projection regex — matches Haiku's actual phrasings
# ("projected", "predicted", "baseline", "expected", "estimated"). Same
# pattern kalshi_edge.py uses for the post-Claude RULE 1 and BUY_NO
# projection gates; kept in sync so a SKIP and an [ACCURACY] record
# always read the same number from the same reasoning string.
_PROJECTED_RUNS_RE = re.compile(
    r"(?:project\w*|predict\w*|baseline|expected|estimate\w*)"
    r"[^\d]{0,60}(\d+(?:\.\d+)?)\s*(?:total\s+)?runs?",
    re.IGNORECASE,
)
_MLB_STATSAPI = "https://statsapi.mlb.com/api/v1"
# Weekly drawdown circuit-breaker. Distinct from MAX_DAILY_LOSS — the
# daily cap blocks new spend mid-day but resets fresh tomorrow. The
# weekly limit blocks ALL trades for the rest of the week once the
# threshold trips, and only resumes when Monday rolls over UTC.
WEEKLY_DRAWDOWN_PCT = float(os.getenv("KALSHI_WEEKLY_DRAWDOWN_PCT", "0.30"))
WEEKLY_STATE_PATH = Path(os.getenv(
    "KALSHI_WEEKLY_STATE", "/app/data/weekly_state.json"
))

# Daily-spend tracking. Resets at UTC midnight. Persisted to disk so the
# cap survives container restarts — without persistence, every redeploy
# zeroes the budget and a noisy deploy day can multiply the effective cap.
_daily_spent = 0.0
_last_reset = datetime.now(timezone.utc).date()

# Single lock guards trades_log.json reads + writes inside this process.
_log_lock = threading.Lock()


def _format_usd(amount: float) -> str:
    sign = "-" if amount < 0 else ""
    a = abs(amount)
    if a >= 1_000_000:
        return f"{sign}${a / 1_000_000:.2f}M"
    if a >= 1_000:
        return f"{sign}${a / 1_000:.1f}K"
    return f"{sign}${a:.2f}"


def _check_daily_reset() -> None:
    global _daily_spent, _last_reset
    today = datetime.now(timezone.utc).date()
    if today > _last_reset:
        _daily_spent = 0.0
        _last_reset = today
        _save_daily_spent()
        print("[trader] daily spend reset", flush=True)


# ─── Weekly drawdown circuit-breaker ──────────────────────────────────
# State machine: track running pnl + spent for the calendar week
# (Monday 00:00 UTC → next Monday 00:00 UTC). If pnl drops below
# -WEEKLY_DRAWDOWN_PCT × BANKROLL, flip paused=true and refuse all
# trades until the week rolls. State persists to disk so a process
# restart mid-pause preserves the lockout.
_weekly_state: dict[str, Any] = {
    "week_start": "",
    "spent": 0.0,
    "pnl": 0.0,
    "paused": False,
    "paused_at": None,
}


def _current_week_monday() -> date:
    """Monday 00:00 UTC of current week. weekday(): Mon=0, Sun=6."""
    today = datetime.now(timezone.utc).date()
    return today - timedelta(days=today.weekday())


def _load_weekly_state() -> dict[str, Any]:
    """Return persisted weekly state; reset to fresh if week has rolled."""
    today_monday = _current_week_monday()
    default = {
        "week_start": today_monday.isoformat(),
        "spent": 0.0, "pnl": 0.0,
        "paused": False, "paused_at": None,
    }
    if not WEEKLY_STATE_PATH.exists():
        return default
    try:
        with WEEKLY_STATE_PATH.open() as f:
            data = json.load(f)
        saved_monday = date.fromisoformat(str(data.get("week_start", "")))
    except Exception as e:
        print(f"[WARN] weekly_state unreadable: {e} — resetting", flush=True)
        return default
    if saved_monday != today_monday:
        return default
    return {
        "week_start": saved_monday.isoformat(),
        "spent": float(data.get("spent", 0.0)),
        "pnl": float(data.get("pnl", 0.0)),
        "paused": bool(data.get("paused", False)),
        "paused_at": data.get("paused_at"),
    }


def _save_weekly_state() -> None:
    WEEKLY_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = WEEKLY_STATE_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(_weekly_state, f, indent=2)
    tmp.replace(WEEKLY_STATE_PATH)


def _check_weekly_reset() -> None:
    """If Monday UTC has rolled since last save, full reset. Logs
    [WEEKLY-RESET] with last week's final pnl as a closing line."""
    global _weekly_state
    today_monday = _current_week_monday().isoformat()
    if _weekly_state["week_start"] != today_monday:
        print(
            f"[WEEKLY-RESET] prev_week={_weekly_state['week_start']} "
            f"final_pnl={_format_usd(_weekly_state['pnl'])} "
            f"spent={_format_usd(_weekly_state['spent'])} "
            f"paused_was={_weekly_state['paused']} "
            f"new_week={today_monday}",
            flush=True,
        )
        _weekly_state = {
            "week_start": today_monday,
            "spent": 0.0, "pnl": 0.0,
            "paused": False, "paused_at": None,
        }
        _save_weekly_state()


def _weekly_pause_embed() -> dict[str, Any]:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return {
        "title": "⚠️ WEEKLY DRAWDOWN HIT — pausing until Monday",
        "color": 0xE74C3C,
        "fields": [
            {"name": "Week start (Mon UTC)",
             "value": _weekly_state["week_start"], "inline": True},
            {"name": "Weekly P&L",
             "value": _format_usd(_weekly_state["pnl"]), "inline": True},
            {"name": "Weekly Spent",
             "value": _format_usd(_weekly_state["spent"]), "inline": True},
            {"name": "Drawdown threshold",
             "value": f"-{WEEKLY_DRAWDOWN_PCT:.0%} of "
                      f"{_format_usd(BANKROLL)} = "
                      f"{_format_usd(-BANKROLL * WEEKLY_DRAWDOWN_PCT)}",
             "inline": False},
            {"name": "Resumes",
             "value": "Monday 00:00 UTC (auto)", "inline": False},
        ],
        "footer": {"text": f"PassivePoly Trader  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _check_weekly_drawdown() -> bool:
    """Compare current weekly pnl against the loss threshold. If it
    crosses, flip the paused flag, save state, and return True so the
    caller can log + Discord-alert. Idempotent — already-paused state
    returns False so we don't double-alert each cycle."""
    global _weekly_state
    if _weekly_state["paused"]:
        return False
    threshold = -BANKROLL * WEEKLY_DRAWDOWN_PCT
    if _weekly_state["pnl"] <= threshold:
        _weekly_state["paused"] = True
        _weekly_state["paused_at"] = datetime.now(timezone.utc).isoformat()
        _save_weekly_state()
        return True
    return False


def _load_daily_spent() -> tuple[date, float]:
    """Return (date, spent) from /app/data/daily_spent.json. If the file is
    missing, malformed, or its date doesn't match today (UTC), return
    (today, 0.0) so a new UTC day starts the cap clean."""
    today = datetime.now(timezone.utc).date()
    if not DAILY_SPENT_PATH.exists():
        return today, 0.0
    try:
        with DAILY_SPENT_PATH.open() as f:
            data = json.load(f)
        saved_date = date.fromisoformat(str(data.get("date", "")))
        spent = float(data.get("spent", 0.0))
    except Exception as e:
        print(f"[WARN] daily_spent unreadable: {e} — resetting", flush=True)
        return today, 0.0
    if saved_date != today:
        return today, 0.0
    return saved_date, spent


def _save_daily_spent() -> None:
    DAILY_SPENT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = DAILY_SPENT_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(
            {"date": _last_reset.isoformat(), "spent": round(_daily_spent, 2)},
            f,
        )
    tmp.replace(DAILY_SPENT_PATH)


# ─── Trades log ────────────────────────────────────────────────────────

def _load_log() -> list[dict[str, Any]]:
    if not TRADES_LOG_PATH.exists():
        return []
    try:
        with TRADES_LOG_PATH.open() as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[WARN] trades_log unreadable: {e}", flush=True)
        return []


def _save_log(entries: list[dict[str, Any]]) -> None:
    TRADES_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = TRADES_LOG_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(entries, f, indent=2)
    tmp.replace(TRADES_LOG_PATH)


def _append_trade(entry: dict[str, Any]) -> None:
    with _log_lock:
        entries = _load_log()
        entries.append(entry)
        _save_log(entries)


# ─── Correlated-bet protection ─────────────────────────────────────────
# We don't want to stack multiple bets on the same team / event in one
# day — correlated outcomes turn one "I was wrong" into a clustered loss
# that blows past MAX_DAILY_LOSS. Block if today's trades_log already has
# an entry with the same ticker, same event (ticker suffix shared across
# bet types — e.g., KXMLBGAME-... and KXMLBSPREAD-... for the same game),
# or any overlapping team/player entity.


def _event_key(ticker: str) -> str:
    """Everything after the series prefix in the ticker — encodes the
    underlying event (date+teams or date+players). Same event_key across
    different series prefixes means "same game, different bet type"."""
    parts = ticker.split("-", 1)
    return parts[1] if len(parts) == 2 else ticker


def _is_today_utc(timestamp_iso: str, today: datetime) -> bool:
    if not timestamp_iso:
        return False
    try:
        dt = datetime.fromisoformat(timestamp_iso.replace("Z", "+00:00"))
    except ValueError:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.date() == today.date()


# Per-series daily caps. Different from _correlation_collision — this
# rejects further trades on a specific series once N have been placed
# today, regardless of whether individual events / entities collide.
# Motivated by 2026-06-14 EOD: a 4-trade KXMLBTEAMTOTAL cluster all
# resolved as losses; even with the cohort filter, repeated bets in
# one series during a single day amplify single-day P&L variance.
# KXMLBTOTAL is the series we'd most want exposure on (only positive-EV
# MLB cohort in the recent 30-day backtest), so cap at 2 — keeps a
# diversified slate without all-in on one prediction loop. Tennis series
# (KXATPMATCH / KXWTAMATCH) are deliberately excluded — independent
# matches, independent prediction inputs, no series-cluster risk.
SERIES_DAILY_CAPS: dict[str, int] = {
    "KXMLBTOTAL": int(os.getenv("KALSHI_KXMLBTOTAL_DAILY_CAP", "2")),
}


def _series_daily_cap_collision(ticker: str) -> str | None:
    """Return a reject_reason if a per-series daily cap is exhausted,
    else None. Counts today's trades whose ticker starts with the
    capped prefix and whose outcome is not 'rejected' (queued, pending,
    or settled all count — only pre-placement rejections are free).
    """
    series = next(
        (p for p in SERIES_DAILY_CAPS if ticker.startswith(p)),
        None,
    )
    if series is None:
        return None
    today = datetime.now(timezone.utc)
    cap = SERIES_DAILY_CAPS[series]
    with _log_lock:
        entries = _load_log()
    count = sum(
        1 for e in entries
        if (e.get("ticker") or "").startswith(series)
        and e.get("outcome") not in (None, "rejected")
        and _is_today_utc(e.get("timestamp", ""), today)
    )
    if count >= cap:
        return (
            f"daily cap on {series} hit "
            f"({count}/{cap} already placed today UTC)"
        )
    return None


def _correlation_collision(ticker: str, entities: list[str]) -> str | None:
    """Return a reject_reason if this trade collides with one we already
    placed today (same ticker, same event, or any shared entity);
    otherwise None. Reads the trades_log under the lock — placed trades
    are written there before we ever consider sizing the next one."""
    today = datetime.now(timezone.utc)
    event_key = _event_key(ticker)
    incoming_entities = {e.lower() for e in (entities or []) if e}
    with _log_lock:
        entries = _load_log()
    for e in entries:
        if e.get("outcome") in (None, "rejected"):
            continue
        if not _is_today_utc(e.get("timestamp", ""), today):
            continue
        existing_ticker = e.get("ticker", "")
        if existing_ticker == ticker:
            return f"already bet on {ticker} today"
        if _event_key(existing_ticker) == event_key:
            return f"same event as {existing_ticker} (already bet today)"
        existing_entities = {x.lower() for x in (e.get("entities") or []) if x}
        overlap = incoming_entities & existing_entities
        if overlap:
            return (
                f"overlaps {existing_ticker} on {sorted(overlap)[0]} "
                "(already bet today)"
            )
    return None


# ─── Kelly sizing ──────────────────────────────────────────────────────

def _half_kelly_pct(true_prob: float, price_cents: float) -> float:
    """Half-Kelly fraction of bankroll for a BUY at price `price_cents`
    with win probability `true_prob` on the side being bought.

    For a contract bought at P cents: pay $P to win $(100 - P), so
    b = (100-P)/P. Half-Kelly halves the raw Kelly to absorb true-prob
    estimation error. Side-neutral — callers pass the side-specific
    price and the probability of THAT side resolving.
    """
    p = max(0.001, min(0.999, true_prob))
    yes_p = max(1.0, min(99.0, price_cents))
    b = (100 - yes_p) / yes_p
    q = 1 - p
    if b <= 0:
        return 0.0
    raw = (b * p - q) / b
    if raw <= 0:
        return 0.0
    return min(raw / 2.0, MAX_BET_PCT)


def _size_trade(item: dict[str, Any]) -> tuple[int, float, float, str | None]:
    """Returns (contracts, bet_size_usd, kelly_pct, reject_reason).
    reject_reason is None on approval.

    Side-aware: for BUY_NO, our win probability is (1 - true_probability)
    and the price we pay per contract is the NO ask (passed in
    `price_for_order_cents`), not the YES ask. The Kelly math is
    symmetric between sides — every contract pays $1 on a win.
    """
    side = item.get("side", "yes")
    true_prob = float(item.get("true_probability", 0))
    price_cents = float(
        item.get("price_for_order_cents")
        or (item.get("no_ask") if side == "no" else item.get("yes_ask"))
        or 50
    )
    effective_prob = (1.0 - true_prob) if side == "no" else true_prob
    kelly_pct = _half_kelly_pct(effective_prob, price_cents)
    bet_size = BANKROLL * kelly_pct
    contracts = int(bet_size / (price_cents / 100)) if price_cents > 0 else 0

    if kelly_pct <= 0:
        return contracts, bet_size, kelly_pct, "Kelly <= 0"
    if contracts < MIN_CONTRACTS or bet_size < MIN_BET_USD:
        return contracts, bet_size, kelly_pct, (
            f"below mins: {contracts}c @ {_format_usd(bet_size)} "
            f"(min {MIN_CONTRACTS}c / {_format_usd(MIN_BET_USD)})"
        )
    if _daily_spent + bet_size > BANKROLL * MAX_DAILY_LOSS:
        return contracts, bet_size, kelly_pct, (
            f"would exceed daily cap "
            f"({_format_usd(_daily_spent)} + {_format_usd(bet_size)} "
            f"> {MAX_DAILY_LOSS:.0%} bankroll)"
        )
    if _weekly_state.get("paused"):
        return contracts, bet_size, kelly_pct, (
            f"weekly drawdown lockout active "
            f"(pnl={_format_usd(_weekly_state['pnl'])} since "
            f"{_weekly_state['week_start']})"
        )
    return contracts, bet_size, kelly_pct, None


# ─── Order placement ───────────────────────────────────────────────────

def _place_order(ticker: str, side: str, contracts: int,
                 price_cents: int) -> tuple[bool, str | None, dict[str, Any] | None]:
    """Returns (placed, error, kalshi_order_dict)."""
    if _is_paper_for_ticker(ticker):
        return True, None, {"paper": True}

    path = "/trade-api/v2/portfolio/orders"
    payload = {
        "action": "buy",
        "client_order_id": f"pp_{ticker}_{int(time.time())}",
        "count": contracts,
        "side": side,
        "ticker": ticker,
        "type": "limit",
    }
    if side == "yes":
        payload["yes_price"] = price_cents
    else:
        payload["no_price"] = price_cents

    try:
        r = requests.post(
            f"{KALSHI_BASE_URL}/portfolio/orders",
            headers=get_auth_headers("POST", path),
            json=payload,
            timeout=15,
        )
    except requests.exceptions.RequestException as e:
        return False, f"network: {e}", None

    if r.status_code >= 400:
        return False, f"HTTP {r.status_code}: {r.text[:300]}", None
    try:
        body = r.json()
    except ValueError as e:
        return False, f"non-JSON: {e}", None
    order = body.get("order")
    if not order:
        return False, f"no order in body (keys={list(body.keys())})", None
    return True, None, order


# ─── Settlement / outcome update ───────────────────────────────────────

def _fetch_market_state(ticker: str) -> dict[str, Any] | None:
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        r = requests.get(
            f"{KALSHI_BASE_URL}/markets/{ticker}",
            headers=get_auth_headers("GET", path),
            timeout=15,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json().get("market")
    except Exception as e:
        print(f"[WARN] market state fetch failed for {ticker}: {e}", flush=True)
        return None


def _resolved_outcome(market: dict[str, Any]) -> str | None:
    """Return 'yes' / 'no' if the market is fully resolved, else None.

    Kalshi exposes `status` and `result` on /markets/{ticker}. status of
    "finalized" / "settled" combined with result in {"yes", "no"} is the
    canonical signal; we also accept a yes/no price pinned to 100/0.
    """
    status = (market.get("status") or "").lower()
    result = (market.get("result") or "").lower()
    if status in ("finalized", "settled") and result in ("yes", "no"):
        return result
    # Fallback: a closed market with prices pinned to extremes
    yes_dollars = market.get("yes_ask_dollars")
    if status in ("closed", "finalized", "settled") and isinstance(yes_dollars, (int, float)):
        if yes_dollars >= 0.99:
            return "yes"
        if yes_dollars <= 0.01:
            return "no"
    return None


def _compute_pnl(entry: dict[str, Any], winning_side: str) -> float:
    """Realized PnL in dollars for the entry given the resolved side.

    For BUY_YES at P cents with N contracts and bet_size = N * P / 100:
      - YES wins:  payout = N * $1.00,  pnl = N - bet_size = N * (1 - P/100)
      - NO wins:   payout = 0,          pnl = -bet_size
    """
    side = entry.get("side", "yes")
    contracts = int(entry.get("contracts", 0))
    bet_size = float(entry.get("bet_size", 0))
    won = (side == winning_side)
    if won:
        return round(contracts * 1.00 - bet_size, 2)
    return round(-bet_size, 2)


_SERIES_PREFIXES = (
    "KXMLBGAME", "KXMLBTOTAL", "KXMLBSPREAD", "KXMLBTEAMTOTAL",
    "KXATPMATCH", "KXWTAMATCH", "KXBTC", "KXAAAGASD",
    "KXNHL", "KXNBA",
)


def _series_of_ticker(ticker: str) -> str:
    """Map a Kalshi ticker to its series prefix for calibration bucketing.
    Returns 'other' for anything not in _SERIES_PREFIXES."""
    for p in _SERIES_PREFIXES:
        if ticker.startswith(p):
            return p
    return "other"


def _update_calibration_running(trades: list[dict]) -> None:
    """Recompute per-series + per-confidence calibration stats from every
    settled trade and persist to /app/data/calibration_running.json.

    Stats per bucket:
      n          — settled trade count
      wins       — count won
      win_rate   — wins / n
      mean_pred  — average of our_prob (the model's claimed probability)
      cal_err_pp — (mean_pred - win_rate) * 100, the calibration gap in
                   percentage points; positive = overconfident
      brier      — mean of (our_prob - outcome)^2 where outcome ∈ {0,1};
                   lower is better; random=0.25
      pnl        — sum of realized PnL

    Recompute-from-scratch (vs incremental update) keeps the file
    correct even if late-arriving outcomes shuffle the trade list.
    Cheap: O(n) once per settlement sweep."""
    def _stats(rows: list[dict]) -> dict | None:
        usable = [
            t for t in rows
            if isinstance(t.get("our_prob"), (int, float))
            and t.get("outcome") in ("won", "lost")
        ]
        if not usable:
            return None
        n = len(usable)
        wins = sum(1 for t in usable if t["outcome"] == "won")
        sum_pred = sum(float(t["our_prob"]) for t in usable)
        sum_brier = sum(
            (float(t["our_prob"]) - (1.0 if t["outcome"] == "won" else 0.0)) ** 2
            for t in usable
        )
        pnl = sum(float(t.get("pnl") or 0) for t in usable)
        mean_pred = sum_pred / n
        win_rate = wins / n
        return {
            "n": n,
            "wins": wins,
            "win_rate": round(win_rate, 4),
            "mean_pred": round(mean_pred, 4),
            "cal_err_pp": round((mean_pred - win_rate) * 100, 2),
            "brier": round(sum_brier / n, 4),
            "pnl": round(pnl, 2),
        }

    settled = [t for t in trades if t.get("outcome") in ("won", "lost")]
    if not settled:
        return

    by_series: dict[str, list[dict]] = {}
    for t in settled:
        s = _series_of_ticker(t.get("ticker", ""))
        by_series.setdefault(s, []).append(t)
    by_conf: dict[str, list[dict]] = {}
    for t in settled:
        c = t.get("confidence", "?") or "?"
        by_conf.setdefault(c, []).append(t)

    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "overall": _stats(settled),
        "by_series": {k: _stats(v) for k, v in by_series.items()},
        "by_confidence": {k: _stats(v) for k, v in by_conf.items()},
    }
    try:
        CALIBRATION_RUNNING_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = CALIBRATION_RUNNING_PATH.with_suffix(".tmp")
        with tmp.open("w") as f:
            json.dump(payload, f, indent=2)
        tmp.replace(CALIBRATION_RUNNING_PATH)
    except Exception as e:
        print(
            f"[WARN] calibration_running write failed: {e}",
            flush=True,
        )


def _fetch_actual_total_runs(ticker: str) -> int | None:
    """Final combined runs (away + home) for the KXMLBTOTAL ticker's
    game. Returns None if the ticker isn't parseable, the schedule
    fetch fails, no game matches, or the game isn't yet final.

    Ticker format: `KXMLBTOTAL-<YYMONDDHHMM><AWAY><HOME>-N`. Team
    abbreviations are variable length (SF, KC, TB are 2-char) so we
    enumerate every game on the date and pick the one whose
    `away+home` concat suffixes the ticker's middle segment — the same
    matching trick scout_buy_no.py uses."""
    if not ticker.startswith("KXMLBTOTAL-"):
        return None
    parts = ticker.split("-")
    if len(parts) < 3:
        return None
    middle = parts[1]
    if len(middle) < 7:
        return None
    date_token, tail_segment = middle[:7], middle[7:]
    try:
        date_iso = datetime.strptime(date_token, "%y%b%d").strftime("%Y-%m-%d")
    except ValueError:
        return None
    try:
        r = requests.get(
            f"{_MLB_STATSAPI}/schedule",
            params={"sportId": 1, "date": date_iso},
            timeout=15,
        )
        r.raise_for_status()
        sched = r.json()
    except (requests.RequestException, ValueError) as e:
        print(
            f"[WARN] accuracy: schedule fetch failed for {ticker} "
            f"({date_iso}): {e}",
            flush=True,
        )
        return None
    for d in (sched or {}).get("dates", []) or []:
        for g in d.get("games", []) or []:
            # Final / Final-Tie / Final-Replay all imply scores are set.
            # Anything else (Scheduled, In Progress, Postponed) is too
            # early — caller will retry on the next settlement sweep.
            state = (g.get("status") or {}).get("codedGameState", "")
            if state not in ("F", "FT", "FR"):
                continue
            teams = g.get("teams") or {}
            a_blob = teams.get("away") or {}
            h_blob = teams.get("home") or {}
            a_abbr = (a_blob.get("team") or {}).get("abbreviation", "")
            h_abbr = (h_blob.get("team") or {}).get("abbreviation", "")
            if not (a_abbr and h_abbr):
                continue
            if not tail_segment.endswith(a_abbr + h_abbr):
                continue
            a_score = a_blob.get("score")
            h_score = h_blob.get("score")
            if not isinstance(a_score, int) or not isinstance(h_score, int):
                continue
            return a_score + h_score
    return None


def _record_prediction_accuracy(entry: dict[str, Any]) -> None:
    """For settled KXMLBTOTAL trades, extract projected_total from the
    placement reasoning, fetch the actual game total from MLB statsapi,
    and persist {projected, actual, error, direction, settled_at}
    keyed by ticker in PREDICTION_ACCURACY_PATH.

    direction is 'CORRECT' if |error| ≤ ACCURACY_TOLERANCE_RUNS, else
    'OVER' (projected > actual — model overestimated runs) or 'UNDER'
    (projected < actual). All failure modes (no projection in
    reasoning, schedule fetch failed, no matching final game) silently
    skip the record — this is a passive observability hook, not a
    blocker for the settlement loop."""
    ticker = entry.get("ticker", "")
    if not ticker.startswith("KXMLBTOTAL"):
        return
    reasoning = entry.get("reasoning") or ""
    m = _PROJECTED_RUNS_RE.search(reasoning)
    if not m:
        return
    try:
        projected = float(m.group(1))
    except ValueError:
        return
    actual = _fetch_actual_total_runs(ticker)
    if actual is None:
        return
    error = round(projected - actual, 2)
    if abs(error) <= ACCURACY_TOLERANCE_RUNS:
        direction = "CORRECT"
    elif error > 0:
        direction = "OVER"
    else:
        direction = "UNDER"
    settled_at = entry.get("settled_at") or datetime.now(timezone.utc).isoformat()
    record = {
        "projected": projected,
        "actual": actual,
        "error": error,
        "direction": direction,
        "settled_at": settled_at,
    }
    print(
        f"[ACCURACY] {ticker} projected={projected} actual={actual} "
        f"error={error:+} direction={direction}",
        flush=True,
    )
    try:
        store: dict[str, dict] = {}
        if PREDICTION_ACCURACY_PATH.exists():
            with PREDICTION_ACCURACY_PATH.open() as f:
                store = json.load(f) or {}
        store[ticker] = record
        PREDICTION_ACCURACY_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = PREDICTION_ACCURACY_PATH.with_suffix(".tmp")
        with tmp.open("w") as f:
            json.dump(store, f, indent=2)
        tmp.replace(PREDICTION_ACCURACY_PATH)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[WARN] prediction_accuracy write failed: {e}", flush=True)


def _check_settlements() -> None:
    with _log_lock:
        entries = _load_log()
        pending_idx = [i for i, e in enumerate(entries) if e.get("outcome") == "pending"]

    if not pending_idx:
        return

    print(f"[trader] checking {len(pending_idx)} pending trades for settlement...", flush=True)
    updated = 0
    for i in pending_idx:
        entry = entries[i]
        ticker = entry.get("ticker", "")
        if not ticker:
            continue
        market = _fetch_market_state(ticker)
        if not market:
            continue
        resolved = _resolved_outcome(market)
        if resolved is None:
            continue
        side = entry.get("side", "yes")
        won = resolved == side
        entry["outcome"] = "won" if won else "lost"
        entry["pnl"] = _compute_pnl(entry, resolved)
        entry["settled_at"] = datetime.now(timezone.utc).isoformat()
        entry["resolved_side"] = resolved
        # Roll this settlement into the weekly P&L tracker. We only
        # count trades placed THIS week — trades from a prior week
        # that settled today carry their pnl to that week's stats,
        # not this week's drawdown calculation.
        ts_str = entry.get("timestamp", "")
        try:
            placed_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            placed_monday = (placed_dt.date()
                             - timedelta(days=placed_dt.weekday())).isoformat()
        except (ValueError, AttributeError):
            placed_monday = ""
        if placed_monday == _weekly_state["week_start"]:
            _weekly_state["pnl"] += float(entry["pnl"])
            _save_weekly_state()
            if _check_weekly_drawdown():
                print(
                    f"[WEEKLY-PAUSE] triggered: pnl="
                    f"{_format_usd(_weekly_state['pnl'])} "
                    f"<= -{WEEKLY_DRAWDOWN_PCT:.0%}*bankroll "
                    f"({_format_usd(-BANKROLL * WEEKLY_DRAWDOWN_PCT)}); "
                    f"trades paused until "
                    f"{(_current_week_monday() + timedelta(days=7)).isoformat()} "
                    f"00:00 UTC",
                    flush=True,
                )
                send_discord(_weekly_pause_embed())
        # Per-trade Brier: (our_prob − outcome)^2, outcome ∈ {0, 1}.
        # Lower is better; random = 0.25. Logged immediately so settlement-
        # time calibration is visible without parsing the JSON file.
        our_prob = entry.get("our_prob")
        if isinstance(our_prob, (int, float)):
            brier = (float(our_prob) - (1.0 if won else 0.0)) ** 2
            entry["brier"] = round(brier, 4)
            print(
                f"[BRIER] {ticker} our_prob={our_prob:.2f} outcome="
                f"{'WON' if won else 'LOST'} brier={brier:.3f} "
                f"conf={entry.get('confidence', '?')}",
                flush=True,
            )
        # Passive accuracy tracker for KXMLBTOTAL: compare the
        # projected_total Claude wrote in reasoning to the actual
        # final score. Skipped silently for other series.
        try:
            _record_prediction_accuracy(entry)
        except Exception as e:
            print(
                f"[WARN] prediction_accuracy unexpected error for "
                f"{ticker}: {type(e).__name__}: {e}",
                flush=True,
            )
        updated += 1
        send_discord(_settlement_embed(entry))
        # Tiny pause to be polite to the API
        time.sleep(0.5)

    if updated:
        with _log_lock:
            # Re-read in case append_trade ran since we loaded — and overlay
            # our updates by ticker+timestamp identity.
            current = _load_log()
            for i in pending_idx:
                e = entries[i]
                key = (e.get("ticker"), e.get("timestamp"))
                for j, c in enumerate(current):
                    if (c.get("ticker"), c.get("timestamp")) == key:
                        current[j] = e
                        break
            _save_log(current)
        # Recompute and persist running calibration from all settled
        # trades — per-series + per-confidence Brier, calibration error,
        # win-rate, P&L. Read by scripts/calibration_live.py and any
        # downstream dashboards.
        _update_calibration_running(current)
        print(f"[trader] settled {updated} trades", flush=True)


# ─── Discord ───────────────────────────────────────────────────────────

def _placement_embed(entry: dict[str, Any], *, placed: bool, error: str | None) -> dict[str, Any]:
    ticker = entry.get("ticker", "")
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    # Per-trade paper flag is set on the entry by _is_paper_for_ticker
    # at placement time — read it back here so the Discord embed shows
    # the correct mode even when PAPER_SERIES is mid-flip.
    if entry.get("paper"):
        mode_str, color = "📄 PAPER", 0xFFAA00
    elif placed:
        mode_str, color = "✅ LIVE — PLACED", 0x2ECC71
    else:
        mode_str, color = "❌ LIVE — FAILED", 0xE74C3C
    fields = [
        {"name": "Status", "value": mode_str, "inline": True},
        {"name": "Side", "value": "🟢 YES" if entry.get("side") == "yes" else "🔴 NO", "inline": True},
        {"name": "Contracts", "value": str(entry.get("contracts", 0)), "inline": True},
        {"name": "Market Price", "value": f"{entry.get('market_price_cents', '?')}¢", "inline": True},
        {"name": "Bet Size", "value": _format_usd(float(entry.get("bet_size", 0))), "inline": True},
        {"name": "Edge", "value": f"{entry.get('edge', 0)*100:+.1f}%", "inline": True},
        {"name": "Our Prob", "value": f"{entry.get('our_prob', 0):.1%}", "inline": True},
        {"name": "Kelly %", "value": f"{entry.get('kelly_pct', 0):.2%}", "inline": True},
        {"name": "Daily Spent", "value": _format_usd(_daily_spent), "inline": True},
    ]
    if error:
        fields.append({"name": "Error", "value": error[:500], "inline": False})
    fields.append({"name": "Market", "value": f"[View on Kalshi]({market_url})", "inline": False})
    return {
        "title": f"⚡ KALSHI TRADER — {entry.get('title', ticker)[:80]}",
        "url": market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Trader  •  {'PAPER' if entry.get('paper') else 'LIVE'}  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _settlement_embed(entry: dict[str, Any]) -> dict[str, Any]:
    ticker = entry.get("ticker", "")
    market_url = f"https://kalshi.com/markets/{ticker}"
    pnl = float(entry.get("pnl", 0))
    won = entry.get("outcome") == "won"
    color = 0x2ECC71 if won else 0xE74C3C
    return {
        "title": f"📋 KALSHI SETTLED — {entry.get('title', ticker)[:80]}",
        "url": market_url,
        "color": color,
        "fields": [
            {"name": "Result", "value": "✅ WIN" if won else "❌ LOSS", "inline": True},
            {"name": "PnL", "value": _format_usd(pnl), "inline": True},
            {"name": "Resolved", "value": entry.get("resolved_side", "?").upper(), "inline": True},
            {"name": "Side Held", "value": entry.get("side", "?").upper(), "inline": True},
            {"name": "Contracts", "value": str(entry.get("contracts", 0)), "inline": True},
            {"name": "Bet Size", "value": _format_usd(float(entry.get("bet_size", 0))), "inline": True},
            {"name": "Market", "value": f"[View on Kalshi]({market_url})", "inline": False},
        ],
        "footer": {"text": f"PassivePoly Trader  •  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_discord(embed: dict[str, Any]) -> None:
    if not WEBHOOK_KALSHI_TRADER:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_TRADER, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_TRADER, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Discord send failed: {e}")


# ─── Main loop ─────────────────────────────────────────────────────────

def run() -> None:
    global _daily_spent, _last_reset, _weekly_state
    _last_reset, _daily_spent = _load_daily_spent()
    _weekly_state = _load_weekly_state()
    mode = "PAPER TRADING" if PAPER_TRADING else "LIVE TRADING"
    print(
        f"Kalshi Trader Agent starting — {mode}, bankroll={_format_usd(BANKROLL)}, "
        f"max_bet={MAX_BET_PCT:.0%}, interval={CHECK_INTERVAL}s, "
        f"daily_spent={_format_usd(_daily_spent)} (restored from {DAILY_SPENT_PATH.name})"
    )
    # Per-series paper override visibility. If PAPER_TRADING is global
    # then PAPER_SERIES is moot — log so the operator doesn't think
    # the per-series list is somehow being honored when it isn't.
    if PAPER_TRADING:
        print(
            f"[trader] paper-series: global PAPER_TRADING=true → "
            f"all trades paper regardless of list "
            f"(list={list(PAPER_SERIES) or '(empty)'})",
            flush=True,
        )
    else:
        print(
            f"[trader] paper-series: PAPER_TRADING=false (live) BUT "
            f"these series stay paper: {list(PAPER_SERIES) or '(none)'}",
            flush=True,
        )
    # Weekly state startup banner. Surfaces a mid-cooldown lockout
    # immediately on restart so the user doesn't think the trader is
    # broken when it's just respecting the drawdown rule.
    print(
        f"[trader] weekly: week_start={_weekly_state['week_start']} "
        f"pnl={_format_usd(_weekly_state['pnl'])} "
        f"spent={_format_usd(_weekly_state['spent'])} "
        f"paused={_weekly_state['paused']} "
        f"drawdown_threshold="
        f"{_format_usd(-BANKROLL * WEEKLY_DRAWDOWN_PCT)}",
        flush=True,
    )
    # Per-series daily caps: visible at startup so a Railway env flip
    # like KALSHI_KXMLBTOTAL_DAILY_CAP=3 is immediately verifiable
    # from the logs instead of requiring an SSH into the container to
    # read os.environ. Empty dict prints as `{}` — no caps configured.
    print(
        f"[trader] series daily caps: {SERIES_DAILY_CAPS}",
        flush=True,
    )
    last_settlement_check = 0.0

    while True:
        cycle_start = time.time()
        _check_daily_reset()
        _check_weekly_reset()

        try:
            items = kalshi_queue.drain_fresh("risk")
            # Highest-edge first: the daily-loss cap and per-side budgets
            # bind mid-cycle, so without this ordering a $5 trade with +0.05
            # edge could eat the budget that a +0.20 edge trade needs. Sort
            # is stable, so ties fall back to enqueue order.
            items = sorted(
                items,
                key=lambda it: float(it.get("edge") or 0),
                reverse=True,
            )
            executed = failed = rejected = paper_count = live_count = 0
            for item in items:
                try:
                    ticker = item.get("ticker", "")
                    side = item.get("side", "yes")
                    # Side-aware order price. BUY_YES pays yes_ask; BUY_NO
                    # pays the NO ask carried as `price_for_order_cents`
                    # (set by kalshi_edge from no_ask_cents). yes_ask is
                    # kept on the payload for context but is not what we
                    # pay on a BUY_NO.
                    yes_cents = int(item.get("yes_ask", 0))
                    price_cents = int(
                        item.get("price_for_order_cents")
                        or (item.get("no_ask") if side == "no" else yes_cents)
                        or 0
                    )
                    if not (
                        ticker
                        and side in ("yes", "no")
                        and 1 <= price_cents <= 99
                    ):
                        print(f"[trader] malformed item, skipping: {item}", flush=True)
                        failed += 1
                        continue

                    contracts, bet_size, kelly_pct, reject = _size_trade(item)
                    if reject:
                        print(f"[trader] reject {ticker}: {reject}", flush=True)
                        rejected += 1
                        continue

                    correlation_reject = _correlation_collision(
                        ticker, item.get("entities") or []
                    )
                    if correlation_reject:
                        print(
                            f"[trader] reject {ticker}: correlation — "
                            f"{correlation_reject}",
                            flush=True,
                        )
                        rejected += 1
                        continue

                    series_cap_reject = _series_daily_cap_collision(ticker)
                    if series_cap_reject:
                        print(
                            f"[CORR-GUARD] {ticker} skip: {series_cap_reject}",
                            flush=True,
                        )
                        rejected += 1
                        continue

                    placed, error, order = _place_order(ticker, side, contracts, price_cents)
                    if placed and side == "no":
                        print(
                            f"[BUY-NO] {ticker} contracts={contracts} "
                            f"no_price={price_cents}c bet=${bet_size:.2f}",
                            flush=True,
                        )
                    timestamp = datetime.now(timezone.utc).isoformat()

                    _our_prob = round(float(item.get("true_probability", 0)), 4)
                    # `market_price` and `market_price_cents` record what
                    # we actually paid per contract on the side we bought
                    # (side-specific). `yes_ask` keeps the YES-side
                    # snapshot for downstream context regardless of side.
                    _mkt_price = round(price_cents / 100, 4)
                    _yes_ask_snapshot = round(yes_cents / 100, 4) if yes_cents else _mkt_price
                    entry = {
                        "ticker": ticker,
                        "title": item.get("title", ""),
                        "our_prob": _our_prob,
                        # Aliases for downstream calibration tooling that
                        # expects the canonical field names. Identical
                        # values; kept alongside our_prob / market_price
                        # so existing dashboards don't break.
                        "true_probability": _our_prob,
                        "market_price": _mkt_price,
                        "yes_ask": _yes_ask_snapshot,
                        "market_price_cents": price_cents,
                        "edge": round(float(item.get("edge", 0)), 4),
                        "confidence": item.get("confidence", "?"),
                        "reasoning": item.get("reasoning", ""),
                        "side": side,
                        "contracts": contracts,
                        "bet_size": round(bet_size, 2),
                        "kelly_pct": round(kelly_pct, 4),
                        "timestamp": timestamp,
                        "paper": _is_paper_for_ticker(ticker),
                        "kalshi_order_id": (order or {}).get("order_id") if order else None,
                        "outcome": "pending" if placed else "rejected",
                        "pnl": 0.0,
                        "placement_error": error,
                        # Persisted for next-cycle correlation checks.
                        "entities": item.get("entities") or [],
                    }
                    _append_trade(entry)
                    send_discord(_placement_embed(entry, placed=placed, error=error))
                    if placed:
                        _daily_spent += bet_size
                        _save_daily_spent()
                        _weekly_state["spent"] += float(bet_size)
                        _save_weekly_state()
                        executed += 1
                        if entry.get("paper"):
                            paper_count += 1
                        else:
                            live_count += 1
                    else:
                        failed += 1
                except Exception as e:
                    print(f"[WARN] trader item crashed: {e}", flush=True)
                    failed += 1

            if items:
                # Split paper/live counts so a mixed-mode session
                # (PAPER_TRADING=false + PAPER_SERIES non-empty) is
                # legible at a glance instead of all being lumped as
                # one verb.
                print(
                    f"[trader] cycle: executed={executed} "
                    f"(paper={paper_count}, live={live_count}), "
                    f"rejected={rejected}, failed={failed}, "
                    f"daily_spent={_format_usd(_daily_spent)}",
                    flush=True,
                )

            # Hourly settlement sweep
            if time.time() - last_settlement_check >= SETTLEMENT_INTERVAL:
                _check_settlements()
                last_settlement_check = time.time()
        except Exception as e:
            print(f"[WARN] trader cycle crashed: {e}", flush=True)

        elapsed = time.time() - cycle_start
        time.sleep(max(0, CHECK_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
