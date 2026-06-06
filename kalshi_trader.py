"""Kalshi trader — sizes, places, and tracks every approved trade.

Pulls approved BUY_YES trades off kalshi_queue stage "risk" (filled by
kalshi_edge), sizes each with half-Kelly capped at MAX_BET_PCT of
bankroll and the remaining daily-loss budget, then places the order.
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
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

import kalshi_queue
from kalshi_auth import KALSHI_BASE_URL, get_auth_headers

WEBHOOK_KALSHI_TRADER = os.getenv("WEBHOOK_KALSHI_TRADER", "")
PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
CHECK_INTERVAL = int(os.getenv("KALSHI_TRADER_INTERVAL", "300"))   # 5 min
SETTLEMENT_INTERVAL = int(os.getenv("KALSHI_SETTLEMENT_INTERVAL", "3600"))  # 1 h
BANKROLL = float(os.getenv("BANKROLL", "500"))
MAX_BET_PCT = float(os.getenv("MAX_BET_PCT", "0.05"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "0.10"))
MIN_BET_USD = float(os.getenv("KALSHI_MIN_BET_USD", "5"))
MIN_CONTRACTS = int(os.getenv("KALSHI_MIN_CONTRACTS", "1"))
TRADES_LOG_PATH = Path(os.getenv("KALSHI_TRADES_LOG", "/app/data/trades_log.json"))

# Daily-spend tracking. Resets at UTC midnight.
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
        print("[trader] daily spend reset", flush=True)


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

def _half_kelly_pct(true_prob: float, yes_cents: float) -> float:
    """Half-Kelly fraction of bankroll for a BUY_YES at price yes_cents.

    For BUY_YES at P cents: pay $P to win $(100 - P), so b = (100-P)/P.
    Half-Kelly halves the raw Kelly to absorb true-prob estimation error.
    """
    p = max(0.001, min(0.999, true_prob))
    yes_p = max(1.0, min(99.0, yes_cents))
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
    """
    true_prob = float(item.get("true_probability", 0))
    yes_cents = float(item.get("yes_ask", 50))
    kelly_pct = _half_kelly_pct(true_prob, yes_cents)
    bet_size = BANKROLL * kelly_pct
    contracts = int(bet_size / (yes_cents / 100)) if yes_cents > 0 else 0

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
    return contracts, bet_size, kelly_pct, None


# ─── Order placement ───────────────────────────────────────────────────

def _place_order(ticker: str, side: str, contracts: int,
                 price_cents: int) -> tuple[bool, str | None, dict[str, Any] | None]:
    """Returns (placed, error, kalshi_order_dict)."""
    if PAPER_TRADING:
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
        print(f"[trader] settled {updated} trades", flush=True)


# ─── Discord ───────────────────────────────────────────────────────────

def _placement_embed(entry: dict[str, Any], *, placed: bool, error: str | None) -> dict[str, Any]:
    ticker = entry.get("ticker", "")
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if PAPER_TRADING:
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
        "footer": {"text": f"PassivePoly Trader  •  {'PAPER' if PAPER_TRADING else 'LIVE'}  •  {now_str}"},
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
    global _daily_spent
    mode = "PAPER TRADING" if PAPER_TRADING else "LIVE TRADING"
    print(
        f"Kalshi Trader Agent starting — {mode}, bankroll={_format_usd(BANKROLL)}, "
        f"max_bet={MAX_BET_PCT:.0%}, interval={CHECK_INTERVAL}s"
    )
    last_settlement_check = 0.0

    while True:
        cycle_start = time.time()
        _check_daily_reset()

        try:
            items = kalshi_queue.drain_fresh("risk")
            executed = failed = rejected = 0
            for item in items:
                try:
                    ticker = item.get("ticker", "")
                    side = item.get("side", "yes")
                    yes_cents = int(item.get("yes_ask", 0))
                    if not (ticker and side == "yes" and 1 <= yes_cents <= 99):
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

                    placed, error, order = _place_order(ticker, side, contracts, yes_cents)
                    timestamp = datetime.now(timezone.utc).isoformat()

                    entry = {
                        "ticker": ticker,
                        "title": item.get("title", ""),
                        "our_prob": round(float(item.get("true_probability", 0)), 4),
                        "market_price": round(yes_cents / 100, 4),
                        "market_price_cents": yes_cents,
                        "edge": round(float(item.get("edge", 0)), 4),
                        "confidence": item.get("confidence", "?"),
                        "reasoning": item.get("reasoning", ""),
                        "side": side,
                        "contracts": contracts,
                        "bet_size": round(bet_size, 2),
                        "kelly_pct": round(kelly_pct, 4),
                        "timestamp": timestamp,
                        "paper": PAPER_TRADING,
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
                        executed += 1
                    else:
                        failed += 1
                except Exception as e:
                    print(f"[WARN] trader item crashed: {e}", flush=True)
                    failed += 1

            if items:
                verb = "simulated" if PAPER_TRADING else "placed"
                print(
                    f"[trader] cycle: {executed} {verb}, {rejected} rejected, "
                    f"{failed} failed, daily_spent={_format_usd(_daily_spent)}",
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
