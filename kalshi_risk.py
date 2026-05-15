"""Kalshi risk agent — Kelly sizing + daily loss caps.

Reads prediction_queue. For each market with a positive edge:
  - Compute the half-Kelly bet fraction
  - Cap at MAX_BET_PCT of bankroll
  - Reject if it would push today's spend over MAX_DAILY_LOSS * bankroll
  - Convert dollar bet to contract count at the asking price
Approved trades go to risk_queue. Rejected ones are alerted but dropped.

Daily state — `_daily_spent`, `_last_reset` — is module-level. Resets at
UTC midnight. If the launcher restarts mid-day, the spent counter resets
to 0 (consistent with execution.py's existing behavior).
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import requests

import kalshi_queue

WEBHOOK_KALSHI_RISK = os.getenv("WEBHOOK_KALSHI_RISK", "")
CHECK_INTERVAL = int(os.getenv("KALSHI_RISK_INTERVAL", "60"))
BANKROLL = float(os.getenv("BANKROLL", "500"))
MAX_BET_PCT = float(os.getenv("MAX_BET_PCT", "0.05"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "0.10"))
MIN_CONTRACTS = int(os.getenv("KALSHI_MIN_CONTRACTS", "1"))
MIN_BET_USD = float(os.getenv("KALSHI_MIN_BET_USD", "5"))

# Daily spend tracking (USD).
_daily_spent = 0.0
_last_reset = datetime.now(timezone.utc).date()


def _half_kelly(true_prob: float, yes_price_cents: float, recommendation: str) -> float:
    """Returns the half-Kelly fraction of bankroll to wager. 0 if no bet.

    For BUY_YES at price P (in cents):
      pay $P to win $(100-P) → b = (100-P)/P, p = true_prob, q = 1-p
    For BUY_NO at price P (i.e. buy YES at 100-P):
      pay $(100-P) to win $P → b = P/(100-P), p = 1-true_prob, q = true_prob

    Half-Kelly halves the raw Kelly fraction — safer in the presence of
    estimation error in `true_prob` (which is exactly the regime we're in).
    """
    p = max(0.001, min(0.999, true_prob))
    yes_p = max(1.0, min(99.0, yes_price_cents))  # avoid divide-by-zero

    if recommendation == "BUY_YES":
        b = (100 - yes_p) / yes_p
        bet_prob = p
    elif recommendation == "BUY_NO":
        b = yes_p / (100 - yes_p)
        bet_prob = 1 - p
    else:
        return 0.0

    q = 1 - bet_prob
    if b <= 0:
        return 0.0
    kelly = (b * bet_prob - q) / b
    if kelly <= 0:
        return 0.0
    return min(kelly / 2.0, MAX_BET_PCT)


def _check_daily_reset() -> None:
    """Reset _daily_spent if the UTC date has rolled over since last cycle."""
    global _daily_spent, _last_reset
    today = datetime.now(timezone.utc).date()
    if today > _last_reset:
        _daily_spent = 0.0
        _last_reset = today
        print("[risk] daily spend reset")


def _format_usd(amount: float) -> str:
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:.2f}"


def _build_embed(item: dict[str, Any], approved: bool, *, contracts: int,
                 price_for_order: int, bet_size: float, kelly_pct: float,
                 reason: str) -> dict[str, Any]:
    title = item.get("title", item.get("ticker", "?"))
    ticker = item.get("ticker", "")
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    rec = item.get("recommendation", "?")
    side_str = "🟢 YES" if rec == "BUY_YES" else ("🔴 NO" if rec == "BUY_NO" else "❓")
    status_str = "✅ APPROVED" if approved else "❌ BLOCKED"
    color = 0x2ECC71 if approved else 0xE74C3C

    return {
        "title": f"⚖️ KALSHI RISK — {title[:80]}",
        "url": market_url,
        "color": color,
        "fields": [
            {"name": "Status", "value": status_str, "inline": True},
            {"name": "Side", "value": side_str, "inline": True},
            {"name": "Edge", "value": f"{item.get('edge', 0)*100:+.1f}%", "inline": True},
            {"name": "Half-Kelly %", "value": f"{kelly_pct:.2%}", "inline": True},
            {"name": "Bet Size", "value": _format_usd(bet_size), "inline": True},
            {"name": "Contracts", "value": str(contracts), "inline": True},
            {"name": "Order Price", "value": f"{price_for_order}¢", "inline": True},
            {"name": "Bankroll", "value": _format_usd(BANKROLL), "inline": True},
            {"name": "Daily Spent", "value": _format_usd(_daily_spent), "inline": True},
            {"name": "Reason", "value": reason, "inline": False},
            {"name": "🔗 Market", "value": f"[View on Kalshi]({market_url})", "inline": False},
        ],
        "footer": {"text": f"PassivePoly Kalshi Risk  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_discord(embed: dict[str, Any]) -> None:
    if not WEBHOOK_KALSHI_RISK:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_RISK, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_RISK, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Discord send failed: {e}")


def run() -> None:
    global _daily_spent
    print(f"Kalshi Risk Agent starting — bankroll={_format_usd(BANKROLL)}, "
          f"max_bet={MAX_BET_PCT:.0%}, max_daily_loss={MAX_DAILY_LOSS:.0%}")
    while True:
        cycle_start = time.time()
        _check_daily_reset()
        items = kalshi_queue.drain_fresh("prediction")

        if items:
            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                  f"Sizing {len(items)} markets from prediction queue...")

        for item in items:
            try:
                rec = item.get("recommendation", "SKIP")
                true_prob = float(item.get("true_probability", 0))
                yes_ask = float(item.get("yes_ask", 50))

                kelly_pct = _half_kelly(true_prob, yes_ask, rec)
                bet_size = BANKROLL * kelly_pct

                if rec == "BUY_YES":
                    price_for_order = int(yes_ask)
                    side = "yes"
                elif rec == "BUY_NO":
                    price_for_order = int(100 - yes_ask)
                    side = "no"
                else:
                    send_discord(_build_embed(
                        item, approved=False, contracts=0, price_for_order=0,
                        bet_size=0, kelly_pct=0, reason="SKIP recommendation",
                    ))
                    continue

                contracts = int(bet_size / (price_for_order / 100)) if price_for_order > 0 else 0

                # Block conditions
                if kelly_pct <= 0:
                    reason = f"Kelly returned ≤ 0 (edge {item.get('edge', 0)*100:+.1f}%)"
                    send_discord(_build_embed(item, False, contracts=0,
                                              price_for_order=price_for_order,
                                              bet_size=0, kelly_pct=0, reason=reason))
                    continue
                if contracts < MIN_CONTRACTS or bet_size < MIN_BET_USD:
                    reason = (f"Below minimums: {contracts}c @ {_format_usd(bet_size)} "
                              f"(min {MIN_CONTRACTS}c / {_format_usd(MIN_BET_USD)})")
                    send_discord(_build_embed(item, False, contracts=contracts,
                                              price_for_order=price_for_order,
                                              bet_size=bet_size, kelly_pct=kelly_pct,
                                              reason=reason))
                    continue
                if _daily_spent + bet_size > BANKROLL * MAX_DAILY_LOSS:
                    reason = (f"Would exceed daily loss cap "
                              f"({_format_usd(_daily_spent)} + {_format_usd(bet_size)} "
                              f"> {MAX_DAILY_LOSS:.0%} of bankroll)")
                    send_discord(_build_embed(item, False, contracts=contracts,
                                              price_for_order=price_for_order,
                                              bet_size=bet_size, kelly_pct=kelly_pct,
                                              reason=reason))
                    continue

                # Approve — push to execution
                approved = {
                    **item,
                    "side": side,
                    "price_for_order_cents": price_for_order,
                    "contracts": contracts,
                    "bet_size_usd": round(bet_size, 2),
                    "kelly_pct": round(kelly_pct, 4),
                    "approved_at": datetime.now(timezone.utc).isoformat(),
                }
                kalshi_queue.enqueue("risk", item["ticker"], approved)
                _daily_spent += bet_size  # reserve allocation — execution may still fail

                reason = (f"Half-Kelly {kelly_pct:.2%}, edge "
                          f"{item.get('edge', 0)*100:+.1f}%, "
                          f"{contracts} contracts @ {price_for_order}¢")
                send_discord(_build_embed(item, True, contracts=contracts,
                                          price_for_order=price_for_order,
                                          bet_size=bet_size, kelly_pct=kelly_pct,
                                          reason=reason))
            except Exception as e:
                print(f"[WARN] Risk sizing failed for {item.get('ticker', '?')}: {e}")

        elapsed = time.time() - cycle_start
        if items:
            print(f"  Done in {elapsed:.1f}s — daily_spent now {_format_usd(_daily_spent)}.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
