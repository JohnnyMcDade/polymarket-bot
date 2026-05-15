"""Kalshi execution agent — places approved orders.

Reads risk_queue (markets already vetted by scanner → research → prediction
→ risk; bet size + side + contract count already computed). For each
approved item, places a Kalshi limit order. In PAPER_TRADING mode the
order is simulated and only logged; in live mode it's a real POST to
/portfolio/orders.

Daily-spent tracking happens in kalshi_risk (it's what gates approvals).
This agent just executes whatever the risk agent approved.

If a paper-mode test goes well and you want to go live:
  1. Verify a handful of paper orders match expectations in Discord
  2. Set PAPER_TRADING=false in Railway env
  3. Redeploy
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import requests

import kalshi_queue
from kalshi_auth import KALSHI_BASE_URL, get_auth_headers

WEBHOOK_KALSHI_EXECUTION = os.getenv("WEBHOOK_KALSHI_EXECUTION", "")
PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
CHECK_INTERVAL = int(os.getenv("KALSHI_EXECUTION_INTERVAL", "300"))


def _format_usd(amount: float) -> str:
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:.2f}"


def place_order(ticker: str, side: str, contracts: int, price_cents: int) -> dict[str, Any] | None:
    """Place a buy order on Kalshi. Returns the order dict on success, None
    on failure. In paper mode never touches the API.
    """
    if PAPER_TRADING:
        return {
            "paper": True,
            "ticker": ticker,
            "side": side,
            "contracts": contracts,
            "price_cents": price_cents,
        }

    path = "/trade-api/v2/portfolio/orders"
    try:
        # When buying YES, the price you pay is yes_ask (cents).
        # When buying NO, you're submitting a YES sell at 100-no_ask which
        # Kalshi's `yes_price` field represents — see API docs.
        yes_price = price_cents if side == "yes" else 100 - price_cents
        payload = {
            "action": "buy",
            "client_order_id": f"pp_{ticker}_{int(time.time())}",
            "count": contracts,
            "side": side,
            "ticker": ticker,
            "type": "limit",
            "yes_price": yes_price,
        }
        r = requests.post(
            f"{KALSHI_BASE_URL}/portfolio/orders",
            headers=get_auth_headers("POST", path),
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("order", {})
    except Exception as e:
        print(f"[WARN] Order failed for {ticker}: {e}")
        return None


def _build_embed(item: dict[str, Any], order: dict[str, Any] | None, *,
                 placed: bool, error: str | None = None) -> dict[str, Any]:
    title = item.get("title", item.get("ticker", "?"))
    ticker = item.get("ticker", "")
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    side = item.get("side", "?")
    side_str = "🟢 YES" if side == "yes" else ("🔴 NO" if side == "no" else "❓")

    if PAPER_TRADING:
        mode = "📄 PAPER"
        color = 0xFFAA00
    elif placed:
        mode = "✅ LIVE — PLACED"
        color = 0x2ECC71
    else:
        mode = "❌ LIVE — FAILED"
        color = 0xE74C3C

    fields = [
        {"name": "Status", "value": mode, "inline": True},
        {"name": "Side", "value": side_str, "inline": True},
        {"name": "Contracts", "value": str(item.get("contracts", 0)), "inline": True},
        {"name": "Price", "value": f"{item.get('price_for_order_cents', '?')}¢", "inline": True},
        {"name": "Bet Size", "value": _format_usd(float(item.get("bet_size_usd", 0))), "inline": True},
        {"name": "Edge", "value": f"{item.get('edge', 0)*100:+.1f}%", "inline": True},
        {"name": "True Prob", "value": f"{item.get('true_probability', 0):.1%}", "inline": True},
        {"name": "Confidence", "value": item.get("confidence", "?"), "inline": True},
    ]

    if order and order.get("order_id"):
        fields.append({"name": "Kalshi Order ID", "value": order["order_id"], "inline": False})
    if error:
        fields.append({"name": "Error", "value": error[:500], "inline": False})

    fields.append({"name": "🔗 Market", "value": f"[View on Kalshi]({market_url})", "inline": False})

    return {
        "title": f"⚡ KALSHI EXECUTION — {title[:80]}",
        "url": market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Execution  •  {'PAPER' if PAPER_TRADING else 'LIVE'}  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_discord(embed: dict[str, Any]) -> None:
    if not WEBHOOK_KALSHI_EXECUTION:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_EXECUTION, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_EXECUTION, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Discord send failed: {e}")


def run() -> None:
    mode = "PAPER TRADING" if PAPER_TRADING else "LIVE TRADING"
    print(f"Kalshi Execution Agent starting — {mode}")
    print(f"  Reads risk_queue, places orders for whatever the risk agent approved.")

    while True:
        cycle_start = time.time()
        items = kalshi_queue.drain_fresh("risk")
        if items:
            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                  f"Executing {len(items)} approved trades...")

        executed = 0
        failed = 0
        for item in items:
            try:
                ticker = item["ticker"]
                side = item.get("side")
                contracts = int(item.get("contracts", 0))
                price_cents = int(item.get("price_for_order_cents", 0))

                if not (ticker and side in ("yes", "no") and contracts >= 1):
                    print(f"[WARN] malformed risk item for {ticker}, skipping")
                    failed += 1
                    continue

                order = place_order(ticker, side, contracts, price_cents)
                placed = order is not None
                send_discord(_build_embed(
                    item, order, placed=placed,
                    error=None if placed else "place_order returned None",
                ))
                if placed:
                    executed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"[WARN] Execution crashed for {item.get('ticker', '?')}: {e}")
                send_discord(_build_embed(item, None, placed=False, error=str(e)))
                failed += 1

        elapsed = time.time() - cycle_start
        if items:
            verb = "simulated" if PAPER_TRADING else "placed"
            print(f"  Done in {elapsed:.1f}s — {executed} orders {verb}, {failed} failed.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
