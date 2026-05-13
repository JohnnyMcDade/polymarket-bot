import os
import time
import requests
from datetime import datetime, timezone
from kalshi_auth import get_auth_headers, KALSHI_BASE_URL

WEBHOOK_KALSHI_EXECUTION = os.getenv("WEBHOOK_KALSHI_EXECUTION", "")
BANKROLL                 = float(os.getenv("BANKROLL", 500))
MAX_BET_PCT              = float(os.getenv("MAX_BET_PCT", 0.05))
MIN_EDGE                 = float(os.getenv("MIN_EDGE", 0.05))
MAX_DAILY_LOSS           = float(os.getenv("MAX_DAILY_LOSS", 0.10))
PAPER_TRADING            = os.getenv("PAPER_TRADING", "true").lower() == "true"
CHECK_INTERVAL           = int(os.getenv("KALSHI_EXECUTION_INTERVAL", 300))

daily_spent  = 0.0
last_reset   = datetime.now(timezone.utc).date()
processed    = set()

def get_balance():
    path = "/trade-api/v2/portfolio/balance"
    try:
        r = requests.get(
            f"{KALSHI_BASE_URL}/portfolio/balance",
            headers=get_auth_headers("GET", path),
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("balance", 0) / 100
    except Exception as e:
        print(f"[WARN] Balance fetch failed: {e}")
        return BANKROLL

def get_markets():
    path = "/trade-api/v2/markets"
    try:
        r = requests.get(
            f"{KALSHI_BASE_URL}/markets",
            headers=get_auth_headers("GET", path),
            params={"limit": 20, "status": "open"},
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("markets", [])
    except Exception as e:
        print(f"[WARN] Market fetch failed: {e}")
        return []

def place_order(ticker, side, contracts, price_cents):
    if PAPER_TRADING:
        print(f"  [PAPER] {contracts} {side} contracts on {ticker} at {price_cents}¢")
        return {"paper": True}

    path = "/trade-api/v2/portfolio/orders"
    try:
        payload = {
            "action":          "buy",
            "client_order_id": f"pp_{ticker}_{int(time.time())}",
            "count":           contracts,
            "side":            side,
            "ticker":          ticker,
            "type":            "limit",
            "yes_price":       price_cents if side == "yes" else 100 - price_cents,
        }
        r = requests.post(
            f"{KALSHI_BASE_URL}/portfolio/orders",
            headers=get_auth_headers("POST", path),
            json=payload,
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("order", {})
    except Exception as e:
        print(f"[WARN] Order failed: {e}")
        return None

def kelly_criterion(yes_price_cents, edge):
    try:
        p = (yes_price_cents / 100) + edge
        p = max(0.01, min(0.99, p))
        q = 1 - p
        b = (100 - yes_price_cents) / yes_price_cents
        kelly = (b * p - q) / b
        return max(0, min(kelly / 2, MAX_BET_PCT))
    except:
        return 0

def format_usd(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def days_until_expiry(close_time_str):
    try:
        if not close_time_str:
            return 999
        close = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
        return max(0, (close - datetime.now(timezone.utc)).days)
    except:
        return 999

def build_embed(market, side, contracts, price_cents, bet_size, edge, approved, reason):
    title      = market.get("title", "Unknown")
    ticker     = market.get("ticker", "")
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    mode       = "📄 PAPER" if PAPER_TRADING else ("✅ LIVE" if approved else "❌ BLOCKED")
    color      = 0xFFAA00 if PAPER_TRADING else (0x2ECC71 if approved else 0xE74C3C)

    fields = [
        {"name": "📊 Status",      "value": mode,                  "inline": True},
        {"name": "🎯 Side",        "value": f"{'🟢 YES' if side=='yes' else '🔴 NO'}", "inline": True},
        {"name": "💰 Bet Size",    "value": format_usd(bet_size),  "inline": True},
        {"name": "🪙 Contracts",   "value": str(contracts),        "inline": True},
        {"name": "📊 Price",       "value": f"{price_cents}¢",     "inline": True},
        {"name": "🎯 Edge",        "value": f"{edge:.1%}",         "inline": True},
        {"name": "🏦 Bankroll",    "value": format_usd(BANKROLL),  "inline": True},
        {"name": "📉 Daily Spent", "value": format_usd(daily_spent), "inline": True},
        {"name": "📌 Reason",      "value": reason,                "inline": False},
        {"name": "🔗 Market",      "value": f"[{title[:60]}]({market_url})", "inline": False},
    ]

    return {
        "title": f"⚡ KALSHI EXECUTION — {title[:60]}",
        "url":   market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Execution  •  {'PAPER' if PAPER_TRADING else 'LIVE'}  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def send_discord(embed):
    if not WEBHOOK_KALSHI_EXECUTION:
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_EXECUTION, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_EXECUTION, json={"embeds": [embed]}, timeout=10)
        time.sleep(1.0)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def run():
    global daily_spent, last_reset, processed
    mode = "PAPER TRADING" if PAPER_TRADING else "LIVE TRADING"
    print(f"Kalshi Execution Agent starting — {mode}")
    print(f"Bankroll: {format_usd(BANKROLL)} | Max bet: {MAX_BET_PCT:.0%} | Min edge: {MIN_EDGE:.0%}")

    while True:
        cycle_start = time.time()

        today = datetime.now(timezone.utc).date()
        if today > last_reset:
            daily_spent = 0.0
            last_reset  = today
            processed.clear()
            print("Daily tracker reset")

        if daily_spent >= BANKROLL * MAX_DAILY_LOSS:
            print(f"  Daily limit reached — skipping")
            time.sleep(CHECK_INTERVAL)
            continue

        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Running execution cycle...")

        markets  = get_markets()
        executed = 0

        for market in markets:
            ticker     = market.get("ticker", "")
            if ticker in processed:
                continue

            yes_ask    = market.get("yes_ask", 50)
            volume     = market.get("volume", 0)
            close_time = market.get("close_time", "")
            days_left  = days_until_expiry(close_time)

            if volume < 10000 or days_left > 30 or days_left == 0:
                continue

            distance = abs((yes_ask / 100) - 0.5)
            if distance >= 0.3:   edge = 0.12
            elif distance >= 0.2: edge = 0.08
            elif distance >= 0.1: edge = 0.05
            else:                 edge = 0.02

            if edge < MIN_EDGE:
                processed.add(ticker)
                continue

            side            = "yes" if yes_ask < 50 else "no"
            price_for_order = yes_ask if side == "yes" else 100 - yes_ask
            kelly_pct       = kelly_criterion(yes_ask, edge)
            bet_size        = BANKROLL * kelly_pct
            contracts       = int(bet_size / (price_for_order / 100)) if price_for_order > 0 else 0

            if contracts < 1 or bet_size < 5:
                processed.add(ticker)
                continue

            if daily_spent + bet_size > BANKROLL * MAX_DAILY_LOSS:
                continue

            reason = f"Edge {edge:.0%} | Kelly {kelly_pct:.1%} | {days_left}d left | Vol {format_usd(volume)}"
            order  = place_order(ticker, side, contracts, price_for_order)

            if order:
                daily_spent += bet_size
                processed.add(ticker)
                embed = build_embed(market, side, contracts, price_for_order, bet_size, edge, True, reason)
                send_discord(embed)
                executed += 1
                time.sleep(1)

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {executed} orders {'simulated' if PAPER_TRADING else 'placed'}.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
