import os
import time
import requests
import json
from datetime import datetime, timezone

WEBHOOK_KALSHI_EXECUTION = os.getenv("WEBHOOK_KALSHI_EXECUTION", "")
WEBHOOK_KALSHI_RISK      = os.getenv("WEBHOOK_KALSHI_RISK", "")
KALSHI_API_KEY           = os.getenv("KALSHI_API_KEY", "")
BANKROLL                 = float(os.getenv("BANKROLL", 500))
MAX_BET_PCT              = float(os.getenv("MAX_BET_PCT", 0.05))
MIN_EDGE                 = float(os.getenv("MIN_EDGE", 0.05))
MAX_DAILY_LOSS           = float(os.getenv("MAX_DAILY_LOSS", 0.10))
PAPER_TRADING            = os.getenv("PAPER_TRADING", "true").lower() == "true"
CHECK_INTERVAL           = int(os.getenv("KALSHI_EXECUTION_INTERVAL", 300))

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

daily_spent   = 0.0
daily_pnl     = 0.0
open_positions = {}
last_reset    = datetime.now(timezone.utc).date()

def get_headers():
    return {
        "Authorization": f"Bearer {KALSHI_API_KEY}",
        "Content-Type": "application/json"
    }

def get_balance():
    try:
        r = requests.get(
            f"{KALSHI_API}/portfolio/balance",
            headers=get_headers(),
            timeout=15
        )
        r.raise_for_status()
        balance_cents = r.json().get("balance", 0)
        return balance_cents / 100
    except Exception as e:
        print(f"[WARN] Balance fetch failed: {e}")
        return BANKROLL

def get_open_positions():
    try:
        r = requests.get(
            f"{KALSHI_API}/portfolio/positions",
            headers=get_headers(),
            params={"limit": 100},
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("market_positions", [])
    except Exception as e:
        print(f"[WARN] Positions fetch failed: {e}")
        return []

def kelly_criterion(yes_price_cents, edge):
    try:
        p = (yes_price_cents / 100) + edge
        p = max(0.01, min(0.99, p))
        q = 1 - p
        b = (100 - yes_price_cents) / yes_price_cents
        kelly = (b * p - q) / b
        half_kelly = kelly / 2
        return max(0, min(half_kelly, MAX_BET_PCT))
    except:
        return 0

def calculate_contracts(bet_size_usd, yes_price_cents):
    try:
        cost_per_contract = yes_price_cents / 100
        if cost_per_contract <= 0:
            return 0
        return int(bet_size_usd / cost_per_contract)
    except:
        return 0

def place_order(ticker, side, contracts, price_cents):
    if PAPER_TRADING:
        print(f"  [PAPER] Would buy {contracts} {side} contracts on {ticker} at {price_cents}¢")
        return {"paper": True, "ticker": ticker, "side": side, "contracts": contracts, "price": price_cents}

    try:
        payload = {
            "action":   "buy",
            "client_order_id": f"pp_{ticker}_{int(time.time())}",
            "count":    contracts,
            "side":     side,
            "ticker":   ticker,
            "type":     "limit",
            "yes_price": price_cents if side == "yes" else 100 - price_cents,
        }
        r = requests.post(
            f"{KALSHI_API}/portfolio/orders",
            headers=get_headers(),
            json=payload,
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("order", {})
    except Exception as e:
        print(f"[WARN] Order placement failed: {e}")
        return None

def format_usd(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def build_execution_embed(market, side, contracts, price_cents, bet_size, edge, approved, reason, paper=False):
    title      = market.get("title", "Unknown")
    ticker     = market.get("ticker", "")
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    status = "📄 PAPER TRADE" if paper else ("✅ ORDER PLACED" if approved else "❌ ORDER BLOCKED")
    color  = 0xFFAA00 if paper else (0x2ECC71 if approved else 0xE74C3C)
    side_str = "🟢 YES" if side == "yes" else "🔴 NO"

    fields = [
        {"name": "📊 Status",       "value": status,                "inline": False},
        {"name": "🎯 Side",         "value": side_str,              "inline": True},
        {"name": "💰 Bet Size",     "value": format_usd(bet_size),  "inline": True},
        {"name": "🪙 Contracts",    "value": str(contracts),        "inline": True},
        {"name": "📊 Price",        "value": f"{price_cents}¢",     "inline": True},
        {"name": "🎯 Edge",         "value": f"{edge:.1%}",         "inline": True},
        {"name": "🏦 Bankroll",     "value": format_usd(BANKROLL),  "inline": True},
        {"name": "📉 Daily Spent",  "value": format_usd(daily_spent), "inline": True},
        {"name": "📌 Reason",       "value": reason,                "inline": False},
        {"name": "🔗 Market",       "value": f"[{title[:60]}]({market_url})", "inline": False},
    ]

    return {
        "title": f"⚡ KALSHI EXECUTION — {title[:60]}",
        "url":   market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Execution  •  {'PAPER MODE' if paper else 'LIVE MODE'}  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def send_discord(webhook_url, embed):
    if not webhook_url:
        return
    try:
        r = requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            retry_after = float(r.json().get("retry_after", 2))
            time.sleep(retry_after + 0.5)
            requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.0)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def get_top_markets():
    try:
        params = {"limit": 20, "status": "open"}
        r = requests.get(
            f"{KALSHI_API}/markets",
            headers=get_headers(),
            params=params,
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("markets", [])
    except Exception as e:
        print(f"[WARN] Market fetch failed: {e}")
        return []

def days_until_expiry(close_time_str):
    try:
        if not close_time_str:
            return 999
        close = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
        now   = datetime.now(timezone.utc)
        return max(0, (close - now).days)
    except:
        return 999

def run():
    global daily_spent, daily_pnl, last_reset

    mode = "PAPER TRADING" if PAPER_TRADING else "LIVE TRADING"
    print(f"Kalshi Execution Agent starting — {mode}")
    print(f"Bankroll: {format_usd(BANKROLL)} | Max bet: {MAX_BET_PCT:.0%} | Min edge: {MIN_EDGE:.0%}")

    if not KALSHI_API_KEY:
        print("[WARN] KALSHI_API_KEY not set — execution disabled!")
        return

    processed_markets = set()

    while True:
        cycle_start = time.time()

        # Reset daily tracker at midnight UTC
        today = datetime.now(timezone.utc).date()
        if today > last_reset:
            daily_spent = 0.0
            daily_pnl   = 0.0
            last_reset  = today
            processed_markets.clear()
            print("Daily tracker reset")

        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Running execution cycle...")

        # Check daily loss limit
        if daily_spent >= BANKROLL * MAX_DAILY_LOSS:
            print(f"  Daily loss limit reached ({format_usd(daily_spent)}) — skipping cycle")
            time.sleep(CHECK_INTERVAL)
            continue

        markets  = get_top_markets()
        executed = 0

        for market in markets:
            ticker     = market.get("ticker", "")
            if ticker in processed_markets:
                continue

            yes_ask    = market.get("yes_ask", 50)
            volume     = market.get("volume", 0)
            close_time = market.get("close_time", "")
            title      = market.get("title", "")
            days_left  = days_until_expiry(close_time)

            # Basic filters
            if volume < 10000:
                continue
            if days_left > 30 or days_left == 0:
                continue

            # Calculate edge
            price_decimal = yes_ask / 100
            distance      = abs(price_decimal - 0.5)
            if distance >= 0.3:   edge = 0.12
            elif distance >= 0.2: edge = 0.08
            elif distance >= 0.1: edge = 0.05
            else:                 edge = 0.02

            # Block if edge too low
            if edge < MIN_EDGE:
                processed_markets.add(ticker)
                continue

            # Determine side
            side = "yes" if yes_ask < 50 else "no"
            price_for_order = yes_ask if side == "yes" else 100 - yes_ask

            # Kelly sizing
            kelly_pct  = kelly_criterion(yes_ask, edge)
            bet_size   = BANKROLL * kelly_pct
            contracts  = calculate_contracts(bet_size, price_for_order)

            # Block if bet too small
            if contracts < 1 or bet_size < 5:
                processed_markets.add(ticker)
                continue

            # Block if daily limit reached
            if daily_spent + bet_size > BANKROLL * MAX_DAILY_LOSS:
                reason = f"Daily limit would be exceeded ({format_usd(daily_spent + bet_size)} > {format_usd(BANKROLL * MAX_DAILY_LOSS)})"
                embed  = build_execution_embed(market, side, contracts, price_for_order, bet_size, edge, False, reason, PAPER_TRADING)
                send_discord(WEBHOOK_KALSHI_EXECUTION, embed)
                continue

            # Place order
            reason = f"Edge {edge:.0%} | Kelly {kelly_pct:.1%} | {days_left} days left | Volume {format_usd(volume)}"
            order  = place_order(ticker, side, contracts, price_for_order)

            if order:
                daily_spent += bet_size
                processed_markets.add(ticker)
                embed = build_execution_embed(market, side, contracts, price_for_order, bet_size, edge, True, reason, PAPER_TRADING)
                send_discord(WEBHOOK_KALSHI_EXECUTION, embed)
                executed += 1
                time.sleep(1)

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {executed} orders {'simulated' if PAPER_TRADING else 'placed'}.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
