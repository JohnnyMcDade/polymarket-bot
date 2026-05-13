import os
import time
import requests
from datetime import datetime, timezone
from kalshi_auth import get_auth_headers, KALSHI_BASE_URL

WEBHOOK_KALSHI_SCANNER = os.getenv("WEBHOOK_KALSHI_SCANNER", "")
CHECK_INTERVAL         = int(os.getenv("KALSHI_SCANNER_INTERVAL", 600))

seen_market_ids = set()

def get_markets():
    path = "/trade-api/v2/markets"
    try:
        params = {"limit": 100, "status": "open"}
        r = requests.get(
            f"{KALSHI_BASE_URL}/markets",
            headers=get_auth_headers("GET", path),
            params=params,
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("markets", [])
    except Exception as e:
        print(f"[WARN] Kalshi market fetch failed: {e}")
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

def calculate_edge(yes_price):
    try:
        price = yes_price / 100
        if price <= 0 or price >= 1:
            return 0
        distance = abs(price - 0.5)
        if distance >= 0.3: return 12
        if distance >= 0.2: return 8
        if distance >= 0.1: return 5
        return 2
    except:
        return 0

def get_signal_strength(volume, days_left, edge):
    score = 0
    if volume >= 100000: score += 3
    elif volume >= 50000: score += 2
    elif volume >= 10000: score += 1
    if days_left <= 3:    score += 3
    elif days_left <= 7:  score += 2
    elif days_left <= 14: score += 1
    if edge >= 10: score += 2
    elif edge >= 5: score += 1
    if score >= 6: return "STRONG"
    if score >= 4: return "MODERATE"
    return "WEAK"

def format_usd(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def build_embed(market, days_left, edge, signal):
    ticker     = market.get("ticker", "")
    title      = market.get("title", "Unknown")
    yes_price  = market.get("yes_ask", 50)
    no_price   = market.get("no_ask", 50)
    volume     = market.get("volume", 0)
    market_url = f"https://kalshi.com/markets/{ticker}"

    if signal == "STRONG":
        color      = 0xFF6600
        signal_str = "🔥🔥🔥 STRONG"
    elif signal == "MODERATE":
        color      = 0x00D4FF
        signal_str = "🔥🔥 MODERATE"
    else:
        color      = 0x888888
        signal_str = "🔥 WEAK"

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    fields = [
        {"name": "📊 Signal",     "value": signal_str,          "inline": True},
        {"name": "📈 Volume",     "value": format_usd(volume),  "inline": True},
        {"name": "⏰ Days Left",  "value": f"{days_left} days", "inline": True},
        {"name": "✅ YES Price",  "value": f"{yes_price}¢",     "inline": True},
        {"name": "❌ NO Price",   "value": f"{no_price}¢",      "inline": True},
        {"name": "🎯 Edge Score", "value": f"{edge}%",          "inline": True},
        {"name": "🔗 Market",     "value": f"[View on Kalshi]({market_url})", "inline": False},
    ]

    return {
        "title": f"🔍 KALSHI SCANNER — {title[:80]}",
        "url":   market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Kalshi Scanner  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def send_discord(embed):
    if not WEBHOOK_KALSHI_SCANNER:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_SCANNER, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            retry_after = float(r.json().get("retry_after", 2))
            print(f"[WARN] Rate limited — waiting {retry_after}s")
            time.sleep(retry_after + 0.5)
            requests.post(WEBHOOK_KALSHI_SCANNER, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def run():
    print("Kalshi Scanner starting...")
    while True:
        cycle_start = time.time()
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Scanning Kalshi markets...")

        markets = get_markets()
        flagged = 0

        for market in markets:
            ticker    = market.get("ticker", "")
            if ticker in seen_market_ids:
                continue

            volume     = market.get("volume", 0)
            close_time = market.get("close_time", "")
            yes_price  = market.get("yes_ask", 50)
            days_left  = days_until_expiry(close_time)

            if volume < 10000:
                continue
            if days_left > 30 or days_left == 0:
                continue

            edge   = calculate_edge(yes_price)
            signal = get_signal_strength(volume, days_left, edge)

            if signal == "WEAK":
                continue

            seen_market_ids.add(ticker)
            embed = build_embed(market, days_left, edge, signal)
            send_discord(embed)
            flagged += 1

        if len(seen_market_ids) > 10_000:
            seen_market_ids.clear()

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {flagged} Kalshi markets flagged.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
