import os
import time
import requests
from datetime import datetime, timezone

WEBHOOK_SCANNER = os.getenv("WEBHOOK_SCANNER", "")
CHECK_INTERVAL  = int(os.getenv("SCANNER_INTERVAL", 300))  # every 5 mins
MIN_LIQUIDITY   = float(os.getenv("MIN_LIQUIDITY", 10000))  # $10k min
MAX_DAYS_LEFT   = int(os.getenv("MAX_DAYS_LEFT", 30))       # resolves within 30 days
MIN_VOLUME      = float(os.getenv("MIN_VOLUME", 5000))      # $5k 24hr volume

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

seen_market_ids = set()

def get_markets():
    try:
        params = {
            "active": "true",
            "closed": "false",
            "limit": 100,
            "order": "volume24hr",
            "ascending": "false"
        }
        r = requests.get(f"{GAMMA_API}/markets", params=params, timeout=15)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else r.json().get("markets", [])
    except Exception as e:
        print(f"[WARN] Market fetch failed: {e}")
        return []

def days_until_resolution(end_date_str):
    try:
        if not end_date_str:
            return 999
        end = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return max(0, (end - now).days)
    except:
        return 999

def calculate_edge(market):
    try:
        price = float(market.get("outcomePrices", ["0.5"])[0])
        if price <= 0 or price >= 1:
            return 0
        # Edge = how far price is from 50/50
        # Markets near 0.1 or 0.9 have more potential
        distance_from_even = abs(price - 0.5)
        return round(distance_from_even * 100, 1)
    except:
        return 0

def format_usd(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def get_signal_strength(liquidity, volume, days_left, edge):
    score = 0
    if liquidity >= 50000: score += 3
    elif liquidity >= 20000: score += 2
    elif liquidity >= 10000: score += 1
    if volume >= 20000: score += 3
    elif volume >= 10000: score += 2
    elif volume >= 5000: score += 1
    if days_left <= 3: score += 3
    elif days_left <= 7: score += 2
    elif days_left <= 14: score += 1
    if edge >= 30: score += 2
    elif edge >= 15: score += 1
    if score >= 7: return "🔥🔥🔥 STRONG"
    if score >= 5: return "🔥🔥 MODERATE"
    return "🔥 WEAK"

def build_scanner_embed(market, days_left, edge, signal):
    question = market.get("question", "Unknown")
    slug = market.get("slug", "")
    liquidity = float(market.get("liquidity", 0))
    volume_24h = float(market.get("volume24hr", 0))
    prices = market.get("outcomePrices", ["0.5", "0.5"])
    outcomes = market.get("outcomes", ["YES", "NO"])
    end_date = market.get("endDate", "")
    market_url = f"https://polymarket.com/event/{slug}"

    try:
        yes_price = float(prices[0])
        no_price = float(prices[1]) if len(prices) > 1 else 1 - yes_price
    except:
        yes_price = 0.5
        no_price = 0.5

    color = 0x00D4FF

    fields = [
        {"name": "📊 Signal", "value": signal, "inline": True},
        {"name": "💧 Liquidity", "value": format_usd(liquidity), "inline": True},
        {"name": "📈 24hr Volume", "value": format_usd(volume_24h), "inline": True},
        {"name": "✅ YES Price", "value": f"{yes_price:.1%}", "inline": True},
        {"name": "❌ NO Price", "value": f"{no_price:.1%}", "inline": True},
        {"name": "⏰ Days Left", "value": f"{days_left} days", "inline": True},
        {"name": "🎯 Edge Score", "value": f"{edge}%", "inline": True},
        {"name": "🔗 Market", "value": f"[View on Polymarket]({market_url})", "inline": False},
    ]

    return {
        "title": f"🔍 SCANNER — {question[:80]}",
        "url": market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Scanner  •  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"},
        "timestamp": datetime.utcnow().isoformat()
    }

def send_discord(embed):
    if not WEBHOOK_SCANNER:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_SCANNER, json={"embeds": [embed]}, timeout=10)
        if r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(0.5)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def run():
    print("Scanner Agent starting...")
    print(f"Min liquidity: {format_usd(MIN_LIQUIDITY)} | Max days: {MAX_DAYS_LEFT} | Min volume: {format_usd(MIN_VOLUME)}")

    while True:
        cycle_start = time.time()
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Scanning markets...")

        markets = get_markets()
        flagged = 0

        for market in markets:
            market_id = market.get("id", "")
            if market_id in seen_market_ids:
                continue

            liquidity = float(market.get("liquidity", 0))
            volume_24h = float(market.get("volume24hr", 0))
            end_date = market.get("endDate", "")
            days_left = days_until_resolution(end_date)

            if liquidity < MIN_LIQUIDITY:
                continue
            if volume_24h < MIN_VOLUME:
                continue
            if days_left > MAX_DAYS_LEFT:
                continue

            edge = calculate_edge(market)
            signal = get_signal_strength(liquidity, volume_24h, days_left, edge)

            if "WEAK" in signal:
                continue

            seen_market_ids.add(market_id)
            embed = build_scanner_embed(market, days_left, edge, signal)
            send_discord(embed)
            flagged += 1
            time.sleep(0.3)

        if len(seen_market_ids) > 10_000:
            seen_market_ids.clear()

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {flagged} markets flagged.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
