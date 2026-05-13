import os
import time
import requests
from datetime import datetime, timezone

WEBHOOK_KALSHI_ALERTS = os.getenv("WEBHOOK_KALSHI_ALERTS", "")
CHECK_INTERVAL        = int(os.getenv("KALSHI_TRACKER_INTERVAL", 60))
MIN_TRADE_SIZE        = float(os.getenv("KALSHI_MIN_TRADE", 1000))
KALSHI_API_KEY        = os.getenv("KALSHI_API_KEY", "")

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

seen_trade_ids = set()

def get_headers():
    return {
        "Authorization": f"Bearer {KALSHI_API_KEY}",
        "Content-Type": "application/json"
    }

def get_recent_trades():
    try:
        params = {"limit": 100}
        r = requests.get(
            f"{KALSHI_API}/trades",
            headers=get_headers(),
            params=params,
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("trades", [])
    except Exception as e:
        print(f"[WARN] Kalshi trades fetch failed: {e}")
        return []

def get_market_details(ticker):
    try:
        r = requests.get(
            f"{KALSHI_API}/markets/{ticker}",
            headers=get_headers(),
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("market", {})
    except:
        return {}

def format_usd(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def get_category(title):
    title_lower = title.lower()
    if any(k in title_lower for k in ["election","president","senate","congress","vote","trump","biden","democrat","republican","political","governor"]):
        return "🏛️ Politics"
    if any(k in title_lower for k in ["nba","nfl","mlb","nhl","ufc","soccer","basketball","football","baseball","hockey","sports","championship","playoffs"]):
        return "🏆 Sports"
    if any(k in title_lower for k in ["bitcoin","crypto","ethereum","fed","rate","inflation","gdp","stock","nasdaq"]):
        return "📈 Finance"
    return "🌐 Other"

def build_embed(trade, market):
    trade_id  = trade.get("trade_id", "")
    ticker    = trade.get("ticker", "")
    side      = trade.get("taker_side", "yes")
    price     = trade.get("yes_price", 50)
    count     = trade.get("count", 0)
    trade_value = (price / 100) * count
    title     = market.get("title", ticker)
    category  = get_category(title)
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    side_str = "🟢 BUY YES" if side == "yes" else "🔴 BUY NO"
    color    = 0x2ECC71 if side == "yes" else 0xE74C3C

    fields = [
        {"name": "📊 Side",        "value": side_str,              "inline": True},
        {"name": "💰 Trade Value", "value": format_usd(trade_value), "inline": True},
        {"name": "🪙 Contracts",   "value": f"{count:,}",          "inline": True},
        {"name": "📊 Price",       "value": f"{price}¢",           "inline": True},
        {"name": "🏷️ Category",   "value": category,              "inline": True},
        {"name": "🕒 Time",        "value": now_str,               "inline": True},
        {"name": "📌 Market",      "value": f"[{title[:80]}]({market_url})", "inline": False},
        {"name": "🔗 View",        "value": f"[Open Kalshi]({market_url})", "inline": False},
    ]

    return {
        "title": f"🐋 KALSHI TRADE — {title[:60]}",
        "url":   market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Kalshi Tracker  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def send_discord(embed):
    if not WEBHOOK_KALSHI_ALERTS:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_ALERTS, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            retry_after = float(r.json().get("retry_after", 2))
            print(f"[WARN] Rate limited — waiting {retry_after}s")
            time.sleep(retry_after + 0.5)
            requests.post(WEBHOOK_KALSHI_ALERTS, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.0)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def run():
    print("Kalshi Tracker starting...")
    if not KALSHI_API_KEY:
        print("[WARN] KALSHI_API_KEY not set!")

    while True:
        cycle_start = time.time()
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Checking Kalshi trades...")

        trades  = get_recent_trades()
        alerted = 0

        for trade in trades:
            trade_id  = trade.get("trade_id", "")
            if trade_id in seen_trade_ids:
                continue

            price       = trade.get("yes_price", 50)
            count       = trade.get("count", 0)
            trade_value = (price / 100) * count

            if trade_value < MIN_TRADE_SIZE:
                seen_trade_ids.add(trade_id)
                continue

            ticker = trade.get("ticker", "")
            market = get_market_details(ticker)

            seen_trade_ids.add(trade_id)
            embed = build_embed(trade, market)
            send_discord(embed)
            alerted += 1
            time.sleep(0.3)

        if len(seen_trade_ids) > 100_000:
            seen_trade_ids.clear()

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {alerted} Kalshi trades alerted.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
