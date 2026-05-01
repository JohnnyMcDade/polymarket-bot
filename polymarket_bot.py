import os
import time
import requests
from datetime import datetime, timezone

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 120))
TOP_N_TRADERS = int(os.getenv("TOP_N_TRADERS", 50))
MIN_TRADE_SIZE = float(os.getenv("MIN_TRADE_SIZE", 500))

DATA_API = "https://data-api.polymarket.com"
POLY_URL = "https://polymarket.com"

seen_tx_hashes = set()
last_seen_ts = int(time.time())

def get_top_traders(n=50):
    try:
        r = requests.get(f"{DATA_API}/v1/leaderboard", timeout=15)
        r.raise_for_status()
        traders = r.json()
        traders.sort(key=lambda t: float(t.get("pnl", 0)), reverse=True)
        return traders[:n]
    except Exception as e:
        print(f"[WARN] Could not fetch leaderboard: {e}")
        return []

def get_recent_trades(wallet, since_ts):
    try:
        params = {
            "user": wallet,
            "type": "TRADE",
            "start": since_ts,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
            "limit": 20
        }
        r = requests.get(f"{DATA_API}/activity", params=params, timeout=15)
        r.raise_for_status()
        result = r.json()
        return result if isinstance(result, list) else []
    except Exception as e:
        print(f"[WARN] Activity fetch failed: {e}")
        return []

def format_usd(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def trader_name(t):
    return t.get("userName") or t.get("pseudonym") or t.get("proxyWallet", "")[:10] + "..."

def build_embed(trade, trader, rank):
    wallet = trader.get("proxyWallet", "")
    name = trader_name(trader)
    side = trade.get("side", "?")
    market = trade.get("title", "Unknown market")
    outcome = trade.get("outcome", "?")
    price = float(trade.get("price", 0))
    share_size = float(trade.get("size", 0))
    trade_value = share_size * price
    ts = trade.get("timestamp", 0)
    slug = trade.get("slug", "")
    dt_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    market_url = f"{POLY_URL}/event/{slug}" if slug else f"{POLY_URL}/leaderboard"
    profile_url = f"{POLY_URL}/profile/{wallet}"
    color = 0x2ECC71 if side == "BUY" else 0xE74C3C
    side_str = "BUY" if side == "BUY" else "SELL"
    embed = {
        "title": f"{side_str} - Rank #{rank} Trader",
        "url": profile_url,
        "color": color,
        "fields": [
            {"name": "Trader", "value": f"[{name}]({profile_url})", "inline": True},
            {"name": "Value", "value": format_usd(trade_value), "inline": True},
            {"name": "Shares", "value": f"{share_size:,.0f}", "inline": True},
            {"name": "Price", "value": f"{price:.2%}", "inline": True},
            {"name": "Market", "value": f"[{market[:80]}]({market_url})", "inline": False},
            {"name": "Outcome", "value": outcome, "inline": True},
            {"name": "Time", "value": dt_str, "inline": True}
        ],
        "footer": {"text": f"Polymarket Whale Tracker - Rank #{rank}"},
        "timestamp": datetime.utcnow().isoformat()
    }
    return embed

def send_discord(embed):
    if not DISCORD_WEBHOOK_URL:
        print(embed)
        return
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]}, timeout=10)
        if r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[WARN] Discord send failed: {e}")

def run():
    global last_seen_ts
    print(f"Bot starting - tracking top {TOP_N_TRADERS} traders, min trade {format_usd(MIN_TRADE_SIZE)}")
    while True:
        cycle_start = time.time()
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Checking...")
        traders = get_top_traders(TOP_N_TRADERS)
        new_alerts = 0
        check_since = last_seen_ts
        for rank, trader in enumerate(traders, start=1):
            wallet = trader.get("proxyWallet")
            if not wallet:
                continue
            for trade in get_recent_trades(wallet, check_since):
                tx_hash = trade.get("transactionHash", "")
                share_size = float(trade.get("size", 0))
                price_each = float(trade.get("price", 0))
                trade_value = share_size * price_each
                if tx_hash in seen_tx_hashes:
                    continue
                if MIN_TRADE_SIZE and trade_value < MIN_TRADE_SIZE:
                    continue
                seen_tx_hashes.add(tx_hash)
                send_discord(build_embed(trade, trader, rank))
                new_alerts += 1
                time.sleep(0.3)
            time.sleep(0.15)
        last_seen_ts = int(cycle_start)
        if len(seen_tx_hashes) > 100_000:
            seen_tx_hashes.clear()
        print(f"Done - {new_alerts} alert(s) sent.")
        time.sleep(max(0, CHECK_INTERVAL - (time.time() - cycle_start)))

if __name__ == "__main__":
    run()
