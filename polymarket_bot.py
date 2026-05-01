import os
import time
import requests
from datetime import datetime, timezone
from collections import defaultdict

# Config
CHECK_INTERVAL  = int(os.getenv("CHECK_INTERVAL", 60))
TOP_N_TRADERS   = int(os.getenv("TOP_N_TRADERS", 15))
MIN_TRADE_SIZE  = float(os.getenv("MIN_TRADE_SIZE", 1000))
STRONG_SIGNAL_WINDOW = 600  # 10 minutes

DATA_API = "https://data-api.polymarket.com"
POLY_URL = "https://polymarket.com"

# One webhook per category - set these in Railway Variables
WEBHOOKS = {
    "politics": os.getenv("WEBHOOK_POLITICS", ""),
    "crypto":   os.getenv("WEBHOOK_CRYPTO", ""),
    "sports":   os.getenv("WEBHOOK_SPORTS", ""),
    "finance":  os.getenv("WEBHOOK_FINANCE", ""),
    "science":  os.getenv("WEBHOOK_SCIENCE", ""),
    "other":    os.getenv("WEBHOOK_OTHER", ""),
    "all":      os.getenv("WEBHOOK_ALL", ""),
    "signals":  os.getenv("WEBHOOK_SIGNALS", ""),
}

# State
seen_tx_hashes = set()
last_seen_ts   = int(time.time())
top_traders    = []
last_trader_refresh   = 0
TRADER_REFRESH_INTERVAL = 86400  # daily

# Strong signal tracking
recent_market_trades = defaultdict(list)

CATEGORY_KEYWORDS = {
    "politics": ["election","president","senate","congress","vote","trump","biden",
                 "democrat","republican","political","governor","primary","ballot",
                 "policy","government","fed ","federal reserve","tariff","nato"],
    "crypto":   ["bitcoin","btc","ethereum","eth","crypto","solana","sol","coin",
                 "token","defi","nft","blockchain","binance","coinbase","doge","xrp"],
    "sports":   ["nba","nfl","mlb","nhl","soccer","football","basketball","baseball",
                 "hockey","tennis","golf","ufc","mma","championship","super bowl",
                 "world cup","playoffs","finals","league","suns","lakers","warriors"],
    "finance":  ["stock","market","s&p","nasdaq","dow","fed rate","interest rate",
                 "gdp","inflation","recession","earnings","ipo","merger"],
    "science":  ["ai","artificial intelligence","space","nasa","climate","covid",
                 "vaccine","drug","fda","tech","technology","openai","gpt"],
}

def get_category(title):
    t = title.lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(k in t for k in kws):
            return cat
    return "other"

def get_monthly_leaderboard(n=15):
    try:
        now = datetime.now(timezone.utc)
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        r = requests.get(
            f"{DATA_API}/v1/leaderboard",
            params={"startDate": int(start_of_month.timestamp())},
            timeout=15
        )
        r.raise_for_status()
        traders = r.json()
        if not isinstance(traders, list) or len(traders) == 0:
            r2 = requests.get(f"{DATA_API}/v1/leaderboard", timeout=15)
            r2.raise_for_status()
            traders = r2.json()
        traders.sort(key=lambda t: float(t.get("pnl", 0)), reverse=True)
        print(f"Loaded top {min(n, len(traders))} traders for {now.strftime('%B %Y')}")
        return traders[:n]
    except Exception as e:
        print(f"[WARN] Leaderboard fetch failed: {e}")
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

def get_confidence(trade_value, rank):
    if trade_value >= 10000 and rank <= 5:
        return "VERY HIGH"
    if trade_value >= 5000 or rank <= 5:
        return "HIGH"
    if trade_value >= 2000 or rank <= 10:
        return "MEDIUM"
    return "MODERATE"

CAT_EMOJI = {
    "politics": "🏛️",
    "crypto": "🪙",
    "sports": "🏆",
    "finance": "📈",
    "science": "🔬",
    "other": "🌐"
}

CONFIDENCE_EMOJI = {
    "VERY HIGH": "🔥🔥🔥",
    "HIGH": "🔥🔥",
    "MEDIUM": "🔥",
    "MODERATE": "⚡"
}

def build_embed(trade, trader, rank, is_strong=False, signal_traders=None):
    wallet = trader.get("proxyWallet", "")
    name = trader_name(trader)
    pnl = float(trader.get("pnl", 0))
    side = trade.get("side", "?")
    market = trade.get("title", "Unknown market")
    outcome = trade.get("outcome", "?")
    price = float(trade.get("price", 0))
    share_size = float(trade.get("size", 0))
    trade_value = share_size * price
    ts = trade.get("timestamp", 0)
    slug = trade.get("slug", "")
    category = get_category(market)
    confidence = get_confidence(trade_value, rank)
    conf_emoji = CONFIDENCE_EMOJI.get(confidence, "⚡")
    cat_emoji = CAT_EMOJI.get(category, "🌐")

    dt_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    market_url = f"{POLY_URL}/event/{slug}" if slug else f"{POLY_URL}/leaderboard"
    profile_url = f"{POLY_URL}/profile/{wallet}"
    side_str = "🟢 BUY" if side == "BUY" else "🔴 SELL"
    color = 0xFF6600 if is_strong else (0x2ECC71 if side == "BUY" else 0xE74C3C)
    title = f"🚨 STRONG SIGNAL  {side_str}" if is_strong else f"{side_str}  Rank #{rank} Trader"

    fields = [
        {"name": "👤 Trader", "value": f"[{name}]({profile_url})", "inline": True},
        {"name": "🏆 Monthly Rank", "value": f"#{rank}", "inline": True},
        {"name": "💰 All-time PnL", "value": format_usd(pnl), "inline": True},
        {"name": "💵 Trade Value", "value": format_usd(trade_value), "inline": True},
        {"name": "🪙 Shares", "value": f"{share_size:,.0f}", "inline": True},
        {"name": "📊 Price", "value": f"{price:.2%}", "inline": True},
        {"name": f"{cat_emoji} Category", "value": category.title(), "inline": True},
        {"name": "🎯 Outcome", "value": outcome, "inline": True},
        {"name": f"{conf_emoji} Confidence", "value": confidence, "inline": True},
        {"name": "📌 Market", "value": f"[{market[:80]}]({market_url})", "inline": False},
        {"name": "🕒 Time", "value": dt_str, "inline": True},
    ]

    if is_strong and signal_traders:
        whales = "\n".join([f"Rank #{r} — {n}" for n, r in signal_traders])
        fields.append({"name": "🐋 Whales Aligned", "value": whales, "inline": False})

    return {
        "title": title,
        "url": profile_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"Polymarket Whale Tracker  {cat_emoji} {category.title()}  {datetime.utcnow().strftime('%B %Y')}"},
        "timestamp": datetime.utcnow().isoformat()
    }

def send_to_webhook(url, embed):
    if not url:
        return
    try:
        r = requests.post(url, json={"embeds": [embed]}, timeout=10)
        if r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(0.5)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def send_alert(embed, category, is_strong=False):
    send_to_webhook(WEBHOOKS["all"], embed)
    send_to_webhook(WEBHOOKS.get(category, ""), embed)
    if is_strong:
        send_to_webhook(WEBHOOKS["signals"], embed)

def check_strong_signal(trade, trader, rank, now_ts):
    market_id = trade.get("conditionId") or trade.get("slug", "")
    outcome = trade.get("outcome", "")
    side = trade.get("side", "")
    key = (market_id, outcome, side)

    recent_market_trades[key] = [
        e for e in recent_market_trades[key]
        if now_ts - e["ts"] <= STRONG_SIGNAL_WINDOW
    ]

    name = trader_name(trader)
    if not any(e["name"] == name for e in recent_market_trades[key]):
        recent_market_trades[key].append({"ts": now_ts, "name": name, "rank": rank})

    if len(recent_market_trades[key]) >= 2:
        return True, [(e["name"], e["rank"]) for e in recent_market_trades[key]]
    return False, []

def run():
    global last_seen_ts, top_traders, last_trader_refresh

    print("Polymarket Whale Bot starting")
    print(f"Top {TOP_N_TRADERS} monthly traders | Min trade: {format_usd(MIN_TRADE_SIZE)} | Check every {CHECK_INTERVAL}s")

    while True:
        cycle_start = time.time()
        now_ts = int(cycle_start)

        if now_ts - last_trader_refresh >= TRADER_REFRESH_INTERVAL or not top_traders:
            top_traders = get_monthly_leaderboard(TOP_N_TRADERS)
            last_trader_refresh = now_ts
            if not top_traders:
                print("[WARN] No traders loaded, retrying in 60s")
                time.sleep(60)
                continue

        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Checking {len(top_traders)} traders...")

        new_alerts = 0
        check_since = last_seen_ts

        for rank, trader in enumerate(top_traders, start=1):
            wallet = trader.get("proxyWallet")
            if not wallet:
                continue

            for trade in get_recent_trades(wallet, check_since):
                tx_hash = trade.get("transactionHash", "")
                if tx_hash in seen_tx_hashes:
                    continue

                share_size = float(trade.get("size", 0))
                price_each = float(trade.get("price", 0))
                trade_value = share_size * price_each

                if MIN_TRADE_SIZE and trade_value < MIN_TRADE_SIZE:
                    continue

                seen_tx_hashes.add(tx_hash)
                category = get_category(trade.get("title", ""))
                is_strong, signal_traders = check_strong_signal(trade, trader, rank, now_ts)
                embed = build_embed(trade, trader, rank, is_strong, signal_traders if is_strong else None)
                send_alert(embed, category, is_strong)

                if is_strong:
                    print(f"  STRONG SIGNAL: {trade.get('title','')[:50]}")

                new_alerts += 1
                time.sleep(0.3)

            time.sleep(0.15)

        last_seen_ts = now_ts

        if len(seen_tx_hashes) > 100_000:
            seen_tx_hashes.clear()

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s  {new_alerts} alert(s) sent.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
