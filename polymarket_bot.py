"""
Polymarket Top-50 Trader Alert Bot
"""

import os
import time
import requests
from datetime import datetime, timezone

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
CHECK_INTERVAL      = int(os.getenv("CHECK_INTERVAL", 120))
TOP_N_TRADERS       = int(os.getenv("TOP_N_TRADERS", 50))
MIN_TRADE_SIZE      = float(os.getenv("MIN_TRADE_SIZE", 500))

DATA_API   = "https://data-api.polymarket.com"
POLY_URL   = "https://polymarket.com"

seen_tx_hashes: set = set()
last_seen_ts:   int = int(time.time())

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
        params = {"user": wallet, "type": "TRADE", "start": since_ts, "sortBy": "TIMESTAMP", "sortDirection": "DESC", "limit": 20}
        r = requests.get(f"{DATA_API}/activity", params=params, timeout=15)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception as e:
        print(f"[WARN] Activity fetch failed for {wallet[:10]}: {e}")
        return []

def format_side(side):
    return "🟢 BUY" if side == "BUY" else "🔴 SELL"

def format_usd(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def trader_display(t):
    return t.get("userName") or t.get("pseudonym") or t.get("proxyWallet", "")[:10] + "..."

def build_embed(trade, trader, rank):
    wallet      = trader.get("proxyWallet", "")
    name        = trader_display(trader)
    side        = trade.get("side", "?")
    market      = trade.get("title", "Unknown market")
    outcome     = trade.get("outcome", "?")
    price       = float(trade.get("price", 0))
    share_size  = float(trade.get("size", 0))
    trade_value = share_size * price
    ts          = trade.get("timestamp", 0)
    slug        = trade.get("slug", "")
    dt_str      = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    market_url  = f"{POLY_URL}/event/{slug}" if slug else f"{POLY_URL}/leaderboard"
    profile_url = f"{POLY_URL}/profile/{wallet}"
    color       = 0x2ECC71 if side == "BUY" else 0xE74C3C
    return {
        "title": f"{format_side(side)}  •  Rank #{rank} Trader",
        "url": profile_url,
        "color": color,
        "fields": [
            {"name": "👤 Trader",  "value": f"[{name}]({profile_url})",      "inline": True},
            {"name": "💰 Value",   "value": format_usd(trade_value),          "inline": True},
            {"name": "🪙 Shares",  "value": f"{share_size:,.0f}",             "inline": True},
