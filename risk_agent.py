import os
import time
import requests
from datetime import datetime, timezone

WEBHOOK_RISK     = os.getenv("WEBHOOK_RISK", "")
CHECK_INTERVAL   = int(os.getenv("RISK_INTERVAL", 900))
BANKROLL         = float(os.getenv("BANKROLL", 1000))
MAX_BET_PCT      = float(os.getenv("MAX_BET_PCT", 0.05))
MIN_EDGE         = float(os.getenv("MIN_EDGE", 0.05))
MAX_OPEN_BETS    = int(os.getenv("MAX_OPEN_BETS", 5))
MAX_DAILY_LOSS   = float(os.getenv("MAX_DAILY_LOSS", 0.10))

GAMMA_API = "https://gamma-api.polymarket.com"

seen_risk_ids = set()
daily_loss    = 0.0

def get_markets(limit=20):
    try:
        params = {
            "active": "true",
            "closed": "false",
            "limit": limit,
            "order": "volume24hr",
            "ascending": "false"
        }
        r = requests.get(f"{GAMMA_API}/markets", params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else data.get("markets", [])
    except Exception as e:
        print(f"[WARN] Market fetch failed: {e}")
        return []

def kelly_criterion(prob_win, odds):
    """
    Kelly Criterion formula: f = (bp - q) / b
    Where:
    - b = decimal odds - 1 (net odds)
    - p = probability of winning
    - q = probability of losing (1 - p)
    Returns fraction of bankroll to bet (0 to 1)
    """
    if prob_win <= 0 or prob_win >= 1 or odds <= 1:
        return 0
    b = odds - 1
    p = prob_win
    q = 1 - p
    kelly = (b * p - q) / b
    # Use half kelly for safety
    half_kelly = kelly / 2
    return max(0, min(half_kelly, MAX_BET_PCT))

def calculate_edge(yes_price):
    """
    Edge = difference between our estimated probability and market price
    For simplicity we use a base assumption that markets near extremes
    have more edge potential
    """
    if yes_price <= 0 or yes_price >= 1:
        return 0
    # Markets between 0.1-0.3 or 0.7-0.9 have more potential edge
    distance = abs(yes_price - 0.5)
    if distance >= 0.3:
        return 0.12  # 12% edge estimate for extreme markets
    if distance >= 0.2:
        return 0.08  # 8% edge estimate
    if distance >= 0.1:
        return 0.05  # 5% edge estimate
    return 0.02      # 2% edge for near 50/50 markets

def get_risk_rating(bet_size, bankroll, edge, days_left):
    pct = bet_size / bankroll
    if pct > 0.08 or edge < 0.03 or days_left > 25:
        return "🔴 HIGH RISK"
    if pct > 0.04 or edge < 0.06 or days_left > 14:
        return "🟡 MEDIUM RISK"
    return "🟢 LOW RISK"

def format_usd(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def days_until_resolution(end_date_str):
    try:
        if not end_date_str:
            return 999
        end = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return max(0, (end - now).days)
    except:
        return 999

def build_risk_embed(market, bet_size, kelly_pct, edge, risk_rating, days_left, approved):
    question   = market.get("question", "Unknown")
    slug       = market.get("slug", "")
    liquidity  = float(market.get("liquidity", 0))
    volume_24h = float(market.get("volume24hr", 0))
    prices     = market.get("outcomePrices", ["0.5", "0.5"])
    market_url = f"https://polymarket.com/event/{slug}"

    try:
        yes_price = float(prices[0])
        no_price  = float(prices[1]) if len(prices) > 1 else 1 - yes_price
    except:
        yes_price = 0.5
        no_price  = 0.5

    max_bet    = BANKROLL * MAX_BET_PCT
    daily_left = (BANKROLL * MAX_DAILY_LOSS) - daily_loss

    color  = 0x2ECC71 if approved else 0xE74C3C
    status = "✅ TRADE APPROVED" if approved else "❌ TRADE BLOCKED"

    fields = [
        {"name": "📊 Status",          "value": status,                    "inline": False},
        {"name": "⚠️ Risk Rating",     "value": risk_rating,               "inline": True},
        {"name": "💰 Recommended Bet", "value": format_usd(bet_size),      "inline": True},
        {"name": "📈 Kelly %",         "value": f"{kelly_pct:.1%}",        "inline": True},
        {"name": "🎯 Edge Estimate",   "value": f"{edge:.1%}",             "inline": True},
        {"name": "💵 YES Price",       "value": f"{yes_price:.1%}",        "inline": True},
        {"name": "❌ NO Price",        "value": f"{no_price:.1%}",         "inline": True},
        {"name": "⏰ Days Left",       "value": f"{days_left} days",       "inline": True},
        {"name": "💧 Liquidity",       "value": format_usd(liquidity),     "inline": True},
        {"name": "🏦 Bankroll",        "value": format_usd(BANKROLL),      "inline": True},
        {"name": "🔒 Max Bet",         "value": format_usd(max_bet),       "inline": True},
        {"name": "📉 Daily Loss Left", "value": format_usd(daily_left),    "inline": True},
        {"name": "🔗 Market",          "value": f"[View on Polymarket]({market_url})", "inline": False},
    ]

    return {
        "title": f"⚖️ RISK AGENT — {question[:80]}",
        "url":   market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Risk Agent  •  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def send_discord(embed):
    if not WEBHOOK_RISK:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_RISK, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            retry_after = r.json().get("retry_after", 2)
            print(f"[WARN] Rate limited — waiting {retry_after}s")
            time.sleep(float(retry_after) + 0.5)
            requests.post(WEBHOOK_RISK, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def run():
    global daily_loss
    print("Risk Agent starting...")
    print(f"Bankroll: {format_usd(BANKROLL)} | Max bet: {MAX_BET_PCT:.0%} | Min edge: {MIN_EDGE:.0%}")

    last_reset = datetime.now(timezone.utc).date()

    while True:
        cycle_start = time.time()

        # Reset daily loss tracker at midnight UTC
        today = datetime.now(timezone.utc).date()
        if today > last_reset:
            daily_loss  = 0.0
            last_reset  = today
            seen_risk_ids.clear()
            print("Daily loss tracker reset")

        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Running risk analysis...")

        markets  = get_markets(20)
        analyzed = 0

        for market in markets:
            market_id = market.get("id", "")
            if market_id in seen_risk_ids:
                continue

            prices    = market.get("outcomePrices", ["0.5"])
            end_date  = market.get("endDate", "")
            liquidity = float(market.get("liquidity", 0))
            days_left = days_until_resolution(end_date)

            try:
                yes_price = float(prices[0])
            except:
                yes_price = 0.5

            # Skip markets with low liquidity or too far out
            if liquidity < 10000:
                continue
            if days_left > 30:
                continue

            edge      = calculate_edge(yes_price)
            odds      = 1 / yes_price if yes_price > 0 else 2
            kelly_pct = kelly_criterion(yes_price, odds)
            bet_size  = BANKROLL * kelly_pct

            # Block conditions
            blocked = False
            if edge < MIN_EDGE:
                blocked = True
            if bet_size < 10:
                blocked = True
            if daily_loss >= BANKROLL * MAX_DAILY_LOSS:
                blocked = True
                print(f"  Daily loss limit reached — blocking all trades")

            risk_rating = get_risk_rating(bet_size, BANKROLL, edge, days_left)
            approved    = not blocked

            # Only send approved trades or high conviction blocked ones
            if not approved and edge < 0.03:
                continue

            seen_risk_ids.add(market_id)
            embed = build_risk_embed(market, bet_size, kelly_pct, edge, risk_rating, days_left, approved)
            send_discord(embed)
            analyzed += 1
            time.sleep(0.3)

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {analyzed} markets analyzed.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
