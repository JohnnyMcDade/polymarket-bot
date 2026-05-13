import os
import time
import requests
from datetime import datetime, timezone

WEBHOOK_KALSHI_POSTMORTEM = os.getenv("WEBHOOK_KALSHI_POSTMORTEM", "")
CHECK_INTERVAL            = int(os.getenv("KALSHI_POSTMORTEM_INTERVAL", 3600))
KALSHI_API_KEY            = os.getenv("KALSHI_API_KEY", "")
ANTHROPIC_API_KEY         = os.getenv("ANTHROPIC_API_KEY", "")

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

checked_markets = set()
win_count       = 0
loss_count      = 0
total_pnl       = 0.0

def get_headers():
    return {
        "Authorization": f"Bearer {KALSHI_API_KEY}",
        "Content-Type": "application/json"
    }

def get_settled_positions():
    try:
        r = requests.get(
            f"{KALSHI_API}/portfolio/positions",
            headers=get_headers(),
            params={"limit": 100, "settlement_status": "settled"},
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("market_positions", [])
    except Exception as e:
        print(f"[WARN] Settled positions fetch failed: {e}")
        return []

def get_portfolio_history():
    try:
        r = requests.get(
            f"{KALSHI_API}/portfolio/settlements",
            headers=get_headers(),
            params={"limit": 50},
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("settlements", [])
    except Exception as e:
        print(f"[WARN] Portfolio history fetch failed: {e}")
        return []

def analyze_with_claude(question, result, pnl, our_side):
    if not ANTHROPIC_API_KEY:
        return "API key not set."
    prompt = f"""You are a prediction market post-mortem analyst for Kalshi.

MARKET: {question}
OUR POSITION: {our_side}
RESULT: {result}
PNL: ${pnl:.2f}

In 2-3 sentences:
1. Why did this outcome happen?
2. What should we do differently next time?
3. What signal did we miss or get right?

Be concise and specific."""

    try:
        headers = {
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        payload = {
            "model": "claude-opus-4-5",
            "max_tokens": 300,
            "messages": [{"role": "user", "content": prompt}]
        }
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=payload,
            timeout=30
        )
        r.raise_for_status()
        return r.json()["content"][0]["text"]
    except Exception as e:
        print(f"[WARN] Claude API failed: {e}")
        return "Analysis unavailable."

def format_usd(amount):
    if amount >= 0:
        return f"+${amount:.2f}"
    return f"-${abs(amount):.2f}"

def get_win_rate():
    total = win_count + loss_count
    if total == 0:
        return 0
    return win_count / total

def build_postmortem_embed(settlement, analysis, won):
    global win_count, loss_count, total_pnl

    ticker    = settlement.get("ticker", "")
    title     = settlement.get("market_title", ticker)
    revenue   = settlement.get("revenue", 0) / 100
    pnl       = revenue
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if won:
        win_count  += 1
        total_pnl  += pnl
        color       = 0x2ECC71
        result_str  = "✅ WIN"
    else:
        loss_count += 1
        total_pnl  += pnl
        color       = 0xE74C3C
        result_str  = "❌ LOSS"

    win_rate = get_win_rate()
    total    = win_count + loss_count

    fields = [
        {"name": "📊 Result",      "value": result_str,                          "inline": True},
        {"name": "💰 PnL",         "value": format_usd(pnl),                    "inline": True},
        {"name": "📈 Total PnL",   "value": format_usd(total_pnl),              "inline": True},
        {"name": "✅ Wins",        "value": str(win_count),                     "inline": True},
        {"name": "❌ Losses",      "value": str(loss_count),                    "inline": True},
        {"name": "🎯 Win Rate",    "value": f"{win_rate:.1%} ({total} trades)", "inline": True},
        {"name": "🔗 Market",      "value": f"[{title[:60]}]({market_url})",    "inline": False},
    ]

    if analysis:
        fields.append({"name": "🧠 Claude Analysis", "value": analysis[:500], "inline": False})

    return {
        "title": f"📋 KALSHI POST-MORTEM — {title[:60]}",
        "url":   market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Kalshi Post-Mortem  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def send_discord(embed):
    if not WEBHOOK_KALSHI_POSTMORTEM:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_POSTMORTEM, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            retry_after = float(r.json().get("retry_after", 2))
            time.sleep(retry_after + 0.5)
            requests.post(WEBHOOK_KALSHI_POSTMORTEM, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def run():
    print("Kalshi Post-Mortem Agent starting...")
    if not KALSHI_API_KEY:
        print("[WARN] KALSHI_API_KEY not set!")

    while True:
        cycle_start = time.time()
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Checking settled Kalshi positions...")

        settlements = get_portfolio_history()
        analyzed    = 0

        for settlement in settlements:
            ticker = settlement.get("ticker", "")
            if ticker in checked_markets:
                continue

            revenue = settlement.get("revenue", 0) / 100
            won     = revenue > 0
            title   = settlement.get("market_title", ticker)
            our_side = "YES" if settlement.get("yes_count", 0) > 0 else "NO"
            result   = "WIN" if won else "LOSS"

            analysis = analyze_with_claude(title, result, revenue, our_side)

            checked_markets.add(ticker)
            embed = build_postmortem_embed(settlement, analysis, won)
            send_discord(embed)
            analyzed += 1
            time.sleep(2)

        if len(checked_markets) > 10_000:
            checked_markets.clear()

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {analyzed} settlements analyzed.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
