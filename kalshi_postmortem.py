import os
import time
import requests
from datetime import datetime, timezone
from kalshi_auth import get_auth_headers, KALSHI_BASE_URL

WEBHOOK_KALSHI_POSTMORTEM = os.getenv("WEBHOOK_KALSHI_POSTMORTEM", "")
CHECK_INTERVAL            = int(os.getenv("KALSHI_POSTMORTEM_INTERVAL", 3600))
ANTHROPIC_API_KEY         = os.getenv("ANTHROPIC_API_KEY", "")

checked  = set()
wins     = 0
losses   = 0
total_pnl = 0.0

def get_settlements():
    path = "/trade-api/v2/portfolio/settlements"
    try:
        r = requests.get(
            f"{KALSHI_BASE_URL}/portfolio/settlements",
            headers=get_auth_headers("GET", path),
            params={"limit": 50},
            timeout=15
        )
        r.raise_for_status()
        return r.json().get("settlements", [])
    except Exception as e:
        print(f"[WARN] Settlements fetch failed: {e}")
        return []

def analyze_with_claude(title, result, pnl, our_side):
    if not ANTHROPIC_API_KEY:
        return "API key not set."
    prompt = f"""You are a prediction market analyst reviewing a Kalshi trade.

MARKET: {title}
OUR POSITION: {our_side}
RESULT: {result}
PNL: ${pnl:.2f}

In 2-3 sentences: Why did this happen? What should we do differently? What signal did we miss?"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-opus-4-5",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        r.raise_for_status()
        return r.json()["content"][0]["text"]
    except Exception as e:
        print(f"[WARN] Claude API failed: {e}")
        return "Analysis unavailable."

def format_pnl(amount):
    return f"+${amount:.2f}" if amount >= 0 else f"-${abs(amount):.2f}"

def build_embed(settlement, analysis, won):
    global wins, losses, total_pnl
    ticker     = settlement.get("ticker", "")
    title      = settlement.get("market_title", ticker)
    revenue    = settlement.get("revenue", 0) / 100
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if won:
        wins      += 1
        total_pnl += revenue
        color      = 0x2ECC71
        result_str = "✅ WIN"
    else:
        losses    += 1
        total_pnl += revenue
        color      = 0xE74C3C
        result_str = "❌ LOSS"

    total    = wins + losses
    win_rate = wins / total if total > 0 else 0

    fields = [
        {"name": "📊 Result",    "value": result_str,                         "inline": True},
        {"name": "💰 PnL",       "value": format_pnl(revenue),               "inline": True},
        {"name": "📈 Total PnL", "value": format_pnl(total_pnl),             "inline": True},
        {"name": "✅ Wins",      "value": str(wins),                         "inline": True},
        {"name": "❌ Losses",    "value": str(losses),                       "inline": True},
        {"name": "🎯 Win Rate",  "value": f"{win_rate:.1%} ({total} total)", "inline": True},
        {"name": "🔗 Market",    "value": f"[{title[:60]}]({market_url})",   "inline": False},
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
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_POSTMORTEM, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_POSTMORTEM, json={"embeds": [embed]}, timeout=10)
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def run():
    print("Kalshi Post-Mortem Agent starting...")
    while True:
        cycle_start = time.time()
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Checking settled Kalshi positions...")

        settlements = get_settlements()
        analyzed    = 0

        for s in settlements:
            ticker = s.get("ticker", "")
            if ticker in checked:
                continue

            revenue  = s.get("revenue", 0) / 100
            won      = revenue > 0
            title    = s.get("market_title", ticker)
            our_side = "YES" if s.get("yes_count", 0) > 0 else "NO"
            result   = "WIN" if won else "LOSS"
            analysis = analyze_with_claude(title, result, revenue, our_side)

            checked.add(ticker)
            embed = build_embed(s, analysis, won)
            send_discord(embed)
            analyzed += 1
            time.sleep(2)

        if len(checked) > 10_000:
            checked.clear()

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {analyzed} settlements analyzed.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
