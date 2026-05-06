import os
import time
import requests
from datetime import datetime, timezone

WEBHOOK_POSTMORTEM = os.getenv("WEBHOOK_POSTMORTEM", "")
CHECK_INTERVAL     = int(os.getenv("POSTMORTEM_INTERVAL", 3600))  # every hour
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")

GAMMA_API = "https://gamma-api.polymarket.com"

checked_market_ids = set()
win_count  = 0
loss_count = 0
total_pnl  = 0.0

def get_resolved_markets(limit=20):
    try:
        params = {
            "closed": "true",
            "limit": limit,
            "order": "volume24hr",
            "ascending": "false"
        }
        r = requests.get(f"{GAMMA_API}/markets", params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else data.get("markets", [])
    except Exception as e:
        print(f"[WARN] Resolved market fetch failed: {e}")
        return []

def analyze_with_claude(question, winning_outcome, our_recommendation, yes_price):
    if not ANTHROPIC_API_KEY:
        return "API key not set — skipping Claude analysis."

    prompt = f"""You are a prediction market post-mortem analyst.

MARKET: {question}
WINNING OUTCOME: {winning_outcome}
OUR RECOMMENDATION WAS: {our_recommendation}
MARKET PRICE AT TIME: {yes_price:.1%}

In 2-3 sentences analyze:
1. Why did this outcome happen?
2. What signal did we miss or get right?
3. What should we do differently next time?

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
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def get_win_rate():
    total = win_count + loss_count
    if total == 0:
        return 0
    return win_count / total

def build_postmortem_embed(market, winning_outcome, analysis, was_correct):
    global win_count, loss_count

    question   = market.get("question", "Unknown")
    slug       = market.get("slug", "")
    volume     = float(market.get("volume", 0))
    market_url = f"https://polymarket.com/event/{slug}"

    if was_correct:
        win_count += 1
        color  = 0x2ECC71
        result = "✅ CORRECT PREDICTION"
    else:
        loss_count += 1
        color  = 0xE74C3C
        result = "❌ WRONG PREDICTION"

    win_rate = get_win_rate()
    total    = win_count + loss_count

    fields = [
        {"name": "📊 Result",         "value": result,                              "inline": False},
        {"name": "🏆 Winning Outcome","value": winning_outcome,                     "inline": True},
        {"name": "📈 Total Volume",   "value": format_usd(volume),                  "inline": True},
        {"name": "✅ Wins",           "value": str(win_count),                      "inline": True},
        {"name": "❌ Losses",         "value": str(loss_count),                     "inline": True},
        {"name": "🎯 Win Rate",       "value": f"{win_rate:.1%} ({total} total)",   "inline": True},
        {"name": "🔗 Market",         "value": f"[View on Polymarket]({market_url})","inline": False},
    ]

    if analysis:
        fields.append({"name": "🧠 Claude's Post-Mortem", "value": analysis[:500], "inline": False})

    return {
        "title": f"📋 POST-MORTEM — {question[:80]}",
        "url":   market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Post-Mortem  •  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def send_discord(embed):
    if not WEBHOOK_POSTMORTEM:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_POSTMORTEM, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            retry_after = r.json().get("retry_after", 2)
            print(f"[WARN] Rate limited — waiting {retry_after}s")
            time.sleep(float(retry_after) + 0.5)
            requests.post(WEBHOOK_POSTMORTEM, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def run():
    print("Post-Mortem Agent starting...")

    while True:
        cycle_start = time.time()
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Checking resolved markets...")

        markets  = get_resolved_markets(20)
        analyzed = 0

        for market in markets:
            market_id = market.get("id", "")
            if market_id in checked_market_ids:
                continue

            question = market.get("question", "")
            outcomes = market.get("outcomes", ["YES", "NO"])
            prices   = market.get("outcomePrices", ["0.5", "0.5"])

            try:
                yes_price = float(prices[0])
            except:
                yes_price = 0.5

            # Determine winning outcome
            winner = market.get("winner", "")
            if not winner:
                checked_market_ids.add(market_id)
                continue

            # Determine if our prediction would have been correct
            # If YES price was < 0.5 we would have recommended BUY_NO
            # If YES price was > 0.5 we would have recommended BUY_YES
            our_rec = "BUY_YES" if yes_price > 0.5 else "BUY_NO"
            was_correct = (our_rec == "BUY_YES" and "yes" in winner.lower()) or \
                         (our_rec == "BUY_NO" and "no" in winner.lower())

            analysis = analyze_with_claude(question, winner, our_rec, yes_price)

            checked_market_ids.add(market_id)
            embed = build_postmortem_embed(market, winner, analysis, was_correct)
            send_discord(embed)
            analyzed += 1
            time.sleep(2)

        if len(checked_market_ids) > 10_000:
            checked_market_ids.clear()

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {analyzed} markets analyzed.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
