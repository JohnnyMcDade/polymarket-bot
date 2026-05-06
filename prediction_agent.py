import os
import time
import requests
import feedparser
from datetime import datetime, timezone

WEBHOOK_PREDICTIONS = os.getenv("WEBHOOK_PREDICTIONS", "")
CHECK_INTERVAL      = int(os.getenv("PREDICTION_INTERVAL", 600))
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")

GAMMA_API = "https://gamma-api.polymarket.com"

RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/topNews",
    "https://rss.cnn.com/rss/edition.rss",
    "https://feeds.bbci.co.uk/news/rss.xml",
    "https://www.reddit.com/r/politics/top/.rss?limit=10",
    "https://www.reddit.com/r/worldnews/top/.rss?limit=10",
    "https://www.reddit.com/r/polymarket/top/.rss?limit=10",
]

seen_prediction_ids = set()

def get_markets(limit=10):
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

def fetch_news(market_question):
    articles = []
    keywords = [w for w in market_question.lower().split() if len(w) > 3]
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:15]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                text    = f"{title} {summary}".lower()
                if any(kw in text for kw in keywords):
                    articles.append(f"{title}: {summary[:200]}")
        except:
            pass
    return articles[:5]

def ask_claude(market_question, yes_price, articles):
    if not ANTHROPIC_API_KEY:
        print("[WARN] No ANTHROPIC_API_KEY set")
        return None

    news_context = "\n".join(articles) if articles else "No relevant news found."

    prompt = f"""You are an expert prediction market analyst. Analyze this market and estimate the true probability.

MARKET QUESTION: {market_question}

CURRENT MARKET PRICE (YES): {yes_price:.1%}

RELEVANT NEWS:
{news_context}

Analyze this carefully and respond in this EXACT format:
TRUE_PROBABILITY: [number between 0 and 1, e.g. 0.65]
CONFIDENCE: [LOW/MEDIUM/HIGH]
EDGE: [number between -1 and 1, e.g. 0.08 means 8% edge]
RECOMMENDATION: [BUY_YES/BUY_NO/SKIP]
REASONING: [2-3 sentences explaining your analysis]

Be precise and analytical. Only recommend BUY if edge is above 5%."""

    try:
        headers = {
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        payload = {
            "model": "claude-opus-4-5",
            "max_tokens": 500,
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
        return None

def parse_claude_response(response):
    result = {
        "true_probability": None,
        "confidence": "LOW",
        "edge": 0,
        "recommendation": "SKIP",
        "reasoning": ""
    }
    if not response:
        return result

    for line in response.split("\n"):
        line = line.strip()
        if line.startswith("TRUE_PROBABILITY:"):
            try:
                result["true_probability"] = float(line.split(":")[1].strip())
            except:
                pass
        elif line.startswith("CONFIDENCE:"):
            result["confidence"] = line.split(":")[1].strip()
        elif line.startswith("EDGE:"):
            try:
                result["edge"] = float(line.split(":")[1].strip())
            except:
                pass
        elif line.startswith("RECOMMENDATION:"):
            result["recommendation"] = line.split(":")[1].strip()
        elif line.startswith("REASONING:"):
            result["reasoning"] = line.split(":", 1)[1].strip()

    return result

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

def build_prediction_embed(market, parsed, yes_price, days_left, articles_found):
    question   = market.get("question", "Unknown")
    slug       = market.get("slug", "")
    liquidity  = float(market.get("liquidity", 0))
    volume_24h = float(market.get("volume24hr", 0))
    market_url = f"https://polymarket.com/event/{slug}"

    true_prob  = parsed["true_probability"]
    confidence = parsed["confidence"]
    edge       = parsed["edge"]
    rec        = parsed["recommendation"]
    reasoning  = parsed["reasoning"]

    rec_emoji = {
        "BUY_YES": "🟢 BUY YES",
        "BUY_NO":  "🔴 BUY NO",
        "SKIP":    "⚪ SKIP"
    }.get(rec, "⚪ SKIP")

    conf_emoji = {
        "HIGH":   "🔥🔥🔥",
        "MEDIUM": "🔥🔥",
        "LOW":    "🔥"
    }.get(confidence, "🔥")

    color = {
        "BUY_YES": 0x2ECC71,
        "BUY_NO":  0xE74C3C,
        "SKIP":    0x888888
    }.get(rec, 0x888888)

    fields = [
        {"name": "🎯 Recommendation",    "value": rec_emoji,                          "inline": True},
        {"name": f"{conf_emoji} Confidence", "value": confidence,                     "inline": True},
        {"name": "📊 Edge",              "value": f"{edge:.1%}",                      "inline": True},
        {"name": "💰 Market Price YES",  "value": f"{yes_price:.1%}",                 "inline": True},
        {"name": "🧠 True Probability",  "value": f"{true_prob:.1%}" if true_prob else "N/A", "inline": True},
        {"name": "⏰ Days Left",         "value": f"{days_left} days",                "inline": True},
        {"name": "💧 Liquidity",         "value": format_usd(liquidity),              "inline": True},
        {"name": "📈 24hr Volume",       "value": format_usd(volume_24h),             "inline": True},
        {"name": "📰 News Articles",     "value": str(articles_found),                "inline": True},
        {"name": "🔗 Market",            "value": f"[View on Polymarket]({market_url})", "inline": False},
    ]

    if reasoning:
        fields.append({"name": "💭 Claude's Analysis", "value": reasoning[:500], "inline": False})

    return {
        "title": f"🤖 PREDICTION — {question[:80]}",
        "url":   market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Prediction Agent  •  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def send_discord(embed):
    if not WEBHOOK_PREDICTIONS:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_PREDICTIONS, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            retry_after = r.json().get("retry_after", 2)
            print(f"[WARN] Rate limited — waiting {retry_after}s")
            time.sleep(float(retry_after) + 0.5)
            requests.post(WEBHOOK_PREDICTIONS, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def run():
    print("Prediction Agent starting...")
    if not ANTHROPIC_API_KEY:
        print("[WARN] ANTHROPIC_API_KEY not set — predictions will be skipped!")

    while True:
        cycle_start = time.time()
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Running predictions...")

        markets    = get_markets(10)
        predicted  = 0

        for market in markets:
            market_id = market.get("id", "")
            if market_id in seen_prediction_ids:
                continue

            question  = market.get("question", "")
            prices    = market.get("outcomePrices", ["0.5"])
            end_date  = market.get("endDate", "")
            liquidity = float(market.get("liquidity", 0))
            days_left = days_until_resolution(end_date)

            if liquidity < 10000:
                continue
            if days_left > 30:
                continue

            try:
                yes_price = float(prices[0])
            except:
                yes_price = 0.5

            articles = fetch_news(question)
            response = ask_claude(question, yes_price, articles)

            if not response:
                continue

            parsed = parse_claude_response(response)

            if parsed["recommendation"] == "SKIP" and parsed["confidence"] == "LOW":
                seen_prediction_ids.add(market_id)
                continue

            seen_prediction_ids.add(market_id)
            embed = build_prediction_embed(market, parsed, yes_price, days_left, len(articles))
            send_discord(embed)
            predicted += 1
            time.sleep(2)

        if len(seen_prediction_ids) > 10_000:
            seen_prediction_ids.clear()

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {predicted} predictions sent.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
