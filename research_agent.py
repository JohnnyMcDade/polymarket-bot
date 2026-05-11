import os
import time
import requests
import feedparser
from datetime import datetime, timezone

WEBHOOK_RESEARCH = os.getenv("WEBHOOK_RESEARCH", "")
CHECK_INTERVAL   = int(os.getenv("RESEARCH_INTERVAL", 900))

GAMMA_API = "https://gamma-api.polymarket.com"

# Real-time RSS feeds — Google News updates every few minutes
RSS_FEEDS = [
    # Google News — always current, updates every few minutes
    "https://news.google.com/rss/search?q=polymarket&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=prediction+market+today&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=NBA+today+2026&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=NFL+2026&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=UFC+fight+2026&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=election+politics+2026&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=crypto+bitcoin+today&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=stock+market+today+2026&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=sports+results+today&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=breaking+news+today&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=MLB+baseball+2026&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=soccer+football+2026&hl=en-US&gl=US&ceid=US:en",
    # ESPN — live sports news
    "https://www.espn.com/espn/rss/news",
    "https://www.espn.com/espn/rss/nba/news",
    "https://www.espn.com/espn/rss/nfl/news",
    "https://www.espn.com/espn/rss/mlb/news",
    # Reddit NEW (not top) — posts from last few minutes
    "https://www.reddit.com/r/polymarket/new/.rss?limit=25",
    "https://www.reddit.com/r/sportsbook/new/.rss?limit=25",
    "https://www.reddit.com/r/nba/new/.rss?limit=15",
    "https://www.reddit.com/r/nfl/new/.rss?limit=15",
    "https://www.reddit.com/r/politics/new/.rss?limit=15",
    "https://www.reddit.com/r/worldnews/new/.rss?limit=15",
    "https://www.reddit.com/r/cryptocurrency/new/.rss?limit=15",
    "https://www.reddit.com/r/wallstreetbets/new/.rss?limit=15",
]

# Sentiment keywords
BULLISH_WORDS = [
    "win","wins","winning","victory","leads","ahead","surge","surges",
    "rises","rising","up","higher","positive","strong","confirmed",
    "approved","passes","passed","likely","probable","expected","yes",
    "support","supported","gains","gained","beats","beat","ahead",
    "advances","advance","leads","leading","favored","favorite",
    "dominates","dominant","crushes","blowout","landslide","record",
]

BEARISH_WORDS = [
    "lose","loses","losing","loss","defeat","behind","drops","falls",
    "falling","down","lower","negative","weak","rejected","fails",
    "failed","unlikely","improbable","unexpected","no","oppose","opposed",
    "drops","dropped","misses","missed","below","trailing","underdog",
    "struggle","struggles","struggling","collapse","collapses","crash",
    "upset","controversy","scandal","suspended","injured","injury",
]

seen_research_ids = set()

def get_top_markets(limit=20):
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

def fetch_rss_articles():
    articles = []
    successful_feeds = 0
    for feed_url in RSS_FEEDS:
        try:
            # Set user agent to avoid blocks
            feedparser.USER_AGENT = "PassivePoly Research Agent 1.0"
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:10]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                # Include published date if available
                published = entry.get("published", "")
                text = f"{title} {summary} {published}".lower()
                if text.strip():
                    articles.append(text)
            if feed.entries:
                successful_feeds += 1
        except Exception as e:
            print(f"[WARN] RSS fetch failed for {feed_url[:50]}: {e}")
    print(f"  Loaded {len(articles)} articles from {successful_feeds}/{len(RSS_FEEDS)} feeds")
    return articles

def analyze_sentiment(market_question, articles):
    question_lower = market_question.lower()
    # Extract meaningful keywords (skip short common words)
    stop_words = {"will","the","a","an","in","on","at","to","for","of","and","or","is","be","by","with","this","that","from","are","was","were","has","have","had"}
    keywords = [w for w in question_lower.split() if len(w) > 3 and w not in stop_words]

    relevant = []
    for article in articles:
        matches = sum(1 for kw in keywords if kw in article)
        if matches >= 2:  # Require at least 2 keyword matches for relevance
            relevant.append(article)

    if not relevant:
        return 0, 0, "NEUTRAL", []

    bullish_count = 0
    bearish_count = 0

    for article in relevant:
        for word in BULLISH_WORDS:
            if f" {word} " in f" {article} ":
                bullish_count += 1
        for word in BEARISH_WORDS:
            if f" {word} " in f" {article} ":
                bearish_count += 1

    total = bullish_count + bearish_count
    if total == 0:
        sentiment = "NEUTRAL"
    elif bullish_count > bearish_count * 1.5:
        sentiment = "BULLISH"
    elif bearish_count > bullish_count * 1.5:
        sentiment = "BEARISH"
    else:
        sentiment = "MIXED"

    return bullish_count, bearish_count, sentiment, relevant[:3]

def get_sentiment_emoji(sentiment):
    return {
        "BULLISH": "🟢",
        "BEARISH": "🔴",
        "MIXED":   "🟡",
        "NEUTRAL": "⚪"
    }.get(sentiment, "⚪")

def format_usd(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    return f"${amount:.2f}"

def get_market_alignment(yes_price, sentiment):
    if sentiment == "BULLISH" and yes_price < 0.5:
        return "⚡ CONTRARIAN — Market says NO but sentiment is BULLISH"
    if sentiment == "BEARISH" and yes_price > 0.5:
        return "⚡ CONTRARIAN — Market says YES but sentiment is BEARISH"
    if sentiment == "BULLISH" and yes_price >= 0.5:
        return "✅ ALIGNED — Market and sentiment both BULLISH"
    if sentiment == "BEARISH" and yes_price <= 0.5:
        return "✅ ALIGNED — Market and sentiment both BEARISH"
    return "➖ NEUTRAL — No clear signal"

def build_research_embed(market, bullish, bearish, sentiment, relevant_articles):
    question   = market.get("question", "Unknown")
    slug       = market.get("slug", "")
    liquidity  = float(market.get("liquidity", 0))
    volume_24h = float(market.get("volume24hr", 0))
    prices     = market.get("outcomePrices", ["0.5", "0.5"])
    market_url = f"https://polymarket.com/event/{slug}"

    try:
        yes_price = float(prices[0])
    except:
        yes_price = 0.5

    sentiment_emoji = get_sentiment_emoji(sentiment)
    alignment       = get_market_alignment(yes_price, sentiment)

    color = {
        "BULLISH": 0x2ECC71,
        "BEARISH": 0xE74C3C,
        "MIXED":   0xF39C12,
        "NEUTRAL": 0x888888
    }.get(sentiment, 0x888888)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    fields = [
        {"name": f"{sentiment_emoji} Sentiment",      "value": sentiment,                   "inline": True},
        {"name": "🟢 Bullish Signals",                "value": str(bullish),                "inline": True},
        {"name": "🔴 Bearish Signals",                "value": str(bearish),                "inline": True},
        {"name": "📰 Relevant Articles",              "value": str(len(relevant_articles)), "inline": True},
        {"name": "💰 YES Price",                      "value": f"{yes_price:.1%}",          "inline": True},
        {"name": "💧 Liquidity",                      "value": format_usd(liquidity),       "inline": True},
        {"name": "🔀 Market Alignment",               "value": alignment,                   "inline": False},
        {"name": "🕒 Data As Of",                     "value": now_str,                     "inline": True},
        {"name": "🔗 Market",                         "value": f"[View on Polymarket]({market_url})", "inline": False},
    ]

    if relevant_articles:
        snippet = relevant_articles[0][:300]
        fields.append({"name": "📄 Top Article Snippet", "value": snippet, "inline": False})

    return {
        "title": f"🧠 RESEARCH — {question[:80]}",
        "url":   market_url,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Research  •  Real-time data  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def send_discord(embed):
    if not WEBHOOK_RESEARCH:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_RESEARCH, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            retry_after = float(r.json().get("retry_after", 2))
            print(f"[WARN] Rate limited — waiting {retry_after}s")
            time.sleep(retry_after + 0.5)
            requests.post(WEBHOOK_RESEARCH, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Send failed: {e}")

def run():
    print("Research Agent starting...")
    print(f"Check interval: {CHECK_INTERVAL}s | Feeds: {len(RSS_FEEDS)}")

    while True:
        cycle_start = time.time()
        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{now_str}] Fetching real-time RSS feeds...")

        articles = fetch_rss_articles()

        if not articles:
            print("  No articles fetched — retrying next cycle")
            time.sleep(CHECK_INTERVAL)
            continue

        markets    = get_top_markets(20)
        print(f"  Analyzing {len(markets)} top markets...")

        researched = 0
        for market in markets:
            market_id = market.get("id", "")
            if market_id in seen_research_ids:
                continue

            question = market.get("question", "")
            bullish, bearish, sentiment, relevant = analyze_sentiment(question, articles)

            if sentiment == "NEUTRAL" or len(relevant) == 0:
                seen_research_ids.add(market_id)
                continue

            seen_research_ids.add(market_id)
            embed = build_research_embed(market, bullish, bearish, sentiment, relevant)
            send_discord(embed)
            researched += 1

        if len(seen_research_ids) > 10_000:
            seen_research_ids.clear()

        elapsed = time.time() - cycle_start
        print(f"  Done in {elapsed:.1f}s — {researched} markets researched.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))

if __name__ == "__main__":
    run()
