"""Kalshi research agent — adds news sentiment to scanner-flagged markets.

Reads the scanner_queue, fetches recent news headlines from Google News
RSS (with the market title as the search query) and ESPN's top-headlines
feed (for sports markets), runs a lightweight keyword-based sentiment
score, then pushes the enriched market to the research_queue.

Sentiment is intentionally simple (positive/negative keyword count over
the headline corpus). The heavy lifting — actual edge detection — happens
in kalshi_prediction.py with a Claude call that gets these headlines as
input. The research agent's job is to provide that context, not to
guess at probabilities itself.
"""

from __future__ import annotations

import os
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any

import feedparser
import requests

import kalshi_queue

WEBHOOK_KALSHI_RESEARCH = os.getenv("WEBHOOK_KALSHI_RESEARCH", "")
CHECK_INTERVAL = int(os.getenv("KALSHI_RESEARCH_INTERVAL", "60"))
MAX_HEADLINES_PER_MARKET = int(os.getenv("KALSHI_RESEARCH_HEADLINES", "8"))

# Word lists for the keyword scorer. Kept short on purpose — false positives
# from a too-aggressive lexicon are worse than misses, since Claude in
# kalshi_prediction sees the actual headlines and can override our score.
_POSITIVE_WORDS = {
    "win", "wins", "beat", "beats", "victory", "victorious", "surge", "soars",
    "rally", "rallies", "boost", "boosts", "gain", "gains", "rise", "rises",
    "advance", "advances", "lead", "leads", "ahead", "favored", "favorite",
    "approve", "approved", "pass", "passes", "passed", "succeed", "succeeds",
    "agreement", "deal", "support", "supports", "endorse", "endorsed",
    "record", "high", "milestone", "growth", "expand", "expands",
}
_NEGATIVE_WORDS = {
    "lose", "loses", "lost", "defeat", "defeats", "defeated", "fall", "falls",
    "drop", "drops", "plunge", "plunges", "slump", "slumps", "decline", "declines",
    "trail", "trails", "behind", "underdog", "reject", "rejected", "block", "blocks",
    "blocked", "fail", "fails", "failed", "crisis", "scandal", "investigate",
    "investigated", "indict", "indicted", "delay", "delayed", "postpone",
    "postponed", "cancel", "cancelled", "suspended", "ban", "banned", "ruling against",
}

# Sports keywords used to decide whether to pull ESPN's feed alongside
# Google News. Lighter than scanner's full set — we just need a coarse
# "is this sports?" check.
_SPORTS_HINT = {
    "nba", "nfl", "mlb", "nhl", "ufc", "championship", "playoff", "playoffs",
    "world cup", "super bowl", "world series", "stanley cup", "vs.", "game",
    "match", "tournament",
}

# ESPN top-headlines feed. Filtered locally for relevance.
_ESPN_FEED = "https://www.espn.com/espn/rss/news"


def _is_sports(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in _SPORTS_HINT)


def _google_news_query(title: str) -> str:
    """Build a Google News RSS URL for the market's headline. Trims to a
    few high-signal words so we don't drown the search in market-template
    fluff like "Will the..." or "by EOY 2026".
    """
    # Drop common market-template prefixes.
    cleaned = title
    for prefix in ("Will ", "Will the ", "Will any ", "Will it "):
        if cleaned.lower().startswith(prefix.lower()):
            cleaned = cleaned[len(prefix):]
            break
    # Take up to ~10 meaningful words.
    words = [w for w in cleaned.split() if len(w) > 2][:10]
    q = " ".join(words)
    return (
        "https://news.google.com/rss/search?"
        + urllib.parse.urlencode({"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"})
    )


def fetch_headlines(market_title: str) -> list[str]:
    """Pulls headlines relevant to `market_title` from Google News + (if
    sports) ESPN. Returns up to MAX_HEADLINES_PER_MARKET strings.
    """
    headlines: list[str] = []

    # Google News — keyword search
    try:
        feed = feedparser.parse(_google_news_query(market_title))
        for entry in feed.entries[: MAX_HEADLINES_PER_MARKET]:
            title = (entry.get("title") or "").strip()
            if title:
                headlines.append(title)
    except Exception as e:
        print(f"[WARN] Google News fetch failed for {market_title!r}: {e}")

    # ESPN — only filter the global feed for sports markets
    if _is_sports(market_title) and len(headlines) < MAX_HEADLINES_PER_MARKET:
        try:
            feed = feedparser.parse(_ESPN_FEED)
            # Filter ESPN entries to ones that share keywords with the market title.
            keywords = {w.lower() for w in market_title.split() if len(w) > 3}
            for entry in feed.entries:
                title = (entry.get("title") or "").strip()
                title_lower = title.lower()
                if title and any(kw in title_lower for kw in keywords):
                    headlines.append(title)
                    if len(headlines) >= MAX_HEADLINES_PER_MARKET:
                        break
        except Exception as e:
            print(f"[WARN] ESPN fetch failed: {e}")

    return headlines[:MAX_HEADLINES_PER_MARKET]


def score_sentiment(headlines: list[str]) -> tuple[float, str]:
    """Returns (score, label). Score is in [-1, +1].
      label ∈ {"BULLISH", "BEARISH", "NEUTRAL"} — bullish == positive news
      flow, which suggests the YES side is more likely.
    """
    if not headlines:
        return 0.0, "NEUTRAL"
    pos = neg = 0
    for h in headlines:
        hl = h.lower()
        pos += sum(1 for w in _POSITIVE_WORDS if w in hl)
        neg += sum(1 for w in _NEGATIVE_WORDS if w in hl)
    if pos + neg == 0:
        return 0.0, "NEUTRAL"
    score = (pos - neg) / (pos + neg)
    if score > 0.15:
        return score, "BULLISH"
    if score < -0.15:
        return score, "BEARISH"
    return score, "NEUTRAL"


def _build_embed(item: dict[str, Any], sentiment_label: str, score: float,
                 headlines: list[str]) -> dict[str, Any]:
    title = item.get("title", item.get("ticker", "?"))
    ticker = item.get("ticker", "")
    color = {"BULLISH": 0x2ECC71, "BEARISH": 0xE74C3C, "NEUTRAL": 0x95A5A6}[sentiment_label]
    label_emoji = {"BULLISH": "📈", "BEARISH": "📉", "NEUTRAL": "➖"}[sentiment_label]
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    headline_preview = "\n".join(f"• {h[:90]}" for h in headlines[:5]) or "_(no headlines found)_"

    return {
        "title": f"🧠 KALSHI RESEARCH — {title[:80]}",
        "url": market_url,
        "color": color,
        "fields": [
            {"name": f"{label_emoji} Sentiment", "value": f"{sentiment_label} ({score:+.2f})", "inline": True},
            {"name": "📰 Headlines", "value": str(len(headlines)), "inline": True},
            {"name": "💲 YES Price", "value": f"{item.get('yes_ask', '?')}¢", "inline": True},
            {"name": "📌 Top Headlines", "value": headline_preview[:1000], "inline": False},
            {"name": "🔗 Market", "value": f"[View on Kalshi]({market_url})", "inline": False},
        ],
        "footer": {"text": f"PassivePoly Kalshi Research  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_discord(embed: dict[str, Any]) -> None:
    if not WEBHOOK_KALSHI_RESEARCH:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_RESEARCH, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_RESEARCH, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Discord send failed: {e}")


def run() -> None:
    print("Kalshi Research Agent starting...")
    while True:
        cycle_start = time.time()
        items = kalshi_queue.drain_fresh("scanner")
        if items:
            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                  f"Researching {len(items)} markets from scanner queue...")

        for item in items:
            try:
                headlines = fetch_headlines(item.get("title", item.get("ticker", "")))
                score, label = score_sentiment(headlines)

                enriched = {
                    **item,
                    "sentiment_score": round(score, 3),
                    "sentiment_label": label,
                    "headlines": headlines,
                }
                kalshi_queue.enqueue("research", item["ticker"], enriched)
                send_discord(_build_embed(item, label, score, headlines))
            except Exception as e:
                print(f"[WARN] Research failed for {item.get('ticker', '?')}: {e}")

        elapsed = time.time() - cycle_start
        if items:
            print(f"  Done in {elapsed:.1f}s — {len(items)} markets researched.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
