"""Kalshi prediction agent — Claude edge finder.

Reads research_queue (markets with sentiment + headlines), asks Claude for
a TRUE_PROBABILITY estimate given market price + news, computes edge as
(true_prob - market_implied_prob), and pushes markets where |edge| >=
MIN_EDGE to prediction_queue.

Markets without edge are dropped silently — that's the filter point in the
pipeline. The risk agent next consumes prediction_queue and decides bet size.
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests

import kalshi_queue

WEBHOOK_KALSHI_PREDICTIONS = os.getenv("WEBHOOK_KALSHI_PREDICTIONS", "")
CHECK_INTERVAL = int(os.getenv("KALSHI_PREDICTION_INTERVAL", "60"))
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL_PREDICTION", "claude-sonnet-4-6")
KALSHI_MIN_EDGE = float(os.getenv("KALSHI_MIN_EDGE", "0.05"))


def _build_prompt(item: dict[str, Any]) -> str:
    title = item.get("title", "Unknown market")
    yes_ask = float(item.get("yes_ask", 50))
    market_implied = yes_ask / 100.0  # in [0,1]
    sentiment_label = item.get("sentiment_label", "NEUTRAL")
    sentiment_score = float(item.get("sentiment_score", 0))
    headlines = item.get("headlines") or []
    days_left = item.get("days_left", "?")

    headlines_text = "\n".join(f"  - {h}" for h in headlines[:8]) or "  (no headlines)"

    return (
        "You are a prediction-market analyst. Estimate the TRUE probability "
        "of YES for this Kalshi market, then compute the edge vs. the market price.\n\n"
        f"MARKET: {title}\n"
        f"DAYS UNTIL RESOLUTION: {days_left}\n"
        f"MARKET YES PRICE: {yes_ask}¢  (market implies {market_implied:.2%} YES)\n"
        f"NEWS SENTIMENT: {sentiment_label} (score {sentiment_score:+.2f})\n"
        "RECENT HEADLINES:\n"
        f"{headlines_text}\n\n"
        "Respond in EXACTLY this format (no extra prose):\n"
        "TRUE_PROBABILITY: <float 0.0-1.0>\n"
        "EDGE: <float, positive if YES is undervalued, negative if YES is overvalued>\n"
        "CONFIDENCE: <LOW | MEDIUM | HIGH>\n"
        "RECOMMENDATION: <BUY_YES | BUY_NO | SKIP>\n"
        "REASONING: <one or two short sentences>"
    )


def _ask_claude(prompt: str) -> dict[str, Any] | None:
    """Returns parsed fields or None on any failure. Anthropic 401 / 429 /
    parse errors all surface as None — caller skips the market.
    """
    if not ANTHROPIC_API_KEY:
        print("[WARN] ANTHROPIC_API_KEY not set — skipping prediction")
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": ANTHROPIC_MODEL,
                "max_tokens": 400,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        if r.status_code == 429:
            print("[WARN] Anthropic rate limited — backing off this cycle")
            time.sleep(10)
            return None
        r.raise_for_status()
        text = r.json()["content"][0]["text"]
    except Exception as e:
        print(f"[WARN] Claude prediction call failed: {e}")
        return None

    fields: dict[str, Any] = {}
    for line in text.splitlines():
        m = re.match(r"^\s*([A-Z_]+)\s*:\s*(.+)$", line)
        if m:
            fields[m.group(1).strip()] = m.group(2).strip()

    try:
        true_prob = float(fields.get("TRUE_PROBABILITY", "nan"))
        edge = float(fields.get("EDGE", "nan"))
    except ValueError:
        print(f"[WARN] Claude returned unparseable probability/edge: {fields}")
        return None
    if not (0 <= true_prob <= 1):
        print(f"[WARN] true_prob out of range: {true_prob}")
        return None
    if not (-1 <= edge <= 1):
        print(f"[WARN] edge out of range: {edge}")
        return None

    return {
        "true_probability": true_prob,
        "edge": edge,
        "confidence": fields.get("CONFIDENCE", "LOW").upper(),
        "recommendation": fields.get("RECOMMENDATION", "SKIP").upper(),
        "reasoning": fields.get("REASONING", ""),
    }


def _build_embed(item: dict[str, Any], prediction: dict[str, Any]) -> dict[str, Any]:
    title = item.get("title", item.get("ticker", "?"))
    ticker = item.get("ticker", "")
    yes_ask = float(item.get("yes_ask", 50))
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    rec = prediction["recommendation"]
    color = 0x2ECC71 if rec == "BUY_YES" else (0xE74C3C if rec == "BUY_NO" else 0x95A5A6)
    edge_pct = prediction["edge"] * 100

    return {
        "title": f"🎯 KALSHI PREDICTION — {title[:80]}",
        "url": market_url,
        "color": color,
        "fields": [
            {"name": "📊 Market Price", "value": f"{yes_ask}¢", "inline": True},
            {"name": "🧮 True Probability",
             "value": f"{prediction['true_probability']:.1%}", "inline": True},
            {"name": "🎯 Edge", "value": f"{edge_pct:+.1f}%", "inline": True},
            {"name": "🔮 Confidence", "value": prediction["confidence"], "inline": True},
            {"name": "💡 Recommendation", "value": rec, "inline": True},
            {"name": "🧠 Sentiment", "value": item.get("sentiment_label", "?"), "inline": True},
            {"name": "💬 Reasoning", "value": prediction.get("reasoning", "")[:500], "inline": False},
            {"name": "🔗 Market", "value": f"[View on Kalshi]({market_url})", "inline": False},
        ],
        "footer": {"text": f"PassivePoly Kalshi Prediction  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_discord(embed: dict[str, Any]) -> None:
    if not WEBHOOK_KALSHI_PREDICTIONS:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_PREDICTIONS, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_PREDICTIONS, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Discord send failed: {e}")


def run() -> None:
    print("Kalshi Prediction Agent starting...")
    print(f"  model={ANTHROPIC_MODEL}  min_edge={KALSHI_MIN_EDGE:.0%}")
    while True:
        cycle_start = time.time()
        items = kalshi_queue.drain_fresh("research")
        if items:
            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                  f"Predicting on {len(items)} markets from research queue...")

        kept = 0
        dropped = 0
        for item in items:
            try:
                prediction = _ask_claude(_build_prompt(item))
                if not prediction:
                    dropped += 1
                    continue

                # Edge gate: require absolute edge ≥ MIN_EDGE in the SAME direction
                # as the recommendation. SKIP recs always drop, regardless of edge.
                if prediction["recommendation"] == "SKIP":
                    dropped += 1
                    continue
                if abs(prediction["edge"]) < KALSHI_MIN_EDGE:
                    dropped += 1
                    continue

                enriched = {**item, **prediction}
                kalshi_queue.enqueue("prediction", item["ticker"], enriched)
                send_discord(_build_embed(item, prediction))
                kept += 1
            except Exception as e:
                print(f"[WARN] Prediction failed for {item.get('ticker', '?')}: {e}")
                dropped += 1

        elapsed = time.time() - cycle_start
        if items:
            print(f"  Done in {elapsed:.1f}s — {kept} kept, {dropped} dropped.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
