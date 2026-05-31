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
KALSHI_PREDICTION_MIN_EDGE = float(os.getenv("KALSHI_PREDICTION_MIN_EDGE", "0.05"))
# Note: keep in sync with the noise-floor value in _SYSTEM_PROMPT — if you
# raise this above 0.05, also update the prompt so Claude's SKIP threshold
# matches the agent's keep threshold.
KALSHI_PREDICTION_BATCH_SIZE = int(os.getenv("KALSHI_PREDICTION_BATCH_SIZE", "5"))
# 200 output tokens per market is enough for the structured response
# (TICKER + 5 short fields + a 1-sentence REASONING). Scaled by batch
# size so a 5-market batch gets 1000 tokens of room.
MAX_TOKENS_PER_MARKET = 200


# System prompt is intentionally substantive — it carries the full
# methodology so per-call user messages can stay lean. Marked cacheable
# below; Anthropic's prompt cache requires ~1024 tokens of cached
# content to activate on Sonnet, so keep this from shrinking.
_SYSTEM_PROMPT = """You are a prediction-market analyst specializing in Kalshi binary outcome markets. Your task is to estimate the TRUE probability of YES for each market in the request and compute the edge versus the current market price.

INPUT FORMAT
Each request contains one or more markets, each introduced by a line of the form "=== MARKET N ===". For every market the input gives you:
- TICKER: the unique Kalshi market identifier — you must echo this exactly in your response so we can match outputs to inputs
- TITLE: the question being asked (resolves YES or NO)
- DAYS UNTIL RESOLUTION: integer number of days remaining before the market settles
- MARKET YES PRICE: the YES side ask in cents (0-100); this is the market-implied probability of YES
- NEWS SENTIMENT: a label (BULLISH / BEARISH / NEUTRAL) and signed score from a coarse keyword-based pre-processor
- RECENT HEADLINES: up to 8 recent news titles judged relevant to the market

EDGE INTERPRETATION
- edge = true_probability - market_implied_probability
- Positive edge means YES is undervalued by the market (consider BUY_YES)
- Negative edge means YES is overvalued by the market (consider BUY_NO)
- Small edges (|edge| < 0.05) are noise — recommend SKIP regardless of direction

CONFIDENCE GUIDANCE
- HIGH: clear news catalyst, strong sentiment alignment with the underlying facts, short resolution window, well-known event
- MEDIUM: moderate signal, some ambiguity, partial information, or a longer window where the trajectory is still readable
- LOW: weak/no headlines, long resolution window with little visibility, conflicting signals, or unfamiliar topic where you cannot reason from first principles

RECOMMENDATION RULES
- BUY_YES only if edge >= +0.05 AND confidence is MEDIUM or HIGH
- BUY_NO only if edge <= -0.05 AND confidence is MEDIUM or HIGH
- SKIP in all other cases — including any LOW-confidence call regardless of edge size, and any market where the headlines do not actually bear on the resolution criterion

CAUTION ON SENTIMENT SCORES
The NEWS SENTIMENT label is from a keyword counter — it is a coarse pre-filter, nothing more. Always read the actual headlines and judge whether they bear on the market's resolution criterion. A "BULLISH" label on headlines unrelated to the resolution question should be treated as no signal, and you should reduce confidence accordingly. Likewise, a "NEUTRAL" or empty headline set on a high-volume short-dated market is itself informative — the market may already price in known information.

RESPONSE FORMAT
For each input market, respond with exactly one block in EXACTLY this format, with blocks separated by a line containing only three dashes:

TICKER: <ticker, echoed from the input>
TRUE_PROBABILITY: <float between 0.0 and 1.0>
EDGE: <float between -1.0 and 1.0, computed as true_probability minus market_implied_probability>
CONFIDENCE: <LOW | MEDIUM | HIGH>
RECOMMENDATION: <BUY_YES | BUY_NO | SKIP>
REASONING: <one or two short sentences explaining the key driver>
---

CRITICAL FORMATTING RULES
- Always include the TICKER line first so responses can be matched to inputs
- Always emit the --- separator between blocks AND after the final block
- Use the EXACT field names shown above, uppercase, followed by a colon and a space
- REASONING must fit on a single line — no internal newlines
- Do not include any other prose before, between, or after the response blocks
- Do not wrap the response in code fences or markdown
- Respond with exactly one block per input market — no skipping inputs, no extra blocks

EXAMPLE 1 — SKIP (edge below threshold)
Input:
=== MARKET 1 ===
TICKER: KXNFLGAME-25DEC25KCBAL-KC
TITLE: Will the Chiefs beat the Ravens on Christmas Day?
DAYS UNTIL RESOLUTION: 3
MARKET YES PRICE: 58¢ (market implies 58.00% YES)
NEWS SENTIMENT: BULLISH (score +0.42)
RECENT HEADLINES:
  - Mahomes returns to full practice ahead of Ravens game
  - Ravens defense ranks 4th in DVOA, will be Chiefs' toughest test
  - Christmas Day kickoff confirmed for 1pm ET

Output:
TICKER: KXNFLGAME-25DEC25KCBAL-KC
TRUE_PROBABILITY: 0.62
EDGE: 0.04
CONFIDENCE: LOW
RECOMMENDATION: SKIP
REASONING: Slight lean toward Chiefs given Mahomes back at full health, but the 4% edge is below the noise threshold and Ravens defense is legitimately strong.
---

EXAMPLE 2 — BUY_NO (clear overpricing of YES)
Input:
=== MARKET 1 ===
TICKER: KXFEDDECISION-26JAN-CUT25
TITLE: Will the Fed cut rates by 25bps at the January meeting?
DAYS UNTIL RESOLUTION: 12
MARKET YES PRICE: 72¢ (market implies 72.00% YES)
NEWS SENTIMENT: BEARISH (score -0.31)
RECENT HEADLINES:
  - CPI print surprises to the upside, core inflation reaccelerates
  - Powell signals patience in Jackson Hole remarks
  - Fed funds futures pricing in fewer cuts after jobs report
  - Two FOMC hawks publicly oppose a January cut

Output:
TICKER: KXFEDDECISION-26JAN-CUT25
TRUE_PROBABILITY: 0.45
EDGE: -0.27
CONFIDENCE: HIGH
RECOMMENDATION: BUY_NO
REASONING: Hot CPI plus hawkish Fed commentary materially undermines the case for a January cut; market YES price has not caught up to the data shift.
---"""


def _build_market_block(idx: int, item: dict[str, Any]) -> str:
    ticker = item.get("ticker", "")
    title = item.get("title", "Unknown market")
    yes_ask = float(item.get("yes_ask", 50))
    market_implied = yes_ask / 100.0
    sentiment_label = item.get("sentiment_label", "NEUTRAL")
    sentiment_score = float(item.get("sentiment_score", 0))
    headlines = item.get("headlines") or []
    days_left = item.get("days_left", "?")

    headlines_text = "\n".join(f"  - {h}" for h in headlines[:8]) or "  (no headlines)"

    return (
        f"=== MARKET {idx} ===\n"
        f"TICKER: {ticker}\n"
        f"TITLE: {title}\n"
        f"DAYS UNTIL RESOLUTION: {days_left}\n"
        f"MARKET YES PRICE: {yes_ask}¢ (market implies {market_implied:.2%} YES)\n"
        f"NEWS SENTIMENT: {sentiment_label} (score {sentiment_score:+.2f})\n"
        f"RECENT HEADLINES:\n{headlines_text}"
    )


def _build_user_message(items: list[dict[str, Any]]) -> str:
    return "\n\n".join(_build_market_block(i + 1, it) for i, it in enumerate(items))


def _parse_batch_response(text: str) -> dict[str, dict[str, Any]]:
    """Parse Claude's multi-market response into {ticker: prediction_dict}.
    Markets with malformed fields are dropped — caller skips any ticker
    missing from the returned dict.
    """
    results: dict[str, dict[str, Any]] = {}
    for block in (b.strip() for b in text.split("---")):
        if not block:
            continue
        fields: dict[str, str] = {}
        for line in block.splitlines():
            m = re.match(r"^\s*([A-Z_]+)\s*:\s*(.+)$", line)
            if m:
                fields[m.group(1).strip()] = m.group(2).strip()
        ticker = fields.get("TICKER", "").strip()
        if not ticker:
            continue
        try:
            true_prob = float(fields.get("TRUE_PROBABILITY", "nan"))
            edge = float(fields.get("EDGE", "nan"))
        except ValueError:
            print(f"[WARN] Unparseable probability/edge for {ticker}: {fields}")
            continue
        if not (0 <= true_prob <= 1):
            print(f"[WARN] true_prob out of range for {ticker}: {true_prob}")
            continue
        if not (-1 <= edge <= 1):
            print(f"[WARN] edge out of range for {ticker}: {edge}")
            continue
        results[ticker] = {
            "true_probability": true_prob,
            "edge": edge,
            "confidence": fields.get("CONFIDENCE", "LOW").upper(),
            "recommendation": fields.get("RECOMMENDATION", "SKIP").upper(),
            "reasoning": fields.get("REASONING", ""),
        }
    return results


def _ask_claude_batch(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Send a batch of markets in one call. Returns {ticker: prediction};
    missing tickers were dropped (parse failure or model omission). Returns
    {} on transport/429 failure — caller skips the whole batch.
    """
    if not ANTHROPIC_API_KEY:
        print("[WARN] ANTHROPIC_API_KEY not set — skipping prediction")
        return {}
    if not items:
        return {}
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
                "max_tokens": MAX_TOKENS_PER_MARKET * len(items),
                "system": [
                    {
                        "type": "text",
                        "text": _SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                "messages": [
                    {"role": "user", "content": _build_user_message(items)}
                ],
            },
            timeout=60,
        )
        if r.status_code != 200:
            print(
                f"[ERROR] Anthropic status={r.status_code} body: {r.text}",
                flush=True,
            )
        if r.status_code == 429:
            print("[WARN] Anthropic rate limited — backing off this cycle", flush=True)
            time.sleep(10)
            return {}
        r.raise_for_status()
        body = r.json()
        text = body["content"][0]["text"]
        # DIAGNOSTIC: dump the raw Claude response so we can see why
        # markets are being SKIP'd. Tag with the first ticker in the
        # batch for orientation. Remove once the SKIP-on-everything
        # issue is understood.
        first_ticker = items[0].get("ticker", "?") if items else "?"
        print(
            f"[CLAUDE-RAW] batch_size={len(items)} first_ticker={first_ticker} "
            f"response:\n{text}",
            flush=True,
        )
        # Log cache stats so we can confirm caching is actually firing;
        # if the system prompt slips back under the model's minimum
        # cacheable size, cache_read will stay at 0 and we'll know.
        usage = body.get("usage", {})
        if usage:
            print(
                f"[USAGE] in={usage.get('input_tokens', 0)} "
                f"out={usage.get('output_tokens', 0)} "
                f"cache_create={usage.get('cache_creation_input_tokens', 0)} "
                f"cache_read={usage.get('cache_read_input_tokens', 0)} "
                f"batch={len(items)}",
                flush=True,
            )
    except Exception as e:
        print(f"[WARN] Claude prediction call failed: {e}", flush=True)
        return {}

    return _parse_batch_response(text)


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


def _diagnose_api_key() -> None:
    """One-shot startup check: surface common Railway env-var corruption
    (hidden newline / trailing space / non-printable byte) BEFORE the
    first Anthropic call so a bad key shows up clearly in deploy logs
    instead of as an opaque 401/400 per request.
    """
    key = os.getenv("ANTHROPIC_API_KEY", "")
    if not key:
        print("[KEYCHECK] ANTHROPIC_API_KEY is empty")
        return
    raw_len = len(key)
    stripped_len = len(key.strip())
    has_ws = key != key.strip()
    has_nonprint = any(not (32 <= ord(c) < 127) for c in key)
    prefix_ok = key.startswith("sk-ant-")
    print(
        f"[KEYCHECK] len={raw_len} (stripped={stripped_len}) "
        f"prefix_ok={prefix_ok} has_surrounding_whitespace={has_ws} "
        f"has_nonprintable_char={has_nonprint} "
        f"head={key[:10]!r} tail={key[-4:]!r}"
    )
    if has_ws or has_nonprint:
        print(
            "[KEYCHECK] WARNING: API key has whitespace or non-printable "
            "bytes — Railway → Variables → ANTHROPIC_API_KEY → re-enter "
            "the last character (or delete + re-paste cleanly)."
        )


def run() -> None:
    print("Kalshi Prediction Agent starting...")
    print(f"  model={ANTHROPIC_MODEL}  min_edge={KALSHI_PREDICTION_MIN_EDGE:.0%}")
    _diagnose_api_key()
    while True:
        cycle_start = time.time()
        items = kalshi_queue.drain_fresh("research")
        if items:
            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                  f"Predicting on {len(items)} markets from research queue...")

        kept = 0
        dropped = 0
        for start in range(0, len(items), KALSHI_PREDICTION_BATCH_SIZE):
            batch = items[start:start + KALSHI_PREDICTION_BATCH_SIZE]
            predictions = _ask_claude_batch(batch)

            for item in batch:
                ticker = item.get("ticker", "")
                prediction = predictions.get(ticker)
                if not prediction:
                    dropped += 1
                    continue
                try:
                    # Edge gate: require absolute edge ≥ MIN_EDGE in the SAME direction
                    # as the recommendation. SKIP recs always drop, regardless of edge.
                    if prediction["recommendation"] == "SKIP":
                        dropped += 1
                        continue
                    if abs(prediction["edge"]) < KALSHI_PREDICTION_MIN_EDGE:
                        dropped += 1
                        continue

                    enriched = {**item, **prediction}
                    kalshi_queue.enqueue("prediction", ticker, enriched)
                    send_discord(_build_embed(item, prediction))
                    kept += 1
                except Exception as e:
                    print(f"[WARN] Prediction handling failed for {ticker}: {e}")
                    dropped += 1

        elapsed = time.time() - cycle_start
        if items:
            print(f"  Done in {elapsed:.1f}s — {kept} kept, {dropped} dropped.")
        time.sleep(max(0, CHECK_INTERVAL - elapsed))


if __name__ == "__main__":
    run()
