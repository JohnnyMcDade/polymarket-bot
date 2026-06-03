"""Kalshi edge agent — batched Claude evaluator.

Fetches open Kalshi markets every KALSHI_EDGE_INTERVAL seconds, filters
out dead / illiquid / already-seen markets, then asks Claude (in
batches of EDGE_BATCH_SIZE) for a TRUE_PROBABILITY estimate using the
sports-stats JSON dumped by kalshi_stats.py as context.

Cost discipline (in priority order):
  1. NO Claude call if stats_cache.json is missing or > 24h old.
  2. NO Claude call if there are zero unseen markets in this cycle.
  3. Each call carries the full stats block + methodology in the system
     prompt with cache_control=ephemeral so back-to-back batches hit
     the prompt cache (cross-cycle hits are unlikely at 30-min cadence —
     don't count on them).
  4. Only BUY when |edge| >= KALSHI_MIN_EDGE AND confidence == HIGH.
     Everything else gets dropped silently — no enqueue, no Discord.

Approved trades go to kalshi_queue stage "risk", which kalshi_trader
drains. Reuses the existing 4-stage queue rather than introducing a
new file — matches the prior pipeline's volatility profile.
"""

from __future__ import annotations

import json
import os
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

import kalshi_queue
from kalshi_auth import KALSHI_BASE_URL, get_auth_headers

WEBHOOK_KALSHI_EDGE = os.getenv("WEBHOOK_KALSHI_EDGE", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL_KALSHI_EDGE", "claude-sonnet-4-6")
CHECK_INTERVAL = int(os.getenv("KALSHI_EDGE_INTERVAL", "1800"))   # 30 min
MIN_EDGE = float(os.getenv("KALSHI_MIN_EDGE", "0.10"))             # 10%
BATCH_SIZE = int(os.getenv("KALSHI_EDGE_BATCH_SIZE", "10"))
MAX_MARKETS_PER_CYCLE = int(os.getenv("KALSHI_EDGE_MAX_MARKETS", "40"))
DEBUG_LOG = os.getenv("KALSHI_EDGE_DEBUG_LOG", "").lower() in ("1", "true", "yes")

# Timing window. Pre-game stats only have an edge before the game starts,
# and odds move fast in the last few minutes — so we want close_time to be
# at least MIN_SECS_TO_CLOSE in the future but no more than MAX_SECS_TO_CLOSE.
# OPEN_AGE_IN_PROGRESS + CLOSE_SOON_FOR_IN_PROGRESS together flag "market
# opened hours ago and closes soon" as likely-in-progress and skip it.
MIN_SECS_TO_CLOSE = int(os.getenv("KALSHI_MIN_SECS_TO_CLOSE", "1800"))      # 30 min
MAX_SECS_TO_CLOSE = int(os.getenv("KALSHI_MAX_SECS_TO_CLOSE", "86400"))     # 24 h
OPEN_AGE_IN_PROGRESS = int(os.getenv("KALSHI_OPEN_AGE_IN_PROGRESS", "10800"))   # 3 h
CLOSE_SOON_FOR_IN_PROGRESS = int(os.getenv("KALSHI_CLOSE_SOON_IN_PROGRESS", "7200"))  # 2 h
STATS_CACHE_PATH = Path(os.getenv("KALSHI_STATS_CACHE", "stats_cache.json"))
SEEN_CACHE_PATH = Path(os.getenv("KALSHI_EDGE_SEEN_CACHE", "edge_seen.json"))

# Sports series we actually evaluate. Pulling /markets per series with a
# close-time window cuts the fetch from 5000 mostly-irrelevant rows to
# only in-window contracts the stats cache can actually score.
SERIES_TICKERS = [
    s.strip() for s in os.getenv(
        "KALSHI_EDGE_SERIES",
        "KXMLBGAME,KXMLBTOTAL,KXMLBSPREAD,KXNHLGAME,KXATPMATCH,KXWTAMATCH,"
        "KXNBAGAME,KXAAAGASD,KXCPI,KXFED,KXBTC",
    ).split(",") if s.strip()
]

# ─── Seen-cache: skip markets we've already evaluated ───────────────────

def _load_seen() -> set[str]:
    if not SEEN_CACHE_PATH.exists():
        return set()
    try:
        with SEEN_CACHE_PATH.open() as f:
            data = json.load(f)
        return set(data.get("tickers", []))
    except Exception as e:
        print(f"[WARN] edge_seen.json unreadable: {e}", flush=True)
        return set()


def _save_seen(seen: set[str]) -> None:
    if len(seen) > 20_000:
        # Cap memory + disk. Drop oldest by simple slicing; we have no
        # ordering signal in a set, so this is a coarse trim.
        seen = set(list(seen)[-10_000:])
    tmp = SEEN_CACHE_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump({"tickers": sorted(seen)}, f)
    tmp.replace(SEEN_CACHE_PATH)


# ─── Stats cache ────────────────────────────────────────────────────────

def _load_stats_cache() -> dict[str, Any] | None:
    """Return stats dict if fresh (<24h), else None — caller skips the cycle."""
    if not STATS_CACHE_PATH.exists():
        print("[edge] stats_cache.json missing — skipping cycle", flush=True)
        return None
    try:
        with STATS_CACHE_PATH.open() as f:
            cache = json.load(f)
    except Exception as e:
        print(f"[edge] stats_cache.json unreadable: {e} — skipping cycle", flush=True)
        return None

    fetched_at = cache.get("fetched_at", "")
    if not fetched_at:
        print("[edge] stats_cache.json has no fetched_at — skipping cycle", flush=True)
        return None
    try:
        dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
    except ValueError:
        print(f"[edge] stats_cache.json fetched_at unparseable: {fetched_at!r}", flush=True)
        return None
    # Claude sometimes returns a date-only string ("2026-05-31"), which
    # fromisoformat parses to a NAIVE datetime. Subtracting that from an
    # aware datetime raises TypeError and crashes the whole cycle.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age_h = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    if age_h > 24:
        print(f"[edge] stats_cache is {age_h:.1f}h old (>24h) — skipping cycle", flush=True)
        return None
    return cache


# ─── Market fetch + filter ──────────────────────────────────────────────

def fetch_markets() -> list[dict[str, Any]]:
    path = "/trade-api/v2/markets"
    out: list[dict[str, Any]] = []
    now_ts = int(datetime.now(timezone.utc).timestamp())
    # Kalshi sports markets close 1-3 weeks AFTER the game itself
    # (settlement window) — close_time is NOT the game time. The
    # in-process timing check uses expected_expiration_time instead;
    # this 30-day bound is just a sanity ceiling for the API fetch.
    max_close_ts = now_ts + 30 * 86400
    for series in SERIES_TICKERS:
        cursor = None
        fetched = 0
        try:
            for _ in range(5):
                params: dict[str, Any] = {
                    "limit": 1000,
                    "status": "open",
                    "series_ticker": series,
                    "max_close_ts": max_close_ts,
                }
                if cursor:
                    params["cursor"] = cursor
                r = requests.get(
                    f"{KALSHI_BASE_URL}/markets",
                    headers=get_auth_headers("GET", path),
                    params=params,
                    timeout=15,
                )
                r.raise_for_status()
                data = r.json()
                markets = data.get("markets", []) or []
                out.extend(markets)
                fetched += len(markets)
                cursor = data.get("cursor")
                if not cursor:
                    break
        except Exception as e:
            print(f"[WARN] Kalshi fetch failed series={series}: {e}", flush=True)
            continue
        print(f"[edge] fetched series={series} count={fetched}", flush=True)
    return out


def _parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _check_timing(game_time_str: str, open_time_str: str,
                  now: datetime) -> tuple[bool, str, float]:
    """Return (ok, drop_reason, seconds_to_game).

    game_time_str should be the market's expected_expiration_time /
    occurrence_datetime (the game itself), NOT close_time (which is the
    post-game settlement window — often 1-3 weeks later for NHL/NBA).

    drop_reason is "" when ok=True, otherwise one of:
      no_close, ended, starting_soon, too_far, in_progress
    """
    close_dt = _parse_iso(game_time_str)
    if close_dt is None:
        return False, "no_close", 0.0
    secs = (close_dt - now).total_seconds()
    if secs <= 0:
        return False, "ended", secs
    if secs < MIN_SECS_TO_CLOSE:
        return False, "starting_soon", secs
    if secs > MAX_SECS_TO_CLOSE:
        return False, "too_far", secs
    open_dt = _parse_iso(open_time_str)
    if open_dt is not None:
        opened_ago = (now - open_dt).total_seconds()
        if opened_ago > OPEN_AGE_IN_PROGRESS and secs < CLOSE_SOON_FOR_IN_PROGRESS:
            return False, "in_progress", secs
    return True, "", secs


# Player / team name extraction. The stats cache JSON already names every
# relevant player and team, so the "parse" is really a substring match
# from the title against the cache — that's the safest signal we have
# without a full NER pass.

def _extract_entities(title: str, stats: dict[str, Any]) -> list[str]:
    found: list[str] = []
    title_l = title.lower()

    # MLB players from leader boards + notable list
    mlb = stats.get("mlb", {}) or {}
    candidates: set[str] = set()
    for board in (mlb.get("hitting_leaders", {}) or {}).values():
        for row in board or []:
            if row.get("player"):
                candidates.add(row["player"])
    for board in (mlb.get("pitching_leaders", {}) or {}).values():
        for row in board or []:
            if row.get("player"):
                candidates.add(row["player"])
    for row in mlb.get("notable_players", []) or []:
        if row.get("player"):
            candidates.add(row["player"])
    # NBA active
    for row in (stats.get("nba", {}) or {}).get("active_players", []) or []:
        if row.get("player"):
            candidates.add(row["player"])
    # NHL top scorers
    for row in (stats.get("nhl", {}) or {}).get("top_scorers", []) or []:
        if row.get("player"):
            candidates.add(row["player"])
    # MLB team abbrevs from standings
    teams = set((mlb.get("standings", {}) or {}).keys())

    for name in candidates:
        # Match on last name (more reliable than full-name string match
        # because titles often abbreviate first names).
        last = name.split()[-1].lower() if name else ""
        if last and len(last) >= 4 and last in title_l:
            found.append(name)
    for abbr in teams:
        if abbr and re.search(rf"\b{re.escape(abbr)}\b", title, re.IGNORECASE):
            found.append(abbr)

    # Dedupe, preserve order
    seen: set[str] = set()
    out: list[str] = []
    for e in found:
        if e not in seen:
            seen.add(e)
            out.append(e)
    return out


def _filter_markets(markets: list[dict[str, Any]], stats: dict[str, Any],
                    seen: set[str]) -> list[dict[str, Any]]:
    drops = {"seen": 0, "dead": 0, "illiquid": 0}
    timing_drops = {"no_close": 0, "ended": 0, "starting_soon": 0,
                    "too_far": 0, "in_progress": 0}
    now = datetime.now(timezone.utc)
    kept: list[dict[str, Any]] = []
    for m in markets:
        ticker = m.get("ticker") or ""
        if not ticker:
            continue
        if ticker in seen:
            drops["seen"] += 1
            continue
        ya = float(m.get("yes_ask_dollars", 0) or 0)
        if ya <= 0.0:
            drops["illiquid"] += 1
            continue
        if ya >= 0.99:
            drops["dead"] += 1
            continue
        game_time = (
            m.get("expected_expiration_time")
            or m.get("occurrence_datetime")
            or m.get("close_time", "")
        )
        ok, reason, secs_to_close = _check_timing(
            game_time, m.get("open_time", ""), now
        )
        if not ok:
            timing_drops[reason] += 1
            continue
        title = m.get("title", "") or ""
        entities = _extract_entities(title, stats)
        kept.append({
            "ticker": ticker,
            "title": title,
            "yes_ask_cents": int(round(ya * 100)),
            "hours_left": round(secs_to_close / 3600, 1),
            "close_time": m.get("close_time", ""),
            "open_time": m.get("open_time", ""),
            "entities": entities,
        })
        if len(kept) >= MAX_MARKETS_PER_CYCLE:
            break

    print(
        f"[edge] filter: kept={len(kept)} "
        f"seen={drops['seen']} dead={drops['dead']} illiquid={drops['illiquid']}",
        flush=True,
    )
    print(
        f"[TIMING] dropped={sum(timing_drops.values())} "
        f"ended={timing_drops['ended']} "
        f"starting_soon={timing_drops['starting_soon']} "
        f"in_progress={timing_drops['in_progress']} "
        f"too_far={timing_drops['too_far']} "
        f"no_close={timing_drops['no_close']} "
        f"(window: {MIN_SECS_TO_CLOSE//60}min–{MAX_SECS_TO_CLOSE//3600}h)",
        flush=True,
    )
    return kept


# ─── Claude call ────────────────────────────────────────────────────────

_METHODOLOGY = f"""You are a Kalshi prediction-market edge finder. For each market in the user message, estimate the TRUE probability of YES using the STATS CONTEXT block in this system prompt.

The STATS CONTEXT block has two halves:
- SPORTS STATS — team scoring, pitcher ERA/WHIP, standings, leaders, today's pitching matchups. Use these for MLB / NHL / NBA / ATP / WTA markets.
- ECONOMIC DATA — current national gas price, latest CPI, Fed funds target + next FOMC meeting expectations, BTC spot. Use these for KXAAAGASD / KXCPI / KXFED / KXBTC markets, combined with your own knowledge of macro trends, central-bank reaction functions, and recent price action.

EDGE = true_probability - market_implied_probability  (market price in cents / 100)

RECOMMENDATION RULES
- BUY only if edge >= +{MIN_EDGE:.2f} AND confidence is HIGH AND you have specific, directly relevant data:
    - for sports: the named team or player appears in SPORTS STATS
    - for economic: the current value of the macro variable is in ECONOMIC DATA and resolution is close enough that the variable is unlikely to swing materially
- SKIP in every other case — including BUY_NO opportunities. We only act on positive-edge BUY_YES bets in this build.
- SKIP if the market's resolution depends on something not covered by either block (politics, weather, awards, esports, etc.).

CONFIDENCE GUIDANCE
- HIGH: data directly answers the question (e.g. "Will Judge hit 50 HR?" with HR count + pace in stats; "Will gas avg be < $3.50 on Jun 5?" with current gas at $3.42 and stable trend), market resolves within the next 24 hours, no obvious lurking-variable risk
- MEDIUM: data is relevant but partial (team standings inform a division winner but a lot can change; a CPI print is a week away and you have last month but not consensus)
- LOW: data is tangential or stale relative to the market

RESPONSE FORMAT
For each input market emit exactly one block in this format, separated by a line containing only three dashes:

TICKER: <ticker echoed from input>
TRUE_PROBABILITY: <float 0.0-1.0>
EDGE: <float -1.0-1.0>
CONFIDENCE: <LOW|MEDIUM|HIGH>
RECOMMENDATION: <BUY|SKIP>
REASONING: <one sentence pointing at the specific stat that drove the call>
---

CRITICAL RULES
- Echo TICKER exactly so we can match outputs to inputs.
- Always emit --- after every block including the last.
- No prose before, between, or after the blocks. No markdown fences.
- One block per input market, no skipping, no extras."""


def _build_system_prompt(stats: dict[str, Any]) -> str:
    # Keep the stats JSON compact in the prompt — every extra token is
    # paid for on the first call of the day (cache write). Drop indentation.
    stats_json = json.dumps(stats, separators=(",", ":"))
    return (
        _METHODOLOGY
        + "\n\nSTATS CONTEXT (refresh date in fetched_at, includes both sports and economic blocks):\n"
        + stats_json
    )


def _build_user_message(items: list[dict[str, Any]]) -> str:
    blocks = []
    for i, it in enumerate(items, 1):
        ya = it["yes_ask_cents"]
        entities = ", ".join(it.get("entities") or []) or "(none matched)"
        blocks.append(
            f"=== MARKET {i} ===\n"
            f"TICKER: {it['ticker']}\n"
            f"TITLE: {it['title']}\n"
            f"HOURS UNTIL RESOLUTION: {it['hours_left']}\n"
            f"MARKET YES PRICE: {ya}¢ (implies {ya/100:.2%} YES)\n"
            f"STATS ENTITIES MATCHED: {entities}"
        )
    return "\n\n".join(blocks)


def _parse_response(text: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
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
            print(f"[WARN] edge: unparseable numbers for {ticker}: {fields}", flush=True)
            continue
        if not (0 <= true_prob <= 1) or not (-1 <= edge <= 1):
            print(f"[WARN] edge: out-of-range for {ticker}: prob={true_prob} edge={edge}", flush=True)
            continue
        out[ticker] = {
            "true_probability": true_prob,
            "edge": edge,
            "confidence": fields.get("CONFIDENCE", "LOW").upper(),
            "recommendation": fields.get("RECOMMENDATION", "SKIP").upper(),
            "reasoning": fields.get("REASONING", ""),
        }
    return out


def _ask_claude(system_prompt: str, batch: list[dict[str, Any]],
                log_raw: bool = False) -> dict[str, dict[str, Any]]:
    if not ANTHROPIC_API_KEY or not batch:
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
                "max_tokens": 200 * len(batch),
                "system": [
                    {
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                "messages": [{"role": "user", "content": _build_user_message(batch)}],
            },
            timeout=120,
        )
    except Exception as e:
        print(f"[WARN] edge Claude call failed: {e}", flush=True)
        return {}

    if r.status_code == 429:
        print("[WARN] edge Anthropic rate-limited — skipping batch", flush=True)
        return {}
    if r.status_code != 200:
        print(f"[ERROR] edge Anthropic status={r.status_code}: {r.text[:500]}", flush=True)
        return {}

    body = r.json()
    usage = body.get("usage", {})
    if usage:
        print(
            f"[USAGE] in={usage.get('input_tokens', 0)} "
            f"out={usage.get('output_tokens', 0)} "
            f"cache_create={usage.get('cache_creation_input_tokens', 0)} "
            f"cache_read={usage.get('cache_read_input_tokens', 0)} "
            f"agent=edge batch={len(batch)}",
            flush=True,
        )
    text_parts = [b.get("text", "") for b in body.get("content", []) if b.get("type") == "text"]
    raw_text = "\n".join(text_parts)
    if log_raw and DEBUG_LOG:
        print(f"[edge-debug] raw Claude response (batch_size={len(batch)}):\n{raw_text}\n[edge-debug] end raw response", flush=True)
    return _parse_response(raw_text)


# ─── Discord ────────────────────────────────────────────────────────────

def _build_embed(item: dict[str, Any], pred: dict[str, Any]) -> dict[str, Any]:
    title = item.get("title", item.get("ticker", "?"))
    ticker = item.get("ticker", "")
    market_url = f"https://kalshi.com/markets/{ticker}"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    edge_pct = pred["edge"] * 100
    return {
        "title": f"🎯 KALSHI EDGE — {title[:80]}",
        "url": market_url,
        "color": 0x2ECC71,
        "fields": [
            {"name": "Market Price", "value": f"{item.get('yes_ask_cents', '?')}¢", "inline": True},
            {"name": "True Probability", "value": f"{pred['true_probability']:.1%}", "inline": True},
            {"name": "Edge", "value": f"{edge_pct:+.1f}%", "inline": True},
            {"name": "Confidence", "value": pred["confidence"], "inline": True},
            {"name": "Recommendation", "value": pred["recommendation"], "inline": True},
            {"name": "Hours Left", "value": str(item.get("hours_left", "?")), "inline": True},
            {"name": "Reasoning", "value": pred.get("reasoning", "")[:500] or "—", "inline": False},
            {"name": "Market", "value": f"[View on Kalshi]({market_url})", "inline": False},
        ],
        "footer": {"text": f"PassivePoly Kalshi Edge  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_discord(embed: dict[str, Any]) -> None:
    if not WEBHOOK_KALSHI_EDGE:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_EDGE, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_EDGE, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Discord send failed: {e}")


# ─── Main loop ──────────────────────────────────────────────────────────

def run() -> None:
    print(
        f"Kalshi Edge Agent starting — model={ANTHROPIC_MODEL}, "
        f"interval={CHECK_INTERVAL}s, batch={BATCH_SIZE}, min_edge={MIN_EDGE:.0%}, "
        f"debug_log={DEBUG_LOG}"
    )
    print(
        f"[edge] timing env: MIN_SECS_TO_CLOSE={MIN_SECS_TO_CLOSE} "
        f"MAX_SECS_TO_CLOSE={MAX_SECS_TO_CLOSE} "
        f"OPEN_AGE_IN_PROGRESS={OPEN_AGE_IN_PROGRESS} "
        f"CLOSE_SOON_FOR_IN_PROGRESS={CLOSE_SOON_FOR_IN_PROGRESS}",
        flush=True,
    )
    seen = _load_seen()
    print(f"[edge] loaded {len(seen)} previously-seen tickers", flush=True)
    cycle = 0

    while True:
        cycle += 1
        cycle_start = time.time()
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(
            f"[edge-heartbeat] cycle={cycle} ts={ts} seen={len(seen)} "
            f"interval={CHECK_INTERVAL}s",
            flush=True,
        )
        try:
            stats = _load_stats_cache()
            if stats is None:
                print(f"[edge] cycle={cycle} no-op: stats unavailable", flush=True)
            else:
                markets = fetch_markets()
                print(
                    f"[edge] cycle={cycle} fetched {len(markets)} open markets",
                    flush=True,
                )
                candidates = _filter_markets(markets, stats, seen)

                if not candidates:
                    print(
                        f"[edge] cycle={cycle} no candidates after filter — "
                        "skipping Claude call",
                        flush=True,
                    )
                else:
                    system_prompt = _build_system_prompt(stats)
                    approved = 0
                    skipped = 0
                    for start in range(0, len(candidates), BATCH_SIZE):
                        batch = candidates[start : start + BATCH_SIZE]
                        preds = _ask_claude(system_prompt, batch, log_raw=(start == 0))
                        for it in batch:
                            ticker = it["ticker"]
                            seen.add(ticker)
                            pred = preds.get(ticker)
                            if not pred:
                                skipped += 1
                                if DEBUG_LOG:
                                    print(f"[edge-debug] {ticker} no prediction parsed", flush=True)
                                continue
                            if DEBUG_LOG:
                                print(
                                    f"[edge-debug] {ticker} rec={pred['recommendation']} "
                                    f"edge={pred['edge']:+.3f} conf={pred['confidence']} "
                                    f"true_p={pred['true_probability']:.3f} "
                                    f"ask={it['yes_ask_cents']}c hrs={it['hours_left']} "
                                    f"why={pred.get('reasoning', '')[:200]!r}",
                                    flush=True,
                                )
                            if (
                                pred["recommendation"] != "BUY"
                                or pred["confidence"] != "HIGH"
                                or pred["edge"] < MIN_EDGE
                            ):
                                skipped += 1
                                continue
                            approved += 1
                            payload = {
                                "ticker": ticker,
                                "title": it["title"],
                                "yes_ask": it["yes_ask_cents"],
                                "hours_left": it["hours_left"],
                                "close_time": it["close_time"],
                                "true_probability": pred["true_probability"],
                                "edge": pred["edge"],
                                "confidence": pred["confidence"],
                                "recommendation": "BUY_YES",  # rename for trader's side mapping
                                "reasoning": pred["reasoning"],
                                "side": "yes",
                                "price_for_order_cents": it["yes_ask_cents"],
                            }
                            kalshi_queue.enqueue("risk", ticker, payload)
                            send_discord(_build_embed(it, pred))
                    _save_seen(seen)
                    print(
                        f"[edge] cycle={cycle} done: approved={approved} "
                        f"skipped={skipped}",
                        flush=True,
                    )
        except Exception as e:
            # Print the full traceback so we can pinpoint where the cycle
            # died — bare str(e) was hiding the real cause (e.g. naive vs
            # aware datetime mismatch from a date-only fetched_at).
            print(
                f"[WARN] edge cycle={cycle} crashed: {type(e).__name__}: {e}\n"
                f"{traceback.format_exc()}",
                flush=True,
            )

        elapsed = time.time() - cycle_start
        sleep_s = max(0.0, CHECK_INTERVAL - elapsed)
        next_wake = (datetime.now(timezone.utc)
                     + timedelta(seconds=sleep_s)).strftime("%H:%M:%S")
        print(
            f"[edge-sleep] cycle={cycle} elapsed={elapsed:.1f}s "
            f"sleeping={sleep_s:.0f}s next_wake={next_wake} UTC",
            flush=True,
        )
        time.sleep(sleep_s)


if __name__ == "__main__":
    run()
