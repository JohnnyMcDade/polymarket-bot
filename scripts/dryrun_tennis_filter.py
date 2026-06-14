#!/usr/bin/env python3
"""Dry-run the tennis production filter against TODAY's live Kalshi markets.

WHAT THIS IS:
  A one-shot script you run from your laptop to answer: "for every
  KXATPMATCH / KXWTAMATCH market currently open on Kalshi, would our
  production filter let it through if Claude returned HIGH+BUY?" It
  bypasses the Claude call entirely — assumes pred = {confidence: HIGH,
  recommendation: BUY} — so it tests the deterministic gates
  downstream of the model: tennis mispricing filter, grass-specialist
  filter, line-move guard. Doesn't touch the recalibration demote
  (lives in trades_log on Railway).

WHAT IT'S FOR:
  - Wimbledon prep on June 14, 2026: confirm the wire-up works on real
    tennis markets BEFORE the next 30-min production cycle, not after.
  - During Wimbledon: rerun anytime to see what the filter would pick
    for tomorrow's matches with current rankings + grass-specialist
    data, without waiting for a deploy.

WHAT IT IS NOT:
  - It does NOT decide whether a real trade happens. It only mirrors
    the post-Claude gate logic. Real trades still require Claude to
    return HIGH+BUY for a given market.
  - It does NOT use the live edge_seen.json cache, so the same
    markets that production already SKIP'd as "seen" will still show
    up here as candidates.

Run:
    python3 scripts/dryrun_tennis_filter.py
    python3 scripts/dryrun_tennis_filter.py --tour atp
    python3 scripts/dryrun_tennis_filter.py --verbose
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# Run from project root so kalshi_auth / kalshi_edge / kalshi_stats resolve.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import requests

from kalshi_auth import KALSHI_BASE_URL, get_auth_headers
import kalshi_stats  # for fetching fresh tennis rankings + recent matches
import kalshi_edge   # for _tennis_filter_passes + _extract_entities


def fetch_open_markets(series_ticker: str) -> list[dict]:
    """Pull all currently-open Kalshi markets for one series."""
    path = "/trade-api/v2/markets"
    out: list[dict] = []
    cursor = None
    for _ in range(5):  # bounded loop; Kalshi pages at 1000 per call
        params: dict = {
            "limit": 1000,
            "status": "open",
            "series_ticker": series_ticker,
        }
        if cursor:
            params["cursor"] = cursor
        r = requests.get(
            f"{KALSHI_BASE_URL}/markets",
            headers=get_auth_headers("GET", path),
            params=params,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("markets", []) or [])
        cursor = data.get("cursor")
        if not cursor:
            break
    return out


def build_stats_block() -> dict:
    """Fresh tennis rankings + recent matches via kalshi_stats. Avoids
    SSHing into the container — we re-pull from ESPN to keep this
    script standalone."""
    tennis = kalshi_stats._fetch_tennis_block()
    return {"tennis": tennis}


def synth_pred() -> dict:
    """Synthesize Claude's verdict as the most permissive one possible
    so we test the downstream gates. If even this hypothetical HIGH/BUY
    rec gets rejected, the market would never trade regardless of what
    Claude actually said. Edge of 0.10 is mid-range; not the cap test."""
    return {
        "confidence": "HIGH",
        "recommendation": "BUY",
        "edge": 0.10,
        "true_probability": 0.85,
        "reasoning": "DRY-RUN synthetic prediction",
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tour", choices=("atp", "wta", "both"), default="both")
    p.add_argument("--verbose", action="store_true",
                   help="Print full reason on every market (not just PASSes)")
    args = p.parse_args()

    print("Pulling fresh tennis rankings + recent matches...", flush=True)
    stats = build_stats_block()
    t = stats["tennis"]
    print(
        f"  atp_rankings: {len(t.get('atp_rankings') or [])}  "
        f"wta_rankings: {len(t.get('wta_rankings') or [])}  "
        f"atp_recent: {len(t.get('atp_recent') or [])}  "
        f"wta_recent: {len(t.get('wta_recent') or [])}",
        flush=True,
    )

    specialists = kalshi_edge._load_grass_specialists()
    print(f"  grass specialists loaded: {len(specialists)}")

    series_list = []
    if args.tour in ("atp", "both"):
        series_list.append("KXATPMATCH")
    if args.tour in ("wta", "both"):
        series_list.append("KXWTAMATCH")

    print()
    pred = synth_pred()
    passes: list[dict] = []
    fails: list[tuple[str, str, dict]] = []
    funnel = {"fetched": 0, "no_ask": 0, "priced": 0}

    for series in series_list:
        print(f"Fetching open {series} markets...", flush=True)
        try:
            markets = fetch_open_markets(series)
        except Exception as e:
            print(f"  [ERROR] {series} fetch failed: {e}", flush=True)
            continue
        print(f"  {len(markets)} open markets", flush=True)
        funnel["fetched"] += len(markets)

        for m in markets:
            ticker = m.get("ticker", "")
            title = m.get("title", "")
            yes_ask = m.get("yes_ask")
            if not isinstance(yes_ask, (int, float)) or yes_ask <= 0:
                # Open but no live order book — production drops these
                # as "illiquid". Surface the count here so the dry-run
                # explains "0 PASS" when the books are simply asleep.
                funnel["no_ask"] += 1
                continue
            yes_ask = int(yes_ask)
            funnel["priced"] += 1

            entities = kalshi_edge._extract_entities(title, stats)
            item = {
                "ticker": ticker,
                "title": title,
                "yes_ask_cents": yes_ask,
                "entities": entities,
            }
            ok, reason = kalshi_edge._tennis_filter_passes(item, pred, stats)
            if ok:
                passes.append({"ticker": ticker, "title": title,
                               "yes_ask": yes_ask, "reason": reason})
            else:
                fails.append((ticker, reason, item))
                if args.verbose:
                    print(
                        f"  SKIP {ticker:<40} {yes_ask:>3}c — {reason}",
                        flush=True,
                    )

    # ── Report ────────────────────────────────────────────────────────
    print()
    print("=" * 78)
    print(f"DRY-RUN RESULT — {datetime.now(timezone.utc).isoformat()}")
    print("=" * 78)
    print(
        f"\nFunnel: fetched={funnel['fetched']} | "
        f"open-but-no-ask={funnel['no_ask']} | "
        f"with-live-ask={funnel['priced']}"
    )
    if funnel["priced"] == 0 and funnel["fetched"] > 0:
        print(
            "  → no live order books right now; filter has nothing to "
            "evaluate. Re-run during US trading hours."
        )

    print(f"\nPASSING markets ({len(passes)}):")
    if passes:
        for p in passes:
            print(f"  ✓ {p['ticker']:<42} {p['yes_ask']:>3}c")
            print(f"      {p['title'][:80]}")
            print(f"      {p['reason']}")
    else:
        print("  (none — no current Kalshi tennis market would clear the gate)")

    # Categorize fails by the leading reason token. Helps you see at a
    # glance which gate is killing the most candidates — e.g. is it
    # mostly "gap too small", "ask too high", "missing rankings", or
    # the grass-specialist requirement?
    print(f"\nSKIPPED markets ({len(fails)}) — grouped by reason:")
    bucket = Counter()
    for _, reason, _ in fails:
        # First few words usually identify the gate
        key = " ".join(reason.split()[:3])[:50]
        bucket[key] += 1
    for reason, n in bucket.most_common():
        print(f"  {n:>4}  {reason}")

    print(
        f"\nTotal: {len(passes)} PASS, {len(fails)} SKIP "
        f"(of {len(passes) + len(fails)} markets with live ask prices)"
    )
    print(
        f"\nNote: this assumes Claude returns HIGH/BUY on every market. "
        f"In production, Claude SKIPs most tickers first; this script "
        f"surfaces what would happen if it didn't."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
