#!/usr/bin/env python3
"""Scout open KXATPMATCH / KXWTAMATCH markets through the production
tennis filter, grouped by tournament — Wimbledon called out separately
when present.

Mirrors kalshi_edge._tennis_filter_passes exactly so the PASS list
matches what the live bot would approve if Claude returned HIGH/BUY.
Unlike scripts/dryrun_tennis_filter.py (which fetches rankings fresh
from ESPN), this script loads stats_cache.json so the answers match
what production is currently seeing in its cache.

Run
    railway ssh ... 'python3 /app/scripts/scout_wimbledon.py'
    python3 scripts/scout_wimbledon.py
    python3 scripts/scout_wimbledon.py --stats-cache /tmp/stats_cache.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import requests

from kalshi_auth import KALSHI_BASE_URL, get_auth_headers
import kalshi_edge


def fetch_open_markets(series_ticker: str) -> list[dict]:
    """Paged pull of every currently-open Kalshi market for one series.
    Kalshi pages at 1000; bounded to 5 pages to stay polite — tennis
    fits inside one page in practice."""
    path = "/trade-api/v2/markets"
    out: list[dict] = []
    cursor = None
    for _ in range(5):
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


def load_stats_cache(path: Path) -> dict:
    """Return the parsed stats_cache. Caller-friendly error on missing
    or unreadable file — without rankings the whole filter is moot."""
    if not path.exists():
        print(f"[ERROR] stats_cache not found: {path}", file=sys.stderr)
        print(
            "        Pass --stats-cache /path/to/stats_cache.json, or "
            "copy from Railway:\n"
            "        railway ssh ... 'cat /app/data/stats_cache.json' "
            "> /tmp/stats_cache.json",
            file=sys.stderr,
        )
        sys.exit(1)
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        print(f"[ERROR] stats_cache unreadable: {e}", file=sys.stderr)
        sys.exit(1)


def synth_pred() -> dict:
    """The most permissive Claude verdict possible so the gate result
    is purely determined by the downstream filter. If even this rec
    SKIPs, the market would never trade."""
    return {
        "confidence": "HIGH",
        "recommendation": "BUY",
        "edge": 0.10,
        "true_probability": 0.85,
        "reasoning": "scout synthetic prediction",
    }


def classify_tournament(title: str) -> str:
    """Three-bucket tournament classifier:
      - 'wimbledon' — the slam itself
      - 'grass-other' — every other grass-court event from
        kalshi_edge.GRASS_TOURNAMENTS
      - 'non-grass' — everything else (hard / clay / unknown)

    Kept deliberately coarse: the production grass-specialist gate uses
    the same list, so a market the scout buckets as grass-other will
    also be evaluated by that gate during the filter run."""
    t = (title or "").lower()
    if "wimbledon" in t:
        return "wimbledon"
    if any(g in t for g in kalshi_edge.GRASS_TOURNAMENTS if g != "wimbledon"):
        return "grass-other"
    return "non-grass"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--stats-cache",
        default="/app/data/stats_cache.json",
        help="Path to stats_cache.json (default: /app/data/stats_cache.json)",
    )
    p.add_argument(
        "--tour",
        choices=("atp", "wta", "both"),
        default="both",
        help="Restrict to one tour (default: both)",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print every SKIP individually (default: summary by reason)",
    )
    args = p.parse_args()

    stats = load_stats_cache(Path(args.stats_cache))
    tennis = stats.get("tennis") or {}
    fetched_at = stats.get("fetched_at", "?")
    n_atp = len(tennis.get("atp_rankings") or [])
    n_wta = len(tennis.get("wta_rankings") or [])
    specialists = kalshi_edge._load_grass_specialists()

    print(
        f"SCOUT WIMBLEDON — {datetime.now(timezone.utc).isoformat()}\n"
        f"stats_cache: {args.stats_cache}  (fetched_at={fetched_at})\n"
        f"  atp_rankings: {n_atp}  wta_rankings: {n_wta}  "
        f"grass specialists: {len(specialists)}\n"
        f"  filter thresholds: yes_ask<={kalshi_edge.TENNIS_MAX_ASK_CENTS}c, "
        f"rank_gap>{kalshi_edge.TENNIS_MIN_RANK_GAP}, "
        f"grass_delta>={kalshi_edge.GRASS_MIN_DELTA_DIFF_PP:.1f}pp\n"
    )

    series_list = []
    if args.tour in ("atp", "both"):
        series_list.append("KXATPMATCH")
    if args.tour in ("wta", "both"):
        series_list.append("KXWTAMATCH")

    pred = synth_pred()
    # Per-tournament-bucket accumulators. Each entry is a list of
    # {ticker, title, yes_ask, passes, reason} so we can show passes
    # in full and summarize fails by leading reason token.
    buckets: dict[str, list[dict]] = {
        "wimbledon": [], "grass-other": [], "non-grass": [],
    }
    funnel = {"fetched": 0, "no_ask": 0, "priced": 0}

    for series in series_list:
        try:
            markets = fetch_open_markets(series)
        except requests.RequestException as e:
            print(f"  [ERROR] {series} fetch failed: {e}", flush=True)
            continue
        funnel["fetched"] += len(markets)
        for m in markets:
            ticker = m.get("ticker", "")
            title = m.get("title", "")
            yes_ask_raw = m.get("yes_ask")
            if not isinstance(yes_ask_raw, (int, float)) or yes_ask_raw <= 0:
                funnel["no_ask"] += 1
                # Still bucket the title so the tournament group shows
                # accurate "no live ask" counts — otherwise Wimbledon
                # could be empty just because all books are asleep.
                buckets[classify_tournament(title)].append({
                    "ticker": ticker, "title": title, "yes_ask": None,
                    "passes": False, "reason": "no live yes_ask",
                })
                continue
            yes_ask = int(yes_ask_raw)
            funnel["priced"] += 1
            entities = kalshi_edge._extract_entities(title, stats)
            item = {
                "ticker": ticker, "title": title,
                "yes_ask_cents": yes_ask, "entities": entities,
            }
            ok, reason = kalshi_edge._tennis_filter_passes(item, pred, stats)
            buckets[classify_tournament(title)].append({
                "ticker": ticker, "title": title, "yes_ask": yes_ask,
                "passes": ok, "reason": reason,
            })

    def render_bucket(label: str, rows: list[dict]) -> None:
        print("=" * 78)
        print(f"{label} — {len(rows)} markets")
        print("=" * 78)
        if not rows:
            print("  (no open markets in this bucket)")
            print()
            return
        passes = [r for r in rows if r["passes"]]
        fails = [r for r in rows if not r["passes"]]
        print(f"  PASS: {len(passes)}   SKIP: {len(fails)}\n")
        if passes:
            print(f"  PASSING ({len(passes)}):")
            for r in passes:
                ask_s = f"{r['yes_ask']}c" if r["yes_ask"] is not None else "—"
                print(f"    ✓ {r['ticker']:<42} {ask_s:>4}")
                print(f"        {r['title'][:88]}")
                print(f"        {r['reason']}")
            print()
        if fails:
            if args.verbose:
                print(f"  SKIPPING ({len(fails)}):")
                for r in fails:
                    ask_s = (
                        f"{r['yes_ask']}c" if r["yes_ask"] is not None else "—"
                    )
                    print(f"    ✗ {r['ticker']:<42} {ask_s:>4} — {r['reason']}")
                print()
            else:
                # Group SKIPs by the first three words of the reason —
                # usually identifies the gate (e.g. "gap=12 <=",
                # "yes_ask=78c >", "only 1 ranked").
                counts: Counter = Counter()
                for r in fails:
                    key = " ".join(r["reason"].split()[:3])[:50]
                    counts[key] += 1
                print(f"  SKIP reasons ({len(fails)}):")
                for reason, n in counts.most_common():
                    print(f"    {n:>3}  {reason}")
                print()

    render_bucket("🎾 WIMBLEDON", buckets["wimbledon"])
    render_bucket("🌱 OTHER GRASS EVENTS", buckets["grass-other"])
    render_bucket("🔵 NON-GRASS TENNIS", buckets["non-grass"])

    total_pass = sum(1 for rows in buckets.values() for r in rows if r["passes"])
    total = sum(len(rs) for rs in buckets.values())
    print(
        f"OVERALL: {total_pass} PASS / {total} markets   "
        f"(fetched={funnel['fetched']}, no-ask={funnel['no_ask']}, "
        f"priced={funnel['priced']})"
    )
    if funnel["priced"] == 0 and funnel["fetched"] > 0:
        print(
            "  → all order books asleep; filter has nothing to evaluate. "
            "Re-run during US trading hours."
        )
    print(
        "\nNote: assumes Claude returns HIGH/BUY on every market. In "
        "production most tickers are SKIPed by Claude first — this scout "
        "only surfaces what would happen if it weren't."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
