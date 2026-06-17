"""One-shot backfill: seed price_history.json from edge_price_history.json.

The line-move guard in kalshi_edge has been writing a single
latest-price entry per ticker to edge_price_history.json for some
time (~3,900 entries on 2026-06-17). The new time-series
price_history.json starts empty after deploy. This script reads the
latest-price file and writes one entry per ticker into the time-series
file so the 30-day backtest has a starting dataset instead of building
from zero.

Idempotent:
  - If price_history.json already has a series for a ticker, that
    ticker is SKIPPED (no double-counting of the latest price).
  - If price_history.json doesn't exist, it's created.
  - The TTL on subsequent record_market_prices() calls will prune any
    entries older than 30 days from this seed, so old backfilled
    points self-clean over time.

Run via SSH:
    cat scripts/backfill_price_history.py | railway ssh <args> 'python3 -'
or directly on the box:
    python3 scripts/backfill_price_history.py
"""

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import kalshi_stats

SRC = os.getenv("KALSHI_EDGE_PRICE_HISTORY", "/app/data/edge_price_history.json")
DST = str(kalshi_stats.PRICE_HISTORY_PATH)


def main() -> int:
    if not os.path.exists(SRC):
        print(f"source {SRC} not found — nothing to backfill")
        return 1
    src = json.load(open(SRC))
    if not isinstance(src, dict):
        print(f"source {SRC} not a dict (type={type(src).__name__})")
        return 1
    print(f"source: {len(src)} tickers in {SRC}")

    try:
        dst = json.load(open(DST))
        if not isinstance(dst, dict):
            dst = {}
    except (FileNotFoundError, json.JSONDecodeError):
        dst = {}
    print(f"destination: {len(dst)} tickers already in {DST}")

    added = 0
    skipped_present = 0
    skipped_bad = 0
    for ticker, entry in src.items():
        if not isinstance(entry, dict):
            skipped_bad += 1
            continue
        if ticker in dst and dst[ticker]:
            skipped_present += 1
            continue
        try:
            ts = float(entry.get("ts", 0))
            price_cents = int(entry.get("price_cents", 0))
        except (TypeError, ValueError):
            skipped_bad += 1
            continue
        if ts <= 0 or not (1 <= price_cents <= 99):
            skipped_bad += 1
            continue
        dst.setdefault(ticker, []).append({"ts": ts, "ask": price_cents})
        added += 1

    text = json.dumps(dst, separators=(",", ":"))
    os.makedirs(os.path.dirname(DST), exist_ok=True)
    open(DST, "w").write(text)
    n_obs = sum(len(v) for v in dst.values())
    print(
        f"backfill done: added={added} "
        f"skipped_already_present={skipped_present} "
        f"skipped_bad={skipped_bad}"
    )
    print(
        f"wrote {DST}: {len(text)} bytes, "
        f"{n_obs} observations across {len(dst)} tickers"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
