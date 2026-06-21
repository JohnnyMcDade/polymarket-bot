#!/usr/bin/env python3
"""Refresh ~/polymarket-bot/data/local_cache.json with all open markets
for the series scouts care about, by hitting Kalshi's public /markets
endpoint (no auth required).

Lets scout_*.py and ad-hoc exploration run offline against a recent
snapshot instead of needing `railway ssh` or live network for every
iteration. Re-run whenever the snapshot feels stale.

    python3 scripts/refresh_local_cache.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests


KALSHI_MARKETS_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
SERIES = ["KXMLBTOTAL", "KXMLBSPREAD", "KXATPMATCH", "KXWTAMATCH"]
CACHE_PATH = Path.home() / "polymarket-bot" / "data" / "local_cache.json"


def fetch_series(series_ticker: str) -> list[dict]:
    """Page through every open market for one series. Kalshi caps page
    size at 1000 and returns a `cursor` on each response until the last
    page (empty string)."""
    markets: list[dict] = []
    cursor = ""
    while True:
        params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor
        r = requests.get(KALSHI_MARKETS_URL, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        markets.extend(body.get("markets", []))
        cursor = body.get("cursor") or ""
        if not cursor:
            break
    return markets


def main() -> int:
    snapshot: dict[str, list[dict]] = {}
    for series in SERIES:
        snapshot[series] = fetch_series(series)
        print(f"  {series}: {len(snapshot[series])} open markets")

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "refreshed_at": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "markets": snapshot,
    }
    CACHE_PATH.write_text(json.dumps(payload, indent=2))
    total = sum(len(v) for v in snapshot.values())
    print(f"Wrote {total} markets to {CACHE_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
