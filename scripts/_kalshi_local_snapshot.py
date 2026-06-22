"""Helper for scouts to fall back to a local Kalshi snapshot when the
public API is unreachable (e.g. flaky wifi, dev offline, Railway SSH
not available).

Two on-disk snapshots are checked, in this order:

  1. data/kalshi_snapshot.json — single-series payload written by the
     `python3 -c "..."` one-liner documented in the README. Contains
     whatever series was last snapshotted (typically KXMLBTOTAL).
  2. data/local_cache.json — multi-series payload written by
     scripts/refresh_local_cache.py. Carries KXMLBTOTAL, KXMLBSPREAD,
     KXATPMATCH, KXWTAMATCH.

Returns the list of markets matching `series_ticker`, or [] if no
snapshot covers it. Callers should treat [] as "no fallback available"
and surface that to the operator rather than silently swallowing it.
"""
from __future__ import annotations

import json
from pathlib import Path


REPO_DATA = Path(__file__).resolve().parent.parent / "data"
SNAPSHOT_PATH = REPO_DATA / "kalshi_snapshot.json"
LOCAL_CACHE_PATH = REPO_DATA / "local_cache.json"


def load_local_markets(series_ticker: str) -> tuple[list[dict], str | None]:
    """Return (markets, source_path) for the series, falling back across
    both snapshot files. Source is None when no snapshot has the
    series."""
    if SNAPSHOT_PATH.exists():
        try:
            payload = json.loads(SNAPSHOT_PATH.read_text())
        except (OSError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, dict):
            markets = [
                m for m in payload.get("markets", [])
                if (m.get("ticker") or "").startswith(series_ticker)
            ]
            if markets:
                return markets, str(SNAPSHOT_PATH)

    if LOCAL_CACHE_PATH.exists():
        try:
            payload = json.loads(LOCAL_CACHE_PATH.read_text())
        except (OSError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, dict):
            by_series = payload.get("markets") or {}
            if isinstance(by_series, dict):
                markets = by_series.get(series_ticker) or []
                if markets:
                    return markets, str(LOCAL_CACHE_PATH)

    return [], None
