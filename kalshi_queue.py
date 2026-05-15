"""Thread-safe in-memory queues for the Kalshi pipeline.

Each pipeline stage owns one queue. Items flow:

    kalshi_scanner    → scanner_queue
    kalshi_research   ← scanner_queue   → research_queue
    kalshi_prediction ← research_queue  → prediction_queue
    kalshi_risk       ← prediction_queue → risk_queue
    kalshi_execution  ← risk_queue       → (places orders)

Items are dicts keyed by ticker (Kalshi's market id). Inserting an item
that already exists in a stage replaces the old entry — the freshest read
wins. Each entry carries a `_queued_at` timestamp; consumers automatically
drop anything older than STALE_AFTER_SECONDS (default 1h) so dead market
data can't pile up if a downstream agent is failing.

All mutations go through a single `threading.Lock` shared across stages.
The lock is held for microseconds — pure dict mutations, no IO inside it.

Usage (consumer pattern):

    items = kalshi_queue.drain_fresh("scanner")
    for it in items:
        try:
            enriched = process(it)
            kalshi_queue.enqueue("research", it["ticker"], enriched)
        except Exception as e:
            log.warning("dropped item: %s", e)

This is in-memory: every kalshi agent MUST run as a thread in the same
process (see launcher.py). Subprocess-spawned agents would each see an
empty queue.
"""

from __future__ import annotations

import threading
import time
from typing import Any

# Default: drop items older than 1 hour. Set via KALSHI_QUEUE_STALE_SECONDS.
import os
STALE_AFTER_SECONDS = int(os.getenv("KALSHI_QUEUE_STALE_SECONDS", "3600"))

_VALID_STAGES = ("scanner", "research", "prediction", "risk")

_lock = threading.Lock()
_stages: dict[str, dict[str, dict[str, Any]]] = {s: {} for s in _VALID_STAGES}


def _check_stage(stage: str) -> None:
    if stage not in _VALID_STAGES:
        raise ValueError(f"unknown stage {stage!r}; valid: {_VALID_STAGES}")


def enqueue(stage: str, ticker: str, data: dict[str, Any]) -> None:
    """Add or replace an item in `stage`'s queue.

    Replacing rather than appending means if the scanner re-finds the same
    market 10 minutes later with newer prices, downstream agents see the
    updated snapshot — they never operate on stale numbers when fresher
    ones are available.
    """
    _check_stage(stage)
    if not ticker:
        return  # silently drop — no usable key
    with _lock:
        _stages[stage][ticker] = {
            "data": data,
            "_queued_at": time.time(),
        }


def drain_fresh(stage: str) -> list[dict[str, Any]]:
    """Return all items currently in `stage`, dropping stale ones, then
    clear the stage. Items are returned as flat dicts: the stored `data`
    payload merged with `ticker` and `_queued_at` keys.

    The drain-and-clear pattern means each consumer cycle processes a
    snapshot. If processing fails the items are gone — accept that loss
    in exchange for simplicity. Items the scanner sees again next cycle
    will re-enter the pipeline naturally.
    """
    _check_stage(stage)
    cutoff = time.time() - STALE_AFTER_SECONDS
    out: list[dict[str, Any]] = []
    with _lock:
        items = list(_stages[stage].items())
        _stages[stage].clear()
    # Filter outside the lock — cheap and keeps the critical section tiny.
    for ticker, entry in items:
        if entry["_queued_at"] < cutoff:
            continue
        out.append({
            "ticker": ticker,
            "_queued_at": entry["_queued_at"],
            **entry["data"],
        })
    return out


def peek_counts() -> dict[str, int]:
    """Return current item count per stage. Diagnostic, non-mutating.

    Useful for the launcher / a future health probe to see whether the
    pipeline is flowing or backed up.
    """
    with _lock:
        return {stage: len(items) for stage, items in _stages.items()}


def clear_all() -> None:
    """Drop everything from every stage. For tests / hard restarts only."""
    with _lock:
        for stage in _VALID_STAGES:
            _stages[stage].clear()
