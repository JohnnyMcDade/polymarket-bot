"""Cross-thread whale signal store.

The Kalshi tracker thread publishes every whale trade it alerts on; the
edge agent thread reads recent ones to boost confidence on markets the
room is already piling into. In-memory only — both writers and readers
are threads in the same launcher process, and stale-on-restart is fine
because the signal is "what whales did in the last hour."
"""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Optional

# Bounded ring of recent whale trades, oldest -> newest. 500 is enough
# for the polling cadence we run at without ever filling and dropping
# fresh signals.
_MAX = 500
_trades: deque[dict] = deque(maxlen=_MAX)
_lock = threading.Lock()


def record_trade(ticker: str, side: str, value_usd: float) -> None:
    """Append a whale trade to the ring. `side` must be the Kalshi
    taker_side string ("yes" or "no")."""
    if not ticker or not side:
        return
    with _lock:
        _trades.append({
            "ticker": ticker,
            "side": side.lower(),
            "value_usd": float(value_usd),
            "ts": time.time(),
        })


def get_signal(ticker: str, side: str = "yes",
               min_value_usd: float = 1000.0,
               max_age_secs: float = 3600.0) -> Optional[dict]:
    """Return the most recent whale trade matching this ticker AND side,
    above the value threshold, within the time window — or None.
    """
    if not ticker:
        return None
    side_l = side.lower()
    cutoff = time.time() - max_age_secs
    with _lock:
        # Iterate newest -> oldest; once we drop below the cutoff the
        # rest of the ring is older too, so stop early.
        for t in reversed(_trades):
            if t["ts"] < cutoff:
                break
            if (t["ticker"] == ticker
                    and t["side"] == side_l
                    and t["value_usd"] >= min_value_usd):
                return dict(t)
    return None
