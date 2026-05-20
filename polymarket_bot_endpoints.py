"""HTTP read-only API used by the ugc-pipeline (@passivepoly TikTok account).

DROP THIS FILE INTO THE polymarket-bot REPO ALONGSIDE launcher.py.

Wiring (two lines added to launcher.py):

    # near the top of launcher.py
    import threading
    from polymarket_bot_endpoints import start_api_server

    # at the very end, AFTER the 6 agent threads have been started
    threading.Thread(
        target=start_api_server,
        kwargs={"host": "0.0.0.0", "port": int(os.environ.get("PORT", 8000))},
        daemon=True,
    ).start()

Railway exposes the value of the PORT env var as the externally-routable port
for the service, so the API will be reachable at the service URL once deployed.

Add to polymarket-bot/requirements.txt (likely already mostly there):
    fastapi>=0.115
    uvicorn[standard]>=0.30
    pydantic>=2.7

Auth: Bearer token in PASSIVEPOLY_BACKEND_TOKEN env. Generate one (any random
string, e.g. `python -c 'import secrets; print(secrets.token_urlsafe(32))'`)
and set the same value in the ugc-pipeline's .env as PASSIVEPOLY_BACKEND_TOKEN
and PASSIVEPOLY_BACKEND_URL=https://<your-railway-service>.up.railway.app .

Endpoints (READ-ONLY — these never mutate state):
  GET /api/alerts/today
  GET /api/stats/win-loss?days=N
  GET /api/whales/biggest?hours=N
  GET /api/markets/notable-resolution

Each endpoint has a clearly-marked `# TODO(data layer)` block where you fill
in the actual read against whatever store your 6 agents write to (Postgres,
SQLite, Redis, JSON files, in-memory queue). The response schemas below
match what the ugc-pipeline's `integrations/passivepoly_backend.py` expects;
keep these shapes — that's the contract.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field


# ── Polymarket public APIs (same ones polymarket_bot.py + scanner_agent.py
# + postmortem_agent.py use). Each `_fetch_*` below re-derives the answer
# from these instead of reading the bot's in-memory state, because the bot
# is a separate subprocess from this API server (launcher.py spawns it via
# subprocess.run) and there is no shared persistence layer. The thresholds
# and selection rules mirror the bot — same min trade size, same leaderboard
# size, same resolution semantics — so the answers are equivalent to what
# the bot would have alerted on for the window, not a separate signal.
_DATA_API = "https://data-api.polymarket.com"
_GAMMA_API = "https://gamma-api.polymarket.com"

# Defaults match polymarket_bot.py's CHECK_INTERVAL / MIN_TRADE_SIZE /
# TOP_N_TRADERS env defaults — override on Railway to keep the API and bot
# in sync if you tune the bot.
_MIN_TRADE_SIZE = float(os.getenv("MIN_TRADE_SIZE", 1000))
_TOP_N_TRADERS = int(os.getenv("TOP_N_TRADERS", 15))
_HTTP_TIMEOUT = 15
_MIN_NOTABLE_MARKET_VOLUME = float(os.getenv("MIN_NOTABLE_VOLUME", 10000))


class _PolymarketUnavailable(Exception):
    """Raised when a downstream Polymarket API call fails. Endpoints map to 502."""


# --- Auth -------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=False)


def _require_token(creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> None:
    expected = os.environ.get("PASSIVEPOLY_BACKEND_TOKEN")
    if not expected:
        # Refuse to serve unauthenticated traffic. Fail closed.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="server is missing PASSIVEPOLY_BACKEND_TOKEN env var",
        )
    if creds is None or creds.scheme.lower() != "bearer" or creds.credentials != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing bearer token",
        )


# --- Response schemas (the contract with ugc-pipeline) ----------------------

class WhaleAlert(BaseModel):
    id: str
    timestamp: datetime
    market: str
    market_id: str | None = None
    whale_address: str | None = None
    amount_usd: float
    direction: str = Field(description='"BUY YES" / "BUY NO" / "SELL YES" / "SELL NO"')
    implied_probability_before: float | None = None
    implied_probability_after: float | None = None
    system_confidence: float | None = Field(default=None, description="0.0-1.0, system's read on the alert")


class WinLossSnapshot(BaseModel):
    window_days: int
    wins: int
    losses: int
    pending: int = 0
    total_alerts: int
    win_rate: float
    biggest_win_pct: float | None = None
    biggest_loss_pct: float | None = None


class BiggestWhaleMove(BaseModel):
    window_hours: int
    whale_address: str | None = None
    market: str
    market_id: str | None = None
    amount_usd: float
    direction: str
    implied_prob_change: float | None = None
    timestamp: datetime


class NotableResolution(BaseModel):
    market: str
    market_id: str | None = None
    resolved_at: datetime
    outcome: str
    system_called_it_correctly: bool | None = None
    system_confidence_at_call: float | None = None


# --- App --------------------------------------------------------------------

app = FastAPI(
    title="PassivePoly Backend API",
    description="Read-only HTTP surface consumed by the ugc-pipeline.",
    version="1.0.0",
)


@app.exception_handler(_PolymarketUnavailable)
def _polymarket_unavailable_handler(_request, exc):
    """Turn upstream Polymarket failures into clean 502s instead of 500s."""
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content={"detail": f"upstream Polymarket API unavailable: {exc}"},
    )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Liveness probe. Public (no auth) so Railway can hit it."""
    return {"status": "ok"}


@app.get("/api/alerts/today", response_model=list[WhaleAlert], dependencies=[Depends(_require_token)])
def alerts_today() -> list[WhaleAlert]:
    """All whale-tracker alerts fired since 00:00 UTC today."""
    cutoff = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # TODO(data layer): replace with the real read from your alerts store.
    # Examples:
    #   - SQLAlchemy:    return [WhaleAlert.model_validate(row) for row in session.scalars(select(Alert).where(Alert.timestamp >= cutoff)).all()]
    #   - Postgres+psycopg: cur.execute("SELECT ... WHERE timestamp >= %s", (cutoff,))
    #   - JSON files:    glob data/alerts/*.json filtered by timestamp
    rows = _fetch_alerts_since(cutoff)
    return [WhaleAlert(**row) for row in rows]


@app.get("/api/stats/win-loss", response_model=WinLossSnapshot, dependencies=[Depends(_require_token)])
def win_loss(days: int = Query(default=7, ge=1, le=90)) -> WinLossSnapshot:
    """Aggregate win/loss over the trailing `days` window."""
    window_start = datetime.now(tz=timezone.utc) - timedelta(days=days)

    # TODO(data layer): aggregate from your resolved-alerts table.
    # Likely a single SELECT with COUNT(...) FILTER (WHERE outcome = 'win') etc.
    agg = _aggregate_win_loss_since(window_start)
    total = agg["wins"] + agg["losses"] + agg.get("pending", 0)
    decided = agg["wins"] + agg["losses"]
    return WinLossSnapshot(
        window_days=days,
        wins=agg["wins"],
        losses=agg["losses"],
        pending=agg.get("pending", 0),
        total_alerts=total,
        win_rate=(agg["wins"] / decided) if decided else 0.0,
        biggest_win_pct=agg.get("biggest_win_pct"),
        biggest_loss_pct=agg.get("biggest_loss_pct"),
    )


@app.get("/api/whales/biggest", response_model=BiggestWhaleMove, dependencies=[Depends(_require_token)])
def biggest_whale(hours: int = Query(default=24, ge=1, le=168)) -> BiggestWhaleMove:
    """The single largest whale move in the last `hours`."""
    window_start = datetime.now(tz=timezone.utc) - timedelta(hours=hours)

    # TODO(data layer): SELECT * FROM whale_moves WHERE timestamp >= window_start ORDER BY amount_usd DESC LIMIT 1
    row = _fetch_biggest_whale_move_since(window_start)
    if row is None:
        raise HTTPException(status_code=404, detail="no whale moves in window")
    return BiggestWhaleMove(window_hours=hours, **row)


@app.get("/api/markets/notable-resolution", response_model=NotableResolution, dependencies=[Depends(_require_token)])
def notable_resolution() -> NotableResolution:
    """The most recent market resolution worth talking about — typically a
    market the system flagged correctly, or one that resolved against
    consensus.
    """
    # TODO(data layer): your "notable" definition — e.g. resolved within
    # the last 48h AND (system_called_it_correctly = true OR amount_usd > X).
    row = _fetch_notable_resolution()
    if row is None:
        raise HTTPException(status_code=404, detail="no notable resolution")
    return NotableResolution(**row)


# ── Data layer ────────────────────────────────────────────────────────────
# Re-derives whale-tracker + win/loss data from Polymarket's public APIs
# using the same thresholds polymarket_bot.py + postmortem_agent.py use.
# NOT a read against bot in-memory state — that state lives in a separate
# subprocess. See header comment for rationale.

def _get_top_traders(top_n: int) -> list[dict[str, Any]]:
    """Mirror of polymarket_bot.py:get_monthly_leaderboard — top monthly
    traders sorted by PnL. Falls back to all-time leaderboard if the
    monthly one is empty (same fallback behavior).
    """
    now = datetime.now(tz=timezone.utc)
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    try:
        r = requests.get(
            f"{_DATA_API}/v1/leaderboard",
            params={"startDate": int(start_of_month.timestamp())},
            timeout=_HTTP_TIMEOUT,
        )
        r.raise_for_status()
        traders = r.json()
        if not isinstance(traders, list) or not traders:
            r2 = requests.get(f"{_DATA_API}/v1/leaderboard", timeout=_HTTP_TIMEOUT)
            r2.raise_for_status()
            traders = r2.json()
        if not isinstance(traders, list):
            return []
        traders.sort(key=lambda t: float(t.get("pnl", 0) or 0), reverse=True)
        return traders[:top_n]
    except (requests.RequestException, ValueError) as e:
        raise _PolymarketUnavailable(f"leaderboard fetch failed: {e}") from e


def _get_recent_trades(wallet: str, since_ts: int, limit: int = 20) -> list[dict[str, Any]]:
    """Mirror of polymarket_bot.py:get_recent_trades."""
    try:
        r = requests.get(
            f"{_DATA_API}/activity",
            params={
                "user": wallet,
                "type": "TRADE",
                "start": since_ts,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
                "limit": limit,
            },
            timeout=_HTTP_TIMEOUT,
        )
        r.raise_for_status()
        result = r.json()
        return result if isinstance(result, list) else []
    except (requests.RequestException, ValueError):
        # Per-wallet failures are tolerated — we keep going through the
        # leaderboard rather than 502'ing the whole request.
        return []


def _confidence_for(trade_value: float, rank: int) -> float:
    """0.0-1.0 mapping mirroring polymarket_bot.py:get_confidence's tiers."""
    if trade_value >= 10000 and rank <= 5:
        return 0.95   # VERY HIGH
    if trade_value >= 5000 or rank <= 5:
        return 0.85   # HIGH
    if trade_value >= 2000 or rank <= 10:
        return 0.70   # MEDIUM
    return 0.55       # MODERATE


def _fetch_alerts_since(cutoff: datetime) -> list[dict[str, Any]]:
    """For each top trader, fetch their TRADE activity since `cutoff` and
    return any trades whose value clears `_MIN_TRADE_SIZE` — exactly the
    filter polymarket_bot.py applies before sending a Discord alert.
    """
    traders = _get_top_traders(_TOP_N_TRADERS)
    if not traders:
        return []

    cutoff_ts = int(cutoff.timestamp())
    out: list[dict[str, Any]] = []
    seen_tx: set[str] = set()

    for rank, trader in enumerate(traders, start=1):
        wallet = trader.get("proxyWallet")
        if not wallet:
            continue
        for trade in _get_recent_trades(wallet, cutoff_ts):
            tx_hash = trade.get("transactionHash") or ""
            if not tx_hash or tx_hash in seen_tx:
                continue
            share_size = float(trade.get("size", 0) or 0)
            price = float(trade.get("price", 0) or 0)
            trade_value = share_size * price
            if trade_value < _MIN_TRADE_SIZE:
                continue
            seen_tx.add(tx_hash)

            ts = int(trade.get("timestamp", 0) or 0)
            timestamp = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(tz=timezone.utc)
            side = trade.get("side", "?")
            outcome = trade.get("outcome", "?")
            out.append({
                "id": tx_hash,
                "timestamp": timestamp,
                "market": trade.get("title", "Unknown market"),
                "market_id": trade.get("conditionId") or trade.get("slug"),
                "whale_address": wallet,
                "amount_usd": round(trade_value, 2),
                "direction": f"{side} {outcome}".strip(),
                "implied_probability_before": None,   # not exposed by /activity
                "implied_probability_after": round(price, 4) if 0 < price < 1 else None,
                "system_confidence": _confidence_for(trade_value, rank),
            })

    out.sort(key=lambda a: a["timestamp"], reverse=True)
    return out


def _get_resolved_markets(limit: int, order: str = "endDate") -> list[dict[str, Any]]:
    """Mirror of postmortem_agent.py:get_resolved_markets, plus an order
    parameter so notable-resolution can ask for most-recent-first.
    """
    try:
        r = requests.get(
            f"{_GAMMA_API}/markets",
            params={"closed": "true", "limit": limit, "order": order, "ascending": "false"},
            timeout=_HTTP_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("markets", []) or []
        return []
    except (requests.RequestException, ValueError) as e:
        raise _PolymarketUnavailable(f"resolved-markets fetch failed: {e}") from e


def _derive_winner(market: dict[str, Any]) -> str:
    """Return the winning outcome label (e.g. "Yes" / "No") for a resolved
    market, or "" if it can't be determined.

    Polymarket's gamma-api `/markets?closed=true` no longer populates the
    `winner` field — it's empty on every resolved market we see. The
    resolution is still encoded in `outcomePrices`: the outcome whose
    settled price is "1" (or closest to 1) is the winner.

    Both `outcomes` and `outcomePrices` come back as JSON-encoded strings
    inside the JSON response (e.g. '["Yes", "No"]'), so we parse them.
    Falls back to the legacy `winner` field if it ever comes back populated.
    """
    legacy = (market.get("winner") or "").strip()
    if legacy:
        return legacy

    import json as _json
    outcomes_raw = market.get("outcomes")
    prices_raw = market.get("outcomePrices")
    try:
        outcomes = _json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        prices = _json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    except (ValueError, TypeError):
        return ""
    if not isinstance(outcomes, list) or not isinstance(prices, list):
        return ""
    if len(outcomes) != len(prices) or not outcomes:
        return ""
    try:
        float_prices = [float(p or 0) for p in prices]
    except (ValueError, TypeError):
        return ""
    # Resolved markets settle to 0/1. Pick the index closest to 1.
    top_idx = max(range(len(float_prices)), key=lambda i: float_prices[i])
    # Sanity check: if no price is anywhere near 1, the market may not be
    # actually resolved yet despite `closed=true`. Don't claim a winner.
    if float_prices[top_idx] < 0.9:
        return ""
    return str(outcomes[top_idx])


def _model_call_was_correct(market: dict[str, Any]) -> tuple[bool | None, float | None, str]:
    """Heuristic: would our model's pre-resolution recommendation match the
    actual winner? Returns (was_correct, yes_price_at_call, winner_str).

    For resolved markets, gamma-api's `outcomePrices` is the SETTLED price
    (0 or 1), not a pre-resolution mid-market — so we can't actually know
    what our model would have recommended. Returns (None, None, winner)
    in that case. Callers that need a real signal here would have to log
    pre-resolution prices when the bot first sees the market.
    """
    winner = _derive_winner(market)
    return None, None, winner


def _aggregate_win_loss_since(cutoff: datetime) -> dict[str, Any]:
    """Count resolved markets since `cutoff`.

    We use `closedTime` (when the market actually resolved), not `endDate`
    (the scheduled deadline — gamma-api sometimes returns endDates years
    in the future for already-closed markets).

    NOTE: gamma-api's `outcomePrices` for resolved markets is the SETTLED
    price (0/1), not a pre-resolution mid-market — so we can't compute
    "did our model call it right" from this endpoint alone. We report
    wins/losses as 0 and put every resolved market under `pending` to
    signal "decided by Polymarket, undecided by our scoring." If the bot
    starts logging pre-resolution prices to a shared store, this is the
    spot to plug them in.
    """
    markets = _get_resolved_markets(limit=100, order="closedTime")
    wins = 0
    losses = 0
    pending = 0

    for m in markets:
        closed_str = m.get("closedTime") or m.get("endDate") or ""
        if not closed_str:
            continue
        try:
            # closedTime comes back as "2026-04-14 22:08:00+00" — both
            # space- and T-separated forms parse via fromisoformat.
            resolved_at = datetime.fromisoformat(closed_str.replace("Z", "+00:00"))
        except ValueError:
            continue
        if resolved_at.tzinfo is None:
            resolved_at = resolved_at.replace(tzinfo=timezone.utc)
        if resolved_at < cutoff:
            continue

        if not _derive_winner(m):
            continue   # not actually resolved yet
        pending += 1   # decided by Polymarket, we have no pre-call price to score

    return {
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "biggest_win_pct": None,
        "biggest_loss_pct": None,
    }


def _fetch_biggest_whale_move_since(cutoff: datetime) -> dict[str, Any] | None:
    """Pick the single largest trade from the `_fetch_alerts_since` result.

    Same filter set the bot applies (top-N traders × min trade size), so
    the answer is "the biggest move the bot would have alerted on in the
    window," not just "any large trade on Polymarket."
    """
    alerts = _fetch_alerts_since(cutoff)
    if not alerts:
        return None
    biggest = max(alerts, key=lambda a: a["amount_usd"])
    return {
        "whale_address": biggest.get("whale_address"),
        "market": biggest["market"],
        "market_id": biggest.get("market_id"),
        "amount_usd": biggest["amount_usd"],
        "direction": biggest["direction"],
        "implied_prob_change": None,   # would need pre/post snapshots we don't have
        "timestamp": biggest["timestamp"],
    }


def _fetch_notable_resolution() -> dict[str, Any] | None:
    """Most recent resolved market with at least `_MIN_NOTABLE_MARKET_VOLUME`
    volume. Notability heuristic matches what makes sense for the TikTok
    content surface: large enough to be recognizable, actually resolved
    (winner derivable from `outcomePrices`), recent.

    Sort key is `closedTime` (when the market actually resolved), NOT
    `endDate` — gamma-api sometimes returns endDates years in the future
    for markets that closed early.

    `system_called_it_correctly` / `system_confidence_at_call` are left
    None: gamma-api's `outcomePrices` for resolved markets is the SETTLED
    price (0/1), not a pre-resolution mid-market, so the "did our model
    call it" heuristic can't be honestly computed from this endpoint.
    """
    markets = _get_resolved_markets(limit=50, order="closedTime")
    best: dict[str, Any] | None = None

    for m in markets:
        winner = _derive_winner(m)
        if not winner:
            continue
        try:
            volume = float(m.get("volume", 0) or 0)
        except (ValueError, TypeError):
            volume = 0.0
        if volume < _MIN_NOTABLE_MARKET_VOLUME:
            continue
        closed_str = m.get("closedTime") or m.get("endDate") or ""
        if not closed_str:
            continue
        try:
            resolved_at = datetime.fromisoformat(closed_str.replace("Z", "+00:00"))
        except ValueError:
            continue
        if resolved_at.tzinfo is None:
            resolved_at = resolved_at.replace(tzinfo=timezone.utc)

        candidate = {
            "market": m.get("question", "Unknown"),
            "market_id": m.get("id"),
            "resolved_at": resolved_at,
            "outcome": winner,
            "system_called_it_correctly": None,
            "system_confidence_at_call": None,
            "_volume": volume,   # internal sort hint; stripped before return
        }
        if best is None or candidate["resolved_at"] > best["resolved_at"]:
            best = candidate

    if best is None:
        return None
    best.pop("_volume", None)
    return best


# --- Server entry point -----------------------------------------------------

def start_api_server(*, host: str = "0.0.0.0", port: int = 8000) -> None:
    """Blocks. Call from a daemon thread inside launcher.py — see the module
    docstring for the exact wiring snippet.
    """
    import uvicorn
    uvicorn.run(app, host=host, port=port, log_level="info", access_log=False)


if __name__ == "__main__":
    # Allow standalone run for local testing: `python polymarket_bot_endpoints.py`
    start_api_server()
