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

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field


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


# --- Data layer stubs (FILL THESE IN) ---------------------------------------
# These are the only functions the rest of the file calls. Replace each body
# with the read appropriate to your storage layer. Return the shape shown
# inline. The endpoint handlers above wrap them in pydantic models.
# ---------------------------------------------------------------------------

def _fetch_alerts_since(cutoff: datetime) -> list[dict[str, Any]]:
    """Return a list of dicts shaped like WhaleAlert."""
    raise NotImplementedError(
        "Wire _fetch_alerts_since to read from the whale-tracker agent's data store."
    )


def _aggregate_win_loss_since(cutoff: datetime) -> dict[str, Any]:
    """Return:
      {
        "wins": int,
        "losses": int,
        "pending": int,                    # optional
        "biggest_win_pct": float | None,   # optional
        "biggest_loss_pct": float | None,  # optional
      }
    """
    raise NotImplementedError(
        "Wire _aggregate_win_loss_since to compute over the postmortem agent's outputs."
    )


def _fetch_biggest_whale_move_since(cutoff: datetime) -> dict[str, Any] | None:
    """Return a dict shaped like BiggestWhaleMove (without `window_hours`),
    or None if there were no moves.
    """
    raise NotImplementedError(
        "Wire _fetch_biggest_whale_move_since to your whale-moves table."
    )


def _fetch_notable_resolution() -> dict[str, Any] | None:
    """Return a dict shaped like NotableResolution, or None."""
    raise NotImplementedError(
        "Wire _fetch_notable_resolution to your resolved-markets table."
    )


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
