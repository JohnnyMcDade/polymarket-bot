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


# ─── Operator dashboard ──────────────────────────────────────────────────
# Single-page HTML view of trades_log + go_live_state. Reads from /app/data/
# at request time (cheap — JSON files are small). No auth so it's
# load-and-go from any Railway-public URL; if you ever expose this to
# people outside, gate with HTTPBearer like the /api/* routes.

from collections import defaultdict
from pathlib import Path
import json
from fastapi.responses import HTMLResponse, RedirectResponse

TRADES_LOG_PATH = Path(os.getenv("KALSHI_TRADES_LOG", "/app/data/trades_log.json"))
GO_LIVE_STATE_PATH = Path(os.getenv("KALSHI_GO_LIVE_STATE", "/app/data/go_live_state.json"))
STATS_CACHE_PATH = Path(os.getenv("KALSHI_STATS_CACHE", "/app/data/stats_cache.json"))

_DASH_SERIES_PREFIXES = (
    "KXMLBGAME", "KXMLBTOTAL", "KXMLBSPREAD", "KXMLBTEAMTOTAL",
    "KXATPMATCH", "KXWTAMATCH", "KXBTC", "KXAAAGASD",
    "KXNHL", "KXNBA",
)


def _dash_series_of(ticker: str) -> str:
    for p in _DASH_SERIES_PREFIXES:
        if ticker.startswith(p):
            return p
    return "other"


def _dash_fmt_pnl(v: float) -> tuple[str, str]:
    """Return (label, css-class) so positive/negative can be styled."""
    if v > 0:
        return f"+${v:.2f}", "pos"
    if v < 0:
        return f"-${abs(v):.2f}", "neg"
    return "$0.00", "muted"


def _dash_load_trades() -> list[dict]:
    if not TRADES_LOG_PATH.exists():
        return []
    try:
        with TRADES_LOG_PATH.open() as f:
            d = json.load(f)
    except Exception:
        return []
    return d if isinstance(d, list) else d.get("trades", [])


def _dash_load_go_live() -> dict:
    if not GO_LIVE_STATE_PATH.exists():
        return {}
    try:
        with GO_LIVE_STATE_PATH.open() as f:
            return json.load(f)
    except Exception:
        return {}


def _dash_render_cumulative_svg(trades: list[dict], width: int = 800, height: int = 200) -> str:
    """Inline SVG cumulative-pnl line chart. One point per settled trade
    in chronological order. Zero-line shown in light grey for orientation."""
    settled = [t for t in trades if t.get("outcome") in ("won", "lost")]
    settled.sort(key=lambda t: t.get("settled_at", t.get("timestamp", "")))
    if not settled:
        return ('<svg width="{w}" height="{h}"><text x="{tx}" y="{ty}" '
                'fill="#999" text-anchor="middle" font-family="system-ui">'
                'no settled trades yet</text></svg>').format(
            w=width, h=height, tx=width / 2, ty=height / 2)
    cum: list[float] = []
    s = 0.0
    for t in settled:
        s += float(t.get("pnl") or 0)
        cum.append(s)
    cmin = min(cum + [0.0])
    cmax = max(cum + [0.0])
    span = cmax - cmin or 1.0
    pad = 10
    plot_h = height - 2 * pad
    plot_w = width - 2 * pad
    pts = []
    for i, v in enumerate(cum):
        x = pad + (i / max(1, len(cum) - 1)) * plot_w
        y = pad + plot_h - ((v - cmin) / span) * plot_h
        pts.append(f"{x:.1f},{y:.1f}")
    # Zero line position
    zero_y = pad + plot_h - ((0 - cmin) / span) * plot_h
    last = cum[-1]
    color = "#2a9d2a" if last >= 0 else "#c0392b"
    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="Cumulative P&L line chart">'
        f'<line x1="{pad}" y1="{zero_y:.1f}" x2="{width - pad}" y2="{zero_y:.1f}" '
        f'stroke="#ccc" stroke-dasharray="3,3"/>'
        f'<polyline fill="none" stroke="{color}" stroke-width="2" '
        f'points="{" ".join(pts)}"/>'
        f'<text x="{width - pad - 4}" y="{pad + 14}" fill="{color}" '
        f'text-anchor="end" font-family="system-ui" font-weight="600">'
        f'last: {("+" if last >= 0 else "")}${last:.2f}</text>'
        f'<text x="{pad}" y="{pad + 14}" fill="#888" '
        f'font-family="system-ui" font-size="11">'
        f'n={len(cum)}  range: ${cmin:.0f}..${cmax:.0f}</text>'
        f'</svg>'
    )


def _dash_compute_per_series(trades: list[dict]) -> list[dict]:
    settled = [t for t in trades if t.get("outcome") in ("won", "lost")]
    by: dict[str, list[dict]] = defaultdict(list)
    for t in settled:
        by[_dash_series_of(t.get("ticker", ""))].append(t)
    rows: list[dict] = []
    for series, group in by.items():
        n = len(group)
        wins = sum(1 for t in group if t["outcome"] == "won")
        losses = n - wins
        pnl = sum(float(t.get("pnl") or 0) for t in group)
        preds = [float(t["our_prob"]) for t in group
                 if isinstance(t.get("our_prob"), (int, float))]
        mean_pred = sum(preds) / len(preds) if preds else None
        wr = wins / n if n else 0.0
        cal_err = (mean_pred - wr) * 100 if mean_pred is not None else None
        rows.append({
            "series": series, "n": n, "w": wins, "l": losses,
            "wr": wr, "pnl": pnl, "cal_err": cal_err,
        })
    # Sort by trade count desc — bigger cohorts first
    rows.sort(key=lambda r: -r["n"])
    return rows


def _dash_overall(trades: list[dict]) -> dict:
    settled = [t for t in trades if t.get("outcome") in ("won", "lost")]
    n = len(settled)
    wins = sum(1 for t in settled if t["outcome"] == "won")
    pnl = sum(float(t.get("pnl") or 0) for t in settled)
    pending = sum(1 for t in trades if t.get("outcome") == "pending")
    return {
        "n": n, "wins": wins, "wr": wins / n if n else 0.0,
        "pnl": pnl, "pending": pending, "total_placed": len(trades),
    }


def _dash_recent_table(trades: list[dict], limit: int = 10) -> list[dict]:
    """Most recent N trades regardless of outcome. Sorted by timestamp desc."""
    out = sorted(trades, key=lambda t: t.get("timestamp", ""), reverse=True)
    return out[:limit]


def _dash_go_live(state: dict) -> dict:
    history = state.get("history") or []
    latest = history[-1] if history else {}
    return {
        "is_live": bool(state.get("is_live")),
        "consecutive_pass_count": int(state.get("consecutive_pass_count") or 0),
        "passes": int(latest.get("passes") or 0),
        "total": int(latest.get("total") or 4),
        "last_report_date": state.get("last_report_date") or "—",
    }


@app.get("/dashboard", response_class=HTMLResponse)
@app.get("/", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    """Operator dashboard. Reads all source files at request time so it
    always reflects the latest disk state. <meta http-equiv=refresh>
    pulls a fresh render every 60s without JS."""
    trades = _dash_load_trades()
    overall = _dash_overall(trades)
    series_rows = _dash_compute_per_series(trades)
    recent = _dash_recent_table(trades, limit=10)
    gl = _dash_go_live(_dash_load_go_live())
    svg = _dash_render_cumulative_svg(trades)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    pnl_str, pnl_cls = _dash_fmt_pnl(overall["pnl"])

    series_table_rows = []
    for r in series_rows:
        p_str, p_cls = _dash_fmt_pnl(r["pnl"])
        cal_str = (f"{r['cal_err']:+.1f}pp"
                   if r["cal_err"] is not None else "—")
        series_table_rows.append(
            f"<tr>"
            f"<td>{r['series']}</td>"
            f"<td>{r['n']}</td>"
            f"<td>{r['w']}/{r['l']}</td>"
            f"<td>{r['wr']*100:.1f}%</td>"
            f"<td class='{p_cls}'>{p_str}</td>"
            f"<td>{cal_str}</td>"
            f"</tr>"
        )

    recent_table_rows = []
    for t in recent:
        ts = (t.get("timestamp") or "")[:16].replace("T", " ")
        series = _dash_series_of(t.get("ticker", ""))
        side = t.get("side", "?")
        edge = t.get("edge")
        edge_str = f"{float(edge)*100:+.1f}%" if isinstance(edge, (int, float)) else "—"
        outcome = t.get("outcome", "—")
        pnl_v = float(t.get("pnl") or 0)
        pn_str, pn_cls = _dash_fmt_pnl(pnl_v)
        oc_class = {"won": "pos", "lost": "neg"}.get(outcome, "muted")
        paper = " 📄" if t.get("paper") else " 💰"
        recent_table_rows.append(
            f"<tr>"
            f"<td>{ts}</td>"
            f"<td>{series}{paper}</td>"
            f"<td>{side}</td>"
            f"<td>{edge_str}</td>"
            f"<td class='{oc_class}'>{outcome}</td>"
            f"<td class='{pn_cls}'>{pn_str}</td>"
            f"</tr>"
        )

    is_live_badge = (
        "<span style='background:#2a9d2a;color:#fff;padding:2px 8px;"
        "border-radius:4px;font-size:0.85em;'>LIVE</span>"
        if gl["is_live"]
        else "<span style='background:#888;color:#fff;padding:2px 8px;"
        "border-radius:4px;font-size:0.85em;'>PAPER</span>"
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="60">
<title>Polymarket Bot — Dashboard</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, system-ui, "Segoe UI", sans-serif;
          max-width: 980px; margin: 1em auto; padding: 0 1em; color: #222; }}
  h1, h2 {{ margin: 0.5em 0; }}
  .summary {{ display: flex; gap: 1em; flex-wrap: wrap; margin: 1em 0 1.5em; }}
  .card {{ background: #f7f7f9; padding: 10px 16px; border-radius: 6px;
           border: 1px solid #e5e5e9; min-width: 140px; }}
  .card .label {{ color: #777; font-size: 0.85em; display: block; }}
  .card .value {{ font-size: 1.5em; font-weight: 600; display: block; margin-top: 2px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 0.5em 0 1.5em;
           font-size: 0.95em; }}
  th, td {{ border: 1px solid #e5e5e9; padding: 6px 10px; text-align: right; }}
  th {{ background: #f5f5f7; font-weight: 600; }}
  th:first-child, td:first-child {{ text-align: left; }}
  .pos {{ color: #1e7e1e; font-weight: 500; }}
  .neg {{ color: #b22222; font-weight: 500; }}
  .muted {{ color: #888; }}
  .footer {{ margin-top: 2em; padding-top: 1em; border-top: 1px solid #eee;
             color: #999; font-size: 0.8em; }}
</style>
</head>
<body>
<h1>Polymarket Bot {is_live_badge}</h1>
<p class="muted">Last refresh: {now_iso} UTC · Auto-refresh: 60s</p>

<div class="summary">
  <div class="card">
    <span class="label">Settled trades</span>
    <span class="value">{overall['n']}</span>
  </div>
  <div class="card">
    <span class="label">Win rate</span>
    <span class="value">{overall['wr']*100:.1f}%</span>
  </div>
  <div class="card">
    <span class="label">Total P&amp;L</span>
    <span class="value {pnl_cls}">{pnl_str}</span>
  </div>
  <div class="card">
    <span class="label">Pending</span>
    <span class="value">{overall['pending']}</span>
  </div>
  <div class="card">
    <span class="label">Go-live criteria</span>
    <span class="value">{gl['passes']} / {gl['total']}</span>
  </div>
</div>

<h2>Cumulative P&amp;L</h2>
{svg}

<h2>By series</h2>
<table>
<thead>
<tr><th>Series</th><th>Trades</th><th>W/L</th><th>Win%</th><th>P&amp;L</th><th>Cal err</th></tr>
</thead>
<tbody>
{"".join(series_table_rows) or "<tr><td colspan='6' class='muted'>no settled trades yet</td></tr>"}
</tbody>
</table>

<h2>Last 10 trades</h2>
<table>
<thead>
<tr><th>Date</th><th>Series</th><th>Side</th><th>Edge</th><th>Result</th><th>P&amp;L</th></tr>
</thead>
<tbody>
{"".join(recent_table_rows) or "<tr><td colspan='6' class='muted'>no trades yet</td></tr>"}
</tbody>
</table>

<div class="footer">
  Data sources: <code>{TRADES_LOG_PATH}</code>, <code>{GO_LIVE_STATE_PATH}</code>.
  All trade counts include paper + live. Cal err = mean_pred − win_rate
  (positive = overconfident). 📄 paper · 💰 live.
</div>
</body>
</html>"""
    return HTMLResponse(content=html)


@app.get("/api/alerts/today", response_model=list[WhaleAlert], dependencies=[Depends(_require_token)])
def alerts_today() -> list[WhaleAlert]:
    """Top 7 whale-tracker alerts from the trailing 7 days, one per distinct
    market, ranked by that market's aggregated whale volume across the window.

    Path is unchanged for backward compat with the ugc-pipeline, but the
    semantics widened from "everything since 00:00 UTC today" to "trailing
    7 days, top 7 markets, single representative trade per market" so the
    @passivepoly TikTok --week mode can feature a distinct market per day
    without re-querying.
    """
    window_start = datetime.now(tz=timezone.utc) - timedelta(days=7)
    rows = _fetch_alerts_since(window_start)
    rows = _top_n_distinct_markets(rows, n=7)
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


def _top_n_distinct_markets(rows: list[dict[str, Any]],
                             n: int) -> list[dict[str, Any]]:
    """Group rows by market_id, sum amount_usd per group, return the top-N
    groups' single largest trade as the representative — ranked by the
    group's aggregated whale volume."""
    from collections import defaultdict

    by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        mid = r.get("market_id") or r.get("market")
        if mid is None:
            continue
        by_market[mid].append(r)

    def _volume(group: list[dict[str, Any]]) -> float:
        return sum(float(a.get("amount_usd") or 0.0) for a in group)

    ranked_groups = sorted(by_market.values(), key=_volume, reverse=True)[:n]
    representatives: list[dict[str, Any]] = []
    for group in ranked_groups:
        rep = max(group, key=lambda a: float(a.get("amount_usd") or 0.0))
        representatives.append(rep)
    return representatives


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
