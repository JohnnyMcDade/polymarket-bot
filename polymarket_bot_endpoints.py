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
GATE_ACTIVITY_PATH = Path(os.getenv("KALSHI_GATE_ACTIVITY", "/app/data/gate_activity.json"))
PREDICTION_ACCURACY_PATH = Path(os.getenv(
    "KALSHI_PREDICTION_ACCURACY", "/app/data/prediction_accuracy.json"
))

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

    def _aggregate(label: str, group: list[dict], side: str | None = None) -> dict:
        n = len(group)
        wins = sum(1 for t in group if t["outcome"] == "won")
        losses = n - wins
        pnl = sum(float(t.get("pnl") or 0) for t in group)
        preds = [float(t["our_prob"]) for t in group
                 if isinstance(t.get("our_prob"), (int, float))]
        mean_pred = sum(preds) / len(preds) if preds else None
        wr = wins / n if n else 0.0
        cal_err = (mean_pred - wr) * 100 if mean_pred is not None else None
        return {
            "series": label, "n": n, "w": wins, "l": losses,
            "wr": wr, "pnl": pnl, "cal_err": cal_err, "side": side,
        }

    rows: list[dict] = []
    for series, group in by.items():
        parent_n = len(group)
        parent = _aggregate(series, group)
        # KXMLBTOTAL BUY_NO staged rollout (2026-06-17) — surface NO bets
        # as a separate row when any have settled, so the cohort's WR is
        # visible without conflating it with the YES baseline. Sort key
        # pins the sub-row to its parent so the table reads cleanly even
        # when another series has more trades.
        parent["_sort"] = (-parent_n, 0)
        rows.append(parent)
        if series == "KXMLBTOTAL":
            no_group = [t for t in group if t.get("side") == "no"]
            if no_group:
                child = _aggregate("KXMLBTOTAL (NO)", no_group, side="no")
                child["_sort"] = (-parent_n, 1)
                rows.append(child)
    rows.sort(key=lambda r: r["_sort"])
    for r in rows:
        r.pop("_sort", None)
    return rows


def _dash_compute_per_confidence(trades: list[dict]) -> list[dict]:
    """Aggregate by Claude's emitted CONFIDENCE tier (HIGH/MEDIUM/etc).
    Mirror of _dash_compute_per_series so the dashboard can surface the
    'HIGH inversion' calibration check — if HIGH-conf bets are losing
    money while MEDIUM is winning, the model's self-confidence is
    inverted and the auto-go-live criteria need investigation."""
    settled = [t for t in trades if t.get("outcome") in ("won", "lost")]
    by: dict[str, list[dict]] = defaultdict(list)
    for t in settled:
        by[(t.get("confidence") or "UNKNOWN").upper()].append(t)
    rows: list[dict] = []
    for conf, group in by.items():
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
            "confidence": conf, "n": n, "w": wins, "l": losses,
            "wr": wr, "pnl": pnl, "cal_err": cal_err,
        })
    # Fixed display order — HIGH first, then MEDIUM, then anything else
    # (LOW would be a Claude SKIP that somehow placed; UNKNOWN is a data
    # bug). Stable order makes the row scan repeatable across reloads.
    order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    rows.sort(key=lambda r: (order.get(r["confidence"], 9), r["confidence"]))
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


# Default cut-off matches the calibration window in go_live_criteria.json
# (registered 2026-06-11 with since_date 2026-06-07). Trades before that
# pre-date the KALSHI_MAX_EDGE cap from 24ae529 + the KXBTC bucket filter
# from 65e0aeb, so they're a historical sample for now-fixed bugs and
# don't represent current bot performance. The dashboard defaults to the
# windowed view; `?since=all` shows lifetime stats.
_DASH_DEFAULT_SINCE = "2026-06-07"

# Per-series disable cutoffs. When a series was disabled in production
# after a given date, trades placed before that date no longer reflect
# current bot performance — they're damage from the now-disabled cohort.
# Excluded from the default windowed view; `?since=all` still shows them.
# KXMLBTEAMTOTAL: disabled 2026-06-14 after 60d backtest showed the
# N≥5 elite-starter cohort lost its lift in the recent 30d window.
_DASH_SERIES_DISABLED_BEFORE: dict[str, str] = {
    "KXMLBTEAMTOTAL": "2026-06-15",
}


def _dash_pre_disable_dropout(t: dict) -> bool:
    """True when the trade is in a now-disabled series and was placed
    before that series' disable date — so the windowed view should hide
    it. Returns False for every trade in a still-active series."""
    ticker = t.get("ticker", "")
    ts_date = (t.get("timestamp") or "")[:10]
    for series, disable_date in _DASH_SERIES_DISABLED_BEFORE.items():
        if ticker.startswith(series) and ts_date < disable_date:
            return True
    return False


_DASH_COHORT_ERA_CAP = 3.50  # mirrors kalshi_edge.BACKTEST_FILTER_ERA_CAP
# BTC filter F&G thresholds — read from env so the dashboard always
# reflects whatever the production edge agent is using. Defaults to 20
# (matching kalshi_edge.py's code default); env override KALSHI_BTC_FG_
# EXTREME_LOW currently set to 21 in Railway, making F&G=20 a PASS.
_DASH_BTC_FG_LOW = int(os.getenv("KALSHI_BTC_FG_EXTREME_LOW", "20"))
_DASH_BTC_FG_HIGH = int(os.getenv("KALSHI_BTC_FG_EXTREME_HIGH", "80"))

# Stadium names per home team abbreviation. Current 2026 names — historical
# names (Safeco/Minute Maid/Gerry Weber-style legacy aliases) are not
# tracked here; if Kalshi titles ever use a sponsor name we don't know
# about we'll fall back to the abbr.
_DASH_STADIUM: dict[str, str] = {
    "ARI": "Chase Field",
    "ATL": "Truist Park",
    "BAL": "Camden Yards",
    "BOS": "Fenway Park",
    "CHC": "Wrigley Field",
    "CWS": "Guaranteed Rate Field",
    "CIN": "Great American Ball Park",
    "CLE": "Progressive Field",
    "COL": "Coors Field",
    "DET": "Comerica Park",
    "HOU": "Daikin Park",
    "KC":  "Kauffman Stadium",
    "LAA": "Angel Stadium",
    "LAD": "Dodger Stadium",
    "MIA": "LoanDepot Park",
    "MIL": "American Family Field",
    "MIN": "Target Field",
    "NYM": "Citi Field",
    "NYY": "Yankee Stadium",
    "OAK": "Sutter Health Park",  # Sacramento, since 2025
    "PHI": "Citizens Bank Park",
    "PIT": "PNC Park",
    "SD":  "Petco Park",
    "SEA": "T-Mobile Park",
    "SF":  "Oracle Park",
    "STL": "Busch Stadium",
    "TB":  "Tropicana Field",
    "TEX": "Globe Life Field",
    "TOR": "Rogers Centre",
    "WSH": "Nationals Park",
}


def _dash_utc_to_az_str(utc_hhmm: str) -> str:
    """Convert a 'HH:MM' UTC time string to Arizona local 12-hour clock.
    Arizona doesn't observe DST so it's MST year-round = UTC-7. Returns
    e.g. '6:40 PM AZ'. Falls back to the original string on parse error
    so the caller can still display something."""
    try:
        hh, mm = utc_hhmm.split(":")
        h = (int(hh) - 7) % 24
        m = int(mm)
        suffix = "AM" if h < 12 else "PM"
        h12 = h % 12 or 12
        return f"{h12}:{m:02d} {suffix} AZ"
    except (ValueError, AttributeError):
        return f"{utc_hhmm}Z"


def _dash_load_stats_cache() -> dict:
    if not STATS_CACHE_PATH.exists():
        return {}
    try:
        with STATS_CACHE_PATH.open() as f:
            return json.load(f)
    except Exception:
        return {}


def _dash_next_buy_no_candidate(stats: dict) -> dict | None:
    """The nearest upcoming game that would satisfy the production
    BUY_NO eligibility cohort: both starters' SEASON ERA < 3.50. Production
    also requires a `-9` or `-10` ticker on Kalshi (we assume those always
    list for KXMLBTOTAL — typical) plus the projection floor (Claude's
    runtime call, can't know from stats). So this answers "which game
    is the next plausible BUY_NO fire candidate" — not a guarantee, but
    the right next-thing-to-watch row.

    Returns the same shape as _dash_qualifying_games rows so the
    template can reuse the formatting, plus extra fields:
      a_era, h_era — both starters' season ERAs
      buy_no_t_minus_30_iso — UTC ISO of game_start - 30min (the T-30min
        eviction trigger time)
    """
    games = (stats.get("mlb", {}) or {}).get("upcoming_games", []) or []
    today_iso = datetime.now(timezone.utc).date().isoformat()
    candidates: list[dict] = []
    for g in games:
        gd = str(g.get("game_date", ""))
        if gd < today_iso:
            continue
        aw = g.get("away_pitcher") or {}
        hp = g.get("home_pitcher") or {}
        ae = aw.get("era")  # season ERA, matches production BUY_NO rule
        he = hp.get("era")
        if not isinstance(ae, (int, float)) or not isinstance(he, (int, float)):
            continue
        if not (float(ae) < 3.50 and float(he) < 3.50):
            continue
        candidates.append({
            "date": gd,
            "matchup": f"{g.get('away','?')}@{g.get('home','?')}",
            "away_pitcher": aw.get("player", ""),
            "home_pitcher": hp.get("player", ""),
            "a_era": float(ae),
            "h_era": float(he),
            "avg_era": (float(ae) + float(he)) / 2.0,
            "park_factor": g.get("park_factor"),
            "weather": g.get("weather") or {},
            "start_time_utc": g.get("start_time_utc", "?"),
        })
    if not candidates:
        return None
    candidates.sort(key=lambda r: (r["date"], r["start_time_utc"]))
    return candidates[0]


def _dash_t_minus_30_iso(game_date: str, start_time_utc: str) -> str | None:
    """Compute the T-30min-start eviction UTC ISO from game date + UTC
    start time (HH:MM format from the stats cache). Returns None on
    parse failure so the caller can render '—' instead of crashing."""
    try:
        h, m = start_time_utc.split(":")
        start = datetime.fromisoformat(f"{game_date}T{int(h):02d}:{int(m):02d}:00+00:00")
        return (start - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M UTC")
    except (ValueError, AttributeError):
        return None


def _dash_qualifying_games(stats: dict, max_rows: int = 5) -> list[dict]:
    """Return today/tomorrow's MLB games that pass the KXMLBTOTAL
    cohort filter (avg starter rolling_era_last3 ≤ _DASH_COHORT_ERA_CAP).

    Strategy: collect all upcoming_games whose game_date is today or
    later. For each, compute avg ERA when both starters have data;
    classify into tiers and keep those that qualify. Sort by date asc
    then avg_era asc, take top max_rows. The 'next' game per the user
    spec just falls out as the head of the list."""
    games = (stats.get("mlb", {}) or {}).get("upcoming_games", []) or []
    today_iso = datetime.now(timezone.utc).date().isoformat()
    rows: list[dict] = []
    for g in games:
        gd = str(g.get("game_date", ""))
        if gd < today_iso:
            continue
        aw = (g.get("away_pitcher") or {})
        hp = (g.get("home_pitcher") or {})
        ae = aw.get("rolling_era_last3")
        he = hp.get("rolling_era_last3")
        if not isinstance(ae, (int, float)) or not isinstance(he, (int, float)):
            continue
        avg = (float(ae) + float(he)) / 2.0
        if avg > _DASH_COHORT_ERA_CAP:
            continue
        rows.append({
            "date": gd,
            "matchup": f"{g.get('away','?')}@{g.get('home','?')}",
            "away_pitcher": aw.get("player", ""),
            "home_pitcher": hp.get("player", ""),
            "avg_era": avg,
            "tier": "elite" if avg <= 2.50 else "good",
            "park_factor": g.get("park_factor"),
            "weather": g.get("weather") or {},
            "start_time_utc": g.get("start_time_utc", "?"),
        })
    rows.sort(key=lambda r: (r["date"], r["avg_era"]))
    return rows[:max_rows]


RECALIB_STATE_PATH = Path(os.getenv(
    "KALSHI_RECALIB_STATE", "/app/data/recalibration_demote.json"
))

# Wimbledon countdown — surfaced in a dashboard highlight so the
# operator sees the slam ramp-up coming. Override via env when the
# 2027 draw publishes.
WIMBLEDON_START_ISO = os.getenv("KALSHI_WIMBLEDON_START", "2026-06-30")
SEEN_CACHE_PATH = Path(os.getenv(
    "KALSHI_EDGE_SEEN_CACHE", "/app/data/edge_seen.json"
))


def _dash_tennis_market_count(window_secs: int = 7200) -> int:
    """Count distinct KXATPMATCH + KXWTAMATCH tickers stamped in
    edge_seen.json within the last `window_secs` seconds. The edge
    agent stamps every fetched market into seen, so a recent stamp =
    market still being fetched from Kalshi = market still open. 2h
    window gives ~4 edge cycles of coverage without including stale
    tickers from completed matches earlier in the day."""
    if not SEEN_CACHE_PATH.exists():
        return 0
    try:
        seen = json.loads(SEEN_CACHE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return 0
    if not isinstance(seen, dict):
        return 0
    now = datetime.now(timezone.utc).timestamp()
    cutoff = now - window_secs
    count = 0
    for ticker, ts in seen.items():
        if not isinstance(ts, (int, float)):
            continue
        if ts < cutoff:
            continue
        if ticker.startswith("KXATPMATCH") or ticker.startswith("KXWTAMATCH"):
            count += 1
    return count


def _dash_wimbledon_countdown() -> dict:
    """Days until Wimbledon start + currently-open ATP/WTA market count.
    Negative days means Wimbledon is already in progress or past.
    Color tier:
      'green'  — < 7 days (final ramp-up; Wimbledon backtest cohort
                 with the +6.5pp slam favorite lift is live very soon)
      'yellow' — 7-30 days
      'muted'  — > 30 days or already past"""
    today = datetime.now(timezone.utc).date()
    try:
        start = datetime.fromisoformat(WIMBLEDON_START_ISO).date()
    except ValueError:
        return {"days": None, "tier": "muted", "tennis_open": 0,
                "start_iso": WIMBLEDON_START_ISO}
    delta = (start - today).days
    if delta < 0:
        tier = "muted"
    elif delta < 7:
        tier = "green"
    elif delta <= 30:
        tier = "yellow"
    else:
        tier = "muted"
    return {
        "days": delta,
        "tier": tier,
        "tennis_open": _dash_tennis_market_count(),
        "start_iso": WIMBLEDON_START_ISO,
    }


def _dash_recalib_status() -> dict:
    """Snapshot of the HIGH→MEDIUM recalibration cooldown state. Returns:
      active (bool), demote_until_iso (str|None), reason (str),
      hours_left (float|None).

    Mirrors kalshi_edge._is_recalib_demoting's persistence model: the
    state file lingers after the cooldown expires, so "active" means
    demote_until_ts > now, not just "file exists." When inactive, the
    file is either absent or carries an expired window."""
    if not RECALIB_STATE_PATH.exists():
        return {"active": False, "demote_until_iso": None,
                "reason": "no demote state file — never triggered",
                "hours_left": None}
    try:
        state = json.loads(RECALIB_STATE_PATH.read_text())
    except (json.JSONDecodeError, OSError) as e:
        return {"active": False, "demote_until_iso": None,
                "reason": f"state unreadable: {e}", "hours_left": None}
    demote_until = float(state.get("demote_until_ts") or 0)
    now = datetime.now(timezone.utc).timestamp()
    if demote_until <= now:
        return {
            "active": False,
            "demote_until_iso": state.get("demote_until_iso"),
            "reason": (
                f"window expired — last trigger: "
                f"{state.get('trigger_reason', 'unknown')}"
            ),
            "hours_left": None,
        }
    return {
        "active": True,
        "demote_until_iso": state.get("demote_until_iso"),
        "reason": state.get("trigger_reason", "unknown"),
        "hours_left": (demote_until - now) / 3600.0,
    }


def _dash_gate_activity() -> dict:
    """Read the rolling 24h funnel snapshot kalshi_edge writes after each
    cycle. Returns:
      cycles_in_window (int|None — None if file missing/unreadable),
      totals_24h (dict[str, int]),
      updated_at (str|None).

    The dashboard renders fixed rows for the named Rule-1 / BUY_NO /
    sanity gates plus a couple of context counters; absent keys read as
    zero. Missing file is treated as "agent hasn't completed a cycle
    since deploy" — not an error."""
    if not GATE_ACTIVITY_PATH.exists():
        return {"cycles_in_window": None, "totals_24h": {}, "updated_at": None}
    try:
        state = json.loads(GATE_ACTIVITY_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {"cycles_in_window": None, "totals_24h": {}, "updated_at": None}
    return {
        "cycles_in_window": state.get("cycles_in_window"),
        "totals_24h": state.get("totals_24h") or {},
        "updated_at": state.get("updated_at"),
    }


def _dash_prediction_accuracy(since: str | None) -> dict:
    """Aggregate KXMLBTOTAL projection-vs-actual records from
    /app/data/prediction_accuracy.json. Honors the dashboard's
    `since` filter (settled_at >= since), or includes everything
    when since is None / 'all'.

    Returns:
      n (int), mean_error (float|None — None when n=0),
      n_over (int), n_under (int), n_correct (int),
      bias ('OVER'|'UNDER'|'EVEN'|None) — whichever count is higher,
      EVEN on a tie, None when n=0,
      severity ('green'|'yellow'|'red'|'none') for color coding."""
    empty = {
        "n": 0, "mean_error": None, "n_over": 0, "n_under": 0,
        "n_correct": 0, "bias": None, "severity": "none",
    }
    if not PREDICTION_ACCURACY_PATH.exists():
        return empty
    try:
        store = json.loads(PREDICTION_ACCURACY_PATH.read_text()) or {}
    except (json.JSONDecodeError, OSError):
        return empty
    rows = list(store.values())
    if since and since.lower() != "all":
        rows = [
            r for r in rows
            if (r.get("settled_at") or "")[:10] >= since
        ]
    if not rows:
        return empty
    errs = [abs(float(r["error"])) for r in rows]
    mean_err = sum(errs) / len(errs)
    n_over = sum(1 for r in rows if r.get("direction") == "OVER")
    n_under = sum(1 for r in rows if r.get("direction") == "UNDER")
    n_correct = sum(1 for r in rows if r.get("direction") == "CORRECT")
    if n_over > n_under:
        bias = "OVER"
    elif n_under > n_over:
        bias = "UNDER"
    else:
        bias = "EVEN"
    if mean_err > 2.0:
        severity = "red"
    elif mean_err >= 1.0:
        severity = "yellow"
    else:
        severity = "green"
    return {
        "n": len(rows),
        "mean_error": mean_err,
        "n_over": n_over,
        "n_under": n_under,
        "n_correct": n_correct,
        "bias": bias,
        "severity": severity,
    }


def _dash_btc_status(stats: dict) -> dict:
    """Snapshot of the macro signals that gate KXBTC trading. Returns:
      fng_value (int|None), fng_class (str), momentum_pct (float|None),
      filter_pass (bool), reason (str — empty if PASS).

    Matches the live filter check in kalshi_edge._btc_filter_passes:
    PASS iff F&G < BTC_FG_LOW or F&G > BTC_FG_HIGH. (Per-market price
    and edge gates also apply at production time — this just answers
    "could the F&G gate let anything through right now?".)"""
    econ = (stats.get("economic") or {})
    fng_value = econ.get("crypto_fear_greed_value")
    fng_class = econ.get("crypto_fear_greed_classification", "") or ""
    momentum = econ.get("btc_24h_momentum_pct")
    if not isinstance(fng_value, (int, float)):
        return {"fng_value": None, "fng_class": fng_class,
                "momentum_pct": momentum, "filter_pass": False,
                "reason": "no F&G reading in stats cache"}
    fng_int = int(fng_value)
    if _DASH_BTC_FG_LOW <= fng_int <= _DASH_BTC_FG_HIGH:
        reason = (
            f"F&G={fng_int} in calm zone "
            f"[{_DASH_BTC_FG_LOW}, {_DASH_BTC_FG_HIGH}]"
        )
        return {"fng_value": fng_int, "fng_class": fng_class,
                "momentum_pct": momentum, "filter_pass": False,
                "reason": reason}
    zone = "extreme fear" if fng_int < _DASH_BTC_FG_LOW else "extreme greed"
    return {"fng_value": fng_int, "fng_class": fng_class,
            "momentum_pct": momentum, "filter_pass": True,
            "reason": f"F&G={fng_int} in {zone} zone"}


def _dash_render_frequency_svg(trades: list[dict], days: int = 14,
                               width: int = 800, height: int = 200) -> str:
    """14-day bar chart of trade count per day. Independent of the
    `since` filter so the trend signal stays stable across views.

    X-axis: oldest → newest, left → right. One bar per day.
    Bar height proportional to that day's trade count. Day label
    (MM-DD) under each bar. Count above each bar when > 0."""
    today = datetime.now(timezone.utc).date()
    dates = [(today - timedelta(days=i)).isoformat()
             for i in range(days - 1, -1, -1)]
    counts: dict[str, int] = {d: 0 for d in dates}
    for t in trades:
        ts = (t.get("timestamp") or "")[:10]
        if ts in counts:
            counts[ts] += 1
    series = [counts[d] for d in dates]
    if not any(series):
        return ('<svg width="{w}" height="{h}"><text x="{tx}" y="{ty}" '
                'fill="#999" text-anchor="middle" font-family="system-ui">'
                'no trades placed in the last {d} days</text></svg>').format(
            w=width, h=height, tx=width / 2, ty=height / 2, d=days)
    pad_top = 20
    pad_bot = 30
    pad_lr = 24
    plot_h = height - pad_top - pad_bot
    plot_w = width - 2 * pad_lr
    bar_w = (plot_w / days) * 0.75
    bar_gap = (plot_w / days) * 0.25
    max_c = max(series) or 1
    bars: list[str] = []
    labels: list[str] = []
    counts_text: list[str] = []
    for i, d in enumerate(dates):
        c = counts[d]
        x = pad_lr + i * (bar_w + bar_gap) + bar_gap / 2
        h_bar = (c / max_c) * plot_h if c else 0
        y = pad_top + plot_h - h_bar
        bars.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
            f'height="{h_bar:.1f}" fill="#4a8fd8" rx="2"/>'
        )
        # Date label below bar — show MM-DD; print every-other for
        # 14-day chart to avoid crowding
        if i % 2 == 0:
            labels.append(
                f'<text x="{x + bar_w/2:.1f}" y="{height - 8}" '
                f'fill="#888" text-anchor="middle" font-family="system-ui" '
                f'font-size="10">{d[5:]}</text>'
            )
        if c > 0:
            counts_text.append(
                f'<text x="{x + bar_w/2:.1f}" y="{y - 4:.1f}" '
                f'fill="#333" text-anchor="middle" font-family="system-ui" '
                f'font-size="11" font-weight="600">{c}</text>'
            )
    total = sum(series)
    avg_per_day = total / days
    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="Trade frequency last {days} days">'
        f'<text x="{pad_lr}" y="{pad_top - 6}" fill="#888" '
        f'font-family="system-ui" font-size="11">'
        f'total: {total} · avg: {avg_per_day:.1f}/day · peak: {max_c}/day'
        f'</text>'
        f'{"".join(bars)}{"".join(counts_text)}{"".join(labels)}'
        f'</svg>'
    )


@app.get("/dashboard", response_class=HTMLResponse)
@app.get("/", response_class=HTMLResponse)
def dashboard(since: str = _DASH_DEFAULT_SINCE) -> HTMLResponse:
    """Operator dashboard. Reads all source files at request time so it
    always reflects the latest disk state. <meta http-equiv=refresh>
    pulls a fresh render every 60s without JS.

    `since` query param: ISO date (YYYY-MM-DD). Trades with timestamp
    >= that date are included. Pass `?since=all` to disable the window
    and see lifetime stats including historical disasters from
    pre-filter-fix days."""
    all_trades = _dash_load_trades()
    if since and since.lower() != "all":
        trades = [
            t for t in all_trades
            if (t.get("timestamp") or "")[:10] >= since
            and not _dash_pre_disable_dropout(t)
        ]
    else:
        trades = all_trades
    overall = _dash_overall(trades)
    series_rows = _dash_compute_per_series(trades)
    confidence_rows = _dash_compute_per_confidence(trades)
    recent = _dash_recent_table(trades, limit=10)
    gl = _dash_go_live(_dash_load_go_live())
    svg = _dash_render_cumulative_svg(trades)
    # Frequency chart uses ALL trades (not just window-filtered) so the
    # 14-day rolling trend is independent of the page's since filter.
    freq_svg = _dash_render_frequency_svg(all_trades, days=14)
    stats_cache = _dash_load_stats_cache()
    qualifying_games = _dash_qualifying_games(stats_cache, max_rows=5)
    next_buy_no = _dash_next_buy_no_candidate(stats_cache)
    btc_status = _dash_btc_status(stats_cache)
    recalib_status = _dash_recalib_status()
    gate_activity = _dash_gate_activity()
    prediction_accuracy = _dash_prediction_accuracy(
        since if (since and since.lower() != "all") else None
    )
    wimbledon = _dash_wimbledon_countdown()
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Window controls — single bar with current scope + toggle link.
    is_windowed = bool(since and since.lower() != "all")
    in_window_n = len(trades)
    total_n = len(all_trades)
    hidden_n = total_n - in_window_n
    if is_windowed:
        window_label = f"Since {since} (post-fix baseline)"
        toggle_href = "/dashboard?since=all"
        toggle_label = (
            f"click to show all {total_n} trades including "
            f"{hidden_n} pre-fix losses →"
        )
    else:
        window_label = "All time"
        toggle_href = f"/dashboard?since={_DASH_DEFAULT_SINCE}"
        toggle_label = f"click to show post-fix baseline (since {_DASH_DEFAULT_SINCE}) →"
    window_bar = (
        f'<p class="window-bar">'
        f'Window: <strong>{window_label}</strong> '
        f'<span class="muted">({in_window_n} of {total_n} trades)</span> · '
        f'<a href="{toggle_href}">{toggle_label}</a>'
        f'</p>'
    )

    # Best-game highlight. Picks the first row from qualifying_games (the
    # list is pre-sorted by date asc, avg_era asc — so the head is the
    # nearest qualifying game with the lowest avg ERA). Label depends on
    # whether it's today vs. a future date.
    today_iso = datetime.now(timezone.utc).date().isoformat()
    if qualifying_games:
        bg = qualifying_games[0]
        prefix = "Best game today" if bg["date"] == today_iso else (
            f"Best upcoming ({bg['date']})"
        )
        # Stadium name from the home team. Matchup is 'AWAY@HOME', so the
        # home abbr is the part after '@'.
        home_abbr = bg["matchup"].split("@", 1)[-1] if "@" in bg["matchup"] else ""
        stadium = _DASH_STADIUM.get(home_abbr, home_abbr or "—")
        pf = bg.get("park_factor")
        pf_str = (
            f"{stadium} {pf:.2f} park factor"
            if isinstance(pf, (int, float)) else stadium
        )
        # UTC → Arizona local (MST, no DST → UTC-7) per spec.
        az_time = _dash_utc_to_az_str(bg["start_time_utc"])
        w = bg.get("weather") or {}
        wstr = ""
        if w.get("temp_f") is not None and w.get("wind_mph") is not None:
            wstr = f", {w['temp_f']:.0f}°F {w['wind_mph']:.0f}mph {w.get('wind_dir','')}"
        best_game_html = (
            f'<div class="highlight highlight-green">'
            f'🎯 <strong>{prefix}:</strong> {bg["matchup"]} — '
            f'<strong>{bg["tier"].capitalize()}</strong> tier '
            f'({bg["avg_era"]:.2f} avg ERA), '
            f'{az_time}, {pf_str}{wstr}'
            f'</div>'
        )
    else:
        best_game_html = (
            '<div class="highlight highlight-muted">'
            '🎯 No qualifying KXMLBTOTAL games in upcoming_games — '
            'expect no trades on this series until lineups confirm '
            'or tomorrow’s slate hydrates.'
            '</div>'
        )

    # BTC market-status highlight. Color tracks filter_pass: green when
    # the F&G gate is in extreme zone, yellow/muted when calm.
    bs = btc_status
    if bs["fng_value"] is None:
        btc_html = (
            f'<div class="highlight highlight-muted">'
            f'🟡 BTC: F&amp;G data not in cache yet — '
            f'next macro refresh will populate it.'
            f'</div>'
        )
    else:
        icon = "🟢" if bs["filter_pass"] else "🟡"
        verdict = "PASS" if bs["filter_pass"] else "SKIP"
        mom = bs["momentum_pct"]
        mom_str = (
            f"{mom:+.2f}% 24h" if isinstance(mom, (int, float)) else "no 24h data"
        )
        if bs["filter_pass"]:
            reason_tail = f"({bs['reason']})"
        else:
            reason_tail = (
                f"(need F&amp;G &lt; {_DASH_BTC_FG_LOW} "
                f"or &gt; {_DASH_BTC_FG_HIGH})"
            )
        cls = "highlight-green" if bs["filter_pass"] else "highlight-muted"
        btc_html = (
            f'<div class="highlight {cls}">'
            f'{icon} <strong>BTC:</strong> F&amp;G={bs["fng_value"]} '
            f'({bs["fng_class"]}) {mom_str} → filter <strong>{verdict}</strong> '
            f'{reason_tail}'
            f'</div>'
        )

    # Next-expected-trade block. Two side-by-side cards (BUY YES + BUY
    # NO) showing the nearest qualifying game with its T-30min-start
    # eviction time — when the bot will re-evaluate after seen-cache
    # eviction releases the ticker. Helps the operator know "is the
    # quiet hour normal or stuck."
    def _trade_card(label: str, color_class: str, row: dict | None,
                    is_buy_no: bool) -> str:
        if not row:
            return (
                f'<div class="trade-card {color_class}">'
                f'<div class="trade-card-label">{label}</div>'
                f'<div class="trade-card-empty">No qualifying game on the slate.</div>'
                f'</div>'
            )
        evict_utc = _dash_t_minus_30_iso(row["date"], row["start_time_utc"])
        evict_str = evict_utc or "—"
        az_time = _dash_utc_to_az_str(row["start_time_utc"])
        if is_buy_no:
            era_line = (
                f'{row["away_pitcher"][:18]} ({row.get("a_era", 0):.2f}) vs '
                f'{row["home_pitcher"][:18]} ({row.get("h_era", 0):.2f})'
            )
        else:
            era_line = (
                f'{row["away_pitcher"][:18]} vs {row["home_pitcher"][:18]} '
                f'— avg ERA {row["avg_era"]:.2f}'
            )
        return (
            f'<div class="trade-card {color_class}">'
            f'<div class="trade-card-label">{label}</div>'
            f'<div class="trade-card-game">'
            f'<strong>{row["matchup"]}</strong> · {row["date"]} · {az_time}'
            f'</div>'
            f'<div class="trade-card-pitchers">{era_line}</div>'
            f'<div class="trade-card-evict">'
            f'Earliest re-eval: <strong>{evict_str}</strong> '
            f'<span class="muted">(T-30min-start eviction)</span>'
            f'</div>'
            f'</div>'
        )

    buy_yes_card = _trade_card(
        "🟢 Next BUY YES candidate",
        "trade-card-yes",
        qualifying_games[0] if qualifying_games else None,
        is_buy_no=False,
    )
    buy_no_card = _trade_card(
        "🟠 Next BUY NO candidate",
        "trade-card-no",
        next_buy_no,
        is_buy_no=True,
    )
    next_trade_html = (
        f'<div class="trade-cards">{buy_yes_card}{buy_no_card}</div>'
    )

    # Recalibration cooldown badge. Active = current Discord-level
    # warning; healthy = quiet confirmation. Renders as a one-line
    # highlight matching the BTC status block above.
    rs = recalib_status
    if rs["active"]:
        hours_left = rs.get("hours_left") or 0.0
        until_iso = rs.get("demote_until_iso") or "?"
        # Trim ISO to minute precision for readability — full microsecond
        # precision is noise here.
        until_short = until_iso[:16].replace("T", " ") + " UTC"
        recalib_html = (
            f'<div class="highlight highlight-muted">'
            f'🟡 <strong>Recalibration cooldown: ACTIVE</strong> — '
            f'demoting HIGH→MEDIUM until {until_short} '
            f'({hours_left:.1f}h left). Trigger: {rs["reason"]}'
            f'</div>'
        )
    else:
        recalib_html = (
            f'<div class="highlight highlight-green">'
            f'🟢 <strong>Recalibration: healthy</strong> — '
            f'no active HIGH→MEDIUM cooldown. '
            f'<span class="muted">({rs["reason"]})</span>'
            f'</div>'
        )

    # Wimbledon countdown — green when < 7 days out (final ramp-up where
    # the slam favorite-lift backtest cohort kicks in). When < 0 the slam
    # is in-progress or past and we just say so.
    w = wimbledon
    if w["days"] is None:
        wimbledon_html = ""  # malformed env config — skip block
    else:
        days = w["days"]
        if days < 0:
            stage_msg = f"in progress or past ({-days}d ago start)"
        elif days == 0:
            stage_msg = "**starts today**"
        elif days == 1:
            stage_msg = "**starts tomorrow**"
        else:
            stage_msg = f"starts in <strong>{days} days</strong>"
        tennis_n = w["tennis_open"]
        cls = {
            "green": "highlight-green",
            "yellow": "highlight-muted",
            "muted": "highlight-muted",
        }[w["tier"]]
        wimbledon_html = (
            f'<div class="highlight {cls}">'
            f'🎾 <strong>Wimbledon</strong> {stage_msg} '
            f'(start: {w["start_iso"]}) · '
            f'<strong>{tennis_n}</strong> ATP/WTA markets currently open'
            f'</div>'
        )

    pnl_str, pnl_cls = _dash_fmt_pnl(overall["pnl"])

    # Build the qualifying-games table HTML. If no rows, show a hint
    # rather than an empty table — distinguishes "no eligible games"
    # from "stats_cache is missing".
    qual_rows_html: list[str] = []
    for q in qualifying_games:
        w = q.get("weather") or {}
        wparts = []
        if w.get("temp_f") is not None:
            wparts.append(f"{w['temp_f']:.0f}°F")
        if w.get("wind_mph") is not None:
            wparts.append(
                f"{w['wind_mph']:.0f}mph {w.get('wind_dir', '')}".strip()
            )
        wstr = ", ".join(wparts) if wparts else "—"
        tier_color = "#1e7e1e" if q["tier"] == "elite" else "#4a8fd8"
        pf = q.get("park_factor")
        pf_str = f"{pf:.2f}" if isinstance(pf, (int, float)) else "—"
        qual_rows_html.append(
            f"<tr>"
            f"<td>{q['date']}</td>"
            f"<td>{q['matchup']}</td>"
            f"<td>{q['away_pitcher'][:18]} ({q.get('away_pitcher','')!r})</td>".replace("'", "")
            if False else  # never — keep the simpler renderer below
            f"<td>{q['date']}</td>"
        )
    qual_rows_html = []  # rebuild cleanly
    for q in qualifying_games:
        w = q.get("weather") or {}
        wparts = []
        if w.get("temp_f") is not None:
            wparts.append(f"{w['temp_f']:.0f}°F")
        if w.get("wind_mph") is not None:
            wparts.append(
                f"{w['wind_mph']:.0f}mph {w.get('wind_dir', '')}".strip()
            )
        wstr = ", ".join(wparts) if wparts else "—"
        pf = q.get("park_factor")
        pf_str = f"{pf:.2f}" if isinstance(pf, (int, float)) else "—"
        tier_color = "#1e7e1e" if q["tier"] == "elite" else "#4a8fd8"
        qual_rows_html.append(
            f"<tr>"
            f"<td>{q['date']}</td>"
            f"<td>{q['matchup']}</td>"
            f"<td>{q['start_time_utc']}Z</td>"
            f"<td>{q['away_pitcher'][:20]}</td>"
            f"<td>{q['home_pitcher'][:20]}</td>"
            f"<td>{q['avg_era']:.2f}</td>"
            f"<td><span style='color:{tier_color};font-weight:600;'>"
            f"{q['tier']}</span></td>"
            f"<td>{pf_str}</td>"
            f"<td>{wstr}</td>"
            f"</tr>"
        )

    series_table_rows = []
    for r in series_rows:
        p_str, p_cls = _dash_fmt_pnl(r["pnl"])
        cal_str = (f"{r['cal_err']:+.1f}pp"
                   if r["cal_err"] is not None else "—")
        # Orange-tint the BUY_NO sub-row to match the Discord embed color
        # and signal at a glance that it's a side-stratified cohort, not
        # a different series.
        row_class = " class='side-no-row'" if r.get("side") == "no" else ""
        series_table_rows.append(
            f"<tr{row_class}>"
            f"<td>{r['series']}</td>"
            f"<td>{r['n']}</td>"
            f"<td>{r['w']}/{r['l']}</td>"
            f"<td>{r['wr']*100:.1f}%</td>"
            f"<td class='{p_cls}'>{p_str}</td>"
            f"<td>{cal_str}</td>"
            f"</tr>"
        )

    # Confidence-breakdown rows. The "HIGH inversion" check: if HIGH WR
    # < MEDIUM WR despite HIGH being the model's own self-confident bets,
    # something is miscalibrated — either the prompt rewards over-claiming
    # HIGH, or the edge gate is letting bad HIGH bets through. Flagging
    # visually with red on HIGH when MEDIUM beats it (both n >= 5).
    medium_row = next((r for r in confidence_rows if r["confidence"] == "MEDIUM"), None)
    high_inverted = (
        medium_row is not None
        and medium_row["n"] >= 5
        and any(
            r["confidence"] == "HIGH" and r["n"] >= 5 and r["wr"] < medium_row["wr"]
            for r in confidence_rows
        )
    )
    confidence_table_rows = []
    for r in confidence_rows:
        p_str, p_cls = _dash_fmt_pnl(r["pnl"])
        cal_str = (f"{r['cal_err']:+.1f}pp"
                   if r["cal_err"] is not None else "—")
        row_class = ""
        if (
            r["confidence"] == "HIGH" and high_inverted
            and r["n"] >= 5 and medium_row and r["wr"] < medium_row["wr"]
        ):
            row_class = " class='conf-inverted'"
        confidence_table_rows.append(
            f"<tr{row_class}>"
            f"<td>{r['confidence']}</td>"
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
        # Orange-tinted badge for NO bets, neutral badge for YES — matches
        # the Discord embed color (#E67E22) so side-stratified rows read
        # the same in both UIs.
        side_class = "side-no" if side == "no" else (
            "side-yes" if side == "yes" else "side-unknown"
        )
        side_html = f"<span class='side-badge {side_class}'>{side.upper()}</span>"
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
            f"<td>{side_html}</td>"
            f"<td>{edge_str}</td>"
            f"<td class='{oc_class}'>{outcome}</td>"
            f"<td class='{pn_cls}'>{pn_str}</td>"
            f"</tr>"
        )

    # Gate Activity (last 24h). One row per named gate so dormant gates
    # are visible too (0 count). cycles_in_window = None means the edge
    # agent hasn't completed a cycle since deploy — show a hint instead.
    _gate_rows_spec: list[tuple[str, str, str]] = [
        ("rule1_self_enforced", "Rule 1 self-enforced",
         "Claude cited Rule 1 in SKIP reasoning"),
        ("rule1_violation", "Rule 1 violation (code)",
         "Code gate caught a BUY that contradicts projection"),
        ("edge_sanity_fail", "Edge sanity fail",
         "Claimed edge disagrees with true_p − ask by &gt; 0.15"),
        ("buy_no_ineligible", "BUY NO ineligible",
         "Out of -9 cohort or wrong confidence tier"),
        ("buy_no_projection_fail", "BUY NO projection fail",
         "Projection > 7.5 on a -9 ticker"),
        ("max_edge_cap", "Max edge cap",
         "Claimed edge above 18% sanity cap (context)"),
        ("claude_skip", "Claude SKIP (other)",
         "Claude voted SKIP without a Rule 1 mention (context)"),
    ]
    if gate_activity["cycles_in_window"] is None:
        gate_activity_html = (
            '<div class="highlight highlight-muted">'
            '🟡 Gate activity file not present yet — '
            'edge agent has not completed a cycle since deploy.'
            '</div>'
        )
    else:
        gate_rows_html: list[str] = []
        for key, label, hint in _gate_rows_spec:
            count = int(gate_activity["totals_24h"].get(key, 0))
            count_cls = "muted" if count == 0 else ""
            gate_rows_html.append(
                f"<tr>"
                f"<td>{label}</td>"
                f"<td class='{count_cls}'>{count}</td>"
                f"<td class='muted'>{hint}</td>"
                f"</tr>"
            )
        cyc_n = gate_activity["cycles_in_window"]
        upd = (gate_activity["updated_at"] or "")[:19].replace("T", " ")
        gate_activity_html = (
            f'<p class="muted" style="margin-top:0;">'
            f'Rolling 24h post-Claude drop counts across '
            f'<strong>{cyc_n}</strong> cycle(s). '
            f'Updated {upd} UTC. Dormant gates (0) stay visible so you '
            f'can spot a regression where one stops firing.'
            f'</p>'
            f'<table>'
            f'<thead><tr><th>Gate</th><th>24h count</th><th>What it catches</th></tr></thead>'
            f'<tbody>{"".join(gate_rows_html)}</tbody>'
            f'</table>'
        )

    # Prediction Accuracy (KXMLBTOTAL). Colored by mean absolute
    # error in runs: green < 1.0 (essentially right), yellow 1.0-2.0
    # (off but within tolerance), red > 2.0 (systematically off).
    # Bias is whichever of OVER / UNDER has more entries (EVEN on a
    # tie) — direction the model is most often wrong in.
    pa = prediction_accuracy
    if pa["n"] == 0:
        prediction_accuracy_html = (
            '<div class="highlight highlight-muted">'
            '🟡 <strong>Prediction accuracy (KXMLBTOTAL):</strong> '
            'no accuracy records yet for this window.'
            '</div>'
        )
    else:
        cls_map = {
            "green": "highlight-green",
            "yellow": "highlight-muted",
            "red": "highlight-red",
        }
        icon_map = {"green": "🟢", "yellow": "🟡", "red": "🔴"}
        cls = cls_map[pa["severity"]]
        icon = icon_map[pa["severity"]]
        bias_str = (
            f"{pa['bias']} ({pa['n_over']} OVER / {pa['n_under']} UNDER)"
            if pa["bias"] != "EVEN"
            else f"EVEN ({pa['n_over']} OVER / {pa['n_under']} UNDER)"
        )
        prediction_accuracy_html = (
            f'<div class="highlight {cls}">'
            f'{icon} <strong>Prediction accuracy (KXMLBTOTAL):</strong>'
            f'<ul style="margin:6px 0 0 0; padding-left:20px;">'
            f'<li>Mean error: <strong>{pa["mean_error"]:.2f} runs</strong></li>'
            f'<li>Bias: <strong>{bias_str}</strong></li>'
            f'<li>Direction correct: '
            f'<strong>{pa["n_correct"]}/{pa["n"]}</strong></li>'
            f'</ul>'
            f'</div>'
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
  .window-bar {{ background: #eef4ff; border: 1px solid #cfdcf5;
                 padding: 8px 14px; border-radius: 6px; margin: 0.5em 0 1em; }}
  .window-bar a {{ margin-left: 0.5em; color: #1a4fb5; text-decoration: none; }}
  .window-bar a:hover {{ text-decoration: underline; }}
  .highlight {{ padding: 10px 14px; border-radius: 6px; margin: 0.4em 0;
                font-size: 0.95em; }}
  .highlight-green {{ background: #e6f6e6; border: 1px solid #b6e0b6;
                       color: #1b4d1b; }}
  .highlight-muted {{ background: #fff8e0; border: 1px solid #f0d97a;
                       color: #6b5712; }}
  .highlight-red {{ background: #fdecea; border: 1px solid #f5b5b0;
                     color: #8a1c14; }}
  .side-badge {{ display: inline-block; padding: 1px 7px; border-radius: 4px;
                  font-size: 0.78em; font-weight: 600; letter-spacing: 0.03em; }}
  .side-yes {{ background: #e6f6e6; color: #1b4d1b; }}
  .side-no {{ background: #fbe5d0; color: #8a4416; }}
  .side-unknown {{ background: #efefef; color: #888; }}
  .side-no-row {{ background: #fff4ea; }}
  .side-no-row td:first-child {{ color: #8a4416; font-weight: 600; }}
  .conf-inverted {{ background: #fdecea; }}
  .conf-inverted td:first-child {{ color: #b22222; font-weight: 700; }}
  .trade-cards {{ display: flex; gap: 1em; flex-wrap: wrap; margin: 0.5em 0 1.5em; }}
  .trade-card {{ flex: 1 1 0; min-width: 280px; padding: 12px 16px;
                 border-radius: 6px; border: 1px solid #e5e5e9;
                 background: #f7f7f9; font-size: 0.92em; line-height: 1.5; }}
  .trade-card-yes {{ background: #e6f6e6; border-color: #b6e0b6; }}
  .trade-card-no {{ background: #fbe5d0; border-color: #f0c89c; }}
  .trade-card-label {{ font-size: 0.85em; font-weight: 600;
                       color: #555; margin-bottom: 4px; }}
  .trade-card-game {{ font-size: 1.05em; margin-bottom: 4px; }}
  .trade-card-pitchers {{ color: #444; font-size: 0.88em; margin-bottom: 4px; }}
  .trade-card-evict {{ font-size: 0.88em; color: #333; }}
  .trade-card-empty {{ color: #888; font-style: italic; }}
</style>
</head>
<body>
<h1>Polymarket Bot {is_live_badge}</h1>
<p class="muted">Last refresh: {now_iso} UTC · Auto-refresh: 60s</p>
{window_bar}
{best_game_html}
{btc_html}
{recalib_html}
{wimbledon_html}

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

<h2>Trade frequency — last 14 days</h2>
{freq_svg}

<h2>Gate activity — last 24h</h2>
{gate_activity_html}

<h2>Next expected trade</h2>
{next_trade_html}

<h2>Next qualifying KXMLBTOTAL games</h2>
<p class="muted" style="margin-top:0;">
Games where avg(starter rolling_era_last3) ≤ {_DASH_COHORT_ERA_CAP} —
the production cohort filter. If empty, expect KXMLBTOTAL to SKIP today.
</p>
<table>
<thead>
<tr><th>Date</th><th>Matchup</th><th>Start</th><th>Away SP</th><th>Home SP</th><th>Avg ERA</th><th>Tier</th><th>Park</th><th>Weather</th></tr>
</thead>
<tbody>
{"".join(qual_rows_html) or "<tr><td colspan='9' class='muted'>no upcoming games clear the cohort filter — today is a likely no-trade day for KXMLBTOTAL</td></tr>"}
</tbody>
</table>

<h2>By series</h2>
<table>
<thead>
<tr><th>Series</th><th>Trades</th><th>W/L</th><th>Win%</th><th>P&amp;L</th><th>Cal err</th></tr>
</thead>
<tbody>
{"".join(series_table_rows) or "<tr><td colspan='6' class='muted'>no settled trades yet</td></tr>"}
</tbody>
</table>

<h2>By confidence</h2>
<p class="muted" style="margin-top:0;">
HIGH-conf bets should out-perform MEDIUM (the model picked them with
less uncertainty). If HIGH win-rate sits below MEDIUM with n &gt;= 5 on
both, the row is flagged red — that's a HIGH-inversion signal.
</p>
<table>
<thead>
<tr><th>Confidence</th><th>Trades</th><th>W/L</th><th>Win%</th><th>P&amp;L</th><th>Cal err</th></tr>
</thead>
<tbody>
{"".join(confidence_table_rows) or "<tr><td colspan='6' class='muted'>no settled trades yet</td></tr>"}
</tbody>
</table>

<h2>Prediction accuracy (KXMLBTOTAL)</h2>
{prediction_accuracy_html}

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
