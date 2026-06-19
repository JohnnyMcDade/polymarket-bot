"""Kalshi win-rate reporter.

Reads trades_log.json once a day at KALSHI_WINRATE_HOUR UTC, computes
the performance numbers everyone actually cares about, and ships them
as one Discord embed:

  - Win rate %, decided trades only (pending excluded)
  - Total PnL in dollars
  - ROI % vs total stake deployed
  - Best + worst single trade
  - Avg edge on winning trades vs losing trades
  - Breakdown by series (KXMLBGAME, KXATPMATCH, …) and confidence (HIGH/MEDIUM)

Also appends a per-day snapshot to winrate_history.csv so we accumulate
our own backtest dataset over time. Schema is long-format:
  date, dimension, key, decided, wins, losses, win_rate, total_pnl, avg_edge
Downstream consumers should dedup on (date, dimension, key) keeping the
last write — same-day reruns will produce dupes by design.

Zero Claude calls — pure pandas-light math on the JSON log.
"""

from __future__ import annotations

import csv
import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable

import requests

WEBHOOK_KALSHI_WINRATE = os.getenv("WEBHOOK_KALSHI_WINRATE", "")
WINRATE_HOUR = int(os.getenv("KALSHI_WINRATE_HOUR", "7"))
# Weekly summary fires every Monday at WEEKLY_HOUR UTC. Reuses the same
# webhook as the daily — it's the same audience and channel; the embed
# title makes the cadence obvious.
WEEKLY_HOUR = int(os.getenv("KALSHI_WEEKLY_HOUR", "8"))
WEEKLY_DOW = int(os.getenv("KALSHI_WEEKLY_DOW", "0"))  # 0 = Monday
# Nightly BUY_NO watchdog: fires at BUY_NO_WATCH_HOUR UTC to report
# whether any side=no trade fired in the last 24h. Default 23:00 UTC =
# right as US evening MLB games start (T-30min-start eviction has just
# fired for tonight's elite-pitching matchups). Designed for the first
# 30 days of the BUY_NO staged rollout (2026-06-17) — surfaces silent
# non-firing as well as actual fires.
BUY_NO_WATCH_HOUR = int(os.getenv("KALSHI_BUY_NO_WATCH_HOUR", "23"))
# Public dashboard URL surfaced in the daily Discord embed so anyone
# subscribed to the channel can one-click into the live HTML view.
# Override via env if the Railway public domain ever changes.
DASHBOARD_URL = os.getenv(
    "KALSHI_DASHBOARD_URL",
    "https://worker-production-0858.up.railway.app/dashboard",
)
TRADES_LOG_PATH = Path(os.getenv("KALSHI_TRADES_LOG", "/app/data/trades_log.json"))
CSV_HISTORY_PATH = Path(os.getenv("KALSHI_WINRATE_CSV", "/app/data/winrate_history.csv"))
# Macro signals (F&G + BTC spot) surfaced in the daily 7AM embed so the
# operator sees today's gating posture without opening the dashboard.
# Path mirrors kalshi_stats.STATS_CACHE_PATH default — using the env
# var (not a direct import) so the two modules stay loosely coupled.
STATS_CACHE_PATH = Path(os.getenv("KALSHI_STATS_CACHE", "/app/data/stats_cache.json"))
# Prediction accuracy snapshot for the 7AM daily embed. Written by
# kalshi_trader._record_prediction_accuracy at each KXMLBTOTAL
# settlement. Read-only here — we never write back.
PREDICTION_ACCURACY_PATH = Path(os.getenv(
    "KALSHI_PREDICTION_ACCURACY", "/app/data/prediction_accuracy.json"
))
# Threshold for the daily "needs systematic correction" flag in the
# accuracy embed field. Mean abs error > this AND at least
# PREDICTION_ACCURACY_FLAG_MIN_N samples both required — small-n noise
# would otherwise trip the flag.
PREDICTION_ACCURACY_FLAG_MIN_ERROR = float(os.getenv(
    "KALSHI_ACCURACY_FLAG_MIN_ERROR", "1.5"
))
PREDICTION_ACCURACY_FLAG_MIN_N = int(os.getenv(
    "KALSHI_ACCURACY_FLAG_MIN_N", "15"
))

# Auto-go-live: flip PAPER_TRADING=false via Railway API when all 4 calibration
# criteria pass on N consecutive daily reports. Disabled by default — user must
# set KALSHI_AUTO_LIVE_ENABLED=true on Railway to arm. `skipDeploys: true` is
# used so the redeploy is a separate, manual step (the process running this
# code is the one being redeployed; auto-redeploy would kill the Discord post
# mid-flight).
AUTO_LIVE_ENABLED = os.getenv("KALSHI_AUTO_LIVE_ENABLED", "false").lower() in ("1", "true", "yes")
REQUIRED_CONSECUTIVE_PASSES = int(os.getenv("KALSHI_AUTO_LIVE_CONSECUTIVE", "2"))
GO_LIVE_STATE_PATH = Path(os.getenv("KALSHI_GO_LIVE_STATE", "/app/data/go_live_state.json"))
RAILWAY_API_URL = os.getenv("RAILWAY_API_URL", "https://backboard.railway.com/graphql/v2")
RAILWAY_API_TOKEN = os.getenv("RAILWAY_API_TOKEN", "")
RAILWAY_PROJECT_ID = os.getenv("RAILWAY_PROJECT_ID", "")
RAILWAY_SERVICE_ID = os.getenv("RAILWAY_SERVICE_ID", "")
RAILWAY_ENVIRONMENT_ID = os.getenv("RAILWAY_ENVIRONMENT_ID", "")
PUSHOVER_TOKEN = os.getenv("PUSHOVER_TOKEN", "")
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY", "")
CSV_FIELDS = [
    "date", "dimension", "key",
    "decided", "wins", "losses", "win_rate",
    "total_pnl", "avg_edge",
]


def _format_usd(amount: float) -> str:
    sign = "-" if amount < 0 else ""
    a = abs(amount)
    if a >= 1_000_000:
        return f"{sign}${a / 1_000_000:.2f}M"
    if a >= 1_000:
        return f"{sign}${a / 1_000:.1f}K"
    return f"{sign}${a:.2f}"


def _load_trades() -> list[dict[str, Any]]:
    if not TRADES_LOG_PATH.exists():
        return []
    try:
        with TRADES_LOG_PATH.open() as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[WARN] trades_log unreadable: {e}", flush=True)
        return []


def _load_macro_snapshot() -> dict[str, Any]:
    """Pull F&G + BTC spot + 24h momentum from the stats cache for the
    daily embed. Returns a dict with formatted strings ready for Discord
    field values, plus an `ok` flag — False when the cache is missing
    or the macro block is empty (so the embed renders a graceful '—'
    rather than blank fields)."""
    if not STATS_CACHE_PATH.exists():
        return {"ok": False, "reason": "no stats_cache.json"}
    try:
        with STATS_CACHE_PATH.open() as f:
            stats = json.load(f)
    except Exception as e:
        return {"ok": False, "reason": f"stats_cache unreadable: {e}"}
    econ = stats.get("economic") or {}
    fng = econ.get("crypto_fear_greed_value")
    fng_class = econ.get("crypto_fear_greed_classification") or ""
    momentum = econ.get("btc_24h_momentum_pct")
    btc_spot = econ.get("btc_spot_usd")
    if fng is None and btc_spot is None:
        return {"ok": False, "reason": "macro block empty"}
    return {
        "ok": True,
        "fng": fng,
        "fng_class": fng_class,
        "momentum_pct": momentum,
        "btc_spot": btc_spot,
    }


def _load_prediction_accuracy_stats() -> dict[str, Any]:
    """Aggregate every KXMLBTOTAL projection-vs-actual record from
    /app/data/prediction_accuracy.json for the daily 7AM embed.

    Returns:
      n (int), mean_error (float|None — MAE in runs, None when n=0),
      n_over / n_under / n_correct (int),
      bias ('OVER'|'UNDER'|'EVEN'|None — whichever count is higher),
      flag (bool — True when MAE > PREDICTION_ACCURACY_FLAG_MIN_ERROR
      AND n >= PREDICTION_ACCURACY_FLAG_MIN_N; trips the "needs
      systematic correction" warning).

    No `since` filter: the bot only began writing accuracy records
    after the 2026-06-18 deploy, so every record is post-fix by
    construction — narrowing the window would just shrink n for no
    benefit."""
    empty = {
        "n": 0, "mean_error": None, "n_over": 0, "n_under": 0,
        "n_correct": 0, "bias": None, "flag": False,
    }
    if not PREDICTION_ACCURACY_PATH.exists():
        return empty
    try:
        store = json.loads(PREDICTION_ACCURACY_PATH.read_text()) or {}
    except (json.JSONDecodeError, OSError):
        return empty
    rows = list(store.values())
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
    flag = (
        mean_err > PREDICTION_ACCURACY_FLAG_MIN_ERROR
        and len(rows) >= PREDICTION_ACCURACY_FLAG_MIN_N
    )
    return {
        "n": len(rows),
        "mean_error": mean_err,
        "n_over": n_over,
        "n_under": n_under,
        "n_correct": n_correct,
        "bias": bias,
        "flag": flag,
    }


def _series_of(ticker: str) -> str:
    if not ticker:
        return "UNKNOWN"
    return ticker.split("-", 1)[0]


def _breakdown(
    decided: list[dict[str, Any]],
    key_fn: Callable[[dict[str, Any]], str],
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for t in decided:
        k = key_fn(t) or "UNKNOWN"
        groups.setdefault(k, []).append(t)
    out: dict[str, dict[str, Any]] = {}
    for k, items in groups.items():
        wins = [t for t in items if t["outcome"] == "won"]
        losses = [t for t in items if t["outcome"] == "lost"]
        pnl = sum(float(t.get("pnl", 0)) for t in items)
        edge_sum = sum(float(t.get("edge", 0)) for t in items)
        out[k] = {
            "decided": len(items),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / len(items) if items else 0.0,
            "total_pnl": pnl,
            "avg_edge": edge_sum / len(items) if items else 0.0,
        }
    return out


def _compute_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    decided = [t for t in trades if t.get("outcome") in ("won", "lost")]
    pending = [t for t in trades if t.get("outcome") == "pending"]
    wins = [t for t in decided if t["outcome"] == "won"]
    losses = [t for t in decided if t["outcome"] == "lost"]

    total_pnl = sum(float(t.get("pnl", 0)) for t in decided)
    total_stake = sum(float(t.get("bet_size", 0)) for t in decided)
    roi = (total_pnl / total_stake) if total_stake > 0 else 0.0

    win_rate = (len(wins) / len(decided)) if decided else 0.0

    avg_edge_wins = (
        sum(float(t.get("edge", 0)) for t in wins) / len(wins) if wins else 0.0
    )
    avg_edge_losses = (
        sum(float(t.get("edge", 0)) for t in losses) / len(losses) if losses else 0.0
    )
    avg_edge_overall = (
        sum(float(t.get("edge", 0)) for t in decided) / len(decided) if decided else 0.0
    )

    best = max(decided, key=lambda t: float(t.get("pnl", 0)), default=None)
    worst = min(decided, key=lambda t: float(t.get("pnl", 0)), default=None)

    by_series = _breakdown(decided, lambda t: _series_of(t.get("ticker", "")))
    by_confidence = _breakdown(decided, lambda t: t.get("confidence") or "UNKNOWN")

    return {
        "total_trades": len(trades),
        "decided": len(decided),
        "pending": len(pending),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "total_stake": total_stake,
        "roi": roi,
        "avg_edge_wins": avg_edge_wins,
        "avg_edge_losses": avg_edge_losses,
        "avg_edge_overall": avg_edge_overall,
        "best": best,
        "worst": worst,
        "by_series": by_series,
        "by_confidence": by_confidence,
    }


def _format_breakdown(breakdown: dict[str, dict[str, Any]], limit: int = 6) -> str:
    if not breakdown:
        return "—"
    items = sorted(breakdown.items(), key=lambda kv: -kv[1]["decided"])[:limit]
    lines = []
    for k, b in items:
        lines.append(
            f"`{k:<12}` {b['win_rate']:.0%} "
            f"({b['wins']}W/{b['losses']}L) "
            f"{_format_usd(b['total_pnl'])} edge={b['avg_edge']*100:+.0f}%"
        )
    return "\n".join(lines)


def _append_csv(stats: dict[str, Any]) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows: list[dict[str, Any]] = [{
        "date": today,
        "dimension": "overall",
        "key": "overall",
        "decided": stats["decided"],
        "wins": stats["wins"],
        "losses": stats["losses"],
        "win_rate": f"{stats['win_rate']:.4f}",
        "total_pnl": f"{stats['total_pnl']:.2f}",
        "avg_edge": f"{stats['avg_edge_overall']:.4f}",
    }]
    for dim, key in (("series", "by_series"), ("confidence", "by_confidence")):
        for name, b in stats[key].items():
            rows.append({
                "date": today,
                "dimension": dim,
                "key": name,
                "decided": b["decided"],
                "wins": b["wins"],
                "losses": b["losses"],
                "win_rate": f"{b['win_rate']:.4f}",
                "total_pnl": f"{b['total_pnl']:.2f}",
                "avg_edge": f"{b['avg_edge']:.4f}",
            })
    CSV_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_header = not CSV_HISTORY_PATH.exists()
    with CSV_HISTORY_PATH.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[winrate] appended {len(rows)} rows to {CSV_HISTORY_PATH}", flush=True)


def _build_embed(s: dict[str, Any]) -> dict[str, Any]:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    color = 0x2ECC71 if s["total_pnl"] >= 0 else 0xE74C3C

    def _trade_line(t: dict[str, Any] | None) -> str:
        if not t:
            return "—"
        title = (t.get("title") or t.get("ticker") or "?")[:60]
        return f"{title} → {_format_usd(float(t.get('pnl', 0)))}"

    fields = [
        {"name": "🏆 Win Rate",
         "value": f"{s['win_rate']:.1%} ({s['wins']}W / {s['losses']}L)",
         "inline": True},
        {"name": "💰 Total PnL", "value": _format_usd(s["total_pnl"]), "inline": True},
        {"name": "📈 ROI", "value": f"{s['roi']*100:+.1f}%", "inline": True},
        {"name": "💵 Total Staked", "value": _format_usd(s["total_stake"]), "inline": True},
        {"name": "📊 Decided Trades", "value": str(s["decided"]), "inline": True},
        {"name": "⏳ Pending", "value": str(s["pending"]), "inline": True},
        {"name": "✅ Avg Edge — Wins",
         "value": f"{s['avg_edge_wins']*100:+.1f}%", "inline": True},
        {"name": "❌ Avg Edge — Losses",
         "value": f"{s['avg_edge_losses']*100:+.1f}%", "inline": True},
        {"name": "Δ", "value": f"{(s['avg_edge_wins']-s['avg_edge_losses'])*100:+.1f}%", "inline": True},
        {"name": "🥇 Best Trade", "value": _trade_line(s["best"]), "inline": False},
        {"name": "🥶 Worst Trade", "value": _trade_line(s["worst"]), "inline": False},
        {"name": "📋 By Series", "value": _format_breakdown(s["by_series"]) or "—", "inline": False},
        {"name": "🎯 By Confidence", "value": _format_breakdown(s["by_confidence"]) or "—", "inline": False},
    ]

    if s["decided"] == 0:
        fields.insert(0, {
            "name": "Status",
            "value": "No decided trades yet — report will populate as markets settle.",
            "inline": False,
        })

    # Macro snapshot — F&G + BTC. Sits right above the dashboard link so
    # it's the last thing scanned before the operator decides whether to
    # open the dashboard. Same source as the dashboard's BTC highlight
    # (stats_cache.json economic block) so the two never disagree.
    macro = _load_macro_snapshot()
    if macro.get("ok"):
        fng = macro["fng"]
        fng_class = macro["fng_class"]
        mom = macro["momentum_pct"]
        btc = macro["btc_spot"]
        if isinstance(fng, (int, float)):
            fng_int = int(fng)
            if fng_int < 21:
                fng_emoji = "🟢"  # extreme fear → contrarian bounce signal
            elif fng_int > 80:
                fng_emoji = "🔴"  # extreme greed → contrarian reversal signal
            else:
                fng_emoji = "🟡"  # calm zone → KXBTC filter blocks
            fng_str = f"{fng_emoji} {fng_int} ({fng_class})" if fng_class else f"{fng_emoji} {fng_int}"
        else:
            fng_str = "—"
        if isinstance(btc, (int, float)):
            btc_str = f"${btc:,.0f}"
        else:
            btc_str = "—"
        if isinstance(mom, (int, float)):
            arrow = "📈" if mom >= 0 else "📉"
            mom_str = f"{arrow} {mom:+.2f}% 24h"
        else:
            mom_str = "—"
        fields.append({"name": "😨 Fear & Greed", "value": fng_str, "inline": True})
        fields.append({"name": "₿ BTC spot", "value": btc_str, "inline": True})
        fields.append({"name": "📊 BTC 24h", "value": mom_str, "inline": True})
    else:
        fields.append({
            "name": "📊 Macro",
            "value": f"—  ({macro.get('reason', 'unavailable')})",
            "inline": False,
        })

    # Prediction accuracy (KXMLBTOTAL). Compares projected_total
    # extracted from each settled trade's reasoning string against
    # actual final game runs from statsapi.mlb.com. Trips a FLAG
    # line when MAE > 1.5 runs over ≥ 15 samples — at that point a
    # systematic +1.5 run correction to KXMLBTOTAL projections is
    # justified by the data, not noise.
    pa = _load_prediction_accuracy_stats()
    if pa["n"] == 0:
        pa_value = "—  (no accuracy records yet)"
    else:
        flag_str = (
            f"\n⚠️ **FLAG**: mean error > "
            f"{PREDICTION_ACCURACY_FLAG_MIN_ERROR:.1f} runs over "
            f"{pa['n']} samples — consider a systematic +"
            f"{PREDICTION_ACCURACY_FLAG_MIN_ERROR:.1f} run correction "
            f"to KXMLBTOTAL projections."
            if pa["flag"] else ""
        )
        pa_value = (
            f"Mean error: **{pa['mean_error']:.2f} runs** | "
            f"Bias: **{pa['bias']}** "
            f"({pa['n_over']} OVER / {pa['n_under']} UNDER)\n"
            f"Direction correct: **{pa['n_correct']}/{pa['n']}**"
            f"{flag_str}"
        )
    fields.append({
        "name": "🎯 Prediction Accuracy (KXMLBTOTAL)",
        "value": pa_value,
        "inline": False,
    })

    # Clickable dashboard link as the last field. The /dashboard route
    # mirrors most of this report (overall + per-series tables + last
    # 10 trades) and adds an SVG cumulative-P&L chart that doesn't fit
    # in a Discord embed.
    fields.append({
        "name": "📊 View Dashboard",
        "value": (
            "[Open live dashboard]"
            f"({DASHBOARD_URL})"
        ),
        "inline": False,
    })
    return {
        "title": "📊 KALSHI WIN-RATE — daily report",
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Kalshi Win-Rate  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _pace_estimate(
    trades: list[dict[str, Any]],
    since_iso: str | None,
    target_n: int,
) -> dict[str, Any]:
    """Days remaining until trade count reaches target_n at the current pace.

    Uses calendar days between `since_iso` (UTC midnight if bare date) and now.
    Returns dict with keys: pace_per_day, days_remaining (None if pace<=0 and
    threshold not yet met).
    """
    if not since_iso:
        return {"pace_per_day": None, "days_remaining": None}
    iso = since_iso if "T" in since_iso else f"{since_iso}T00:00:00+00:00"
    try:
        since_dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return {"pace_per_day": None, "days_remaining": None}
    days = max((datetime.now(timezone.utc) - since_dt).total_seconds() / 86400.0, 1.0)
    n = len(trades)
    pace = n / days
    if n >= target_n:
        return {"pace_per_day": pace, "days_remaining": 0.0}
    if pace <= 0:
        return {"pace_per_day": 0.0, "days_remaining": None}
    return {"pace_per_day": pace, "days_remaining": (target_n - n) / pace}


def _last_n_pnl_path(trades: list[dict[str, Any]], k: int = 5) -> str:
    """Cumulative PnL after each of the last k trades, chronological."""
    if not trades:
        return "—"
    ordered = sorted(
        trades,
        key=lambda t: str(t.get("settled_at") or t.get("timestamp") or ""),
    )
    recent = ordered[-k:]
    cum = 0.0
    parts = []
    for t in recent:
        cum += float(t.get("pnl", 0))
        sign = "+" if cum >= 0 else "−"
        parts.append(f"{sign}${abs(cum):.0f}")
    return " → ".join(parts)


def _build_calibration_embed(r: dict[str, Any]) -> dict[str, Any]:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    n = r["n"]
    since_iso = r.get("since_iso")
    title_suffix = f" (since {since_iso})" if since_iso else ""

    if n == 0 or not r.get("overall"):
        return {
            "title": f"📐 KALSHI CALIBRATION — daily{title_suffix}",
            "color": 0x95A5A6,
            "fields": [{
                "name": "Status",
                "value": (
                    f"No settled trades in window "
                    f"(filtered {r['n']} of {r['raw_n']})."
                ),
                "inline": False,
            }],
            "footer": {"text": f"PassivePoly Kalshi Calibration  •  {now_str}"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    overall = r["overall"]
    checks = r["criteria_checks"]
    passes = sum(1 for _, p, _ in checks if p)
    total = len(checks) or 1
    if r["all_pass"]:
        color = 0x2ECC71
    elif passes >= max(total // 2, 1):
        color = 0xF39C12
    else:
        color = 0xE74C3C

    high = r["conf"].get("HIGH")
    high_str = f"{high['cal_err']:.1%} (n={high['n']})" if high else "— (no HIGH)"

    series_ranked = sorted(r["series"].values(), key=lambda s: -s["pnl"])[:2]
    if series_ranked:
        series_str = "\n".join(
            f"`{s['label']:<11}` {s['win_rate']:.0%} "
            f"({s['wins']}W/{s['n'] - s['wins']}L) "
            f"{_format_usd(s['pnl'])} cal_err={s['cal_err']:.0%}"
            for s in series_ranked
        )
    else:
        series_str = "—"

    if checks:
        crit_str = "\n".join(
            f"{'✅' if passed else '❌'} {label} — {actual}"
            for label, passed, actual in checks
        )
    else:
        crit_str = "—"

    target_n = int(((r.get("criteria") or {}).get("criteria") or {}).get("min_settled_trades", 50))
    pace = _pace_estimate(r["trades"], since_iso, target_n)
    if n >= target_n:
        pace_str = f"N={target_n} threshold met ✓"
    elif pace["pace_per_day"]:
        pace_str = (
            f"{pace['pace_per_day']:.1f} trades/day → "
            f"~{pace['days_remaining']:.0f} days to N={target_n}"
        )
    else:
        pace_str = "insufficient data"

    path_str = _last_n_pnl_path(r["trades"], k=5)
    verdict = "GO LIVE" if r["all_pass"] else "STAY PAPER"

    fields = [
        {"name": "🪟 Window",
         "value": f"{n} of {r['raw_n']} settled",
         "inline": True},
        {"name": "🏆 Win Rate",
         "value": f"{overall['win_rate']:.1%} ({overall['wins']}W/{n - overall['wins']}L)",
         "inline": True},
        {"name": "💰 PnL",
         "value": _format_usd(overall["pnl"]),
         "inline": True},
        {"name": "📊 Brier",
         "value": f"{r['brier']:.3f} (vs 0.25)",
         "inline": True},
        {"name": "📉 ECE",
         "value": f"{r['ece']:.1%}",
         "inline": True},
        {"name": "🎯 HIGH cal err",
         "value": high_str,
         "inline": True},
        {"name": f"🚦 Go-Live: {passes}/{total} → {verdict}",
         "value": crit_str,
         "inline": False},
        {"name": "🥇 Best Series (by PnL)",
         "value": series_str,
         "inline": False},
        {"name": "📅 Pace",
         "value": pace_str,
         "inline": False},
        {"name": "📈 Cumulative PnL (last 5)",
         "value": f"`{path_str}`",
         "inline": False},
        {"name": "📊 View Dashboard",
         "value": f"[Open live dashboard]({DASHBOARD_URL})",
         "inline": False},
    ]

    return {
        "title": f"📐 KALSHI CALIBRATION — daily{title_suffix}",
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Kalshi Calibration  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _load_go_live_state() -> dict[str, Any]:
    if not GO_LIVE_STATE_PATH.exists():
        return {
            "consecutive_pass_count": 0,
            "last_pass_date": None,
            "last_report_date": None,
            "is_live": False,
            "live_flipped_at": None,
            "trigger_snapshot": None,
            "history": [],
        }
    try:
        with GO_LIVE_STATE_PATH.open() as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] go_live_state unreadable: {e}", flush=True)
        return {
            "consecutive_pass_count": 0,
            "last_pass_date": None,
            "last_report_date": None,
            "is_live": False,
            "live_flipped_at": None,
            "trigger_snapshot": None,
            "history": [],
        }


def _save_go_live_state(state: dict[str, Any]) -> None:
    GO_LIVE_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with GO_LIVE_STATE_PATH.open("w") as f:
        json.dump(state, f, indent=2)


def _railway_set_variable(name: str, value: str) -> tuple[bool, str]:
    """Upsert a service-scoped Railway variable via GraphQL.

    Uses `skipDeploys: true` so this does NOT auto-redeploy. The caller (or a
    human reviewing the Discord notification) must redeploy for the new value
    to take effect on the running container.

    Returns (ok, message). On success message is "ok"; on failure it carries
    the diagnostic to surface in logs.
    """
    if not RAILWAY_API_TOKEN:
        return False, "RAILWAY_API_TOKEN not set"
    if not (RAILWAY_PROJECT_ID and RAILWAY_SERVICE_ID and RAILWAY_ENVIRONMENT_ID):
        return False, "RAILWAY_PROJECT_ID / SERVICE_ID / ENVIRONMENT_ID not all set"
    query = (
        "mutation variableUpsert($input: VariableUpsertInput!) { "
        "variableUpsert(input: $input) }"
    )
    payload = {
        "query": query,
        "variables": {
            "input": {
                "projectId": RAILWAY_PROJECT_ID,
                "environmentId": RAILWAY_ENVIRONMENT_ID,
                "serviceId": RAILWAY_SERVICE_ID,
                "name": name,
                "value": value,
                "skipDeploys": True,
            }
        },
    }
    try:
        r = requests.post(
            RAILWAY_API_URL,
            headers={
                "Authorization": f"Bearer {RAILWAY_API_TOKEN}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
    except Exception as e:
        return False, f"request failed: {type(e).__name__}: {e}"
    if r.status_code != 200:
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    try:
        body = r.json()
    except Exception as e:
        return False, f"non-JSON response: {type(e).__name__}: {e}"
    if body.get("errors"):
        return False, f"GraphQL errors: {body['errors']}"
    return True, "ok"


def _send_pushover(title: str, message: str) -> bool:
    if not (PUSHOVER_TOKEN and PUSHOVER_USER_KEY):
        return False
    try:
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": PUSHOVER_TOKEN,
                "user": PUSHOVER_USER_KEY,
                "title": title,
                "message": message,
                "priority": 1,
            },
            timeout=10,
        )
        if r.status_code != 200:
            print(f"[WARN] Pushover {r.status_code}: {r.text[:200]}", flush=True)
            return False
        return True
    except Exception as e:
        print(f"[WARN] Pushover send failed: {e}", flush=True)
        return False


def _build_going_live_embed(
    cal: dict[str, Any], state: dict[str, Any], railway_ok: bool, railway_msg: str
) -> dict[str, Any]:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    overall = cal["overall"]
    n = cal["n"]
    high = cal["conf"].get("HIGH")
    high_str = f"{high['cal_err']:.1%} (n={high['n']})" if high else "—"

    crit_lines = "\n".join(
        f"✅ {label} — {actual}" for label, _, actual in cal["criteria_checks"]
    )

    bankroll = (cal.get("criteria") or {}).get("live_bankroll_usd", "?")

    if railway_ok:
        action_value = (
            f"`PAPER_TRADING=false` set on Railway with `skipDeploys: true`.\n"
            f"**Run `railway redeploy` to activate live trading** — the running "
            f"container still has `PAPER_TRADING=true` until restart."
        )
    else:
        action_value = (
            f"⚠️ Railway variable update FAILED: {railway_msg}\n"
            f"Flip `PAPER_TRADING=false` manually in the Railway dashboard, "
            f"then redeploy."
        )

    return {
        "title": "🚀 GOING LIVE — all 4 criteria passed!",
        "color": 0x9B59B6,
        "fields": [
            {"name": "🏆 Stats that triggered it",
             "value": (
                 f"n={n}, win rate {overall['win_rate']:.1%} "
                 f"({overall['wins']}W/{n - overall['wins']}L)\n"
                 f"PnL {_format_usd(overall['pnl'])}, "
                 f"Brier {cal['brier']:.3f}, ECE {cal['ece']:.1%}\n"
                 f"HIGH cal err {high_str}"
             ),
             "inline": False},
            {"name": "✅ Criteria", "value": crit_lines, "inline": False},
            {"name": "💵 Live bankroll", "value": f"${bankroll}", "inline": True},
            {"name": "📊 Consecutive passes",
             "value": f"{state.get('consecutive_pass_count', '?')}",
             "inline": True},
            {"name": "🕒 Triggered at",
             "value": state.get("live_flipped_at", now_str),
             "inline": True},
            {"name": "🎯 Action", "value": action_value, "inline": False},
        ],
        "footer": {"text": f"PassivePoly Auto-Live Trigger  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _check_auto_live(cal: dict[str, Any]) -> None:
    """Daily auto-go-live evaluator. Called after the calibration report.

    State machine (persisted at GO_LIVE_STATE_PATH):
      - If already live: no-op.
      - If cal has no trades in window: no-op (counter unchanged).
      - On a fail: counter resets to 0.
      - On a pass: counter += 1 if yesterday also passed, else counter = 1
        (a missed report breaks the streak).
      - When counter >= REQUIRED_CONSECUTIVE_PASSES and AUTO_LIVE_ENABLED:
        call Railway API to set PAPER_TRADING=false (skipDeploys=true), post
        the going-live Discord embed, send Pushover, mark state as live.

    AUTO_LIVE_ENABLED=false short-circuits the flip but still tracks the
    counter and logs `[AUTO-LIVE] would flip but disabled` so the user can
    see the threshold was met in the logs before arming the feature.
    """
    state = _load_go_live_state()
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    if state.get("is_live"):
        print("[AUTO-LIVE] already live, skipping", flush=True)
        return

    if state.get("last_report_date") == today:
        print(
            f"[AUTO-LIVE] already processed today ({today}), skipping",
            flush=True,
        )
        return

    if not cal.get("overall"):
        print(
            "[AUTO-LIVE] no trades in window — counter unchanged",
            flush=True,
        )
        state["last_report_date"] = today
        _save_go_live_state(state)
        return

    passes = sum(1 for _, p, _ in cal["criteria_checks"] if p)
    total = len(cal["criteria_checks"])
    all_pass = bool(cal.get("all_pass"))

    state.setdefault("history", []).append({
        "date": today,
        "all_pass": all_pass,
        "passes": passes,
        "total": total,
        "n": cal["n"],
        "win_rate": cal["overall"]["win_rate"],
        "pnl": cal["overall"]["pnl"],
        "brier": cal["brier"],
        "ece": cal["ece"],
    })
    state["history"] = state["history"][-60:]
    state["last_report_date"] = today

    if not all_pass:
        prior = state.get("consecutive_pass_count", 0)
        state["consecutive_pass_count"] = 0
        if prior:
            print(
                f"[AUTO-LIVE] criteria {passes}/{total} — counter reset "
                f"(was {prior})",
                flush=True,
            )
        else:
            print(
                f"[AUTO-LIVE] criteria {passes}/{total} — no streak",
                flush=True,
            )
        _save_go_live_state(state)
        return

    if state.get("last_pass_date") == yesterday:
        state["consecutive_pass_count"] = state.get("consecutive_pass_count", 0) + 1
    else:
        if state.get("last_pass_date") not in (None, yesterday):
            print(
                f"[AUTO-LIVE] gap in streak — last pass was "
                f"{state['last_pass_date']!r}, expected {yesterday!r}; "
                f"restarting counter at 1",
                flush=True,
            )
        state["consecutive_pass_count"] = 1
    state["last_pass_date"] = today

    count = state["consecutive_pass_count"]
    if count < REQUIRED_CONSECUTIVE_PASSES:
        print(
            f"[AUTO-LIVE] criteria {passes}/{total} pass "
            f"(consecutive={count}/{REQUIRED_CONSECUTIVE_PASSES}) — awaiting next",
            flush=True,
        )
        _save_go_live_state(state)
        return

    print(
        f"[AUTO-LIVE] criteria {passes}/{total} pass "
        f"(consecutive={count}) — THRESHOLD MET",
        flush=True,
    )

    if not AUTO_LIVE_ENABLED:
        print(
            "[AUTO-LIVE] KALSHI_AUTO_LIVE_ENABLED=false — would flip but disabled. "
            "Set the env var to true on Railway to arm.",
            flush=True,
        )
        _save_go_live_state(state)
        return

    railway_ok, railway_msg = _railway_set_variable("PAPER_TRADING", "false")
    if railway_ok:
        print(
            "[AUTO-LIVE] PAPER_TRADING=false set via Railway API "
            "(skipDeploys=true). Redeploy required to activate.",
            flush=True,
        )
        state["is_live"] = True
        state["live_flipped_at"] = now.isoformat()
        state["trigger_snapshot"] = {
            "n": cal["n"],
            "win_rate": cal["overall"]["win_rate"],
            "pnl": cal["overall"]["pnl"],
            "brier": cal["brier"],
            "ece": cal["ece"],
            "checks": cal["criteria_checks"],
        }
    else:
        print(
            f"[AUTO-LIVE] Railway API call FAILED: {railway_msg}",
            flush=True,
        )
    _save_go_live_state(state)

    try:
        send_discord(_build_going_live_embed(cal, state, railway_ok, railway_msg))
    except Exception as e:
        print(
            f"[WARN] going-live Discord post failed: {type(e).__name__}: {e}",
            flush=True,
        )

    pushover_title = (
        "🚀 KALSHI BOT — GOING LIVE" if railway_ok
        else "⚠️ KALSHI BOT — auto-live FAILED"
    )
    pushover_msg = (
        f"All 4 go-live criteria passed for {count} consecutive days.\n"
        f"n={cal['n']}, win rate {cal['overall']['win_rate']:.1%}, "
        f"PnL ${cal['overall']['pnl']:+.2f}\n\n"
        f"{'PAPER_TRADING set to false. Redeploy required to activate.' if railway_ok else f'Railway API failed: {railway_msg}. Flip manually.'}"
    )
    _send_pushover(pushover_title, pushover_msg)


def send_discord(embed: dict[str, Any]) -> None:
    if not WEBHOOK_KALSHI_WINRATE:
        print(embed)
        return
    try:
        r = requests.post(WEBHOOK_KALSHI_WINRATE, json={"embeds": [embed]}, timeout=10)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            requests.post(WEBHOOK_KALSHI_WINRATE, json={"embeds": [embed]}, timeout=10)
        elif r.status_code not in (200, 204):
            print(f"[WARN] Discord {r.status_code}: {r.text[:100]}")
        time.sleep(1.5)
    except Exception as e:
        print(f"[WARN] Discord send failed: {e}")


def _seconds_until_next_hour(target_hour: int) -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _seconds_until_next_weekly(target_dow: int, target_hour: int) -> float:
    """Seconds until the next occurrence of weekday `target_dow` at
    `target_hour` UTC. weekday: 0 = Monday, 6 = Sunday. If today is
    target_dow but we're past target_hour, schedules for next week."""
    now = datetime.now(timezone.utc)
    days_ahead = (target_dow - now.weekday()) % 7
    target = now.replace(
        hour=target_hour, minute=0, second=0, microsecond=0
    ) + timedelta(days=days_ahead)
    if target <= now:
        target += timedelta(days=7)
    return (target - now).total_seconds()


# ─── Weekly summary (2026-06-17) ──────────────────────────────────────
# Fires Mondays at 08:00 UTC on a separate daemon thread. Uses the same
# _compute_stats path as the daily, just with trades pre-filtered to a
# 7-day window. Posts to the same WEBHOOK_KALSHI_WINRATE channel — same
# audience, different cadence, distinct title so the embed is obvious.

def _filter_trades_to_last_week(
    trades: list[dict[str, Any]], now: datetime | None = None
) -> tuple[list[dict[str, Any]], datetime, datetime]:
    """Return (trades_in_window, week_start, week_end). Window is the
    7 days ending at `now`, by placement timestamp. We use placement
    rather than settlement so "this week's bot activity" includes
    trades placed Sunday that will only resolve Monday — those still
    represent decisions made in the window."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=7)
    cutoff_iso = cutoff.isoformat()
    in_window = [
        t for t in trades
        if (t.get("timestamp") or "") >= cutoff_iso
    ]
    return in_window, cutoff, now


def _top_series_by_pnl(
    by_series: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    """Pick the series with the highest PnL among decided trades. Returns
    None when the breakdown is empty. PnL wins over WR because high WR
    on a 1-trade series is noise; PnL is the dollar-weighted answer to
    'which series carried this week.' Returns the series stats with a
    `key` field added so the caller can render the name."""
    if not by_series:
        return None
    name, stats = max(
        by_series.items(),
        key=lambda kv: (
            float(kv[1].get("total_pnl", 0)),
            int(kv[1].get("decided", 0)),
        ),
    )
    return {**stats, "key": name}


def _build_weekly_embed(
    s: dict[str, Any],
    week_start: datetime,
    week_end: datetime,
    gl: dict[str, Any],
) -> dict[str, Any]:
    color = 0x2ECC71 if s["total_pnl"] >= 0 else 0xE74C3C
    week_label = (
        f"{week_start.strftime('%b %d')} → {week_end.strftime('%b %d')}"
    )

    def _trade_line(t: dict[str, Any] | None) -> str:
        if not t:
            return "—"
        title = (t.get("title") or t.get("ticker") or "?")[:60]
        side = (t.get("side") or "").upper()
        side_tag = f" [{side}]" if side in ("YES", "NO") else ""
        return f"{title}{side_tag} → {_format_usd(float(t.get('pnl', 0)))}"

    top_series = _top_series_by_pnl(s.get("by_series") or [])
    if top_series:
        ts_str = (
            f"**{top_series['key']}** — {top_series['decided']} trades, "
            f"{top_series['win_rate']:.0%} WR, "
            f"{_format_usd(top_series['total_pnl'])}"
        )
    else:
        ts_str = "—  (no decided trades this week)"

    # Go-live progress: status badge + most-recent passes/total.
    gl_passes = gl.get("passes", 0)
    gl_total = gl.get("total", 4)
    gl_streak = gl.get("consecutive_pass_count", 0)
    if gl.get("is_live"):
        gl_str = (
            f"✅ **LIVE** since {gl.get('last_report_date', '?')} — "
            f"latest day {gl_passes}/{gl_total} criteria"
        )
    else:
        gl_str = (
            f"📋 {gl_passes}/{gl_total} criteria passing · "
            f"consecutive-pass streak: {gl_streak}"
        )

    fields = [
        {"name": "📅 Window", "value": week_label, "inline": True},
        {"name": "🏆 Win Rate",
         "value": f"{s['win_rate']:.1%} ({s['wins']}W / {s['losses']}L)",
         "inline": True},
        {"name": "💰 PnL", "value": _format_usd(s["total_pnl"]), "inline": True},
        {"name": "📊 Decided", "value": str(s["decided"]), "inline": True},
        {"name": "⏳ Pending", "value": str(s["pending"]), "inline": True},
        {"name": "📈 ROI", "value": f"{s['roi']*100:+.1f}%", "inline": True},
        {"name": "🥇 Best Trade", "value": _trade_line(s.get("best")), "inline": False},
        {"name": "🥶 Worst Trade", "value": _trade_line(s.get("worst")), "inline": False},
        {"name": "🏅 Top Series (by PnL)", "value": ts_str, "inline": False},
        {"name": "🎯 Go-Live Progress", "value": gl_str, "inline": False},
        {"name": "📊 View Dashboard",
         "value": f"[Open live dashboard]({DASHBOARD_URL})", "inline": False},
    ]

    if s["decided"] == 0:
        fields.insert(1, {
            "name": "Status",
            "value": (
                "No decided trades in the 7-day window — either a quiet "
                "week or everything is still pending."
            ),
            "inline": False,
        })

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return {
        "title": f"📅 KALSHI WEEKLY SUMMARY — {week_label}",
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Kalshi Weekly  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _do_weekly_report() -> None:
    trades = _load_trades()
    in_window, week_start, week_end = _filter_trades_to_last_week(trades)
    stats = _compute_stats(in_window)
    try:
        gl_state = _load_go_live_state()
    except Exception as e:
        print(f"[WARN] weekly: go-live state load failed: {e}", flush=True)
        gl_state = {}
    history = gl_state.get("history") or []
    latest = history[-1] if history else {}
    gl_view = {
        "is_live": bool(gl_state.get("is_live")),
        "consecutive_pass_count": int(gl_state.get("consecutive_pass_count") or 0),
        "passes": int(latest.get("passes") or 0),
        "total": int(latest.get("total") or 4),
        "last_report_date": gl_state.get("last_report_date") or "—",
    }
    print(
        f"[weekly] window={week_start.date()}..{week_end.date()} "
        f"in_window={len(in_window)} decided={stats['decided']} "
        f"wr={stats['win_rate']:.1%} pnl={_format_usd(stats['total_pnl'])} "
        f"gl={gl_view['passes']}/{gl_view['total']}",
        flush=True,
    )
    send_discord(_build_weekly_embed(stats, week_start, week_end, gl_view))


def _run_weekly_loop() -> None:
    print(
        f"[weekly] Kalshi weekly summary armed — fires "
        f"weekday={WEEKLY_DOW} at {WEEKLY_HOUR:02d}:00 UTC",
        flush=True,
    )
    while True:
        wait_s = _seconds_until_next_weekly(WEEKLY_DOW, WEEKLY_HOUR)
        print(f"[weekly] next report in {wait_s/3600:.1f}h", flush=True)
        time.sleep(wait_s)
        try:
            _do_weekly_report()
        except Exception as e:
            print(f"[WARN] weekly cycle crashed: {e}", flush=True)
            time.sleep(60)


# ─── BUY_NO nightly watchdog (2026-06-17) ─────────────────────────────
# Posts a daily status to Discord answering: "Did any BUY_NO fire in
# the last 24h? If not, why might that be?" Designed for the first 30
# days of the BUY_NO staged rollout — silent non-firing is the failure
# mode we most want to catch (the prompt could be too restrictive, the
# eligibility code could be force-dropping every attempt, or the cohort
# could just be too narrow).

def _build_buy_no_watch_embed(
    trades_24h: list[dict[str, Any]],
    no_trades_24h: list[dict[str, Any]],
    pending_no: list[dict[str, Any]],
    lifetime_no_count: int,
) -> dict[str, Any]:
    n_24h = len(trades_24h)
    n_no_24h = len(no_trades_24h)
    n_yes_24h = sum(1 for t in trades_24h if t.get("side") == "yes")
    fired = n_no_24h > 0

    if fired:
        title = f"✅ KALSHI BUY_NO WATCH — {n_no_24h} fired in last 24h"
        color = 0xE67E22  # orange to match Discord BUY_NO embeds
    elif n_24h == 0:
        title = "⚠️ KALSHI BUY_NO WATCH — no trades at all in last 24h"
        color = 0x95A5A6  # muted grey
    else:
        title = "⚠️ KALSHI BUY_NO WATCH — no BUY_NO in last 24h"
        color = 0xF1C40F  # yellow

    fields = [
        {"name": "📊 Last 24h", "value": f"{n_24h} trades total", "inline": True},
        {"name": "🟢 YES bets", "value": str(n_yes_24h), "inline": True},
        {"name": "🟠 NO bets", "value": str(n_no_24h), "inline": True},
    ]

    if no_trades_24h:
        lines = []
        for t in no_trades_24h[:5]:
            ticker = t.get("ticker", "?")
            tail = ticker.rsplit("-", 1)[-1] if "-" in ticker else "?"
            our_p = t.get("our_prob")
            paid = t.get("market_price_cents")
            edge = t.get("edge")
            outcome = t.get("outcome", "?")
            our_str = f"{our_p:.2f}" if isinstance(our_p, (int, float)) else "?"
            edge_str = f"{edge*100:+.0f}%" if isinstance(edge, (int, float)) else "?"
            lines.append(
                f"`-{tail}` paid={paid}¢ our={our_str} edge={edge_str} → {outcome}"
            )
        fields.append({
            "name": "🟠 NO trades placed",
            "value": "\n".join(lines),
            "inline": False,
        })
    else:
        # Diagnostic: explain *what might be missing* when nothing fired
        diag = (
            "Either Claude didn't find a qualifying setup tonight, or the "
            "eligibility gate (KXMLBTOTAL `-9`/`-10` only, MEDIUM-only, "
            "both starters ERA < 3.50, projected_total below the line) "
            "blocked every attempt. Check Railway logs for "
            "`[BUY-NO-INELIGIBLE]` to distinguish — silence on both = no "
            "attempt; ineligible lines present = Claude tried + got dropped."
        )
        fields.append({"name": "🔍 Diagnostic", "value": diag, "inline": False})

    if pending_no:
        fields.append({
            "name": "⏳ Pending NO trades",
            "value": str(len(pending_no)),
            "inline": True,
        })

    fields.append({
        "name": "📚 Lifetime NO total",
        "value": str(lifetime_no_count),
        "inline": True,
    })

    fields.append({
        "name": "📊 View Dashboard",
        "value": f"[Open live dashboard]({DASHBOARD_URL})",
        "inline": False,
    })

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return {
        "title": title,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly BUY_NO Watch  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _do_buy_no_watch_report() -> None:
    trades = _load_trades()
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=24)).isoformat()
    trades_24h = [t for t in trades if (t.get("timestamp") or "") >= cutoff]
    no_trades_24h = [t for t in trades_24h if t.get("side") == "no"]
    pending_no = [
        t for t in trades
        if t.get("side") == "no" and t.get("outcome") == "pending"
    ]
    lifetime_no_count = sum(1 for t in trades if t.get("side") == "no")
    print(
        f"[buy_no_watch] 24h_total={len(trades_24h)} "
        f"24h_no={len(no_trades_24h)} pending_no={len(pending_no)} "
        f"lifetime_no={lifetime_no_count}",
        flush=True,
    )
    send_discord(_build_buy_no_watch_embed(
        trades_24h, no_trades_24h, pending_no, lifetime_no_count,
    ))


def _run_buy_no_watch_loop() -> None:
    print(
        f"[buy_no_watch] BUY_NO watchdog armed — fires daily at "
        f"{BUY_NO_WATCH_HOUR:02d}:00 UTC",
        flush=True,
    )
    while True:
        wait_s = _seconds_until_next_hour(BUY_NO_WATCH_HOUR)
        print(f"[buy_no_watch] next report in {wait_s/3600:.1f}h", flush=True)
        time.sleep(wait_s)
        try:
            _do_buy_no_watch_report()
        except Exception as e:
            print(f"[WARN] buy_no_watch cycle crashed: {e}", flush=True)
            time.sleep(60)


def _do_report() -> None:
    trades = _load_trades()
    stats = _compute_stats(trades)
    print(
        f"[winrate] decided={stats['decided']} wins={stats['wins']} "
        f"losses={stats['losses']} pnl={_format_usd(stats['total_pnl'])} "
        f"roi={stats['roi']*100:+.1f}% "
        f"series={len(stats['by_series'])} conf={len(stats['by_confidence'])}",
        flush=True,
    )
    try:
        _append_csv(stats)
    except Exception as e:
        print(f"[WARN] winrate CSV append failed: {e}", flush=True)
    send_discord(_build_embed(stats))

    # Calibration report (same channel, immediately after winrate).
    # `since_date` from go_live_criteria.json windows the analysis to post-fix
    # trades (default 2026-06-07 after the KALSHI_MAX_EDGE cap in 24ae529).
    try:
        from calibration import compute_calibration
        cal = compute_calibration()
        o = cal.get("overall")
        passes = sum(1 for _, p, _ in cal["criteria_checks"] if p)
        total = len(cal["criteria_checks"])
        if o:
            print(
                f"[winrate] calibration: n={cal['n']}/{cal['raw_n']} "
                f"win_rate={o['win_rate']:.1%} brier={cal['brier']:.3f} "
                f"ece={cal['ece']:.1%} pass={passes}/{total}",
                flush=True,
            )
        else:
            print(f"[winrate] calibration: n=0 in window", flush=True)
        send_discord(_build_calibration_embed(cal))
    except Exception as e:
        print(
            f"[WARN] calibration report failed: {type(e).__name__}: {e}",
            flush=True,
        )
        return

    # Auto-go-live evaluator. Walls itself off in try/except so a Railway API
    # blip or state-file issue can never break the rest of the daily report.
    try:
        _check_auto_live(cal)
    except Exception as e:
        print(
            f"[WARN] auto-live evaluator crashed: {type(e).__name__}: {e}",
            flush=True,
        )


def _should_catch_up_now() -> bool:
    """True if today's daily report hasn't fired yet AND we're past
    WINRATE_HOUR UTC. Without this, a launcher restart at HH:MM where
    HH > WINRATE_HOUR causes _seconds_until_next_hour() to return ~24h
    and silently skip a day of calibration. Reads
    go_live_state.last_report_date which _do_report → _check_auto_live
    sets to today's date on a successful run."""
    now = datetime.now(timezone.utc)
    if now.hour < WINRATE_HOUR:
        return False  # natural sleep-loop will catch the normal window
    today_str = now.strftime("%Y-%m-%d")
    try:
        state = _load_go_live_state()
        return state.get("last_report_date") != today_str
    except Exception:
        return True  # state unreadable → fire it, the loop will recover


def run() -> None:
    print(
        f"Kalshi Win-Rate Agent starting — daily at {WINRATE_HOUR:02d}:00 UTC, "
        f"weekly summary Mondays at {WEEKLY_HOUR:02d}:00 UTC, "
        f"BUY_NO watchdog daily at {BUY_NO_WATCH_HOUR:02d}:00 UTC"
    )
    # Weekly summary runs in its own daemon thread so launcher.py needs
    # no change. No catch-up on weekly — a missed Monday post is mildly
    # annoying but not data-losing (unlike the daily, which feeds
    # calibration and auto-go-live state).
    threading.Thread(
        target=_run_weekly_loop, name="kalshi-weekly", daemon=True
    ).start()
    # BUY_NO nightly watchdog. Same daemon-thread pattern, posts to the
    # same webhook with a distinct title. Designed for the first 30 days
    # of the BUY_NO staged rollout (deployed 2026-06-17) — surfaces
    # silent non-firing as well as actual fires.
    threading.Thread(
        target=_run_buy_no_watch_loop, name="kalshi-buy-no-watch", daemon=True
    ).start()
    # Catch-up fire on startup. Without this, a deploy/restart anywhere in
    # the 07:01-23:59 UTC window puts the sleep loop on tomorrow's clock
    # and silently skips today's calibration report — exactly what happened
    # on 2026-06-14 when three back-to-back tennis-related deploys all
    # landed in that window.
    try:
        if _should_catch_up_now():
            state = _load_go_live_state()
            print(
                f"[winrate] startup catch-up: last_report_date="
                f"{state.get('last_report_date')!r} — firing today's report now",
                flush=True,
            )
            _do_report()
    except Exception as e:
        print(
            f"[WARN] winrate startup catch-up crashed: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )
    while True:
        wait_s = _seconds_until_next_hour(WINRATE_HOUR)
        print(f"[winrate] next report in {wait_s/3600:.1f}h", flush=True)
        time.sleep(wait_s)
        try:
            _do_report()
        except Exception as e:
            print(f"[WARN] winrate cycle crashed: {e}", flush=True)
            time.sleep(60)


if __name__ == "__main__":
    run()
