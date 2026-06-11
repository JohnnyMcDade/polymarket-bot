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
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable

import requests

WEBHOOK_KALSHI_WINRATE = os.getenv("WEBHOOK_KALSHI_WINRATE", "")
WINRATE_HOUR = int(os.getenv("KALSHI_WINRATE_HOUR", "7"))
TRADES_LOG_PATH = Path(os.getenv("KALSHI_TRADES_LOG", "/app/data/trades_log.json"))
CSV_HISTORY_PATH = Path(os.getenv("KALSHI_WINRATE_CSV", "/app/data/winrate_history.csv"))
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
    ]

    return {
        "title": f"📐 KALSHI CALIBRATION — daily{title_suffix}",
        "color": color,
        "fields": fields,
        "footer": {"text": f"PassivePoly Kalshi Calibration  •  {now_str}"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


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


def run() -> None:
    print(f"Kalshi Win-Rate Agent starting — fires daily at {WINRATE_HOUR:02d}:00 UTC")
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
