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
