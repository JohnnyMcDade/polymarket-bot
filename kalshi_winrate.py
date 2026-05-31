"""Kalshi win-rate reporter.

Reads trades_log.json once a day at KALSHI_WINRATE_HOUR UTC, computes
the performance numbers everyone actually cares about, and ships them
as one Discord embed:

  - Win rate %, decided trades only (pending excluded)
  - Total PnL in dollars
  - ROI % vs total stake deployed
  - Best + worst single trade
  - Avg edge on winning trades vs losing trades

Zero Claude calls — pure pandas-light math on the JSON log.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests

WEBHOOK_KALSHI_WINRATE = os.getenv("WEBHOOK_KALSHI_WINRATE", "")
WINRATE_HOUR = int(os.getenv("KALSHI_WINRATE_HOUR", "7"))
TRADES_LOG_PATH = Path(os.getenv("KALSHI_TRADES_LOG", "trades_log.json"))


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

    best = max(decided, key=lambda t: float(t.get("pnl", 0)), default=None)
    worst = min(decided, key=lambda t: float(t.get("pnl", 0)), default=None)

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
        "best": best,
        "worst": worst,
    }


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
        f"roi={stats['roi']*100:+.1f}%",
        flush=True,
    )
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
