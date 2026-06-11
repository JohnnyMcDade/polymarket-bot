#!/usr/bin/env python3
"""Calibration analysis for the Kalshi edge agent.

Reads the trades log and reports:
- Per-confidence-tier calibration (HIGH/MEDIUM/LOW)
- Per-probability-bucket calibration
- Per-series calibration (KXBTC, KXMLBGAME, KXMLBTOTAL, etc.)
- Brier score, log loss, Expected Calibration Error (ECE)
- Pre-registered go-live criteria check

Paths default to the container layout (/app/data/...) and can be overridden via
KALSHI_TRADES_LOG / KALSHI_GO_LIVE_CRITERIA env vars.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

TRADES_LOG = os.getenv("KALSHI_TRADES_LOG", "/app/data/trades_log.json")
CRITERIA = os.getenv("KALSHI_GO_LIVE_CRITERIA", "/app/data/go_live_criteria.json")


def load_trades() -> list[dict[str, Any]]:
    with open(TRADES_LOG) as f:
        raw = json.load(f)
    return [t for t in raw if t.get("outcome") in ("won", "lost")]


def _parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def filter_since(trades: list[dict[str, Any]], since_iso: str | None) -> list[dict[str, Any]]:
    """Keep trades whose `timestamp` is >= `since_iso` (UTC).

    Accepts a bare date (YYYY-MM-DD, interpreted as 00:00 UTC) or a full ISO
    timestamp. Trades with no/unparseable timestamp are dropped.
    """
    if not since_iso:
        return trades
    iso = since_iso if "T" in since_iso else f"{since_iso}T00:00:00+00:00"
    try:
        cutoff = _parse_iso(iso)
    except ValueError as e:
        raise SystemExit(f"--since: bad ISO value {since_iso!r}: {e}")
    out = []
    for t in trades:
        ts = t.get("timestamp")
        if not ts:
            continue
        try:
            if _parse_iso(str(ts)) >= cutoff:
                out.append(t)
        except ValueError:
            continue
    return out


def resolve_since(cli_since: str | None, criteria_path: str) -> tuple[str | None, str]:
    """Returns (since_iso, source_label). CLI flag wins over criteria file."""
    if cli_since:
        return cli_since, "CLI --since"
    p = Path(criteria_path)
    if p.exists():
        try:
            with open(p) as f:
                doc = json.load(f)
        except Exception:
            return None, ""
        sd = doc.get("since_date")
        if sd:
            return sd, f"{criteria_path}:since_date"
    return None, ""


def series_of(ticker: str) -> str:
    m = re.match(r"^([A-Z]+)-", ticker or "")
    return m.group(1) if m else "OTHER"


def bucket_for(p: float) -> str:
    if p < 0.5: return "<0.5"
    if p < 0.6: return "0.5-0.6"
    if p < 0.7: return "0.6-0.7"
    if p < 0.8: return "0.7-0.8"
    if p < 0.9: return "0.8-0.9"
    return "0.9-1.0"


def summarize(label: str, items: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not items:
        return None
    n = len(items)
    wins = sum(1 for t in items if t["outcome"] == "won")
    mean_pred = sum(float(t["our_prob"]) for t in items) / n
    actual = wins / n
    return {
        "label": label,
        "n": n,
        "wins": wins,
        "win_rate": actual,
        "mean_pred": mean_pred,
        "cal_err": abs(mean_pred - actual),
        "pnl": sum(float(t["pnl"]) for t in items),
    }


def brier(items: list[dict[str, Any]]) -> float:
    n = len(items)
    if n == 0:
        return float("nan")
    return sum(
        (float(t["our_prob"]) - (1.0 if t["outcome"] == "won" else 0.0)) ** 2
        for t in items
    ) / n


def log_loss(items: list[dict[str, Any]], eps: float = 1e-9) -> float:
    n = len(items)
    if n == 0:
        return float("nan")
    total = 0.0
    for t in items:
        p = min(1.0 - eps, max(eps, float(t["our_prob"])))
        y = 1.0 if t["outcome"] == "won" else 0.0
        total += y * math.log(p) + (1.0 - y) * math.log(1.0 - p)
    return -total / n


def ece(bucket_summaries: dict[str, dict[str, Any]]) -> float:
    n_total = sum(b["n"] for b in bucket_summaries.values())
    if n_total == 0:
        return 0.0
    return sum((b["n"] / n_total) * b["cal_err"] for b in bucket_summaries.values())


def render_table(rows: list[dict[str, Any]], cols: list[str]) -> None:
    widths = {
        c: max(len(c), max((len(str(r.get(c, ""))) for r in rows), default=0))
        for c in cols
    }
    print(" | ".join(c.ljust(widths[c]) for c in cols))
    print("-+-".join("-" * widths[c] for c in cols))
    for r in rows:
        print(" | ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))


def fmt(s: dict[str, Any], label_key: str) -> dict[str, Any]:
    return {
        label_key: s["label"],
        "n": s["n"],
        "wins": s["wins"],
        "win_rate": f"{s['win_rate']:.1%}",
        "mean_pred": f"{s['mean_pred']:.1%}",
        "cal_err": f"{s['cal_err']:.1%}",
        "pnl": f"${s['pnl']:+.2f}",
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--since",
        default=None,
        help="ISO date (YYYY-MM-DD) or ISO timestamp. Filters trades to those "
             "with timestamp >= cutoff (UTC). Overrides `since_date` in the "
             "criteria file.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    raw_trades = load_trades()
    raw_n = len(raw_trades)
    since_iso, since_src = resolve_since(args.since, CRITERIA)
    trades = filter_since(raw_trades, since_iso)
    n = len(trades)
    print(f"\n=== Kalshi Bot Calibration Analysis ===")
    print(f"Source : {TRADES_LOG}")
    if since_iso:
        print(f"Window : trades since {since_iso} ({since_src})")
        print(f"Filtered: {n} of {raw_n} settled trades")
    else:
        print(f"Window : all settled trades")
        print(f"Settled trades : {n}")
    if n == 0:
        print("No settled trades in window.")
        return 0

    # By confidence tier
    print("\n--- By confidence tier ---")
    by_conf: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        by_conf[str(t.get("confidence") or "?")].append(t)
    rows = []
    conf_summaries: dict[str, dict[str, Any]] = {}
    for conf in ("HIGH", "MEDIUM", "LOW"):
        s = summarize(conf, by_conf.get(conf, []))
        if s:
            rows.append(fmt(s, "tier"))
            conf_summaries[conf] = s
    render_table(rows, ["tier", "n", "wins", "win_rate", "mean_pred", "cal_err", "pnl"])

    # By probability bucket
    print("\n--- By predicted probability bucket ---")
    by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        by_bucket[bucket_for(float(t["our_prob"]))].append(t)
    rows = []
    bucket_summaries: dict[str, dict[str, Any]] = {}
    for b in ("<0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-0.9", "0.9-1.0"):
        s = summarize(b, by_bucket.get(b, []))
        if s:
            rows.append(fmt(s, "bucket"))
            bucket_summaries[b] = s
    render_table(rows, ["bucket", "n", "wins", "win_rate", "mean_pred", "cal_err", "pnl"])
    print(f"\nECE (overall) : {ece(bucket_summaries):.1%}")

    # By series
    print("\n--- By series ---")
    by_series: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        by_series[series_of(t.get("ticker", ""))].append(t)
    rows = []
    for ser in sorted(by_series.keys()):
        s = summarize(ser, by_series[ser])
        if s:
            rows.append(fmt(s, "series"))
    render_table(rows, ["series", "n", "wins", "win_rate", "mean_pred", "cal_err", "pnl"])

    # Overall scoring
    print("\n--- Scoring metrics ---")
    wins = sum(1 for t in trades if t["outcome"] == "won")
    mean_pred = sum(float(t["our_prob"]) for t in trades) / n
    print(f"Overall    : n={n}  wins={wins}  win_rate={wins/n:.1%}  mean_pred={mean_pred:.1%}  cal_err={abs(mean_pred - wins/n):.1%}")
    print(f"Brier score: {brier(trades):.4f}   (0.25 = always-guess-50%, lower is better)")
    print(f"Log loss   : {log_loss(trades):.4f}")
    print(f"Total PnL  : ${sum(float(t['pnl']) for t in trades):+.2f}")

    # Go-live criteria
    print("\n--- Pre-registered go-live criteria ---")
    crit_path = Path(CRITERIA)
    if not crit_path.exists():
        print(f"(no criteria file at {CRITERIA})")
        return 0
    with open(crit_path) as f:
        crit_doc = json.load(f)
    c = crit_doc.get("criteria", {})
    print(f"Registered  : {crit_doc.get('registered_at_utc') or crit_doc.get('registered_at') or '?'}")
    print(f"Live bankroll on pass: ${crit_doc.get('live_bankroll_usd', '?')}")
    print(f"Criteria    : {json.dumps(c)}")
    print()

    min_n = int(c.get("min_settled_trades", 50))
    min_wr = float(c.get("min_win_rate", 0.55))
    req_pnl = bool(c.get("require_pnl_positive", True))
    high_cap = float(c.get("max_high_conf_cal_err", 0.15))

    overall_wr = wins / n
    overall_pnl = sum(float(t["pnl"]) for t in trades)
    high = conf_summaries.get("HIGH")
    high_err = high["cal_err"] if high else None

    checks: list[tuple[str, bool, str]] = [
        (f"1. Settled trades >= {min_n}",
         n >= min_n,
         f"have {n}"),
        (f"2. Win rate >= {min_wr:.0%}",
         overall_wr >= min_wr,
         f"have {overall_wr:.1%}"),
        (f"3. PnL positive" if req_pnl else "3. (PnL not required)",
         (overall_pnl > 0) if req_pnl else True,
         f"have ${overall_pnl:+.2f}"),
        (f"4. HIGH cal err < {high_cap:.0%}",
         high_err is not None and high_err < high_cap,
         f"have {high_err:.1%} on n={high['n']}" if high else "no HIGH trades yet"),
    ]
    for label, passed, actual in checks:
        mark = "PASS" if passed else "FAIL"
        print(f"  [{mark}] {label} -- {actual}")
    all_pass = all(p for _, p, _ in checks)
    print(f"\nVERDICT: {'GO LIVE' if all_pass else 'STAY PAPER'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
