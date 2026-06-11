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


def load_trades(path: str | None = None) -> list[dict[str, Any]]:
    with open(path or TRADES_LOG) as f:
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


def compute_calibration(
    trades_log_path: str = TRADES_LOG,
    criteria_path: str = CRITERIA,
    cli_since: str | None = None,
) -> dict[str, Any]:
    """Compute the full calibration report as a structured dict.

    Reads trades_log_path, applies the --since window (CLI flag wins over
    `since_date` in the criteria file), and returns:
      raw_n, n, since_iso, since_src, overall, conf, buckets, series,
      brier, log_loss, ece, criteria (parsed JSON), criteria_checks, all_pass,
      trades (the filtered list, for callers that want their own pass).
    """
    trades_log_path = str(trades_log_path)
    criteria_path = str(criteria_path)

    raw_trades = load_trades(trades_log_path)
    raw_n = len(raw_trades)

    since_iso, since_src = resolve_since(cli_since, criteria_path)
    trades = filter_since(raw_trades, since_iso)
    n = len(trades)

    by_conf: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        by_conf[str(t.get("confidence") or "?")].append(t)
    conf_summaries: dict[str, dict[str, Any]] = {}
    for conf in ("HIGH", "MEDIUM", "LOW"):
        s = summarize(conf, by_conf.get(conf, []))
        if s:
            conf_summaries[conf] = s

    by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        by_bucket[bucket_for(float(t["our_prob"]))].append(t)
    bucket_summaries: dict[str, dict[str, Any]] = {}
    for b in ("<0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-0.9", "0.9-1.0"):
        s = summarize(b, by_bucket.get(b, []))
        if s:
            bucket_summaries[b] = s

    by_series: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        by_series[series_of(t.get("ticker", ""))].append(t)
    series_summaries: dict[str, dict[str, Any]] = {}
    for ser, items in by_series.items():
        s = summarize(ser, items)
        if s:
            series_summaries[ser] = s

    if n:
        wins = sum(1 for t in trades if t["outcome"] == "won")
        mean_pred = sum(float(t["our_prob"]) for t in trades) / n
        total_pnl = sum(float(t["pnl"]) for t in trades)
        overall = {
            "label": "overall",
            "n": n,
            "wins": wins,
            "win_rate": wins / n,
            "mean_pred": mean_pred,
            "cal_err": abs(mean_pred - wins / n),
            "pnl": total_pnl,
        }
    else:
        overall = None

    crit_doc: dict[str, Any] = {}
    criteria_checks: list[tuple[str, bool, str]] = []
    all_pass = False
    crit_path = Path(criteria_path)
    if crit_path.exists():
        try:
            with open(crit_path) as f:
                crit_doc = json.load(f)
        except Exception as e:
            print(f"[WARN] criteria file unreadable: {e}", flush=True)
            crit_doc = {}
        if overall and crit_doc:
            c = crit_doc.get("criteria", {})
            min_n_req = int(c.get("min_settled_trades", 50))
            min_wr = float(c.get("min_win_rate", 0.55))
            req_pnl = bool(c.get("require_pnl_positive", True))
            high_cap = float(c.get("max_high_conf_cal_err", 0.15))
            min_high_n = int(c.get("min_high_conf_n", 10))
            high = conf_summaries.get("HIGH")

            # HIGH calibration criterion requires BOTH: enough HIGH trades to
            # be statistically meaningful (n >= min_high_n), AND cal_err under
            # high_cap. Without the n-floor a single lucky 99% HIGH trade
            # could trip the criterion at n=1.
            if high and high["n"] >= min_high_n:
                high_pass = high["cal_err"] < high_cap
                high_actual = f"have {high['cal_err']:.1%} on n={high['n']}"
            elif high:
                high_pass = False
                high_actual = (
                    f"have n={high['n']}, need n>={min_high_n} "
                    f"({high['cal_err']:.1%} cal err)"
                )
            else:
                high_pass = False
                high_actual = f"no HIGH trades yet (need n>={min_high_n})"

            criteria_checks = [
                (f"Settled trades >= {min_n_req}",
                 overall["n"] >= min_n_req,
                 f"have {overall['n']}"),
                (f"Win rate >= {min_wr:.0%}",
                 overall["win_rate"] >= min_wr,
                 f"have {overall['win_rate']:.1%}"),
                ("PnL positive" if req_pnl else "(PnL not required)",
                 (overall["pnl"] > 0) if req_pnl else True,
                 f"have ${overall['pnl']:+.2f}"),
                (f"HIGH cal err < {high_cap:.0%} on n>={min_high_n}",
                 high_pass,
                 high_actual),
            ]
            all_pass = all(p for _, p, _ in criteria_checks)

    return {
        "trades_log_path": trades_log_path,
        "criteria_path": criteria_path,
        "raw_n": raw_n,
        "n": n,
        "since_iso": since_iso,
        "since_src": since_src,
        "trades": trades,
        "overall": overall,
        "conf": conf_summaries,
        "buckets": bucket_summaries,
        "series": series_summaries,
        "brier": brier(trades) if n else float("nan"),
        "log_loss": log_loss(trades) if n else float("nan"),
        "ece": ece(bucket_summaries) if n else 0.0,
        "criteria": crit_doc,
        "criteria_checks": criteria_checks,
        "all_pass": all_pass,
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


def _print_report(r: dict[str, Any]) -> None:
    print(f"\n=== Kalshi Bot Calibration Analysis ===")
    print(f"Source : {r['trades_log_path']}")
    if r["since_iso"]:
        print(f"Window : trades since {r['since_iso']} ({r['since_src']})")
        print(f"Filtered: {r['n']} of {r['raw_n']} settled trades")
    else:
        print(f"Window : all settled trades")
        print(f"Settled trades : {r['n']}")
    if r["n"] == 0:
        print("No settled trades in window.")
        return

    print("\n--- By confidence tier ---")
    rows = [fmt(r["conf"][k], "tier") for k in ("HIGH", "MEDIUM", "LOW") if k in r["conf"]]
    render_table(rows, ["tier", "n", "wins", "win_rate", "mean_pred", "cal_err", "pnl"])

    print("\n--- By predicted probability bucket ---")
    bucket_order = ["<0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-0.9", "0.9-1.0"]
    rows = [fmt(r["buckets"][b], "bucket") for b in bucket_order if b in r["buckets"]]
    render_table(rows, ["bucket", "n", "wins", "win_rate", "mean_pred", "cal_err", "pnl"])
    print(f"\nECE (overall) : {r['ece']:.1%}")

    print("\n--- By series ---")
    rows = [fmt(r["series"][k], "series") for k in sorted(r["series"].keys())]
    render_table(rows, ["series", "n", "wins", "win_rate", "mean_pred", "cal_err", "pnl"])

    o = r["overall"]
    print("\n--- Scoring metrics ---")
    print(f"Overall    : n={o['n']}  wins={o['wins']}  win_rate={o['win_rate']:.1%}  "
          f"mean_pred={o['mean_pred']:.1%}  cal_err={o['cal_err']:.1%}")
    print(f"Brier score: {r['brier']:.4f}   (0.25 = always-guess-50%, lower is better)")
    print(f"Log loss   : {r['log_loss']:.4f}")
    print(f"Total PnL  : ${o['pnl']:+.2f}")

    print("\n--- Pre-registered go-live criteria ---")
    if not r["criteria"]:
        print(f"(no criteria file at {r['criteria_path']})")
        return
    crit_doc = r["criteria"]
    print(f"Registered  : {crit_doc.get('registered_at_utc') or crit_doc.get('registered_at') or '?'}")
    print(f"Live bankroll on pass: ${crit_doc.get('live_bankroll_usd', '?')}")
    print(f"Criteria    : {json.dumps(crit_doc.get('criteria', {}))}")
    print()
    for label, passed, actual in r["criteria_checks"]:
        mark = "PASS" if passed else "FAIL"
        print(f"  [{mark}] {label} -- {actual}")
    print(f"\nVERDICT: {'GO LIVE' if r['all_pass'] else 'STAY PAPER'}")


def main() -> int:
    args = parse_args()
    r = compute_calibration(cli_since=args.since)
    _print_report(r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
