#!/usr/bin/env python3
"""Read trades_log.json and print live per-series + per-confidence
calibration. Surfaces which series the model predicts most accurately,
which ones are overconfident, and whether the recent window is trending
better or worse than the prior one.

Math definitions:
  win_rate    : settled-trade win share (1 = always wins, 0 = always loses)
  mean_pred   : average of our_prob (the model's claimed YES probability
                at trade placement)
  cal_err_pp  : (mean_pred − win_rate) × 100, in percentage points.
                Positive = overconfident; the model overstates the true
                probability of YES. Negative = underconfident.
  brier       : mean of (our_prob − outcome)^2, outcome ∈ {0, 1}. Lower
                is better. 0.25 = random guessing at 50%, 0.0 = oracle.

Run from anywhere with Python; defaults read /app/data/trades_log.json
(works under `railway ssh`). Pass --trades-log <path> for any other file.

Run
    railway ssh ... 'python3 /app/scripts/calibration_live.py'
    python3 scripts/calibration_live.py --trades-log /tmp/trades.json
    python3 scripts/calibration_live.py --recent-n 10
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


SERIES_PREFIXES = (
    "KXMLBGAME", "KXMLBTOTAL", "KXMLBSPREAD", "KXMLBTEAMTOTAL",
    "KXATPMATCH", "KXWTAMATCH", "KXBTC", "KXAAAGASD",
    "KXNHL", "KXNBA",
)


def series_of(ticker: str) -> str:
    for p in SERIES_PREFIXES:
        if ticker.startswith(p):
            return p
    return "other"


def stats(rows: list[dict]) -> dict | None:
    """Per-bucket calibration. None when no rows have a usable our_prob
    + settled outcome (incomplete records are silently skipped)."""
    usable = [
        t for t in rows
        if isinstance(t.get("our_prob"), (int, float))
        and t.get("outcome") in ("won", "lost")
    ]
    if not usable:
        return None
    n = len(usable)
    wins = sum(1 for t in usable if t["outcome"] == "won")
    preds = [float(t["our_prob"]) for t in usable]
    mean_pred = sum(preds) / n
    win_rate = wins / n
    brier = sum(
        (float(t["our_prob"]) - (1.0 if t["outcome"] == "won" else 0.0)) ** 2
        for t in usable
    ) / n
    pnl = sum(float(t.get("pnl") or 0) for t in usable)
    return {
        "n": n,
        "wins": wins,
        "win_rate": win_rate,
        "mean_pred": mean_pred,
        "cal_err": mean_pred - win_rate,
        "brier": brier,
        "pnl": pnl,
    }


def print_table(label: str, rows_by_key: list[tuple[str, dict]]) -> None:
    print(f"\n=== {label} ===")
    print(
        f"  {'key':<18} {'n':>3} {'wr':>5} {'pred':>5} {'cal_err':>8} "
        f"{'brier':>6} {'pnl':>6}"
    )
    for key, st in rows_by_key:
        print(
            f"  {key:<18} {st['n']:>3} {st['win_rate']:>4.0%} "
            f"{st['mean_pred']:>4.0%} {st['cal_err']*100:>+7.1f}pp "
            f"{st['brier']:>5.3f}  ${st['pnl']:>+5.0f}"
        )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--trades-log",
        default="/app/data/trades_log.json",
        help="Path to trades_log.json (default: /app/data/trades_log.json)",
    )
    p.add_argument(
        "--recent-n", type=int, default=10,
        help="Window size for the trend comparison (default 10)",
    )
    args = p.parse_args()

    path = Path(args.trades_log)
    if not path.exists():
        print(f"[ERROR] trades log not found: {path}", file=sys.stderr)
        return 1

    with path.open() as f:
        data = json.load(f)
    trades = data if isinstance(data, list) else data.get("trades", [])
    settled = [t for t in trades if t.get("outcome") in ("won", "lost")]

    print(
        f"CALIBRATION LIVE — {datetime.now(timezone.utc).isoformat()}\n"
        f"trades_log: {path}\n"
        f"total trades in log: {len(trades)}\n"
        f"settled: {len(settled)}\n"
        f"window for trend: last {args.recent_n} vs prior {args.recent_n}"
    )

    if not settled:
        print("\nNo settled trades yet — run after first settlement sweep.")
        return 0

    overall = stats(settled)
    if overall:
        print(
            f"\nOVERALL  n={overall['n']}  wr={overall['win_rate']:.0%}  "
            f"mean_pred={overall['mean_pred']:.0%}  "
            f"cal_err={overall['cal_err']*100:+.1f}pp  "
            f"brier={overall['brier']:.3f}  pnl=${overall['pnl']:+.0f}"
        )

    # By series — sorted by best calibration (lowest |cal_err|) first
    by_series: dict[str, list[dict]] = defaultdict(list)
    for t in settled:
        by_series[series_of(t.get("ticker", ""))].append(t)
    series_rows = [
        (s, stats(rs)) for s, rs in by_series.items() if stats(rs) is not None
    ]
    series_rows.sort(key=lambda x: abs(x[1]["cal_err"]))
    print_table("BY SERIES (sorted: best-calibrated first)", series_rows)

    if series_rows:
        best = series_rows[0]
        worst = series_rows[-1]
        print(
            f"\n  MOST calibrated:  {best[0]} "
            f"(cal_err {best[1]['cal_err']*100:+.1f}pp, "
            f"brier {best[1]['brier']:.3f}, n={best[1]['n']})"
        )
        print(
            f"  LEAST calibrated: {worst[0]} "
            f"(cal_err {worst[1]['cal_err']*100:+.1f}pp, "
            f"brier {worst[1]['brier']:.3f}, n={worst[1]['n']})"
        )

    # By confidence
    by_conf: dict[str, list[dict]] = defaultdict(list)
    for t in settled:
        by_conf[t.get("confidence", "?") or "?"].append(t)
    conf_rows = []
    for c in ("HIGH", "MEDIUM", "LOW", "?"):
        st = stats(by_conf.get(c, []))
        if st:
            conf_rows.append((c, st))
    print_table("BY CONFIDENCE", conf_rows)

    # Trend: most recent N vs preceding N
    print(f"\n=== TREND (last {args.recent_n} vs prior {args.recent_n}) ===")
    if len(settled) < args.recent_n * 2:
        print(
            f"  (need ≥{args.recent_n * 2} settled trades; "
            f"have {len(settled)})"
        )
    else:
        recent = settled[-args.recent_n:]
        prior = settled[-args.recent_n * 2: -args.recent_n]
        r = stats(recent)
        p_ = stats(prior)
        if r and p_:
            print(
                f"  prior  n={p_['n']}  brier={p_['brier']:.3f}  "
                f"cal_err={p_['cal_err']*100:+.1f}pp  wr={p_['win_rate']:.0%}"
            )
            print(
                f"  recent n={r['n']}  brier={r['brier']:.3f}  "
                f"cal_err={r['cal_err']*100:+.1f}pp  wr={r['win_rate']:.0%}"
            )
            d_brier = r["brier"] - p_["brier"]
            d_cal = abs(r["cal_err"]) - abs(p_["cal_err"])

            def dirword(delta: float, lower_better: bool) -> str:
                if abs(delta) < 1e-6:
                    return "flat"
                if (delta < 0) == lower_better:
                    return "BETTER"
                return "WORSE"

            print(
                f"  Δbrier   = {d_brier:+.3f}  → {dirword(d_brier, True)}"
            )
            print(
                f"  Δ|cal_err| = {d_cal*100:+.1f}pp  → "
                f"{dirword(d_cal, True)}"
            )
            if d_brier < 0 and d_cal < 0:
                print("  Verdict: calibration is IMPROVING.")
            elif d_brier > 0 and d_cal > 0:
                print("  Verdict: calibration is DEGRADING.")
            else:
                print("  Verdict: mixed — one metric better, one worse.")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
