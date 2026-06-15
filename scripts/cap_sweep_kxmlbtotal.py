#!/usr/bin/env python3
"""KXMLBTOTAL per-night cap sweep.

ANSWERS:
  - At line=8.5 / δ=0.75 (the 60-day optimal cell) restricted to the
    elite + good starter-avg cohort, what's the win rate if we cap at
    1 / 2 / 3 / 5 / unlimited bets per night?
  - Is the production 2/night cap the right number? Should it move?

WHAT IT IS NOT:
  - A test of the 3¢ line-move guard. Backtest data has no historical
    Kalshi prices; we can't replay what a sharp move would have
    triggered. The line-move guard is a market-microstructure filter,
    not a model-edge filter, so it can only be validated against live
    Kalshi data after the fact.

METHODOLOGY:
  1. Reuse backtest_kalshi.run_total_backtest() to compute per-game
     predictions over the last `--days` calendar days (default 60).
  2. Apply the backtest-validated cohort filter:
       starter_avg_era ≤ BACKTEST_FILTER_ERA_CAP (3.50)
     This mirrors production's _backtest_cohort_passes() logic.
  3. Apply the line/δ filter (default line=8.5, δ=0.75 — the highest-
     lift cell in the 60-day backtest).
  4. Group qualifying bets by game_date (proxy for "night").
  5. Sweep cap N ∈ {1, 2, 3, 5, ∞}. For each N, take the FIRST N bets
     per night (matches the production trader's FIFO behavior — first
     to clear the queue gets sized first). Compute aggregate win rate
     and Wilson 95% lower bound across all nights.

Run:
    python3 scripts/cap_sweep_kxmlbtotal.py
    python3 scripts/cap_sweep_kxmlbtotal.py --days 90 --line 9.5 --delta 0.5
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import backtest_kalshi as bk

BACKTEST_FILTER_ERA_CAP = 3.50  # mirrors kalshi_edge.BACKTEST_FILTER_ERA_CAP
CAPS = (1, 2, 3, 5, 10_000)  # 10_000 = "no cap"


def _wilson_lower(c: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = c / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return (centre - margin) / denom


def _wilson_upper(c: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 1.0
    p = c / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return (centre + margin) / denom


def _evaluate_bet(predicted: float, actual: float, line: float, delta: float) -> int | None:
    """Mirrors backtest_kalshi._sweep_at_line single-bet logic. Returns
    1 if we'd win the trade, 0 if we'd lose, None if we'd skip (no
    edge or ties)."""
    if actual == line:
        return None
    if predicted >= line + delta:
        bet_over = True
    elif predicted <= line - delta:
        bet_over = False
    else:
        return None  # SKIP
    actual_over = actual > line
    return 1 if bet_over == actual_over else 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--days", type=int, default=60)
    p.add_argument("--season", type=int, default=datetime.now(timezone.utc).year)
    p.add_argument("--line", type=float, default=8.5,
                   help="OVER/UNDER threshold (default 8.5; 60-day optimal cell)")
    p.add_argument("--delta", type=float, default=0.75,
                   help="Edge buffer (default 0.75)")
    p.add_argument(
        "--no-cohort-filter", action="store_true",
        help="Skip the elite/good cohort filter — test cap on raw predictor",
    )
    args = p.parse_args()

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=args.days)
    print(f"Backtest window: {start} → {end}  ({args.days} days)")
    print(f"Threshold: line={args.line}  δ={args.delta}")
    print(
        f"Cohort filter: "
        f"{'OFF (raw predictor)' if args.no_cohort_filter else f'starter_avg_era ≤ {BACKTEST_FILTER_ERA_CAP}'}"
    )

    print("\nFetching schedule + pitcher gamelogs (cached)...")
    games = bk.fetch_schedule(start.isoformat(), end.isoformat())
    print(f"  {len(games)} finished games loaded")
    pitcher_ids = {pid for g in games
                   for pid in (g.home_pitcher_id, g.away_pitcher_id)
                   if pid}
    gamelogs_by_pid = {}
    for pid in sorted(pitcher_ids):
        gamelogs_by_pid[pid] = bk.fetch_pitcher_gamelog(pid, args.season)
    print(f"  {len(gamelogs_by_pid)} pitcher gamelogs ready")

    print("\nRunning total backtest...")
    rows, skipped = bk.run_total_backtest(games, gamelogs_by_pid)
    print(f"  {len(rows)} rows  skipped: {skipped}")

    # Cohort filter
    if args.no_cohort_filter:
        cohort_rows = rows
    else:
        cohort_rows = [
            r for r in rows
            if r.get("starter_avg_era") is not None
            and r["starter_avg_era"] <= BACKTEST_FILTER_ERA_CAP
        ]
    print(f"  rows in cohort: {len(cohort_rows)}")

    # Evaluate each row at the chosen (line, delta) cell. Drop SKIPs.
    bets = []
    for r in cohort_rows:
        outcome = _evaluate_bet(
            r["predicted_total"], r["actual_total"], args.line, args.delta
        )
        if outcome is None:
            continue
        bets.append({
            "date": r["date"],
            "outcome": outcome,
            "predicted_total": r["predicted_total"],
            "actual_total": r["actual_total"],
            "edge_magnitude": abs(r["predicted_total"] - args.line),
        })
    print(f"  qualifying bets (after δ filter): {len(bets)}")

    if not bets:
        print("\nNo qualifying bets — relax --line/--delta or --no-cohort-filter.")
        return 1

    # Group by date, preserve fetch order within each date (proxy for FIFO
    # production behavior — first to clear the queue gets placed first).
    by_date: dict[str, list[dict]] = defaultdict(list)
    for b in bets:
        by_date[b["date"]].append(b)

    n_nights = len(by_date)
    nights_with_n: dict[int, int] = defaultdict(int)
    for d, bs in by_date.items():
        nights_with_n[len(bs)] += 1
    print(f"\nNights with qualifying bets: {n_nights}")
    print(f"  distribution: " + ", ".join(
        f"{k}-bet:{v}" for k, v in sorted(nights_with_n.items())
    ))

    # ── Sweep caps ─────────────────────────────────────────────────────
    print("\n" + "=" * 76)
    print("CAP SWEEP — FIFO selection (first N qualifying bets per night)")
    print("=" * 76)
    print(
        f"  {'cap':<5} {'bets':>5} {'wins':>5} {'win_rate':>10} "
        f"{'wilson95%_CI':>20} {'lift_vs_∞':>12}"
    )

    # Baseline: no cap (∞)
    inf_bets = []
    for d, bs in by_date.items():
        inf_bets.extend(bs)
    inf_n = len(inf_bets)
    inf_wins = sum(b["outcome"] for b in inf_bets)
    inf_wr = inf_wins / inf_n if inf_n else 0.0

    sweep_results = []
    for cap in CAPS:
        capped = []
        for d, bs in by_date.items():
            capped.extend(bs[:cap])
        n = len(capped)
        wins = sum(b["outcome"] for b in capped)
        wr = wins / n if n else 0.0
        wlo = _wilson_lower(wins, n)
        whi = _wilson_upper(wins, n)
        cap_label = "∞" if cap >= 10_000 else str(cap)
        lift = (wr - inf_wr) * 100
        print(
            f"  {cap_label:<5} {n:>5} {wins:>5} {wr:>9.1%}  "
            f"[{wlo:>5.1%}, {whi:>5.1%}]  {lift:>+9.1f}pp"
        )
        sweep_results.append((cap, n, wins, wr, wlo, whi))

    # ── Selection criterion alternative: top-N by edge magnitude ──────
    print("\n" + "=" * 76)
    print("CAP SWEEP — EDGE-MAGNITUDE selection (top N bets per night by |pred−line|)")
    print("=" * 76)
    print(
        f"  {'cap':<5} {'bets':>5} {'wins':>5} {'win_rate':>10} "
        f"{'wilson95%_CI':>20} {'lift_vs_∞':>12}"
    )
    for cap in CAPS:
        capped = []
        for d, bs in by_date.items():
            ranked = sorted(bs, key=lambda b: -b["edge_magnitude"])
            capped.extend(ranked[:cap])
        n = len(capped)
        wins = sum(b["outcome"] for b in capped)
        wr = wins / n if n else 0.0
        wlo = _wilson_lower(wins, n)
        whi = _wilson_upper(wins, n)
        cap_label = "∞" if cap >= 10_000 else str(cap)
        lift = (wr - inf_wr) * 100
        print(
            f"  {cap_label:<5} {n:>5} {wins:>5} {wr:>9.1%}  "
            f"[{wlo:>5.1%}, {whi:>5.1%}]  {lift:>+9.1f}pp"
        )

    # ── Interpretation ─────────────────────────────────────────────────
    print("\n" + "=" * 76)
    print("INTERPRETATION")
    print("=" * 76)
    best_fifo = max(sweep_results[:-1], key=lambda x: x[4])  # exclude ∞
    cap_label = "∞" if best_fifo[0] >= 10_000 else str(best_fifo[0])
    print(
        f"  Best-Wilson cap (FIFO): {cap_label}-per-night → "
        f"wilson_lo={best_fifo[4]:.1%}"
    )
    if best_fifo[0] == 2:
        verdict = "matches production setting — leave at 2"
    elif best_fifo[0] < 2:
        verdict = f"suggests TIGHTENING cap to {best_fifo[0]}"
    else:
        verdict = f"suggests LOOSENING cap to {best_fifo[0]}"
    print(f"  → {verdict}")

    # Honest note on the line-move guard
    print("\n  3¢ LINE-MOVE GUARD: not testable here — no historical Kalshi")
    print("  prices in backtest data. The line-move guard can only be")
    print("  validated by counting [LINE-MOVE-SKIP] log lines in production")
    print("  and comparing the win rate on guarded vs unguarded markets")
    print("  once we have ≥30 of each. Re-check after 2 weeks of live data.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
