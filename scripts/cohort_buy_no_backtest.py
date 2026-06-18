"""Production-cohort BUY_NO backtest. Reuses backtest_kalshi's cached
data; applies the exact filters the production rule enforces:
  - KXMLBTOTAL line in {8.5, 9.5}  (== `-9` and `-10` tickers)
  - direction: UNDER only           (== BUY_NO on these tickers)
  - both starters' rolling_era_last3 < ERA_CAP  (default 3.50)

We show two cohort definitions and the production reading:
  (a) BOTH starters < cap — strict, matches production rule
  (b) AVG starter ERA < cap — looser, matches the existing dashboard
      qualifying-games definition

Reports n, win_rate, Wilson 95% LB, and lift over the always-UNDER
baseline computed on the SAME filtered cohort (not the full slate —
baseline shifts in the elite-pitching subset).

Run
    python3 scripts/cohort_buy_no_backtest.py              # 60d, cap 3.50
    python3 scripts/cohort_buy_no_backtest.py 180          # 180d, cap 3.50
    python3 scripts/cohort_buy_no_backtest.py 60 --era-cap 3.75
"""
from datetime import datetime, timezone, timedelta
import argparse
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import backtest_kalshi as bt


def _wilson_lower(c, n, z=1.96):
    if n == 0:
        return 0.0
    p = c / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return (centre - margin) / denom


def under_wr(rows, T):
    """For rows in this cohort, the always-UNDER win-rate (baseline) AND
    the predictor-led UNDER win-rate. Predictor 'bet UNDER' is the
    existing rule: predicted_total <= T - delta (we sweep delta)."""
    actuals = [r['actual_total'] for r in rows if r['actual_total'] != T]
    n_total = len(actuals)
    under_w = sum(1 for a in actuals if a < T)
    baseline = under_w / n_total if n_total else 0.0
    return baseline, n_total


def sweep_under(rows, T, deltas):
    actuals_pred = [(r['predicted_total'], r['actual_total']) for r in rows
                    if r['actual_total'] != T]
    out = []
    for d in deltas:
        w = l = 0
        for p, a in actuals_pred:
            if p <= T - d:  # predictor says UNDER
                if a < T:
                    w += 1
                else:
                    l += 1
        n = w + l
        wr = w / n if n else 0.0
        wlo = _wilson_lower(w, n)
        out.append({'delta': d, 'n': n, 'w': w, 'l': l, 'wr': wr, 'wlo': wlo})
    return out


def main(days=60, era_cap=3.50):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    print(f"Fetching schedule {start} → {end}...")
    print(f"ERA cap: < {era_cap}")
    games = bt.fetch_schedule(start.isoformat(), end.isoformat())
    print(f"  {len(games)} finished games")
    pids = {pid for g in games
            for pid in (g.home_pitcher_id, g.away_pitcher_id) if pid}
    print(f"Loading gamelogs for {len(pids)} pitchers (cached)...")
    gamelogs = {pid: bt.fetch_pitcher_gamelog(pid, end.year) for pid in pids}
    rows, skipped = bt.run_total_backtest(games, gamelogs)
    print(f"  {len(rows)} rows pre-filter, skipped={skipped}")

    # Production cohort: BOTH starters' ERA (rolling_era_last3 proxy) < era_cap
    strict = [r for r in rows
              if r['home_era'] is not None and r['away_era'] is not None
              and r['home_era'] < era_cap and r['away_era'] < era_cap]
    # Looser cohort: AVG of the two ERAs < era_cap (dashboard definition)
    avg_lt = [r for r in rows
              if r['starter_avg_era'] < era_cap]

    print(f"\nStrict cohort (both starters < {era_cap}): n={len(strict)}")
    print(f"Avg-ERA cohort (avg < {era_cap}):           n={len(avg_lt)}")

    deltas = (0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5)
    for cohort_name, cohort in (
        (f"STRICT (both starters < {era_cap} — matches production rule)", strict),
        (f"LOOSER (avg starter ERA < {era_cap} — dashboard definition)", avg_lt),
    ):
        print()
        print("=" * 80)
        print(f"COHORT: {cohort_name}")
        print("=" * 80)
        if not cohort:
            print("  (empty cohort — try --days 180 for more sample)")
            continue
        for T in (8.5, 9.5):
            ticker_n = "-9" if T == 8.5 else "-10"
            baseline, n_total = under_wr(cohort, T)
            print(f"\n  Line T={T}  (ticker {ticker_n})  cohort_n={len(cohort)}")
            print(f"    always-UNDER baseline ON THIS COHORT: {baseline:.1%}  (n_eligible={n_total})")
            sweep = sweep_under(cohort, T, deltas)
            print(f"    {'δ':>4}   {'n':>4}  {'wins':>5}  {'wr':>7}  {'wlo':>7}  {'lift':>7}  {'★ stable?'}")
            for s in sweep:
                lift = s['wr'] - baseline
                stable = s['wlo'] > baseline and s['n'] >= 15 and lift >= 0.02
                star = "  ★" if stable else "   "
                wr_s = f"{s['wr']*100:5.1f}%" if s['n'] else "  —  "
                wlo_s = f"{s['wlo']*100:5.1f}%" if s['n'] else "  —  "
                lift_s = f"{lift*100:+5.1f}%" if s['n'] else "  —  "
                print(f"    δ={s['delta']:<4.2f} {s['n']:>4}  {s['w']:>5}  {wr_s}  {wlo_s}  {lift_s} {star}")


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument("days", nargs="?", type=int, default=60,
                   help="Backtest window in days (default 60)")
    p.add_argument("--era-cap", type=float, default=3.50,
                   help="Season ERA ceiling per starter (default 3.50)")
    args = p.parse_args()
    main(args.days, args.era_cap)
