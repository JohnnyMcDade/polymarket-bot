"""KXNFLWINS season win-total predictor backtest.

Fetches N seasons of NFL team-season data via nfl_data_py and tests a
predictor built from:
  - Prior season wins (regressed toward league mean)
  - Strength of schedule (prior-year average opponent wins; forward-looking)
  - QB situation (primary starter same vs changed vs unknown)

Predicts each team's regular-season wins, compares to actual, and
surfaces Wilson-stable cells where the model beats a naive prior-year
baseline.

Vegas O/U comparison is deliberately omitted at this stage — no clean
public data source exists for historical season win totals, and we're
avoiding ToS-protected scraping. A Vegas overlay can be layered on
later once historical lines are sourced.

Run (uses python3.12 because pandas does not yet ship wheels for 3.14):
    python3.12 backtest_nfl.py
    python3.12 backtest_nfl.py --seasons 2018 2019 2020 2021 2022 2023 2024
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter

import nfl_data_py as nfl
import pandas as pd


WILSON_MIN_N = 15  # mirrors backtest_kalshi.py's cohort-stability threshold


def _wilson_lower(c: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = c / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return (centre - margin) / denom


def fetch_team_seasons(seasons: list[int]) -> pd.DataFrame:
    """Regular-season-only team-season records with opponent list and
    primary starting QB id. Primary QB = the qb_id appearing most often
    as this team's starter across that season's REG games."""
    sched = nfl.import_schedules(seasons)
    sched = sched[sched["game_type"] == "REG"].copy()
    # Drop unfinished games (scores still NaN — e.g. mid-season fetch).
    sched = sched.dropna(subset=["home_score", "away_score"])

    rows = []
    for season in sorted(set(seasons)):
        s = sched[sched["season"] == season]
        teams = sorted(set(s["home_team"]) | set(s["away_team"]))
        for team in teams:
            home = s[s["home_team"] == team]
            away = s[s["away_team"] == team]
            wins = int(
                (home["home_score"] > home["away_score"]).sum()
                + (away["away_score"] > away["home_score"]).sum()
            )
            losses = int(
                (home["home_score"] < home["away_score"]).sum()
                + (away["away_score"] < away["home_score"]).sum()
            )
            ties = int(
                (home["home_score"] == home["away_score"]).sum()
                + (away["away_score"] == away["home_score"]).sum()
            )
            opponents = list(home["away_team"]) + list(away["home_team"])
            qbs = list(home["home_qb_id"].dropna()) + list(away["away_qb_id"].dropna())
            primary_qb = Counter(qbs).most_common(1)[0][0] if qbs else None
            rows.append(
                {
                    "season": int(season),
                    "team": team,
                    "wins": wins,
                    "losses": losses,
                    "ties": ties,
                    "games": wins + losses + ties,
                    "opponents": opponents,
                    "primary_qb_id": primary_qb,
                }
            )
    return pd.DataFrame(rows)


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Adds prior_wins / sos / qb_status. Drops rows missing prior season
    (the earliest year in the fetch is the lookup table, not a backtest
    row)."""
    wins_by = {(r.season, r.team): r.wins for r in df.itertuples()}
    qb_by = {(r.season, r.team): r.primary_qb_id for r in df.itertuples()}

    feats = []
    for r in df.itertuples():
        prior_wins = wins_by.get((r.season - 1, r.team))
        if prior_wins is None:
            continue
        opp_priors = [wins_by.get((r.season - 1, o)) for o in r.opponents]
        opp_priors = [w for w in opp_priors if w is not None]
        if not opp_priors:
            continue
        # SOS = avg opponent prior-year wins, normalized to a win-pct
        # against a 17-game baseline. ~0.50 = average schedule.
        sos = sum(opp_priors) / len(opp_priors) / 17.0
        prior_qb = qb_by.get((r.season - 1, r.team))
        cur_qb = qb_by.get((r.season, r.team))
        if prior_qb is None or cur_qb is None:
            qb_status = "unknown"
        elif prior_qb == cur_qb:
            qb_status = "same"
        else:
            qb_status = "changed"
        feats.append(
            {
                "season": r.season,
                "team": r.team,
                "actual_wins": r.wins,
                "games": r.games,
                "prior_wins": prior_wins,
                "sos": sos,
                "qb_status": qb_status,
            }
        )
    return pd.DataFrame(feats)


def predict(prior_wins: float, sos: float, qb_status: str) -> float:
    """Linear-blend predictor. Weights are intuition-based starting points;
    the backtest surfaces which cells they actually outperform the naive
    baseline in.

      base    = 0.65 * prior_wins + 0.35 * 8.5  (regress toward league mean)
      sos_adj = (0.5 - sos) * 5  clipped to [-1.5, +1.5]
                (tough schedule subtracts, easy schedule adds)
      qb_adj  = -1.0 if 'changed', 0 otherwise
    """
    base = 0.65 * prior_wins + 0.35 * 8.5
    sos_adj = max(-1.5, min(1.5, (0.5 - sos) * 5.0))
    qb_adj = -1.0 if qb_status == "changed" else 0.0
    return base + sos_adj + qb_adj


def backtest(feats: pd.DataFrame) -> pd.DataFrame:
    out = feats.copy()
    out["pred"] = out.apply(
        lambda r: predict(r["prior_wins"], r["sos"], r["qb_status"]), axis=1
    )
    out["naive_pred"] = out["prior_wins"]
    out["err"] = (out["pred"] - out["actual_wins"]).abs()
    out["naive_err"] = (out["naive_pred"] - out["actual_wins"]).abs()
    out["beats_naive"] = out["err"] < out["naive_err"]
    return out


def _cell_line(label_pad: int, label: str, sub: pd.DataFrame) -> str:
    n = len(sub)
    if n == 0:
        return f"  {label:<{label_pad}}  (empty)"
    beats = int(sub["beats_naive"].sum())
    wlo = _wilson_lower(beats, n)
    marker = " ✓stable" if n >= WILSON_MIN_N and wlo > 0.50 else ""
    return (
        f"  {label:<{label_pad}}  n={n:>3}  "
        f"MAE={sub['err'].mean():.2f} (naive {sub['naive_err'].mean():.2f})  "
        f"beats={beats}/{n} ({beats/n:.0%})  wilson_lo={wlo:.0%}{marker}"
    )


def report(out: pd.DataFrame) -> None:
    n = len(out)
    print("=" * 78)
    print(f"NFL WIN-TOTAL PREDICTOR BACKTEST — {n} team-seasons")
    print("=" * 78)
    mae = out["err"].mean()
    naive_mae = out["naive_err"].mean()
    rmse = (out["err"] ** 2).mean() ** 0.5
    naive_rmse = (out["naive_err"] ** 2).mean() ** 0.5
    beats = int(out["beats_naive"].sum())
    pct = beats / n
    print(
        f"\nOverall MAE:  {mae:.2f} wins  (naive: {naive_mae:.2f})  "
        f"delta {mae - naive_mae:+.2f}"
    )
    print(
        f"Overall RMSE: {rmse:.2f}  (naive: {naive_rmse:.2f})  "
        f"delta {rmse - naive_rmse:+.2f}"
    )
    print(
        f"% beats naive: {pct:.1%}  ({beats}/{n})  "
        f"wilson_lo={_wilson_lower(beats, n):.1%}"
    )
    if mae >= naive_mae:
        print("\nWARNING: predictor does NOT beat the naive baseline on average.")
    else:
        print("\nOK: predictor beats the naive baseline on average.")

    print("\n--- By QB status ---")
    for status in ("same", "changed", "unknown"):
        print(_cell_line(10, status, out[out["qb_status"] == status]))

    print("\n--- By SOS tier ---")
    SOS = [
        (0.00, 0.45, "easy"),
        (0.45, 0.50, "below_avg"),
        (0.50, 0.55, "above_avg"),
        (0.55, 1.00, "tough"),
    ]
    for lo, hi, label in SOS:
        sub = out[(out["sos"] >= lo) & (out["sos"] < hi)]
        print(_cell_line(10, label, sub))

    print("\n--- By prior_wins tier ---")
    WIN = [
        (0, 5, "bad (<=4)"),
        (5, 8, "below (5-7)"),
        (8, 11, "avg (8-10)"),
        (11, 18, "strong (11+)"),
    ]
    for lo, hi, label in WIN:
        sub = out[(out["prior_wins"] >= lo) & (out["prior_wins"] < hi)]
        print(_cell_line(14, label, sub))

    print("\n--- Wilson-stable cross-cells (qb x sos x prior_wins, n >= 15) ---")
    found = []
    for qb_st in ("same", "changed"):
        for slo, shi, sl in SOS:
            for wlo_w, whi_w, wl in WIN:
                sub = out[
                    (out["qb_status"] == qb_st)
                    & (out["sos"] >= slo)
                    & (out["sos"] < shi)
                    & (out["prior_wins"] >= wlo_w)
                    & (out["prior_wins"] < whi_w)
                ]
                if len(sub) < WILSON_MIN_N:
                    continue
                beats_c = int(sub["beats_naive"].sum())
                wlo_v = _wilson_lower(beats_c, len(sub))
                if wlo_v <= 0.50:
                    continue
                found.append(
                    (wlo_v, beats_c / len(sub), beats_c, len(sub), qb_st, sl, wl)
                )
    if not found:
        print("  (none — try more seasons or different bucket edges)")
    else:
        for wlo_v, wr, beats_c, n_c, qb_st, sl, wl in sorted(found, reverse=True):
            print(
                f"  qb={qb_st:<8} sos={sl:<10} prior={wl:<14} "
                f"n={n_c:>3} beats={beats_c}/{n_c} ({wr:.0%}) "
                f"wilson_lo={wlo_v:.0%}  *stable"
            )

    print("\n--- Top 10 worst predictions (largest |pred - actual|) ---")
    worst = out.sort_values("err", ascending=False).head(10)
    for r in worst.itertuples():
        print(
            f"  {r.season} {r.team:>3}  pred={r.pred:>4.1f}  actual={r.actual_wins:>2}  "
            f"err={r.err:>4.1f}  prior={int(r.prior_wins)}  "
            f"sos={r.sos:.3f}  qb={r.qb_status}"
        )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--seasons",
        nargs="+",
        type=int,
        default=[2020, 2021, 2022, 2023, 2024],
        help="Backtest seasons. The script auto-fetches the year before "
        "the earliest as a lookup table for prior_wins.",
    )
    args = p.parse_args()

    earliest = min(args.seasons)
    fetch_years = sorted(set(args.seasons + [earliest - 1]))
    print(
        f"Fetching NFL schedule for {fetch_years} "
        f"(includes {earliest - 1} as prior-year lookup)..."
    )
    df = fetch_team_seasons(fetch_years)
    print(f"  {len(df)} team-seasons loaded")
    feats = compute_features(df)
    # Restrict the report to the seasons the user actually asked for.
    feats = feats[feats["season"].isin(args.seasons)].reset_index(drop=True)
    print(f"  {len(feats)} team-seasons in backtest window with prior features")
    if len(feats) == 0:
        print("ERROR: no usable rows after prior-year join.", file=sys.stderr)
        return 1
    out = backtest(feats)
    report(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
