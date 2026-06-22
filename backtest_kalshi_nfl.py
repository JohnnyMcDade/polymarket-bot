#!/usr/bin/env python3
"""Kalshi NFL season-win-totals backtester.

Standalone, modeled after backtest_kalshi_tennis.py. Uses nfl_data_py
for 3 seasons of completed regular-season game data and predicts each
team's regular-season win total from three classic features:

  1. PRIOR-YEAR WINS — the strongest single predictor of next-year wins.
     Mean-regressed toward the league average (8.5 in a 17-game schedule)
     because last year's outlier records carry more luck than next-year
     skill.
  2. PYTHAGOREAN EXPECTATION — prior-year expected win pct from points
     scored / allowed using the NFL-tuned 2.37 exponent (Pythagorean
     wins are more stable year-to-year than raw wins).
  3. STRENGTH OF SCHEDULE — average opponent prior-year win pct across
     the current season's schedule. Built from the actual current-year
     schedule, so each team gets its own opponent set.

The model blends them with weights derived from a simple in-sample
linear fit (see WEIGHTS in code; they can be retuned by passing
--refit, which prints the OLS coefficients and exits).

WHAT WE TEST
  Two questions, both reported per backtest season:

    a) Model accuracy — mean absolute error of predicted wins vs actual
       wins, vs the naive baseline "league average (8.5) for every
       team" and "last year's wins repeated".

    b) Which teams beat their PREDICTED total most often. This is the
       "find teams that beat their total" question — directionally
       useful for spotting structural under/overrating without
       requiring real Vegas data.

  Optional: if you drop a CSV at data/vegas_win_totals.csv with columns
  season,team,vegas_total, the report also adds a "vs Vegas" section
  with hit rate (model OVER → did team beat Vegas total) and which
  teams cleared their Vegas total most often. nfl_data_py does NOT
  ship Vegas season-win totals — you have to source those yourself
  (Action Network / OddsShark / scraping each season's openers).

CAVEATS
  - First eligible season needs the prior season loaded for features,
    so 3 seasons of data = 2 backtest seasons by default. Pass --years
    to widen.
  - 2020 had a 16-game schedule, 2021+ went to 17. Win totals are not
    normalized — we predict total wins in whatever schedule applied,
    and compare on the same footing.
  - Tied games (rare) count as 0.5 wins per team for prior-year
    features and as not-a-cover for over/under against Vegas totals
    (Vegas books typically refund pushes; for a backtest, exact-match
    counts as no-cover either side).

Run
    python3.12 backtest_kalshi_nfl.py
    python3.12 backtest_kalshi_nfl.py --years 2022 2023 2024 2025
    python3.12 backtest_kalshi_nfl.py --refit         # print OLS coefs
    python3.12 backtest_kalshi_nfl.py --vegas data/vegas_win_totals.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

try:
    import nfl_data_py as nfl
except ImportError:
    print(
        "nfl_data_py is required. Install with:\n"
        "  python3.12 -m pip install --break-system-packages --user nfl_data_py",
        file=sys.stderr,
    )
    sys.exit(2)


REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"

# 2.37 is the league-fit Pythagorean exponent that's become the NFL
# standard since Schatz / Football Outsiders' work — substantially
# better than the baseball 2.0 default.
PYTHAG_EXPONENT = 2.37

# Linear blend weights. These were eyeballed from rough OLS on
# 2022→2024; --refit will print fresh coefficients.
WEIGHTS = {
    "prior_wins_regressed": 0.45,
    "pythag_wins_regressed": 0.35,
    "sos_adjust": 0.20,
}

# Mean-regression weights. The fraction of last-year's outlier delta
# we expect to carry over. ~60% of prior-year results stick, the rest
# regresses toward league mean — historical NFL year-to-year wins
# correlation is in that neighborhood.
REGRESSION_KEEP_FRACTION = 0.60


@dataclass
class TeamSeason:
    season: int
    team: str
    wins: float
    losses: float
    ties: int
    games: int
    points_for: int
    points_against: int

    @property
    def win_pct(self) -> float:
        return self.wins / self.games if self.games else 0.0

    @property
    def pythag_win_pct(self) -> float:
        pf = max(self.points_for, 1)
        pa = max(self.points_against, 1)
        return (pf ** PYTHAG_EXPONENT) / (
            pf ** PYTHAG_EXPONENT + pa ** PYTHAG_EXPONENT
        )

    @property
    def pythag_wins(self) -> float:
        return self.pythag_win_pct * self.games


def aggregate_team_seasons(season: int) -> dict[str, TeamSeason]:
    """Roll completed REG games into per-team season totals."""
    sched = nfl.import_schedules([season])
    reg = sched[sched["game_type"] == "REG"].copy()
    reg = reg[reg["home_score"].notna() & reg["away_score"].notna()]
    teams: dict[str, dict[str, float]] = defaultdict(lambda: {
        "wins": 0.0, "losses": 0.0, "ties": 0, "games": 0,
        "pf": 0, "pa": 0,
    })
    for _, row in reg.iterrows():
        h, a = row["home_team"], row["away_team"]
        hs, as_ = int(row["home_score"]), int(row["away_score"])
        teams[h]["games"] += 1
        teams[a]["games"] += 1
        teams[h]["pf"] += hs
        teams[h]["pa"] += as_
        teams[a]["pf"] += as_
        teams[a]["pa"] += hs
        if hs > as_:
            teams[h]["wins"] += 1
            teams[a]["losses"] += 1
        elif hs < as_:
            teams[a]["wins"] += 1
            teams[h]["losses"] += 1
        else:
            teams[h]["wins"] += 0.5
            teams[a]["wins"] += 0.5
            teams[h]["ties"] += 1
            teams[a]["ties"] += 1
    out: dict[str, TeamSeason] = {}
    for team, agg in teams.items():
        out[team] = TeamSeason(
            season=season,
            team=team,
            wins=agg["wins"],
            losses=agg["losses"],
            ties=agg["ties"],
            games=int(agg["games"]),
            points_for=int(agg["pf"]),
            points_against=int(agg["pa"]),
        )
    return out


def opponents_for(season: int, team: str) -> list[str]:
    sched = nfl.import_schedules([season])
    reg = sched[sched["game_type"] == "REG"]
    opps: list[str] = []
    for _, row in reg.iterrows():
        if row["home_team"] == team:
            opps.append(row["away_team"])
        elif row["away_team"] == team:
            opps.append(row["home_team"])
    return opps


def schedule_opponents(season: int) -> dict[str, list[str]]:
    """All opponents per team for a season's REG schedule."""
    sched = nfl.import_schedules([season])
    reg = sched[sched["game_type"] == "REG"]
    out: dict[str, list[str]] = defaultdict(list)
    for _, row in reg.iterrows():
        out[row["home_team"]].append(row["away_team"])
        out[row["away_team"]].append(row["home_team"])
    return dict(out)


def regress_to_mean(value: float, league_mean: float) -> float:
    return league_mean + REGRESSION_KEEP_FRACTION * (value - league_mean)


@dataclass
class Prediction:
    season: int
    team: str
    games: int
    actual_wins: float
    pred_wins: float
    prior_wins: float
    prior_pythag_wins: float
    sos_prior_win_pct: float


def predict_season(
    season: int,
    prior_seasons: dict[str, TeamSeason],
    schedule: dict[str, list[str]],
    actual: dict[str, TeamSeason],
) -> list[Prediction]:
    if not prior_seasons:
        return []

    # League mean for THIS season's schedule length, so 16-game and
    # 17-game seasons regress to their own midpoints.
    games_per_team = {t: ts.games for t, ts in actual.items()}
    league_mean_wins = sum(games_per_team.values()) / (2 * len(games_per_team))

    # Prior-year league mean wins (for the SOS opponent strength baseline).
    prior_games = [ts.games for ts in prior_seasons.values()]
    prior_mean_wins = sum(prior_games) / (2 * len(prior_games)) if prior_games else 8.5
    prior_mean_win_pct = 0.5  # by construction

    preds: list[Prediction] = []
    teams = sorted(actual.keys())
    for team in teams:
        if team not in schedule:
            continue
        games = actual[team].games

        # Feature 1: prior-year wins, regressed and scaled to current schedule.
        if team in prior_seasons:
            prior = prior_seasons[team]
            scale = games / prior.games if prior.games else 1.0
            prior_wins_scaled = prior.wins * scale
            prior_wins_regressed = regress_to_mean(prior_wins_scaled, league_mean_wins)
            prior_pythag_scaled = prior.pythag_wins * scale
            prior_pythag_regressed = regress_to_mean(prior_pythag_scaled, league_mean_wins)
        else:
            prior_wins_scaled = league_mean_wins
            prior_wins_regressed = league_mean_wins
            prior_pythag_regressed = league_mean_wins

        # Feature 3: SOS — mean of opponents' prior-year win pct, deviation
        # from 0.500. Negative deviation = harder schedule → reduce wins.
        opps = schedule[team]
        opp_win_pcts = []
        for o in opps:
            if o in prior_seasons:
                opp_win_pcts.append(prior_seasons[o].win_pct)
        if opp_win_pcts:
            sos = sum(opp_win_pcts) / len(opp_win_pcts)
        else:
            sos = prior_mean_win_pct
        sos_delta = sos - prior_mean_win_pct
        # SOS adjustment: shift predicted wins down for tougher schedule.
        # One full SOS point (e.g. .500 → 1.500) is a hypothetical
        # ceiling; in practice SOS deltas span ±0.05. Translate that
        # into a wins-equivalent by multiplying by games (a +0.05 SOS
        # means 5% more wins expected from baseline opponents → ~0.85
        # extra projected wins in a 17-game schedule).
        sos_wins_adjust = -sos_delta * games

        pred = (
            WEIGHTS["prior_wins_regressed"] * prior_wins_regressed
            + WEIGHTS["pythag_wins_regressed"] * prior_pythag_regressed
            + WEIGHTS["sos_adjust"] * sos_wins_adjust
        )
        # Renormalize: the SOS term is an adjustment, not a level. Add it
        # on top of the level-weighted prior+pythag.
        level = (
            (WEIGHTS["prior_wins_regressed"] * prior_wins_regressed
             + WEIGHTS["pythag_wins_regressed"] * prior_pythag_regressed)
            / (WEIGHTS["prior_wins_regressed"] + WEIGHTS["pythag_wins_regressed"])
        )
        pred_wins = level + WEIGHTS["sos_adjust"] * sos_wins_adjust
        pred_wins = max(0.0, min(float(games), pred_wins))

        preds.append(Prediction(
            season=season,
            team=team,
            games=games,
            actual_wins=actual[team].wins,
            pred_wins=pred_wins,
            prior_wins=prior_wins_scaled,
            prior_pythag_wins=prior_pythag_regressed,
            sos_prior_win_pct=sos,
        ))
    return preds


def mean_abs_error(preds: Iterable[Prediction], baseline_fn) -> float:
    preds = list(preds)
    if not preds:
        return 0.0
    return sum(abs(p.actual_wins - baseline_fn(p)) for p in preds) / len(preds)


def report_season(season: int, preds: list[Prediction]) -> None:
    if not preds:
        print(f"  [skip] No predictions for {season} (likely no prior-year data).")
        return
    league_mean = sum(p.games for p in preds) / (2 * len(preds))
    mae_model = mean_abs_error(preds, lambda p: p.pred_wins)
    mae_naive = mean_abs_error(preds, lambda _p: league_mean)
    mae_prior = mean_abs_error(preds, lambda p: p.prior_wins)

    print(f"\n=== {season} season backtest ({len(preds)} teams) ===")
    print(f"  Model MAE              : {mae_model:.2f} wins")
    print(f"  Naive (league mean)    : {mae_naive:.2f} wins")
    print(f"  Naive (prior-yr wins)  : {mae_prior:.2f} wins")
    delta_naive = mae_naive - mae_model
    delta_prior = mae_prior - mae_model
    print(f"  Edge over league-mean  : {delta_naive:+.2f} wins")
    print(f"  Edge over prior-yr     : {delta_prior:+.2f} wins")

    # Top "beat predicted" teams: largest positive (actual - predicted).
    over = sorted(preds, key=lambda p: p.actual_wins - p.pred_wins, reverse=True)[:6]
    under = sorted(preds, key=lambda p: p.actual_wins - p.pred_wins)[:6]
    print("\n  Teams that beat their predicted total most:")
    for p in over:
        diff = p.actual_wins - p.pred_wins
        print(f"    {p.team:<4} actual={p.actual_wins:>4.1f}  pred={p.pred_wins:>4.1f}  Δ={diff:+.2f}")
    print("\n  Teams that missed their predicted total most:")
    for p in under:
        diff = p.actual_wins - p.pred_wins
        print(f"    {p.team:<4} actual={p.actual_wins:>4.1f}  pred={p.pred_wins:>4.1f}  Δ={diff:+.2f}")


def load_vegas_totals(path: Path) -> dict[tuple[int, str], float]:
    if not path.exists():
        return {}
    out: dict[tuple[int, str], float] = {}
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                season = int(row["season"])
                team = row["team"].strip().upper()
                vegas = float(row["vegas_total"])
            except (KeyError, ValueError):
                continue
            out[(season, team)] = vegas
    return out


def report_vs_vegas(
    all_preds: list[Prediction],
    vegas: dict[tuple[int, str], float],
) -> None:
    matched = [(p, vegas[(p.season, p.team)])
               for p in all_preds if (p.season, p.team) in vegas]
    if not matched:
        print("\n  [vegas] No matching rows in vegas CSV — skipping vs-Vegas section.")
        return
    print(f"\n=== vs Vegas season win totals ({len(matched)} team-seasons) ===")

    model_correct = 0
    coverage = 0
    pushes = 0
    for p, vt in matched:
        if p.pred_wins == vt:
            continue
        model_side = "OVER" if p.pred_wins > vt else "UNDER"
        if p.actual_wins == vt:
            pushes += 1
            continue
        actual_side = "OVER" if p.actual_wins > vt else "UNDER"
        coverage += 1
        if model_side == actual_side:
            model_correct += 1
    if coverage:
        wr = model_correct / coverage
        print(f"  Model side hit rate    : {wr:.1%} ({model_correct}/{coverage}, {pushes} pushes)")

    # Best "Vegas-beaters" — teams whose actual wins beat Vegas total by the
    # largest margin.
    sorted_by_v = sorted(matched, key=lambda mv: mv[0].actual_wins - mv[1], reverse=True)[:8]
    print("\n  Biggest Vegas overs (actual − vegas):")
    for p, vt in sorted_by_v:
        diff = p.actual_wins - vt
        print(f"    {p.season} {p.team:<4} actual={p.actual_wins:>4.1f}  vegas={vt:>4.1f}  Δ={diff:+.2f}")

    sorted_by_v_under = sorted(matched, key=lambda mv: mv[0].actual_wins - mv[1])[:8]
    print("\n  Biggest Vegas unders (actual − vegas):")
    for p, vt in sorted_by_v_under:
        diff = p.actual_wins - vt
        print(f"    {p.season} {p.team:<4} actual={p.actual_wins:>4.1f}  vegas={vt:>4.1f}  Δ={diff:+.2f}")


def refit_weights(all_preds: list[Prediction]) -> None:
    """Print rough OLS coefficients fit on actual ~ prior + pythag + SOS."""
    try:
        import numpy as np  # noqa: WPS433
    except ImportError:
        print("numpy required for --refit", file=sys.stderr)
        return
    if not all_preds:
        print("No predictions to fit.")
        return
    X = np.array([[p.prior_wins, p.prior_pythag_wins,
                   -(p.sos_prior_win_pct - 0.5) * p.games, 1.0]
                  for p in all_preds])
    y = np.array([p.actual_wins for p in all_preds])
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    print("\nOLS fit (actual_wins ~ prior + pythag + sos_adjust + intercept):")
    print(f"  prior_wins coef     : {coef[0]:+.3f}")
    print(f"  pythag_wins coef    : {coef[1]:+.3f}")
    print(f"  sos_adjust coef     : {coef[2]:+.3f}")
    print(f"  intercept           : {coef[3]:+.3f}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--years", type=int, nargs="+", default=[2022, 2023, 2024, 2025],
        help="Seasons to load. First season is used only as prior — "
             "remaining seasons are backtested. Default: 2022 2023 2024 2025.",
    )
    parser.add_argument(
        "--vegas", default=str(DATA_DIR / "vegas_win_totals.csv"),
        help="Optional CSV of season,team,vegas_total rows.",
    )
    parser.add_argument(
        "--refit", action="store_true",
        help="Print OLS weights from in-sample fit then exit.",
    )
    args = parser.parse_args()

    years = sorted(set(args.years))
    if len(years) < 2:
        print("Need at least 2 seasons (1 as prior).", file=sys.stderr)
        sys.exit(1)

    print(f"Loading seasons: {years}")
    season_data: dict[int, dict[str, TeamSeason]] = {}
    for yr in years:
        season_data[yr] = aggregate_team_seasons(yr)
        n_teams = len(season_data[yr])
        first = next(iter(season_data[yr].values()), None)
        games = first.games if first else 0
        print(f"  {yr}: {n_teams} teams, {games} games/team")

    all_preds: list[Prediction] = []
    for i, yr in enumerate(years):
        if i == 0:
            continue
        prior_year = years[i - 1]
        schedule = schedule_opponents(yr)
        preds = predict_season(
            yr, season_data[prior_year], schedule, season_data[yr],
        )
        all_preds.extend(preds)
        report_season(yr, preds)

    if args.refit:
        refit_weights(all_preds)
        return

    vegas = load_vegas_totals(Path(args.vegas))
    report_vs_vegas(all_preds, vegas)


if __name__ == "__main__":
    main()
