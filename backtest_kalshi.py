#!/usr/bin/env python3
"""Kalshi MLB backtester — deterministic heuristic on historical games (v2).

WHAT THIS IS:
  A fast strategy-iteration tool. For every finished MLB game in the
  last N days, it predicts the winner from pitcher ERA + team record +
  home/away using a closed-form formula, then compares to actual results.
  Stratifies accuracy by pitcher quality, side, record gap, and combos —
  with a Wilson 95% lower bound so we surface cohorts that are
  statistically distinguishable from a coin flip, not just lucky n=15s.

WHAT THIS IS NOT:
  A faithful replay of kalshi_edge.py. The production agent makes calls
  through Claude (Sonnet 4.6) — not a deterministic function of features.
  This script is a parallel heuristic on the same feature set, useful for:
    1. Finding which feature combos historically clear 55%+ directional
       accuracy (the floor any betting strategy needs).
    2. Iterating on weights and thresholds in seconds, not Claude-API
       dollars and minutes.
  A combo that hits 60%+ here is a strong candidate to encode into the
  edge prompt; one stuck at 50% means the features alone aren't enough.

v2 CHANGES — future-knowledge bias removed:
  - Team records pulled per-date with /standings?date=X-1 (one call per
    unique game date). Reflects W-L as of the night before the game.
  - Pitcher ERA pulled from /people/{id}/stats?stats=gameLog (one call
    per unique pitcher). Each gameLog split's `era` field is the
    cumulative season ERA after that game, so the ERA "going into"
    a game on date X is the ERA from the most recent prior start.
  - Games with no prior starts for a pitcher (season debut / call-up)
    are skipped — no signal, no fake league-average fallback.
  - Games before standings exist (season opening day) are skipped.

REMAINING v2 limitations (honest):
  - Still a heuristic. Claude has access to context (injuries, recent
    form, lineup news) that closed-form features can't capture.
  - No market-price data — we measure directional accuracy, not edge.
    A real strategy needs accuracy > implied market probability, not
    just > 50%.
  - Pitcher gameLog `era` is cumulative season ERA, not rolling form.
    A pitcher who got shelled in his last 3 starts looks the same as
    one who was steady — both same season ERA. Rolling-window ERA
    would be a v3 enhancement.

Run:
    python backtest_kalshi.py                    # default: 60 days, this season
    python backtest_kalshi.py --days 30
    python backtest_kalshi.py --no-cache         # force fresh API pulls
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

MLB_BASE = "https://statsapi.mlb.com/api/v1"
CACHE_DIR = Path.home() / ".cache" / "backtest_kalshi"

# ---------------------------------------------------------------------------
# Heuristic constants — tweak these to iterate on strategy
# ---------------------------------------------------------------------------
HOME_BASE_WP = 0.54              # MLB historical home-field edge
PITCHER_ERA_WEIGHT = 0.025       # WP swing per ERA point of difference
RECORD_WPCT_WEIGHT = 0.30        # WP swing per 1.000 of wpct difference
HIGH_CONF_THRESHOLD = 0.65
MEDIUM_CONF_THRESHOLD = 0.55

ERA_TIERS = [
    (2.50, "elite"),
    (3.50, "good"),
    (4.50, "average"),
    (5.50, "below_avg"),
    (99.0, "bad"),
]

WPCT_BUCKETS = [
    (0.00, 0.05, "0-5%"),
    (0.05, 0.10, "5-10%"),
    (0.10, 0.20, "10-20%"),
    (0.20, 1.00, "20%+"),
]


def _prev_day(d: str) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()


def _wilson_lower(c: int, n: int, z: float = 1.96) -> float:
    """Wilson 95% lower bound for a binomial proportion. Lets us surface
    cohorts whose win rate is statistically distinguishable from 0.50,
    rather than rewarding lucky small samples."""
    if n == 0:
        return 0.0
    p = c / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return (centre - margin) / denom


@dataclass
class Game:
    game_pk: int
    date: str
    home_team_id: int
    away_team_id: int
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    home_pitcher_id: int | None
    away_pitcher_id: int | None
    actual_winner: str  # "home" or "away"


# ---------------------------------------------------------------------------
# MLB API fetchers (cached on disk)
# ---------------------------------------------------------------------------
def _cache_path(name: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / name


def _cached_get(name: str, url: str, params: dict, ttl_s: int = 86400) -> dict:
    path = _cache_path(name)
    if path.exists() and (time.time() - path.stat().st_mtime) < ttl_s:
        return json.loads(path.read_text())
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    path.write_text(json.dumps(data))
    return data


def fetch_schedule(start: str, end: str) -> list[Game]:
    data = _cached_get(
        f"schedule_{start}_{end}.json",
        f"{MLB_BASE}/schedule",
        {
            "sportId": 1,
            "startDate": start,
            "endDate": end,
            "gameType": "R",
            "hydrate": "probablePitcher",
        },
    )
    games: list[Game] = []
    for date_block in data.get("dates", []):
        date = date_block.get("date", "")
        for g in date_block.get("games", []):
            if g.get("status", {}).get("abstractGameState") != "Final":
                continue
            home = g["teams"]["home"]
            away = g["teams"]["away"]
            h_score = home.get("score")
            a_score = away.get("score")
            if h_score is None or a_score is None or h_score == a_score:
                continue
            winner = "home" if h_score > a_score else "away"
            games.append(Game(
                game_pk=g["gamePk"],
                date=date,
                home_team_id=home["team"]["id"],
                away_team_id=away["team"]["id"],
                home_team=home["team"].get("name", ""),
                away_team=away["team"].get("name", ""),
                home_score=h_score,
                away_score=a_score,
                home_pitcher_id=(home.get("probablePitcher") or {}).get("id"),
                away_pitcher_id=(away.get("probablePitcher") or {}).get("id"),
                actual_winner=winner,
            ))
    return games


def fetch_team_records_as_of(season: int, as_of_date: str) -> dict[int, dict[str, Any]]:
    """Standings as they were at end-of-day on as_of_date. The MLB API's
    `date` parameter returns records through games completed that date."""
    data = _cached_get(
        f"standings_{season}_{as_of_date}.json",
        f"{MLB_BASE}/standings",
        {
            "leagueId": "103,104",
            "season": season,
            "seasonType": "R",
            "date": as_of_date,
        },
    )
    out: dict[int, dict[str, Any]] = {}
    for rec in data.get("records", []):
        for tr in rec.get("teamRecords", []):
            team = tr.get("team", {})
            tid = team.get("id")
            if tid is None:
                continue
            wins = int(tr.get("wins", 0))
            losses = int(tr.get("losses", 0))
            tot = wins + losses
            wpct = wins / tot if tot > 0 else 0.5
            out[int(tid)] = {"wins": wins, "losses": losses, "wpct": wpct, "name": team.get("name")}
    return out


def fetch_pitcher_gamelog(pid: int, season: int) -> list[dict[str, Any]]:
    """Full-season pitching game log for `pid`. Returns sorted list of
    {date, era} entries where `era` is cumulative season ERA through
    that game (so to get ERA going into a game on date X, take the most
    recent entry with date <= X-1)."""
    if pid is None:
        return []
    path = _cache_path(f"gamelog_{pid}_{season}.json")
    if path.exists():
        return json.loads(path.read_text())
    entries: list[dict[str, Any]] = []
    try:
        r = requests.get(
            f"{MLB_BASE}/people/{pid}/stats",
            params={
                "stats": "gameLog",
                "season": season,
                "group": "pitching",
                "sportId": 1,
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        for stat_block in data.get("stats", []):
            for split in stat_block.get("splits", []):
                d = split.get("date")
                era_str = split.get("stat", {}).get("era")
                if not d or era_str in (None, "-.--"):
                    continue
                try:
                    entries.append({"date": d, "era": float(era_str)})
                except ValueError:
                    continue
        entries.sort(key=lambda e: e["date"])
    except (requests.RequestException, ValueError, KeyError):
        entries = []
    path.write_text(json.dumps(entries))
    return entries


def era_as_of(gamelog: list[dict[str, Any]], as_of_date: str) -> float | None:
    """Cumulative season ERA from the most recent entry on-or-before as_of_date.
    Returns None if the pitcher had no recorded starts in that window."""
    best: float | None = None
    for entry in gamelog:
        if entry["date"] <= as_of_date:
            best = entry["era"]
        else:
            break
    return best


# ---------------------------------------------------------------------------
# Heuristic predictor
# ---------------------------------------------------------------------------
def era_tier(era: float | None) -> str:
    if era is None:
        return "unknown"
    for cutoff, name in ERA_TIERS:
        if era <= cutoff:
            return name
    return "bad"


def predict(
    game: Game,
    pitcher_eras: dict[int, float | None],
    team_records: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    home_era = pitcher_eras.get(game.home_pitcher_id) if game.home_pitcher_id else None
    away_era = pitcher_eras.get(game.away_pitcher_id) if game.away_pitcher_id else None
    home_rec = team_records.get(game.home_team_id, {})
    away_rec = team_records.get(game.away_team_id, {})
    home_wpct = float(home_rec.get("wpct", 0.5))
    away_wpct = float(away_rec.get("wpct", 0.5))

    p = HOME_BASE_WP
    if home_era is not None and away_era is not None:
        p += (away_era - home_era) * PITCHER_ERA_WEIGHT
    p += (home_wpct - away_wpct) * RECORD_WPCT_WEIGHT
    p = max(0.05, min(0.95, p))

    favorite = "home" if p >= 0.5 else "away"
    fav_p = p if p >= 0.5 else 1.0 - p
    if fav_p >= HIGH_CONF_THRESHOLD:
        conf = "HIGH"
    elif fav_p >= MEDIUM_CONF_THRESHOLD:
        conf = "MEDIUM"
    else:
        conf = "SKIP"

    fav_era = home_era if favorite == "home" else away_era
    return {
        "home_prob": p,
        "favorite": favorite,
        "favorite_prob": fav_p,
        "confidence": conf,
        "home_era": home_era,
        "away_era": away_era,
        "fav_era": fav_era,
        "home_wpct": home_wpct,
        "away_wpct": away_wpct,
        "wpct_diff": abs(home_wpct - away_wpct),
    }


def run_backtest_as_of(
    games: list[Game],
    gamelogs_by_pid: dict[int, list[dict[str, Any]]],
    standings_by_date: dict[str, dict[int, dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    skipped = {"low_conf": 0, "no_pitcher_data": 0, "no_record": 0}
    for g in games:
        as_of = _prev_day(g.date)
        team_records = standings_by_date.get(as_of, {})
        if not team_records or g.home_team_id not in team_records or g.away_team_id not in team_records:
            skipped["no_record"] += 1
            continue
        home_era = (
            era_as_of(gamelogs_by_pid.get(g.home_pitcher_id, []), as_of)
            if g.home_pitcher_id else None
        )
        away_era = (
            era_as_of(gamelogs_by_pid.get(g.away_pitcher_id, []), as_of)
            if g.away_pitcher_id else None
        )
        if home_era is None or away_era is None:
            skipped["no_pitcher_data"] += 1
            continue
        # `predict()` takes a flat pitcher_eras dict — build one for this game
        pitcher_eras = {g.home_pitcher_id: home_era, g.away_pitcher_id: away_era}
        pred = predict(g, pitcher_eras, team_records)
        if pred["confidence"] == "SKIP":
            skipped["low_conf"] += 1
            continue
        row = asdict(g)
        row.update(pred)
        row["correct"] = pred["favorite"] == g.actual_winner
        row["as_of_date"] = as_of
        rows.append(row)
    return rows, skipped


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def _winrate(rows: list[dict[str, Any]]) -> tuple[float, int, int]:
    n = len(rows)
    if n == 0:
        return 0.0, 0, 0
    c = sum(1 for r in rows if r["correct"])
    return c / n, c, n


def _line(label: str, rows: list[dict[str, Any]], min_n: int = 0) -> str:
    wr, c, n = _winrate(rows)
    marker = " ★" if n >= max(min_n, 15) and wr >= 0.55 else ""
    return f"  {label:<14} n={n:>4}  win_rate={wr:>6.1%}  ({c}W/{n - c}L){marker}"


def report(rows: list[dict[str, Any]], skipped: dict[str, int]) -> None:
    print("=" * 78)
    print(f"KALSHI MLB BACKTEST — {len(rows)} simulated trades")
    print("=" * 78)
    print("Skipped:", skipped)
    if not rows:
        print("\nNo rows — try expanding --days or check cache freshness.")
        return

    wr, c, n = _winrate(rows)
    print(f"\nOverall: {wr:.1%} ({c}/{n}) — break-even target is 55%+")

    print("\n--- By confidence tier ---")
    for conf in ("HIGH", "MEDIUM"):
        print(_line(conf, [r for r in rows if r["confidence"] == conf]))

    print("\n--- By favored-pitcher ERA tier ---")
    for tier in ("elite", "good", "average", "below_avg", "bad"):
        print(_line(tier, [r for r in rows if era_tier(r["fav_era"]) == tier]))

    print("\n--- By favorite side ---")
    for side in ("home", "away"):
        print(_line(side, [r for r in rows if r["favorite"] == side]))

    print("\n--- By |home_wpct - away_wpct| ---")
    for lo, hi, label in WPCT_BUCKETS:
        print(_line(label, [r for r in rows if lo <= r["wpct_diff"] < hi]))

    print("\n--- 55%+ win-rate combos (n >= 15, ranked by Wilson 95% lower bound) ---")
    print("    Wilson_lo > 0.50 = cohort is statistically distinguishable from coin-flip.")
    found: list[tuple[float, float, int, int, str, str, str]] = []
    for tier in ("elite", "good", "average", "below_avg", "bad"):
        for side in ("home", "away"):
            for lo, hi, label in WPCT_BUCKETS:
                subset = [
                    r for r in rows
                    if era_tier(r["fav_era"]) == tier
                    and r["favorite"] == side
                    and lo <= r["wpct_diff"] < hi
                ]
                wr, c, n = _winrate(subset)
                if n >= 15 and wr >= 0.55:
                    wlo = _wilson_lower(c, n)
                    found.append((wlo, wr, n, c, tier, side, label))
    if not found:
        print("  (none — try a longer window, different weights, or relax to 53%)")
    else:
        for wlo, wr, n, c, tier, side, label in sorted(found, reverse=True):
            stable = " ✓stable" if wlo > 0.50 else ""
            print(
                f"  fav-pitcher={tier:<10} side={side:<5} wpct_diff={label:<8} "
                f"n={n:>4} win_rate={wr:.1%} ({c}/{n}) wilson_lo={wlo:.1%}{stable}"
            )

    print("\n--- Current heuristic weights (edit constants at top of file to iterate) ---")
    print(f"  HOME_BASE_WP            = {HOME_BASE_WP}")
    print(f"  PITCHER_ERA_WEIGHT      = {PITCHER_ERA_WEIGHT}  (WP per ERA-point diff)")
    print(f"  RECORD_WPCT_WEIGHT      = {RECORD_WPCT_WEIGHT}  (WP per wpct diff)")
    print(f"  HIGH_CONF_THRESHOLD     = {HIGH_CONF_THRESHOLD}")
    print(f"  MEDIUM_CONF_THRESHOLD   = {MEDIUM_CONF_THRESHOLD}")
    print()
    print("v2 caveats — what's still imperfect:")
    print("  - Pitcher gameLog `era` is cumulative season ERA, not rolling form.")
    print("    A pitcher who's been hot vs cold lately looks identical.")
    print("  - No market-price feed → directional accuracy only. Live strategy")
    print("    needs accuracy > implied market probability, not just > 50%.")
    print("  - Heuristic ignores injuries, lineup news, weather, bullpen depth.")
    print("    Vegas closing lines clear ~57% on this feature set + more — that")
    print("    is the realistic ceiling for any pure-features-based agent.")
    print("=" * 78)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--season", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    if args.no_cache and CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=args.days)

    print(f"Fetching schedule {start} → {end}...", flush=True)
    games = fetch_schedule(start.isoformat(), end.isoformat())
    print(f"  {len(games)} finished games loaded")

    as_of_dates = sorted({_prev_day(g.date) for g in games})
    print(
        f"Fetching as-of standings for {len(as_of_dates)} unique game dates "
        f"(season {args.season}, cached on disk)...",
        flush=True,
    )
    standings_by_date: dict[str, dict[int, dict[str, Any]]] = {}
    for i, d in enumerate(as_of_dates, 1):
        standings_by_date[d] = fetch_team_records_as_of(args.season, d)
        if i % 15 == 0:
            print(f"  {i}/{len(as_of_dates)}...", flush=True)
    print(f"  done ({sum(1 for v in standings_by_date.values() if v)} dates with data)")

    pitcher_ids = {
        pid for g in games
        for pid in (g.home_pitcher_id, g.away_pitcher_id)
        if pid
    }
    print(
        f"Fetching gameLog for {len(pitcher_ids)} unique pitchers (cached on disk)...",
        flush=True,
    )
    gamelogs_by_pid: dict[int, list[dict[str, Any]]] = {}
    for i, pid in enumerate(sorted(pitcher_ids), 1):
        gamelogs_by_pid[pid] = fetch_pitcher_gamelog(pid, args.season)
        if i % 25 == 0:
            print(f"  {i}/{len(pitcher_ids)}...", flush=True)
    with_data = sum(1 for v in gamelogs_by_pid.values() if v)
    print(f"  done ({with_data} pitchers with gameLog entries)")

    rows, skipped = run_backtest_as_of(games, gamelogs_by_pid, standings_by_date)
    print()
    report(rows, skipped)


if __name__ == "__main__":
    main()
