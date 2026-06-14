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

v3 CHANGES — rolling-window ERA replaces season cumulative:
  - rolling_era_as_of() returns IP-weighted ERA over the pitcher's last
    ROLLING_WINDOW (default 3) starts on-or-before the as-of date.
    Captures hot/cold form: a pitcher shelled in his last 3 looks
    materially worse than one steady, even if season cumulative is
    similar.
  - Games where the pitcher has fewer than ROLLING_WINDOW prior starts
    are skipped (no fallback to season cumulative). Costs sample size,
    especially early-season, but keeps the answer to "does rolling
    form predict outcomes" uncontaminated.
  - gameLog cache filename versioned to `_v2` (now stores per-game IP
    and ER, not just cumulative ERA). Old caches are bypassed.

REMAINING limitations (honest):
  - Still a heuristic. Claude has access to context (injuries, lineup
    news, weather, bullpen) that closed-form features can't capture.
  - No market-price data — we measure directional accuracy, not edge.
    A real strategy needs accuracy > implied market probability, not
    just > 50%.
  - Rolling window is starts-based, not days-based. A pitcher who
    started 4 weeks ago looks the same as one who started 4 days ago,
    as long as both have 3 prior starts.

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

ROLLING_WINDOW = 3  # last-N starts for rolling ERA


def _prev_day(d: str) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()


def _parse_ip(s: Any) -> float:
    """MLB API encodes innings as decimals where the tenths digit is
    outs-recorded (5.1 = 5⅓, 5.2 = 5⅔). Convert to real innings."""
    if s is None:
        return 0.0
    try:
        f = float(s)
    except (TypeError, ValueError):
        return 0.0
    whole = int(f)
    tenths = round((f - whole) * 10)
    return whole + tenths / 3.0


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


def _wilson_upper(c: int, n: int, z: float = 1.96) -> float:
    """Wilson 95% upper bound — paired with _wilson_lower for the full CI."""
    if n == 0:
        return 1.0
    p = c / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return (centre + margin) / denom


# Kalshi YES-contract prices (cents) where we sample expected ROI in the
# break-even table. Bracketed to cover typical MLB game-winner pricing for
# moderate (~55¢) through strong (~80¢) favorites.
BREAKEVEN_PRICES_CENTS = (40, 50, 55, 60, 65, 70, 75, 80)


def _break_even_block(c: int, n: int, label: str, indent: str = "  ") -> list[str]:
    """For a cohort of n bets with c wins, format a break-even & ROI table
    answering 'at what Kalshi price does this cohort net positive ROI?'.

    Pure arithmetic: for a Kalshi YES contract at price m (cents/100), one
    dollar wagered pays (1-m)/m on a win and -1 on a loss. Expected ROI
    per dollar bet = (p - m) / m where p is our directional accuracy.

    Two columns: 'raw' uses point-estimate p, 'conservative' uses the
    Wilson 95% lower bound (so positive ROI at the conservative number
    means we're 95% confident the true ROI is positive at that price)."""
    if n == 0:
        return [f"{indent}{label}: empty"]
    p_raw = c / n
    p_wlo = _wilson_lower(c, n)
    p_wup = _wilson_upper(c, n)
    lines = [
        f"{indent}{label}",
        f"{indent}  sample: n={n}  wins={c}  win_rate={p_raw:.1%}  "
        f"Wilson95%=[{p_wlo:.1%}, {p_wup:.1%}]",
        f"{indent}  break-even Kalshi YES price:  "
        f"{round(p_raw * 100)}¢ raw / {round(p_wlo * 100)}¢ conservative",
    ]
    # ROI rows: emit a compact two-line table.
    raw_cells = []
    cons_cells = []
    for m_c in BREAKEVEN_PRICES_CENTS:
        m = m_c / 100.0
        raw = (p_raw - m) / m
        cons = (p_wlo - m) / m
        raw_cells.append(f"{m_c:>3d}¢:{raw:>+7.1%}")
        cons_cells.append(f"{m_c:>3d}¢:{cons:>+7.1%}")
    lines.append(f"{indent}  ROI per $1 (raw):          " + "  ".join(raw_cells))
    lines.append(f"{indent}  ROI per $1 (conservative): " + "  ".join(cons_cells))
    return lines


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
    {date, era, ip, er} entries where:
      - era is cumulative season ERA after that game
      - ip is innings pitched in that single game (decimal — 5⅓ = 5.333)
      - er is earned runs in that single game
    Enables both cumulative and rolling-window ERA lookups.

    Cache filename is versioned (_v2) so older caches without ip/er
    are bypassed and re-fetched on first run."""
    if pid is None:
        return []
    path = _cache_path(f"gamelog_{pid}_{season}_v2.json")
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
                stat = split.get("stat", {})
                era_str = stat.get("era")
                ip_str = stat.get("inningsPitched")
                er = stat.get("earnedRuns")
                if not d or era_str in (None, "-.--") or ip_str is None or er is None:
                    continue
                try:
                    entries.append({
                        "date": d,
                        "era": float(era_str),
                        "ip": _parse_ip(ip_str),
                        "er": int(er),
                    })
                except (ValueError, TypeError):
                    continue
        entries.sort(key=lambda e: e["date"])
    except (requests.RequestException, ValueError, KeyError):
        entries = []
    path.write_text(json.dumps(entries))
    return entries


def rolling_era_as_of(
    gamelog: list[dict[str, Any]],
    as_of_date: str,
    window: int = ROLLING_WINDOW,
) -> float | None:
    """IP-weighted ERA over the pitcher's last `window` starts on-or-before
    `as_of_date`. Computed as (sum_ER * 9) / sum_IP — the standard ERA
    formula applied to the windowed slice, not a naive arithmetic mean
    of game-level ERAs (which would let one 1-IP blowup dominate two
    7-IP gems).

    Returns None if the pitcher has fewer than `window` prior starts —
    no fallback to season cumulative ERA, because that would contaminate
    the answer to 'does rolling form predict outcomes given enough data'.
    """
    prior = [e for e in gamelog if e["date"] <= as_of_date]
    if len(prior) < window:
        return None
    recent = prior[-window:]
    total_ip = sum(e["ip"] for e in recent)
    total_er = sum(e["er"] for e in recent)
    if total_ip <= 0:
        return None
    return (total_er * 9.0) / total_ip


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
            rolling_era_as_of(gamelogs_by_pid.get(g.home_pitcher_id, []), as_of)
            if g.home_pitcher_id else None
        )
        away_era = (
            rolling_era_as_of(gamelogs_by_pid.get(g.away_pitcher_id, []), as_of)
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

    # ── Break-even analysis ────────────────────────────────────────────
    # For each meaningful cohort, show the Kalshi YES price at which our
    # directional accuracy stops beating the market. A 65% cohort breaks
    # even at 65¢; below that we profit, above we lose. Conservative
    # column uses Wilson 95% lower bound for a stricter "are we 95%
    # confident this still pays" test.
    print("\n--- BREAK-EVEN ANALYSIS (directional accuracy → expected ROI vs Kalshi YES price) ---")
    print("    Profitable when Kalshi prices the YES contract below our win rate.")
    print("    'conservative' uses Wilson 95% lower bound — positive there means we're")
    print("    95% confident the live ROI is positive at that price.")
    print()

    c_all = sum(1 for r in rows if r["correct"])
    for line in _break_even_block(c_all, len(rows), "OVERALL (all decided picks)"):
        print(line)
    print()
    for conf in ("HIGH", "MEDIUM"):
        subset = [r for r in rows if r["confidence"] == conf]
        if not subset:
            continue
        c_s = sum(1 for r in subset if r["correct"])
        for line in _break_even_block(c_s, len(subset), f"{conf}-confidence picks"):
            print(line)
        print()

    # Stable combos only — same Wilson > 50% filter as the combo finder
    print("    Per-cohort break-even (Wilson > 50% combos only):")
    any_stable = False
    for tier in ("elite", "good", "average", "below_avg", "bad"):
        for side in ("home", "away"):
            for lo, hi, label in WPCT_BUCKETS:
                subset = [
                    r for r in rows
                    if era_tier(r["fav_era"]) == tier
                    and r["favorite"] == side
                    and lo <= r["wpct_diff"] < hi
                ]
                if len(subset) < 15:
                    continue
                c_s = sum(1 for r in subset if r["correct"])
                if _wilson_lower(c_s, len(subset)) <= 0.50:
                    continue
                any_stable = True
                cohort = f"fav-pitcher={tier}, side={side}, wpct_diff={label}"
                for line in _break_even_block(c_s, len(subset), cohort, indent="    "):
                    print(line)
                print()
    if not any_stable:
        print("    (no Wilson-stable combo at this window — try wider date range)")
        print()

    print("--- Current heuristic weights (edit constants at top of file to iterate) ---")
    print(f"  HOME_BASE_WP            = {HOME_BASE_WP}")
    print(f"  PITCHER_ERA_WEIGHT      = {PITCHER_ERA_WEIGHT}  (WP per ERA-point diff)")
    print(f"  RECORD_WPCT_WEIGHT      = {RECORD_WPCT_WEIGHT}  (WP per wpct diff)")
    print(f"  HIGH_CONF_THRESHOLD     = {HIGH_CONF_THRESHOLD}")
    print(f"  MEDIUM_CONF_THRESHOLD   = {MEDIUM_CONF_THRESHOLD}")
    print()
    print(f"\nPitcher ERA = rolling last-{ROLLING_WINDOW} starts (IP-weighted, not naive avg).")
    print("Games where a starter has <3 prior starts as of game-date-1 are skipped.")
    print()
    print("Caveats — what's still imperfect:")
    print("  - No market-price feed → directional accuracy only. Live strategy")
    print("    needs accuracy > implied market probability, not just > 50%.")
    print("  - Heuristic ignores injuries, lineup news, weather, bullpen depth.")
    print("    Vegas closing lines clear ~57% on this feature set + more — that")
    print("    is the realistic ceiling for any pure-features-based agent.")
    print("=" * 78)


# ============================================================================
# TOTALS / TEAM-TOTALS BACKTEST (extension)
# ============================================================================
# Two new Kalshi series:
#   - KXMLBTOTAL: combined-runs OVER/UNDER for the full game
#   - KXMLBTEAMTOTAL: single-team runs OVER/UNDER (one ticker per team)
#
# Feature set differs from the winner predictor:
#   - Target = home_score + away_score (totals) or single-team score
#   - Predictor combines team rs_per_game / ra_per_game (derived directly
#     from the schedule, leak-free to as_of_date) with both starters'
#     rolling ERAs (reused from the existing gamelog cache)
#   - Threshold × edge-δ sweep: for each candidate line T and edge δ,
#     bet OVER if predicted >= T+δ, UNDER if <= T-δ, else SKIP
#
# Limitation honestly stated: no bullpen-ERA feature in v1. The production
# edge prompt uses bullpens[abbr].bullpen_era_15d which would need per-team
# per-as-of-date statsapi pulls (~5400 calls for 180 days × 30 teams).
# Documented in the report footer as the next improvement.

TOTAL_LINES = (7.5, 8.5, 9.5, 10.5, 11.5)
TEAM_TOTAL_LINES = (2.5, 3.5, 4.5, 5.5)
EDGE_DELTAS = (0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5)
HOME_BAT_BOOST_RUNS = 0.3
LEAGUE_AVG_ERA = 4.0       # neutral anchor for ERA adjustments
PITCHER_RUN_WEIGHT = 0.25  # runs shaved per ERA-point above neutral, per pitcher
OPP_PITCHER_RUN_WEIGHT = 0.4  # team-total: own runs shaved per ERA-point of opposing starter


def derive_team_run_stats(
    games: list[Game], as_of_date: str
) -> dict[int, dict[str, float]]:
    """Each team's runs scored / runs allowed per game over the season
    SO FAR (games whose date < as_of_date). Pure schedule arithmetic —
    no extra API, leak-free."""
    agg: dict[int, dict[str, float]] = {}
    for g in games:
        if g.date >= as_of_date:
            continue
        for tid, rs, ra in (
            (g.home_team_id, g.home_score, g.away_score),
            (g.away_team_id, g.away_score, g.home_score),
        ):
            d = agg.setdefault(tid, {"rs": 0, "ra": 0, "n": 0})
            d["rs"] += rs
            d["ra"] += ra
            d["n"] += 1
    return {
        tid: {
            "rs_per_game": d["rs"] / d["n"],
            "ra_per_game": d["ra"] / d["n"],
            "games": d["n"],
        }
        for tid, d in agg.items()
        if d["n"] > 0
    }


def predict_total(
    game: Game,
    home_era: float | None,
    away_era: float | None,
    team_stats: dict[int, dict[str, float]],
) -> float | None:
    """Predicted combined runs. Average of (own RS, opp RA) for each side,
    plus +0.3 home-batting boost, shaded by both starters' ERA delta
    from league average. Returns None if any input missing."""
    home = team_stats.get(game.home_team_id)
    away = team_stats.get(game.away_team_id)
    if not home or not away or home_era is None or away_era is None:
        return None
    home_exp = (home["rs_per_game"] + away["ra_per_game"]) / 2.0
    away_exp = (away["rs_per_game"] + home["ra_per_game"]) / 2.0
    base_total = home_exp + away_exp + HOME_BAT_BOOST_RUNS
    starter_adj = (
        (home_era - LEAGUE_AVG_ERA) + (away_era - LEAGUE_AVG_ERA)
    ) * PITCHER_RUN_WEIGHT
    return base_total + starter_adj


def predict_team_total(
    game: Game,
    side: str,
    home_era: float | None,
    away_era: float | None,
    team_stats: dict[int, dict[str, float]],
) -> float | None:
    """Predicted runs for one team. Own rs_per_game + opp ra_per_game
    averaged, +0.3 if home, shaded by opposing starter's ERA delta from
    league average. Returns None if any input missing."""
    if side == "home":
        own_tid, opp_tid, opp_era = game.home_team_id, game.away_team_id, away_era
    elif side == "away":
        own_tid, opp_tid, opp_era = game.away_team_id, game.home_team_id, home_era
    else:
        return None
    own = team_stats.get(own_tid)
    opp = team_stats.get(opp_tid)
    if not own or not opp or opp_era is None:
        return None
    base = (own["rs_per_game"] + opp["ra_per_game"]) / 2.0
    if side == "home":
        base += HOME_BAT_BOOST_RUNS
    starter_adj = (opp_era - LEAGUE_AVG_ERA) * OPP_PITCHER_RUN_WEIGHT
    return base + starter_adj


def run_total_backtest(
    games: list[Game],
    gamelogs_by_pid: dict[int, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    skipped = {"no_pitcher_data": 0, "no_team_stats": 0}
    team_stats_cache: dict[str, dict[int, dict[str, float]]] = {}
    for g in games:
        as_of = _prev_day(g.date)
        if as_of not in team_stats_cache:
            team_stats_cache[as_of] = derive_team_run_stats(games, as_of)
        team_stats = team_stats_cache[as_of]
        home_era = (
            rolling_era_as_of(gamelogs_by_pid.get(g.home_pitcher_id, []), as_of)
            if g.home_pitcher_id else None
        )
        away_era = (
            rolling_era_as_of(gamelogs_by_pid.get(g.away_pitcher_id, []), as_of)
            if g.away_pitcher_id else None
        )
        if home_era is None or away_era is None:
            skipped["no_pitcher_data"] += 1
            continue
        predicted = predict_total(g, home_era, away_era, team_stats)
        if predicted is None:
            skipped["no_team_stats"] += 1
            continue
        rows.append({
            "date": g.date,
            "home_team": g.home_team,
            "away_team": g.away_team,
            "predicted_total": predicted,
            "actual_total": g.home_score + g.away_score,
            "home_era": home_era,
            "away_era": away_era,
            "starter_avg_era": (home_era + away_era) / 2.0,
        })
    return rows, skipped


def run_team_total_backtest(
    games: list[Game],
    gamelogs_by_pid: dict[int, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    skipped = {"no_pitcher_data": 0, "no_team_stats": 0}
    team_stats_cache: dict[str, dict[int, dict[str, float]]] = {}
    for g in games:
        as_of = _prev_day(g.date)
        if as_of not in team_stats_cache:
            team_stats_cache[as_of] = derive_team_run_stats(games, as_of)
        team_stats = team_stats_cache[as_of]
        home_era = (
            rolling_era_as_of(gamelogs_by_pid.get(g.home_pitcher_id, []), as_of)
            if g.home_pitcher_id else None
        )
        away_era = (
            rolling_era_as_of(gamelogs_by_pid.get(g.away_pitcher_id, []), as_of)
            if g.away_pitcher_id else None
        )
        if home_era is None or away_era is None:
            skipped["no_pitcher_data"] += 2
            continue
        for side, actual, opp_era in (
            ("home", g.home_score, away_era),
            ("away", g.away_score, home_era),
        ):
            predicted = predict_team_total(g, side, home_era, away_era, team_stats)
            if predicted is None:
                skipped["no_team_stats"] += 1
                continue
            rows.append({
                "date": g.date,
                "team": g.home_team if side == "home" else g.away_team,
                "side": side,
                "predicted_runs": predicted,
                "actual_runs": actual,
                "opp_era": opp_era,
            })
    return rows, skipped


def _sweep_at_line(
    rows: list[dict[str, Any]],
    line_T: float,
    delta: float,
    pred_key: str,
    actual_key: str,
) -> tuple[int, int, int]:
    """Bet OVER if predicted >= T+δ, UNDER if predicted <= T-δ, else SKIP.
    Return (wins, losses, skipped). Lines are half-integer so ties never
    occur in real game scores; we still guard with `==` to be safe."""
    wins = losses = skipped = 0
    for r in rows:
        p = r[pred_key]
        a = r[actual_key]
        if a == line_T:
            continue
        if p >= line_T + delta:
            bet_over = True
        elif p <= line_T - delta:
            bet_over = False
        else:
            skipped += 1
            continue
        actual_over = a > line_T
        if bet_over == actual_over:
            wins += 1
        else:
            losses += 1
    return wins, losses, skipped


def _report_totals_like(
    rows: list[dict[str, Any]],
    skipped: dict[str, int],
    series_name: str,
    pred_key: str,
    actual_key: str,
    lines: tuple[float, ...],
    cohort_field: str,
    cohort_label: str,
    default_line: float,
    default_delta: float,
) -> None:
    """Shared report scaffold for KXMLBTOTAL and KXMLBTEAMTOTAL — only
    the threshold list, prediction field, and cohort dimension differ."""
    print("=" * 78)
    print(f"{series_name} BACKTEST — {len(rows)} simulated bets pre-filter")
    print("=" * 78)
    print("Skipped:", skipped)
    if not rows:
        print("\nNo rows — try expanding --days or rebuilding the pitcher cache.")
        return

    preds = [r[pred_key] for r in rows]
    actuals = [r[actual_key] for r in rows]
    print(
        f"\nPrediction calibration: mean_pred={sum(preds)/len(preds):.2f}  "
        f"mean_actual={sum(actuals)/len(actuals):.2f}  "
        f"(if mean_pred is systematically off, edit the weight constants)"
    )

    # ─── Naive baseline per line ───────────────────────────────────────
    # Critical sanity check: at a non-market-balanced line (e.g. 11.5 on
    # MLB totals), simply ALWAYS betting one side wins ~70% of the time
    # because that's how the run-total distribution sits. Our predictor
    # only adds value if it beats this baseline — Kalshi prices already
    # reflect the baseline, so we earn no edge from rediscovering it.
    print("\n--- Naive baselines (no prediction, just always bet one side) ---")
    baselines: dict[float, tuple[float, float]] = {}
    for T in lines:
        over_w = sum(1 for a in actuals if a > T)
        under_w = sum(1 for a in actuals if a < T)
        n_total = sum(1 for a in actuals if a != T)
        if n_total == 0:
            continue
        over_wr = over_w / n_total
        under_wr = under_w / n_total
        baselines[T] = (over_wr, under_wr)
        better = max(over_wr, under_wr)
        side = "OVER" if over_wr >= under_wr else "UNDER"
        print(
            f"  line={T:<5.1f}  always-OVER={over_wr:>5.1%}  "
            f"always-UNDER={under_wr:>5.1%}  best-trivial={better:>5.1%} ({side})"
        )
    print(
        "  Your predictor MUST beat best-trivial at each line to earn any edge"
    )
    print("  vs a Kalshi market that already reflects the run distribution.")

    print(f"\n--- Win-rate sweep: line × edge-δ (vs trivial-baseline lift) ---")
    print(f"{'line ↓ / δ →':>14}  " + "  ".join(f"δ={d:<4.2f}" for d in EDGE_DELTAS))
    print("  (cell = our win-rate; ★ = beats best-trivial baseline at that line by >=2%)")
    found: list[tuple[float, float, int, int, float, float, float]] = []
    for T in lines:
        baseline_best = max(baselines.get(T, (0.5, 0.5)))
        cells_wr: list[tuple[float, int, bool]] = []
        for d in EDGE_DELTAS:
            w, l, _ = _sweep_at_line(rows, T, d, pred_key, actual_key)
            n = w + l
            wr = (w / n) if n else 0.0
            beats = (n >= 15 and wr >= baseline_best + 0.02)
            cells_wr.append((wr, n, beats))
            if beats and wr >= 0.55:
                wlo = _wilson_lower(w, n)
                found.append((wlo, wr, w, n, T, d, baseline_best))
        wr_row = "  ".join(
            f"{wr:>5.1%}{'★' if beats else ' '}"
            for (wr, n, beats) in cells_wr
        )
        n_row = "  ".join(f"n={n:<5}" for (wr, n, _) in cells_wr)
        print(
            f"  T={T:<5.1f}base={baseline_best:>4.0%}  {wr_row}"
        )
        print(f"  {' ' * 18}{n_row}")

    print(
        "\n--- Stable combos (Wilson 95% lower bound > best-trivial baseline) ---"
    )
    print("  These are the only rows that represent real predictive signal.")
    stable = [x for x in found if x[0] > x[6]]  # wilson_lo > baseline_best
    if not stable:
        print("  (none — at every line, the predictor's Wilson 95% lower bound")
        print("   sits at or below the trivial 'always one side' baseline. This")
        print("   means we have NO statistically-supported edge in this window.)")
    else:
        for wlo, wr, w, n, T, d, base in sorted(stable, reverse=True)[:12]:
            lift = wr - base
            print(
                f"  line={T:<5.1f} δ={d:<4.2f} n={n:>4} win_rate={wr:>5.1%} "
                f"baseline={base:>5.1%} lift={lift:+5.1%} wilson_lo={wlo:>5.1%}"
            )

    print(
        f"\n--- {cohort_label} cohort at line={default_line}, δ={default_delta} ---"
    )
    cohort_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        if cohort_field == "starter_avg_era":
            key = era_tier(r["starter_avg_era"])
        elif cohort_field == "opp_era":
            key = era_tier(r["opp_era"])
        elif cohort_field == "side":
            key = r["side"]
        else:
            key = "all"
        cohort_groups[key].append(r)
    for key in ("elite", "good", "average", "below_avg", "bad", "unknown",
                "home", "away"):
        subset = cohort_groups.get(key)
        if not subset:
            continue
        w, l, _ = _sweep_at_line(
            subset, default_line, default_delta, pred_key, actual_key
        )
        n = w + l
        if n == 0:
            continue
        wr = w / n
        wlo = _wilson_lower(w, n)
        marker = " ★" if n >= 15 and wr >= 0.55 else ""
        print(
            f"  {key:<10}  n={n:>4}  win_rate={wr:>5.1%}  "
            f"({w}W/{l}L)  wilson_lo={wlo:>5.1%}{marker}"
        )

    print("\n--- Optimal threshold (highest LIFT over baseline, n >= 30) ---")
    print("  Lift = (our win_rate − best-trivial baseline at that line).")
    print("  Positive lift is the only thing that matters — high raw win-rate")
    print("  on a low/high line is just rediscovering the run distribution.")
    best: tuple[float, float, int, int, float, float, float] | None = None
    for T in lines:
        base = max(baselines.get(T, (0.5, 0.5)))
        for d in EDGE_DELTAS:
            w, l, _ = _sweep_at_line(rows, T, d, pred_key, actual_key)
            n = w + l
            if n < 30:
                continue
            wr = w / n
            lift = wr - base
            wlo = _wilson_lower(w, n)
            if best is None or lift > best[0]:
                best = (lift, wr, w, n, T, d, wlo)
    if best is None:
        print("  (no cell with n >= 30 — expand --days)")
    else:
        lift, wr, w, n, T, d, wlo = best
        if lift >= 0.02 and wlo > max(baselines.get(T, (0.5, 0.5))):
            verdict = "REAL SIGNAL"
        elif lift >= 0.02:
            verdict = "LIFT PRESENT BUT NOT STATISTICALLY ROBUST"
        else:
            verdict = "NO LIFT — predictor adds nothing the market can't price"
        print(
            f"  line={T}  δ={d}  n={n}  win_rate={wr:.1%}  "
            f"lift={lift:+.1%}  wilson_lo={wlo:.1%}  → {verdict}"
        )

    print("\nCaveats:")
    print("  - No bullpen-ERA feature in v1 (production prompt uses 15-day")
    print("    bullpen ERA + save_conv). Adding it would need ~5400 cached")
    print("    /teams/<id>/stats pulls; defer until v1 numbers justify it.")
    print("  - No market-price data → directional accuracy only.")
    print("  - PITCHER_RUN_WEIGHT, OPP_PITCHER_RUN_WEIGHT, LEAGUE_AVG_ERA")
    print("    are first-pass guesses — calibrate against mean_actual then")
    print("    re-run.")
    print("=" * 78)


def report_total(rows: list[dict[str, Any]], skipped: dict[str, int]) -> None:
    _report_totals_like(
        rows,
        skipped,
        series_name="KXMLBTOTAL",
        pred_key="predicted_total",
        actual_key="actual_total",
        lines=TOTAL_LINES,
        cohort_field="starter_avg_era",
        cohort_label="By starter-avg-ERA tier (both starters averaged)",
        default_line=9.5,
        default_delta=0.5,
    )


def report_team_total(rows: list[dict[str, Any]], skipped: dict[str, int]) -> None:
    _report_totals_like(
        rows,
        skipped,
        series_name="KXMLBTEAMTOTAL",
        pred_key="predicted_runs",
        actual_key="actual_runs",
        lines=TEAM_TOTAL_LINES,
        cohort_field="opp_era",
        cohort_label="By opposing-starter ERA tier",
        default_line=4.5,
        default_delta=0.5,
    )
    # Extra: home vs away breakdown — KXMLBTEAMTOTAL has a clear home/away
    # asymmetry (home batting boost) that's worth surfacing separately.
    print("\n--- KXMLBTEAMTOTAL home vs away batting (line=4.5, δ=0.5) ---")
    for side in ("home", "away"):
        subset = [r for r in rows if r["side"] == side]
        w, l, _ = _sweep_at_line(subset, 4.5, 0.5, "predicted_runs", "actual_runs")
        n = w + l
        if n == 0:
            print(f"  {side:<5}: empty")
            continue
        wr = w / n
        wlo = _wilson_lower(w, n)
        marker = " ★" if n >= 15 and wr >= 0.55 else ""
        print(
            f"  {side:<5}  n={n:>4}  win_rate={wr:>5.1%}  ({w}W/{l}L)  "
            f"wilson_lo={wlo:>5.1%}{marker}"
        )
    print("=" * 78)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--season", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--start", type=str, default=None, help="YYYY-MM-DD; overrides --days when paired with --end")
    parser.add_argument("--end", type=str, default=None, help="YYYY-MM-DD; overrides --days when paired with --start")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument(
        "--mode",
        choices=("winner", "total", "team-total", "all"),
        default="winner",
        help="winner (default, original game-winner backtest), total "
             "(KXMLBTOTAL), team-total (KXMLBTEAMTOTAL), or all three.",
    )
    args = parser.parse_args()

    if args.no_cache and CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)

    if args.start and args.end:
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
        end = datetime.strptime(args.end, "%Y-%m-%d").date()
    elif args.start or args.end:
        parser.error("--start and --end must be provided together")
    else:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=args.days)

    print(f"Fetching schedule {start} → {end}...", flush=True)
    games = fetch_schedule(start.isoformat(), end.isoformat())
    print(f"  {len(games)} finished games loaded")

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

    if args.mode in ("winner", "all"):
        as_of_dates = sorted({_prev_day(g.date) for g in games})
        print(
            f"\nFetching as-of standings for {len(as_of_dates)} unique game dates "
            f"(season {args.season}, cached on disk)...",
            flush=True,
        )
        standings_by_date: dict[str, dict[int, dict[str, Any]]] = {}
        for i, d in enumerate(as_of_dates, 1):
            standings_by_date[d] = fetch_team_records_as_of(args.season, d)
            if i % 15 == 0:
                print(f"  {i}/{len(as_of_dates)}...", flush=True)
        print(f"  done ({sum(1 for v in standings_by_date.values() if v)} dates with data)")
        rows, skipped = run_backtest_as_of(games, gamelogs_by_pid, standings_by_date)
        print()
        report(rows, skipped)

    if args.mode in ("total", "all"):
        print("\nRunning KXMLBTOTAL backtest...", flush=True)
        rows_t, skipped_t = run_total_backtest(games, gamelogs_by_pid)
        print()
        report_total(rows_t, skipped_t)

    if args.mode in ("team-total", "all"):
        print("\nRunning KXMLBTEAMTOTAL backtest...", flush=True)
        rows_tt, skipped_tt = run_team_total_backtest(games, gamelogs_by_pid)
        print()
        report_team_total(rows_tt, skipped_tt)


if __name__ == "__main__":
    main()
