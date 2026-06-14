#!/usr/bin/env python3
"""Kalshi ATP/WTA tennis backtester — Wimbledon-ready (June 30 cutoff).

DATA — hybrid by design (decision made during recon):
  - Historical backtest: Jeff Sackmann tennis_atp / tennis_wta match CSVs.
    Each row already carries surface, both players' rank-at-match, IDs,
    winner, tournament metadata. No name-matching, no surface lookup
    table, no atp/wta scoreboard mixing pain.
  - Live picks (Wimbledon match-day): ESPN tennis scoreboard. Sackmann's
    repo updates aren't real-time during a slam — ESPN is. The live
    client lives at the bottom; backtest never touches it.

FEATURES (all leak-free — built only from matches with tourney_date <
current match's tourney_date):

  1. Ranking differential — log(rank_low / rank_high). Log because the
     jump from #5 → #50 is far more meaningful than #105 → #150.

  2. Recent form — last 5 prior matches per player, win rate. Cross-
     surface (small sample on a single surface is usually too noisy).

  3. Surface specialist edge — player's win rate on THIS surface minus
     their overall win rate, computed over prior matches. Captures clay
     courters / grass specialists / hardcourt grinders relatively, so
     we don't double-count overall quality (already in ranking).

  4. Head-to-head — share of prior meetings won by player A (higher-
     ranked). Only applied when both players have at least 2 priors.

PREDICTOR
  P(higher-ranked wins) = 0.5
    + clip(log(rank_low/rank_high) * RANK_WEIGHT, ±0.40)
    + (formA - formB) * FORM_WEIGHT
    + (surfA_edge - surfB_edge) * SURFACE_WEIGHT
    + h2h_signed * H2H_WEIGHT
  Clipped to [0.05, 0.95]. Confidence tiers: HIGH ≥ 0.65, MEDIUM ≥ 0.55,
  SKIP otherwise.

NAIVE BASELINE
  "Always pick higher-ranked" — tennis's version of "always pick home
  team". Historically clears ~65% on tour-level main draws. Our model
  has to beat THAT, not 50%, to earn any edge over a market that prices
  rankings in trivially.

REPORT
  - Overall win rate vs naive baseline
  - By surface (clay / grass / hard / carpet)
  - By ranking gap bucket (the threshold sweep)
  - By confidence tier
  - Best-performing cohorts with Wilson 95% lower bound — only Wilson_lo
    > naive baseline counts as real signal
  - Break-even Kalshi YES price + ROI table per cohort

CAVEATS — read these before trusting numbers:
  - tourney_date is start-of-tournament, not match-day. Features computed
    "as of" tourney_date treat all matches in the same event as having
    no prior knowledge of each other. A SF prediction can't see QF
    results from the same tournament. Conservative but slightly stale
    for round-by-round form.
  - Sackmann's match files don't include withdrawals/walkovers explicitly
    — we drop rows where score contains 'W/O', 'DEF', 'RET' to avoid
    confusing the form/H2H features with non-match outcomes.
  - Surface count for "specialist edge" needs MIN_SURFACE_PRIORS prior
    matches on that surface; players new to a surface get None and skip
    the surface adjustment entirely (not a neutral 0).

Run
    python backtest_kalshi_tennis.py                  # 180d, both tours
    python backtest_kalshi_tennis.py --days 90 --tour atp
    python backtest_kalshi_tennis.py --no-cache       # force fresh CSVs
    python backtest_kalshi_tennis.py --live-espn      # show today's
                                                     # ATP/WTA matches
                                                     # with model picks
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import shutil
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import requests

CACHE_DIR = Path.home() / ".cache" / "backtest_kalshi_tennis"
SACKMANN_ATP = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
SACKMANN_WTA = "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master"
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/tennis"

# ---------------------------------------------------------------------------
# Tunable predictor weights (edit and re-run)
# ---------------------------------------------------------------------------
RANK_WEIGHT = 0.10            # WP swing per unit of log(rank_low/rank_high)
RANK_CAP = 0.40               # max abs ranking contribution
FORM_WEIGHT = 0.12            # WP swing per 1.0 of form-rate diff
SURFACE_WEIGHT = 0.20         # WP swing per 1.0 of surface-edge diff
H2H_WEIGHT = 0.06             # WP swing per 1.0 of signed H2H share
HIGH_CONF_THRESHOLD = 0.65
MEDIUM_CONF_THRESHOLD = 0.55

FORM_K = 5
MIN_FORM_MATCHES = FORM_K     # require full window — no partial form
MIN_OVERALL_PRIORS = 10       # before computing surface_edge
MIN_SURFACE_PRIORS = 5        # surface-specific sample for edge calc
MIN_H2H = 2                   # before using H2H feature

RANK_GAP_BUCKETS = [
    (0, 5, "0-5"),
    (5, 15, "5-15"),
    (15, 35, "15-35"),
    (35, 75, "35-75"),
    (75, 150, "75-150"),
    (150, 10_000, "150+"),
]

SURFACES = ("Hard", "Clay", "Grass", "Carpet")

BREAKEVEN_PRICES_CENTS = (40, 50, 55, 60, 65, 70, 75, 80)


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------
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


def _winrate(rows: list[dict]) -> tuple[float, int, int]:
    n = len(rows)
    if n == 0:
        return 0.0, 0, 0
    c = sum(1 for r in rows if r["correct"])
    return c / n, c, n


# ---------------------------------------------------------------------------
# Cache + Sackmann fetcher
# ---------------------------------------------------------------------------
def _cache_path(name: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / name


def _cached_text(name: str, url: str, ttl_s: int = 86400) -> str:
    path = _cache_path(name)
    if path.exists() and (time.time() - path.stat().st_mtime) < ttl_s:
        return path.read_text()
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    path.write_text(r.text)
    return r.text


def _cached_json(name: str, url: str, ttl_s: int = 3600) -> dict:
    path = _cache_path(name)
    if path.exists() and (time.time() - path.stat().st_mtime) < ttl_s:
        return json.loads(path.read_text())
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    path.write_text(json.dumps(data))
    return data


# ---------------------------------------------------------------------------
# Sackmann match data
# ---------------------------------------------------------------------------
@dataclass
class Match:
    tour: str             # "atp" or "wta"
    date: str             # YYYY-MM-DD (parsed from tourney_date YYYYMMDD)
    tourney_name: str
    surface: str
    round: str
    best_of: int
    winner_id: int
    winner_name: str
    winner_rank: int
    loser_id: int
    loser_name: str
    loser_rank: int


def _parse_int(s: str) -> Optional[int]:
    try:
        return int(s) if s and s != "" else None
    except ValueError:
        return None


def _is_complete_score(score: str) -> bool:
    """Drop walkovers, retirements, and defaults — those rows aren't
    meaningful 'match outcomes' for form/H2H purposes."""
    if not score:
        return False
    s = score.upper()
    return not any(tok in s for tok in ("W/O", "WO", "RET", "DEF", "ABN"))


def load_sackmann_matches(tour: str, years: list[int]) -> list[Match]:
    """Load and parse Sackmann match CSVs for given tour + years. Filters
    to rows with both players ranked + a completed score line."""
    base = SACKMANN_ATP if tour == "atp" else SACKMANN_WTA
    prefix = f"{tour}_matches"
    out: list[Match] = []
    for year in years:
        url = f"{base}/{prefix}_{year}.csv"
        cache = f"{prefix}_{year}.csv"
        try:
            text = _cached_text(cache, url)
        except requests.HTTPError:
            # Year file may not exist yet (e.g. requested year is future)
            continue
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            wr = _parse_int(row.get("winner_rank", ""))
            lr = _parse_int(row.get("loser_rank", ""))
            wid = _parse_int(row.get("winner_id", ""))
            lid = _parse_int(row.get("loser_id", ""))
            d = row.get("tourney_date", "")
            if not (wr and lr and wid and lid and len(d) == 8):
                continue
            if not _is_complete_score(row.get("score", "")):
                continue
            try:
                date_iso = (
                    f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                )
                datetime.strptime(date_iso, "%Y-%m-%d")  # validate
            except ValueError:
                continue
            best_of = _parse_int(row.get("best_of", "")) or 3
            out.append(Match(
                tour=tour,
                date=date_iso,
                tourney_name=row.get("tourney_name", ""),
                surface=row.get("surface", "Unknown").strip() or "Unknown",
                round=row.get("round", ""),
                best_of=best_of,
                winner_id=wid,
                winner_name=row.get("winner_name", ""),
                winner_rank=wr,
                loser_id=lid,
                loser_name=row.get("loser_name", ""),
                loser_rank=lr,
            ))
    return out


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------
@dataclass
class PlayerMatchHist:
    """One row in a player's match history. Surface and opponent kept
    for the surface-edge and H2H feature builders."""
    date: str
    surface: str
    opponent_id: int
    won: bool


def build_player_histories(matches: list[Match]) -> dict[int, list[PlayerMatchHist]]:
    """Per-player ascending-by-date list of {date, surface, opponent, won}.
    Both winner and loser get an entry per match."""
    h: dict[int, list[PlayerMatchHist]] = defaultdict(list)
    for m in matches:
        h[m.winner_id].append(PlayerMatchHist(
            date=m.date, surface=m.surface,
            opponent_id=m.loser_id, won=True,
        ))
        h[m.loser_id].append(PlayerMatchHist(
            date=m.date, surface=m.surface,
            opponent_id=m.winner_id, won=False,
        ))
    for pid in h:
        h[pid].sort(key=lambda x: x.date)
    return h


def build_h2h(matches: list[Match]) -> dict[tuple[int, int], list[tuple[str, int]]]:
    """{(min_id, max_id): [(date, winner_id), ...]} sorted asc by date.
    Symmetric key keeps lookups O(1) regardless of who's player A."""
    h2h: dict[tuple[int, int], list[tuple[str, int]]] = defaultdict(list)
    for m in matches:
        key = (min(m.winner_id, m.loser_id), max(m.winner_id, m.loser_id))
        h2h[key].append((m.date, m.winner_id))
    for key in h2h:
        h2h[key].sort(key=lambda x: x[0])
    return h2h


def recent_form(history: list[PlayerMatchHist], as_of: str, k: int = FORM_K) -> Optional[float]:
    """Win rate over last `k` matches strictly before as_of date.
    Returns None if fewer than k prior matches — no partial-window
    fallback (would silently weight tiny samples as if they were full)."""
    prior = [h for h in history if h.date < as_of]
    if len(prior) < k:
        return None
    last_k = prior[-k:]
    return sum(1 for h in last_k if h.won) / k


def surface_edge(
    history: list[PlayerMatchHist], as_of: str, surface: str,
    min_overall: int = MIN_OVERALL_PRIORS, min_surf: int = MIN_SURFACE_PRIORS,
) -> Optional[float]:
    """(win-rate on `surface`) − (overall win-rate). Positive = specialist,
    negative = struggles relative to own baseline. Returns None when
    either sample is too thin to be meaningful."""
    prior = [h for h in history if h.date < as_of]
    if len(prior) < min_overall:
        return None
    surf = [h for h in prior if h.surface == surface]
    if len(surf) < min_surf:
        return None
    overall_wr = sum(1 for h in prior if h.won) / len(prior)
    surf_wr = sum(1 for h in surf if h.won) / len(surf)
    return surf_wr - overall_wr


def h2h_share(
    h2h: dict[tuple[int, int], list[tuple[str, int]]],
    p_focus: int, p_other: int, as_of: str,
    min_n: int = MIN_H2H,
) -> Optional[float]:
    """Returns (p_focus prior wins / total prior meetings) − 0.5, so the
    sign matches 'does p_focus lead the H2H'. None if too few priors."""
    key = (min(p_focus, p_other), max(p_focus, p_other))
    prior = [e for e in h2h.get(key, []) if e[0] < as_of]
    if len(prior) < min_n:
        return None
    focus_wins = sum(1 for d, w in prior if w == p_focus)
    return focus_wins / len(prior) - 0.5


# ---------------------------------------------------------------------------
# Predictor
# ---------------------------------------------------------------------------
@dataclass
class Prediction:
    p_a_wins: float            # P(higher-ranked player wins)
    confidence: str            # HIGH / MEDIUM / SKIP
    rank_signal: float         # log(rank_low/rank_high), pre-weight
    form_delta: Optional[float]
    surface_delta: Optional[float]
    h2h_delta: Optional[float]


def predict_match(
    rank_a: int, rank_b: int,         # a is higher-ranked (lower number)
    form_a: Optional[float], form_b: Optional[float],
    surf_a: Optional[float], surf_b: Optional[float],
    h2h_a_share: Optional[float],
) -> Prediction:
    # rank_a < rank_b by convention (A is higher-ranked)
    rank_signal = math.log(rank_b / rank_a)
    rank_contrib = max(-RANK_CAP, min(RANK_CAP, rank_signal * RANK_WEIGHT))

    form_delta = None
    if form_a is not None and form_b is not None:
        form_delta = form_a - form_b

    surface_delta = None
    if surf_a is not None and surf_b is not None:
        surface_delta = surf_a - surf_b

    p = 0.5 + rank_contrib
    if form_delta is not None:
        p += form_delta * FORM_WEIGHT
    if surface_delta is not None:
        p += surface_delta * SURFACE_WEIGHT
    if h2h_a_share is not None:
        p += h2h_a_share * H2H_WEIGHT

    p = max(0.05, min(0.95, p))

    fav_p = max(p, 1 - p)
    if fav_p >= HIGH_CONF_THRESHOLD:
        conf = "HIGH"
    elif fav_p >= MEDIUM_CONF_THRESHOLD:
        conf = "MEDIUM"
    else:
        conf = "SKIP"

    return Prediction(
        p_a_wins=p, confidence=conf, rank_signal=rank_signal,
        form_delta=form_delta, surface_delta=surface_delta,
        h2h_delta=h2h_a_share,
    )


# ---------------------------------------------------------------------------
# Backtest loop
# ---------------------------------------------------------------------------
def run_backtest(
    matches: list[Match],
    window_start: str,
    window_end: str,
    histories: dict[int, list[PlayerMatchHist]],
    h2h: dict[tuple[int, int], list[tuple[str, int]]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """For each match whose tourney_date is inside the [start, end] window,
    compute features and a prediction. Returns (rows, skipped_counts)."""
    rows: list[dict[str, Any]] = []
    skipped = {"low_conf": 0, "no_form": 0, "out_of_window": 0}
    for m in matches:
        if not (window_start <= m.date <= window_end):
            skipped["out_of_window"] += 1
            continue
        # A = higher-ranked (lower rank number)
        if m.winner_rank < m.loser_rank:
            a_id, a_name, a_rank = m.winner_id, m.winner_name, m.winner_rank
            b_id, b_name, b_rank = m.loser_id, m.loser_name, m.loser_rank
            a_won = True
        else:
            a_id, a_name, a_rank = m.loser_id, m.loser_name, m.loser_rank
            b_id, b_name, b_rank = m.winner_id, m.winner_name, m.winner_rank
            a_won = False

        form_a = recent_form(histories.get(a_id, []), m.date)
        form_b = recent_form(histories.get(b_id, []), m.date)
        if form_a is None or form_b is None:
            skipped["no_form"] += 1
            continue

        surf_a = surface_edge(histories.get(a_id, []), m.date, m.surface)
        surf_b = surface_edge(histories.get(b_id, []), m.date, m.surface)
        h2h_a = h2h_share(h2h, a_id, b_id, m.date)

        pred = predict_match(
            a_rank, b_rank, form_a, form_b, surf_a, surf_b, h2h_a,
        )
        if pred.confidence == "SKIP":
            skipped["low_conf"] += 1
            continue

        # Predicted winner = A if p_a_wins >= 0.5 else B.
        # Naive baseline = always pick A (higher-ranked).
        model_picks_a = pred.p_a_wins >= 0.5
        rank_gap = b_rank - a_rank
        rows.append({
            "tour": m.tour,
            "date": m.date,
            "tourney": m.tourney_name,
            "round": m.round,
            "surface": m.surface,
            "a_name": a_name, "a_rank": a_rank,
            "b_name": b_name, "b_rank": b_rank,
            "rank_gap": rank_gap,
            "p_a_wins": pred.p_a_wins,
            "confidence": pred.confidence,
            "model_pick_a": model_picks_a,
            "naive_pick_a": True,
            "actual_a_won": a_won,
            "correct": model_picks_a == a_won,
            "naive_correct": a_won,
            "form_delta": pred.form_delta,
            "surface_delta": pred.surface_delta,
            "h2h_delta": pred.h2h_delta,
            "predicted_upset": not model_picks_a,
        })
    return rows, skipped


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def _line(label: str, rows: list[dict], baseline_wr: Optional[float] = None,
          min_n_star: int = 25) -> str:
    wr, c, n = _winrate(rows)
    if n == 0:
        return f"  {label:<18} n=   0  (empty)"
    wlo = _wilson_lower(c, n)
    marker = ""
    if baseline_wr is not None and n >= min_n_star and wr > baseline_wr + 0.02:
        marker = " ★"
    return (
        f"  {label:<18} n={n:>4}  win_rate={wr:>6.1%}  "
        f"({c}W/{n - c}L)  wilson_lo={wlo:>5.1%}{marker}"
    )


def _break_even_block(c: int, n: int, label: str, indent: str = "  ") -> list[str]:
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


def report(rows: list[dict], skipped: dict[str, int], tour_label: str) -> None:
    print("=" * 80)
    print(f"KALSHI TENNIS BACKTEST — {tour_label.upper()} — {len(rows)} predictions")
    print("=" * 80)
    print("Skipped:", skipped)
    if not rows:
        print("\nNo rows — try expanding --days or check Sackmann cache.")
        return

    wr, c, n = _winrate(rows)
    naive_c = sum(1 for r in rows if r["naive_correct"])
    naive_wr = naive_c / n
    print(f"\nOverall model:   {wr:.1%} ({c}/{n})")
    print(f"Naive baseline:  {naive_wr:.1%} ({naive_c}/{n})  (always-higher-ranked)")
    print(f"Lift vs naive:   {(wr - naive_wr):+.1%}")
    print(f"  Break-even target on Kalshi YES at ~50¢ is 50%+; vs naive,")
    print(f"  lift > 0 means our model adds info the rank market can't price.")

    print("\n--- By confidence tier ---")
    for conf in ("HIGH", "MEDIUM"):
        subset = [r for r in rows if r["confidence"] == conf]
        print(_line(conf, subset, baseline_wr=naive_wr))

    print("\n--- By surface ---")
    for surf in SURFACES:
        subset = [r for r in rows if r["surface"] == surf]
        if not subset:
            continue
        wr_s, c_s, n_s = _winrate(subset)
        naive_s = sum(1 for r in subset if r["naive_correct"]) / n_s if n_s else 0
        wlo = _wilson_lower(c_s, n_s)
        print(
            f"  {surf:<8} n={n_s:>4}  model={wr_s:>5.1%}  "
            f"naive={naive_s:>5.1%}  lift={wr_s - naive_s:+5.1%}  "
            f"wilson_lo={wlo:>5.1%}"
        )

    print("\n--- By ranking gap (the 'threshold sweep') ---")
    print("  Higher-ranked-wins rate by gap = how predictive ranking alone is.")
    print("  Model column = how OUR predictor (rank + form + surface + h2h) does.")
    for lo, hi, label in RANK_GAP_BUCKETS:
        subset = [r for r in rows if lo <= r["rank_gap"] < hi]
        if not subset:
            continue
        n_s = len(subset)
        model_c = sum(1 for r in subset if r["correct"])
        naive_c_s = sum(1 for r in subset if r["naive_correct"])
        wr_m = model_c / n_s
        wr_n = naive_c_s / n_s
        wlo_m = _wilson_lower(model_c, n_s)
        print(
            f"  gap={label:<8} n={n_s:>4}  model={wr_m:>5.1%}  "
            f"naive={wr_n:>5.1%}  lift={wr_m - wr_n:+5.1%}  "
            f"wilson_lo(model)={wlo_m:>5.1%}"
        )

    print("\n--- Predicted upsets only (model picks lower-ranked) ---")
    print("  This is where the model EARNS its lift — if our 'upset' picks")
    print("  beat the naive baseline, we have signal; if they regress to")
    print("  50% or below, the model is confused, not insightful.")
    upsets = [r for r in rows if r["predicted_upset"]]
    if upsets:
        n_u = len(upsets)
        c_u = sum(1 for r in upsets if r["correct"])
        wr_u = c_u / n_u
        wlo_u = _wilson_lower(c_u, n_u)
        print(
            f"  upset picks  n={n_u:>4}  win_rate={wr_u:>5.1%}  "
            f"({c_u}W/{n_u - c_u}L)  wilson_lo={wlo_u:>5.1%}"
        )
    else:
        print("  (model never picked an upset — weights may be too rank-heavy)")

    print("\n--- Stable cohorts (Wilson 95% lower bound > naive baseline) ---")
    print("  These are the only slices with statistically supported edge.")
    found: list[tuple[float, float, int, int, str]] = []
    cohort_specs = []
    for surf in SURFACES:
        for lo, hi, label in RANK_GAP_BUCKETS:
            cohort_specs.append((
                f"surface={surf}, gap={label}",
                [r for r in rows if r["surface"] == surf and lo <= r["rank_gap"] < hi],
            ))
    for conf in ("HIGH", "MEDIUM"):
        for surf in SURFACES:
            cohort_specs.append((
                f"conf={conf}, surface={surf}",
                [r for r in rows if r["confidence"] == conf and r["surface"] == surf],
            ))
    for name, subset in cohort_specs:
        n_s = len(subset)
        if n_s < 25:
            continue
        c_s = sum(1 for r in subset if r["correct"])
        wlo = _wilson_lower(c_s, n_s)
        naive_c_s = sum(1 for r in subset if r["naive_correct"])
        naive_wr_s = naive_c_s / n_s
        if wlo > naive_wr_s:
            found.append((wlo, c_s / n_s, c_s, n_s, name))
    if not found:
        print("  (none — at every cohort, Wilson 95% lower bound is below the")
        print("   naive baseline. Model has no statistically supported edge")
        print("   over 'just pick higher-ranked' in this window.)")
    else:
        for wlo, wr_s, c_s, n_s, name in sorted(found, reverse=True)[:15]:
            print(
                f"  {name:<32}  n={n_s:>4}  win_rate={wr_s:>5.1%}  "
                f"wilson_lo={wlo:>5.1%}"
            )

    print("\n--- Best surface for edge (lift over naive baseline) ---")
    surface_lifts: list[tuple[float, str, int, float, float]] = []
    for surf in SURFACES:
        subset = [r for r in rows if r["surface"] == surf]
        if len(subset) < 25:
            continue
        n_s = len(subset)
        wr_m = sum(1 for r in subset if r["correct"]) / n_s
        wr_n = sum(1 for r in subset if r["naive_correct"]) / n_s
        surface_lifts.append((wr_m - wr_n, surf, n_s, wr_m, wr_n))
    if surface_lifts:
        for lift, surf, n_s, wr_m, wr_n in sorted(surface_lifts, reverse=True):
            print(f"  {surf:<8} n={n_s:>4}  lift={lift:+5.1%}  "
                  f"(model={wr_m:.1%}, naive={wr_n:.1%})")
    else:
        print("  (insufficient sample on any surface)")

    print("\n--- Optimal ranking-gap threshold (n >= 30, ranked by lift) ---")
    best: list[tuple[float, str, int, float, float, float]] = []
    for lo, hi, label in RANK_GAP_BUCKETS:
        subset = [r for r in rows if lo <= r["rank_gap"] < hi]
        if len(subset) < 30:
            continue
        n_s = len(subset)
        wr_m = sum(1 for r in subset if r["correct"]) / n_s
        wr_n = sum(1 for r in subset if r["naive_correct"]) / n_s
        wlo_m = _wilson_lower(sum(1 for r in subset if r["correct"]), n_s)
        best.append((wr_m - wr_n, label, n_s, wr_m, wr_n, wlo_m))
    if best:
        best.sort(reverse=True)
        for lift, label, n_s, wr_m, wr_n, wlo in best:
            verdict = "real" if wlo > wr_n else "noise"
            print(f"  gap={label:<8} n={n_s:>4}  lift={lift:+5.1%}  "
                  f"model={wr_m:.1%}  naive={wr_n:.1%}  wilson_lo={wlo:.1%}  "
                  f"[{verdict}]")
    else:
        print("  (no bucket with n >= 30 — expand --days)")

    print("\n--- BREAK-EVEN ANALYSIS (model accuracy → Kalshi YES ROI) ---")
    print("  Profitable when YES contract priced below our win rate.")
    print("  'conservative' uses Wilson 95% lower bound.")
    print()
    for line in _break_even_block(c, n, "OVERALL (all model picks)"):
        print(line)
    print()
    for conf in ("HIGH", "MEDIUM"):
        subset = [r for r in rows if r["confidence"] == conf]
        if subset:
            c_s = sum(1 for r in subset if r["correct"])
            for line in _break_even_block(c_s, len(subset), f"{conf}-confidence"):
                print(line)
            print()

    print("--- Current heuristic weights (edit constants at top of file) ---")
    print(f"  RANK_WEIGHT             = {RANK_WEIGHT}  (per log(rank_low/rank_high))")
    print(f"  RANK_CAP                = {RANK_CAP}")
    print(f"  FORM_WEIGHT             = {FORM_WEIGHT}  (per 1.0 of form-rate diff)")
    print(f"  SURFACE_WEIGHT          = {SURFACE_WEIGHT}  (per 1.0 of surface-edge diff)")
    print(f"  H2H_WEIGHT              = {H2H_WEIGHT}  (per 1.0 of signed H2H share)")
    print(f"  HIGH_CONF_THRESHOLD     = {HIGH_CONF_THRESHOLD}")
    print(f"  MEDIUM_CONF_THRESHOLD   = {MEDIUM_CONF_THRESHOLD}")
    print()
    print("Caveats:")
    print("  - tourney_date is event start, not match-day. Same-tournament")
    print("    prior rounds are not visible to our features.")
    print("  - No market-price data → directional accuracy only; live edge")
    print("    needs accuracy > implied market probability, not just > naive.")
    print("  - Best-of-5 ATP slams behave differently than BO3 tour matches —")
    print("    consider splitting if BO5 cohort underperforms in your output.")
    print("=" * 80)


# ---------------------------------------------------------------------------
# ESPN live client (Wimbledon match-day path — NOT used by the backtest)
# ---------------------------------------------------------------------------
def _normalize_name(name: str) -> str:
    """Strip accents, lowercase, drop hyphens/dots/extra whitespace.
    Used for matching ESPN displayName to Sackmann's '<First> <Last>'
    format. Tennis names are messy; this catches Félix Auger-Aliassime
    vs Felix Auger Aliassime and similar."""
    s = unicodedata.normalize("NFD", name)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower().replace("-", " ").replace(".", " ").replace(",", " ")
    return " ".join(s.split())


def fetch_espn_scoreboard(tour: str, date_yyyymmdd: str) -> dict:
    """Live ESPN scoreboard for one tour for one date."""
    url = f"{ESPN_BASE}/{tour}/scoreboard?dates={date_yyyymmdd}"
    return _cached_json(f"espn_{tour}_{date_yyyymmdd}.json", url, ttl_s=1800)


def _surface_for_event(event_name: str, venue: dict) -> Optional[str]:
    """Tournament name → surface lookup for the live path. Sackmann's
    historical data has surface in-row; ESPN doesn't, so we fall back to
    a known-tournament map. Returns None for unknown events — caller
    must skip the surface feature rather than guess."""
    n = (event_name or "").lower()
    if any(s in n for s in ("wimbledon", "queen", "halle", "stuttgart", "eastbourne", "mallorca", "newport", "s-hertogenbosch", "hertogenbosch")):
        return "Grass"
    if any(s in n for s in ("roland garros", "french open", "monte carlo", "madrid", "rome", "barcelona", "houston", "estoril", "munich")):
        return "Clay"
    if "australian open" in n or "us open" in n or "indian wells" in n or "miami" in n:
        return "Hard"
    return None  # tour-level event we don't have mapped


def predict_live_from_espn(
    tour: str, date_yyyymmdd: str,
    rankings_by_name: dict[str, int],
    histories_by_name: dict[str, list[PlayerMatchHist]],
    h2h_by_name_pair: dict[tuple[str, str], list[tuple[str, int]]],
) -> list[dict]:
    """For every scheduled or upcoming singles match on a given date,
    produce a model prediction. Skips matches where either player is
    unrankable or unmatched in Sackmann name index."""
    data = fetch_espn_scoreboard(tour, date_yyyymmdd)
    today = f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"
    out: list[dict] = []
    for event in data.get("events", []):
        ev_name = event.get("name", "")
        surface = _surface_for_event(ev_name, event.get("venue", {}))
        for grouping in event.get("groupings", []):
            for comp in grouping.get("competitions", []):
                slug = (comp.get("type") or {}).get("slug", "")
                if slug not in ("mens-singles", "womens-singles"):
                    continue
                comps = comp.get("competitors", [])
                if len(comps) != 2:
                    continue
                names = [_normalize_name(
                    (c.get("athlete") or {}).get("fullName", "")
                ) for c in comps]
                ranks = [rankings_by_name.get(n) for n in names]
                if not all(ranks):
                    continue
                # A = higher-ranked
                if ranks[0] < ranks[1]:
                    a_idx, b_idx = 0, 1
                else:
                    a_idx, b_idx = 1, 0
                a_name, b_name = names[a_idx], names[b_idx]
                a_rank, b_rank = ranks[a_idx], ranks[b_idx]
                form_a = recent_form(histories_by_name.get(a_name, []), today)
                form_b = recent_form(histories_by_name.get(b_name, []), today)
                if form_a is None or form_b is None:
                    continue
                surf_a = (
                    surface_edge(histories_by_name.get(a_name, []), today, surface)
                    if surface else None
                )
                surf_b = (
                    surface_edge(histories_by_name.get(b_name, []), today, surface)
                    if surface else None
                )
                pair_key = (min(a_name, b_name), max(a_name, b_name))
                h2h_prior = [
                    e for e in h2h_by_name_pair.get(pair_key, [])
                    if e[0] < today
                ]
                h2h_a = None
                if len(h2h_prior) >= MIN_H2H:
                    a_wins = sum(1 for d, w in h2h_prior if w == a_name)
                    h2h_a = a_wins / len(h2h_prior) - 0.5
                pred = predict_match(
                    a_rank, b_rank, form_a, form_b, surf_a, surf_b, h2h_a,
                )
                out.append({
                    "tour": tour,
                    "event": ev_name,
                    "surface": surface or "Unknown",
                    "round": (comp.get("round") or {}).get("displayName", ""),
                    "a_name": comps[a_idx].get("athlete", {}).get("fullName"),
                    "b_name": comps[b_idx].get("athlete", {}).get("fullName"),
                    "a_rank": a_rank, "b_rank": b_rank,
                    "p_a_wins": pred.p_a_wins,
                    "confidence": pred.confidence,
                    "model_pick": (
                        comps[a_idx].get("athlete", {}).get("fullName")
                        if pred.p_a_wins >= 0.5 else
                        comps[b_idx].get("athlete", {}).get("fullName")
                    ),
                })
    return out


def _build_name_indexed_features(matches: list[Match]) -> tuple[
    dict[str, int],
    dict[str, list[PlayerMatchHist]],
    dict[tuple[str, str], list[tuple[str, int]]],
]:
    """For the live path: most-recent rank per (normalized) player name +
    histories keyed by normalized name + name-pair H2H. Sackmann player
    IDs aren't reachable from ESPN, so name is our only join."""
    rank_by_name: dict[str, int] = {}
    rank_date_by_name: dict[str, str] = {}
    hist_by_name: dict[str, list[PlayerMatchHist]] = defaultdict(list)
    h2h_by_pair: dict[tuple[str, str], list[tuple[str, int]]] = defaultdict(list)
    for m in matches:
        wn = _normalize_name(m.winner_name)
        ln = _normalize_name(m.loser_name)
        for nm, rk in ((wn, m.winner_rank), (ln, m.loser_rank)):
            if nm not in rank_date_by_name or m.date > rank_date_by_name[nm]:
                rank_by_name[nm] = rk
                rank_date_by_name[nm] = m.date
        hist_by_name[wn].append(PlayerMatchHist(
            date=m.date, surface=m.surface, opponent_id=0, won=True,
        ))
        hist_by_name[ln].append(PlayerMatchHist(
            date=m.date, surface=m.surface, opponent_id=0, won=False,
        ))
        key = (min(wn, ln), max(wn, ln))
        h2h_by_pair[key].append((m.date, wn))
    for nm in hist_by_name:
        hist_by_name[nm].sort(key=lambda x: x.date)
    for k in h2h_by_pair:
        h2h_by_pair[k].sort(key=lambda x: x[0])
    return rank_by_name, hist_by_name, h2h_by_pair


def run_live_espn(tours: list[str], matches: list[Match]) -> None:
    rank_by_name, hist_by_name, h2h_by_pair = _build_name_indexed_features(matches)
    today_dt = datetime.now(timezone.utc).date()
    date_str = today_dt.strftime("%Y%m%d")
    print("=" * 80)
    print(f"LIVE ESPN PICKS — {today_dt.isoformat()}")
    print("=" * 80)
    any_picks = False
    for tour in tours:
        preds = predict_live_from_espn(
            tour, date_str, rank_by_name, hist_by_name, h2h_by_pair,
        )
        if not preds:
            continue
        any_picks = True
        print(f"\n--- {tour.upper()} ({len(preds)} matches) ---")
        for p in preds:
            mark = "★" if p["confidence"] == "HIGH" else " "
            print(
                f"{mark} {p['event'][:25]:<25}  {p['surface']:<6} {p['round'][:8]:<8}  "
                f"{p['a_name'][:18]:<18}(#{p['a_rank']:<3}) vs "
                f"{p['b_name'][:18]:<18}(#{p['b_rank']:<3})  "
                f"P(A)={p['p_a_wins']:.0%}  {p['confidence']:<6}  → {p['model_pick']}"
            )
    if not any_picks:
        print("\nNo matches found (or all skipped due to missing rank/form).")
        print("Sackmann match files lag a slam by a few days — early-round")
        print("picks may have form gaps until the repo catches up.")
    print("=" * 80)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=180,
                        help="Backtest window in days (default 180)")
    parser.add_argument("--tour", choices=("atp", "wta", "both"), default="both")
    parser.add_argument("--no-cache", action="store_true",
                        help="Wipe cache before run (forces fresh CSVs)")
    parser.add_argument("--live-espn", action="store_true",
                        help="Skip backtest; show today's ESPN matches with model picks")
    args = parser.parse_args()

    if args.no_cache and CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)

    end_dt = datetime.now(timezone.utc).date()
    start_dt = end_dt - timedelta(days=args.days)
    # History feature lookback: need the year prior for form/surface/H2H.
    # Pull current + prior year regardless of window size.
    years_to_load = sorted({start_dt.year - 1, start_dt.year, end_dt.year})
    tours = ["atp", "wta"] if args.tour == "both" else [args.tour]

    print(f"Window: {start_dt} → {end_dt}  ({args.days} days)")
    print(f"Years loaded for history: {years_to_load}")
    print(f"Tours: {', '.join(t.upper() for t in tours)}")

    all_matches: dict[str, list[Match]] = {}
    for tour in tours:
        print(f"\nLoading {tour.upper()} matches from Sackmann (cached on disk)...", flush=True)
        ms = load_sackmann_matches(tour, years_to_load)
        ms.sort(key=lambda m: m.date)
        print(f"  {len(ms)} matches across {years_to_load}")
        all_matches[tour] = ms

    if args.live_espn:
        # For the live path we want both tours' histories joined into one
        # name-indexed feature set, regardless of --tour, so a Wimbledon
        # day shows ATP and WTA together.
        combined = []
        for tour in ("atp", "wta"):
            combined.extend(load_sackmann_matches(tour, years_to_load))
        combined.sort(key=lambda m: m.date)
        run_live_espn(tours, combined)
        return

    for tour in tours:
        ms = all_matches[tour]
        histories = build_player_histories(ms)
        h2h = build_h2h(ms)
        rows, skipped = run_backtest(
            ms, start_dt.isoformat(), end_dt.isoformat(), histories, h2h,
        )
        print()
        report(rows, skipped, tour)


if __name__ == "__main__":
    main()
