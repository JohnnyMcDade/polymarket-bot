#!/usr/bin/env python3
"""Validate resources/grass_specialists.json against this week's actual
ATP grass-swing results from ESPN.

WHAT IT ANSWERS:
  1. Which men's-singles matches happened at Halle / Queens / Stuttgart /
     s-Hertogenbosch this week? Who won, who lost.
  2. For each match, what's the grass-specialist delta of each player
     according to our bundled JSON?
  3. Which players appeared this week but AREN'T in the 90-player list?
     (Candidate adds for next regen.)
  4. Which players ON the list lost early this week? (Potential decay
     candidates — but n=1-2 matches is far too thin to drop them.)
  5. If the production tennis filter had been live this week, which
     matches would have PASSed assuming Kalshi YES ≤ 62¢?

WHAT IT DOES NOT DO:
  - Auto-write the JSON. The JSON is built from ~380 days of career
    data; one week's results don't move the needle on a single
    player's career grass_wr − overall_wr delta. Use this script to
    surface what to investigate, then re-run the full regen against
    fresh Sackmann data when ready.
  - Use historical Kalshi prices (we don't store them). Filter
    simulation reports "would PASS if Kalshi YES ≤ 62¢" — the actual
    price gate is fact-of-trade.

Run
    python3 scripts/validate_grass_specialists.py
    python3 scripts/validate_grass_specialists.py --days-back 14
"""

from __future__ import annotations

import argparse
import json
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import kalshi_stats  # for ATP rankings via _fetch_tennis_rankings

ESPN_ATP_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard"
GRASS_JSON = ROOT / "resources" / "grass_specialists.json"

# Tournaments to capture. We use case-insensitive substring match on the
# event name so Kalshi-style "Halle" or ESPN's official "Terra Wortmann
# Open" both resolve to the same target. Halle and Queens are the user's
# primary asks; the other two grass-swing events are included because
# they share specialist signal.
GRASS_TOURNAMENT_PATTERNS = {
    "halle": ("halle", "terra wortmann"),
    "queens": ("queen's", "queens", "cinch", "hsbc championships"),
    "stuttgart": ("boss open", "stuttgart open"),
    "s-hertogenbosch": ("libéma", "libema", "s-hertogenbosch", "hertogenbosch"),
}

GRASS_MIN_DELTA_DIFF_PP = 5.0  # mirrors production filter default
TENNIS_MIN_RANK_GAP = 50
TENNIS_MAX_ASK_CENTS = 62


def _normalize_name(name: str) -> str:
    """Strip accents + hyphens + lowercase. Lets us match ESPN's
    'Félix Auger-Aliassime' to Sackmann's 'Felix Auger Aliassime'."""
    s = unicodedata.normalize("NFD", name)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower().replace("-", " ").replace(".", " ").replace(",", " ")
    return " ".join(s.split())


def fetch_grass_matches(days_back: int = 10) -> list[dict[str, Any]]:
    """Return DEDUPED list of completed men's-singles matches on grass
    during the last `days_back` days. ESPN's scoreboard for a given date
    returns the tournament's cumulative match list (not just matches
    played that day), so iterating dates produces massive duplicates —
    we key on competition.id to keep each match exactly once."""
    today = datetime.now(timezone.utc).date()
    out: dict[str, dict[str, Any]] = {}
    for offset in range(days_back):
        d = today - timedelta(days=offset)
        url = f"{ESPN_ATP_SCOREBOARD}?dates={d.strftime('%Y%m%d')}"
        try:
            data = requests.get(url, timeout=15).json()
        except Exception as e:
            print(f"[WARN] ESPN fetch for {d} failed: {e}", flush=True)
            continue
        for event in (data.get("events") or []):
            ev_name = event.get("name") or ""
            ev_name_l = ev_name.lower()
            tournament_key = next(
                (k for k, patterns in GRASS_TOURNAMENT_PATTERNS.items()
                 if any(p in ev_name_l for p in patterns)),
                None,
            )
            if tournament_key is None:
                continue
            for grp in (event.get("groupings") or []):
                for comp in (grp.get("competitions") or []):
                    comp_id = comp.get("id")
                    if not comp_id or comp_id in out:
                        continue
                    if (comp.get("type") or {}).get("slug") != "mens-singles":
                        continue
                    status = (comp.get("status") or {}).get("type") or {}
                    if not status.get("completed"):
                        continue
                    comps = comp.get("competitors") or []
                    if len(comps) != 2:
                        continue
                    winner_name = loser_name = ""
                    winner_score = loser_score = ""
                    for c in comps:
                        ath = c.get("athlete") or {}
                        nm = ath.get("displayName") or ath.get("fullName") or ""
                        sc = c.get("score") or ""
                        if c.get("winner"):
                            winner_name, winner_score = nm, sc
                        else:
                            loser_name, loser_score = nm, sc
                    if not (winner_name and loser_name):
                        continue
                    # Prefer the comp's own date (the match's date) over
                    # the query date when ESPN serves a richer payload.
                    match_date = (comp.get("date") or "")[:10] or d.isoformat()
                    out[comp_id] = {
                        "comp_id": comp_id,
                        "date": match_date,
                        "tournament_key": tournament_key,
                        "tournament_name": ev_name,
                        "round": (comp.get("round") or {}).get("displayName", ""),
                        "winner_name": winner_name,
                        "loser_name": loser_name,
                        "winner_score": winner_score,
                        "loser_score": loser_score,
                    }
    return sorted(out.values(), key=lambda m: m["date"])


def load_specialists() -> dict[str, dict[str, Any]]:
    """Return {normalized_name: {delta_pp, grass_wr, overall_wr,
    grass_n, all_n}}. Keyed by normalized name so ESPN's display
    names resolve cleanly."""
    if not GRASS_JSON.exists():
        return {}
    data = json.loads(GRASS_JSON.read_text())
    return {
        _normalize_name(name): info
        for name, info in (data.get("players") or {}).items()
    }


def load_rankings() -> dict[str, int]:
    """Fresh ATP top-150 from ESPN, indexed by normalized name."""
    rows = kalshi_stats._fetch_tennis_rankings(kalshi_stats._ESPN_ATP_RANKINGS)
    out: dict[str, int] = {}
    for r in rows:
        nm = r.get("player")
        rk = r.get("rank")
        if nm and isinstance(rk, int):
            out[_normalize_name(nm)] = rk
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--days-back", type=int, default=10,
                   help="Look back this many days for grass matches (default 10)")
    args = p.parse_args()

    print("Loading grass specialists JSON + fresh ATP rankings...")
    specialists = load_specialists()
    rankings = load_rankings()
    print(f"  specialists: {len(specialists)} players in resources/grass_specialists.json")
    print(f"  rankings:    {len(rankings)} players in ATP top-150")

    print(f"\nFetching ESPN ATP scoreboard for the last {args.days_back} days...")
    matches = fetch_grass_matches(args.days_back)
    print(f"  found {len(matches)} completed men's-singles grass matches")

    if not matches:
        print("(none — grass-swing events may not have started or ESPN data missing)")
        return 0

    # Tally by tournament + by player
    by_tourney: dict[str, list[dict]] = defaultdict(list)
    player_grass_w: dict[str, int] = defaultdict(int)
    player_grass_l: dict[str, int] = defaultdict(int)
    for m in matches:
        by_tourney[m["tournament_key"]].append(m)
        wn = _normalize_name(m["winner_name"])
        ln = _normalize_name(m["loser_name"])
        player_grass_w[wn] += 1
        player_grass_l[ln] += 1

    print(f"\n=== Matches by tournament ===")
    for k, ms in sorted(by_tourney.items()):
        sample_name = ms[0]["tournament_name"]
        print(f"  {k:<18} {sample_name:<32} matches={len(ms)}")

    # ── Player coverage: who appeared, who's missing from our JSON ──
    appeared = set(player_grass_w.keys()) | set(player_grass_l.keys())
    missing = appeared - set(specialists.keys())
    on_list = appeared & set(specialists.keys())
    print(
        f"\n=== Coverage ===\n"
        f"  Players who appeared in grass matches this week: {len(appeared)}\n"
        f"  On our specialists list: {len(on_list)}\n"
        f"  NOT on our list:         {len(missing)}"
    )

    # Players who went 2+ wins this week but aren't on the list → likely
    # need adding. Tournament wins on grass are uncommon for non-specialists.
    print(f"\n=== Candidates for addition (≥2 grass wins this week, NOT in JSON) ===")
    candidates = []
    for name in sorted(missing):
        if player_grass_w[name] >= 2:
            display = name.title()
            rk = rankings.get(name, "?")
            candidates.append((player_grass_w[name], player_grass_l[name], display, rk))
    if not candidates:
        print("  (none — all 2+ winners this week are already on the list)")
    else:
        candidates.sort(reverse=True)
        for w, l, name, rk in candidates:
            print(f"  {name:<28} W/L={w}-{l}  current_rank=#{rk}")

    # Players on list who went 0+ losses this week
    print(f"\n=== On-list players who lost this week (potential decay — n is small) ===")
    decayed = []
    for name in sorted(on_list):
        if player_grass_l[name] >= 1:
            info = specialists.get(name) or {}
            decayed.append((
                info.get("delta_pp", 0.0),
                name.title(),
                player_grass_w[name],
                player_grass_l[name],
            ))
    if not decayed:
        print("  (none on-list lost this week)")
    else:
        decayed.sort(reverse=True)
        for delta, name, w, l in decayed[:15]:
            tag = " ← positive delta but lost" if delta > 5 else ""
            print(f"  {name:<28} stored_delta={delta:+5.1f}pp  this_week={w}-{l}{tag}")

    # ── Filter simulation per match ──
    print(f"\n=== Filter simulation: what would have PASSed if Kalshi YES ≤ {TENNIS_MAX_ASK_CENTS}¢? ===")
    print(
        f"  Rule: YES = higher-ranked, gap > {TENNIS_MIN_RANK_GAP}, "
        f"grass-spec(YES) − grass-spec(opponent) ≥ +{GRASS_MIN_DELTA_DIFF_PP:.1f}pp"
    )
    print()
    sim_rows = []
    for m in matches:
        wn = _normalize_name(m["winner_name"])
        ln = _normalize_name(m["loser_name"])
        w_rk = rankings.get(wn)
        l_rk = rankings.get(ln)
        if not (w_rk and l_rk):
            sim_rows.append((m, "skip-missing-rank", None, None))
            continue
        # Identify higher-ranked
        if w_rk < l_rk:
            higher_won = True
            hi_name, hi_rk = wn, w_rk
            lo_name, lo_rk = ln, l_rk
        else:
            higher_won = False
            hi_name, hi_rk = ln, l_rk
            lo_name, lo_rk = wn, w_rk
        gap = lo_rk - hi_rk
        if gap <= TENNIS_MIN_RANK_GAP:
            sim_rows.append((m, "skip-gap-too-small", gap, None))
            continue
        hi_delta = (specialists.get(hi_name) or {}).get("delta_pp")
        lo_delta = (specialists.get(lo_name) or {}).get("delta_pp")
        if hi_delta is None or lo_delta is None:
            sim_rows.append((m, "skip-missing-grass-data", gap, None))
            continue
        diff = hi_delta - lo_delta
        if diff < GRASS_MIN_DELTA_DIFF_PP:
            sim_rows.append((m, "skip-grass-spec-diff", gap, diff))
            continue
        sim_rows.append((m, "PASS (pending price)", gap, diff))
        # Record whether YES side won
        sim_rows[-1] = (m, "PASS (pending price)", gap, diff)
        sim_rows[-1] = (
            *sim_rows[-1][:2], gap, diff, higher_won,
        )

    pass_rows = [r for r in sim_rows if r[1].startswith("PASS")]
    print(f"  PASS markets ({len(pass_rows)} of {len(sim_rows)}):")
    for row in pass_rows:
        m, status, gap, diff, *rest = row
        higher_won = rest[0] if rest else None
        outcome = "✓ YES won" if higher_won else "✗ upset" if higher_won is False else "?"
        print(
            f"    {m['date']}  {m['tournament_key']:<10} "
            f"{m['winner_name']} d. {m['loser_name']:<22} "
            f"gap={gap:>3}  grass_diff={diff:+5.1f}pp  → {outcome}"
        )

    skip_by_reason: dict[str, int] = defaultdict(int)
    for row in sim_rows:
        skip_by_reason[row[1]] += 1
    print(f"\n  SKIP breakdown:")
    for reason, n in sorted(skip_by_reason.items(), key=lambda x: -x[1]):
        print(f"    {n:>3}  {reason}")

    # ── Quality of the filter: did PASS markets resolve as predicted? ──
    pass_results = [r for r in sim_rows if r[1].startswith("PASS")]
    if pass_results:
        wins = sum(1 for row in pass_results if row[4] is True)
        n = len(pass_results)
        print(
            f"\n=== Filter-PASS resolution this week ===\n"
            f"  PASSed matches: {n}\n"
            f"  YES (higher-ranked specialist) won: {wins}\n"
            f"  Hit rate: {wins/n:.0%}\n"
            f"  (small sample — directional signal only; backtest baseline 73.5%)"
        )

    print(f"\nTo regenerate the full JSON from updated Sackmann data, re-run the")
    print(f"grass deep-dive analysis in the dryrun_tennis_filter session and copy")
    print(f"the result to resources/grass_specialists.json.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
