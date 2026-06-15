#!/usr/bin/env python3
"""Regenerate resources/grass_specialists.json from fresh Sackmann
match data.

The JSON is consumed by the production tennis filter's grass-court
gate. Each entry stores the player's career grass-vs-overall delta —
how much their grass win rate exceeds their overall win rate, in pp.

Run after a grass tournament (Halle / Queens / Wimbledon / etc) to
pick up new players whose career grass-match count has crossed the
N=10 threshold or whose recent results materially shift their delta.

Run:
    python3 scripts/regen_grass_specialists.py
    python3 scripts/regen_grass_specialists.py --refresh
        # delete cached Sackmann CSVs first → force fresh fetch
    python3 scripts/regen_grass_specialists.py --years 2024 2025 2026
        # custom window
    python3 scripts/regen_grass_specialists.py --dry-run
        # show diff only; don't write
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backtest_kalshi_tennis import load_sackmann_matches, CACHE_DIR

GRASS_JSON = ROOT / "resources" / "grass_specialists.json"
MIN_GRASS_MATCHES = 10
MIN_TOTAL_MATCHES = 50

# Players we KNOW should be on the list per scripts/validate_grass_specialists
# — Halle/Queens scout 2026-06-14. Used purely for highlighting in the
# diff output; doesn't influence inclusion logic.
WATCH_PLAYERS = (
    "Frances Tiafoe", "Marin Cilic", "Mattia Bellucci", "Benjamin Bonzi",
    "Martin Landaluce", "Kamil Majchrzak", "Sho Shimabukuro",
    "Martin Damm", "Raphael Collignon",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    end_year = datetime.now(timezone.utc).year
    parser.add_argument(
        "--years", nargs="+", type=int,
        default=[end_year - 2, end_year - 1, end_year],
        help=f"Years to load (default: {[end_year-2, end_year-1, end_year]})",
    )
    parser.add_argument(
        "--refresh", action="store_true",
        help="Delete Sackmann match-file caches before fetching",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute diff but don't write the JSON",
    )
    args = parser.parse_args()

    if args.refresh:
        print(f"Refreshing Sackmann cache in {CACHE_DIR}...")
        for tour in ("atp", "wta"):
            for year in args.years:
                p = CACHE_DIR / f"{tour}_matches_{year}.csv"
                if p.exists():
                    print(f"  rm {p}")
                    p.unlink()
                else:
                    print(f"  (not cached: {p.name})")

    print(f"\nLoading Sackmann data for years {args.years}...")
    all_matches = []
    for tour in ("atp", "wta"):
        ms = load_sackmann_matches(tour, args.years)
        print(f"  {tour}: {len(ms)} matches loaded")
        all_matches.extend(ms)

    # Tally per-player W/L overall vs grass
    rec: dict[str, dict[str, int]] = defaultdict(
        lambda: {"all_w": 0, "all_l": 0, "grass_w": 0, "grass_l": 0}
    )
    for m in all_matches:
        rec[m.winner_name]["all_w"] += 1
        rec[m.loser_name]["all_l"] += 1
        if m.surface == "Grass":
            rec[m.winner_name]["grass_w"] += 1
            rec[m.loser_name]["grass_l"] += 1

    new_players: dict[str, dict] = {}
    for name, r in rec.items():
        gn = r["grass_w"] + r["grass_l"]
        an = r["all_w"] + r["all_l"]
        if gn < MIN_GRASS_MATCHES or an < MIN_TOTAL_MATCHES:
            continue
        gwr = r["grass_w"] / gn
        awr = r["all_w"] / an
        new_players[name] = {
            "delta_pp": round((gwr - awr) * 100, 1),
            "grass_wr": round(gwr, 3),
            "overall_wr": round(awr, 3),
            "grass_n": gn,
            "all_n": an,
        }

    # Diff vs existing JSON
    existing: dict[str, dict] = {}
    if GRASS_JSON.exists():
        existing = json.loads(GRASS_JSON.read_text()).get("players") or {}

    added = sorted(set(new_players) - set(existing))
    removed = sorted(set(existing) - set(new_players))
    common = set(new_players) & set(existing)
    delta_changes = []
    for name in common:
        old_delta = existing[name].get("delta_pp", 0.0)
        new_delta = new_players[name]["delta_pp"]
        if abs(new_delta - old_delta) >= 1.0:
            delta_changes.append((name, old_delta, new_delta))

    print(f"\n{'='*70}")
    print(f"RESULT")
    print(f"{'='*70}")
    print(f"  OLD count:  {len(existing)}")
    print(f"  NEW count:  {len(new_players)}")
    print(f"  Δ count:    {len(new_players) - len(existing):+d}")
    print(f"  Added:      {len(added)}")
    print(f"  Removed:    {len(removed)}")
    print(f"  Changed:    {len(delta_changes)}  (|Δ delta_pp| ≥ 1.0)")

    if added:
        print(f"\nWATCH-list players added this run:")
        any_watch = False
        for tn in WATCH_PLAYERS:
            if tn in new_players and tn not in existing:
                info = new_players[tn]
                print(
                    f"  + {tn:<28} delta={info['delta_pp']:+5.1f}pp "
                    f"grass_wr={info['grass_wr']:.0%} "
                    f"n={info['grass_n']}/{info['all_n']}"
                )
                any_watch = True
        if not any_watch:
            print("  (none of the watch-list players cleared the threshold)")

        other_added = [n for n in added if n not in WATCH_PLAYERS]
        if other_added:
            print(f"\nOther adds ({len(other_added)}, top 10 by |delta_pp|):")
            top = sorted(
                other_added,
                key=lambda n: -abs(new_players[n]["delta_pp"]),
            )[:10]
            for n in top:
                info = new_players[n]
                print(
                    f"  + {n:<28} delta={info['delta_pp']:+5.1f}pp "
                    f"n={info['grass_n']}/{info['all_n']}"
                )

    if removed:
        print(f"\nRemoved (no longer meet thresholds — {len(removed)}):")
        for n in removed[:10]:
            old = existing[n]
            print(
                f"  - {n:<28} was delta={old.get('delta_pp', 0):+5.1f}pp "
                f"n={old.get('grass_n', '?')}/{old.get('all_n', '?')}"
            )

    if delta_changes:
        print(f"\nBiggest delta shifts (top 10 by |Δdelta|):")
        for name, old, new in sorted(
            delta_changes, key=lambda x: -abs(x[2] - x[1])
        )[:10]:
            arrow = "↑" if new > old else "↓"
            print(
                f"  {arrow} {name:<28} {old:+5.1f}pp → {new:+5.1f}pp "
                f"(Δ {new-old:+.1f}pp)"
            )

    if args.dry_run:
        print(f"\n[DRY-RUN] not writing {GRASS_JSON}")
        return 0

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_years": args.years,
        "min_grass_matches": MIN_GRASS_MATCHES,
        "min_total_matches": MIN_TOTAL_MATCHES,
        "players": dict(sorted(
            new_players.items(), key=lambda x: -x[1]["delta_pp"]
        )),
    }
    GRASS_JSON.parent.mkdir(parents=True, exist_ok=True)
    GRASS_JSON.write_text(json.dumps(payload, indent=2))
    print(f"\nWrote {GRASS_JSON}")
    print(f"  size:    {GRASS_JSON.stat().st_size} bytes")
    print(f"  players: {len(new_players)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
