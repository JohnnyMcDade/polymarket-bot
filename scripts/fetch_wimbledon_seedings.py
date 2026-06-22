#!/usr/bin/env python3
"""Fetch current Wimbledon draw seedings from ESPN and merge them
into resources/grass_specialists.json under the per-player
`wimbledon_seed` field.

Used by the grass filter (kalshi_edge._wimbledon_seed_of) to annotate
filter-pass reasons with seed info — "Seeded #3 at Wimbledon + grass
specialist" is a stronger signal than rank gap alone.

The draw is typically published 2-3 days before main draw begins
(late June). Before that, ESPN returns an empty payload and this
script no-ops cleanly — safe to wire into a cron from mid-June.

Run:
    python3 scripts/fetch_wimbledon_seedings.py
    python3 scripts/fetch_wimbledon_seedings.py --dry-run
    python3 scripts/fetch_wimbledon_seedings.py --year 2027

ESPN tennis API (site v2) returns tournament draws by event slug; the
Wimbledon slug is "wimbledon" for both ATP and WTA tours. Seedings live
in the draw "competitors" array. We pull ATP + WTA and merge.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
GRASS_JSON = ROOT / "resources" / "grass_specialists.json"

ESPN_DRAW_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/tennis/"
    "{tour}/tournaments/wimbledon/draws"
)
HTTP_TIMEOUT = 15


def _fetch_tour_seedings(tour: str, year: int) -> dict[str, int]:
    """Return {player_name: seed_number} for one tour ("atp" or "wta").

    ESPN's payload shape varies between draw states (pre-draw vs.
    live). We walk competitors looking for a numeric `seed` and a
    display name. Missing fields → player skipped (unseeded or pre-
    draw). Any HTTP/parse failure raises — caller decides whether
    to abort or continue with partial data."""
    url = ESPN_DRAW_URL.format(tour=tour)
    params = {"season": str(year)}
    r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    payload = r.json() or {}

    out: dict[str, int] = {}

    def _walk(node):
        if isinstance(node, dict):
            seed = node.get("seed")
            name = (
                node.get("displayName")
                or node.get("fullName")
                or (node.get("athlete") or {}).get("displayName")
            )
            if name and isinstance(seed, (int, str)):
                try:
                    s = int(str(seed).strip())
                except (TypeError, ValueError):
                    s = None
                if s and 1 <= s <= 32:
                    out[name] = s
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for v in node:
                _walk(v)

    _walk(payload)
    return out


def fetch_seedings(year: int) -> dict[str, int]:
    """Fetch and merge ATP + WTA Wimbledon seedings. Returns empty
    dict if both tours are pre-draw or ESPN is unreachable — caller
    can no-op rather than wiping good cached data."""
    merged: dict[str, int] = {}
    for tour in ("atp", "wta"):
        try:
            seeds = _fetch_tour_seedings(tour, year)
            print(f"[espn:{tour}] {len(seeds)} seeded players", flush=True)
            merged.update(seeds)
        except requests.RequestException as e:
            print(f"[WARN] ESPN {tour} fetch failed: {e}", flush=True)
        except (ValueError, KeyError) as e:
            print(f"[WARN] ESPN {tour} parse failed: {e}", flush=True)
    return merged


def merge_into_json(seeds: dict[str, int], dry_run: bool) -> int:
    """Merge seedings into grass_specialists.json. Returns the
    number of players that gained or changed a seed. Unseeded
    players retain their existing record (we only set, never clear,
    so a transient ESPN outage doesn't wipe the file)."""
    if not seeds:
        print("[merge] no seeds to apply — skipping write", flush=True)
        return 0

    with GRASS_JSON.open() as f:
        data = json.load(f)
    players = data.setdefault("players", {})

    changed = 0
    matched_in_cohort = 0
    for name, seed in seeds.items():
        info = players.get(name)
        if info is None:
            continue
        matched_in_cohort += 1
        if info.get("wimbledon_seed") != seed:
            info["wimbledon_seed"] = seed
            changed += 1

    data["wimbledon_seeds_updated_at"] = datetime.now(timezone.utc).isoformat()

    print(
        f"[merge] {len(seeds)} seeds fetched, "
        f"{matched_in_cohort} matched cohort, {changed} updated",
        flush=True,
    )
    if dry_run:
        print("[merge] --dry-run: not writing", flush=True)
        return changed

    with GRASS_JSON.open("w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    print(f"[merge] wrote {GRASS_JSON}", flush=True)
    return changed


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=datetime.now(timezone.utc).year)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    seeds = fetch_seedings(args.year)
    if not seeds:
        print(
            "[espn] no seedings available yet — Wimbledon draw is "
            "typically posted 2-3 days before main draw. No-op.",
            flush=True,
        )
        return 0
    merge_into_json(seeds, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
