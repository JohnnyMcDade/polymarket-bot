#!/usr/bin/env python3
"""Scout open KXNFLWINS markets and show every team's current yes_ask
across all listed win-total strikes.

KXNFLWINS markets are listed one per (team × win-threshold) pair, e.g.
KXNFLWINS-27IND-9 = "Indianapolis wins 9+ games in the 2026-27 season".
Each team typically has ~13 strikes (roughly 4+ through 16+ wins), so
the scout pulls every open ticker, groups by event (team), and prints:

  - all listed strikes with yes_ask / no_ask
  - the market-implied win total — the strike whose yes_ask is closest
    to $0.50 (the 50/50 line). That number is what the market thinks
    the team's most likely win total is; teams listed in over/under
    style use this as the central tendency.
  - a flag for missing teams (NFL has 32 — anything below 32 means
    Kalshi hasn't listed yet or the event was pulled)

This is the scouting tool for when we enable NFL strategy. Run early
in the day, eyeball where the market is pricing each team, decide
which strikes (if any) clear our cohort gate at trade time.

Run:
    python3 scripts/scout_nfl.py
    python3 scripts/scout_nfl.py --team KC
    python3 scripts/scout_nfl.py --json  # machine-readable output

API: public Kalshi REST. No auth needed for listing open markets.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from typing import Any

import requests


KALSHI_MARKETS_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
SERIES_TICKER = "KXNFLWINS"
PAGE_LIMIT = 200
NFL_TEAM_COUNT = 32

# Maps the trailing team code (as it appears in the Kalshi event
# ticker, e.g. "27IND" → "IND") to division. Kalshi uses the standard
# 3-letter abbreviations except a handful (JAX may be JAC, WAS may be
# WSH) — both spellings included so the lookup never misses.
NFL_DIVISIONS: dict[str, list[str]] = {
    "AFC East":  ["BUF", "MIA", "NE",  "NYJ"],
    "AFC North": ["BAL", "CIN", "CLE", "PIT"],
    "AFC South": ["HOU", "IND", "JAX", "TEN"],
    "AFC West":  ["DEN", "KC",  "LV",  "LAC"],
    "NFC East":  ["DAL", "NYG", "PHI", "WAS"],
    "NFC North": ["CHI", "DET", "GB",  "MIN"],
    "NFC South": ["ATL", "CAR", "NO",  "TB"],
    "NFC West":  ["ARI", "LAR", "SF",  "SEA"],
}
# Alternate abbreviations Kalshi has been observed to use. Maps the
# variant → canonical so division grouping doesn't drop a team when
# the listing uses an alias.
ABBR_ALIASES: dict[str, str] = {
    "JAC": "JAX",
    "WSH": "WAS",
    "LA":  "LAR",  # ambiguous historically; current LA = Rams
}

TEAM_TO_DIVISION: dict[str, str] = {
    team: div for div, teams in NFL_DIVISIONS.items() for team in teams
}


def division_of(abbr: str) -> str:
    """Map team abbreviation → division name. Returns "Unknown" when
    Kalshi lists a team code we don't recognize (printed in its own
    "Unknown" bucket so unmapped teams aren't silently dropped)."""
    canonical = ABBR_ALIASES.get(abbr, abbr)
    return TEAM_TO_DIVISION.get(canonical, "Unknown")


def fetch_all_markets() -> list[dict[str, Any]]:
    """Pull every open KXNFLWINS market across all pages. ~32 teams ×
    ~13 strikes = ~400 markets, more than a single page can hold."""
    out: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        params: dict[str, Any] = {
            "series_ticker": SERIES_TICKER,
            "status": "open",
            "limit": PAGE_LIMIT,
        }
        if cursor:
            params["cursor"] = cursor
        r = requests.get(KALSHI_MARKETS_URL, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        out.extend(body.get("markets") or [])
        cursor = body.get("cursor")
        if not cursor:
            break
    return out


def team_abbr_from_event(event_ticker: str) -> str:
    """KXNFLWINS-27IND → IND. Pulls the trailing team code off the
    event ticker; the season prefix (27 = 2026-27) is stripped."""
    parts = event_ticker.split("-")
    if len(parts) < 2:
        return event_ticker
    tail = parts[-1]
    # tail looks like "27IND" — strip leading digits
    i = 0
    while i < len(tail) and tail[i].isdigit():
        i += 1
    return tail[i:] or tail


def implied_win_total(markets: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The strike whose yes_ask is closest to $0.50. None when no
    market in the group has a yes_ask quote (illiquid event)."""
    candidates = [
        m for m in markets
        if _to_float(m.get("yes_ask_dollars")) is not None
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda m: abs(_to_float(m["yes_ask_dollars"]) - 0.50),
    )


def _to_float(v: Any) -> float | None:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--team",
        help="Filter to a single team abbreviation (e.g. KC, IND, SF).",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of the human-readable table.",
    )
    args = p.parse_args()

    try:
        markets = fetch_all_markets()
    except requests.RequestException as e:
        print(f"[ERROR] Kalshi fetch failed: {e}", file=sys.stderr)
        return 1

    if not markets:
        print("[INFO] no open KXNFLWINS markets — series may not be listed")
        return 0

    by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in markets:
        by_event[m.get("event_ticker", "")].append(m)

    team_filter = args.team.upper() if args.team else None
    teams: list[dict[str, Any]] = []
    for event, ms in by_event.items():
        abbr = team_abbr_from_event(event)
        if team_filter and abbr != team_filter:
            continue
        ms_sorted = sorted(ms, key=lambda x: x.get("floor_strike", 0))
        impl = implied_win_total(ms_sorted)
        teams.append({
            "team": abbr,
            "division": division_of(abbr),
            "event_ticker": event,
            "strikes": [
                {
                    "ticker": m.get("ticker"),
                    "strike": m.get("floor_strike"),
                    "yes_ask": _to_float(m.get("yes_ask_dollars")),
                    "yes_bid": _to_float(m.get("yes_bid_dollars")),
                    "no_ask": _to_float(m.get("no_ask_dollars")),
                    "volume_fp": _to_float(m.get("volume_fp")) or 0.0,
                    "open_interest_fp": _to_float(m.get("open_interest_fp")) or 0.0,
                }
                for m in ms_sorted
            ],
            "implied_win_total": impl.get("floor_strike") if impl else None,
            "implied_yes_ask": _to_float(impl.get("yes_ask_dollars")) if impl else None,
        })

    teams.sort(key=lambda t: t["team"])

    if args.json:
        json.dump(teams, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
        return 0

    header = (
        f"SCOUT KXNFLWINS — {len(teams)} team(s), "
        f"{len(markets)} open markets"
    )
    if not team_filter and len(by_event) < NFL_TEAM_COUNT:
        missing = NFL_TEAM_COUNT - len(by_event)
        header += f"  ⚠ {missing} team(s) not yet listed"
    print(header)
    print()

    # Group teams by division for display. Iterate NFL_DIVISIONS in
    # declaration order (AFC East, North, …, NFC West) so the layout
    # is stable across runs; anything Kalshi lists under an unmapped
    # code lands in a trailing "Unknown" bucket.
    by_division: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in teams:
        by_division[t["division"]].append(t)
    division_order = list(NFL_DIVISIONS.keys())
    if "Unknown" in by_division:
        division_order.append("Unknown")

    for division in division_order:
        ts = by_division.get(division) or []
        if not ts:
            continue
        # Use canonical codes (apply ABBR_ALIASES) so an alias like
        # JAC doesn't show JAX as missing despite being effectively listed.
        listed_codes = {ABBR_ALIASES.get(t["team"], t["team"]) for t in ts}
        expected = set(NFL_DIVISIONS.get(division, []))
        missing_in_div = sorted(expected - listed_codes) if expected else []
        head = f"=== {division}  ({len(ts)}/4 listed)"
        if missing_in_div:
            head += f"  missing: {', '.join(missing_in_div)}"
        print(head)
        for t in ts:
            impl = t["implied_win_total"]
            impl_ask = t["implied_yes_ask"]
            if impl is not None:
                tag = (
                    f"implied o/u {impl - 0.5:.1f} "
                    f"(yes_ask ${impl_ask:.2f} @ {impl}+ wins)"
                )
            else:
                tag = "no quotes"
            print(f"--- {t['team']:>3} ({t['event_ticker']})  {tag}")
            print(
                f"     {'strike':<8}{'yes_ask':>10}{'yes_bid':>10}"
                f"{'no_ask':>10}{'volume':>10}{'OI':>10}"
            )
            for s in t["strikes"]:
                ya = f"${s['yes_ask']:.2f}" if s["yes_ask"] is not None else "—"
                yb = f"${s['yes_bid']:.2f}" if s["yes_bid"] is not None else "—"
                na = f"${s['no_ask']:.2f}" if s["no_ask"] is not None else "—"
                marker = "  ← implied" if s["strike"] == impl else ""
                print(
                    f"     {str(s['strike']) + '+':<8}{ya:>10}{yb:>10}{na:>10}"
                    f"{s['volume_fp']:>10.0f}{s['open_interest_fp']:>10.0f}"
                    f"{marker}"
                )
            print()

    if not team_filter and len(by_event) < NFL_TEAM_COUNT:
        listed = {team_abbr_from_event(e) for e in by_event}
        print(f"[WARN] only {len(listed)}/{NFL_TEAM_COUNT} teams have open events")
    return 0


if __name__ == "__main__":
    sys.exit(main())
