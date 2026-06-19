#!/usr/bin/env python3
"""Scout open KXMLBTOTAL markets against the current stats_cache to flag
which games qualify for the BUY_NO eligibility cohort.

The cohort is defined in kalshi_edge.py's KXMLBTOTAL BUY_NO ELIGIBILITY
section:
  - Ticker tail must be -9 (T=8.5 line). -10 was removed 2026-06-18
    after the 180d backtest showed no Wilson-stable lift at T=9.5.
  - Both probable starters' season ERA must be < 3.50
  - (At runtime: projected_total must clear the line on the NO side)

This script checks the cohort+pitching gates from data alone — the
projection condition is decided by Claude at trade time and isn't
testable from stats. A game listed as "QUALIFIES" means the pitching
side is favorable; whether the bot actually emits BUY_NO depends on
Claude's runtime run-total projection.

Run
    railway ssh ... 'python3 /app/scripts/scout_buy_no.py --date 2026-06-18'
    python3 scripts/scout_buy_no.py --date 2026-06-18 \\
        --stats-cache /tmp/stats_cache.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


KALSHI_MARKETS_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
ERA_CAP = 3.50


def fetch_kxmlbtotal_markets(limit: int = 200) -> list[dict]:
    """Fetch one page of open KXMLBTOTAL markets. 200 is generous enough
    for a full evening slate; bump if a heavy day truncates."""
    r = requests.get(
        KALSHI_MARKETS_URL,
        params={
            "series_ticker": "KXMLBTOTAL",
            "status": "open",
            "limit": limit,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("markets", [])


def ticker_date_token(date_iso: str) -> str:
    """YYYY-MM-DD → YYMMMDD (e.g. 2026-06-18 → 26JUN18) — the format
    Kalshi embeds in MLB tickers."""
    dt = datetime.strptime(date_iso, "%Y-%m-%d")
    return dt.strftime("%y%b%d").upper()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--date",
        default=(datetime.now(timezone.utc) + timedelta(days=1))
        .strftime("%Y-%m-%d"),
        help="game_date to scout, YYYY-MM-DD (default: tomorrow UTC)",
    )
    p.add_argument(
        "--stats-cache",
        default="/app/data/stats_cache.json",
        help="Path to stats_cache.json (default: /app/data/stats_cache.json)",
    )
    p.add_argument(
        "--era-cap",
        type=float,
        default=ERA_CAP,
        help=f"Season ERA ceiling for BUY_NO eligibility (default {ERA_CAP})",
    )
    args = p.parse_args()

    sc_path = Path(args.stats_cache)
    if not sc_path.exists():
        print(f"[ERROR] stats_cache not found: {sc_path}", file=sys.stderr)
        return 1

    with sc_path.open() as f:
        sc = json.load(f)

    games_on_date = [
        g for g in (sc.get("mlb", {}) or {}).get("upcoming_games", []) or []
        if g.get("game_date") == args.date
    ]
    if not games_on_date:
        print(f"[INFO] no upcoming_games entries for {args.date}")
        return 0

    date_tok = ticker_date_token(args.date)
    try:
        markets = fetch_kxmlbtotal_markets()
    except requests.RequestException as e:
        print(f"[ERROR] Kalshi API fetch failed: {e}", file=sys.stderr)
        return 1

    tickers_on_date = [
        m for m in markets if date_tok in m.get("ticker", "")
    ]
    # Map (away, home) → set of available tails. Variable-length team
    # abbrs (SF, KC are 2-char) mean we test endswith(away+home)
    # against the ticker's middle segment rather than parsing offsets.
    available_tails: dict[tuple[str, str], set[str]] = {}
    for m in tickers_on_date:
        ticker = m.get("ticker", "")
        parts = ticker.split("-")
        if len(parts) < 3:
            continue
        middle = parts[1]
        tail = parts[-1]
        for g in games_on_date:
            ah = (g.get("away") or "") + (g.get("home") or "")
            if ah and middle.endswith(ah):
                key = (g["away"], g["home"])
                available_tails.setdefault(key, set()).add(tail)
                break

    print(
        f"SCOUT BUY_NO — game_date={args.date}  era_cap=<{args.era_cap}\n"
        f"stats_cache: {sc_path}  "
        f"(fetched_at={sc.get('fetched_at', '?')})\n"
        f"games on date: {len(games_on_date)}  "
        f"KXMLBTOTAL tickers on date: {len(tickers_on_date)}"
    )
    print()

    qualifying: list[dict] = []
    not_qualifying: list[dict] = []
    no_data: list[dict] = []
    for g in games_on_date:
        away = g.get("away", "?")
        home = g.get("home", "?")
        ap = g.get("away_pitcher") or {}
        hp = g.get("home_pitcher") or {}
        ap_era = ap.get("era")
        hp_era = hp.get("era")
        cohort_tails = sorted(
            available_tails.get((away, home), set()) & {"9"},
            key=int,
        )
        row = {
            "matchup": f"{away}@{home}",
            "start": (g.get("start_time_utc") or "?") + " UTC",
            "away_p": ap.get("player", "?"),
            "home_p": hp.get("player", "?"),
            "ap_era": ap_era,
            "hp_era": hp_era,
            "cohort_tails": cohort_tails,
        }
        if ap_era is None or hp_era is None:
            no_data.append(row)
        elif (
            ap_era < args.era_cap
            and hp_era < args.era_cap
            and cohort_tails
        ):
            qualifying.append(row)
        else:
            not_qualifying.append(row)

    def fmt_row(r: dict) -> str:
        tails_str = (
            ",".join(f"-{t}" for t in r["cohort_tails"]) or "—"
        )
        ap = f"{r['away_p']} ({r['ap_era']})" if r["ap_era"] is not None else f"{r['away_p']} (?)"
        hp = f"{r['home_p']} ({r['hp_era']})" if r["hp_era"] is not None else f"{r['home_p']} (?)"
        return (
            f"  {r['matchup']:<9} {r['start']:<10}  "
            f"away: {ap:<32}  home: {hp:<32}  cohort: {tails_str}"
        )

    def section(title: str, rows: list[dict]) -> None:
        print(f"=== {title} ({len(rows)}) ===")
        for r in rows:
            print(fmt_row(r))
        print()

    section(
        f"BUY_NO PITCHING GATE PASS (both ERA < {args.era_cap})",
        qualifying,
    )
    if qualifying:
        print(
            "  ↑ Pitching gate is favorable. BUY_NO will fire ONLY if "
            "Claude's runtime\n"
            "    projected_total comes in ≤ 7.5 (T=8.5 line, δ ≥ 1.00) "
            "at trade time."
        )
        print()
    section(
        "PITCHING GATE FAIL (at least one ERA ≥ cap, or no -9 listed)",
        not_qualifying,
    )
    if no_data:
        section("PITCHER DATA MISSING (probable starter TBD)", no_data)

    return 0


if __name__ == "__main__":
    sys.exit(main())
