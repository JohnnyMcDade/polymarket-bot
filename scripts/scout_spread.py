#!/usr/bin/env python3
"""Scout open KXMLBSPREAD markets against the current stats_cache to
surface which spread tickers qualify for the pilot rollout cohort:

  - Line ∈ (1.5, 2.5), i.e. ticker tail ending in `2` or `3`
  - Spread-team's PROJECTED_MARGIN ≥ line + 1.0 (the δ-gate)
  - Both HOME-spread and AWAY-spread directions are eligible

Mirrors the KXMLBSPREAD methodology block in kalshi_edge.py's prompt,
including the projection formula:

    margin = spread_rs_per_game - opp_rs_per_game
           + 0.5 × (opp_rolling_era - spread_rolling_era)
           + (+0.3 if spread-team is home else -0.3)

This script computes the spread-team projection from cached data alone
— it does not call Claude. A ticker listed as "QUALIFIES" means the
data side of the gate is clear; the live trader still subjects the
ticker to Claude's recommendation, the structured PROJECTED_MARGIN
gate, and the per-night cap.

Run
    railway ssh ... 'python3 /app/scripts/scout_spread.py --date 2026-06-20'
    python3 scripts/scout_spread.py --date 2026-06-20 \\
        --stats-cache /tmp/stats_cache.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _kalshi_local_snapshot import load_local_markets  # noqa: E402


KALSHI_MARKETS_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
HOME_ADVANTAGE_RUNS = 0.3
ERA_WEIGHT = 0.5
COHORT_LINES = (1.5, 2.5)
# δ threshold (PROJECTED_MARGIN must clear `line + DELTA` in spread-team's
# favor). Same env var as the production gate so the scout and the live
# bot agree on which tickers qualify. Default 0.75 per the 2026-06-20 sweep.
DELTA = float(os.getenv("KALSHI_SPREAD_DELTA", "0.75"))


def fetch_kxmlbspread_markets(limit: int = 200) -> list[dict]:
    r = requests.get(
        KALSHI_MARKETS_URL,
        params={
            "series_ticker": "KXMLBSPREAD",
            "status": "open",
            "limit": limit,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("markets", [])


def ticker_date_token(date_iso: str) -> str:
    dt = datetime.strptime(date_iso, "%Y-%m-%d")
    return dt.strftime("%y%b%d").upper()


def parse_spread_tail(ticker: str) -> tuple[str, int] | None:
    """Pull (team_abbr, N) from a KXMLBSPREAD ticker tail like `-MIL2`.
    Returns None if the tail doesn't match the expected shape."""
    m = re.search(r"-([A-Z]+)(\d+)$", ticker)
    if not m:
        return None
    return m.group(1), int(m.group(2))


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
    args = p.parse_args()

    sc_path = Path(args.stats_cache)
    if not sc_path.exists():
        print(f"[ERROR] stats_cache not found: {sc_path}", file=sys.stderr)
        return 1

    with sc_path.open() as f:
        sc = json.load(f)

    mlb = sc.get("mlb", {}) or {}
    team_scoring = mlb.get("team_scoring", {}) or {}
    games_on_date = [
        g for g in mlb.get("upcoming_games", []) or []
        if g.get("game_date") == args.date
    ]
    if not games_on_date:
        print(f"[INFO] no upcoming_games entries for {args.date}")
        return 0

    # Index games by team-pair (both orderings) for fast lookup
    game_by_team: dict[str, dict] = {}
    for g in games_on_date:
        away = g.get("away") or ""
        home = g.get("home") or ""
        if away:
            game_by_team[away] = g
        if home:
            game_by_team[home] = g

    date_tok = ticker_date_token(args.date)
    try:
        markets = fetch_kxmlbspread_markets()
    except requests.RequestException as e:
        local, src = load_local_markets("KXMLBSPREAD")
        if not local:
            print(f"[ERROR] Kalshi API fetch failed: {e}", file=sys.stderr)
            print("[ERROR] No local snapshot available either.", file=sys.stderr)
            return 1
        print(
            f"[WARN] Kalshi API unreachable ({e}); using local snapshot "
            f"from {src} ({len(local)} markets).",
            file=sys.stderr,
        )
        markets = local

    tickers_on_date = [m for m in markets if date_tok in m.get("ticker", "")]

    print(
        f"SCOUT KXMLBSPREAD — game_date={args.date}\n"
        f"stats_cache: {sc_path}  "
        f"(fetched_at={sc.get('fetched_at', '?')})\n"
        f"games on date: {len(games_on_date)}  "
        f"KXMLBSPREAD tickers on date: {len(tickers_on_date)}  "
        f"cohort lines: {COHORT_LINES}, δ-gate: ≥ line + {DELTA:.1f}"
    )
    print()

    qualifying: list[dict] = []
    no_qualify_data: list[dict] = []
    fails_gate: list[dict] = []

    for m in tickers_on_date:
        ticker = m.get("ticker", "")
        yes_ask = m.get("yes_ask")
        no_ask = m.get("no_ask")
        parsed = parse_spread_tail(ticker)
        if not parsed:
            continue
        spread_team, n = parsed
        line = n - 0.5
        g = game_by_team.get(spread_team)
        if not g:
            no_qualify_data.append({
                "ticker": ticker,
                "reason": f"no upcoming_game for {spread_team}",
            })
            continue
        home_abbr = g.get("home") or ""
        away_abbr = g.get("away") or ""
        is_home = spread_team == home_abbr
        opp_abbr = away_abbr if is_home else home_abbr
        spread_stats = team_scoring.get(spread_team) or {}
        opp_stats = team_scoring.get(opp_abbr) or {}
        spread_rs = spread_stats.get("rs_per_game")
        opp_rs = opp_stats.get("rs_per_game")
        # Use rolling_era_last3 (matches backtest predictor input). Fall
        # back to season ERA if rolling missing — the prompt asks Claude
        # to do the same, and we mirror that for parity.
        spread_pitcher = (
            g.get("home_pitcher") if is_home else g.get("away_pitcher")
        ) or {}
        opp_pitcher = (
            g.get("away_pitcher") if is_home else g.get("home_pitcher")
        ) or {}
        spread_era = (
            spread_pitcher.get("rolling_era_last3")
            or spread_pitcher.get("era")
        )
        opp_era = (
            opp_pitcher.get("rolling_era_last3")
            or opp_pitcher.get("era")
        )
        ha = HOME_ADVANTAGE_RUNS if is_home else -HOME_ADVANTAGE_RUNS

        if (
            spread_rs is None or opp_rs is None
            or spread_era is None or opp_era is None
        ):
            no_qualify_data.append({
                "ticker": ticker,
                "reason": (
                    f"missing inputs: "
                    f"spread_rs={spread_rs}, opp_rs={opp_rs}, "
                    f"spread_era={spread_era}, opp_era={opp_era}"
                ),
            })
            continue

        margin = (
            spread_rs - opp_rs
            + ERA_WEIGHT * (opp_era - spread_era)
            + ha
        )
        threshold = line + DELTA

        row = {
            "ticker": ticker,
            "matchup": f"{away_abbr}@{home_abbr}",
            "spread_team": spread_team,
            "direction": "HOME" if is_home else "AWAY",
            "line": line,
            "n": n,
            "margin": margin,
            "threshold": threshold,
            "yes_ask": yes_ask,
            "no_ask": no_ask,
            "spread_p": spread_pitcher.get("player", "?"),
            "opp_p": opp_pitcher.get("player", "?"),
            "spread_era": spread_era,
            "opp_era": opp_era,
            "spread_rs": spread_rs,
            "opp_rs": opp_rs,
        }
        if line not in COHORT_LINES:
            row["reason"] = f"line={line} not in cohort {COHORT_LINES}"
            fails_gate.append(row)
        elif margin < threshold:
            row["reason"] = (
                f"projected_margin={margin:+.2f} < threshold={threshold:.1f}"
            )
            fails_gate.append(row)
        else:
            qualifying.append(row)

    qualifying.sort(key=lambda r: -r["margin"])
    fails_gate.sort(key=lambda r: (r["line"] not in COHORT_LINES, -r["margin"]))

    def fmt_qual(r: dict) -> str:
        ask_str = (
            f"yes={r['yes_ask']}¢" if r["yes_ask"] is not None
            else "yes=unpriced"
        )
        return (
            f"  {r['ticker']:<42}  "
            f"{r['direction']}-spread  "
            f"{r['spread_team']:<3} margin={r['margin']:+.2f} "
            f"≥ threshold={r['threshold']:.1f}  "
            f"(line={r['line']})  "
            f"{r['spread_p']} {r['spread_era']} vs "
            f"{r['opp_p']} {r['opp_era']}  "
            f"{ask_str}"
        )

    print(f"=== QUALIFIES ({len(qualifying)}) ===")
    if qualifying:
        print(
            "  Data-side of the gate is clear. Live trader still subjects each\n"
            "  to Claude + structured PROJECTED_MARGIN gate + per-night cap.\n"
        )
        for r in qualifying:
            print(fmt_qual(r))
    else:
        print("  (none)")
    print()

    print(f"=== FAILS GATE ({len(fails_gate)}) ===")
    for r in fails_gate[:20]:  # cap at 20; daily slate can include 100+
        reason = r.get("reason", "")
        print(
            f"  {r['ticker']:<42}  {r['direction']}-spread  "
            f"line={r['line']}  margin={r['margin']:+.2f}  "
            f"— {reason}"
        )
    if len(fails_gate) > 20:
        print(f"  … {len(fails_gate) - 20} more (truncated)")
    print()

    if no_qualify_data:
        print(f"=== NO DATA ({len(no_qualify_data)}) ===")
        for r in no_qualify_data[:10]:
            print(f"  {r['ticker']:<42}  — {r['reason']}")
        if len(no_qualify_data) > 10:
            print(f"  … {len(no_qualify_data) - 10} more (truncated)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
