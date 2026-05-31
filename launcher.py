import os
import subprocess
import sys
import threading
import traceback
from polymarket_bot_endpoints import start_api_server

# The 4 new Kalshi agents are imported (NOT subprocessed) because the
# trader and edge agents share state through kalshi_queue.py's in-memory
# queues. Subprocess-spawned agents would each see an empty queue. The
# Polymarket agents below stay subprocess-spawned because they don't
# share state with anything else.
import kalshi_stats
import kalshi_edge
import kalshi_trader
import kalshi_winrate
import kalshi_tracker  # whale watcher — independent of the trading pipeline


# ── Polymarket agents (subprocess-wrapped, unchanged) ─────────────────────

def run_bot():
    try:
        subprocess.run([sys.executable, 'polymarket_bot.py'])
    except Exception as e:
        print(f"Bot error: {e}")
        traceback.print_exc()

def run_scanner():
    try:
        print("Scanner thread starting...")
        subprocess.run([sys.executable, 'scanner_agent.py'])
    except Exception as e:
        print(f"Scanner error: {e}")
        traceback.print_exc()

def run_research():
    try:
        print("Research thread starting...")
        subprocess.run([sys.executable, 'research_agent.py'])
    except Exception as e:
        print(f"Research error: {e}")
        traceback.print_exc()

def run_risk():
    try:
        print("Risk thread starting...")
        subprocess.run([sys.executable, 'risk_agent.py'])
    except Exception as e:
        print(f"Risk error: {e}")
        traceback.print_exc()

def run_prediction():
    try:
        print("Prediction thread starting...")
        subprocess.run([sys.executable, 'prediction_agent.py'])
    except Exception as e:
        print(f"Prediction error: {e}")
        traceback.print_exc()

def run_postmortem():
    try:
        print("Post-mortem thread starting...")
        subprocess.run([sys.executable, 'postmortem_agent.py'])
    except Exception as e:
        print(f"Post-mortem error: {e}")
        traceback.print_exc()


# ── Kalshi pipeline agents (in-process — share kalshi_queue state) ────────
# Each wrapper has try/except so one agent crashing doesn't kill the others.
# Stats fires daily at 06:00 UTC, edge every 30 min, trader every 5 min,
# winrate daily at 07:00 UTC. The trader pulls from kalshi_queue stage
# "risk" which is what edge enqueues into.

def run_kalshi_stats():
    try:
        print("Kalshi Stats thread starting...")
        kalshi_stats.run()
    except Exception as e:
        print(f"Kalshi Stats error: {e}")
        traceback.print_exc()

def run_kalshi_edge():
    try:
        print("Kalshi Edge thread starting...")
        kalshi_edge.run()
    except Exception as e:
        print(f"Kalshi Edge error: {e}")
        traceback.print_exc()

def run_kalshi_trader():
    try:
        print("Kalshi Trader thread starting...")
        kalshi_trader.run()
    except Exception as e:
        print(f"Kalshi Trader error: {e}")
        traceback.print_exc()

def run_kalshi_winrate():
    try:
        print("Kalshi Win-Rate thread starting...")
        kalshi_winrate.run()
    except Exception as e:
        print(f"Kalshi Win-Rate error: {e}")
        traceback.print_exc()

def run_kalshi_tracker():
    try:
        print("Kalshi Tracker thread starting...")
        kalshi_tracker.run()
    except Exception as e:
        print(f"Kalshi Tracker error: {e}")
        traceback.print_exc()


print("Launcher starting all processes...")

# Polymarket agents (6 subprocess wrappers, unchanged)
t1  = threading.Thread(target=run_bot)
t2  = threading.Thread(target=run_scanner)
t3  = threading.Thread(target=run_research)
t4  = threading.Thread(target=run_risk)
t5  = threading.Thread(target=run_prediction)
t6  = threading.Thread(target=run_postmortem)

# Kalshi pipeline (4 in-process threads sharing kalshi_queue state)
t7  = threading.Thread(target=run_kalshi_stats)
t8  = threading.Thread(target=run_kalshi_edge)
t9  = threading.Thread(target=run_kalshi_trader)
t10 = threading.Thread(target=run_kalshi_winrate)

# Kalshi whale tracker — parallel, not part of the trading pipeline
t11 = threading.Thread(target=run_kalshi_tracker)

for t in (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11):
    t.start()

# FastAPI server for the TikTok UGC ads pipeline (unchanged)
threading.Thread(
    target=start_api_server,
    kwargs={"host": "0.0.0.0", "port": int(os.environ.get("PORT", 8000))},
    daemon=True,
).start()
print("API server thread started on port", os.environ.get("PORT", 8000))

for t in (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11):
    t.join()
