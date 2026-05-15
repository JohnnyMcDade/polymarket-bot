import os
import subprocess
import sys
import threading
import traceback
from polymarket_bot_endpoints import start_api_server

# The 6 Kalshi pipeline agents are imported (NOT subprocessed) because they
# share state through kalshi_queue.py's in-memory queues. Subprocess-spawned
# kalshi agents would each see an empty queue. The Polymarket agents below
# stay subprocess-spawned because they don't share state with anything else.
import kalshi_scanner
import kalshi_research
import kalshi_prediction
import kalshi_risk
import kalshi_execution
import kalshi_postmortem
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
# The agents themselves catch per-cycle exceptions, so reaching these
# handlers means a crash at module load or a non-loop-recoverable error.

def run_kalshi_scanner():
    try:
        print("Kalshi Scanner thread starting...")
        kalshi_scanner.run()
    except Exception as e:
        print(f"Kalshi Scanner error: {e}")
        traceback.print_exc()

def run_kalshi_research():
    try:
        print("Kalshi Research thread starting...")
        kalshi_research.run()
    except Exception as e:
        print(f"Kalshi Research error: {e}")
        traceback.print_exc()

def run_kalshi_prediction():
    try:
        print("Kalshi Prediction thread starting...")
        kalshi_prediction.run()
    except Exception as e:
        print(f"Kalshi Prediction error: {e}")
        traceback.print_exc()

def run_kalshi_risk():
    try:
        print("Kalshi Risk thread starting...")
        kalshi_risk.run()
    except Exception as e:
        print(f"Kalshi Risk error: {e}")
        traceback.print_exc()

def run_kalshi_execution():
    try:
        print("Kalshi Execution thread starting...")
        kalshi_execution.run()
    except Exception as e:
        print(f"Kalshi Execution error: {e}")
        traceback.print_exc()

def run_kalshi_postmortem():
    try:
        print("Kalshi Post-Mortem thread starting...")
        kalshi_postmortem.run()
    except Exception as e:
        print(f"Kalshi Post-Mortem error: {e}")
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

# Kalshi pipeline (6 in-process threads sharing kalshi_queue state)
t7  = threading.Thread(target=run_kalshi_scanner)
t8  = threading.Thread(target=run_kalshi_research)
t9  = threading.Thread(target=run_kalshi_prediction)
t10 = threading.Thread(target=run_kalshi_risk)
t11 = threading.Thread(target=run_kalshi_execution)
t12 = threading.Thread(target=run_kalshi_postmortem)

# Kalshi whale tracker — parallel, not part of the trading pipeline
t13 = threading.Thread(target=run_kalshi_tracker)

for t in (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13):
    t.start()

# FastAPI server for the TikTok UGC ads pipeline (unchanged)
threading.Thread(
    target=start_api_server,
    kwargs={"host": "0.0.0.0", "port": int(os.environ.get("PORT", 8000))},
    daemon=True,
).start()
print("API server thread started on port", os.environ.get("PORT", 8000))

for t in (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13):
    t.join()
