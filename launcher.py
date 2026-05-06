import subprocess
import sys
import threading
import traceback

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

print("Launcher starting all processes...")
t1 = threading.Thread(target=run_bot)
t2 = threading.Thread(target=run_scanner)
t3 = threading.Thread(target=run_research)
t4 = threading.Thread(target=run_risk)
t5 = threading.Thread(target=run_prediction)

t1.start()
t2.start()
t3.start()
t4.start()
t5.start()

t1.join()
t2.join()
t3.join()
t4.join()
t5.join()
