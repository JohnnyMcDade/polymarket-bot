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

print("Launcher starting both processes...")
t1 = threading.Thread(target=run_bot)
t2 = threading.Thread(target=run_scanner)

t1.start()
t2.start()

t1.join()
t2.join()
