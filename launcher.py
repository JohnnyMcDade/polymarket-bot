import subprocess
import sys
import threading

def run_bot():
    subprocess.run([sys.executable, 'polymarket_bot.py'])

def run_scanner():
    subprocess.run([sys.executable, 'scanner_agent.py'])

t1 = threading.Thread(target=run_bot)
t2 = threading.Thread(target=run_scanner)

t1.start()
t2.start()

t1.join()
t2.join()
