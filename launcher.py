import subprocess
import sys

p1 = subprocess.Popen([sys.executable, 'polymarket_bot.py'])
p2 = subprocess.Popen([sys.executable, 'scanner_agent.py'])
p1.wait()
p2.wait()
