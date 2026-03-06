"""Start both FastAPI server and Telegram bot together."""
import subprocess
import sys
import os

port = os.getenv("PORT", "8000")

procs = [
    subprocess.Popen([sys.executable, "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", port]),
    subprocess.Popen([sys.executable, "-m", "bot.main"]),
]

for p in procs:
    p.wait()
