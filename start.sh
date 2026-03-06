#!/bin/bash
# Start both the FastAPI server and Telegram bot

# Start FastAPI in background
python -m uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8000} &

# Start Telegram bot
python -m bot.main &

# Wait for any process to exit
wait -n

# Exit with status of process that exited first
exit $?
