#!/bin/bash
# Start both the FastAPI server and Telegram bot

# Import/refresh products from xlsx on every deploy
# This ensures Rassvet_Master changes reach the app database immediately
FORCE_REIMPORT=1 python -m backend.services.import_products || echo "WARNING: Product import failed"

# Start FastAPI in background
python -m uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8000} &

# Start Telegram bot
python -m bot.main &

# Wait for any process to exit
wait -n

# Exit with status of process that exited first
exit $?
