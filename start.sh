#!/bin/bash
# Start both the FastAPI server and Telegram bot

# Import/refresh products from xlsx on every deploy (additive — matches railway.toml).
# DO NOT add FORCE_REIMPORT=1 here: it triggers DELETE FROM products/producers/categories
# in import_products.py:252, which breaks server-side cart persistence (cart_items.product_id
# becomes orphaned). Live deploys via railway.toml run this without the flag — keep parity.
python -m backend.services.import_products || echo "WARNING: Product import failed"

# Start FastAPI in background
python -m uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8000} &

# Start Telegram bot
python -m bot.main &

# Wait for any process to exit
wait -n

# Exit with status of process that exited first
exit $?
