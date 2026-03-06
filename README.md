# Telegram Katalog — Qurilish Materiallari

Telegram Mini App for browsing wholesale construction materials catalog, adding products to cart, and exporting orders as PDF/Excel.

## Quick Start

### 1. Setup
```bash
cp .env.example .env
# Edit .env with your BOT_TOKEN and WEBAPP_URL

pip install -r requirements.txt
cd frontend && npm install && npm run build && cd ..
```

### 2. Import Products
```bash
python -m backend.services.import_products
```

### 3. Upload Product Images
```bash
# Place product images in a folder, named by product code or name
python -m backend.services.image_manager /path/to/your/images/
```

### 4. Run Locally
```bash
# Terminal 1: API server
uvicorn backend.main:app --reload --port 8000

# Terminal 2: Telegram bot
python -m bot.main

# Terminal 3: Frontend dev server
cd frontend && npm run dev
```

### 5. Deploy (Railway/Render)
```bash
docker build -t catalog-bot .
docker run -p 8000:8000 --env-file .env catalog-bot
```

## Environment Variables
| Variable | Description |
|----------|-------------|
| `BOT_TOKEN` | Telegram Bot API token from @BotFather |
| `WEBAPP_URL` | Public URL where the Mini App is hosted |
| `DATABASE_PATH` | SQLite database path (default: `./data/catalog.db`) |
| `IMAGES_DIR` | Product images directory (default: `./images`) |
| `PORT` | Server port (default: `8000`) |

## Re-importing Products
To update the product catalog from a new spreadsheet:
```bash
# Replace the file and re-run import
cp new_products.xls data/products.xls
python -m backend.services.import_products
```

## Future Features
- Client balance checking (add `users` + `balances` tables)
- Order history and tracking
- Multi-language support (Russian, English)
- Admin panel for managing products
