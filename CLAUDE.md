# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Rassvet Catalogue is a Telegram Mini App for wholesale product ordering. Three-tier architecture: React frontend (Telegram WebApp) → FastAPI backend → SQLite database, with an Aiogram Telegram bot for admin operations. Deployed on Railway with persistent `/data` volume.

## Business Context

Rassvet is a family-owned construction materials wholesaler (Uzbekistan, run by Ulugbek's father). Clients are existing B2B customers only — no public browsing, no anonymous self-signup; access is gated by the `allowed_clients` phone whitelist (2,039+ records). UI is Uzbek Latin script. All schedules, reminders, and log timestamps are Tashkent time (GMT+5).

**Workspace scope:** This `telegram-catalog/` folder is the live app. The parent folder (`../`) also holds analytics tooling (cashback/loyalty workbooks, `tools/`, root-level Python scripts), the photo pipeline (`product_photos_original/` → `bgremoval_poc/` → `images_transparent/` → `images_allocation/`), and ops state in `../obsidian-vault/` (morning briefings, command center, MOC, error log, inbox — check there before assuming a task is new). Do not edit `../telegram-catalog-latest/` — stale Mar 23 copy. Current parent-root diagnostic docs (Apr 13 2026): `SCORING_ALGORITHM_TECHNICAL.md`, `README_DIAGNOSTICS.md`, `DIAGNOSTIC_SUMMARY.txt`, `prompt_debtors_command.md`. `PROJECT_ROADMAP.md` and `WEEKLY_UPDATE_WORKFLOW.md` are stale (Mar).

## Working in this project — read these first

These artifacts govern how to work here. Read in order before starting any task:

- **`.claude/rules/`** — six numbered rules + `07-proactive-anticipation.md` (audit before declaring done; every defer needs a trigger; cost-to-fix vs cost-to-defer; meta-work doesn't auto-attribute to active session). Loaded automatically as project context — but cite the specific rule by number when invoking it.
- **`.claude/sessions/_index.md`** — letter-and-cross-cutting charter index. Every active letter (A–Y) and every cross-cutting concern (Agent / Bot / Communications / Ops) has a charter here.
- **`.claude/sessions/_conventions.md`** — letter immutability, cross-session topic rules, progress marking. Letters retire when sessions retire; subsessions use `<L><digit>`; cross-cutting concerns get charters not letters.
- **`.claude/sessions/_workflow.md`** — slash command reference, daily lifecycle, hooks, "where state lives" table. Has a §0 listing every active deferral with its named resolution trigger.
- **`obsidian-vault/📒 Session Scope Map.md`** — canonical commit-attribution reference; nine numbered decision rules answer "which session does this commit belong to?"
- **`obsidian-vault/📊 Session Status Dashboard.md`** — live status. Active table is auto-regenerated from session-log YAML frontmatter by `tools/rebuild_views.py` — don't edit the active region directly.

## Hard Constraints — Never Change Without Asking

Load-bearing decisions. Stop and confirm with Ulugbek before touching:
- **No CSS-in-JS, no UI frameworks (Material UI, Bootstrap, Chakra, etc.)** — Tailwind is OK and load-bearing: configured with Telegram theme tokens (`tg-bg`, `tg-text`, etc. — see `frontend/tailwind.config.js`) and used across 18 of 24 components since the initial commit. Goal is bundle discipline, not zero-utility-CSS — current main chunk is 317K JS + 28K CSS, budget is **<400K JS + <50K CSS** for the main chunk
- **No chart libraries** — pure inline SVG
- **SQLite schema** (`backend/database.py`) — additive `ALTER TABLE` only, never destructive DDL
- **1C XLS parser** — `xlrd` + `cp1251`; never assume UTF-8 for 1C files
- **Cyrillic handling** — preserve cp1251 round-trip; NFC normalization for search
- **Deploy mechanism** — `railway up` from local is the **only** deploy path. Railway's GitHub auto-deploy was disabled on 2026-05-14 after it silently overwrote a `railway up` upload by rebuilding from the `origin/main` head. The GitHub repo is still linked for code-backup browsing; pushes do not trigger builds. If a deployed change goes missing, suspect a stale `railway up` working tree — not a missed GitHub push.

## Common Commands

### Development

```bash
# Backend (from repo root)
uvicorn backend.main:app --reload --port 8000

# Bot (separate terminal)
python -m bot.main

# Frontend dev server (proxies /api to :8000)
cd frontend && npm run dev

# Start both backend + bot together
python start_all.py
```

### Build & Deploy

```bash
# Build frontend (required before commit if frontend changed)
cd frontend && npm run build

# Deploy to Railway — local upload only. GitHub does NOT trigger deploys.
railway up

# Check deploy logs
railway logs
```

### Testing

```bash
# Run all tests (pytest.ini scopes to ./tests/ only — backend/tests/ must be run explicitly)
pytest

# Run a single test file
pytest tests/test_search.py

# Run a specific test
pytest tests/test_fifo.py::test_function_name

# Run backend endpoint tests (not picked up by default pytest config)
pytest backend/tests/

# Syntax check changed Python files
python -c "import py_compile; py_compile.compile('backend/main.py', doraise=True)"

# Production smoke test
python tools/smoke_test.py
```

Tests get a fresh temp SQLite per call via `tests/conftest.py::db`; the fixture re-points `backend.database.DATABASE_PATH` at a tempfile and runs full `init_db()`. Use `seed_products` fixture for a minimal product set.

### Linting & dead-code

```bash
# Install dev tools (one-time)
pip install -r requirements-dev.txt

# Run before commit — config in ruff.toml
python -m ruff check backend bot

# Auto-fix safe issues (F401 unused imports, etc.)
python -m ruff check backend bot --fix

# High-confidence dead-code scan (false-positive prone — verify before deleting)
python -m vulture backend bot --min-confidence 80
```

Ruff config (`ruff.toml`) targets py310, enables Pyflakes (`F`) + critical pycodestyle rules, ignores `E402` (this codebase intentionally embeds `from backend...` imports inside functions to defer loading). `F821` (undefined name) catches real bugs — never ignore at file level.

### Data Operations

```bash
# Import products from Excel
python -m backend.services.import_products

# Import clients
python -m backend.services.import_clients

# Update display names
python -m backend.services.update_display_names

# Sync product images
python -m backend.services.sync_images

# Backup database
python tools/backup_db.py
```

## Architecture

### Request Flow

- **User** → opens Telegram Mini App → loads React SPA from `/frontend/dist/` → calls `/api/*` endpoints
- **Admin** → sends bot command (e.g., `/prices` with XLS attachment) → bot parses file → POSTs to `/api/admin/*` with `admin_key`
- **App startup** → `init_db()` runs all `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE` migrations

### Railway Startup Sequence

`railway.toml`'s `startCommand` chains maintenance scripts before `start_all.py`. The order matters — each step assumes prior ones ran:

1. `backup_users backup` → snapshot users to JSON (pre-import safety net)
2. `tools/backup_db.py --startup` + `tools/verify_backup.py --startup` → daily DB backup
3. `import_products` → `update_display_names` → `sync_images` → `import_clients`
4. `backup_users restore` → re-link users (some imports clobber user→client links)
5. `classify_lifecycle` → `update_popularity` → `backfill_clients_history` → `fix_numeric_client_id_1c` → `prune_old_data`
6. `start_all.py` → launches FastAPI (port 8000) + bot polling together

If a deploy fails, check where in this chain it broke before assuming app-level bugs.

### Three Services

| Service | Entry Point | Port | Notes |
|---------|-------------|------|-------|
| FastAPI | `backend/main:app` | 8000 | Serves API, images, admin dashboard, and React SPA |
| Bot | `bot/main` | — | Telegram polling, admin commands, scheduled reminders |
| Frontend | `frontend/` (Vite) | 5173 | Dev only; production is pre-built static in `frontend/dist/` |

### Middleware Stack (order matters)

GZip → CacheControl → RateLimit → CORS → routers

### Static Mount Order (order matters)

`/admin` → `/images` → `/` (React SPA catch-all, must be last)

### Database

Single SQLite file (`data/catalog.db`, 40 tables from `CREATE TABLE IF NOT EXISTS` statements). All migrations in `backend/database.py:init_db()` using `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE`. Never use destructive DDL. WAL journal mode + foreign keys ON.

Access pattern: `from backend.database import get_db` returns a connection with a custom `_DictRow` factory supporting both `row["col"]` and `row[0]` (also supports `.get()`, unlike `sqlite3.Row`). Use `get_sibling_client_ids(conn, client_id)` to resolve multi-phone registrations sharing one `client_id_1c`.

### Bot Structure

- `bot/main.py` — dispatcher setup, registers handlers
- `bot/handlers/` — 8 handler modules: `admin, location, orders, registration, score, support, testclient, uploads`
- `bot/shared.py` — constants: BOT_TOKEN, WEBAPP_URL, DATABASE_PATH, group chat IDs, `is_admin()` helper
- `bot/help_spec.py` — declarative command registry; new commands register here to appear in `/help`
- `bot/reminders.py` — scheduled tasks (Tashkent GMT+5): **10:00** morning upload nudge, **17:00** EOD missing-uploads check, nightly backup. All bot commands are admin-only (gated by `is_admin()` against `ADMIN_IDS`).
- Handler naming: `async def cmd_name(message: Message)`

### 1C Upload Types (8 canonical)

Each ingested via a bot command replying to an XLS. Idempotent via `INSERT OR REPLACE` + `UNIQUE`.
- `balances_uzs` — client account balances in UZS
- `balances_usd` — client account balances in USD
- `stock` — warehouse inventory (units on hand per product)
- `prices` — wholesale price list (UZS + USD per product)
- `debtors` — outstanding receivables per client
- `realorders` — realizatsiya (shipments released to clients)
- `cash` — kassa (collected payments — basis for compensation calc, NOT realorders)
- `fxrate` — daily UZS↔USD exchange rate

### Frontend Routing

7 pages in `App.jsx`: Catalog → Producers → Products → ProductDetail → Cart → Register → Cabinet. No react-router — state-based page switching. Cart is server-side (persisted in DB, not localStorage).

## Key Patterns

- **Admin auth**: bot commands check `is_admin(message.from_user.id)` against `ADMIN_IDS` env var. API admin endpoints require `admin_key` form parameter validated against `ADMIN_API_KEY` env var.
- **1C imports**: always `cp1251` encoding for XLS files, parsed with `xlrd`. Use `INSERT OR REPLACE` with `UNIQUE` constraints. Workbook-level analytics (cashback, top-10, buckets) lives in parent-root scripts (`../build_cashback_model.py`, `../add_buckets_sheet.py`, `../add_top10_sheet.py`, `../relink_formulas.py`) — separate from app ingestion.
- **Pending patches** (`../session-F-*.patch`, `../session-g-phases-3-5.patch`, `../0001..0003-*.patch`) are historical reference — do not auto-apply; confirm with Ulugbek. `0003-multi-name-search.patch` is empty (0 bytes).
- **Dual currency**: UZS and USD tracked separately everywhere — never convert between them.
- **Search**: `search_text` pre-computed on import. Ranking: exact (4) > starts-with (3) > contains (2) > fuzzy/trigram (1, threshold ≥ 0.25).
- **Client auth**: phone-based matching against `allowed_clients` table. 4-layer auth: SQLite → JSON backup → Telegram CloudStorage → env var overrides.
- **Error handling**: uncaught exceptions auto-posted to Admin Telegram group (5-min rate limit per signature). Zero silent failures.
- **Analytics**: fire-and-forget background thread logging — failures never break user UX.

## Environment Variables

**Required**: `BOT_TOKEN`, `WEBAPP_URL`, `DATABASE_PATH` (default: `./data/catalog.db`), `IMAGES_DIR` (default: `./images`)

**Group chats**: `ADMIN_GROUP_CHAT_ID`, `DAILY_GROUP_CHAT_ID`, `ORDER_GROUP_CHAT_ID`, `INVENTORY_GROUP_CHAT_ID`

**Auth**: `ADMIN_IDS` (comma-separated Telegram user IDs), `ADMIN_API_KEY`, `ALWAYS_APPROVED_IDS`

See `.env.example` for full list. `../.credentials` exists in the parent folder root — never read, log, or commit its contents.

## Runtime

- Python 3.10.14 (from `runtime.txt`)
- Frontend dist is committed to git (Railway serves it directly)
- Product images in `images/` are also committed to git
- Railway persistent volume mounted at `/data` — DB, backups, archives live there
