# Rassvet Catalogue — Telegram Mini App

Wholesale construction-materials ordering for Rassvet, a family-owned B2B distributor in Uzbekistan. Three-tier architecture deployed on Railway.

```
Telegram client ──► Mini App (React SPA, frontend/) ──► FastAPI (backend/) ──► SQLite (/data/catalog.db)
                                                            ▲
                                                            │
                                                  Aiogram bot (bot/) ──► admin commands, scheduled reminders
```

Production: https://telegram-catalog-production.up.railway.app
Health: https://telegram-catalog-production.up.railway.app/api/health/deep

---

## Quick start (fresh clone)

```bash
git clone https://github.com/Essarasm/telegram-catalog.git
cd telegram-catalog

# 1. Install pre-commit hooks (closes the schema/chat-ID/bundle drift classes).
bash scripts/install-git-hooks.sh

# 2. Backend setup (Python 3.10.14 to match Railway runtime.txt).
python3.10 -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt

# 3. Frontend setup.
cd frontend && npm ci && cd ..

# 4. Env vars — copy + fill.
cp .env.example .env
$EDITOR .env

# 5. Verify everything works.
.venv/bin/python -m pytest -q                       # backend + bot tests
cd frontend && npm run build && cd ..                # frontend build
python3 tools/check_bundle_size.py                   # bundle within budget
bash tools/audit.sh                                  # foundation health check
```

If everything is ✅ at the end of step 5, you're set up.

---

## Common commands

```bash
# ── Dev servers (run in separate terminals) ──
.venv/bin/python -m uvicorn backend.main:app --reload --port 8000   # backend
.venv/bin/python -m bot.main                                          # bot
cd frontend && npm run dev                                            # frontend (proxies /api)

# ── Single-command "run both" ──
.venv/bin/python start_all.py

# ── Tests ──
.venv/bin/python -m pytest -q                       # all 200 tests
.venv/bin/python -m pytest tests/test_<name>.py     # one file
.venv/bin/python -m pytest -k <pattern> -v          # by name

# ── Lint ──
.venv/bin/python -m ruff check backend bot
.venv/bin/python -m vulture backend bot --min-confidence 80

# ── Build + deploy (production) ──
cd frontend && ./node_modules/.bin/vite build && cd ..
curl -sS https://telegram-catalog-production.up.railway.app/api/health/deep \
    | python3 -m json.tool | grep latest_db_backup   # backup fresh?
railway up                                            # deploy from local
railway logs                                          # watch the rollout

# ── Database ──
sqlite3 data/catalog.db                              # interactive SQL
sqlite3 data/catalog.db "SELECT MAX(version) FROM schema_version"
.venv/bin/python tools/backup_db.py                  # manual local backup

# ── Foundation health ──
bash tools/audit.sh                                  # fast (<30 sec)
# `/audit` in Claude Code for the deep agent-driven audit
```

---

## Where the docs live

This project has multiple layers of documentation, each with a specific purpose:

| Location | Purpose | Read when |
|---|---|---|
| `CLAUDE.md` (this directory) | Project overview, hard constraints, architecture, env vars | Onboarding + before non-trivial changes |
| `RUNBOOK_BACKUP_RESTORE.md` | Backup & emergency restore procedures | During an incident |
| `.claude/rules/0[1-9]-*.md` | Numbered project rules (session logging, code conventions, git workflow, etc.) | Before starting any work block |
| `.claude/sessions/_index.md` | Active session charters (letter-coded + cross-cutting) | When attributing commit / picking up unfinished work |
| `../obsidian-vault/ADRs/` | Architecture Decision Records — *why* major calls were made | When you want to understand "why didn't they just do the obvious thing?" |
| `../obsidian-vault/🐛 Error Log.md` | Every bug fixed, with pattern tag | First thing to grep when investigating unexpected behavior |
| `../obsidian-vault/📊 Session Status Dashboard.md` | Live session status | Daily check / morning brief |
| `../Notion Command Center.md` | Single page with active TODOs, weekly cadence, habits | Daily ritual + weekly planning |

The `obsidian-vault/` lives in the parent directory (it's a separate Obsidian workspace, not part of the deployable code).

---

## Architecture at a glance

### Request flow

- **User** → opens Telegram Mini App → loads React SPA from `/frontend/dist/` → calls `/api/*` endpoints
- **Admin** → sends bot command (e.g. `/prices` with XLS attachment) → bot parses file → POSTs to `/api/admin/*` with `admin_key`
- **Boot** → `railway.toml` startCommand runs backup → import chain → maintenance scripts → `start_all.py` → FastAPI + bot together

### Three services

| Service | Entry point | Port | Notes |
|---|---|---|---|
| FastAPI | `backend/main:app` | 8000 | Serves API, images, admin dashboard, React SPA |
| Bot | `bot/main` | — | Telegram long-polling, admin commands, scheduled reminders |
| Frontend | `frontend/` (Vite) | 5173 | Dev only; production is pre-built `frontend/dist/` |

### Hard constraints (never change without ADR + discussion)

See `CLAUDE.md` "Hard Constraints" section. Summary:

- **No CSS-in-JS, no UI frameworks beyond Tailwind** (ratified in [ADR-0001](../obsidian-vault/ADRs/0001-ratify-tailwind.md))
- **All schema in `backend/database.py:init_db()`** (ratified in [ADR-0002](../obsidian-vault/ADRs/0002-schema-migrations-in-init-db.md))
- **Chat IDs in `backend/services/group_config.py`** (ratified in [ADR-0003](../obsidian-vault/ADRs/0003-canonical-config-modules.md))
- **`railway.toml` is the canonical boot** (ratified in [ADR-0004](../obsidian-vault/ADRs/0004-railway-toml-canonical-boot.md))
- **Bundle budget: <400K JS + <50K CSS for the main chunk** (enforced by `tools/check_bundle_size.py`, pre-commit hook, and CI)

---

## Pre-commit hooks (foundation guards)

The hook at `scripts/git-hooks/pre-commit` (symlinked into `.git/hooks/`) enforces:

1. **Python syntax** — fails commit on `py_compile` error
2. **Schema-DDL discipline** — blocks `CREATE/ALTER TABLE` outside `backend/database.py`
3. **Chat-ID discipline** — blocks `os.getenv("..._CHAT_ID", "literal")` outside `group_config.py`
4. **Tests** — fails commit if `pytest -q` doesn't pass
5. **Frontend build** — rebuilds `dist/` if frontend source changed
6. **Bundle budget** — fails commit if dist exceeds the JS/CSS budget

Override env vars (for the rare legitimate exception):

```bash
SKIP_SCHEMA_CHECK=1      # bypass schema-DDL guard
SKIP_CHATID_CHECK=1      # bypass chat-ID guard
SKIP_FRONTEND_BUILD=1    # skip frontend rebuild (docs-only commits)
SKIP_BUNDLE_CHECK=1      # skip bundle-budget check
```

These are honor-system. CI mirrors the same checks at the remote-repo level (see `.github/workflows/ci.yml`), so override-and-push will be caught there.

---

## Deploy

```bash
# 0. Pre-flight: confirm prod backup is fresh
curl -sS https://telegram-catalog-production.up.railway.app/api/health/deep \
    | python3 -m json.tool | grep latest_db_backup

# 1. Build frontend (only if frontend source changed)
cd frontend && ./node_modules/.bin/vite build && cd ..

# 2. Deploy
railway up

# 3. Watch logs for migration messages + Uvicorn start
railway logs

# 4. Smoke-test
python3 tools/smoke_test.py
```

Railway uses Railpack + `railway.toml`'s `startCommand`. GitHub push does **not** trigger a deploy — only `railway up` does. See [ADR-0004](../obsidian-vault/ADRs/0004-railway-toml-canonical-boot.md).

---

## When something goes wrong

1. **First** — grep the Error Log (`../obsidian-vault/🐛 Error Log.md`) for keywords from the symptom. The pattern index at the bottom is the canonical "have we seen this before?" lookup. Per `.claude/rules/08-error-debugging.md` Rule 1.
2. **Then** — check `railway logs` for recent errors.
3. **Then** — `bash tools/audit.sh` for a foundation health snapshot.
4. **Finally** — if it's genuinely new, multi-hypothesis analysis (`.claude/rules/08-error-debugging.md` Rule 2), then fix + append to Error Log.

---

## License

Private. Not for redistribution.
