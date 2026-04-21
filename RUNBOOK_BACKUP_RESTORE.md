# Backup & Restore Runbook

**Scope:** production SQLite database for the Rassvet Catalogue Telegram Mini App.
**Owner:** Ulugbek (product owner). **On-call:** whoever has Railway CLI access.
**Last verified:** 2026-04-19.

> If you're reading this during an incident, jump to **[Emergency Restore](#emergency-restore)**.

---

## 1. What gets backed up

| Artifact | Source | Where it lives | Frequency | Retention |
|---|---|---|---|---|
| SQLite DB (`.sql.gz`) | `sqlite3 .iterdump` + gzip | `/data/db_backups/` on Railway volume | Nightly 03:00 Tashkent + every deploy startup | Last 14 |
| Offsite DB copy | Admin group Telegram | chat `-5224656051` | Nightly 03:00 Tashkent | Unbounded (manual prune) |
| `users_backup.json` | `/users/backup` writer | `/data/users_backup.json` | On every successful login refresh | Latest only |
| `approved_overrides.json` | Manual edits | Repo root (`telegram-catalog/approved_overrides.json`) | On edit | Git-tracked |
| CloudStorage (per-user) | Telegram Mini App | Telegram servers | Per-session | Per Telegram policy |

Everything above is idempotently restorable. There is no separate WAL — the dump is a full logical snapshot.

---

## 2. Pre-flight: verify a backup is fresh

Run these two commands before trusting the last backup:

```bash
# From your laptop, against prod
curl -s https://telegram-catalog-production.up.railway.app/api/health/deep \
  | python3 -c "import sys, json; d=json.load(sys.stdin); \
      print('latest_backup:', d['checks'].get('latest_db_backup')); \
      print('backup_bytes :', d['checks'].get('latest_db_backup_bytes')); \
      print('db_bytes     :', d['checks'].get('db_bytes'))"
```

Expected: `latest_db_backup` is today's or yesterday's date, backup bytes > 1 MB.

To actively test the latest backup (decompress + load into a temp DB + sanity-check row counts):

```bash
railway run python tools/verify_backup.py
```

If `verify_backup` fails, the nightly job also alerts the Admin group — check there first.

---

## 3. List available backups

```bash
# Local-to-Railway
railway run ls -lh /data/db_backups/

# Or via health endpoint (easier, no shell hop)
curl -s https://telegram-catalog-production.up.railway.app/api/health/deep \
  | python3 -m json.tool | grep -A1 backup
```

Offsite copies: search the Admin Telegram group (chat ID `-5224656051`) for the
most recent message with a `.sql.gz` attachment — that's the daily offsite copy.

---

## 4. Emergency restore

**Assume:** production DB is corrupted or lost. Goal: get the Mini App back on the most recent good snapshot with minimal data loss.

### Step 1 — Snapshot the broken state (DO NOT skip)

Even if the DB looks "empty" or corrupt, keep a copy. Investigating a production
incident later is impossible if you've already overwritten the evidence.

```bash
railway run cp /data/catalog.db /data/catalog.db.broken-$(date +%Y%m%d-%H%M)
```

### Step 2 — Pick your restore source (preference order)

1. **Latest in `/data/db_backups/`** — nightly `.sql.gz`, always the first choice.
2. **Offsite copy from Admin Telegram group** — use if (1) the volume itself is gone, or (2) you need a point-in-time older than 14 days.
3. **Previous Railway volume snapshot** — Railway dashboard → project → volume → "Restore from backup". Coarse-grained (whole volume) but recovers everything including images.

### Step 3 — Restore

**From nightly dump (most common):**

```bash
# Pick the file (usually yesterday's if this is a morning incident)
BACKUP=/data/db_backups/catalog_2026-04-19.sql.gz

railway run bash -c "
  set -e
  mv /data/catalog.db /data/catalog.db.pre-restore-\$(date +%H%M%S)
  gunzip -c $BACKUP | sqlite3 /data/catalog.db
  sqlite3 /data/catalog.db 'PRAGMA integrity_check;'
"
```

Expected last line: `ok`. Anything else → STOP and try the next backup.

**From Telegram offsite copy:**

1. Download the `.sql.gz` from the Admin group to your laptop.
2. Upload to Railway volume:

   ```bash
   railway run bash -c "mkdir -p /data/restore_tmp"
   # Use Railway's file upload (Dashboard → Volumes → Upload) or:
   railway run bash -c "cat > /data/restore_tmp/dump.sql.gz" < path/to/downloaded.sql.gz
   ```

3. Then apply as in the nightly-dump step above, pointing `BACKUP` at `/data/restore_tmp/dump.sql.gz`.

### Step 4 — Bounce the app

```bash
railway redeploy      # or: railway service restart
```

The `railway.toml` start command will re-run startup migrations (`init_db`), sync images,
and verify the backup. Watch `railway logs` for `✓ startup complete`.

### Step 5 — Smoke test

```bash
python3 tools/smoke_test.py
```

All 12 checks must pass. If any fail, the DB is loaded but something else is wrong — check `/api/health/deep` for clues and consult the session log for the most recent schema migration.

### Step 6 — Tell users

Post in the Admin group:

> ⚠️ Tiklash amalga oshirildi. `<timestamp>` dan oldingi buyurtmalar va ro'yxatdan o'tishlar yo'qolgan bo'lishi mumkin. Qayta kiriting.

Then page Ulugbek if he isn't already aware.

---

## 5. Recovering *partial* data

Sometimes only one table is wrong (e.g., `real_orders` re-import went sideways).
Don't restore the whole DB — extract just the table from the backup:

```bash
railway run bash -c "
  gunzip -c /data/db_backups/catalog_2026-04-18.sql.gz \
    | grep -E '^(CREATE TABLE real_orders|INSERT INTO real_orders)' \
    > /tmp/real_orders_only.sql

  sqlite3 /data/catalog.db 'DROP TABLE real_orders;'
  sqlite3 /data/catalog.db < /tmp/real_orders_only.sql
"
```

Then run whatever import the user meant to run originally (`/realorders`, etc.)
so downstream tables (`real_order_items`) stay consistent.

---

## 6. Restoring user auth state

The DB covers `users`, `allowed_clients`, and `orders` — but the approval state
is also duplicated in three other places (intentionally, for resilience):

1. `/data/users_backup.json` — written on every successful check/approve.
2. Telegram CloudStorage (per-device) — read by the frontend on boot.
3. `approved_overrides.json` in the repo — env-level allowlist.

If the restored DB has stale approvals, users can re-authenticate and the
`/users/check` endpoint will self-heal the `is_approved` flag from the JSON
backup. No manual intervention needed.

---

## 7. Schema migrations vs. restore

Restored DB was from before a schema change? That's fine — `init_db()` runs on
every startup and applies `ALTER TABLE` additively. It never drops columns.

Check `backend/database.py` → `init_db()` for the current migration list.
`SCHEMA_VERSION` is tracked in the `meta` table.

---

## 8. What *not* to do

- **Don't** restore without snapshotting the broken DB first.
- **Don't** `rm /data/catalog.db` before the new one is in place. Move it.
- **Don't** run `sqlite3 < dump.sql` directly onto a live DB — always restore onto a moved-aside filename, then swap.
- **Don't** force-push changes to fix a schema issue. Migrations are additive; add a new `ALTER TABLE` in `init_db()` and deploy.
- **Don't** skip `PRAGMA integrity_check` — a partially-restored DB will fail silently on first write.

---

## 9. Testing this runbook

Every quarter, run a fire drill:

```bash
# On a throwaway Railway service (NOT prod):
gunzip -c catalog_YYYY-MM-DD.sql.gz | sqlite3 /tmp/test_restore.db
sqlite3 /tmp/test_restore.db "PRAGMA integrity_check; SELECT COUNT(*) FROM users;"
```

Log the drill result in `session-logs/`. If the last drill is older than 6 months, this runbook is stale — re-test before trusting it.
