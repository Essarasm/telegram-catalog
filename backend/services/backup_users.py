"""Backup & restore users table across Railway deploys.

The SQLite DB can be wiped when Railway recreates the volume or the
import_products script drops/recreates tables.  This module keeps a
separate JSON file (/data/users_backup.json) that survives those events.

Usage (in startCommand):
  1. BEFORE init_db / import: python -m backend.services.backup_users backup
  2. AFTER  init_db / import: python -m backend.services.backup_users restore
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

BACKUP_PATH = os.getenv("USERS_BACKUP_PATH", "/data/users_backup.json")
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")


def backup():
    """Save all users from SQLite to JSON (if the DB & table exist)."""
    import sqlite3
    if not os.path.exists(DATABASE_PATH):
        print("[backup_users] No DB found, skipping backup.")
        return

    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        # Check if users table exists
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        ).fetchall()]
        if 'users' not in tables:
            print("[backup_users] No users table, skipping backup.")
            conn.close()
            return

        rows = conn.execute("SELECT * FROM users").fetchall()
        conn.close()

        if not rows:
            print("[backup_users] Users table empty, skipping backup.")
            return

        users = [dict(r) for r in rows]
        # Merge with existing backup (don't lose users that were already backed up)
        existing = []
        if os.path.exists(BACKUP_PATH):
            try:
                with open(BACKUP_PATH, 'r') as f:
                    existing = json.load(f)
            except (json.JSONDecodeError, IOError):
                existing = []

        # Merge: current DB users override, keep any extras from backup
        merged = {u['telegram_id']: u for u in existing}
        for u in users:
            merged[u['telegram_id']] = u
        final = list(merged.values())

        # Ensure backup directory exists
        backup_dir = os.path.dirname(BACKUP_PATH)
        if backup_dir and not os.path.exists(backup_dir):
            os.makedirs(backup_dir, exist_ok=True)

        # Atomic write via temp file
        import tempfile
        tmp_fd, tmp_path = tempfile.mkstemp(dir=backup_dir or '/data', suffix='.json.tmp')
        try:
            with os.fdopen(tmp_fd, 'w') as f:
                json.dump(final, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, BACKUP_PATH)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        print(f"[backup_users] Backed up {len(final)} users to {BACKUP_PATH}")

    except Exception as e:
        print(f"[backup_users] Backup error (non-fatal): {e}")
        import traceback
        traceback.print_exc()


def restore():
    """Restore users from JSON backup into the current DB."""
    if not os.path.exists(BACKUP_PATH):
        print("[backup_users] No backup file found, skipping restore.")
        return

    try:
        with open(BACKUP_PATH, 'r') as f:
            users = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"[backup_users] Cannot read backup: {e}")
        return

    if not users:
        print("[backup_users] Backup is empty, nothing to restore.")
        return

    from backend.database import get_db
    conn = get_db()

    restored = 0
    for u in users:
        try:
            conn.execute(
                """INSERT INTO users (telegram_id, phone, first_name, last_name,
                   username, latitude, longitude, is_approved, client_id, registered_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(telegram_id) DO UPDATE SET
                       phone = COALESCE(excluded.phone, users.phone),
                       first_name = COALESCE(excluded.first_name, users.first_name),
                       last_name = COALESCE(excluded.last_name, users.last_name),
                       username = COALESCE(excluded.username, users.username),
                       latitude = COALESCE(excluded.latitude, users.latitude),
                       longitude = COALESCE(excluded.longitude, users.longitude),
                       is_approved = MAX(excluded.is_approved, users.is_approved),
                       client_id = COALESCE(excluded.client_id, users.client_id)""",
                (
                    u.get('telegram_id'),
                    u.get('phone'),
                    u.get('first_name'),
                    u.get('last_name'),
                    u.get('username'),
                    u.get('latitude'),
                    u.get('longitude'),
                    u.get('is_approved', 0),
                    u.get('client_id'),
                    u.get('registered_at'),
                ),
            )
            restored += 1
        except Exception as e:
            print(f"[backup_users] Failed to restore user {u.get('telegram_id')}: {e}")

    conn.commit()
    conn.close()
    print(f"[backup_users] Restored {restored} users from backup.")


def save_user_to_backup(user_dict):
    """Immediately persist a single user to the backup file.

    Call this after /register or /approve so the backup is always
    up-to-date — not just at startup time.
    """
    import tempfile

    tid = user_dict.get('telegram_id')
    if not tid:
        print("[backup_users] save_user_to_backup: no telegram_id, skipping")
        return

    try:
        # Ensure backup directory exists
        backup_dir = os.path.dirname(BACKUP_PATH)
        if backup_dir and not os.path.exists(backup_dir):
            os.makedirs(backup_dir, exist_ok=True)
            print(f"[backup_users] Created backup directory: {backup_dir}")

        existing = []
        if os.path.exists(BACKUP_PATH):
            try:
                with open(BACKUP_PATH, 'r') as f:
                    existing = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"[backup_users] Could not read existing backup: {e}")
                existing = []

        merged = {u['telegram_id']: u for u in existing if 'telegram_id' in u}

        # If user already exists in backup, preserve is_approved=1
        old = merged.get(tid, {})
        old_approved = old.get('is_approved', 0) or 0
        new_approved = user_dict.get('is_approved', 0) or 0
        user_dict['is_approved'] = max(old_approved, new_approved)
        merged[tid] = user_dict

        # Atomic write: write to temp file first, then rename
        final_data = list(merged.values())
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=backup_dir or '/data',
            suffix='.json.tmp'
        )
        try:
            with os.fdopen(tmp_fd, 'w') as f:
                json.dump(final_data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, BACKUP_PATH)
            print(f"[backup_users] Saved user {tid} to backup ({len(final_data)} total users)")
        except Exception as e:
            # Clean up temp file if rename failed
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise e

    except Exception as e:
        print(f"[backup_users] save_user_to_backup ERROR for user {tid}: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "backup"
    if action == "backup":
        backup()
    elif action == "restore":
        restore()
    else:
        print(f"Usage: python -m backend.services.backup_users [backup|restore]")
