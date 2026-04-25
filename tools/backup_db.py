"""Nightly DB backup: SQLite `.dump` + gzip → /data/db_backups/

Rotates: keeps last 14 daily dumps. On every deploy this runs and if
no backup exists for today, creates one. Safe to re-run multiple times
a day (idempotent — same date file gets overwritten).

Recovery path:
  sqlite3 /data/catalog.db < /data/db_backups/catalog_YYYY-MM-DD.sql.gz

(…decompress first with `gunzip` — the .gz is to keep file size small
since the full DB dump can be ~100MB uncompressed.)
"""
import gzip
import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

TASHKENT = timezone(timedelta(hours=5))
DB_PATH = os.environ.get('DATABASE_PATH', '/data/catalog.db')
BACKUP_DIR = os.environ.get('DB_BACKUP_DIR', '/data/db_backups')
KEEP_LAST_N = int(os.environ.get('DB_BACKUP_KEEP', '7'))


def _iterdump_bytes(conn):
    """Stream every SQL statement from conn.iterdump() as UTF-8 bytes."""
    for line in conn.iterdump():
        yield (line + "\n").encode("utf-8")


def main(startup=False):
    if not os.path.exists(DB_PATH):
        if startup:
            print(f'[backup_db] DB not found at {DB_PATH}, skipping.')
            return None
        raise FileNotFoundError(DB_PATH)

    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
    except OSError as e:
        if startup:
            print(f'[backup_db] cannot create {BACKUP_DIR}: {e}. Skipping.')
            return None
        raise

    today_iso = datetime.now(TASHKENT).strftime('%Y-%m-%d')
    dump_path = os.path.join(BACKUP_DIR, f'catalog_{today_iso}.sql.gz')

    # Stream dump to gzipped file to avoid holding the whole thing in memory
    src = sqlite3.connect(DB_PATH)
    try:
        with gzip.open(dump_path, 'wb', compresslevel=9) as gz:
            for chunk in _iterdump_bytes(src):
                gz.write(chunk)
    finally:
        src.close()

    size = os.path.getsize(dump_path)
    print(f'[backup_db] wrote {dump_path} ({size / 1_000_000:.1f} MB gzipped)')

    # Rotate
    files = sorted([p for p in os.listdir(BACKUP_DIR)
                    if p.startswith('catalog_') and p.endswith('.sql.gz')])
    while len(files) > KEEP_LAST_N:
        old = files.pop(0)
        try:
            os.remove(os.path.join(BACKUP_DIR, old))
            print(f'[backup_db] rotated out {old}')
        except OSError as e:
            print(f'[backup_db] could not remove {old}: {e}')
    return dump_path


if __name__ == '__main__':
    startup_mode = '--startup' in sys.argv
    try:
        path = main(startup=startup_mode)
        print(f'[backup_db] done. Latest: {path}')
    except Exception as e:
        if startup_mode:
            print(f'[backup_db] ERROR (startup, continuing boot): {e}')
            sys.exit(0)
        raise
