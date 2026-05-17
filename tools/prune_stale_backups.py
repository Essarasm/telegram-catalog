"""Prune /data/*.bak files older than 14 days.

Pre-operation safety backups (created by surgical tools like
`tools/dedup_allowed_clients.py`, the phone-slot cleanup tool, etc.) save
the full DB to `/data/catalog.db.pre_<op>_<ts>.bak` before doing anything
destructive. Without this rotation they accumulate forever — each one
is the full DB size (~60 MB) and 2 of them ate 28% of the /data volume
cap by 2026-05-17.

Why 14 days: matches the daily `db_backups/` retention in `backup_db.py`.
If a surgical op went wrong, the daily backups for the 14 days following
it have the post-op state — so the pre-op .bak's value-add window is
~14 days. After that it's dead weight.

Runs on every Railway deploy via `railway.toml` startCommand chain,
right after `backup_db.py` + `verify_backup.py`. Safe to re-run
(idempotent — only deletes if file is older than the cutoff).

Skip via env: SKIP_PRUNE_STALE_BAKS=1.
"""
import os
import time

DATA_DIR = os.environ.get('DATA_DIR', '/data')
RETENTION_DAYS = int(os.environ.get('BAK_RETENTION_DAYS', '14'))


def main(startup=False):
    if os.environ.get('SKIP_PRUNE_STALE_BAKS') == '1':
        print('[prune_stale_backups] skipped (SKIP_PRUNE_STALE_BAKS=1)')
        return

    if not os.path.isdir(DATA_DIR):
        if startup:
            print(f'[prune_stale_backups] {DATA_DIR} not found, skipping.')
            return
        raise FileNotFoundError(DATA_DIR)

    cutoff = time.time() - (RETENTION_DAYS * 86400)
    deleted_count = 0
    deleted_bytes = 0

    for fname in os.listdir(DATA_DIR):
        if not fname.endswith('.bak'):
            continue
        path = os.path.join(DATA_DIR, fname)
        if not os.path.isfile(path):
            continue
        try:
            mtime = os.path.getmtime(path)
        except OSError as e:
            print(f'[prune_stale_backups] stat failed for {fname}: {e}')
            continue
        if mtime >= cutoff:
            continue
        try:
            size = os.path.getsize(path)
            os.unlink(path)
            deleted_count += 1
            deleted_bytes += size
            age_days = (time.time() - mtime) / 86400
            print(
                f'[prune_stale_backups] removed {fname} '
                f'({size / 1024 / 1024:.1f} MB, age {age_days:.0f}d)'
            )
        except OSError as e:
            print(f'[prune_stale_backups] failed to remove {fname}: {e}')

    if deleted_count:
        print(
            f'[prune_stale_backups] {deleted_count} file(s), '
            f'{deleted_bytes / 1024 / 1024:.1f} MB total freed'
        )
    else:
        print('[prune_stale_backups] nothing to prune')


if __name__ == '__main__':
    startup = '--startup' in os.sys.argv
    main(startup=startup)
