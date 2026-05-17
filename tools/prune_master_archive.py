"""Prune /data/master_archive/Client_Master_*.xlsx files older than 12 weeks.

`_send_master_auto_export()` in bot/reminders.py writes a weekly snapshot
of the Client Master xlsx to /data/master_archive/ on Mondays at 08:00
Tashkent. Each is ~5 MB. Without rotation this grows ~22 MB/month, ~260
MB/year — well into volume-cap territory over time.

Why 12 weeks (~3 months): aligns with the project's quarterly-audit
mental model. 12 weekly snapshots = ~66 MB ceiling. Enough history for
detecting "this client used to be active in February" patterns, not so
much that it crowds out room for daily DB backups.

Runs on every Railway deploy via `railway.toml` startCommand chain,
right after `prune_stale_backups.py`. Safe to re-run (idempotent).

Skip via env: SKIP_PRUNE_MASTER_ARCHIVE=1.
Tunable via env: MASTER_ARCHIVE_RETENTION_DAYS=N (default 84).
"""
import os
import time

ARCHIVE_DIR = os.environ.get('MASTER_ARCHIVE_DIR', '/data/master_archive')
RETENTION_DAYS = int(os.environ.get('MASTER_ARCHIVE_RETENTION_DAYS', '84'))


def main(startup=False):
    if os.environ.get('SKIP_PRUNE_MASTER_ARCHIVE') == '1':
        print('[prune_master_archive] skipped (SKIP_PRUNE_MASTER_ARCHIVE=1)')
        return

    if not os.path.isdir(ARCHIVE_DIR):
        if startup:
            print(f'[prune_master_archive] {ARCHIVE_DIR} not found, skipping.')
            return
        raise FileNotFoundError(ARCHIVE_DIR)

    cutoff = time.time() - (RETENTION_DAYS * 86400)
    deleted_count = 0
    deleted_bytes = 0

    for fname in os.listdir(ARCHIVE_DIR):
        if not (fname.startswith('Client_Master_') and fname.endswith('.xlsx')):
            continue
        path = os.path.join(ARCHIVE_DIR, fname)
        if not os.path.isfile(path):
            continue
        try:
            mtime = os.path.getmtime(path)
        except OSError as e:
            print(f'[prune_master_archive] stat failed for {fname}: {e}')
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
                f'[prune_master_archive] removed {fname} '
                f'({size / 1024 / 1024:.1f} MB, age {age_days:.0f}d)'
            )
        except OSError as e:
            print(f'[prune_master_archive] failed to remove {fname}: {e}')

    if deleted_count:
        print(
            f'[prune_master_archive] {deleted_count} file(s), '
            f'{deleted_bytes / 1024 / 1024:.1f} MB total freed'
        )
    else:
        print('[prune_master_archive] nothing to prune')


if __name__ == '__main__':
    startup = '--startup' in os.sys.argv
    main(startup=startup)
