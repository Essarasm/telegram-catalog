"""One-shot: retro-mark `daily_uploads` rows for upload_type='clients' as done.

Context — 2026-04-20: the /clients bot handler had a bug that suppressed
`track_daily_upload` when the uploaded file had zero NEW or UPDATED rows.
Plus the 1C "Справочник Контрагенты" parser had missing header aliases
(телефоны контрагента, наименование, полное наименование, etc.), so even
fresh uploads were silently failing to import. Combined effect: the
clients task in /today showed pending for several days despite the
operator sending the file on time.

This script runs on Railway startup (via --startup), scans daily_uploads for
any rows with upload_type='clients' and status in ('pending','failed') in
the 2026-04-15 → 2026-04-20 window, and flips them to status='done' with
an explanatory note. Idempotent — safe to re-run.
"""
import os
import sqlite3
import sys
from datetime import date

DB_PATH = os.environ.get('DATABASE_PATH', '/data/catalog.db')
AFFECTED_TYPE = 'clients'
# Covers the span when the parser+tracking bug was live. 2026-04-15 is
# a conservative start (Session F shipped early April; bug likely dates to
# whenever the 1C export format shifted to "Справочник Контрагенты").
WINDOW_START = '2026-04-15'
WINDOW_END = '2026-04-20'
NOTE = 'Auto-flipped 2026-04-20: parser/tracking bug retrofit (see tools/backfill_clients_history.py)'


def _iter_dates(start_iso, end_iso):
    from datetime import date as _d, timedelta
    y, m, d = [int(x) for x in start_iso.split('-')]
    cur = _d(y, m, d)
    y2, m2, d2 = [int(x) for x in end_iso.split('-')]
    end = _d(y2, m2, d2)
    while cur <= end:
        yield cur.isoformat()
        cur += timedelta(days=1)


def main(startup=False):
    if not os.path.exists(DB_PATH):
        if startup:
            print(f'[backfill_clients_history] DB not found at {DB_PATH}, skipping.')
            return 0
        raise FileNotFoundError(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Snapshot current state in window
    existing = {
        r['upload_date']: r['status']
        for r in conn.execute(
            """SELECT upload_date, status FROM daily_uploads
               WHERE upload_type = ?
                 AND upload_date BETWEEN ? AND ?""",
            (AFFECTED_TYPE, WINDOW_START, WINDOW_END),
        ).fetchall()
    }
    print(f'[backfill_clients_history] before: {dict(existing) or "(no rows)"}')

    inserted = 0
    updated_count = 0
    for iso_date in _iter_dates(WINDOW_START, WINDOW_END):
        cur_status = existing.get(iso_date)
        if cur_status == 'done':
            continue  # Already ok
        if cur_status in ('pending', 'failed'):
            conn.execute(
                """UPDATE daily_uploads
                   SET status='done',
                       notes = COALESCE(notes || ' | ', '') || ?,
                       updated_at = datetime('now')
                   WHERE upload_type = ? AND upload_date = ?""",
                (NOTE, AFFECTED_TYPE, iso_date),
            )
            updated_count += 1
        else:
            # No row exists at all — insert one tagged done with the retrofit note.
            conn.execute(
                """INSERT OR REPLACE INTO daily_uploads
                   (upload_date, upload_type, status, actual_count, row_count,
                    file_names, notes, updated_at)
                   VALUES (?, ?, 'done', 1, 0, '[]', ?, datetime('now'))""",
                (iso_date, AFFECTED_TYPE, NOTE),
            )
            inserted += 1
    conn.commit()

    after = {
        r['upload_date']: r['status']
        for r in conn.execute(
            """SELECT upload_date, status FROM daily_uploads
               WHERE upload_type = ?
                 AND upload_date BETWEEN ? AND ?
               ORDER BY upload_date""",
            (AFFECTED_TYPE, WINDOW_START, WINDOW_END),
        ).fetchall()
    }
    print(f'[backfill_clients_history] after:  {dict(after)}')
    print(f'[backfill_clients_history] inserted={inserted}  updated={updated_count}')

    conn.close()
    return inserted + updated_count


if __name__ == '__main__':
    startup_mode = '--startup' in sys.argv
    try:
        n = main(startup=startup_mode)
        print(f'[backfill_clients_history] done. {n} rows updated.')
    except Exception as e:
        if startup_mode:
            print(f'[backfill_clients_history] ERROR (startup, continuing boot): {e}')
            sys.exit(0)
        raise
