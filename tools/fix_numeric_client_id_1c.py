"""One-shot retrofit: repair allowed_clients rows whose `client_id_1c` column
got overwritten with a numeric "Код" value from a /clients upload.

Background — 2026-04-20: the /clients parser added aliases for the 1C
Справочник Контрагенты export. One alias, `"код": "client_id_1c"`, wrongly
routed the numeric 1C internal code (e.g. "1701") into a field meant to
hold the 1C NAME string (e.g. "АЗИЗ Акрамов Челак"). This broke the
/testclient picker: numeric labels ("1701", "1490", …) showed up as
buttons instead of client names.

Behaviour:
  For every row where client_id_1c is purely digits:
    - If name is a proper non-numeric string → copy name into client_id_1c.
    - Else                                   → clear client_id_1c (set NULL).
Idempotent: second run finds nothing to fix.
"""
import os
import sqlite3
import sys

DB_PATH = os.environ.get('DATABASE_PATH', '/data/catalog.db')


def main(startup=False):
    if not os.path.exists(DB_PATH):
        if startup:
            print(f'[fix_numeric_client_id_1c] DB not found at {DB_PATH}, skipping.')
            return 0
        raise FileNotFoundError(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """SELECT id, name, client_id_1c
           FROM allowed_clients
           WHERE client_id_1c IS NOT NULL AND client_id_1c != ''"""
    ).fetchall()

    fixed_from_name = 0
    cleared = 0
    for r in rows:
        cid = (r['client_id_1c'] or '').strip()
        if not cid or not cid.isdigit():
            continue  # already a proper string
        nm = (r['name'] or '').strip()
        if nm and not nm.isdigit():
            conn.execute(
                "UPDATE allowed_clients SET client_id_1c = ? WHERE id = ?",
                (nm, r['id']),
            )
            fixed_from_name += 1
        else:
            conn.execute(
                "UPDATE allowed_clients SET client_id_1c = NULL WHERE id = ?",
                (r['id'],),
            )
            cleared += 1
    conn.commit()

    total_affected = fixed_from_name + cleared
    print(f'[fix_numeric_client_id_1c] scanned {len(rows)} rows with client_id_1c; '
          f'fixed {fixed_from_name} from name, cleared {cleared}, '
          f'total affected {total_affected}.')

    # Verify: no purely-numeric client_id_1c should remain
    leftover = conn.execute(
        "SELECT COUNT(*) FROM allowed_clients "
        "WHERE client_id_1c IS NOT NULL AND client_id_1c != '' "
        "AND client_id_1c GLOB '[0-9]*' AND NOT client_id_1c GLOB '*[^0-9]*'"
    ).fetchone()[0]
    print(f'[fix_numeric_client_id_1c] remaining numeric-only client_id_1c: {leftover}')

    conn.close()
    return total_affected


if __name__ == '__main__':
    startup_mode = '--startup' in sys.argv
    try:
        n = main(startup=startup_mode)
        print(f'[fix_numeric_client_id_1c] done. {n} rows updated.')
    except Exception as e:
        if startup_mode:
            print(f'[fix_numeric_client_id_1c] ERROR (startup, continuing boot): {e}')
            sys.exit(0)
        raise
