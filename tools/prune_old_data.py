"""Nightly retention cleanup for fast-growing tables.

Runs on every deploy via `--startup` (idempotent; only deletes rows
older than their configured retention). Keeps DB size bounded so
SQLite performance doesn't degrade over months of operation.

Retention (generous defaults — adjust in env if needed):
  - search_logs:             180 days     (typical analytics depth)
  - search_clicks:           180 days
  - product_interest_clicks: 365 days     (demand signal — keep longer)
  - phone_history:           keep forever (audit trail)
  - master_upload_log:       keep forever (audit trail)
  - support_threads:         90 days      (ops conversation history)

Does NOT touch: allowed_clients, users, products, orders, real_orders,
client_balances, client_debts, client_payments, client_scores,
order_feedback, reports. These are business-critical and never pruned.
"""
import os
import sqlite3
import sys

DB_PATH = os.environ.get('DATABASE_PATH', '/data/catalog.db')

RETENTION_RULES = [
    # (table_name, date_column, keep_days)
    ("search_logs",             "created_at", int(os.environ.get("RETAIN_SEARCH_LOGS_DAYS",   "180"))),
    ("search_clicks",           "created_at", int(os.environ.get("RETAIN_SEARCH_CLICKS_DAYS", "180"))),
    ("product_interest_clicks", "clicked_at", int(os.environ.get("RETAIN_INTEREST_CLICKS_DAYS", "365"))),
    ("support_threads",         "created_at", int(os.environ.get("RETAIN_SUPPORT_THREADS_DAYS", "90"))),
]


def main(startup=False):
    if not os.path.exists(DB_PATH):
        if startup:
            print(f'[prune_old_data] DB not found at {DB_PATH}, skipping.')
            return 0
        raise FileNotFoundError(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    deleted_total = 0
    for table, date_col, keep_days in RETENTION_RULES:
        # Skip if table doesn't exist (schema drift between envs)
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if not exists:
            continue
        try:
            cur = conn.execute(
                f"DELETE FROM {table} WHERE {date_col} < datetime('now', ?)",
                (f'-{keep_days} days',),
            )
            n = cur.rowcount or 0
            if n > 0:
                print(f'[prune_old_data] {table}: deleted {n} rows older than {keep_days} days')
                deleted_total += n
        except sqlite3.OperationalError as e:
            print(f'[prune_old_data] {table}: skipped ({e})')

    if deleted_total > 0:
        conn.commit()
        conn.execute("VACUUM")
        print(f'[prune_old_data] VACUUM complete. Total deleted: {deleted_total}')
    else:
        print('[prune_old_data] Nothing to prune.')
    conn.close()
    return deleted_total


if __name__ == '__main__':
    startup_mode = '--startup' in sys.argv
    try:
        n = main(startup=startup_mode)
        print(f'[prune_old_data] done. {n} rows deleted total.')
    except Exception as e:
        if startup_mode:
            print(f'[prune_old_data] ERROR (startup, continuing boot): {e}')
            sys.exit(0)
        raise
