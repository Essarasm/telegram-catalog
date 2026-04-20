"""Recompute products.popularity_score from real_order_items.

Popularity = count of distinct real_orders containing the product in the last
180 days. Used as a secondary sort in search results (within same match tier).

Re-runnable; idempotent. Wired into Railway startCommand.
"""
import os
import sqlite3
import sys
from datetime import date, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.environ.get('DATABASE_PATH', os.path.join(PROJECT_ROOT, 'data', 'catalog.db'))
WINDOW_DAYS = int(os.environ.get('POPULARITY_WINDOW_DAYS', '180'))


def main(startup=False):
    if not os.path.exists(DB_PATH):
        if startup:
            print(f'[update_popularity] DB not found at {DB_PATH}, skipping.')
            return 0
        raise FileNotFoundError(DB_PATH)

    cutoff = (date.today() - timedelta(days=WINDOW_DAYS)).isoformat()
    print(f'[update_popularity] DB={DB_PATH}  window={WINDOW_DAYS} days (cutoff {cutoff})')

    conn = sqlite3.connect(DB_PATH)

    # Check real_orders has data — skip if not (e.g., fresh deploy before first import)
    n_orders = conn.execute(
        "SELECT COUNT(*) FROM real_orders WHERE doc_date >= ?", (cutoff,)
    ).fetchone()[0]
    print(f'[update_popularity] real_orders in window: {n_orders}')
    if n_orders == 0:
        print('[update_popularity] no orders in window, skipping.')
        conn.close()
        return 0

    # Reset scores, then recompute via correlated subquery.
    # Using product_name_1c matching because the FK product_id is not reliably
    # populated on real_order_items rows.
    conn.execute("UPDATE products SET popularity_score = 0")
    conn.execute("""
        UPDATE products
        SET popularity_score = COALESCE((
            SELECT COUNT(DISTINCT ri.real_order_id)
            FROM real_order_items ri
            JOIN real_orders ro ON ro.id = ri.real_order_id
            WHERE ri.product_name_1c = products.name
              AND ro.doc_date >= ?
        ), 0)
    """, (cutoff,))
    conn.commit()

    # Stats
    row = conn.execute(
        """SELECT COUNT(*) AS total,
                  SUM(CASE WHEN popularity_score > 0 THEN 1 ELSE 0 END) AS non_zero,
                  MAX(popularity_score) AS max_score,
                  SUM(popularity_score) AS total_occurrences
           FROM products"""
    ).fetchone()
    print(f'[update_popularity] total={row[0]}  non_zero={row[1]}  max={row[2]}  sum={row[3]}')

    # Show top 5 for sanity
    top = conn.execute(
        "SELECT name, popularity_score FROM products WHERE popularity_score > 0 "
        "ORDER BY popularity_score DESC LIMIT 5"
    ).fetchall()
    print('[update_popularity] top 5:')
    for name, score in top:
        print(f'  {score:4}  {name[:60]}')

    conn.close()
    return row[1]  # non-zero count


if __name__ == '__main__':
    startup_mode = '--startup' in sys.argv
    try:
        non_zero = main(startup=startup_mode)
        print(f'[update_popularity] done. {non_zero} products scored.')
    except Exception as e:
        if startup_mode:
            print(f'[update_popularity] ERROR in startup mode, continuing boot: {e}')
            sys.exit(0)
        raise
