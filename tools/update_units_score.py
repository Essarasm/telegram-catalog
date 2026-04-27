"""Recompute products.units_score and categories.units_score from real_order_items.

Score = 0.6 × (units shipped last 30d / 30) + 0.4 × (units shipped prior 60d / 60).
Per-day rates are normalized so the number is interpretable as "recency-weighted
avg units/day". Drives the default catalog sort (top sellers float, cold sinks).

Aggregation:
  products.units_score   — per-product weighted blend (matched by product_name_1c
                           because the FK product_id on real_order_items is not
                           reliably populated — same pattern as update_popularity.py).
  categories.units_score — SUM of products.units_score grouped by category_id.

Producer ranking is computed on-the-fly inside the producers-in-category endpoint
(category-scoped, not global), so producers gets no precomputed column.

Re-runnable; idempotent. Wired into:
  - railway.toml startCommand (boot)
  - end of apply_real_orders_import (fresh shipments)
  - bot/reminders.py 04:30 Tashkent (date-window roll-over on no-import days)
"""
import os
import sqlite3
import sys
from datetime import date, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.environ.get('DATABASE_PATH', os.path.join(PROJECT_ROOT, 'data', 'catalog.db'))


def main(startup=False):
    if not os.path.exists(DB_PATH):
        if startup:
            print(f'[update_units_score] DB not found at {DB_PATH}, skipping.')
            return 0
        raise FileNotFoundError(DB_PATH)

    today = date.today()
    cutoff_30 = (today - timedelta(days=30)).isoformat()
    cutoff_90 = (today - timedelta(days=90)).isoformat()
    print(f'[update_units_score] DB={DB_PATH}  windows: last30>={cutoff_30}  prior60={cutoff_90}..{cutoff_30}')

    conn = sqlite3.connect(DB_PATH)

    n_orders = conn.execute(
        "SELECT COUNT(*) FROM real_orders WHERE doc_date >= ?", (cutoff_90,)
    ).fetchone()[0]
    print(f'[update_units_score] real_orders in 90d window: {n_orders}')
    if n_orders == 0:
        print('[update_units_score] no orders in window, skipping.')
        conn.close()
        return 0

    conn.execute("UPDATE products SET units_score = 0")
    conn.execute("""
        UPDATE products
        SET units_score = COALESCE((
            SELECT 0.6 * SUM(CASE WHEN ro.doc_date >= ? THEN ri.quantity ELSE 0 END) / 30.0
                 + 0.4 * SUM(CASE WHEN ro.doc_date <  ? AND ro.doc_date >= ? THEN ri.quantity ELSE 0 END) / 60.0
            FROM real_order_items ri
            JOIN real_orders ro ON ro.id = ri.real_order_id
            WHERE ri.product_name_1c = products.name
              AND ro.doc_date >= ?
        ), 0)
    """, (cutoff_30, cutoff_30, cutoff_90, cutoff_90))

    conn.execute("""
        UPDATE categories SET units_score = COALESCE((
            SELECT SUM(units_score) FROM products WHERE category_id = categories.id
        ), 0)
    """)
    conn.commit()

    p = conn.execute(
        """SELECT COUNT(*) AS total,
                  SUM(CASE WHEN units_score > 0 THEN 1 ELSE 0 END) AS scored,
                  MAX(units_score) AS max_score
           FROM products"""
    ).fetchone()
    print(f'[update_units_score] products: total={p[0]}  scored={p[1]}  max={p[2] or 0:.2f}')

    top = conn.execute(
        "SELECT name, units_score FROM products WHERE units_score > 0 "
        "ORDER BY units_score DESC LIMIT 5"
    ).fetchall()
    if top:
        print('[update_units_score] top 5 products:')
        for name, score in top:
            print(f'  {score:8.2f}  {name[:60]}')

    top_cats = conn.execute(
        "SELECT name, units_score FROM categories WHERE units_score > 0 "
        "ORDER BY units_score DESC LIMIT 5"
    ).fetchall()
    if top_cats:
        print('[update_units_score] top 5 categories:')
        for name, score in top_cats:
            print(f'  {score:10.2f}  {name[:60]}')

    conn.close()
    return p[1] or 0


if __name__ == '__main__':
    startup_mode = '--startup' in sys.argv
    try:
        scored = main(startup=startup_mode)
        print(f'[update_units_score] done. {scored} products scored.')
    except Exception as e:
        if startup_mode:
            print(f'[update_units_score] ERROR in startup mode, continuing boot: {e}')
            sys.exit(0)
        raise
