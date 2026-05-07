"""Per-supplier reorder recommendation engine — drives /zakazlar Phase 1.

Formula (intentionally simple — works on any data window, no seasonality):
    daily_rate = (units sold in last `window_days`) / window_days
    target     = daily_rate * buffer_days
    suggested_buy = max(0, ceil(target - current_stock))

Returns rows where suggested_buy > 0, sorted by suggested_buy desc.
NO PRICES surfaced (per memory feedback_order_prep_no_prices, 2026-05-06).
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import List, Optional, Tuple

from backend.database import get_db


DEFAULT_WINDOW_DAYS = 90
DEFAULT_BUFFER_DAYS = 30


def list_suppliers_with_products(conn=None) -> List[dict]:
    """Active suppliers that have at least one product mapped via
    latest_supplier_id, plus a synthetic '(noma'lum)' bucket capturing
    products with NULL latest_supplier_id.

    Returns: [{id, name_1c, product_count, low_stock_count}], sorted by
    low_stock_count desc, then product_count desc.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT s.id, s.name_1c,
                   COUNT(p.id) AS product_count,
                   SUM(CASE WHEN COALESCE(p.stock_quantity,0) <= 0 THEN 1 ELSE 0 END) AS oos_count
              FROM suppliers s
              JOIN products p ON p.latest_supplier_id = s.id AND p.is_active = 1
             WHERE s.is_active = 1
             GROUP BY s.id, s.name_1c
             ORDER BY oos_count DESC, product_count DESC, s.name_1c
            """
        ).fetchall()
        result = [dict(r) for r in rows]

        # Synthetic unknown-supplier bucket
        unknown = conn.execute(
            """SELECT COUNT(*) AS n,
                      SUM(CASE WHEN COALESCE(stock_quantity,0) <= 0 THEN 1 ELSE 0 END) AS oos
                 FROM products
                WHERE is_active = 1 AND latest_supplier_id IS NULL"""
        ).fetchone()
        if unknown and unknown["n"] > 0:
            result.append({
                "id": None,
                "name_1c": "(noma'lum supplier)",
                "product_count": unknown["n"],
                "oos_count": unknown["oos"] or 0,
            })
        return result
    finally:
        if own_conn:
            conn.close()


def list_supplier_full(
    supplier_id: Optional[int],
    window_days: int = DEFAULT_WINDOW_DAYS,
    buffer_days: int = DEFAULT_BUFFER_DAYS,
    conn=None,
) -> List[dict]:
    """ALL active products for a supplier (or unmapped if supplier_id=None) with
    stock, window sales, daily rate, suggested_buy, last_sale, lifecycle.

    Returns every product (including those with sufficient stock or zero sales)
    so callers can present a complete picture in xlsx Hammasi sheet.

    Each row: {product_id, name, stock, sold_window, daily_rate, suggested_buy,
               last_sale, lifecycle}.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db()
    try:
        cutoff = (_dt.date.today() - _dt.timedelta(days=window_days)).isoformat()
        if supplier_id is None:
            sup_clause = "p.latest_supplier_id IS NULL"
            params: Tuple = (cutoff,)
        else:
            sup_clause = "p.latest_supplier_id = ?"
            params = (cutoff, supplier_id)

        rows = conn.execute(
            f"""
            SELECT p.id AS product_id,
                   p.name,
                   p.lifecycle,
                   COALESCE(p.stock_quantity, 0) AS stock,
                   COALESCE(s.sold, 0) AS sold_window,
                   s.last_sale
              FROM products p
              LEFT JOIN (
                  SELECT roi.product_id AS pid,
                         SUM(roi.quantity) AS sold,
                         MAX(ro.doc_date) AS last_sale
                    FROM real_order_items roi
                    JOIN real_orders ro ON ro.id = roi.real_order_id
                   WHERE ro.doc_date >= ?
                   GROUP BY roi.product_id
              ) s ON s.pid = p.id
             WHERE p.is_active = 1
               AND {sup_clause}
            """,
            params,
        ).fetchall()

        result = []
        for r in rows:
            sold = r["sold_window"] or 0
            daily = sold / window_days if sold else 0
            target = daily * buffer_days
            suggested = max(0, math.ceil(target - r["stock"]))
            result.append({
                "product_id": r["product_id"],
                "name": r["name"],
                "lifecycle": r["lifecycle"] or "",
                "stock": r["stock"],
                "sold_window": sold,
                "daily_rate": round(daily, 2),
                "suggested_buy": suggested,
                "last_sale": r["last_sale"] or "",
            })
        return result
    finally:
        if own_conn:
            conn.close()


def compute_supplier_reorder(
    supplier_id: Optional[int],
    window_days: int = DEFAULT_WINDOW_DAYS,
    buffer_days: int = DEFAULT_BUFFER_DAYS,
    conn=None,
) -> List[dict]:
    """Subset of list_supplier_full: only products where suggested_buy > 0,
    sorted by suggested_buy desc.
    """
    full = list_supplier_full(supplier_id, window_days, buffer_days, conn)
    out = [r for r in full if r["suggested_buy"] > 0]
    out.sort(key=lambda x: -x["suggested_buy"])
    return out
