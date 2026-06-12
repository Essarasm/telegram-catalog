"""Supply dashboard API — admin endpoints feeding the Supply tab.

All endpoints require admin_key and return JSON. Surfaces:
- /hot-list            cross-supplier top-N reorder candidates
- /supplier-scoreboard one row per supplier with rolled-up metrics
- /lost-demand         top products by demand_signals.quantity in window
- /recent-deliveries   last N days of supply_orders
- /unmapped-count      products lacking latest_supplier_id (gate signal)
- /seasonal-alerts     products with YoY multiplier outside [low, peak] band

NO PRICES surfaced (per memory feedback_order_prep_no_prices).
"""
from __future__ import annotations

from statistics import median

from fastapi import APIRouter, HTTPException, Query

from backend.admin_auth import check_admin_key
from backend.database import get_db
from backend.services.reorder import (
    STATUS_ORDER,
    list_supplier_full,
    list_suppliers_with_products,
)


router = APIRouter(prefix="/api/admin/supply", tags=["admin-supply"])


def _check_admin(admin_key: str):
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _enrich_with_supplier(items, supplier_id, supplier_name):
    for it in items:
        it["supplier_id"] = supplier_id
        it["supplier_name"] = supplier_name
    return items


@router.get("/hot-list")
def hot_list(admin_key: str = Query(...), limit: int = Query(50, ge=1, le=500)):
    """Top-N reorder candidates across all mapped suppliers + unmapped bucket.
    Sorted by status priority, then days_of_cover asc, then suggested_buy desc.
    """
    _check_admin(admin_key)
    conn = get_db()
    try:
        suppliers = list_suppliers_with_products(conn=conn)
        all_items = []
        for s in suppliers:
            items = list_supplier_full(s["id"], conn=conn)
            buy_items = [it for it in items if it["suggested_buy"] > 0]
            _enrich_with_supplier(buy_items, s["id"], s["name_1c"])
            all_items.extend(buy_items)

        all_items.sort(key=lambda x: (
            STATUS_ORDER.get(x["status"], 99),
            float("inf") if x["days_of_cover"] is None else x["days_of_cover"],
            -x["suggested_buy"],
        ))
        return {
            "items": all_items[:limit],
            "total_with_buy": len(all_items),
            "limit": limit,
        }
    finally:
        conn.close()


@router.get("/supplier-scoreboard")
def supplier_scoreboard(admin_key: str = Query(...)):
    """One row per supplier (incl. unmapped bucket) with rolled-up metrics.
    Sorted: needs_order desc, stockout desc, name asc.
    """
    _check_admin(admin_key)
    conn = get_db()
    try:
        suppliers = list_suppliers_with_products(conn=conn)
        scoreboard = []
        for s in suppliers:
            items = list_supplier_full(s["id"], conn=conn)
            n = len(items)
            stockout = sum(1 for it in items if it["status"] == "stockout")
            chronic_stockout = sum(1 for it in items if it["status"] == "chronic_stockout")
            order_now = sum(1 for it in items if it["status"] == "order_now")
            order_soon = sum(1 for it in items if it["status"] == "order_soon")
            no_demand = sum(1 for it in items if it["status"] == "no_recent_demand")
            needs_order = sum(1 for it in items if it["suggested_buy"] > 0)
            total_buy = sum(it["suggested_buy"] for it in items)

            lead_times = [it["lead_time_days"] for it in items]
            median_lead = float(median(lead_times)) if lead_times else 0.0

            if s["id"] is not None:
                last = conn.execute(
                    """SELECT MAX(so.doc_date) AS d
                         FROM supply_orders so
                         JOIN suppliers sup ON sup.name_1c = so.counterparty_name
                        WHERE sup.id = ? AND so.doc_type = 'supply'""",
                    (s["id"],),
                ).fetchone()
                last_supply_date = last["d"] if last else None
                events_ytd = conn.execute(
                    """SELECT COUNT(*) AS n
                         FROM supply_orders so
                         JOIN suppliers sup ON sup.name_1c = so.counterparty_name
                        WHERE sup.id = ? AND so.doc_type = 'supply'
                          AND so.doc_date >= date('now', 'start of year')""",
                    (s["id"],),
                ).fetchone()["n"]
            else:
                last_supply_date = None
                events_ytd = 0

            scoreboard.append({
                "supplier_id": s["id"],
                "supplier_name": s["name_1c"],
                "product_count": n,
                "stockout": stockout,
                "chronic_stockout": chronic_stockout,
                "order_now": order_now,
                "order_soon": order_soon,
                "no_recent_demand": no_demand,
                "needs_order": needs_order,
                "total_buy_qty": total_buy,
                "pct_stockout": round(100 * stockout / n, 1) if n > 0 else 0.0,
                "median_lead_time_days": round(median_lead, 1),
                "last_supply_date": last_supply_date,
                "supply_events_ytd": events_ytd,
            })

        scoreboard.sort(key=lambda x: (
            -x["needs_order"], -x["stockout"], x["supplier_name"] or ""
        ))
        return {"suppliers": scoreboard, "count": len(scoreboard)}
    finally:
        conn.close()


@router.get("/lost-demand")
def lost_demand(
    admin_key: str = Query(...),
    days: int = Query(60, ge=1, le=365),
    limit: int = Query(20, ge=1, le=200),
):
    """Top products by demand_signals.quantity in last N days."""
    _check_admin(admin_key)
    conn = get_db()
    try:
        rows = conn.execute(
            f"""SELECT ds.product_id,
                       p.name,
                       p.latest_supplier_id AS supplier_id,
                       sup.name_1c AS supplier_name,
                       COALESCE(p.stock_quantity, 0) AS stock,
                       SUM(ds.quantity) AS lost_qty,
                       COUNT(DISTINCT ds.telegram_id) AS unique_clients,
                       MAX(date(ds.created_at)) AS last_signal_date
                  FROM demand_signals ds
                  JOIN products p ON p.id = ds.product_id
                  LEFT JOIN suppliers sup ON sup.id = p.latest_supplier_id
                 WHERE date(ds.created_at) >= date('now', '-{int(days)} days')
                   AND p.is_active = 1
                 GROUP BY ds.product_id
                 ORDER BY lost_qty DESC
                 LIMIT ?""",
            (limit,),
        ).fetchall()
        return {
            "items": [dict(r) for r in rows],
            "window_days": days,
        }
    finally:
        conn.close()


@router.get("/recent-deliveries")
def recent_deliveries(
    admin_key: str = Query(...),
    days: int = Query(30, ge=1, le=180),
):
    """Last N days of supply deliveries (doc_type='supply'). Most recent first."""
    _check_admin(admin_key)
    conn = get_db()
    try:
        rows = conn.execute(
            f"""SELECT so.id,
                       so.doc_number,
                       so.doc_date,
                       so.counterparty_name,
                       so.warehouse,
                       so.currency,
                       COUNT(soi.id) AS item_count,
                       SUM(soi.quantity) AS unit_count
                  FROM supply_orders so
                  LEFT JOIN supply_order_items soi ON soi.supply_order_id = so.id
                 WHERE so.doc_type = 'supply'
                   AND date(so.doc_date) >= date('now', '-{int(days)} days')
                 GROUP BY so.id
                 ORDER BY so.doc_date DESC, so.id DESC""",
        ).fetchall()
        return {
            "deliveries": [dict(r) for r in rows],
            "window_days": days,
        }
    finally:
        conn.close()


@router.get("/unmapped-count")
def unmapped_count(admin_key: str = Query(...)):
    """Active products lacking latest_supplier_id — bottleneck for supplier-grouped views."""
    _check_admin(admin_key)
    conn = get_db()
    try:
        total = conn.execute(
            "SELECT COUNT(*) AS n FROM products WHERE is_active = 1"
        ).fetchone()["n"]
        mapped = conn.execute(
            "SELECT COUNT(*) AS n FROM products WHERE is_active = 1 AND latest_supplier_id IS NOT NULL"
        ).fetchone()["n"]
        unmapped = total - mapped
        unmapped_oos = conn.execute(
            """SELECT COUNT(*) AS n FROM products
                WHERE is_active = 1
                  AND latest_supplier_id IS NULL
                  AND COALESCE(stock_quantity, 0) <= 0"""
        ).fetchone()["n"]
        top_producers = conn.execute(
            """SELECT COALESCE(pr.name, '(no producer)') AS producer,
                      COUNT(p.id) AS unmapped_skus
                 FROM products p
                 LEFT JOIN producers pr ON pr.id = p.producer_id
                WHERE p.is_active = 1 AND p.latest_supplier_id IS NULL
                GROUP BY pr.name
                ORDER BY unmapped_skus DESC
                LIMIT 10"""
        ).fetchall()
        return {
            "total_active": total,
            "mapped": mapped,
            "unmapped": unmapped,
            "unmapped_oos": unmapped_oos,
            "pct_mapped": round(100 * mapped / total, 1) if total > 0 else 0.0,
            "top_producers_unmapped": [dict(r) for r in top_producers],
        }
    finally:
        conn.close()


@router.get("/seasonal-alerts")
def seasonal_alerts(
    admin_key: str = Query(...),
    peak_threshold: float = Query(1.5, ge=1.0, le=20.0),
    low_threshold: float = Query(0.5, ge=0.01, le=1.0),
    limit: int = Query(30, ge=1, le=200),
):
    """Products with YoY seasonal multiplier outside the neutral band.
    Peak = entering high season; low = exiting / off-season.
    """
    _check_admin(admin_key)
    conn = get_db()
    try:
        suppliers = list_suppliers_with_products(conn=conn)
        peak, low = [], []
        for s in suppliers:
            items = list_supplier_full(s["id"], conn=conn)
            for it in items:
                if it["seasonal_source"] != "yoy":
                    continue
                if it["seasonal_mult"] >= peak_threshold:
                    it["supplier_id"] = s["id"]
                    it["supplier_name"] = s["name_1c"]
                    peak.append(it)
                elif it["seasonal_mult"] <= low_threshold:
                    it["supplier_id"] = s["id"]
                    it["supplier_name"] = s["name_1c"]
                    low.append(it)

        peak.sort(key=lambda x: -x["seasonal_mult"])
        low.sort(key=lambda x: x["seasonal_mult"])
        return {
            "peak": peak[:limit],
            "low": low[:limit],
            "peak_threshold": peak_threshold,
            "low_threshold": low_threshold,
        }
    finally:
        conn.close()
