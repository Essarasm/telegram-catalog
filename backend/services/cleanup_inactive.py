"""Deactivate stale catalog entries using a rolling-window rule.

A product is considered "stale" if:
  - it hasn't appeared in any /stock upload within the last N days
  - it hasn't been sold within the last N days
  - it hasn't been supplied within the last N days

Triple-gate: all three must be older than N for the product to be deactivated.
That keeps just-arrived items (supplied but not yet in a /stock file) and
slow-mover items that still occasionally sell from being wrongly deactivated.

Two entry points:
  - preview_inactive(days) → counts + sample names, no writes
  - deactivate_inactive(days) → flips is_active to 0, returns the set actually changed
"""
import logging
from backend.database import get_db

logger = logging.getLogger(__name__)


def _stale_query(days: int) -> str:
    """SQL that returns id, name, name_display, last_seen, last_sold, last_supplied
    for active products whose three signals are all older than N days (or NULL).
    """
    return f"""
        WITH last_sold AS (
            SELECT roi.product_id AS pid, MAX(ro.doc_date) AS d
            FROM real_order_items roi
            JOIN real_orders ro ON ro.id = roi.real_order_id
            WHERE roi.product_id IS NOT NULL
            GROUP BY roi.product_id
        ),
        last_supplied AS (
            SELECT soi.matched_product_id AS pid, MAX(so.doc_date) AS d
            FROM supply_order_items soi
            JOIN supply_orders so ON so.id = soi.supply_order_id
            WHERE soi.matched_product_id IS NOT NULL
            GROUP BY soi.matched_product_id
        )
        SELECT p.id, p.name, p.name_display,
               p.stock_last_seen_at AS last_seen,
               ls.d AS last_sold,
               lsup.d AS last_supplied
        FROM products p
        LEFT JOIN last_sold ls ON ls.pid = p.id
        LEFT JOIN last_supplied lsup ON lsup.pid = p.id
        WHERE p.is_active = 1
          AND (p.stock_last_seen_at IS NULL
               OR datetime(p.stock_last_seen_at) < datetime('now', '-{days} days'))
          AND (ls.d IS NULL OR date(ls.d) < date('now', '-{days} days'))
          AND (lsup.d IS NULL OR date(lsup.d) < date('now', '-{days} days'))
    """


def preview_inactive(days: int = 60, conn=None) -> dict:
    """Count + sample products that would be deactivated by deactivate_inactive(days).

    Read-only. Safe to run any time.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db()
    try:
        rows = conn.execute(_stale_query(days)).fetchall()
        active_total = conn.execute(
            "SELECT COUNT(*) AS c FROM products WHERE is_active = 1"
        ).fetchone()["c"]

        # Group by category-ish prefix for readable preview
        # (uses producer name to give an at-a-glance breakdown)
        producer_breakdown = {}
        samples = []
        for r in rows:
            name = r["name_display"] or r["name"] or ""
            samples.append({
                "id": r["id"],
                "name": name[:60],
                "last_seen": r["last_seen"] or "—",
                "last_sold": r["last_sold"] or "—",
                "last_supplied": r["last_supplied"] or "—",
            })
        # Producer rollup
        try:
            prod_rows = conn.execute(f"""
                SELECT pr.name AS producer, COUNT(*) AS c
                FROM ({_stale_query(days)}) stale
                JOIN products p ON p.id = stale.id
                JOIN producers pr ON pr.id = p.producer_id
                GROUP BY pr.name
                ORDER BY c DESC
            """).fetchall()
            for r in prod_rows:
                producer_breakdown[r["producer"]] = r["c"]
        except Exception as e:
            logger.warning(f"producer rollup failed: {e}")

        return {
            "ok": True,
            "days": days,
            "active_total": active_total,
            "would_deactivate": len(rows),
            "samples": samples[:20],
            "producer_breakdown": producer_breakdown,
        }
    finally:
        if own_conn:
            conn.close()


def deactivate_inactive(days: int = 60, conn=None) -> dict:
    """Flip is_active = 0 for products matching the stale rule. Returns count + samples.

    Idempotent: running twice with the same window deactivates the same set
    once and returns 0 the second time.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db()
    try:
        rows = conn.execute(_stale_query(days)).fetchall()
        ids = [r["id"] for r in rows]
        if not ids:
            return {"ok": True, "days": days, "deactivated": 0, "samples": []}

        placeholders = ",".join("?" for _ in ids)
        conn.execute(
            f"UPDATE products SET is_active = 0 WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()

        samples = [
            {
                "id": r["id"],
                "name": (r["name_display"] or r["name"] or "")[:60],
                "last_seen": r["last_seen"] or "—",
                "last_sold": r["last_sold"] or "—",
                "last_supplied": r["last_supplied"] or "—",
            }
            for r in rows[:20]
        ]
        logger.info(f"cleanup_inactive(days={days}): deactivated {len(ids)} products")
        return {
            "ok": True,
            "days": days,
            "deactivated": len(ids),
            "samples": samples,
        }
    finally:
        if own_conn:
            conn.close()
