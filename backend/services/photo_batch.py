"""Catalog-group /foto batch state.

A "batch" is up to 10 product messages posted in the catalog group. Each
item is either pending → photographed (employee replied with File, upload
to Drive succeeded) or pending → skipped (employee tapped ⏭). When every
item in the active batch is non-pending, the next batch can be started
by the caller (handler).

Source ranking matches the Product Cleanup admin tab: active products
without an image, ranked by 60-day order frequency (most-ordered first).
Already-skipped or already-photographed products are excluded so they
don't resurface.
"""
from __future__ import annotations

from typing import Optional

from backend.database import get_db

BATCH_SIZE = 10
WINDOW_DAYS = 60


def _next_batch_id(conn) -> int:
    row = conn.execute("SELECT COALESCE(MAX(batch_id), 0) + 1 FROM photo_batch_items").fetchone()
    return int(row[0]) if row else 1


def get_active_batch_id(conn) -> Optional[int]:
    """Returns the batch_id of the most recent batch if any item in it is
    still pending, otherwise None.
    """
    row = conn.execute(
        """SELECT batch_id FROM photo_batch_items
           WHERE status = 'pending'
           ORDER BY batch_id DESC LIMIT 1"""
    ).fetchone()
    return int(row[0]) if row else None


def pending_items(conn, batch_id: int) -> list[dict]:
    """All still-pending items in a batch, ordered by position."""
    rows = conn.execute(
        """SELECT i.id, i.position, i.product_id, i.message_id,
                  p.name AS product_name_1c, p.name_display,
                  pr.name AS producer_name, c.name AS category_name
           FROM photo_batch_items i
           JOIN products p ON p.id = i.product_id
           LEFT JOIN producers pr ON pr.id = p.producer_id
           LEFT JOIN categories c ON c.id = p.category_id
           WHERE i.batch_id = ? AND i.status = 'pending'
           ORDER BY i.position ASC""",
        (batch_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _pick_next_products(conn, limit: int) -> list[dict]:
    """Active products lacking an image, ranked by 60-day order frequency.
    Excludes any product already referenced in photo_batch_items (any status)
    so we don't resurface skips or duplicate already-photographed items.
    """
    rows = conn.execute(
        """SELECT p.id, p.name AS product_name_1c, p.name_display,
                  pr.name AS producer_name, c.name AS category_name,
                  COUNT(DISTINCT roi.real_order_id) AS order_count
           FROM products p
           LEFT JOIN producers pr ON pr.id = p.producer_id
           LEFT JOIN categories c ON c.id = p.category_id
           JOIN real_order_items roi ON roi.product_id = p.id
           JOIN real_orders ro       ON ro.id = roi.real_order_id
           WHERE p.is_active = 1
             AND (p.image_path IS NULL OR p.image_path = '')
             AND ro.doc_date >= date('now', ?)
             AND p.id NOT IN (SELECT product_id FROM photo_batch_items)
           GROUP BY p.id
           ORDER BY order_count DESC, p.id ASC
           LIMIT ?""",
        (f'-{WINDOW_DAYS} days', limit),
    ).fetchall()
    return [dict(r) for r in rows]


def start_or_resume_batch() -> tuple[int, list[dict], bool]:
    """Returns (batch_id, items, is_new).

    If a pending batch exists, returns it with is_new=False so the caller
    can re-display pending items without re-posting.

    If no pending batch, starts a new batch by inserting up to BATCH_SIZE
    rows (status='pending') and returns is_new=True with the rows that
    were just created. Items include product_id, position, and full
    display context (name_1c, producer, category). message_id is NULL
    until the caller fills it after posting.

    Items list may be shorter than BATCH_SIZE if fewer matching products
    remain (or empty if all missing-photo products have been processed).
    """
    conn = get_db()
    try:
        active = get_active_batch_id(conn)
        if active is not None:
            return active, pending_items(conn, active), False

        next_id = _next_batch_id(conn)
        picks = _pick_next_products(conn, BATCH_SIZE)
        items = []
        for pos, p in enumerate(picks, start=1):
            cur = conn.execute(
                """INSERT INTO photo_batch_items
                   (batch_id, position, product_id, status, created_at)
                   VALUES (?, ?, ?, 'pending', datetime('now'))""",
                (next_id, pos, p["id"]),
            )
            items.append({
                "id": cur.lastrowid,
                "position": pos,
                "product_id": p["id"],
                "product_name_1c": p["product_name_1c"],
                "name_display": p["name_display"],
                "producer_name": p["producer_name"],
                "category_name": p["category_name"],
                "order_count": p["order_count"],
                "message_id": None,
            })
        conn.commit()
        return next_id, items, True
    finally:
        conn.close()


def register_message_id(item_id: int, message_id: int) -> None:
    conn = get_db()
    try:
        conn.execute(
            "UPDATE photo_batch_items SET message_id = ? WHERE id = ?",
            (message_id, item_id),
        )
        conn.commit()
    finally:
        conn.close()


def find_item_by_message_id(message_id: int) -> Optional[dict]:
    """Lookup by Telegram message_id (the reply_to anchor). Returns the
    item with joined product display context, or None.
    """
    conn = get_db()
    try:
        row = conn.execute(
            """SELECT i.id, i.batch_id, i.position, i.product_id, i.message_id,
                      i.status,
                      p.name AS product_name_1c, p.name_display,
                      pr.name AS producer_name, c.name AS category_name
               FROM photo_batch_items i
               JOIN products p ON p.id = i.product_id
               LEFT JOIN producers pr ON pr.id = p.producer_id
               LEFT JOIN categories c ON c.id = p.category_id
               WHERE i.message_id = ?""",
            (message_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def find_item_by_id(item_id: int) -> Optional[dict]:
    conn = get_db()
    try:
        row = conn.execute(
            """SELECT i.id, i.batch_id, i.position, i.product_id, i.message_id,
                      i.status,
                      p.name AS product_name_1c, p.name_display
               FROM photo_batch_items i
               JOIN products p ON p.id = i.product_id
               WHERE i.id = ?""",
            (item_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def mark_photographed(
    item_id: int,
    tg_id: int,
    telegram_file_id: str,
    original_filename: Optional[str],
    mime_type: Optional[str],
    file_size_bytes: Optional[int],
    drive_file_id: Optional[str],
    drive_file_name: Optional[str],
) -> None:
    conn = get_db()
    try:
        conn.execute(
            """UPDATE photo_batch_items
               SET status = 'photographed',
                   photographed_by_tg_id = ?,
                   photographed_at = datetime('now'),
                   telegram_file_id = ?,
                   original_filename = ?,
                   mime_type = ?,
                   file_size_bytes = ?,
                   drive_file_id = ?,
                   drive_file_name = ?
               WHERE id = ?""",
            (
                tg_id, telegram_file_id, original_filename, mime_type,
                file_size_bytes, drive_file_id, drive_file_name, item_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def mark_skipped(item_id: int, tg_id: int) -> None:
    conn = get_db()
    try:
        conn.execute(
            """UPDATE photo_batch_items
               SET status = 'skipped',
                   skipped_by_tg_id = ?,
                   skipped_at = datetime('now')
               WHERE id = ?""",
            (tg_id, item_id),
        )
        conn.commit()
    finally:
        conn.close()


def is_batch_complete(batch_id: int) -> bool:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM photo_batch_items WHERE batch_id = ? AND status = 'pending'",
            (batch_id,),
        ).fetchone()
        return int(row[0]) == 0 if row else True
    finally:
        conn.close()


def batch_stats(batch_id: int) -> dict:
    """Returns counts of {pending, photographed, skipped, total} for a batch."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT status, COUNT(*) AS n
               FROM photo_batch_items WHERE batch_id = ?
               GROUP BY status""",
            (batch_id,),
        ).fetchall()
        out = {"pending": 0, "photographed": 0, "skipped": 0, "total": 0}
        for r in rows:
            out[r["status"]] = int(r["n"])
            out["total"] += int(r["n"])
        return out
    finally:
        conn.close()
