"""Personal cabinet — order history and reorder."""
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from backend.database import get_db

router = APIRouter(prefix="/api/cabinet", tags=["cabinet"])


@router.get("/orders")
def list_orders(telegram_id: int = Query(...)):
    """List all orders for a client, newest first."""
    conn = get_db()
    rows = conn.execute(
        """SELECT id, telegram_id, client_name, total_usd, total_uzs,
                  item_count, status, created_at
           FROM orders
           WHERE telegram_id = ?
           ORDER BY created_at DESC""",
        (telegram_id,),
    ).fetchall()
    conn.close()

    orders = []
    for r in rows:
        orders.append({
            "id": r["id"],
            "client_name": r["client_name"],
            "total_usd": r["total_usd"],
            "total_uzs": r["total_uzs"],
            "item_count": r["item_count"],
            "status": r["status"],
            "created_at": r["created_at"],
        })
    return {"orders": orders}


@router.get("/orders/{order_id}")
def get_order_detail(order_id: int, telegram_id: int = Query(...)):
    """Get full order detail with items. Only owner can view."""
    conn = get_db()
    order = conn.execute(
        """SELECT id, telegram_id, client_name, total_usd, total_uzs,
                  item_count, status, created_at
           FROM orders WHERE id = ? AND telegram_id = ?""",
        (order_id, telegram_id),
    ).fetchone()

    if not order:
        conn.close()
        return JSONResponse({"ok": False, "error": "Order not found"}, status_code=404)

    items = conn.execute(
        """SELECT product_id, product_name, producer_name, quantity, unit, price, currency
           FROM order_items WHERE order_id = ?""",
        (order_id,),
    ).fetchall()
    conn.close()

    return {
        "order": {
            "id": order["id"],
            "client_name": order["client_name"],
            "total_usd": order["total_usd"],
            "total_uzs": order["total_uzs"],
            "item_count": order["item_count"],
            "status": order["status"],
            "created_at": order["created_at"],
        },
        "items": [
            {
                "product_id": it["product_id"],
                "product_name": it["product_name"],
                "producer_name": it["producer_name"],
                "quantity": it["quantity"],
                "unit": it["unit"],
                "price": it["price"],
                "currency": it["currency"],
            }
            for it in items
        ],
    }


@router.post("/orders/{order_id}/reorder")
def reorder(order_id: int, telegram_id: int = Query(...), mode: str = Query("replace")):
    """
    Copy all items from a past order into the user's cart.
    mode='replace' clears cart first, mode='merge' adds to existing cart.
    Returns the new cart contents.
    """
    conn = get_db()

    # Verify ownership
    order = conn.execute(
        "SELECT id FROM orders WHERE id = ? AND telegram_id = ?",
        (order_id, telegram_id),
    ).fetchone()
    if not order:
        conn.close()
        return JSONResponse({"ok": False, "error": "Order not found"}, status_code=404)

    # Get order items
    items = conn.execute(
        "SELECT product_id, quantity FROM order_items WHERE order_id = ? AND product_id IS NOT NULL",
        (order_id,),
    ).fetchall()

    if not items:
        conn.close()
        return JSONResponse({"ok": False, "error": "No reorderable items"}, status_code=400)

    # Check which products still exist and are active
    valid_items = []
    for it in items:
        product = conn.execute(
            "SELECT id FROM products WHERE id = ? AND is_active = 1",
            (it["product_id"],),
        ).fetchone()
        if product:
            valid_items.append({"product_id": it["product_id"], "quantity": it["quantity"]})

    if not valid_items:
        conn.close()
        return JSONResponse({"ok": False, "error": "None of the products are available anymore"}, status_code=400)

    # Clear cart if replacing
    if mode == "replace":
        conn.execute("DELETE FROM cart_items WHERE user_id = ?", (telegram_id,))

    # Insert/merge items into cart
    for vi in valid_items:
        if mode == "merge":
            conn.execute(
                """INSERT INTO cart_items (user_id, product_id, quantity, updated_at)
                   VALUES (?, ?, ?, datetime('now'))
                   ON CONFLICT(user_id, product_id)
                   DO UPDATE SET quantity = quantity + excluded.quantity, updated_at = datetime('now')""",
                (telegram_id, vi["product_id"], vi["quantity"]),
            )
        else:
            conn.execute(
                """INSERT INTO cart_items (user_id, product_id, quantity, updated_at)
                   VALUES (?, ?, ?, datetime('now'))
                   ON CONFLICT(user_id, product_id)
                   DO UPDATE SET quantity = excluded.quantity, updated_at = datetime('now')""",
                (telegram_id, vi["product_id"], vi["quantity"]),
            )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "added_count": len(valid_items),
        "total_requested": len(items),
        "skipped": len(items) - len(valid_items),
    }
