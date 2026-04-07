"""Personal cabinet — order history and reorder."""
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from backend.database import get_db
from backend.services.import_real_orders import (
    list_real_orders_for_client,
    get_real_order_detail,
    find_nearby_wishlist,
    find_nearby_real_orders,
)

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


# ───────────────────────────────────────────
# Real orders (1C "Реализация товаров")
# ───────────────────────────────────────────

@router.get("/real-orders")
def list_real_orders(telegram_id: int = Query(...), limit: int = Query(50, ge=1, le=200)):
    """List real (shipped) orders for a client.

    Looks up the client_id linked to this telegram_id and returns all real
    orders from `real_orders`, newest first.
    """
    conn = get_db()
    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.close()

    if not user or not user["client_id"]:
        return {"ok": True, "orders": [], "linked": False}

    orders = list_real_orders_for_client(user["client_id"], limit=limit)
    return {"ok": True, "orders": orders, "linked": True}


@router.get("/real-orders/{real_order_id}")
def real_order_detail(real_order_id: int, telegram_id: int = Query(...)):
    """Get a single real order with line items. Only the linked client may view."""
    conn = get_db()
    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.close()

    if not user or not user["client_id"]:
        return JSONResponse({"ok": False, "error": "Not linked to a client"}, status_code=403)

    detail = get_real_order_detail(real_order_id)
    if not detail:
        return JSONResponse({"ok": False, "error": "Real order not found"}, status_code=404)

    if detail["order"].get("client_id") != user["client_id"]:
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)

    return {"ok": True, **detail}


@router.get("/compare")
def compare_orders(
    telegram_id: int = Query(...),
    real_order_id: int = Query(None),
    wishlist_order_id: int = Query(None),
    days: int = Query(5, ge=1, le=30),
):
    """Find counterpart orders for the compare view in the Cabinet.

    - Pass `real_order_id` to find wish-list orders within ±days of that
      real order's doc_date for the same client.
    - Pass `wishlist_order_id` to find real orders within ±days of that
      wish-list order's created_at for the same client.

    No automatic linking is created — the user makes the comparison visually.
    """
    conn = get_db()
    user = conn.execute(
        "SELECT telegram_id, client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    if not user:
        conn.close()
        return JSONResponse({"ok": False, "error": "User not found"}, status_code=404)

    if real_order_id:
        ro = conn.execute(
            "SELECT doc_date, client_id FROM real_orders WHERE id = ?",
            (real_order_id,),
        ).fetchone()
        if not ro or ro["client_id"] != user["client_id"]:
            conn.close()
            return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
        conn.close()
        nearby = find_nearby_wishlist(user["client_id"], ro["doc_date"], days=days)
        return {"ok": True, "kind": "wishlist_near_real", "orders": nearby}

    if wishlist_order_id:
        wl = conn.execute(
            "SELECT created_at, telegram_id FROM orders WHERE id = ?",
            (wishlist_order_id,),
        ).fetchone()
        if not wl or wl["telegram_id"] != telegram_id:
            conn.close()
            return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
        conn.close()
        # Use the date portion of created_at for the window
        date_str = (wl["created_at"] or "")[:10]
        nearby = find_nearby_real_orders(telegram_id, date_str, days=days)
        return {"ok": True, "kind": "real_near_wishlist", "orders": nearby}

    conn.close()
    return JSONResponse(
        {"ok": False, "error": "Pass either real_order_id or wishlist_order_id"},
        status_code=400,
    )
