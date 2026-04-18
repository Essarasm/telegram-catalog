"""Personal cabinet — order history and reorder."""
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from backend.database import get_db, get_sibling_client_ids
from backend.services.import_real_orders import (
    list_real_orders_for_client,
    get_real_order_detail,
    find_nearby_wishlist,
    find_nearby_real_orders,
)

router = APIRouter(prefix="/api/cabinet", tags=["cabinet"])


@router.get("/orders")
def list_orders(telegram_id: int = Query(...)):
    """List all wish-list orders for the client linked to this telegram_id.

    Uses client_id (from users.client_id) rather than telegram_id alone,
    so /testclient switches show only the currently-linked client's orders.
    Falls back to telegram_id for legacy orders without a client_id.
    """
    conn = get_db()
    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    client_id = user["client_id"] if user else None

    if client_id:
        rows = conn.execute(
            """SELECT o.id, o.telegram_id, o.client_name, o.client_phone,
                      o.total_usd, o.total_uzs, o.item_count, o.status,
                      o.created_at,
                      (SELECT COUNT(*) FROM confirmed_orders co
                       WHERE co.wishlist_order_id = o.id) AS confirmed_count
               FROM orders o
               WHERE o.client_id = ?
               ORDER BY o.created_at DESC""",
            (client_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT o.id, o.telegram_id, o.client_name, o.client_phone,
                      o.total_usd, o.total_uzs, o.item_count, o.status,
                      o.created_at,
                      (SELECT COUNT(*) FROM confirmed_orders co
                       WHERE co.wishlist_order_id = o.id) AS confirmed_count
               FROM orders o
               WHERE o.telegram_id = ? AND o.client_id IS NULL
               ORDER BY o.created_at DESC""",
            (telegram_id,),
        ).fetchall()
    conn.close()

    orders = []
    for r in rows:
        orders.append({
            "id": r["id"],
            "telegram_id": r["telegram_id"],
            "client_name": r["client_name"],
            "client_phone": r["client_phone"],
            "total_usd": r["total_usd"],
            "total_uzs": r["total_uzs"],
            "item_count": r["item_count"],
            "status": r["status"],
            "created_at": r["created_at"],
            "has_confirmed": bool(r["confirmed_count"]),
        })
    return {"orders": orders}


@router.get("/orders/{order_id}")
def get_order_detail(order_id: int, telegram_id: int = Query(...)):
    """Get full order detail with items. Uses client_id for access check
    so /testclient-linked agents and sibling phones can view."""
    conn = get_db()
    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?", (telegram_id,)
    ).fetchone()
    client_id = user["client_id"] if user else None

    if client_id:
        order = conn.execute(
            """SELECT id, telegram_id, client_name, total_usd, total_uzs,
                      item_count, status, created_at
               FROM orders WHERE id = ? AND client_id = ?""",
            (order_id, client_id),
        ).fetchone()
    else:
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
    client_ids, conn = _get_all_client_ids_for_user(telegram_id)
    if conn:
        conn.close()
    if not client_ids:
        return {"ok": True, "orders": [], "linked": False}

    orders = list_real_orders_for_client(client_ids, limit=limit)
    return {"ok": True, "orders": orders, "linked": True}


@router.get("/real-orders/{real_order_id}")
def real_order_detail(real_order_id: int, telegram_id: int = Query(...)):
    """Get a single real order with line items. Only the linked client may view."""
    client_ids, conn = _get_all_client_ids_for_user(telegram_id)
    if conn:
        conn.close()
    if not client_ids:
        return JSONResponse({"ok": False, "error": "Not linked to a client"}, status_code=403)

    detail = get_real_order_detail(real_order_id)
    if not detail:
        return JSONResponse({"ok": False, "error": "Real order not found"}, status_code=404)

    if detail["order"].get("client_id") not in client_ids:
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

    # Resolve all sibling client IDs for multi-phone clients
    sibling_ids = get_sibling_client_ids(conn, user["client_id"]) if user["client_id"] else []

    if real_order_id:
        ro = conn.execute(
            "SELECT doc_date, client_id FROM real_orders WHERE id = ?",
            (real_order_id,),
        ).fetchone()
        if not ro or ro["client_id"] not in sibling_ids:
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


# ───────────────────────────────────────────
# Rassvet Plus — Client Business Intelligence
# ───────────────────────────────────────────

def _get_client_id_for_user(telegram_id: int):
    """Resolve telegram_id → client_id. Returns None if not linked."""
    conn = get_db()
    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.close()
    if user and user["client_id"]:
        return user["client_id"]
    return None


def _get_all_client_ids_for_user(telegram_id: int):
    """Resolve telegram_id → all sibling client_ids sharing the same client_id_1c.

    One real-world client (shop) can have up to 5 phone registrations.
    Financial data may be linked to any of these IDs. This ensures all phones
    for the same client see the full financial picture.

    Returns (list_of_ids, conn) — caller must close conn.
    Returns (None, None) if user is not linked.
    """
    conn = get_db()
    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    if not user or not user["client_id"]:
        conn.close()
        return None, None
    ids = get_sibling_client_ids(conn, user["client_id"])
    return ids, conn


@router.get("/spend-trend")
def spend_trend(telegram_id: int = Query(...), months: int = Query(12, ge=1, le=36)):
    """Monthly spend aggregates for a client from real_orders + real_order_items.

    Returns per-month totals (UZS and USD) for the last N months where the
    client had at least one shipment.  Used by the "My Business" chart.
    """
    client_ids, conn = _get_all_client_ids_for_user(telegram_id)
    if not client_ids:
        return {"ok": True, "months": [], "linked": False}

    placeholders = ",".join("?" * len(client_ids))
    rows = conn.execute(
        f"""SELECT strftime('%Y-%m', ro.doc_date) AS month,
                  COUNT(DISTINCT ro.id)           AS doc_count,
                  SUM(COALESCE(ri.total_local, 0))     AS total_uzs,
                  SUM(COALESCE(ri.total_currency, 0))  AS total_usd,
                  SUM(COALESCE(ri.quantity, 0))         AS total_qty,
                  COUNT(*)                              AS line_count
           FROM real_orders ro
           JOIN real_order_items ri ON ri.real_order_id = ro.id
           WHERE ro.client_id IN ({placeholders})
           GROUP BY month
           ORDER BY month DESC
           LIMIT ?""",
        (*client_ids, months),
    ).fetchall()
    conn.close()

    # Index existing data by month
    by_month = {}
    for r in rows:
        by_month[r["month"]] = {
            "month": r["month"],
            "doc_count": r["doc_count"],
            "total_uzs": round(float(r["total_uzs"] or 0)),
            "total_usd": round(float(r["total_usd"] or 0), 2),
            "total_qty": round(float(r["total_qty"] or 0), 1),
            "line_count": r["line_count"],
        }

    # Zero-fill missing months in the requested window (ending at current month)
    from datetime import date
    today = date.today()
    data = []
    for i in range(months - 1, -1, -1):
        y = today.year
        m = today.month - i
        while m <= 0:
            m += 12
            y -= 1
        key = f"{y:04d}-{m:02d}"
        if key in by_month:
            data.append(by_month[key])
        else:
            data.append({
                "month": key, "doc_count": 0, "total_uzs": 0, "total_usd": 0,
                "total_qty": 0, "line_count": 0,
            })

    return {"ok": True, "months": data, "linked": True}


@router.get("/top-products")
def top_products(telegram_id: int = Query(...), limit: int = Query(5, ge=1, le=20)):
    """Client's top products by total spend, from real_order_items.

    Returns two lists: top by UZS and top by USD (each up to `limit` items).
    """
    client_ids, conn = _get_all_client_ids_for_user(telegram_id)
    if not client_ids:
        return {"ok": True, "products": [], "top_uzs": [], "top_usd": [], "linked": False}

    placeholders = ",".join("?" * len(client_ids))
    base_query = f"""SELECT ri.product_name_1c                       AS name,
                  SUM(COALESCE(ri.quantity, 0))             AS total_qty,
                  SUM(COALESCE(ri.total_local, 0))         AS total_uzs,
                  SUM(COALESCE(ri.total_currency, 0))      AS total_usd,
                  COUNT(DISTINCT ri.real_order_id)          AS order_count
           FROM real_order_items ri
           JOIN real_orders ro ON ro.id = ri.real_order_id
           WHERE ro.client_id IN ({placeholders})
           GROUP BY ri.product_name_1c"""

    rows_uzs = conn.execute(
        base_query + " HAVING total_uzs > 0 ORDER BY total_uzs DESC, total_qty DESC LIMIT ?",
        (*client_ids, limit),
    ).fetchall()

    rows_usd = conn.execute(
        base_query + " HAVING total_usd > 0 ORDER BY total_usd DESC, total_qty DESC LIMIT ?",
        (*client_ids, limit),
    ).fetchall()
    conn.close()

    def to_list(rows):
        return [{
            "name": (r["name"] or "").strip(),
            "total_qty": round(float(r["total_qty"] or 0), 1),
            "total_uzs": round(float(r["total_uzs"] or 0)),
            "total_usd": round(float(r["total_usd"] or 0), 2),
            "order_count": r["order_count"],
        } for r in rows]

    top_uzs = to_list(rows_uzs)
    top_usd = to_list(rows_usd)

    # Backward compatibility: "products" returns the combined view (UZS first)
    return {"ok": True, "products": top_uzs, "top_uzs": top_uzs, "top_usd": top_usd, "linked": True}


@router.get("/activity-summary")
def activity_summary(telegram_id: int = Query(...)):
    """Order activity summary for a client — current month vs previous, lifetime stats.

    Returns this_month / prev_month order counts + totals, lifetime aggregates,
    and average order size.
    """
    client_ids, conn = _get_all_client_ids_for_user(telegram_id)
    if not client_ids:
        return {"ok": True, "summary": None, "linked": False}

    placeholders = ",".join("?" * len(client_ids))

    # Calendar-based current month and previous month
    from datetime import date
    today = date.today()
    this_month_key = today.strftime("%Y-%m")
    prev_year = today.year if today.month > 1 else today.year - 1
    prev_m = today.month - 1 if today.month > 1 else 12
    prev_month_key = f"{prev_year:04d}-{prev_m:02d}"

    # Fetch stats for exactly these two calendar months
    month_stats = conn.execute(
        f"""SELECT strftime('%Y-%m', ro.doc_date) AS month,
                  COUNT(DISTINCT ro.id)           AS doc_count,
                  SUM(COALESCE(ri.total_local, 0))     AS total_uzs,
                  SUM(COALESCE(ri.total_currency, 0))  AS total_usd,
                  SUM(COALESCE(ri.quantity, 0))         AS total_qty,
                  COUNT(*)                              AS line_count
           FROM real_orders ro
           JOIN real_order_items ri ON ri.real_order_id = ro.id
           WHERE ro.client_id IN ({placeholders})
             AND strftime('%Y-%m', ro.doc_date) IN (?, ?)
           GROUP BY month""",
        (*client_ids, this_month_key, prev_month_key),
    ).fetchall()

    # Index by month key so we can assign to this/prev regardless of presence
    stats_by_month = {row["month"]: row for row in month_stats}

    # Lifetime stats
    lifetime = conn.execute(
        f"""SELECT COUNT(DISTINCT ro.id)           AS total_orders,
                  SUM(COALESCE(ri.total_local, 0))     AS total_uzs,
                  SUM(COALESCE(ri.total_currency, 0))  AS total_usd,
                  SUM(COALESCE(ri.quantity, 0))         AS total_qty,
                  COUNT(*)                              AS total_lines,
                  MIN(ro.doc_date)                      AS first_order,
                  MAX(ro.doc_date)                      AS last_order
           FROM real_orders ro
           JOIN real_order_items ri ON ri.real_order_id = ro.id
           WHERE ro.client_id IN ({placeholders})""",
        tuple(client_ids),
    ).fetchone()
    conn.close()

    def _month_dict(row):
        if not row:
            return {"month": None, "doc_count": 0, "total_uzs": 0, "total_usd": 0,
                    "total_qty": 0, "line_count": 0}
        return {
            "month": row["month"],
            "doc_count": row["doc_count"],
            "total_uzs": round(float(row["total_uzs"] or 0)),
            "total_usd": round(float(row["total_usd"] or 0), 2),
            "total_qty": round(float(row["total_qty"] or 0), 1),
            "line_count": row["line_count"],
        }

    this_month = _month_dict(stats_by_month.get(this_month_key))
    if not this_month.get("month"):
        this_month["month"] = this_month_key
    prev_month = _month_dict(stats_by_month.get(prev_month_key))
    if not prev_month.get("month"):
        prev_month["month"] = prev_month_key

    # Find last active month (regardless of calendar) — useful when current/prev are empty
    last_active_month = None
    if (this_month["doc_count"] == 0) and (prev_month["doc_count"] == 0):
        conn2 = get_db()
        placeholders2 = ",".join("?" * len(client_ids))
        last_row = conn2.execute(
            f"""SELECT strftime('%Y-%m', ro.doc_date) AS month,
                      COUNT(DISTINCT ro.id)           AS doc_count,
                      SUM(COALESCE(ri.total_local, 0))     AS total_uzs,
                      SUM(COALESCE(ri.total_currency, 0))  AS total_usd,
                      SUM(COALESCE(ri.quantity, 0))         AS total_qty,
                      COUNT(*)                              AS line_count
               FROM real_orders ro
               JOIN real_order_items ri ON ri.real_order_id = ro.id
               WHERE ro.client_id IN ({placeholders2})
               GROUP BY month
               ORDER BY month DESC
               LIMIT 1""",
            tuple(client_ids),
        ).fetchone()
        conn2.close()
        last_active_month = _month_dict(last_row)

    total_orders = lifetime["total_orders"] or 0
    avg_uzs = round(float(lifetime["total_uzs"] or 0) / total_orders) if total_orders else 0
    avg_usd = round(float(lifetime["total_usd"] or 0) / total_orders, 2) if total_orders else 0
    avg_items = round(float(lifetime["total_qty"] or 0) / total_orders, 1) if total_orders else 0

    return {
        "ok": True,
        "linked": True,
        "summary": {
            "this_month": this_month,
            "prev_month": prev_month,
            "last_active_month": last_active_month,
            "lifetime": {
                "total_orders": total_orders,
                "total_uzs": round(float(lifetime["total_uzs"] or 0)),
                "total_usd": round(float(lifetime["total_usd"] or 0), 2),
                "total_qty": round(float(lifetime["total_qty"] or 0), 1),
                "total_lines": lifetime["total_lines"] or 0,
                "first_order": lifetime["first_order"],
                "last_order": lifetime["last_order"],
                "avg_order_uzs": avg_uzs,
                "avg_order_usd": avg_usd,
                "avg_order_items": avg_items,
            },
        },
    }


@router.get("/akt-sverki")
def akt_sverki(
    telegram_id: int = Query(...),
    limit: int = Query(80, ge=10, le=500),
):
    """Unified dual-currency акт сверки with FIFO allocation.

    Returns {events: [...], uzs_state, usd_state, client_1c_name, ...}.
    Each event has both uzs_amount and usd_amount; orders are one row per
    real_orders.id; payments are grouped per (date, client_id). FIFO runs
    per currency.
    """
    from backend.services.akt_sverki import build as build_akt_sverki
    client_ids, conn = _get_all_client_ids_for_user(telegram_id)
    client_1c_name = None
    if conn and client_ids:
        # Fetch the 1C client name for the header. Prefer the first sibling's
        # client_id_1c (which stores the Cyrillic 1C name).
        row = conn.execute(
            "SELECT client_id_1c FROM allowed_clients "
            "WHERE id IN ({}) AND client_id_1c IS NOT NULL AND client_id_1c != '' "
            "LIMIT 1".format(",".join("?" * len(client_ids))),
            tuple(client_ids),
        ).fetchone()
        if row:
            client_1c_name = row["client_id_1c"]
    if conn:
        conn.close()
    if not client_ids:
        result = build_akt_sverki([])
    else:
        result = build_akt_sverki(client_ids, events_limit=limit)
    result["client_1c_name"] = client_1c_name
    return result


@router.get("/confirmed-order/{wishlist_order_id}")
def confirmed_order_diff(wishlist_order_id: int, telegram_id: int = Query(...)):
    """Diff the wishlist order against the manager-confirmed (1C) version."""
    import json as _json
    client_ids, conn = _get_all_client_ids_for_user(telegram_id)
    if not client_ids:
        if conn:
            conn.close()
        return {"ok": False, "error": "not linked"}

    placeholders = ",".join("?" * len(client_ids))
    wish_order = conn.execute(
        f"SELECT * FROM orders WHERE id = ? AND client_id IN ({placeholders})",
        (wishlist_order_id,) + tuple(client_ids),
    ).fetchone()
    if not wish_order:
        conn.close()
        return JSONResponse({"ok": False, "error": "not your order"}, status_code=403)

    wish_items = [
        {
            "name": r["product_name"] or "",
            "qty": float(r["quantity"] or 0),
            "price": float(r["price"] or 0),
            "currency": r["currency"] or "UZS",
        }
        for r in conn.execute(
            "SELECT product_name, quantity, price, currency FROM order_items WHERE order_id = ?",
            (wishlist_order_id,),
        ).fetchall()
    ]

    confirmed_row = conn.execute(
        "SELECT * FROM confirmed_orders WHERE wishlist_order_id = ? "
        "ORDER BY id DESC LIMIT 1",
        (wishlist_order_id,),
    ).fetchone()
    conn.close()

    if not confirmed_row:
        return {
            "ok": True,
            "wishlist": {
                "id": wishlist_order_id,
                "total_uzs": wish_order["total_uzs"],
                "total_usd": wish_order["total_usd"],
                "items": wish_items,
            },
            "confirmed": None,
        }

    confirmed_items = _json.loads(confirmed_row["items_json"] or "[]")

    # Diff by normalized name. Exact cyrillic name is the stable join key
    # between /realorders exports and the wishlist item snapshots.
    def _norm(s: str) -> str:
        return " ".join((s or "").lower().split())

    wish_by = {}
    for it in wish_items:
        wish_by.setdefault(_norm(it["name"]), []).append(it)
    conf_by = {}
    for it in confirmed_items:
        conf_by.setdefault(_norm(it["name"]), []).append(it)

    kept, reduced, increased, removed, added = [], [], [], [], []
    all_keys = set(wish_by) | set(conf_by)
    for k in all_keys:
        w_list = wish_by.get(k, [])
        c_list = conf_by.get(k, [])
        w_qty = sum(x["qty"] for x in w_list)
        c_qty = sum(x["qty"] for x in c_list)
        label = (w_list or c_list)[0]["name"]
        if w_qty > 0 and c_qty <= 0:
            removed.append({"name": label, "qty": w_qty})
        elif w_qty <= 0 and c_qty > 0:
            added.append({"name": label, "qty": c_qty})
        elif abs(w_qty - c_qty) < 0.001:
            kept.append({"name": label, "qty": w_qty})
        elif c_qty < w_qty:
            reduced.append({"name": label, "wish_qty": w_qty, "confirmed_qty": c_qty})
        else:
            increased.append({"name": label, "wish_qty": w_qty, "confirmed_qty": c_qty})

    for bucket in (kept, reduced, increased, removed, added):
        bucket.sort(key=lambda x: x["name"])

    return {
        "ok": True,
        "wishlist": {
            "id": wishlist_order_id,
            "total_uzs": wish_order["total_uzs"],
            "total_usd": wish_order["total_usd"],
            "items": wish_items,
        },
        "confirmed": {
            "file_name": confirmed_row["file_name"],
            "confirmed_by_name": confirmed_row["confirmed_by_name"],
            "created_at": confirmed_row["created_at"],
            "doc_number_1c": confirmed_row["doc_number_1c"],
            "doc_date": confirmed_row["doc_date"],
            "total_uzs": confirmed_row["total_uzs"],
            "total_usd": confirmed_row["total_usd"],
            "item_count": confirmed_row["item_count"],
            "items": confirmed_items,
        },
        "diff": {
            "kept": kept,
            "reduced": reduced,
            "increased": increased,
            "removed": removed,
            "added": added,
        },
    }


@router.get("/payments")
def list_client_payments(
    telegram_id: int = Query(...),
    limit: int = Query(10, ge=1, le=100),
):
    """Return the most recent payments from client_payments for this client.

    Used by the Cabinet "Oxirgi to'lovlar" section. Newest first, one row per
    1C document. Amount is shown in the document's original currency.
    """
    client_ids, conn = _get_all_client_ids_for_user(telegram_id)
    if not client_ids:
        if conn:
            conn.close()
        return {"ok": True, "payments": [], "linked": False}

    placeholders = ",".join("?" * len(client_ids))
    rows = conn.execute(
        f"""SELECT id, doc_number_1c, doc_date, doc_time,
                   amount_local, amount_currency, currency,
                   basis, cashflow_category
            FROM client_payments
            WHERE client_id IN ({placeholders})
            ORDER BY doc_date DESC, doc_time DESC, id DESC
            LIMIT ?""",
        tuple(client_ids) + (limit,),
    ).fetchall()
    conn.close()

    payments = []
    for r in rows:
        currency = (r["currency"] or "UZS").upper()
        # Prefer the currency-native amount; fall back to local if unset.
        amount = r["amount_currency"] if (r["amount_currency"] or 0) else r["amount_local"]
        payments.append({
            "id": r["id"],
            "doc_number": r["doc_number_1c"],
            "date": r["doc_date"],
            "time": (r["doc_time"] or "")[:5],
            "amount": float(amount or 0),
            "currency": currency,
            "basis": r["basis"] or "",
            "category": r["cashflow_category"] or "",
        })

    return {"ok": True, "payments": payments, "linked": True}
