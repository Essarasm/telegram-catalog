"""Admin dashboard API — internal analytics for Rassvet's decision-makers.

Phase 2: Supplier auto-detection, clean revenue, client segmentation,
         interactive stock lists, product requests, YoY comparison.

All financial endpoints exclude auto-detected suppliers/accounting entries
unless ?include_suppliers=true is passed.
"""
from fastapi import APIRouter, Query, HTTPException
from backend.database import get_db

router = APIRouter(prefix="/api/admin", tags=["admin"])

ADMIN_KEY = "rassvet2026"


def _check_admin(admin_key: str):
    if admin_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ── Supplier Detection ───────────────────────────────────────────

# SQL CTE that identifies suppliers/accounting entries
# Rule: entities where lifetime collection rate < 5% are not real clients
_ENTITY_FILTER_CTE = """
    entity_rates AS (
        SELECT client_name_1c,
               SUM(period_debit) as total_debit,
               SUM(period_credit) as total_credit,
               CASE
                   WHEN SUM(period_debit) = 0 THEN 'inactive'
                   WHEN SUM(period_credit) * 100.0 / SUM(period_debit) < 5.0 THEN 'supplier'
                   ELSE 'client'
               END as entity_type
        FROM client_balances
        GROUP BY client_name_1c
    )
"""

_CLIENTS_ONLY = "entity_rates.entity_type = 'client'"
_SUPPLIERS_ONLY = "entity_rates.entity_type IN ('supplier', 'inactive')"

# Filter out cumulative records (pre-2025) and end-of-month partials (day != 01)
_PERIOD_FILTER = "cb.period_start >= '2025-01-01' AND strftime('%d', cb.period_start) = '01'"



@router.get("/debug-query")
def debug_query(
    q: str = Query(...),
    admin_key: str = Query(...),
):
    """Run a read-only SQL query for debugging. SELECT only."""
    _check_admin(admin_key)
    q_stripped = q.strip().upper()
    if not q_stripped.startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT queries allowed")
    conn = get_db()
    try:
        rows = conn.execute(q).fetchall()
        result = [dict(r) for r in rows]
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))
    conn.close()
    return {"ok": True, "rows": result, "count": len(result)}


@router.post("/set-test-client")
def set_test_client(
    telegram_id: int = Query(...),
    client_id: int = Query(...),
    admin_key: str = Query(...),
):
    """Set a user's client_id for testing. Same as /testclient but via API."""
    _check_admin(admin_key)
    conn = get_db()
    conn.execute(
        "UPDATE users SET client_id = ? WHERE telegram_id = ?",
        (client_id, telegram_id),
    )
    conn.commit()
    row = conn.execute(
        "SELECT u.client_id, ac.name, ac.client_id_1c FROM users u "
        "LEFT JOIN allowed_clients ac ON u.client_id = ac.id "
        "WHERE u.telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.close()
    return {"ok": True, "client_id": row["client_id"], "name": row["name"], "client_id_1c": row["client_id_1c"]}


@router.post("/cleanup-zero-balances")
def cleanup_zero_balances(admin_key: str = Query(...)):
    """Delete all-zero balance records from client_balances.

    These are records where all 6 financial columns are 0 — they carry
    no information and can mask real balances when they become the
    'latest period' for a client.
    """
    _check_admin(admin_key)
    conn = get_db()
    count = conn.execute(
        """SELECT COUNT(*) FROM client_balances
           WHERE opening_debit = 0 AND opening_credit = 0
             AND period_debit = 0 AND period_credit = 0
             AND closing_debit = 0 AND closing_credit = 0"""
    ).fetchone()[0]

    conn.execute(
        """DELETE FROM client_balances
           WHERE opening_debit = 0 AND opening_credit = 0
             AND period_debit = 0 AND period_credit = 0
             AND closing_debit = 0 AND closing_credit = 0"""
    )
    conn.commit()
    conn.close()
    return {"ok": True, "deleted": count}


# ── Backfill order_items.product_name to Cyrillic ────────────────

@router.post("/backfill-order-item-names")
def backfill_order_item_names(admin_key: str = Query(...)):
    """Session A policy: rewrite `order_items.product_name` for old wish-list
    orders so that the stored line-item name matches the Cyrillic 1C name
    (products.name), not the cleaned Latin display name.

    From commit 325b4cc onward, newly placed orders already save the Cyrillic
    name. This endpoint heals orders placed before that commit: for every
    `order_items` row with a linked `product_id`, we overwrite `product_name`
    with `products.name`. Rows with NULL product_id (free-text items, if any)
    are left untouched.

    Idempotent — running it twice is a no-op because the second pass would
    match `products.name` exactly.
    """
    _check_admin(admin_key)
    conn = get_db()
    # Count rows that would actually change so the response is informative.
    to_update = conn.execute(
        """SELECT COUNT(*)
           FROM order_items oi
           JOIN products p ON p.id = oi.product_id
           WHERE oi.product_id IS NOT NULL
             AND (oi.product_name IS NULL OR oi.product_name <> p.name)"""
    ).fetchone()[0]

    conn.execute(
        """UPDATE order_items
           SET product_name = (
               SELECT p.name FROM products p WHERE p.id = order_items.product_id
           )
           WHERE product_id IS NOT NULL
             AND product_name <> (
               SELECT p.name FROM products p WHERE p.id = order_items.product_id
             )"""
    )
    conn.commit()
    conn.close()
    return {"ok": True, "rows_updated": to_update}


# ── Fix weights from product names ───────────────────────────────

@router.post("/fix-weights")
def fix_weights_from_names(admin_key: str = Query(...)):
    """One-time fix: parse weight from product name (original cyrillic)
    when the DB weight is NULL or a round integer that contradicts a
    decimal weight found in the name.

    E.g. name="Грунтовка акриловая 0.75 кг", weight=1 → weight=0.75
    """
    _check_admin(admin_key)
    from backend.services.parse_weight import parse_weight_from_name

    conn = get_db()
    rows = conn.execute("SELECT id, name, weight FROM products").fetchall()

    updated = []
    for row in rows:
        pid, name, db_weight = row["id"], row["name"], row["weight"]
        parsed = parse_weight_from_name(name or "")
        if parsed is None:
            continue

        # Update if: no weight, or DB weight differs from what the name says
        should_update = False
        if db_weight is None or db_weight == 0:
            should_update = True
        elif round(db_weight, 4) != round(parsed, 4):
            # DB weight doesn't match name — could be wrong Excel data
            # or a bad parse from a previous run
            should_update = True

        if should_update:
            conn.execute("UPDATE products SET weight = ? WHERE id = ?", (parsed, pid))
            updated.append({"id": pid, "name": name, "old": db_weight, "new": parsed})

    conn.commit()
    conn.close()
    return {"ok": True, "updated_count": len(updated), "updated": updated[:50]}


# ── Image rotation fix ───────────────────────────────────────────

@router.post("/fix-image-rotation")
def fix_image_rotation(admin_key: str = Query(...)):
    """Re-process all product images: apply EXIF orientation transpose.

    This fixes photos taken on phones that appear rotated because the
    original process_image didn't apply EXIF orientation metadata.
    Images are re-saved in place with correct orientation.
    """
    _check_admin(admin_key)
    import os
    from PIL import Image, ImageOps

    images_dir = os.getenv("IMAGES_DIR", "./images")
    QUALITY = 85

    fixed = []
    skipped = 0
    errors = []

    for fname in os.listdir(images_dir):
        if not fname.lower().endswith(('.jpg', '.jpeg', '.png')):
            continue
        fpath = os.path.join(images_dir, fname)
        try:
            img = Image.open(fpath)
            exif = img.getexif()
            orientation = exif.get(0x0112)  # EXIF Orientation tag
            if orientation and orientation != 1:
                # Has non-default orientation — needs fixing
                img = ImageOps.exif_transpose(img)
                if img.mode in ('RGBA', 'P'):
                    img = img.convert('RGB')
                img.save(fpath, 'JPEG', quality=QUALITY, optimize=True)
                fixed.append({"file": fname, "orientation": orientation})
            else:
                skipped += 1
            img.close()
        except Exception as e:
            errors.append({"file": fname, "error": str(e)})

    return {
        "ok": True,
        "fixed_count": len(fixed),
        "skipped": skipped,
        "errors": errors[:20],
        "fixed": fixed[:50],
    }


@router.post("/rotate-image")
def rotate_image_endpoint(
    product_id: int = Query(...),
    degrees: int = Query(default=270, description="Rotation degrees counter-clockwise. 270 = 90° clockwise fix"),
    admin_key: str = Query(...),
):
    """Manually rotate a product image by given degrees counter-clockwise.

    Common use: degrees=270 to fix a photo rotated 90° clockwise.
    """
    _check_admin(admin_key)
    import os
    from backend.services.image_manager import rotate_image

    images_dir = os.getenv("IMAGES_DIR", "./images")
    fpath = os.path.join(images_dir, f"{product_id}.jpg")

    if not os.path.exists(fpath):
        raise HTTPException(status_code=404, detail=f"No image for product {product_id}")

    rotate_image(fpath, degrees)
    return {"ok": True, "product_id": product_id, "rotated_degrees": degrees}


@router.post("/rotate-images-batch")
def rotate_images_batch(
    product_ids: str = Query(..., description="Comma-separated product IDs"),
    degrees: int = Query(default=270),
    admin_key: str = Query(...),
):
    """Rotate multiple product images at once."""
    _check_admin(admin_key)
    import os
    from backend.services.image_manager import rotate_image

    images_dir = os.getenv("IMAGES_DIR", "./images")
    ids = [int(x.strip()) for x in product_ids.split(",") if x.strip().isdigit()]

    results = []
    for pid in ids:
        fpath = os.path.join(images_dir, f"{pid}.jpg")
        if os.path.exists(fpath):
            rotate_image(fpath, degrees)
            results.append({"id": pid, "status": "rotated"})
        else:
            results.append({"id": pid, "status": "no_image"})

    return {"ok": True, "results": results}


# ── Revenue Trend ────────────────────────────────────────────────


@router.get("/entities")
def entity_classification(admin_key: str = Query(...)):
    """List all entities with their auto-detected type (client/supplier/inactive).
    Used for the review screen where admin can verify classifications.
    """
    _check_admin(admin_key)
    conn = get_db()

    rows = conn.execute(f"""
        WITH {_ENTITY_FILTER_CTE}
        SELECT er.client_name_1c as name,
               er.entity_type,
               er.total_debit,
               er.total_credit,
               ROUND(er.total_credit * 100.0 / NULLIF(er.total_debit, 0), 1) as pay_pct,
               COUNT(DISTINCT cb.period_start) as months_active,
               COUNT(DISTINCT cb.currency) as currencies
        FROM entity_rates er
        JOIN client_balances cb ON cb.client_name_1c = er.client_name_1c
        GROUP BY er.client_name_1c
        ORDER BY er.total_debit DESC
    """).fetchall()

    conn.close()

    suppliers = [dict(r) for r in rows if r["entity_type"] in ("supplier", "inactive")]
    clients = [dict(r) for r in rows if r["entity_type"] == "client"]

    return {
        "ok": True,
        "total_entities": len(rows),
        "clients_count": len(clients),
        "suppliers_count": len(suppliers),
        "suppliers": suppliers,
        "top_clients": clients[:30],
    }


# ── Revenue Trend (clean) ───────────────────────────────────────


@router.get("/revenue")
def revenue_trend(
    admin_key: str = Query(...),
    include_suppliers: bool = Query(False),
):
    """Monthly revenue trend: SUM(period_debit) by month, by currency.
    Excludes suppliers by default.
    """
    _check_admin(admin_key)
    conn = get_db()

    filter_clause = "" if include_suppliers else f"AND {_CLIENTS_ONLY}"

    rows = conn.execute(f"""
        WITH {_ENTITY_FILTER_CTE}
        SELECT cb.period_start, cb.currency,
               SUM(cb.period_debit) as total_shipments,
               SUM(cb.period_credit) as total_collections,
               COUNT(DISTINCT cb.client_name_1c) as active_clients
        FROM client_balances cb
        JOIN entity_rates ON entity_rates.client_name_1c = cb.client_name_1c
        WHERE (cb.period_debit > 0 OR cb.period_credit > 0)
              AND {_PERIOD_FILTER}
              {filter_clause}
        GROUP BY cb.period_start, cb.currency
        ORDER BY cb.period_start ASC
    """).fetchall()

    conn.close()

    # Detect current calendar month to flag as partial
    from datetime import date
    today = date.today()
    current_month_start = today.replace(day=1).isoformat()

    result = {"UZS": [], "USD": []}
    for r in rows:
        cur = r["currency"]
        if cur not in result:
            result[cur] = []
        is_partial = r["period_start"] == current_month_start
        result[cur].append({
            "period": r["period_start"],
            "shipments": round(r["total_shipments"], 2),
            "collections": round(r["total_collections"], 2),
            "active_clients": r["active_clients"],
            "partial": is_partial,
        })

    # Add YoY comparison for each period
    for cur in result:
        periods = result[cur]
        for p in periods:
            try:
                year = int(p["period"][:4])
                month_day = p["period"][4:]
                prev_period = f"{year - 1}{month_day}"
                prev = next((x for x in periods if x["period"] == prev_period), None)
                if prev and prev["shipments"] > 0:
                    p["yoy_growth"] = round((p["shipments"] - prev["shipments"]) / prev["shipments"] * 100, 1)
                    p["yoy_prev_shipments"] = prev["shipments"]
                else:
                    p["yoy_growth"] = None
                    p["yoy_prev_shipments"] = None
            except Exception:
                p["yoy_growth"] = None
                p["yoy_prev_shipments"] = None

    # Compute last_full_month (the most recent non-partial month)
    last_full = {}
    for cur in result:
        full_months = [p for p in result[cur] if not p.get("partial")]
        last_full[cur] = full_months[-1]["period"] if full_months else None

    return {"ok": True, "data": result, "last_full_month": last_full}


# ── Collection Rate (clean) ──────────────────────────────────────


@router.get("/collections")
def collection_rate(
    admin_key: str = Query(...),
    include_suppliers: bool = Query(False),
):
    """Collection rate by month — excludes suppliers by default."""
    _check_admin(admin_key)
    conn = get_db()

    filter_clause = "" if include_suppliers else f"AND {_CLIENTS_ONLY}"

    rows = conn.execute(f"""
        WITH {_ENTITY_FILTER_CTE}
        SELECT cb.period_start, cb.currency,
               SUM(cb.period_debit) as total_debit,
               SUM(cb.period_credit) as total_credit
        FROM client_balances cb
        JOIN entity_rates ON entity_rates.client_name_1c = cb.client_name_1c
        WHERE {_PERIOD_FILTER} {filter_clause}
        GROUP BY cb.period_start, cb.currency
        ORDER BY cb.period_start ASC
    """).fetchall()

    conn.close()

    result = {"UZS": [], "USD": []}
    for r in rows:
        cur = r["currency"]
        if cur not in result:
            result[cur] = []
        debit = r["total_debit"] or 0
        credit = r["total_credit"] or 0
        rate = round(credit / debit * 100, 1) if debit > 0 else 0
        result[cur].append({
            "period": r["period_start"],
            "total_shipped": round(debit, 2),
            "total_collected": round(credit, 2),
            "collection_rate": rate,
        })

    return {"ok": True, "data": result}


# ── Top Clients (clean) ─────────────────────────────────────────


@router.get("/top-clients")
def top_clients(
    admin_key: str = Query(...),
    currency: str = Query("UZS"),
    limit: int = Query(50, ge=1, le=200),
    include_suppliers: bool = Query(False),
):
    """Top clients ranked by total shipments — excludes suppliers by default."""
    _check_admin(admin_key)
    conn = get_db()

    filter_clause = "" if include_suppliers else f"AND {_CLIENTS_ONLY}"

    rows = conn.execute(f"""
        WITH {_ENTITY_FILTER_CTE}
        SELECT cb.client_name_1c,
               SUM(cb.period_debit) as total_shipped,
               SUM(cb.period_credit) as total_paid,
               COUNT(DISTINCT cb.period_start) as months_active,
               entity_rates.entity_type
        FROM client_balances cb
        JOIN entity_rates ON entity_rates.client_name_1c = cb.client_name_1c
        WHERE cb.currency = ? {filter_clause}
        GROUP BY cb.client_name_1c
        ORDER BY total_shipped DESC
        LIMIT ?
    """, (currency, limit)).fetchall()

    clients = []
    for r in rows:
        latest = conn.execute("""
            SELECT closing_debit, closing_credit, period_start
            FROM client_balances
            WHERE client_name_1c = ? AND currency = ?
            ORDER BY period_start DESC LIMIT 1
        """, (r["client_name_1c"], currency)).fetchone()

        balance = 0
        latest_period = ""
        if latest:
            balance = (latest["closing_debit"] or 0) - (latest["closing_credit"] or 0)
            latest_period = latest["period_start"]

        # Determine segment based on last 3 months vs previous 3 months
        recent = conn.execute("""
            SELECT SUM(period_debit) as recent_ship
            FROM client_balances
            WHERE client_name_1c = ? AND currency = ?
            ORDER BY period_start DESC LIMIT 3
        """, (r["client_name_1c"], currency)).fetchone()

        older = conn.execute("""
            SELECT SUM(period_debit) as older_ship
            FROM client_balances
            WHERE client_name_1c = ? AND currency = ?
              AND period_start < (
                  SELECT period_start FROM client_balances
                  WHERE client_name_1c = ? AND currency = ?
                  ORDER BY period_start DESC LIMIT 1 OFFSET 2
              )
            ORDER BY period_start DESC LIMIT 3
        """, (r["client_name_1c"], currency, r["client_name_1c"], currency)).fetchone()

        recent_val = (recent["recent_ship"] or 0) if recent else 0
        older_val = (older["older_ship"] or 0) if older else 0

        if r["months_active"] <= 2:
            segment = "new"
        elif recent_val == 0:
            segment = "dormant"
        elif older_val > 0 and recent_val > older_val * 1.2:
            segment = "growing"
        elif older_val > 0 and recent_val < older_val * 0.5:
            segment = "declining"
        else:
            segment = "stable"

        pay_pct = round(r["total_paid"] * 100 / r["total_shipped"], 1) if r["total_shipped"] else 0

        clients.append({
            "name": r["client_name_1c"],
            "total_shipped": round(r["total_shipped"], 2),
            "total_paid": round(r["total_paid"], 2),
            "balance": round(balance, 2),
            "months_active": r["months_active"],
            "latest_period": latest_period,
            "segment": segment,
            "pay_pct": pay_pct,
            "entity_type": r["entity_type"],
        })

    conn.close()
    return {"ok": True, "currency": currency, "clients": clients}


# ── Receivables (clean) ─────────────────────────────────────────


@router.get("/receivables")
def receivables(
    admin_key: str = Query(...),
    currency: str = Query("UZS"),
):
    """Total receivables + aging — excludes suppliers automatically."""
    _check_admin(admin_key)
    conn = get_db()

    latest_period = conn.execute(
        "SELECT MAX(period_start) FROM client_balances WHERE currency = ?",
        (currency,)
    ).fetchone()[0]

    if not latest_period:
        conn.close()
        return {"ok": True, "total_receivable": 0, "aging": {}, "currency": currency}

    # Only real clients with positive balance
    rows = conn.execute(f"""
        WITH {_ENTITY_FILTER_CTE}
        SELECT cb.client_name_1c,
               cb.closing_debit - cb.closing_credit as balance
        FROM client_balances cb
        JOIN entity_rates ON entity_rates.client_name_1c = cb.client_name_1c
        WHERE cb.period_start = ? AND cb.currency = ?
          AND (cb.closing_debit - cb.closing_credit) > 0
          AND {_CLIENTS_ONLY}
        ORDER BY balance DESC
    """, (latest_period, currency)).fetchall()

    total_receivable = 0
    aging = {"current": 0, "30_60": 0, "60_90": 0, "90_plus": 0}
    client_count = {"current": 0, "30_60": 0, "60_90": 0, "90_plus": 0}
    aging_clients = {"current": [], "30_60": [], "60_90": [], "90_plus": []}

    for r in rows:
        balance = r["balance"]
        total_receivable += balance
        client_name = r["client_name_1c"]

        months_unpaid = 0
        history = conn.execute("""
            SELECT period_start, period_credit
            FROM client_balances
            WHERE client_name_1c = ? AND currency = ?
            ORDER BY period_start DESC LIMIT 6
        """, (client_name, currency)).fetchall()

        for h in history:
            if (h["period_credit"] or 0) == 0:
                months_unpaid += 1
            else:
                break

        if months_unpaid <= 1:
            bucket = "current"
        elif months_unpaid <= 2:
            bucket = "30_60"
        elif months_unpaid <= 3:
            bucket = "60_90"
        else:
            bucket = "90_plus"

        aging[bucket] += balance
        client_count[bucket] += 1
        aging_clients[bucket].append({
            "name": client_name,
            "balance": round(balance, 2),
            "months_unpaid": months_unpaid,
        })

    conn.close()

    # Sort each bucket by balance descending and keep top 10
    for bucket in aging_clients:
        aging_clients[bucket].sort(key=lambda x: x["balance"], reverse=True)
        aging_clients[bucket] = aging_clients[bucket][:10]

    return {
        "ok": True,
        "currency": currency,
        "latest_period": latest_period,
        "total_receivable": round(total_receivable, 2),
        "total_clients_with_debt": len(rows),
        "aging": {k: round(v, 2) for k, v in aging.items()},
        "aging_client_count": client_count,
        "aging_top_clients": aging_clients,
        "methodology": (
            "Aging buckets are based on consecutive months without any payment "
            "(period_credit = 0). 'Current' = paid within last month. '30-60' = "
            "1-2 months without payments. '60-90' = 2-3 months. '90+' = 3+ months. "
            "Calculated from monthly 1C 'оборотно-сальдовая' data, not invoice dates."
        ),
    }


# ── Client History (drill-down) ──────────────────────────────────


@router.get("/client/{client_name}/history")
def client_history(
    client_name: str,
    admin_key: str = Query(...),
):
    """Per-client balance history — 15-month chart data."""
    _check_admin(admin_key)
    conn = get_db()

    rows = conn.execute("""
        SELECT currency, period_start, period_end,
               opening_debit, opening_credit,
               period_debit, period_credit,
               closing_debit, closing_credit
        FROM client_balances
        WHERE client_name_1c = ?
        ORDER BY currency, period_start ASC
    """, (client_name,)).fetchall()

    conn.close()

    if not rows:
        return {"ok": True, "client_name": client_name, "history": {}}

    history = {}
    for r in rows:
        cur = r["currency"]
        if cur not in history:
            history[cur] = []
        history[cur].append({
            "period": r["period_start"],
            "period_end": r["period_end"],
            "period_debit": round(r["period_debit"] or 0, 2),
            "period_credit": round(r["period_credit"] or 0, 2),
            "closing_debit": round(r["closing_debit"] or 0, 2),
            "closing_credit": round(r["closing_credit"] or 0, 2),
            "balance": round((r["closing_debit"] or 0) - (r["closing_credit"] or 0), 2),
        })

    return {"ok": True, "client_name": client_name, "history": history}


# ── Stock Status (enhanced) ──────────────────────────────────────


@router.get("/demand-signals")
def demand_signals(
    admin_key: str = Query(...),
    days: int = Query(30, ge=1, le=365),
    threshold: int = Query(5, ge=1, le=100),
):
    """Top out-of-stock products ordered by clients.

    Returns products that have been ordered while marked out-of-stock,
    ranked by total demand (order count). Items crossing the threshold
    are flagged as noteworthy signals.
    """
    _check_admin(admin_key)
    conn = get_db()

    # Top out-of-stock products by demand
    top_demand = conn.execute("""
        SELECT ds.product_id,
               p.name_display,
               p.name as name_cyrillic,
               pr.name as producer_name,
               c.name as category_name,
               p.stock_status as current_stock_status,
               COUNT(DISTINCT ds.order_id) as order_count,
               SUM(ds.quantity) as total_quantity,
               COUNT(DISTINCT ds.telegram_id) as unique_clients,
               MIN(ds.created_at) as first_signal,
               MAX(ds.created_at) as last_signal
        FROM demand_signals ds
        JOIN products p ON p.id = ds.product_id
        JOIN producers pr ON pr.id = p.producer_id
        JOIN categories c ON c.id = p.category_id
        WHERE ds.created_at >= datetime('now', ?)
        GROUP BY ds.product_id
        ORDER BY order_count DESC
        LIMIT 50
    """, (f"-{days} days",)).fetchall()

    # Summary stats
    total_signals = conn.execute(
        "SELECT COUNT(*) FROM demand_signals WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    unique_products = conn.execute(
        "SELECT COUNT(DISTINCT product_id) FROM demand_signals WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    conn.close()

    items = []
    noteworthy_count = 0
    for r in top_demand:
        noteworthy = r["order_count"] >= threshold
        if noteworthy:
            noteworthy_count += 1
        items.append({
            "product_id": r["product_id"],
            "name": r["name_display"] or r["name_cyrillic"],
            "name_cyrillic": r["name_cyrillic"],
            "producer": r["producer_name"],
            "category": r["category_name"],
            "current_stock": r["current_stock_status"] or "unknown",
            "order_count": r["order_count"],
            "total_quantity": r["total_quantity"],
            "unique_clients": r["unique_clients"],
            "first_signal": r["first_signal"],
            "last_signal": r["last_signal"],
            "noteworthy": noteworthy,
        })

    return {
        "ok": True,
        "days": days,
        "threshold": threshold,
        "total_signals": total_signals,
        "unique_products": unique_products,
        "noteworthy_count": noteworthy_count,
        "items": items,
    }


# ── Stock Status ─────────────────────────────────────────────────



@router.get("/stock-status")
def stock_status(admin_key: str = Query(...)):
    """Enhanced stock overview with full item lists for each category.

    Returns stock_updated_at per item and a `stale_items` list:
    items not present in the most recent stock upload (likely 0 in 1C
    but excluded from "Прайс лист" export, leaving stale values in DB).
    """
    _check_admin(admin_key)
    conn = get_db()

    # Most recent stock upload timestamp — items NOT updated at this exact
    # time are considered stale (missing from latest upload).
    latest_upload = conn.execute(
        "SELECT MAX(stock_updated_at) FROM products WHERE stock_updated_at IS NOT NULL"
    ).fetchone()[0]

    total_products = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1"
    ).fetchone()[0]

    in_stock = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND stock_quantity > 10"
    ).fetchone()[0]

    low_stock = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND stock_quantity > 0 AND stock_quantity <= 10"
    ).fetchone()[0]

    out_of_stock = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND stock_quantity = 0"
    ).fetchone()[0]

    no_data = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND stock_quantity IS NULL"
    ).fetchone()[0]

    with_photos = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND image_path IS NOT NULL AND image_path != ''"
    ).fetchone()[0]

    # Stale items: have stock data but were NOT in the latest upload.
    # Most likely 0 in 1C (excluded from "Прайс лист" export which only
    # lists in-stock items), so DB still shows old positive value.
    stale_count = 0
    if latest_upload:
        # Anything updated >5 minutes before latest upload is from a
        # previous upload session = not in current 1C export
        stale_count = conn.execute(
            """SELECT COUNT(*) FROM products
               WHERE is_active = 1
                 AND stock_quantity > 0
                 AND stock_updated_at IS NOT NULL
                 AND datetime(stock_updated_at) < datetime(?, '-5 minutes')""",
            (latest_upload,)
        ).fetchone()[0]

    # Full list of low stock items (for uncle's review — Cyrillic names)
    low_stock_items = conn.execute("""
        SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
               pr.name as producer, c.name as category,
               p.stock_quantity, p.price_uzs, p.price_usd, p.stock_updated_at
        FROM products p
        JOIN producers pr ON pr.id = p.producer_id
        JOIN categories c ON c.id = p.category_id
        WHERE p.is_active = 1 AND p.stock_quantity > 0 AND p.stock_quantity <= 10
        ORDER BY p.stock_quantity ASC, p.name
    """).fetchall()

    # Full list of out-of-stock items
    out_of_stock_items = conn.execute("""
        SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
               pr.name as producer, c.name as category,
               p.stock_quantity, p.price_uzs, p.price_usd, p.stock_updated_at
        FROM products p
        JOIN producers pr ON pr.id = p.producer_id
        JOIN categories c ON c.id = p.category_id
        WHERE p.is_active = 1 AND p.stock_quantity = 0
        ORDER BY p.name
    """).fetchall()

    # No data items
    no_data_items = conn.execute("""
        SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
               pr.name as producer, c.name as category,
               p.price_uzs, p.price_usd, p.stock_updated_at
        FROM products p
        JOIN producers pr ON pr.id = p.producer_id
        JOIN categories c ON c.id = p.category_id
        WHERE p.is_active = 1 AND p.stock_quantity IS NULL
        ORDER BY pr.name, p.name
    """).fetchall()

    # Stale items: stock > 0 but not updated in the latest upload.
    # These probably have 0 in 1C but were excluded from the export.
    stale_items = []
    if latest_upload:
        stale_items = conn.execute(
            """SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
                      pr.name as producer, c.name as category,
                      p.stock_quantity, p.price_uzs, p.price_usd, p.stock_updated_at
               FROM products p
               JOIN producers pr ON pr.id = p.producer_id
               JOIN categories c ON c.id = p.category_id
               WHERE p.is_active = 1
                 AND p.stock_quantity > 0
                 AND p.stock_updated_at IS NOT NULL
                 AND datetime(p.stock_updated_at) < datetime(?, '-5 minutes')
               ORDER BY p.stock_quantity ASC, p.name""",
            (latest_upload,)
        ).fetchall()

    # Top ordered products (from app orders)
    top_ordered_app = conn.execute("""
        SELECT oi.product_name, oi.producer_name,
               SUM(oi.quantity) as total_qty,
               COUNT(DISTINCT oi.order_id) as order_count,
               oi.currency, oi.price
        FROM order_items oi
        JOIN orders o ON o.id = oi.order_id
        GROUP BY oi.product_name
        ORDER BY order_count DESC, total_qty DESC
        LIMIT 30
    """).fetchall()

    # Most clicked products from search (demand signal)
    top_clicked = conn.execute("""
        SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
               pr.name as producer, p.stock_quantity,
               COUNT(*) as click_count
        FROM search_clicks sc
        JOIN products p ON p.id = sc.product_id
        JOIN producers pr ON pr.id = p.producer_id
        WHERE sc.created_at >= datetime('now', '-30 days')
        GROUP BY sc.product_id
        ORDER BY click_count DESC
        LIMIT 20
    """).fetchall()

    # Category breakdown
    categories = conn.execute("""
        SELECT c.name as category,
               COUNT(*) as product_count,
               SUM(CASE WHEN p.stock_quantity > 0 THEN 1 ELSE 0 END) as in_stock_count,
               SUM(CASE WHEN p.stock_quantity = 0 THEN 1 ELSE 0 END) as out_of_stock_count,
               SUM(CASE WHEN p.stock_quantity IS NULL THEN 1 ELSE 0 END) as no_data_count,
               SUM(CASE WHEN p.image_path IS NOT NULL AND p.image_path != '' THEN 1 ELSE 0 END) as with_photo
        FROM products p
        JOIN categories c ON c.id = p.category_id
        WHERE p.is_active = 1
        GROUP BY c.id
        ORDER BY product_count DESC
    """).fetchall()

    # Product requests ("Can't find?" submissions — unmet demand)
    product_requests = conn.execute("""
        SELECT request_text, COUNT(*) as request_count,
               MAX(created_at) as last_requested
        FROM product_requests
        GROUP BY LOWER(TRIM(request_text))
        ORDER BY request_count DESC
        LIMIT 20
    """).fetchall()

    # Identified suppliers from 1C (entities flagged as suppliers)
    suppliers_1c = conn.execute(f"""
        WITH {_ENTITY_FILTER_CTE}
        SELECT client_name_1c as name,
               total_debit, total_credit
        FROM entity_rates
        WHERE entity_type = 'supplier'
        ORDER BY total_debit DESC
    """).fetchall()

    # App producers for comparison
    app_producers = conn.execute("""
        SELECT pr.name, pr.product_count,
               SUM(CASE WHEN p.stock_quantity > 0 THEN 1 ELSE 0 END) as in_stock,
               SUM(CASE WHEN p.image_path IS NOT NULL AND p.image_path != '' THEN 1 ELSE 0 END) as with_photo
        FROM producers pr
        LEFT JOIN products p ON p.producer_id = pr.id AND p.is_active = 1
        GROUP BY pr.id
        ORDER BY pr.product_count DESC
        LIMIT 30
    """).fetchall()

    conn.close()

    return {
        "ok": True,
        "latest_upload": latest_upload,
        "stock_summary": {
            "total": total_products,
            "in_stock": in_stock,
            "low_stock": low_stock,
            "out_of_stock": out_of_stock,
            "no_data": no_data,
            "with_photos": with_photos,
            "stale": stale_count,
        },
        "low_stock_items": [dict(r) for r in low_stock_items],
        "out_of_stock_items": [dict(r) for r in out_of_stock_items],
        "no_data_items": [dict(r) for r in no_data_items],
        "stale_items": [dict(r) for r in stale_items],
        "top_ordered_app": [dict(r) for r in top_ordered_app],
        "top_clicked": [dict(r) for r in top_clicked],
        "categories": [dict(r) for r in categories],
        "product_requests": [dict(r) for r in product_requests],
        "suppliers_1c": [dict(r) for r in suppliers_1c],
        "app_producers": [dict(r) for r in app_producers],
    }


# ── Search Insights ──────────────────────────────────────────────


@router.get("/search-insights")
def search_insights(
    admin_key: str = Query(...),
    days: int = Query(30, ge=1, le=365),
):
    """Search analytics summary."""
    _check_admin(admin_key)
    conn = get_db()

    total_searches = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",)
    ).fetchone()[0]

    unique_users = conn.execute(
        "SELECT COUNT(DISTINCT telegram_id) FROM search_logs WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",)
    ).fetchone()[0]

    zero_result_count = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE results_count = 0 AND created_at >= datetime('now', ?)",
        (f"-{days} days",)
    ).fetchone()[0]

    top_queries = conn.execute("""
        SELECT query, COUNT(*) as count,
               ROUND(AVG(results_count), 1) as avg_results
        FROM search_logs
        WHERE created_at >= datetime('now', ?)
        GROUP BY query ORDER BY count DESC LIMIT 20
    """, (f"-{days} days",)).fetchall()

    zero_results = conn.execute("""
        SELECT query, COUNT(*) as count,
               COUNT(DISTINCT telegram_id) as unique_users
        FROM search_logs
        WHERE results_count = 0 AND created_at >= datetime('now', ?)
        GROUP BY query ORDER BY count DESC LIMIT 20
    """, (f"-{days} days",)).fetchall()

    daily_volume = conn.execute("""
        SELECT DATE(created_at) as day,
               COUNT(*) as searches,
               COUNT(DISTINCT telegram_id) as users,
               SUM(CASE WHEN results_count = 0 THEN 1 ELSE 0 END) as zero_results
        FROM search_logs
        WHERE created_at >= datetime('now', ?)
        GROUP BY DATE(created_at) ORDER BY day ASC
    """, (f"-{days} days",)).fetchall()

    conn.close()

    return {
        "ok": True,
        "days": days,
        "overview": {
            "total_searches": total_searches,
            "unique_users": unique_users,
            "zero_result_count": zero_result_count,
            "zero_result_pct": round(zero_result_count / total_searches * 100, 1) if total_searches else 0,
        },
        "top_queries": [dict(r) for r in top_queries],
        "zero_results": [dict(r) for r in zero_results],
        "daily_volume": [dict(r) for r in daily_volume],
    }


# ── Platform Health ──────────────────────────────────────────────


@router.get("/platform-health")
def platform_health(admin_key: str = Query(...)):
    """Platform health metrics."""
    _check_admin(admin_key)
    conn = get_db()

    total_registered = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    total_approved = conn.execute("SELECT COUNT(*) FROM users WHERE is_approved = 1").fetchone()[0]
    users_with_orders = conn.execute("SELECT COUNT(DISTINCT telegram_id) FROM orders").fetchone()[0]

    repeat_users = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT telegram_id FROM orders GROUP BY telegram_id HAVING COUNT(*) >= 2
        )
    """).fetchone()[0]

    total_whitelist = conn.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]

    order_trend = conn.execute("""
        SELECT strftime('%Y-%m', created_at) as month,
               COUNT(*) as order_count,
               COUNT(DISTINCT telegram_id) as unique_buyers,
               SUM(total_usd) as total_usd,
               SUM(total_uzs) as total_uzs,
               SUM(item_count) as total_items
        FROM orders GROUP BY month ORDER BY month ASC
    """).fetchall()

    total_products = conn.execute("SELECT COUNT(*) FROM products WHERE is_active = 1").fetchone()[0]
    with_photos = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND image_path IS NOT NULL AND image_path != ''"
    ).fetchone()[0]
    with_stock = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND stock_quantity IS NOT NULL"
    ).fetchone()[0]
    with_price_usd = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND price_usd > 0"
    ).fetchone()[0]
    with_price_uzs = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND price_uzs > 0"
    ).fetchone()[0]

    clients_with_1c_id = conn.execute(
        "SELECT COUNT(*) FROM allowed_clients WHERE client_id_1c IS NOT NULL AND client_id_1c != ''"
    ).fetchone()[0]
    clients_with_telegram = conn.execute(
        "SELECT COUNT(*) FROM allowed_clients WHERE matched_telegram_id IS NOT NULL"
    ).fetchone()[0]

    balance_clients = conn.execute("SELECT COUNT(DISTINCT client_name_1c) FROM client_balances").fetchone()[0]
    balance_periods = conn.execute("SELECT COUNT(DISTINCT period_start) FROM client_balances").fetchone()[0]

    recent_orders = conn.execute("SELECT COUNT(*) FROM orders WHERE created_at >= datetime('now', '-7 days')").fetchone()[0]
    recent_searches = conn.execute("SELECT COUNT(*) FROM search_logs WHERE created_at >= datetime('now', '-7 days')").fetchone()[0]
    recent_registrations = conn.execute("SELECT COUNT(*) FROM users WHERE registered_at >= datetime('now', '-7 days')").fetchone()[0]

    conn.close()

    return {
        "ok": True,
        "registration_funnel": {
            "whitelist": total_whitelist,
            "registered": total_registered,
            "approved": total_approved,
            "ordered": users_with_orders,
            "repeat": repeat_users,
        },
        "order_trend": [dict(r) for r in order_trend],
        "data_quality": {
            "total_products": total_products,
            "with_photos": with_photos,
            "photo_pct": round(with_photos / total_products * 100, 1) if total_products else 0,
            "with_stock_data": with_stock,
            "stock_pct": round(with_stock / total_products * 100, 1) if total_products else 0,
            "with_price_usd": with_price_usd,
            "with_price_uzs": with_price_uzs,
        },
        "client_data": {
            "total_whitelist": total_whitelist,
            "with_1c_id": clients_with_1c_id,
            "with_telegram": clients_with_telegram,
            "balance_clients": balance_clients,
            "balance_periods": balance_periods,
        },
        "recent_activity": {
            "orders_7d": recent_orders,
            "searches_7d": recent_searches,
            "registrations_7d": recent_registrations,
        },
    }
