"""Admin data-ops endpoints — one-off data corrections, image rotation,
and product-review queue surfaces.

Extracted from `admin.py` to keep that file under the 2,000-line god-module
canary. Endpoints kept on their original `/api/admin/...` URLs so the admin
dashboard frontend and any external scripts do not have to change.
"""
from fastapi import APIRouter, Query, HTTPException, UploadFile, File, Form, Body
from fastapi.responses import JSONResponse

from backend.admin_auth import check_admin_key
from backend.database import get_db


router = APIRouter(prefix="/api/admin", tags=["admin"])


def _check_admin(admin_key: str):
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


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
    """Resolve products.weight to the authoritative kg/unit (Error Log #89).

    Anchors on the sales-derived weight (1C сумма веса / qty), forces kg-sold
    units to 1.0, and only falls back to name-parse for never-shipped products.
    Superseded the old pure name-parse writer, which re-introduced pack weights
    (a /20 кг/ kg-sold item became 20). Goes through the shared helper per
    .claude/rules/12-dual-source-columns.md.
    """
    _check_admin(admin_key)
    from backend.services.product_weight import compute_sales_weights, authoritative_weight

    conn = get_db()
    sales_weights = compute_sales_weights(conn)
    rows = conn.execute(
        "SELECT id, name, weight, unit FROM products WHERE is_active = 1"
    ).fetchall()

    updated = []
    for row in rows:
        pid, name, db_weight, unit = row["id"], row["name"], row["weight"], row["unit"]
        target = authoritative_weight(db_weight, unit, sales_weights.get(pid), name=name)
        if target is None:
            continue
        if db_weight is None or round(db_weight, 4) != round(target, 4):
            conn.execute("UPDATE products SET weight = ? WHERE id = ?", (target, pid))
            updated.append({"id": pid, "name": name, "old": db_weight, "new": target})

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


@router.post("/upload-images")
async def upload_images(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Upload a ZIP of {product_id}.{png,jpg,jpeg,webp} files. Converts every
    image to WebP q=80 on intake (storage + bandwidth durable fix), removes any
    sibling-extension stragglers for the same product_id, then runs sync_images.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    import zipfile
    import tempfile
    import os
    from pathlib import Path
    from backend.services.convert_to_webp import encode_webp_from_bytes

    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)

    images_dir = Path(os.getenv("IMAGES_DIR", "./images"))
    images_dir.mkdir(parents=True, exist_ok=True)

    added = replaced = skipped = errors = 0
    error_files: list[dict] = []
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    try:
        with zipfile.ZipFile(tmp_path, "r") as zf:
            for name in zf.namelist():
                base = Path(name).name
                if not base or base.startswith(".") or base.startswith("__"):
                    continue
                ext = Path(base).suffix.lower()
                if ext not in ('.png', '.jpg', '.jpeg', '.webp'):
                    continue
                stem = Path(base).stem
                try:
                    int(stem)
                except ValueError:
                    skipped += 1
                    continue
                dest = images_dir / f"{stem}.webp"
                existed = any(
                    (images_dir / f"{stem}{e}").exists()
                    for e in ('.webp', '.png', '.jpg', '.jpeg')
                )
                try:
                    src_bytes = zf.read(name)
                    if ext == '.webp':
                        # Already WebP — passthrough, don't re-encode.
                        with open(dest, 'wb') as dst:
                            dst.write(src_bytes)
                    else:
                        encode_webp_from_bytes(src_bytes, dest)
                except Exception as e:
                    errors += 1
                    error_files.append({"file": base, "error": repr(e)[:200]})
                    continue
                # Remove sibling-extension stragglers (old PNG/JPG for this product).
                for stale_ext in ('.png', '.jpg', '.jpeg'):
                    stale = images_dir / f"{stem}{stale_ext}"
                    if stale.exists():
                        stale.unlink(missing_ok=True)
                if existed:
                    replaced += 1
                else:
                    added += 1
    finally:
        os.unlink(tmp_path)

    from backend.services.sync_images import sync
    sync()

    total = sum(1 for f in images_dir.iterdir()
                if f.suffix.lower() in ('.png', '.jpg', '.jpeg', '.webp'))

    return {"ok": True, "added": added, "replaced": replaced,
            "skipped": skipped, "errors": errors, "error_files": error_files[:10],
            "total": total}


@router.get("/unmatched-names")
def get_unmatched_names(admin_key: str = Query(...)):
    """List unresolved unmatched import names."""
    _check_admin(admin_key)
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, occurrences, source, created_at "
        "FROM unmatched_import_names WHERE resolved = 0 "
        "ORDER BY name"
    ).fetchall()
    conn.close()
    return {"ok": True, "count": len(rows), "names": [dict(r) for r in rows]}


# ── New-products review queue ────────────────────────────────────
#
# Surfaces items in "Yangi mahsulotlar" so admins can promote them
# (or anything still flagged auto_classified=1) to their correct
# category/producer. Without this, items dumped into the new-arrivals
# bucket by /prices auto-add or /realorders SKU ingest accumulate
# silently — no aging, no expiry. See product_classifier.py.

@router.get("/new-products-pending")
def new_products_pending(
    admin_key: str = Query(...),
    include_classified: int = Query(0),
):
    """Products that need review.

    Default: items currently sitting in "Yangi mahsulotlar" (no brand-prefix
    match → fell back to the new-arrivals bucket).

    include_classified=1: also include products auto-classified to a brand
    family but never confirmed by an admin (auto_classified=1). These were
    placed by prefix match — usually correct but worth a glance.
    """
    _check_admin(admin_key)
    conn = get_db()

    new_cat = conn.execute(
        "SELECT id FROM categories WHERE name = ?", ("Yangi mahsulotlar",)
    ).fetchone()
    new_cat_id = new_cat["id"] if new_cat else None

    where_parts = ["p.is_active = 1"]
    if include_classified:
        # Items in Yangi mahsulotlar OR auto-classified anywhere
        if new_cat_id is not None:
            where_parts.append(
                f"(p.category_id = {new_cat_id} OR p.auto_classified = 1)"
            )
        else:
            where_parts.append("p.auto_classified = 1")
    else:
        if new_cat_id is None:
            conn.close()
            return {"ok": True, "count": 0, "products": [], "categories": [], "producers": []}
        where_parts.append(f"p.category_id = {new_cat_id}")

    where_sql = " AND ".join(where_parts)

    rows = conn.execute(f"""
        SELECT p.id, p.name, p.name_display, p.created_at, p.auto_classified,
               p.price_usd, p.price_uzs, p.stock_status,
               p.category_id, p.producer_id,
               c.name AS category_name, pr.name AS producer_name
        FROM products p
        LEFT JOIN categories c ON c.id = p.category_id
        LEFT JOIN producers pr ON pr.id = p.producer_id
        WHERE {where_sql}
        ORDER BY (p.created_at IS NULL), p.created_at DESC, p.id DESC
        LIMIT 500
    """).fetchall()

    # Side cache: full lists so the UI can render reassign dropdowns
    categories = conn.execute(
        "SELECT id, name FROM categories ORDER BY name"
    ).fetchall()
    producers = conn.execute(
        "SELECT id, name FROM producers ORDER BY name"
    ).fetchall()

    conn.close()
    return {
        "ok": True,
        "count": len(rows),
        "new_arrivals_category_id": new_cat_id,
        "products": [dict(r) for r in rows],
        "categories": [dict(c) for c in categories],
        "producers": [dict(p) for p in producers],
    }


@router.post("/reassign-product")
def reassign_product(
    product_id: int = Form(...),
    category_id: int = Form(...),
    producer_id: int = Form(...),
    admin_key: str = Form(...),
):
    """Move a product to a different category/producer and clear the
    auto_classified flag so it leaves the review queue.

    Both IDs must reference existing rows; the endpoint refuses partials
    rather than silently leaving the product mis-assigned.
    """
    _check_admin(admin_key)
    conn = get_db()

    prod = conn.execute(
        "SELECT id, category_id, producer_id FROM products WHERE id = ?",
        (product_id,),
    ).fetchone()
    if not prod:
        conn.close()
        raise HTTPException(status_code=404, detail="Product not found")

    cat_ok = conn.execute(
        "SELECT 1 FROM categories WHERE id = ?", (category_id,)
    ).fetchone()
    prod_ok = conn.execute(
        "SELECT 1 FROM producers WHERE id = ?", (producer_id,)
    ).fetchone()
    if not cat_ok:
        conn.close()
        raise HTTPException(status_code=400, detail="Invalid category_id")
    if not prod_ok:
        conn.close()
        raise HTTPException(status_code=400, detail="Invalid producer_id")

    conn.execute(
        "UPDATE products SET category_id = ?, producer_id = ?, auto_classified = 0 "
        "WHERE id = ?",
        (category_id, producer_id, product_id),
    )
    # Refresh denorm counts on the affected categories/producers
    conn.execute("""
        UPDATE categories SET product_count = (
            SELECT COUNT(*) FROM products
            WHERE products.category_id = categories.id AND is_active = 1
        )
    """)
    conn.execute("""
        UPDATE producers SET product_count = (
            SELECT COUNT(*) FROM products
            WHERE products.producer_id = producers.id AND is_active = 1
        )
    """)
    conn.commit()
    conn.close()
    return {"ok": True, "product_id": product_id,
            "category_id": category_id, "producer_id": producer_id}


@router.get("/cleanup-queue")
def cleanup_queue(
    admin_key: str = Query(...),
    window_days: int = Query(60, ge=1, le=365),
    min_orders: int = Query(1, ge=0),
    limit: int = Query(1500, ge=1, le=5000),
):
    """Active products ranked by recent order frequency, with weight suggestions
    and photo/supplier status, for the Product Cleanup admin tab.

    Each row carries enough context that the UI can render current-vs-suggested
    weight, the matched source substring, photo state, and producer/supplier
    without follow-up requests.
    """
    _check_admin(admin_key)
    from backend.services.product_weight import compute_sales_weights, suggest_weight
    from backend.services.photo_state import photo_state

    conn = get_db()
    # Sales-derived weight anchor — the suggestion the tab shows must be the
    # ground-truth kg/unit, not a name-parse pack weight (Error Log #89, rule #12).
    sales_weights = compute_sales_weights(conn)
    rows = conn.execute(
        """
        SELECT
          p.id,
          p.name,
          p.name_display,
          p.weight        AS current_weight,
          p.unit          AS current_unit,
          p.image_path,
          p.category_id,
          p.producer_id,
          p.latest_supplier_id,
          c.name          AS category_name,
          pr.name         AS producer_name,
          s.name_1c       AS supplier_name,
          pb.has_photographed,
          pb.has_skipped,
          pb.last_photographed_at,
          COUNT(DISTINCT roi.real_order_id) AS order_count
        FROM products p
        JOIN real_order_items roi ON roi.product_id = p.id
        JOIN real_orders ro       ON ro.id = roi.real_order_id
        LEFT JOIN categories c    ON c.id = p.category_id
        LEFT JOIN producers pr    ON pr.id = p.producer_id
        LEFT JOIN suppliers s     ON s.id = p.latest_supplier_id
        -- /foto raw-capture state (second compartment for photo_state, rule #12).
        -- Aggregated per product across all batches: 'photographed' (raw in
        -- Drive, awaiting offline trim) and/or 'skipped'.
        LEFT JOIN (
            SELECT product_id,
                   MAX(status = 'photographed')                          AS has_photographed,
                   MAX(status = 'skipped')                               AS has_skipped,
                   MAX(CASE WHEN status = 'photographed'
                            THEN photographed_at END)                    AS last_photographed_at
            FROM photo_batch_items
            GROUP BY product_id
        ) pb ON pb.product_id = p.id
        WHERE p.is_active = 1
          AND ro.doc_date >= date('now', ?)
        GROUP BY p.id
        HAVING order_count >= ?
        ORDER BY order_count DESC, p.id ASC
        LIMIT ?
        """,
        (f'-{window_days} days', min_orders, limit),
    ).fetchall()

    products = []
    for r in rows:
        cur_w = r["current_weight"]
        cur_unit = r["current_unit"]
        suggested_weight_kg, src = suggest_weight(
            cur_unit, sales_weights.get(r["id"]), r["name"] or ""
        )
        if suggested_weight_kg is None:
            action = "manual"
        elif cur_w is None or cur_w == 0:
            action = "fill"
        elif round(float(cur_w), 4) != round(suggested_weight_kg, 4):
            action = "mismatch"
        else:
            action = "match"

        products.append({
            "id": r["id"],
            "name": r["name"],
            "name_display": r["name_display"],
            "category_name": r["category_name"],
            "producer_name": r["producer_name"],
            "supplier_name": r["supplier_name"],
            "current_weight_kg": cur_w,
            "current_unit": cur_unit,
            "suggested_weight_kg": suggested_weight_kg,
            "suggested_value": suggested_weight_kg,
            # Keep the product's real sold unit — never reassign unit from a
            # name-parse, or a шт item would be relabelled кг on confirm.
            "suggested_unit": cur_unit,
            "suggested_source": src,
            "weight_action": action,
            "has_image": bool(r["image_path"]),
            "photo_state": photo_state(
                r["image_path"],
                bool(r["has_photographed"]),
                bool(r["has_skipped"]),
            ),
            "photographed_at": r["last_photographed_at"],
            "order_count_60d": r["order_count"],
        })

    conn.close()
    return {
        "ok": True,
        "count": len(products),
        "window_days": window_days,
        "products": products,
    }


@router.post("/confirm-weight")
def confirm_weight(payload: dict = Body(...)):
    """Apply a weight + unit to one or more products from the Product Cleanup tab.

    payload shape:
      {
        "admin_key": "...",
        "items": [
          {"product_id": 1, "weight_kg": 0.5, "unit": "kg"},
          ...
        ]
      }

    Each item writes products.weight (kg) and optionally products.unit. Returns
    per-item ok/error so a batch can partially succeed without rolling back.
    """
    admin_key = payload.get("admin_key", "")
    _check_admin(admin_key)

    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="items required")

    conn = get_db()
    results = []
    updated = 0
    for it in items:
        pid = it.get("product_id")
        weight = it.get("weight_kg")
        unit = it.get("unit")
        if not isinstance(pid, int) or pid <= 0:
            results.append({"product_id": pid, "ok": False, "error": "bad product_id"})
            continue
        if weight is None or not isinstance(weight, (int, float)) or weight <= 0:
            results.append({"product_id": pid, "ok": False, "error": "bad weight_kg"})
            continue

        exists = conn.execute(
            "SELECT 1 FROM products WHERE id = ?", (pid,)
        ).fetchone()
        if not exists:
            results.append({"product_id": pid, "ok": False, "error": "not found"})
            continue

        if unit and isinstance(unit, str) and unit.strip():
            conn.execute(
                "UPDATE products SET weight = ?, unit = ? WHERE id = ?",
                (float(weight), unit.strip(), pid),
            )
        else:
            conn.execute(
                "UPDATE products SET weight = ? WHERE id = ?",
                (float(weight), pid),
            )
        updated += 1
        results.append({"product_id": pid, "ok": True})

    conn.commit()
    conn.close()
    return {"ok": True, "updated": updated, "results": results}


@router.post("/upload-single-image")
async def upload_single_image(
    product_id: int = Form(...),
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Upload one product photo from the cleanup tab. Mirrors /upload-images'
    WebP conversion + sibling-extension cleanup, but for a single file keyed by
    product_id (not the filename).
    """
    _check_admin(admin_key)

    import os
    from pathlib import Path
    from backend.services.convert_to_webp import encode_webp_from_bytes

    data = await file.read()
    if not data:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)

    ext = Path(file.filename or "").suffix.lower()
    if ext not in (".png", ".jpg", ".jpeg", ".webp"):
        return JSONResponse(
            {"ok": False, "error": f"Unsupported extension: {ext}"},
            status_code=400,
        )

    conn = get_db()
    exists = conn.execute("SELECT 1 FROM products WHERE id = ?", (product_id,)).fetchone()
    conn.close()
    if not exists:
        return JSONResponse({"ok": False, "error": "Product not found"}, status_code=404)

    images_dir = Path(os.getenv("IMAGES_DIR", "./images"))
    images_dir.mkdir(parents=True, exist_ok=True)

    dest = images_dir / f"{product_id}.webp"
    existed = any(
        (images_dir / f"{product_id}{e}").exists()
        for e in (".webp", ".png", ".jpg", ".jpeg")
    )
    try:
        if ext == ".webp":
            with open(dest, "wb") as dst:
                dst.write(data)
        else:
            encode_webp_from_bytes(data, dest)
    except Exception as e:
        return JSONResponse({"ok": False, "error": repr(e)[:200]}, status_code=500)

    for stale_ext in (".png", ".jpg", ".jpeg"):
        stale = images_dir / f"{product_id}{stale_ext}"
        if stale.exists():
            stale.unlink(missing_ok=True)

    from backend.services.sync_images import sync
    sync()

    return {
        "ok": True,
        "product_id": product_id,
        "action": "replaced" if existed else "added",
        "path": f"{product_id}.webp",
    }

