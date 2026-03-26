import threading
from fastapi import APIRouter, Query, UploadFile, File, Form
from fastapi.responses import JSONResponse
from typing import Optional, List
from backend.database import get_db
from backend.services.update_prices import apply_price_updates

router = APIRouter(prefix="/api/products", tags=["products"])


def _log_search_bg(telegram_id, query, results_count, category_id, producer_id):
    """Background thread: log search to search_logs table."""
    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO search_logs (telegram_id, query, results_count, category_id, producer_id)
               VALUES (?, ?, ?, ?, ?)""",
            (telegram_id, query.strip().lower(), results_count, category_id, producer_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass  # Never fail the product request because of logging


@router.get("")
def list_products(
    category_id: Optional[int] = None,
    producer_id: Optional[int] = None,
    search: Optional[str] = None,
    telegram_id: Optional[int] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    """List products with filtering by category, producer, and/or search term."""
    conn = get_db()
    offset = (page - 1) * limit
    conditions = ["p.is_active = 1"]
    params = []

    if category_id:
        conditions.append("p.category_id = ?")
        params.append(category_id)

    if producer_id:
        conditions.append("p.producer_id = ?")
        params.append(producer_id)

    if search:
        search_term = search.strip()
        # Search in product name, display name, AND producer name
        conditions.append(
            "(p.name LIKE ? OR p.name_display LIKE ? OR pr.name LIKE ?)"
        )
        params.extend([f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"])

    where = " AND ".join(conditions)

    total = conn.execute(
        f"""SELECT COUNT(*) FROM products p
            JOIN producers pr ON pr.id = p.producer_id
            WHERE {where}""",
        params,
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT p.id, p.name, p.name_display, p.category_id, c.name as category_name,
                   p.producer_id, pr.name as producer_name,
                   p.unit, p.price_usd, p.price_uzs, p.weight, p.image_path
            FROM products p
            JOIN categories c ON c.id = p.category_id
            JOIN producers pr ON pr.id = p.producer_id
            WHERE {where}
            ORDER BY p.name
            LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()
    conn.close()

    # Log search in background (only on first page to avoid duplicates from pagination)
    if search and page == 1:
        threading.Thread(
            target=_log_search_bg,
            args=(telegram_id or 0, search, total, category_id, producer_id),
            daemon=True,
        ).start()

    return {
        "items": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "pages": (total + limit - 1) // limit,
    }


@router.get("/by-ids")
def get_products_by_ids(ids: str = Query(..., description="Comma-separated product IDs")):
    """Batch lookup: return multiple products by their IDs (for cart reconstruction)."""
    try:
        id_list = [int(x.strip()) for x in ids.split(",") if x.strip()]
    except ValueError:
        return {"items": []}
    if not id_list:
        return {"items": []}

    placeholders = ",".join("?" for _ in id_list)
    conn = get_db()
    rows = conn.execute(
        f"""SELECT p.id, p.name, p.name_display, p.unit,
                   p.price_usd, p.price_uzs, p.image_path
            FROM products p
            WHERE p.id IN ({placeholders})""",
        id_list,
    ).fetchall()
    conn.close()
    return {"items": [dict(r) for r in rows]}


@router.post("/update-prices")
async def update_prices(file: UploadFile = File(...), admin_key: str = Form("")):
    """Upload Excel file to update product prices."""
    if admin_key != "rassvet2026":
        return JSONResponse(status_code=403, content={"error": "Invalid admin key"})
    content = await file.read()
    result = apply_price_updates(content)
    return result


@router.get("/{product_id}")
def get_product(product_id: int):
    """Get single product details."""
    conn = get_db()
    row = conn.execute(
        """SELECT p.*, c.name as category_name, pr.name as producer_name
           FROM products p
           JOIN categories c ON c.id = p.category_id
           JOIN producers pr ON pr.id = p.producer_id
           WHERE p.id = ?""",
        (product_id,),
    ).fetchone()
    conn.close()
    if not row:
        return {"error": "Product not found"}, 404
    return dict(row)
