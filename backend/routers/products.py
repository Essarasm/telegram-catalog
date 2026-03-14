from fastapi import APIRouter, Query
from typing import Optional
from backend.database import get_db

router = APIRouter(prefix="/api/products", tags=["products"])


@router.get("")
def list_products(
    category_id: Optional[int] = None,
    producer_id: Optional[int] = None,
    search: Optional[str] = None,
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
        conditions.append("(p.name LIKE ? OR p.name_display LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where = " AND ".join(conditions)

    total = conn.execute(
        f"SELECT COUNT(*) FROM products p WHERE {where}", params
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

    return {
        "items": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "pages": (total + limit - 1) // limit,
    }


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
