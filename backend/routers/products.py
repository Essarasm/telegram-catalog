from fastapi import APIRouter, Query
from typing import Optional
from backend.database import get_db

router = APIRouter(prefix="/api/products", tags=["products"])


@router.get("")
def list_products(
    category_id: Optional[int] = None,
    search: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    conn = get_db()
    offset = (page - 1) * limit
    conditions = ["p.is_active = 1"]
    params = []

    if category_id:
        conditions.append("p.category_id = ?")
        params.append(category_id)

    if search:
        conditions.append("p.name LIKE ?")
        params.append(f"%{search}%")

    where = " AND ".join(conditions)

    total = conn.execute(
        f"SELECT COUNT(*) FROM products p WHERE {where}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"""SELECT p.id, p.code, p.name, p.category_id, c.name as category_name,
                   p.unit, p.price, p.currency, p.weight, p.image_path
            FROM products p
            JOIN categories c ON c.id = p.category_id
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
    conn = get_db()
    row = conn.execute(
        """SELECT p.*, c.name as category_name
           FROM products p JOIN categories c ON c.id = p.category_id
           WHERE p.id = ?""",
        (product_id,),
    ).fetchone()
    conn.close()
    if not row:
        return {"error": "Product not found"}, 404
    return dict(row)
