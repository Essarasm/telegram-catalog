from fastapi import APIRouter, Depends
from backend.database import get_db

router = APIRouter(prefix="/api/categories", tags=["categories"])


@router.get("")
def list_categories():
    conn = get_db()
    rows = conn.execute("""
        SELECT c.id, c.name, c.sort_order, COUNT(p.id) as product_count
        FROM categories c
        LEFT JOIN products p ON c.id = p.category_id AND p.is_active = 1
        GROUP BY c.id
        ORDER BY c.sort_order
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/{category_id}")
def get_category(category_id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM categories WHERE id = ?", (category_id,)).fetchone()
    conn.close()
    if not row:
        return {"error": "Category not found"}, 404
    return dict(row)
