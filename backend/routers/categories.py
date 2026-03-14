from fastapi import APIRouter
from backend.database import get_db

router = APIRouter(prefix="/api/categories", tags=["categories"])


@router.get("")
def list_categories():
    """List all categories with product counts."""
    conn = get_db()
    rows = conn.execute("""
        SELECT c.id, c.name, c.product_count,
               COUNT(DISTINCT p.producer_id) as producer_count
        FROM categories c
        LEFT JOIN products p ON c.id = p.category_id AND p.is_active = 1
        GROUP BY c.id
        ORDER BY c.product_count DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/{category_id}")
def get_category(category_id: int):
    """Get category details."""
    conn = get_db()
    row = conn.execute("SELECT * FROM categories WHERE id = ?", (category_id,)).fetchone()
    conn.close()
    if not row:
        return {"error": "Category not found"}, 404
    return dict(row)


@router.get("/{category_id}/producers")
def list_producers_in_category(category_id: int):
    """List producers that have products in this category, with counts."""
    conn = get_db()
    rows = conn.execute("""
        SELECT pr.id, pr.name, COUNT(p.id) as product_count
        FROM producers pr
        JOIN products p ON pr.id = p.producer_id
        WHERE p.category_id = ? AND p.is_active = 1
        GROUP BY pr.id
        ORDER BY COUNT(p.id) DESC
    """, (category_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]
