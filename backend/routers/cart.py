"""Server-side cart — eliminates all client-side storage reliability issues."""
from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
from backend.database import get_db

router = APIRouter(prefix="/api/cart", tags=["cart"])


class CartAction(BaseModel):
    user_id: int
    product_id: int
    quantity: int = 1


class CartClear(BaseModel):
    user_id: int


@router.get("")
def get_cart(user_id: int = Query(...)):
    """Get all cart items for a user, with full product details."""
    conn = get_db()
    rows = conn.execute(
        """SELECT c.product_id, c.quantity,
                  p.name, p.name_display, p.unit,
                  p.price_usd, p.price_uzs, p.weight, p.image_path
           FROM cart_items c
           JOIN products p ON p.id = c.product_id
           WHERE c.user_id = ?
           ORDER BY c.rowid""",
        (user_id,),
    ).fetchall()
    conn.close()

    items = []
    for r in rows:
        has_usd = r["price_usd"] and r["price_usd"] > 0
        items.append({
            "id": r["product_id"],
            "name": r["name_display"] or r["name"],
            "name_display": r["name_display"] or r["name"],
            "unit": r["unit"] or "",
            "price": r["price_usd"] if has_usd else (r["price_uzs"] or 0),
            "currency": "USD" if has_usd else "UZS",
            "weight": float(r["weight"] or 0),
            "quantity": r["quantity"],
        })
    return {"items": items}


@router.post("/set")
def set_cart_item(action: CartAction):
    """Add or update a cart item. quantity=0 removes it."""
    conn = get_db()
    if action.quantity <= 0:
        conn.execute(
            "DELETE FROM cart_items WHERE user_id = ? AND product_id = ?",
            (action.user_id, action.product_id),
        )
    else:
        conn.execute(
            """INSERT INTO cart_items (user_id, product_id, quantity, updated_at)
               VALUES (?, ?, ?, datetime('now'))
               ON CONFLICT(user_id, product_id)
               DO UPDATE SET quantity = excluded.quantity, updated_at = datetime('now')""",
            (action.user_id, action.product_id, action.quantity),
        )
    conn.commit()
    conn.close()
    return {"ok": True}


@router.post("/clear")
def clear_cart(body: CartClear):
    """Remove all items from a user's cart."""
    conn = get_db()
    conn.execute("DELETE FROM cart_items WHERE user_id = ?", (body.user_id,))
    conn.commit()
    conn.close()
    return {"ok": True}
