from fastapi import APIRouter
from fastapi.responses import Response
from pydantic import BaseModel
from typing import List, Optional
from backend.database import get_db
from backend.services.export_order import generate_pdf, generate_excel

router = APIRouter(prefix="/api/export", tags=["export"])


class CartItem(BaseModel):
    product_id: int
    quantity: int


class ExportRequest(BaseModel):
    items: List[CartItem]
    format: str = "pdf"  # "pdf" or "xlsx"
    client_name: Optional[str] = ""


@router.post("")
def export_order(req: ExportRequest):
    conn = get_db()

    order_items = []
    for cart_item in req.items:
        row = conn.execute(
            "SELECT name, unit, price, currency FROM products WHERE id = ?",
            (cart_item.product_id,),
        ).fetchone()
        if row:
            order_items.append({
                "name": row["name"],
                "unit": row["unit"],
                "price": row["price"],
                "currency": row["currency"],
                "quantity": cart_item.quantity,
            })
    conn.close()

    if not order_items:
        return Response(content="No valid products in order", status_code=400)

    if req.format == "xlsx":
        data = generate_excel(order_items, req.client_name or "")
        return Response(
            content=data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=buyurtma.xlsx"},
        )
    else:
        data = generate_pdf(order_items, req.client_name or "")
        return Response(
            content=data,
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=buyurtma.pdf"},
        )
