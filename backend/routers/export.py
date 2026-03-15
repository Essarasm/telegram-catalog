from fastapi import APIRouter
from fastapi.responses import Response
from pydantic import BaseModel
from typing import List, Optional
from backend.database import get_db
from backend.services.export_order import generate_pdf, generate_excel
from backend.services.notify_group import send_order_to_group

router = APIRouter(prefix="/api/export", tags=["export"])


class CartItem(BaseModel):
    product_id: int
    quantity: int


class ExportRequest(BaseModel):
    items: List[CartItem]
    format: str = "pdf"  # "pdf" or "xlsx"
    client_name: Optional[str] = ""
    telegram_id: Optional[int] = 0


@router.post("")
def export_order(req: ExportRequest):
    conn = get_db()

    # Look up user phone if telegram_id provided
    client_label = req.client_name or ""
    if req.telegram_id:
        user_row = conn.execute(
            "SELECT phone, first_name, last_name FROM users WHERE telegram_id = ?",
            (req.telegram_id,),
        ).fetchone()
        if user_row and user_row["phone"]:
            name_part = client_label or " ".join(filter(None, [user_row["first_name"], user_row["last_name"]]))
            client_label = f"{name_part} ({user_row['phone']})" if name_part else user_row["phone"]

    order_items = []
    for cart_item in req.items:
        row = conn.execute(
            """SELECT p.name, p.name_display, p.unit, p.price_usd, p.price_uzs, pr.name as producer_name
               FROM products p
               JOIN producers pr ON pr.id = p.producer_id
               WHERE p.id = ?""",
            (cart_item.product_id,),
        ).fetchone()
        if row:
            # Use USD price if available, otherwise UZS
            if row["price_usd"] and row["price_usd"] > 0:
                price = row["price_usd"]
                currency = "USD"
            elif row["price_uzs"] and row["price_uzs"] > 0:
                price = row["price_uzs"]
                currency = "UZS"
            else:
                price = 0
                currency = "USD"

            # Include producer name so managers can identify products
            product_name = row["name_display"] or row["name"]
            full_name = f"{row['producer_name']} — {product_name}" if row["producer_name"] else product_name

            order_items.append({
                "name": full_name,
                "unit": row["unit"],
                "price": price,
                "currency": currency,
                "producer": row["producer_name"],
                "quantity": cart_item.quantity,
            })
    conn.close()

    if not order_items:
        return Response(content="No valid products in order", status_code=400)

    # Always generate Excel for group notification
    excel_data = generate_excel(order_items, client_label)

    # Send order to Telegram sales managers group (non-blocking best-effort)
    try:
        send_order_to_group(order_items, excel_data, client_label)
    except Exception:
        pass  # Don't fail the export if notification fails

    if req.format == "xlsx":
        return Response(
            content=excel_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=buyurtma.xlsx"},
        )
    else:
        data = generate_pdf(order_items, client_label)
        return Response(
            content=data,
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=buyurtma.pdf"},
        )
