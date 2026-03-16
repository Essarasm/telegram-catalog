import uuid
import time
import threading
from fastapi import APIRouter
from fastapi.responses import Response
from pydantic import BaseModel
from typing import List, Optional
from backend.database import get_db
from backend.services.export_order import generate_pdf, generate_excel
from backend.services.notify_group import send_order_to_group

router = APIRouter(prefix="/api/export", tags=["export"])

# Temporary file store for Android download links (auto-expires after 5 min)
_temp_files = {}  # token -> {data, media_type, filename, created}
_TEMP_TTL = 300   # 5 minutes


def _cleanup_temp():
    """Remove expired temp files."""
    now = time.time()
    expired = [k for k, v in _temp_files.items() if now - v["created"] > _TEMP_TTL]
    for k in expired:
        del _temp_files[k]


class CartItem(BaseModel):
    product_id: int
    quantity: int


class ExportRequest(BaseModel):
    items: List[CartItem]
    format: str = "pdf"  # "pdf" or "xlsx"
    client_name: Optional[str] = ""
    telegram_id: Optional[int] = 0


def _build_order_items(req: ExportRequest):
    """Look up product details and build order items list."""
    conn = get_db()

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
            if row["price_usd"] and row["price_usd"] > 0:
                price, currency = row["price_usd"], "USD"
            elif row["price_uzs"] and row["price_uzs"] > 0:
                price, currency = row["price_uzs"], "UZS"
            else:
                price, currency = 0, "USD"

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
    return order_items, client_label


@router.post("")
def export_order(req: ExportRequest):
    order_items, client_label = _build_order_items(req)

    if not order_items:
        return Response(content="No valid products in order", status_code=400)

    # Always generate Excel for group notification
    excel_data = generate_excel(order_items, client_label)

    # Send order to Telegram sales managers group (non-blocking best-effort)
    try:
        send_order_to_group(order_items, excel_data, client_label)
    except Exception:
        pass

    if req.format == "xlsx":
        data = excel_data
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = "buyurtma.xlsx"
    else:
        data = generate_pdf(order_items, client_label)
        media_type = "application/pdf"
        filename = "buyurtma.pdf"

    # Store a temp copy for Android download via GET link
    _cleanup_temp()
    token = uuid.uuid4().hex[:12]
    _temp_files[token] = {
        "data": data,
        "media_type": media_type,
        "filename": filename,
        "created": time.time(),
    }

    return Response(
        content=data,
        media_type=media_type,
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "X-Download-Token": token,
        },
    )


@router.get("/download/{token}")
def download_temp(token: str):
    """Serve a temporary file by token (used for Android Telegram WebView)."""
    _cleanup_temp()
    entry = _temp_files.pop(token, None)
    if not entry:
        return Response(content="Link expired or not found", status_code=404)
    return Response(
        content=entry["data"],
        media_type=entry["media_type"],
        headers={"Content-Disposition": f"inline; filename={entry['filename']}"},
    )
