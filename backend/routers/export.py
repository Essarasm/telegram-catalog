import uuid
import os
import time
import glob
from fastapi import APIRouter
from fastapi.responses import Response, FileResponse, JSONResponse
from pydantic import BaseModel
from typing import List, Optional
from backend.database import get_db
from backend.services.export_order import generate_pdf, generate_excel
from backend.services.notify_group import send_order_to_group, send_file_to_user

router = APIRouter(prefix="/api/export", tags=["export"])

EXPORT_DIR = os.environ.get("EXPORT_DIR", "/data/exports")
EXPORT_TTL = 1800


def _ensure_dir():
    os.makedirs(EXPORT_DIR, exist_ok=True)


def _cleanup_exports():
    _ensure_dir()
    now = time.time()
    for f in glob.glob(os.path.join(EXPORT_DIR, "*")):
        try:
            if now - os.path.getmtime(f) > EXPORT_TTL:
                os.remove(f)
        except OSError:
            pass


class CartItem(BaseModel):
    product_id: int
    quantity: int


class ExportRequest(BaseModel):
    items: List[CartItem]
    format: str = "pdf"
    client_name: Optional[str] = ""
    telegram_id: Optional[int] = 0


def _build_order_items(req: ExportRequest):
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
        return JSONResponse({"ok": False, "error": "No valid products in order"}, status_code=400)

    # Always generate Excel for group notification
    excel_data = generate_excel(order_items, client_label)

    # Send to sales group (best-effort)
    try:
        send_order_to_group(order_items, excel_data, client_label)
    except Exception:
        pass

    # Generate the file in user's chosen format
    if req.format == "xlsx":
        data = excel_data
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = "buyurtma.xlsx"
    else:
        data = generate_pdf(order_items, client_label)
        media_type = "application/pdf"
        filename = "buyurtma.pdf"

    # Try sending file to user's Telegram DM
    sent_to_telegram = False
    if req.telegram_id:
        from datetime import datetime, timezone, timedelta
        timestamp = datetime.now(timezone(timedelta(hours=5))).strftime("%d_%m_%Y_%H%M")
        user_filename = f"buyurtma_{timestamp}.{req.format if req.format == 'xlsx' else 'pdf'}"

        caption = f"\u2705 <b>Buyurtmangiz tayyor!</b>\n\n\U0001f4e6 {len(order_items)} ta mahsulot"
        sent_to_telegram = send_file_to_user(
            telegram_id=req.telegram_id,
            file_bytes=data,
            filename=user_filename,
            media_type=media_type,
            caption=caption,
        )

    # If bot DM worked, return JSON success (no file body needed)
    if sent_to_telegram:
        return JSONResponse({
            "ok": True,
            "sent_to_telegram": True,
        })

    # Fallback: save to disk for download link (Android browser method)
    _cleanup_exports()
    _ensure_dir()
    token = uuid.uuid4().hex[:12]
    ext = "xlsx" if req.format == "xlsx" else "pdf"
    filepath = os.path.join(EXPORT_DIR, f"{token}.{ext}")
    with open(filepath, "wb") as f:
        f.write(data)

    return Response(
        content=data,
        media_type=media_type,
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "X-Download-Token": token,
            "X-Sent-To-Telegram": "false",
        },
    )


@router.get("/download/{token}")
def download_temp(token: str):
    _cleanup_exports()
    for ext in ["pdf", "xlsx"]:
        filepath = os.path.join(EXPORT_DIR, f"{token}.{ext}")
        if os.path.exists(filepath):
            filename = f"buyurtma.{ext}"
            media_type = "application/pdf" if ext == "pdf" else \
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            return FileResponse(
                filepath,
                media_type=media_type,
                filename=filename,
                headers={"Content-Disposition": f"attachment; filename={filename}"},
            )
    return Response(content="Link expired or not found", status_code=404)
