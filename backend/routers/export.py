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
    delivery_type: Optional[str] = "delivery"  # 'delivery' or 'pickup'
    location_district_id: Optional[int] = None  # Session M: delivery location
    location_moljal_id: Optional[int] = None
    parent_order_id: Optional[int] = None  # supplementary order link


def _build_order_items(req: ExportRequest):
    conn = get_db()

    client_label = req.client_name or ""
    client_name_1c = ""
    agent_name = ""
    if req.telegram_id:
        user_row = conn.execute(
            "SELECT phone, first_name, last_name, client_id, is_agent FROM users WHERE telegram_id = ?",
            (req.telegram_id,),
        ).fetchone()
        if user_row and user_row["phone"]:
            name_part = client_label or " ".join(filter(None, [user_row["first_name"], user_row["last_name"]]))
            client_label = f"{name_part} ({user_row['phone']})" if name_part else user_row["phone"]

        # If the user is an agent using /testclient, resolve the CLIENT's
        # 1C name from users.client_id (the /testclient link), and record
        # the agent's own Telegram name separately so the sales group sees
        # both "who placed" and "for whom".
        if user_row and user_row["is_agent"] and user_row["client_id"]:
            agent_name = " ".join(filter(None, [user_row["first_name"], user_row["last_name"]])) or ""
            ac_row = conn.execute(
                "SELECT client_id_1c FROM allowed_clients WHERE id = ? AND client_id_1c != '' LIMIT 1",
                (user_row["client_id"],),
            ).fetchone()
            if ac_row:
                client_name_1c = ac_row["client_id_1c"]

        # Fallback: try matched_telegram_id, then phone match (for regular clients).
        # Prefer client_id_1c (Cyrillic 1C name) over name (Latin app name).
        if not client_name_1c:
            ac_row = conn.execute(
                "SELECT client_id_1c, name FROM allowed_clients "
                "WHERE matched_telegram_id = ? AND (client_id_1c != '' OR name != '') LIMIT 1",
                (req.telegram_id,),
            ).fetchone()
            if not ac_row and user_row and user_row["phone"]:
                import re
                digits = re.sub(r"\D", "", user_row["phone"] or "")
                phone_norm = digits[-9:] if len(digits) >= 9 else digits
                ac_row = conn.execute(
                    "SELECT client_id_1c, name FROM allowed_clients "
                    "WHERE phone_normalized = ? AND (client_id_1c != '' OR name != '') LIMIT 1",
                    (phone_norm,),
                ).fetchone()
            if ac_row:
                client_name_1c = ac_row["client_id_1c"] or ac_row["name"] or ""

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

            # Order documents intentionally use the original 1C Cyrillic name
            # (products.name) so the warehouse / sales team can match orders
            # against raw 1C data while standardization is still in progress.
            # The cleaned Latin name (products.name_display) stays in the app UI.
            # See Session R — Product Catalog Cleanup.
            product_name = row["name"] or row["name_display"]
            full_name = f"{row['producer_name']} — {product_name}" if row["producer_name"] else product_name

            order_items.append({
                "product_id": cart_item.product_id,
                "name": full_name,
                "unit": row["unit"],
                "price": price,
                "currency": currency,
                "producer": row["producer_name"],
                "quantity": cart_item.quantity,
            })
    conn.close()
    return order_items, client_label, client_name_1c


def _save_order_to_db(req: ExportRequest, order_items, client_label):
    """Persist the order to the orders + order_items tables.

    Also logs demand signals for any out-of-stock products in the order.
    """
    conn = get_db()
    try:
        usd_total = sum(it["price"] * it["quantity"] for it in order_items if it.get("currency", "USD") == "USD")
        uzs_total = sum(it["price"] * it["quantity"] for it in order_items if it.get("currency", "USD") == "UZS")

        # Extract phone from client_label if present (format: "Name (phone)")
        client_phone = ""
        if "(" in client_label and client_label.endswith(")"):
            client_phone = client_label[client_label.rfind("(") + 1:-1]

        # Validate delivery_type
        delivery_type = req.delivery_type if req.delivery_type in ('delivery', 'pickup') else 'delivery'

        # Fetch GPS coordinates + client_id from user profile
        user_lat, user_lng, user_addr = None, None, None
        user_client_id = None
        if req.telegram_id:
            geo_row = conn.execute(
                "SELECT latitude, longitude, location_address, client_id FROM users WHERE telegram_id = ?",
                (req.telegram_id,),
            ).fetchone()
            if geo_row:
                if geo_row["latitude"] and geo_row["longitude"]:
                    user_lat = geo_row["latitude"]
                    user_lng = geo_row["longitude"]
                    user_addr = geo_row["location_address"] or ""
                user_client_id = geo_row["client_id"]

        cursor = conn.execute(
            """INSERT INTO orders (telegram_id, client_id, placed_by_telegram_id,
                client_name, client_phone, total_usd, total_uzs, item_count, status,
                delivery_type, location_district_id, location_moljal_id,
                latitude, longitude, location_address, parent_order_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'submitted', ?, ?, ?, ?, ?, ?, ?)""",
            (req.telegram_id or 0, user_client_id, req.telegram_id or 0,
             client_label, client_phone, usd_total, uzs_total, len(order_items), delivery_type,
             req.location_district_id, req.location_moljal_id, user_lat, user_lng, user_addr,
             req.parent_order_id),
        )
        order_id = cursor.lastrowid

        # Look up stock status for all products in this order (one query)
        product_ids = [it["product_id"] for it in order_items if it.get("product_id")]
        stock_map = {}
        if product_ids:
            placeholders = ",".join("?" * len(product_ids))
            rows = conn.execute(
                f"SELECT id, stock_status FROM products WHERE id IN ({placeholders})",
                product_ids,
            ).fetchall()
            stock_map = {r["id"]: r["stock_status"] for r in rows}

        for it in order_items:
            cursor = conn.execute(
                """INSERT INTO order_items (order_id, product_id, product_name, producer_name, quantity, unit, price, currency)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (order_id, it.get("product_id"), it["name"], it.get("producer", ""),
                 it["quantity"], it.get("unit", ""), it["price"], it.get("currency", "USD")),
            )
            order_item_id = cursor.lastrowid

            # Log demand signal if product is out of stock
            pid = it.get("product_id")
            if pid and stock_map.get(pid) == "out_of_stock":
                conn.execute(
                    """INSERT INTO demand_signals
                       (order_id, order_item_id, product_id, telegram_id, quantity, stock_status_at_order)
                       VALUES (?, ?, ?, ?, ?, 'out_of_stock')""",
                    (order_id, order_item_id, pid, req.telegram_id or 0, it["quantity"]),
                )

        conn.commit()
        return order_id
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Failed to save order: {e}")
        return None
    finally:
        conn.close()


@router.post("")
def export_order(req: ExportRequest):
    order_items, client_label, client_name_1c = _build_order_items(req)

    if not order_items:
        return JSONResponse({"ok": False, "error": "No valid products in order"}, status_code=400)

    # Save order to database for history
    order_id = _save_order_to_db(req, order_items, client_label)

    # Build the label that appears on PDF/Excel header
    # When an agent places for a client, show both names
    doc_label = client_label
    if agent_name and client_name_1c:
        doc_label = f"Mijoz (1C): {client_name_1c} | Agent: {agent_name}"
    elif client_name_1c:
        doc_label = f"{client_name_1c} ({client_label})"

    # Always generate Excel for group notification
    excel_data = generate_excel(order_items, doc_label)

    # Generate the file in user's chosen format
    if req.format == "xlsx":
        data = excel_data
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = "buyurtma.xlsx"
    else:
        data = generate_pdf(order_items, doc_label)
        media_type = "application/pdf"
        filename = "buyurtma.pdf"

    # Resolve location names for notification
    location_text = ""
    if req.location_district_id:
        try:
            conn = get_db()
            d_row = conn.execute("SELECT name FROM locations WHERE id = ?", (req.location_district_id,)).fetchone()
            if d_row:
                location_text = d_row["name"]
            if req.location_moljal_id:
                m_row = conn.execute("SELECT name FROM locations WHERE id = ?", (req.location_moljal_id,)).fetchone()
                if m_row:
                    location_text += f" → {m_row['name']}"
            conn.close()
        except Exception:
            pass

    # GPS address as fallback/addition to location_text
    maps_link = ""
    user_lat = user_lng = user_addr = None
    if req.telegram_id:
        try:
            conn = get_db()
            geo_row = conn.execute(
                "SELECT latitude, longitude, location_address FROM users WHERE telegram_id = ?",
                (req.telegram_id,),
            ).fetchone()
            conn.close()
            if geo_row and geo_row["latitude"] and geo_row["longitude"]:
                user_lat = geo_row["latitude"]
                user_lng = geo_row["longitude"]
                user_addr = geo_row["location_address"] or ""
        except Exception:
            pass

    if user_lat and user_lng:
        if not location_text and user_addr:
            location_text = user_addr
        maps_link = f"https://maps.google.com/?q={user_lat},{user_lng}"

    # Send Excel to sales group (always, regardless of user format choice).
    # Capture message ids so we can link the manager's eventual reply-with-Excel
    # (the confirmed order from 1C) back to this wishlist order.
    import logging
    logger = logging.getLogger(__name__)
    try:
        delivery_type = req.delivery_type if req.delivery_type in ('delivery', 'pickup') else 'delivery'
        group_result = send_order_to_group(
            order_items, excel_data, client_label,
            delivery_type=delivery_type, client_name_1c=client_name_1c,
            location_text=location_text, maps_link=maps_link,
            order_id=order_id, agent_name=agent_name,
            parent_order_id=req.parent_order_id,
        )
        if not group_result or not group_result.get("ok"):
            logger.error(f"send_order_to_group failed: {group_result}")
        else:
            text_mid = group_result.get("text_message_id")
            doc_mid = group_result.get("doc_message_id")
            if text_mid or doc_mid:
                conn_ids = get_db()
                try:
                    conn_ids.execute(
                        "UPDATE orders SET sales_group_message_id = ?, "
                        "sales_group_doc_message_id = ? WHERE id = ?",
                        (text_mid, doc_mid, order_id),
                    )
                    conn_ids.commit()
                finally:
                    conn_ids.close()
    except Exception as e:
        logger.error(f"send_order_to_group exception: {e}")

    # Build detailed caption for user DM (same style as group message)
    from datetime import datetime, timezone, timedelta
    usd_items = [it for it in order_items if it.get("currency", "USD") == "USD"]
    uzs_items = [it for it in order_items if it.get("currency", "USD") == "UZS"]
    usd_total = sum(it["price"] * it["quantity"] for it in usd_items)
    uzs_total = sum(it["price"] * it["quantity"] for it in uzs_items)
    total_quantity = sum(it["quantity"] for it in order_items)

    caption_lines = ["\u2705 <b>Buyurtmangiz tayyor!</b>", ""]
    if client_label:
        caption_lines.append(f"\U0001f464 Mijoz: <b>{client_label}</b>")
    caption_lines.append(f"\U0001f4e6 Mahsulotlar: {len(order_items)} ta nomi, {total_quantity} ta dona")
    # Delivery type in user DM
    dt = req.delivery_type if req.delivery_type in ('delivery', 'pickup') else 'delivery'
    if dt == "pickup":
        caption_lines.append("\U0001f4e6 Olib ketish")
    else:
        caption_lines.append("\U0001f69b Yetkazib berish")
        if location_text:
            caption_lines.append(f"\U0001f4cd Manzil: {location_text}")
        if maps_link:
            caption_lines.append(f"\U0001f5fa <a href=\"{maps_link}\">Xaritada ko'rish</a>")
    caption_lines.append("")
    if usd_total > 0:
        caption_lines.append(f"\U0001f4b5 Jami (USD): <b>${usd_total:,.2f}</b>")
    if uzs_total > 0:
        caption_lines.append(f"\U0001f4b4 Jami (UZS): <b>{uzs_total:,.0f} so'm</b>")
    caption = "\n".join(caption_lines)

    # Send file to user's Telegram DM
    sent_to_telegram = False
    if req.telegram_id:
        timestamp = datetime.now(timezone(timedelta(hours=5))).strftime("%d_%m_%Y_%H%M")
        user_filename = f"buyurtma_{timestamp}.{req.format if req.format == 'xlsx' else 'pdf'}"

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
            "order_id": order_id,
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
