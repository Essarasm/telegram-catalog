"""Send order notifications to Telegram — group chat and user DM."""
import os
import io
import logging
import httpx
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
from backend.services.group_config import ORDER_GROUP_CHAT_ID, ONEC_HANDLERS


def _agent_label_for_button(first_name: Optional[str], vehicle: Optional[str],
                            vehicle_capacity_tons: Optional[float]) -> str:
    """Replica of `bot.handlers.order_dispatch._agent_label`. Kept here so
    feedback-driven editMessageText can rebuild the assigned-badge without
    importing from bot/ (backend → bot import would invert the dep)."""
    name = (first_name or "Agent").strip() or "Agent"
    veh = (vehicle or "").strip()
    cap_text = f"{vehicle_capacity_tons:.1f}t" if vehicle_capacity_tons else ""
    if veh and cap_text:
        descriptor = f"{veh}·{cap_text}"
    else:
        descriptor = veh or cap_text
    label = f"{name} ({descriptor})" if descriptor else name
    return label[:64]


def build_dispatch_markup(order_id: int, delivery_status: str,
                          agent_row: Optional[Dict] = None,
                          allow_cancel: bool = True) -> Optional[Dict]:
    """Build the inline-keyboard payload for an order's Sotuv message based on
    its current dispatch state. Used at first send AND on editMessageText
    (Telegram drops the keyboard otherwise).

    Returns the JSON-shaped reply_markup dict (httpx will serialize it),
    or None if order_id is missing.

    `allow_cancel` appends a second-row "✖ Bekor qilish" button (callback
    `ord:cancel:<id>`). The cancel handler re-checks `confirmed_orders` at
    tap time and refuses if the order is already in 1C, so leaving this
    True by default is safe even for confirmed orders.
    """
    if not order_id:
        return None
    status = (delivery_status or "open").lower()
    rows: list = []
    if status == "assigned" and agent_row:
        label = _agent_label_for_button(
            agent_row.get("first_name"),
            agent_row.get("vehicle"),
            agent_row.get("vehicle_capacity_tons"),
        )
        rows.append([
            {"text": f"✅ Biriktirildi: {label}", "callback_data": "disp:noop"}
        ])
    else:
        # 'open' (and any other / unknown state) shows the pick button so the
        # dispatcher can still act.
        rows.append([
            {"text": "🚚 Agent ga biriktirish",
             "callback_data": f"disp:pick:{order_id}"}
        ])
    if allow_cancel:
        rows.append([
            {"text": "✖ Bekor qilish",
             "callback_data": f"ord:cancel:{order_id}"}
        ])
    return {"inline_keyboard": rows}


def send_order_to_group(items: List[Dict], excel_bytes: bytes, client_name: str = "", delivery_type: str = "delivery", client_name_1c: str = "", location_text: str = "", maps_link: str = "", order_id: int = 0, agent_name: str = "", parent_order_id: int = None):
    """Send order summary + Excel file to the sales managers' Telegram group.

    Returns a dict {ok, text_message_id, doc_message_id, text_message_text}
    so the caller can persist the message ids + frozen text on the order
    row and later match manager reply-to messages back to this order.
    """
    if not BOT_TOKEN or not ORDER_GROUP_CHAT_ID:
        logger.warning("ORDER_GROUP_CHAT_ID or BOT_TOKEN not set, skipping group notification")
        return {"ok": False}

    usd_items = [it for it in items if it.get("currency", "USD") == "USD"]
    uzs_items = [it for it in items if it.get("currency", "USD") == "UZS"]
    usd_total = sum(it["price"] * it["quantity"] for it in usd_items)
    uzs_total = sum(it["price"] * it["quantity"] for it in uzs_items)
    total_quantity = sum(it["quantity"] for it in items)
    unique_products = len(items)

    if parent_order_id:
        lines = [f"\U0001f4e6 <b>Qo'shimcha buyurtma #{order_id}</b> (asl: #{parent_order_id})", ""]
    else:
        lines = ["\U0001f4cb <b>Yangi buyurtma!</b>", ""]
        if order_id:
            lines[0] = f"\U0001f4cb <b>Yangi buyurtma #{order_id}</b>"
    if client_name_1c:
        lines.append(f"\U0001f464 Mijoz (1C): <b>{client_name_1c}</b>")
    else:
        lines.append(f"\U0001f464 Mijoz (1C): <i>1C nomi topilmadi</i>")
    if agent_name:
        lines.append(f"\U0001f4bc Agent: <b>{agent_name}</b>")
    if client_name:
        lines.append(f"\U0001f4f1 Telegram: {client_name}")
    lines.append(f"\U0001f4e6 Mahsulotlar: {unique_products} ta nomi, {total_quantity} ta dona")
    lines.append("")
    if usd_total > 0:
        lines.append(f"\U0001f4b5 Jami (USD): <b>${usd_total:,.2f}</b>")
    if uzs_total > 0:
        lines.append(f"\U0001f4b4 Jami (UZS): <b>{uzs_total:,.0f} so'm</b>")
    lines.append("")
    # Delivery type + location
    if delivery_type == "pickup":
        lines.append("\U0001f4e6 Olib ketish")
    else:
        lines.append("\U0001f69b Yetkazib berish")
        if location_text:
            lines.append(f"\U0001f4cd Manzil: {location_text}")
        if maps_link:
            lines.append(f"\U0001f5fa <a href=\"{maps_link}\">Xaritada ko'rish</a>")
    lines.append("")
    lines.append("\U0001f4ce Excel fayl ilova qilingan")

    # Ping the 1C handlers (Alisher + Ibrat by default) so they remember to
    # enter the order into 1C and reply-with-Excel back to this message.
    # `tg://user?id=` mentions fire a real notification even without public
    # usernames; the user must be in the group.
    if ONEC_HANDLERS:
        mentions = ", ".join(
            f"<a href=\"tg://user?id={tg_id}\">{name}</a>"
            for tg_id, name in ONEC_HANDLERS
        )
        lines.append("")
        lines.append(f"\U0001f440 {mentions} — iltimos 1C-ga kirgazib, "
                     f"ushbu xabarga javob bering.")

    message_text = "\n".join(lines)

    try:
        api_url = f"https://api.telegram.org/bot{BOT_TOKEN}"

        # Inline keyboard with dispatch button — admin taps to assign a
        # delivery agent. Handler in bot/handlers/order_dispatch.py picks
        # up the `disp:pick:<order_id>` callback. Skip the button when no
        # order_id (shouldn't happen post-Block-A but defensive).
        send_payload = {
            "chat_id": ORDER_GROUP_CHAT_ID,
            "text": message_text,
            "parse_mode": "HTML",
        }
        markup = build_dispatch_markup(order_id, "open")
        if markup:
            send_payload["reply_markup"] = markup
        text_resp = httpx.post(
            f"{api_url}/sendMessage",
            json=send_payload,
            timeout=10,
        )
        text_mid = None
        try:
            j = text_resp.json()
            if j.get("ok"):
                text_mid = (j.get("result") or {}).get("message_id")
        except Exception:
            pass

        from datetime import datetime, timezone, timedelta
        timestamp = datetime.now(timezone(timedelta(hours=5))).strftime("%d_%m_%Y_%H%M")
        filename = f"buyurtma_{order_id or client_name.replace(' ', '_') or 'noname'}_{timestamp}.xlsx"

        doc_resp = httpx.post(
            f"{api_url}/sendDocument",
            data={"chat_id": ORDER_GROUP_CHAT_ID,
                  "caption": f"#Buyurtma_{order_id}" if order_id else ""},
            files={"document": (filename, io.BytesIO(excel_bytes),
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            timeout=15,
        )
        doc_mid = None
        try:
            j = doc_resp.json()
            if j.get("ok"):
                doc_mid = (j.get("result") or {}).get("message_id")
        except Exception:
            pass

        logger.info(f"Order notification sent to group {ORDER_GROUP_CHAT_ID} "
                    f"(text_mid={text_mid}, doc_mid={doc_mid})")
        return {
            "ok": True,
            "text_message_id": text_mid,
            "doc_message_id": doc_mid,
            "text_message_text": message_text,
        }

    except Exception as e:
        logger.error(f"Failed to send order to group: {e}")
        return {"ok": False}


def send_file_to_user(telegram_id: int, file_bytes: bytes, filename: str,
                      media_type: str, caption: str = ""):
    """Send a file to a user's Telegram DM via the bot.

    Returns True if sent successfully, False otherwise.
    """
    if not BOT_TOKEN or not telegram_id:
        logger.warning("BOT_TOKEN or telegram_id missing, cannot send to user")
        return False

    api_url = f"https://api.telegram.org/bot{BOT_TOKEN}"

    try:
        resp = httpx.post(
            f"{api_url}/sendDocument",
            data={
                "chat_id": telegram_id,
                "caption": caption,
                "parse_mode": "HTML",
            },
            files={"document": (filename, io.BytesIO(file_bytes), media_type)},
            timeout=20,
        )
        result = resp.json()

        if result.get("ok"):
            logger.info(f"File '{filename}' sent to user {telegram_id}")
            return True
        else:
            error_desc = result.get("description", "unknown error")
            logger.error(f"Failed to send file to user {telegram_id}: {error_desc}")
            return False

    except Exception as e:
        logger.error(f"Exception sending file to user {telegram_id}: {e}")
        return False
