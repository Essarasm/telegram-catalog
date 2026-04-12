"""Send order notifications to Telegram — group chat and user DM."""
import os
import io
import logging
import httpx
from typing import List, Dict

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ORDER_GROUP_CHAT_ID = os.getenv("ORDER_GROUP_CHAT_ID", "-1003740010463")


def send_order_to_group(items: List[Dict], excel_bytes: bytes, client_name: str = "", delivery_type: str = "delivery", client_name_1c: str = ""):
    """Send order summary + Excel file to the sales managers' Telegram group."""
    if not BOT_TOKEN or not ORDER_GROUP_CHAT_ID:
        logger.warning("ORDER_GROUP_CHAT_ID or BOT_TOKEN not set, skipping group notification")
        return False

    usd_items = [it for it in items if it.get("currency", "USD") == "USD"]
    uzs_items = [it for it in items if it.get("currency", "USD") == "UZS"]
    usd_total = sum(it["price"] * it["quantity"] for it in usd_items)
    uzs_total = sum(it["price"] * it["quantity"] for it in uzs_items)
    total_quantity = sum(it["quantity"] for it in items)
    unique_products = len(items)

    lines = ["\U0001f4cb <b>Yangi buyurtma!</b>", ""]
    if client_name_1c:
        lines.append(f"\U0001f464 Mijoz (1C): <b>{client_name_1c}</b>")
    else:
        lines.append(f"\U0001f464 Mijoz (1C): <i>1C nomi topilmadi</i>")
    if client_name:
        lines.append(f"\U0001f4f1 Telegram: {client_name}")
    lines.append(f"\U0001f4e6 Mahsulotlar: {unique_products} ta nomi, {total_quantity} ta dona")
    lines.append("")
    if usd_total > 0:
        lines.append(f"\U0001f4b5 Jami (USD): <b>${usd_total:,.2f}</b>")
    if uzs_total > 0:
        lines.append(f"\U0001f4b4 Jami (UZS): <b>{uzs_total:,.0f} so'm</b>")
    lines.append("")
    # Delivery type
    if delivery_type == "pickup":
        lines.append("\U0001f4e6 Olib ketish")
    else:
        lines.append("\U0001f69b Yetkazib berish")
    lines.append("")
    lines.append("\U0001f4ce Excel fayl ilova qilingan")

    message_text = "\n".join(lines)

    try:
        api_url = f"https://api.telegram.org/bot{BOT_TOKEN}"

        httpx.post(
            f"{api_url}/sendMessage",
            json={"chat_id": ORDER_GROUP_CHAT_ID, "text": message_text, "parse_mode": "HTML"},
            timeout=10,
        )

        from datetime import datetime, timezone, timedelta
        timestamp = datetime.now(timezone(timedelta(hours=5))).strftime("%d_%m_%Y_%H%M")
        filename = f"buyurtma_{client_name.replace(' ', '_') or 'noname'}_{timestamp}.xlsx"

        httpx.post(
            f"{api_url}/sendDocument",
            data={"chat_id": ORDER_GROUP_CHAT_ID},
            files={"document": (filename, io.BytesIO(excel_bytes),
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            timeout=15,
        )

        logger.info(f"Order notification sent to group {ORDER_GROUP_CHAT_ID}")
        return True

    except Exception as e:
        logger.error(f"Failed to send order to group: {e}")
        return False


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
