"""Send order notifications to a Telegram group chat."""
import os
import io
import logging
import httpx
from typing import List, Dict

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ORDER_GROUP_CHAT_ID = os.getenv("ORDER_GROUP_CHAT_ID", "")


def send_order_to_group(items: List[Dict], excel_bytes: bytes, client_name: str = ""):
    """Send order summary + Excel file to the sales managers' Telegram group."""
    if not BOT_TOKEN or not ORDER_GROUP_CHAT_ID:
        logger.warning("ORDER_GROUP_CHAT_ID or BOT_TOKEN not set, skipping group notification")
        return False

    # Build a summary message
    usd_items = [it for it in items if it.get("currency", "USD") == "USD"]
    uzs_items = [it for it in items if it.get("currency", "USD") == "UZS"]

    usd_total = sum(it["price"] * it["quantity"] for it in usd_items)
    uzs_total = sum(it["price"] * it["quantity"] for it in uzs_items)
    total_products = sum(it["quantity"] for it in items)

    lines = ["📋 <b>Yangi buyurtma!</b>", ""]
    if client_name:
        lines.append(f"👤 Mijoz: <b>{client_name}</b>")
    lines.append(f"📦 Mahsulotlar soni: {total_products}")
    lines.append("")

    if usd_total > 0:
        lines.append(f"💵 Jami (USD): <b>${usd_total:,.2f}</b>")
    if uzs_total > 0:
        lines.append(f"💴 Jami (UZS): <b>{uzs_total:,.0f} so'm</b>")

    lines.append("")
    lines.append("📎 Batafsil ma'lumot Excel faylda")

    message_text = "\n".join(lines)

    try:
        api_url = f"https://api.telegram.org/bot{BOT_TOKEN}"

        # Send the summary message
        httpx.post(
            f"{api_url}/sendMessage",
            json={
                "chat_id": ORDER_GROUP_CHAT_ID,
                "text": message_text,
                "parse_mode": "HTML",
            },
            timeout=10,
        )

        # Send the Excel file
        from datetime import datetime
        timestamp = datetime.now().strftime("%d_%m_%Y_%H%M")
        filename = f"buyurtma_{client_name.replace(' ', '_') or 'noname'}_{timestamp}.xlsx"

        httpx.post(
            f"{api_url}/sendDocument",
            data={"chat_id": ORDER_GROUP_CHAT_ID},
            files={"document": (filename, io.BytesIO(excel_bytes), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            timeout=15,
        )

        logger.info(f"Order notification sent to group {ORDER_GROUP_CHAT_ID}")
        return True

    except Exception as e:
        logger.error(f"Failed to send order to group: {e}")
        return False
