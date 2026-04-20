"""Send demand-signal alerts to the Inventory Telegram group.

When a hidden (stale/never) product accumulates ≥5 distinct interested clients
within 30 days, notify the Inventory group to consider re-stocking or
redirecting clients to a similar active SKU.
"""
import os
import logging
import httpx

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
INVENTORY_GROUP_CHAT_ID = os.getenv("INVENTORY_GROUP_CHAT_ID", "-5133871411")


def send_interest_alert(product_name: str, lifecycle: str, distinct_users: int,
                        window_days: int = 30, category: str = "",
                        producer: str = "", last_supplied: str = "") -> bool:
    """Post a demand-signal alert to the Inventory group. Returns True on success."""
    if not BOT_TOKEN or not INVENTORY_GROUP_CHAT_ID:
        logger.warning("BOT_TOKEN or INVENTORY_GROUP_CHAT_ID not set, skipping interest alert")
        return False

    lifecycle_label = {
        'stale': "So'nggi yetkazib berish 2025 1-yarim yil (Stale)",
        'never': "So'nggi 16 oyda yetkazib berilmagan (Never)",
    }.get(lifecycle, lifecycle)

    lines = [
        "\U0001f4e6 <b>Mijoz qiziqishi aniqlandi</b>",
        "",
        f"\U0001f6d2 Mahsulot: <b>{product_name}</b>",
    ]
    if producer:
        lines.append(f"\U0001f3ed Ishlab chiqaruvchi: {producer}")
    if category:
        lines.append(f"\U0001f4c2 Kategoriya: {category}")
    lines.append(f"\u231b Holat: {lifecycle_label}")
    if last_supplied:
        lines.append(f"\U0001f4c5 So'nggi yetkazib berish: {last_supplied}")
    lines.append("")
    lines.append(f"\U0001f465 Oxirgi {window_days} kunda qidirgan turli mijozlar: <b>{distinct_users}</b>")
    lines.append("")
    lines.append("\U0001f4a1 Tavsiya: qaytadan kiritishni ko'rib chiqish yoki o'xshash mahsulotga yo'naltirish.")

    text = "\n".join(lines)

    try:
        resp = httpx.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": INVENTORY_GROUP_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
            },
            timeout=10,
        )
        j = resp.json()
        if j.get("ok"):
            logger.info(f"Sent interest alert for: {product_name}")
            return True
        logger.error(f"Interest alert failed: {j}")
        return False
    except Exception as e:
        logger.exception(f"Interest alert exception: {e}")
        return False
