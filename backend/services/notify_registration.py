"""Send new user registration notifications to the manager (Alisher)."""
import os
import logging
import httpx

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
# Alisher's Telegram user ID — will be resolved on first use
MANAGER_USERNAME = "axmatov0902"
MANAGER_CHAT_ID = os.getenv("MANAGER_CHAT_ID", "")


def send_registration_notification(
    telegram_id: int,
    phone: str,
    first_name: str = "",
    last_name: str = "",
    username: str = "",
    latitude: float = None,
    longitude: float = None,
    is_approved: bool = False,
    client_name: str = None,
):
    """Notify manager about a new registration."""
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN not set, skipping registration notification")
        return

    # Send to the sales group instead (guaranteed to work, no need for personal chat ID)
    chat_id = MANAGER_CHAT_ID or os.getenv("ORDER_GROUP_CHAT_ID", "-1003740010463")
    if not chat_id:
        return

    full_name = " ".join(filter(None, [first_name, last_name])).strip() or "—"
    phone_display = phone or "—"
    username_display = f"@{username}" if username else "—"

    if is_approved:
        status = "✅ Avtomatik tasdiqlangan"
        status_detail = f"Mijoz: {client_name}" if client_name else ""
    else:
        status = "⏳ Tasdiqlanmagan (narxlar yashirin)"
        status_detail = "Tasdiqlash uchun quyidagi linkni bosing"

    lines = [
        "👤 <b>Yangi foydalanuvchi ro'yxatdan o'tdi!</b>",
        "",
        f"📛 Ism: <b>{full_name}</b>",
        f"📱 Telefon: <b>{phone_display}</b>",
        f"💬 Username: {username_display}",
        f"🆔 Telegram ID: <code>{telegram_id}</code>",
    ]

    if latitude and longitude:
        lines.append(f"📍 Joylashuv: <a href=\"https://maps.google.com/?q={latitude},{longitude}\">Xaritada ko'rish</a>")
    else:
        lines.append("📍 Joylashuv: yuborilmagan")

    lines.append("")
    lines.append(f"📋 Holat: {status}")
    if status_detail:
        lines.append(status_detail)

    # For unapproved users, add a direct approve link
    if not is_approved:
        approve_url = f"https://telegram-catalog-production.up.railway.app/api/users/approve?telegram_id={telegram_id}&admin_key=rassvet2026"
        lines.append("")
        lines.append(f"✅ <a href=\"{approve_url}\">Tasdiqlash (approve)</a>")

    message_text = "\n".join(lines)

    try:
        api_url = f"https://api.telegram.org/bot{BOT_TOKEN}"
        resp = httpx.post(
            f"{api_url}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": message_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        logger.info(f"Registration notification sent: {resp.status_code}")
        return True
    except Exception as e:
        logger.error(f"Failed to send registration notification: {e}")
        return False
