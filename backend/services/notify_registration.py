"""Send new user registration notifications to the Sotuv bo'limi (Sales) group."""
import os
import logging
import sqlite3
import httpx

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_GROUP_CHAT_ID = os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051")
# Legacy fallbacks (kept for backward compatibility)
MANAGER_CHAT_ID = os.getenv("MANAGER_CHAT_ID", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")


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
    client_id_1c: str = None,
):
    """Notify Sotuv bo'limi about a new registration.

    For matched users: shows 1C client name prominently.
    For unmatched users: saves to unmatched_registrations for later review
    and includes a hint to reply with the 1C name.

    Returns the Telegram message_id of the sent notification (or None).
    """
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN not set, skipping registration notification")
        return None

    chat_id = MANAGER_CHAT_ID or ADMIN_GROUP_CHAT_ID
    if not chat_id:
        return None

    full_name = " ".join(filter(None, [first_name, last_name])).strip() or "—"
    phone_display = phone or "—"
    username_display = f"@{username}" if username else "—"

    lines = [
        "\U0001f464 <b>Yangi foydalanuvchi ro\u2019yxatdan o\u2019tdi!</b>",
        "",
    ]

    # 1C client name — always shown
    if client_name:
        lines.append(f"\U0001f3e2 Mijoz (1C): <b>{client_name}</b>")
        if client_id_1c and client_id_1c != client_name:
            lines.append(f"   \U0001f4cb 1C ID: {client_id_1c}")
    else:
        lines.append("\U0001f3e2 Mijoz (1C): <i>1C nomi topilmadi</i>")

    lines.append("")
    lines.append(f"\U0001f4f1 Telegram: <b>{full_name}</b>")
    lines.append(f"\U0001f4de Telefon: <b>{phone_display}</b>")
    lines.append(f"\U0001f4ac Username: {username_display}")
    lines.append(f"\U0001f194 Telegram ID: <code>{telegram_id}</code>")

    if latitude and longitude:
        lines.append(f"\U0001f4cd Joylashuv: <a href=\"https://maps.google.com/?q={latitude},{longitude}\">Xaritada ko\u2019rish</a>")

    lines.append("")
    if is_approved:
        lines.append("\u2705 Avtomatik tasdiqlangan")
    else:
        lines.append("\u23f3 Tasdiqlanmagan (narxlar yashirin)")
        lines.append(f"\U0001f449 Tasdiqlash: /approve {telegram_id}")
        lines.append("")
        lines.append("<i>\U0001f4dd Javob bering: 1C mijoz nomi yoki 'new'</i>")

    message_text = "\n".join(lines)
    message_id = None

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
        result = resp.json()
        if result.get("ok"):
            message_id = result["result"]["message_id"]
            logger.info(f"Registration notification sent to {chat_id}, msg_id={message_id}")
        else:
            logger.error(f"Telegram API error: {result.get('description')}")

    except Exception as e:
        logger.error(f"Failed to send registration notification: {e}")

    # For unmatched users, save to unmatched_registrations for later review
    if not is_approved and message_id:
        _save_unmatched(telegram_id, phone, first_name, last_name, username, message_id)

    return message_id


def _save_unmatched(telegram_id, phone, first_name, last_name, username, message_id):
    """Insert or update the unmatched_registrations table."""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute(
            """INSERT INTO unmatched_registrations
               (telegram_id, phone, first_name, last_name, username, notification_message_id)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(telegram_id) DO UPDATE SET
                   phone = excluded.phone,
                   notification_message_id = excluded.notification_message_id,
                   status = 'pending',
                   resolved_at = NULL""",
            (telegram_id, phone, first_name, last_name, username, message_id),
        )
        conn.commit()
        conn.close()
        logger.info(f"Saved unmatched registration for telegram_id={telegram_id}")
    except Exception as e:
        logger.error(f"Failed to save unmatched registration: {e}")
