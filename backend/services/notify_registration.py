"""Send new user registration notifications to the Sotuv bo'limi (Sales) group."""
import os
import logging
import sqlite3
import httpx

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")
from backend.services.group_config import ADMIN_GROUP_CHAT_ID, MANAGER_CHAT_ID


def _resolve_notification_chat_id() -> int:
    """Same precedence used by send_registration_notification — exposed so
    the inline-button handlers can edit the original notification message."""
    return MANAGER_CHAT_ID or ADMIN_GROUP_CHAT_ID or 0


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
    For unmatched users: saves to unmatched_registrations + attaches two
    inline buttons (Klientga bog'lash / Yangi klient) so the admin can
    resolve the registration without typing /link or /approve.

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
        "\U0001f464 <b>Yangi foydalanuvchi ro’yxatdan o’tdi!</b>",
        "",
    ]

    # 1C client name — prefer the Cyrillic 1C identifier over the
    # allowed_clients.name column, which can hold a Latin Telegram first_name
    # for rows inserted via the bot linking flow.
    mijoz_1c = client_id_1c or client_name
    if mijoz_1c:
        lines.append(f"\U0001f3e2 Mijoz (1C): <b>{mijoz_1c}</b>")
        if client_name and client_name != mijoz_1c:
            lines.append(f"   \U0001f4cb Nomi: {client_name}")
    else:
        lines.append("\U0001f3e2 Mijoz (1C): <i>1C nomi topilmadi</i>")

    lines.append("")
    lines.append(f"\U0001f4f1 Telegram: <b>{full_name}</b>")
    lines.append(f"\U0001f4de Telefon: <b>{phone_display}</b>")
    lines.append(f"\U0001f4ac Username: {username_display}")
    lines.append(f"\U0001f194 Telegram ID: <code>{telegram_id}</code>")

    if latitude and longitude:
        lines.append(f"\U0001f4cd Joylashuv: <a href=\"https://maps.google.com/?q={latitude},{longitude}\">Xaritada ko’rish</a>")

    lines.append("")
    if is_approved:
        lines.append("✅ Avtomatik tasdiqlangan")
    else:
        lines.append("⏳ Tasdiqlanmagan (narxlar yashirin)")

    message_text = "\n".join(lines)
    message_id = None

    # Inline keyboard — admin taps one of two buttons to either link this
    # user to an existing 1C client (search/picker FSM in
    # bot/handlers/registration_link.py) or create a new unbound-1C client
    # (admin types fullname + address + optional GPS). Buttons are
    # suppressed when the user was auto-approved (already linked); the
    # /link command + reply-based fallback in bot/handlers/registration.py
    # both remain as edge-case escape hatches.
    payload = {
        "chat_id": chat_id,
        "text": message_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if not is_approved:
        payload["reply_markup"] = {
            "inline_keyboard": [
                [
                    {
                        "text": "✅ Klientga bog’lash",
                        "callback_data": f"reg:link:{telegram_id}",
                    },
                    {
                        "text": "\U0001f195 Yangi klient",
                        "callback_data": f"reg:new:{telegram_id}",
                    },
                ],
                # Third option: this registrant is our own staff, not a shop —
                # opens the panel-role picker (admin/cashier/agent/ishchi/observer)
                # via the existing reg:role handler in registration_link.py.
                # Assigning a role marks them is_agent=1 and drops them from /unlinked.
                [
                    {
                        "text": "\U0001f454 Xodim sifatida (rol berish)",
                        "callback_data": f"reg:role:{telegram_id}",
                    },
                ],
            ],
        }

    try:
        api_url = f"https://api.telegram.org/bot{BOT_TOKEN}"
        resp = httpx.post(
            f"{api_url}/sendMessage",
            json=payload,
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


def notify_unbound_clients_linked(linked: list) -> None:
    """Post a summary message after an auto-link sweep heals unbound rows.

    Grouped by source_sheet so admins see which paths got resolved (admin
    'Yangi klient' button vs. agent panel 'Yangi do'kon' vs. reply-based
    'new'). Skips silently when the list is empty.
    """
    if not linked or not BOT_TOKEN:
        return
    chat_id = MANAGER_CHAT_ID or ADMIN_GROUP_CHAT_ID
    if not chat_id:
        return

    by_source: dict = {}
    for row in linked:
        src = row.get("source_sheet") or "unknown"
        by_source.setdefault(src, []).append(row)

    src_label = {
        "admin_panel":     "\U0001f465 Admin",
        "agent_panel":     "\U0001f4bc Agent",
        "bot_new":         "\U0001f916 Bot",
        "bot_new_client":  "\U0001f916 Bot",
        "bot_approved":    "\U0001f916 Bot",
    }

    lines = [
        f"\U0001f517 <b>Avtomatik 1C-bog’lanish</b> ({len(linked)} ta)",
        "",
    ]
    for src, items in sorted(by_source.items()):
        head = src_label.get(src, src)
        lines.append(f"<b>{head}</b> ({len(items)})")
        for it in items[:10]:
            name = it.get("name") or "?"
            c1c = it.get("client_id_1c") or "?"
            lines.append(f"  • {name} → <code>{c1c}</code>")
        if len(items) > 10:
            lines.append(f"  … va yana {len(items) - 10} ta")
        lines.append("")

    try:
        api_url = f"https://api.telegram.org/bot{BOT_TOKEN}"
        httpx.post(
            f"{api_url}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": "\n".join(lines).strip(),
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"notify_unbound_clients_linked failed: {e}")
