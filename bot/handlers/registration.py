"""Registration reply handler — link unmatched registrations to 1C clients.

Sales team replies to unmatched registration notifications in the Admin group
with a 1C client name or 'new' to approve.
"""
import logging

from html import escape as _h

from aiogram import Router, F
from aiogram.types import Message

from bot.shared import (
    get_db, html_escape, normalize_phone,
    ADMIN_GROUP_CHAT_ID,
)

logger = logging.getLogger("bot")
router = Router(name="registration")


@router.message(F.reply_to_message)
async def handle_registration_reply(message: Message):
    """Handle replies to unmatched registration notifications in Admin group."""
    if message.chat.id != ADMIN_GROUP_CHAT_ID:
        return
    if not message.text or not message.text.strip():
        return

    replied_msg_id = message.reply_to_message.message_id
    reply_text = message.text.strip()

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT id, telegram_id, phone, first_name FROM unmatched_registrations "
            "WHERE notification_message_id = ? AND status = 'pending'",
            (replied_msg_id,),
        ).fetchone()

        if not row:
            return

        unreg_id = row["id"]
        tg_id = row["telegram_id"]
        phone = row["phone"]
        user_first_name = row["first_name"] or "—"

        if reply_text.lower() == "new":
            conn.execute(
                "UPDATE unmatched_registrations SET status = 'new_client', resolved_at = datetime('now') WHERE id = ?",
                (unreg_id,),
            )
            conn.execute("UPDATE users SET is_approved = 1 WHERE telegram_id = ?", (tg_id,))

            phone_norm = normalize_phone(phone)
            if phone_norm:
                existing = conn.execute(
                    "SELECT id FROM allowed_clients WHERE phone_normalized = ? LIMIT 1",
                    (phone_norm,),
                ).fetchone()
                if not existing:
                    conn.execute(
                        "INSERT INTO allowed_clients (phone_normalized, name, source_sheet, status, matched_telegram_id) "
                        "VALUES (?, ?, 'bot_new_client', 'active', ?)",
                        (phone_norm, user_first_name, tg_id),
                    )
                    client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                else:
                    client_id = existing["id"]
                    conn.execute("UPDATE allowed_clients SET matched_telegram_id = ? WHERE id = ?", (tg_id, client_id))
                conn.execute("UPDATE users SET client_id = ? WHERE telegram_id = ?", (client_id, tg_id))

            conn.commit()

            try:
                from backend.services.backup_users import save_user_to_backup
                u = conn.execute(
                    "SELECT telegram_id, phone, first_name, last_name, username, latitude, longitude, is_approved, client_id, registered_at FROM users WHERE telegram_id = ?",
                    (tg_id,),
                ).fetchone()
                if u:
                    save_user_to_backup(dict(u))
            except Exception:
                pass

            await message.reply(
                f"✅ <b>Yangi mijoz</b> sifatida belgilandi!\n\n"
                f"📛 {user_first_name}\n"
                f"🆔 {tg_id}\n\n"
                f"Foydalanuvchi tasdiqlandi. 1C da yangi kontragent yarating.",
                parse_mode="HTML",
            )

        else:
            client_row = conn.execute(
                "SELECT id, name, phone_normalized FROM allowed_clients "
                "WHERE LOWER(TRIM(name)) = LOWER(TRIM(?)) AND name != '' LIMIT 1",
                (reply_text,),
            ).fetchone()

            if not client_row:
                client_row = conn.execute(
                    "SELECT id, name, phone_normalized FROM allowed_clients "
                    "WHERE name LIKE ? AND name != '' LIMIT 1",
                    (f"%{reply_text}%",),
                ).fetchone()

            if not client_row:
                await message.reply(
                    f"❌ <b>{_h(reply_text)}</b> — 1C bazasida topilmadi.\n\n"
                    f"Qayta urinib ko'ring yoki <b>new</b> yozing.",
                    parse_mode="HTML",
                )
                return

            client_id = client_row["id"]
            client_name = client_row["name"]

            conn.execute(
                "UPDATE unmatched_registrations SET status = 'linked', linked_client_name = ?, resolved_at = datetime('now') WHERE id = ?",
                (client_name, unreg_id),
            )
            conn.execute("UPDATE users SET is_approved = 1, client_id = ? WHERE telegram_id = ?", (client_id, tg_id))
            conn.execute("UPDATE allowed_clients SET matched_telegram_id = ? WHERE id = ?", (tg_id, client_id))
            conn.commit()

            try:
                from backend.services.backup_users import save_user_to_backup
                u = conn.execute(
                    "SELECT telegram_id, phone, first_name, last_name, username, latitude, longitude, is_approved, client_id, registered_at FROM users WHERE telegram_id = ?",
                    (tg_id,),
                ).fetchone()
                if u:
                    save_user_to_backup(dict(u))
            except Exception:
                pass

            await message.reply(
                f"✅ Bog'landi!\n\n"
                f"🏢 1C mijoz: <b>{_h(client_name)}</b>\n"
                f"📛 Telegram: {_h(user_first_name)}\n"
                f"🆔 {tg_id}\n\n"
                f"Foydalanuvchi tasdiqlandi.",
                parse_mode="HTML",
            )

    except Exception as e:
        logger.error(f"handle_registration_reply error: {e}")
        await message.reply(f"❌ Xatolik: {e}")
    finally:
        conn.close()
