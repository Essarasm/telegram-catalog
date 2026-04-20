"""Client support forwarder — two-way messaging between unapproved clients
and the Admin group.

Flow:
  1. Client opens `t.me/samrassvetbot?start=support` from the Mini App.
  2. Bot replies in their DM with ForceReply prompting them to type the question.
  3. Client replies. Bot forwards the message to the Admin group with a
     "Reply here" header, recording the admin-group message_id ↔ client
     telegram_id mapping in `support_threads`.
  4. Any admin in the Admin group can reply-quote the forwarded message.
     Their reply-text gets DMed back to the client as "🟢 Savdo jamoa: ..."
"""
import logging
from aiogram import Router, F, types
from aiogram.types import ForceReply

from bot.shared import (
    get_db, sender_display_name, html_escape,
    ADMIN_GROUP_CHAT_ID, chat_context,
)

logger = logging.getLogger(__name__)
router = Router()

# Sentinel string embedded in the support prompt and the admin-group
# forwarded header. Used to detect replies on either side without a DB
# round-trip.
CLIENT_PROMPT_MARKER = "💬 Yordam so'rovi"
ADMIN_HEADER_PREFIX = "📨 Mijoz yordam so'rovi"


def _lookup_client_by_admin_msg(admin_msg_id: int):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT client_telegram_id FROM support_threads WHERE admin_message_id = ?",
            (admin_msg_id,),
        ).fetchone()
        return row["client_telegram_id"] if row else None
    finally:
        conn.close()


def _save_thread(admin_msg_id: int, client_tg_id: int, client_msg_id: int | None):
    conn = get_db()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO support_threads
               (admin_message_id, client_telegram_id, client_message_id)
               VALUES (?, ?, ?)""",
            (admin_msg_id, client_tg_id, client_msg_id),
        )
        conn.commit()
    finally:
        conn.close()


async def start_support_prompt(message: types.Message) -> None:
    """Called from cmd_start when the deep-link is 'support'."""
    await message.answer(
        f"{CLIENT_PROMPT_MARKER}\n\n"
        "Yordam so'rovingizni yoki savolingizni yozing.\n"
        "Xabar savdo jamoasiga yetkaziladi — javob shu yerda keladi.",
        reply_markup=ForceReply(selective=True),
    )


@router.message(F.chat.type == "private")
async def forward_client_support_dm(message: types.Message) -> None:
    """Private-chat catcher: if the message is a reply to the support prompt,
    forward it to the Admin group. Otherwise, no-op (let other handlers handle)."""
    # Commands and service messages are handled elsewhere
    if not message.text or message.text.startswith("/"):
        return

    reply_to = message.reply_to_message
    is_support_reply = bool(
        reply_to and reply_to.text and CLIENT_PROMPT_MARKER in reply_to.text
    )
    if not is_support_reply:
        return  # Normal DM — let other handlers process or ignore

    # Build the admin-group header
    user = message.from_user
    if not user:
        return

    display_name = sender_display_name(message)
    username = f"@{user.username}" if user.username else "(username yo'q)"

    # Try to fetch phone / client name from DB for richer context
    phone_hint = ""
    try:
        conn = get_db()
        row = conn.execute(
            """SELECT u.phone, ac.name as client_name
               FROM users u
               LEFT JOIN allowed_clients ac ON ac.matched_telegram_id = u.telegram_id
               WHERE u.telegram_id = ?""",
            (user.id,),
        ).fetchone()
        conn.close()
        if row:
            if row["phone"]:
                phone_hint += f"\n📱 Telefon: <code>{row['phone']}</code>"
            if row["client_name"]:
                phone_hint += f"\n🏷 1C: <b>{html_escape(row['client_name'])}</b>"
    except Exception as e:
        logger.warning(f"support DM context lookup failed: {e}")

    header = (
        f"{ADMIN_HEADER_PREFIX}\n"
        f"👤 {html_escape(display_name)} ({username})\n"
        f"🆔 <code>{user.id}</code>"
        f"{phone_hint}\n\n"
        "💬 Xabar:\n"
        f"{html_escape(message.text)}\n\n"
        "↩️ <i>Javob berish uchun shu xabarga <b>reply</b> qiling — matningiz mijozga yuboriladi.</i>"
    )

    try:
        sent = await message.bot.send_message(
            ADMIN_GROUP_CHAT_ID, header, parse_mode="HTML"
        )
        _save_thread(sent.message_id, user.id, message.message_id)
        await message.reply(
            "✅ Xabaringiz savdo jamoasiga yuborildi.\n"
            "Javob shu yerda keladi — iltimos, kuting."
        )
    except Exception as e:
        logger.exception(f"Failed to forward support DM: {e}")
        await message.reply("❌ Yuborib bo'lmadi. Iltimos, keyinroq urinib ko'ring.")


@router.message(F.chat.id == ADMIN_GROUP_CHAT_ID, F.reply_to_message)
async def relay_admin_reply_to_client(message: types.Message) -> None:
    """If an admin replies-quotes a forwarded support message in Admin group,
    DM the admin's reply text back to the original client."""
    reply_to = message.reply_to_message
    if not reply_to or not reply_to.text:
        return
    if ADMIN_HEADER_PREFIX not in reply_to.text:
        return  # not a reply to a support thread — ignore

    # Commands shouldn't be relayed
    if message.text and message.text.startswith("/"):
        return

    # Find client
    client_tg_id = _lookup_client_by_admin_msg(reply_to.message_id)
    if not client_tg_id:
        logger.warning(
            f"Admin replied to a support msg with no thread record "
            f"(admin_msg_id={reply_to.message_id})"
        )
        return

    admin_name = sender_display_name(message)
    body = message.text or message.caption or ""
    if not body:
        return

    payload = (
        f"🟢 <b>Savdo jamoa ({html_escape(admin_name)})</b>:\n\n"
        f"{html_escape(body)}\n\n"
        "<i>Qo'shimcha savolingiz bo'lsa, shunchaki shu xabarga yozing — "
        "jamoa ko'radi.</i>"
    )
    try:
        await message.bot.send_message(
            client_tg_id, payload, parse_mode="HTML",
            reply_markup=ForceReply(selective=True),
        )
    except Exception as e:
        logger.exception(f"Failed to DM client reply: {e}")
        await message.reply(f"❌ Mijozga yuborib bo'lmadi: {e}")
