"""Inline-button driven new-user link / create flow.

Replaces the reply-based `/link tg_id 1c_name` and `/approve tg_id` admin
commands with two buttons attached to the "Yangi foydalanuvchi
ro'yxatdan o'tdi" notification:

  ✅ Klientga bog'lash  — link this user to an existing 1C client.
                          If the user's phone exactly matches one
                          allowed_clients row, skip the search; else
                          enter LinkClientFlow.awaiting_name → search
                          via search_clients() → render picker.

  🆕 Yangi klient       — create a new allowed_clients row with
                          client_id_1c=NULL, status='active',
                          source_sheet='admin_panel'. Admin types
                          fullname → address → optional GPS pin.

Both paths perform the same downstream writes as cmd_link
(bot/main.py:419-445) and cmd_approve (bot/main.py:238-336), plus an
audit row in agent_client_registrations (the same table the agent
panel uses — admin and agent shop-creations share the audit trail).

Race re-checks: both execute helpers re-query users.is_approved /
client_id before writing, so two admins clicking the same notification
don't double-write — second tap gets "Allaqachon bog'langan".

The legacy /link, /approve, and the reply-based handler in
bot/handlers/registration.py all remain in place as escape hatches.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from backend.services.client_search import search_clients, client_display_label
from backend.services.notify_registration import _resolve_notification_chat_id
from bot.shared import (
    get_db,
    html_escape,
    is_admin_cb,
    normalize_phone,
)

logger = logging.getLogger(__name__)
router = Router(name="registration_link")

TASHKENT_TZ = timezone(timedelta(hours=5))


def _now_tashkent_hhmm() -> str:
    return datetime.now(TASHKENT_TZ).strftime("%H:%M")


# ── States ──────────────────────────────────────────────────────────


class LinkClientFlow(StatesGroup):
    awaiting_name = State()


class NewClientFlow(StatesGroup):
    awaiting_fullname = State()
    awaiting_address = State()
    awaiting_gps_optional = State()


# ── Helpers ─────────────────────────────────────────────────────────


def _cancel_kb(tg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Bekor", callback_data=f"reg:cancel:{tg_id}"),
    ]])


def _admin_display(cb: CallbackQuery) -> str:
    u = cb.from_user
    if not u:
        return "admin"
    if u.username:
        return f"@{u.username}"
    name = " ".join(p for p in [u.first_name or "", u.last_name or ""] if p).strip()
    return name or str(u.id)


def _parse_cb(cb_data: str, expected_parts: int) -> Optional[list]:
    parts = (cb_data or "").split(":")
    if len(parts) != expected_parts:
        return None
    return parts


def _update_approved_overrides(telegram_id: int) -> None:
    """Mirror of the approved_overrides.json update in cmd_link / cmd_approve
    (bot/main.py:312-325). Persists across restarts via JSON; the SQLite-
    users row is authoritative but the JSON keeps a paranoid replica."""
    try:
        overrides_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "approved_overrides.json"
        )
        overrides = {"always_approved_ids": []}
        if os.path.exists(overrides_path):
            with open(overrides_path) as f:
                overrides = json.load(f)
        ids = set(overrides.get("always_approved_ids", []))
        if telegram_id not in ids:
            ids.add(telegram_id)
            overrides["always_approved_ids"] = sorted(ids)
            with open(overrides_path, "w") as f:
                json.dump(overrides, f, indent=2)
    except Exception as e:
        logger.warning(f"approved_overrides update failed for {telegram_id}: {e}")


def _backup_user(conn, telegram_id: int) -> None:
    """Mirror of cmd_link's save_user_to_backup call. Best-effort."""
    try:
        from backend.services.backup_users import save_user_to_backup
        row = conn.execute(
            "SELECT telegram_id, phone, first_name, last_name, username, "
            "latitude, longitude, is_approved, client_id, registered_at "
            "FROM users WHERE telegram_id = ?",
            (telegram_id,),
        ).fetchone()
        if row:
            save_user_to_backup(dict(row))
    except Exception as e:
        logger.warning(f"user backup after link failed for {telegram_id}: {e}")


async def _edit_notification_outcome(
    bot: Bot, chat_id: int, message_id: int, outcome_line: str
) -> None:
    """Append an outcome line to the original notification + strip the
    inline keyboard so the buttons can't be tapped again."""
    if not chat_id or not message_id:
        return
    try:
        await bot.edit_message_reply_markup(
            chat_id=chat_id, message_id=message_id, reply_markup=None
        )
    except Exception as e:
        logger.debug(f"edit_message_reply_markup failed: {e}")
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=outcome_line,
            parse_mode="HTML",
            reply_to_message_id=message_id,
            disable_notification=True,
        )
    except Exception as e:
        logger.warning(f"outcome message failed: {e}")


async def _dm_user_approved(bot: Bot, telegram_id: int, client_name: str) -> None:
    """DM the user that they're approved + linked. Best-effort — DM may
    fail if user blocked the bot, but the DB write already happened."""
    try:
        await bot.send_message(
            chat_id=telegram_id,
            text=(
                "✅ <b>Tasdiqlandi!</b>\n\n"
                f"🏢 1C mijoz: <b>{html_escape(client_name)}</b>\n\n"
                "Ilovani qayta ochsa, narxlarni ko'rasiz."
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning(f"DM approval to user {telegram_id} failed: {e}")


async def _dm_user_pending_1c(bot: Bot, telegram_id: int, fullname: str) -> None:
    try:
        await bot.send_message(
            chat_id=telegram_id,
            text=(
                "✅ <b>Tasdiqlandi!</b>\n\n"
                f"📛 {html_escape(fullname)}\n\n"
                "1C kartochkangiz tayyor bo'lganda balansingiz va "
                "buyurtmalaringiz ham ko'rinadi."
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning(f"DM pending-1c to user {telegram_id} failed: {e}")


def _fetch_notification_ctx(conn, telegram_id: int) -> tuple[Optional[int], Optional[int]]:
    """Return (notification_chat_id, notification_message_id) for editing
    the original "Yangi foydalanuvchi" message. Chat is re-derived via
    the same precedence as the sender (MANAGER → ADMIN_GROUP)."""
    row = conn.execute(
        "SELECT notification_message_id FROM unmatched_registrations "
        "WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    msg_id = row["notification_message_id"] if row else None
    return _resolve_notification_chat_id(), msg_id


# ── Cancel ──────────────────────────────────────────────────────────


@router.callback_query(F.data.startswith("reg:cancel:"))
async def cb_cancel(cb: CallbackQuery, state: FSMContext):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return
    cur = await state.get_state()
    if cur is not None:
        await state.clear()
    try:
        await cb.message.edit_text("❌ Bekor qilindi.")
    except Exception:
        pass
    await cb.answer()


@router.message(Command("bekor"))
async def cmd_bekor(message: Message, state: FSMContext):
    """In-FSM /bekor escape. Registered BEFORE state-bound F.text handlers
    so command messages don't get swallowed mid-flow (feedback_aiogram_
    command_before_fsm_text)."""
    cur = await state.get_state()
    if cur is None:
        return
    if not any(
        cur == s.state for s in (
            LinkClientFlow.awaiting_name,
            NewClientFlow.awaiting_fullname,
            NewClientFlow.awaiting_address,
            NewClientFlow.awaiting_gps_optional,
        )
    ):
        return
    await state.clear()
    await message.answer("❌ Bekor qilindi.")


# ── Entry: Klientga bog'lash ────────────────────────────────────────


@router.callback_query(F.data.startswith("reg:link:"))
async def cb_link_entry(cb: CallbackQuery, state: FSMContext):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return
    parts = _parse_cb(cb.data, 3)
    if not parts or not parts[2].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=True)
        return
    tg_id = int(parts[2])

    conn = get_db()
    try:
        user = conn.execute(
            "SELECT telegram_id, phone, first_name, is_approved, client_id "
            "FROM users WHERE telegram_id = ?",
            (tg_id,),
        ).fetchone()
        if not user:
            await cb.answer("Foydalanuvchi topilmadi", show_alert=True)
            return

        if user["is_approved"] and user["client_id"]:
            await cb.answer("Allaqachon bog'langan", show_alert=True)
            try:
                await cb.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            return

        phone_norm = normalize_phone(user["phone"] or "")
        matched_row = None
        if len(phone_norm) >= 9:
            matched_row = conn.execute(
                "SELECT id, client_id_1c, name FROM allowed_clients "
                "WHERE phone_normalized = ? "
                "AND client_id_1c IS NOT NULL AND client_id_1c != '' "
                "AND COALESCE(status, 'active') NOT LIKE 'merged%' "
                "ORDER BY id LIMIT 2",
                (phone_norm,),
            ).fetchall()

        notif_chat, notif_msg = _fetch_notification_ctx(conn, tg_id)
    finally:
        conn.close()

    await state.update_data(
        target_telegram_id=tg_id,
        notification_chat_id=notif_chat,
        notification_message_id=notif_msg,
    )

    if matched_row and len(matched_row) == 1:
        ac = matched_row[0]
        c1c = client_display_label(ac["client_id_1c"], ac["name"]) or f"ID {ac['id']}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"✅ Ha, bog'lash: {c1c[:40]}",
                callback_data=f"reg:link_confirm:{tg_id}:{ac['id']}",
            )],
            [InlineKeyboardButton(
                text="🔎 Boshqa nom qidirish",
                callback_data=f"reg:link_search:{tg_id}",
            )],
            [InlineKeyboardButton(
                text="❌ Bekor",
                callback_data=f"reg:cancel:{tg_id}",
            )],
        ])
        await cb.message.answer(
            f"📞 Telefon <b>{html_escape(phone_norm)}</b> "
            f"→ <b>{html_escape(c1c)}</b>\n\nBog'laymizmi?",
            parse_mode="HTML",
            reply_markup=kb,
        )
        await cb.answer()
        return

    await state.set_state(LinkClientFlow.awaiting_name)
    await cb.message.answer(
        "🔎 <b>Klient nomini kiriting</b> (1C nomi yoki kompaniya):",
        parse_mode="HTML",
        reply_markup=_cancel_kb(tg_id),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("reg:link_search:"))
async def cb_link_search(cb: CallbackQuery, state: FSMContext):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return
    parts = _parse_cb(cb.data, 3)
    if not parts or not parts[2].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=True)
        return
    tg_id = int(parts[2])
    # Carry forward notification context if state was cleared
    data = await state.get_data()
    if not data.get("target_telegram_id"):
        conn = get_db()
        try:
            notif_chat, notif_msg = _fetch_notification_ctx(conn, tg_id)
        finally:
            conn.close()
        await state.update_data(
            target_telegram_id=tg_id,
            notification_chat_id=notif_chat,
            notification_message_id=notif_msg,
        )
    await state.set_state(LinkClientFlow.awaiting_name)
    await cb.message.answer(
        "🔎 <b>Klient nomini kiriting:</b>",
        parse_mode="HTML",
        reply_markup=_cancel_kb(tg_id),
    )
    await cb.answer()


@router.message(LinkClientFlow.awaiting_name, F.text)
async def msg_link_search_name(message: Message, state: FSMContext):
    data = await state.get_data()
    tg_id = data.get("target_telegram_id")
    if not tg_id:
        await state.clear()
        return

    q = (message.text or "").strip()
    if len(q) < 2:
        await message.answer(
            "Kamida 2 ta harf kiriting.",
            reply_markup=_cancel_kb(tg_id),
        )
        return

    results = search_clients(q, limit=8, fuzzy=True)
    whitelisted = [
        c for c in (results.get("whitelisted") or [])
        if c.get("client_id_1c")
    ]
    if not whitelisted:
        await message.answer(
            f"🔍 «{html_escape(q)}» — topilmadi. Qaytadan kiriting yoki ❌ Bekor.",
            parse_mode="HTML",
            reply_markup=_cancel_kb(tg_id),
        )
        return

    rows = []
    for c in whitelisted[:8]:
        label = (c.get("client_id_1c") or c.get("name") or f"ID {c['id']}")[:55]
        rows.append([InlineKeyboardButton(
            text=label,
            callback_data=f"reg:pick:{tg_id}:{c['id']}",
        )])
    rows.append([InlineKeyboardButton(
        text="❌ Bekor", callback_data=f"reg:cancel:{tg_id}",
    )])
    await message.answer(
        f"<b>«{html_escape(q)}»</b>:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


@router.callback_query(F.data.startswith("reg:pick:"))
async def cb_link_pick(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return
    parts = _parse_cb(cb.data, 4)
    if not parts or not parts[2].isdigit() or not parts[3].isdigit():
        await cb.answer("Noto'g'ri tanlov", show_alert=True)
        return
    tg_id = int(parts[2])
    ac_id = int(parts[3])
    await _execute_link(cb, state, bot, tg_id, ac_id)


@router.callback_query(F.data.startswith("reg:link_confirm:"))
async def cb_link_confirm(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return
    parts = _parse_cb(cb.data, 4)
    if not parts or not parts[2].isdigit() or not parts[3].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=True)
        return
    tg_id = int(parts[2])
    ac_id = int(parts[3])
    await _execute_link(cb, state, bot, tg_id, ac_id)


async def _execute_link(
    cb: CallbackQuery, state: FSMContext, bot: Bot, tg_id: int, ac_id: int
) -> None:
    """Atomic link execution shared by reg:pick and reg:link_confirm.

    Re-checks users.is_approved/client_id immediately before writing to
    block two-admin races. Mirrors the SQL semantics of cmd_link
    (bot/main.py:419-445)."""
    data = await state.get_data()
    notif_chat = data.get("notification_chat_id")
    notif_msg = data.get("notification_message_id")

    conn = get_db()
    try:
        user = conn.execute(
            "SELECT telegram_id, phone, first_name, is_approved, client_id "
            "FROM users WHERE telegram_id = ?",
            (tg_id,),
        ).fetchone()
        if not user:
            await cb.answer("Foydalanuvchi topilmadi", show_alert=True)
            await state.clear()
            return
        if user["is_approved"] and user["client_id"]:
            await cb.answer("Allaqachon bog'langan", show_alert=True)
            try:
                await cb.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await state.clear()
            return

        ac = conn.execute(
            "SELECT id, client_id_1c, name FROM allowed_clients "
            "WHERE id = ? AND COALESCE(status, 'active') NOT LIKE 'merged%'",
            (ac_id,),
        ).fetchone()
        if not ac or not ac["client_id_1c"]:
            await cb.answer("Mijoz endi mavjud emas", show_alert=True)
            await state.clear()
            return

        target_1c = ac["client_id_1c"]
        user_phone_norm = normalize_phone(user["phone"] or "")
        existing_row = conn.execute(
            "SELECT id FROM allowed_clients "
            "WHERE phone_normalized = ? "
            "AND COALESCE(status, 'active') NOT LIKE 'merged%' "
            "ORDER BY id LIMIT 1",
            (user_phone_norm,),
        ).fetchone() if user_phone_norm else None

        if existing_row:
            conn.execute(
                "UPDATE allowed_clients SET client_id_1c = ?, "
                "matched_telegram_id = ? WHERE id = ?",
                (target_1c, tg_id, existing_row["id"]),
            )
            client_id = existing_row["id"]
        else:
            conn.execute(
                "INSERT INTO allowed_clients "
                "(phone_normalized, name, source_sheet, status, "
                " client_id_1c, matched_telegram_id) "
                "VALUES (?, ?, 'bot_linked', 'active', ?, ?)",
                (user_phone_norm, user["first_name"], target_1c, tg_id),
            )
            client_id = conn.execute(
                "SELECT last_insert_rowid()"
            ).fetchone()[0]

        conn.execute(
            "UPDATE users SET is_approved = 1, client_id = ? "
            "WHERE telegram_id = ?",
            (client_id, tg_id),
        )

        conn.execute(
            "UPDATE unmatched_registrations "
            "SET status = 'linked', linked_client_name = ?, "
            "resolved_at = datetime('now') "
            "WHERE telegram_id = ?",
            (target_1c, tg_id),
        )

        conn.commit()
        _backup_user(conn, tg_id)
    finally:
        conn.close()

    _update_approved_overrides(tg_id)

    admin_label = _admin_display(cb)
    await cb.answer(f"✅ Bog'landi: {target_1c[:60]}")
    await _edit_notification_outcome(
        bot,
        notif_chat or 0,
        notif_msg or 0,
        (
            f"✅ <b>Bog'landi:</b> {html_escape(target_1c)}\n"
            f"by {html_escape(admin_label)} · {_now_tashkent_hhmm()}"
        ),
    )
    await _dm_user_approved(bot, tg_id, target_1c)
    await state.clear()


# ── Entry: Yangi klient ─────────────────────────────────────────────


@router.callback_query(F.data.startswith("reg:new:"))
async def cb_new_entry(cb: CallbackQuery, state: FSMContext):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return
    parts = _parse_cb(cb.data, 3)
    if not parts or not parts[2].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=True)
        return
    tg_id = int(parts[2])

    conn = get_db()
    try:
        user = conn.execute(
            "SELECT is_approved, client_id FROM users WHERE telegram_id = ?",
            (tg_id,),
        ).fetchone()
        if user and user["is_approved"] and user["client_id"]:
            await cb.answer("Allaqachon bog'langan", show_alert=True)
            try:
                await cb.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            return
        notif_chat, notif_msg = _fetch_notification_ctx(conn, tg_id)
    finally:
        conn.close()

    await state.update_data(
        target_telegram_id=tg_id,
        notification_chat_id=notif_chat,
        notification_message_id=notif_msg,
        admin_id=cb.from_user.id if cb.from_user else None,
    )
    await state.set_state(NewClientFlow.awaiting_fullname)
    await cb.message.answer(
        "📝 <b>Ism va familiyani kiriting</b> (1C uchun ko'rinadigan ism):",
        parse_mode="HTML",
        reply_markup=_cancel_kb(tg_id),
    )
    await cb.answer()


@router.message(NewClientFlow.awaiting_fullname, F.text)
async def msg_new_fullname(message: Message, state: FSMContext):
    data = await state.get_data()
    tg_id = data.get("target_telegram_id")
    if not tg_id:
        await state.clear()
        return
    name = (message.text or "").strip()
    if len(name) < 2:
        await message.answer(
            "Kamida 2 ta harf kiriting.",
            reply_markup=_cancel_kb(tg_id),
        )
        return
    await state.update_data(new_client_fullname=name)
    await state.set_state(NewClientFlow.awaiting_address)
    await message.answer(
        "🏠 <b>Do'kon manzilini kiriting</b> (matn):",
        parse_mode="HTML",
        reply_markup=_cancel_kb(tg_id),
    )


@router.message(NewClientFlow.awaiting_address, F.text)
async def msg_new_address(message: Message, state: FSMContext):
    data = await state.get_data()
    tg_id = data.get("target_telegram_id")
    if not tg_id:
        await state.clear()
        return
    addr = (message.text or "").strip()
    if len(addr) < 3:
        await message.answer(
            "Kamida 3 ta belgi kiriting.",
            reply_markup=_cancel_kb(tg_id),
        )
        return
    await state.update_data(new_client_address=addr)
    await state.set_state(NewClientFlow.awaiting_gps_optional)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="⏭ O'tkazib yuborish",
            callback_data=f"reg:new_skip_gps:{tg_id}",
        )],
        [InlineKeyboardButton(
            text="❌ Bekor", callback_data=f"reg:cancel:{tg_id}",
        )],
    ])
    await message.answer(
        "📍 <b>GPS pin yuborasizmi?</b> (ixtiyoriy)\n\n"
        "<i>Telegram paperclip → Location, yoki tugmani bosing.</i>",
        parse_mode="HTML",
        reply_markup=kb,
    )


@router.message(NewClientFlow.awaiting_gps_optional, F.location)
async def msg_new_gps(message: Message, state: FSMContext, bot: Bot):
    lat = message.location.latitude if message.location else None
    lng = message.location.longitude if message.location else None
    await _execute_new(message, state, bot, lat=lat, lng=lng)


@router.callback_query(F.data.startswith("reg:new_skip_gps:"))
async def cb_new_skip_gps(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return
    parts = _parse_cb(cb.data, 3)
    if not parts or not parts[2].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=True)
        return
    await _execute_new(cb, state, bot, lat=None, lng=None)
    await cb.answer()


@router.message(NewClientFlow.awaiting_gps_optional, F.text)
async def msg_new_gps_wrong(message: Message, state: FSMContext):
    """Admin typed text instead of sharing a location pin. Re-prompt."""
    data = await state.get_data()
    tg_id = data.get("target_telegram_id")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="⏭ O'tkazib yuborish",
            callback_data=f"reg:new_skip_gps:{tg_id}",
        )],
        [InlineKeyboardButton(
            text="❌ Bekor", callback_data=f"reg:cancel:{tg_id}",
        )],
    ])
    await message.answer(
        "GPS pin yuboring (paperclip → Location) yoki "
        "<b>⏭ O'tkazib yuborish</b> tugmasini bosing.",
        parse_mode="HTML",
        reply_markup=kb,
    )


async def _execute_new(
    source, state: FSMContext, bot: Bot,
    *, lat: Optional[float], lng: Optional[float],
) -> None:
    """Create-or-update the unbound allowed_clients row, update users,
    audit to agent_client_registrations, edit the notification, DM user.

    `source` is either a Message (F.location path) or a CallbackQuery
    (skip-GPS path); both expose .answer() and .from_user. We pull the
    admin id from FSM state which is set on entry."""
    data = await state.get_data()
    tg_id = data.get("target_telegram_id")
    fullname = (data.get("new_client_fullname") or "").strip()
    address = (data.get("new_client_address") or "").strip()
    admin_id = data.get("admin_id")
    notif_chat = data.get("notification_chat_id")
    notif_msg = data.get("notification_message_id")

    if not tg_id or not fullname or not address:
        await state.clear()
        if hasattr(source, "answer"):
            await source.answer("Holat yo'qolgan, qaytadan urinib ko'ring.")
        return

    conn = get_db()
    try:
        user = conn.execute(
            "SELECT telegram_id, phone, first_name, is_approved, client_id "
            "FROM users WHERE telegram_id = ?",
            (tg_id,),
        ).fetchone()
        if not user:
            await state.clear()
            return
        if user["is_approved"] and user["client_id"]:
            # Race: another admin already linked this user. Bail.
            if isinstance(source, CallbackQuery):
                await source.answer("Allaqachon bog'langan", show_alert=True)
            else:
                await source.answer("Bu foydalanuvchi allaqachon bog'langan.")
            await state.clear()
            return

        phone_norm = normalize_phone(user["phone"] or "")
        existing = conn.execute(
            "SELECT id FROM allowed_clients "
            "WHERE phone_normalized = ? "
            "AND COALESCE(status, 'active') NOT LIKE 'merged%' "
            "ORDER BY id LIMIT 1",
            (phone_norm,),
        ).fetchone() if phone_norm else None

        if existing:
            client_id = existing["id"]
            params = [fullname, address, tg_id]
            sql = (
                "UPDATE allowed_clients SET "
                "name = ?, moljal = ?, source_sheet = 'admin_panel', "
                "status = 'active', segment = COALESCE(segment, 'shop'), "
                "matched_telegram_id = ?"
            )
            if lat is not None and lng is not None:
                sql += (
                    ", gps_latitude = ?, gps_longitude = ?, "
                    "gps_set_at = datetime('now'), "
                    "gps_set_by_tg_id = ?, gps_set_by_role = 'admin'"
                )
                params = [
                    fullname, address, tg_id, lat, lng, admin_id, client_id,
                ]
            else:
                params = [fullname, address, tg_id, client_id]
            sql += " WHERE id = ?"
            conn.execute(sql, params)
        else:
            if lat is not None and lng is not None:
                conn.execute(
                    "INSERT INTO allowed_clients "
                    "(phone_normalized, name, moljal, source_sheet, status, "
                    " segment, matched_telegram_id, "
                    " gps_latitude, gps_longitude, gps_set_at, "
                    " gps_set_by_tg_id, gps_set_by_role) "
                    "VALUES (?, ?, ?, 'admin_panel', 'active', 'shop', ?, "
                    "        ?, ?, datetime('now'), ?, 'admin')",
                    (phone_norm, fullname, address, tg_id,
                     lat, lng, admin_id),
                )
            else:
                conn.execute(
                    "INSERT INTO allowed_clients "
                    "(phone_normalized, name, moljal, source_sheet, status, "
                    " segment, matched_telegram_id) "
                    "VALUES (?, ?, ?, 'admin_panel', 'active', 'shop', ?)",
                    (phone_norm, fullname, address, tg_id),
                )
            client_id = conn.execute(
                "SELECT last_insert_rowid()"
            ).fetchone()[0]

        # users update — store the text address in users.location_address;
        # don't touch users.latitude/longitude (those reflect the user's
        # initial Mini App registration pin, not the admin-set venue).
        conn.execute(
            "UPDATE users SET is_approved = 1, client_id = ?, "
            "location_address = ? WHERE telegram_id = ?",
            (client_id, address, tg_id),
        )

        # Audit row — reuse agent_client_registrations (zero-data-loss rule).
        # admin_id stored in agent_telegram_id slot; status='admin_created'.
        conn.execute(
            "INSERT INTO agent_client_registrations "
            "(agent_telegram_id, shop_name, first_name, last_name, venue, "
            " phone_raw, phone_normalized, "
            " gps_latitude, gps_longitude, status, linked_client_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'admin_created', ?)",
            (
                admin_id or 0,
                fullname,
                fullname,  # split is brittle; store full in first_name slot too
                "",
                address,
                user["phone"] or "",
                phone_norm,
                lat,
                lng,
                client_id,
            ),
        )

        conn.execute(
            "UPDATE unmatched_registrations "
            "SET status = 'new_active', linked_client_name = ?, "
            "resolved_at = datetime('now') "
            "WHERE telegram_id = ?",
            (fullname, tg_id),
        )

        conn.commit()
        _backup_user(conn, tg_id)
    finally:
        conn.close()

    _update_approved_overrides(tg_id)

    # Outcome notification edit. Use the admin label from the source's
    # from_user (works for both Message and CallbackQuery).
    admin_user = getattr(source, "from_user", None)
    if admin_user and admin_user.username:
        admin_label = f"@{admin_user.username}"
    elif admin_user:
        admin_label = (
            (admin_user.first_name or "") + " " + (admin_user.last_name or "")
        ).strip() or str(admin_user.id)
    else:
        admin_label = "admin"

    gps_line = (
        f"\n📍 GPS: {lat:.5f}, {lng:.5f}" if (lat is not None and lng is not None) else ""
    )
    await _edit_notification_outcome(
        bot,
        notif_chat or 0,
        notif_msg or 0,
        (
            f"🆕 <b>Yangi mijoz yaratildi:</b> {html_escape(fullname)}\n"
            f"🏠 {html_escape(address)}{gps_line}\n"
            f"⏳ 1C kartochka kutilmoqda · auto-link keyingi importda\n"
            f"by {html_escape(admin_label)} · {_now_tashkent_hhmm()}"
        ),
    )
    await _dm_user_pending_1c(bot, tg_id, fullname)

    if isinstance(source, Message):
        await source.answer(
            f"🆕 Yangi mijoz yaratildi: <b>{html_escape(fullname)}</b>\n"
            f"🏠 {html_escape(address)}{gps_line}",
            parse_mode="HTML",
        )

    await state.clear()
