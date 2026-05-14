"""Driver/agent client-location capture — DRIVER_GROUP_CHAT_ID flow.

Open to anyone in the group (no allowlist). Mirrors the cashier FSM:
/lokatsiya → menu → fuzzy client search → pick client → location pin →
save canonical to allowed_clients.gps_*.

First-confirmed-locks: if the picked client already has gps_latitude
set, the new pin is REJECTED — audit row gets processed_ok=0,
error_reason='client_already_has_gps' (with full lat/lng + reverse
geocode preserved). Admin reviews via location_attempts and decides
if the canonical pin needs renewal.
"""
from __future__ import annotations

import logging

from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
)

from bot.shared import get_db, html_escape, DRIVER_GROUP_CHAT_ID
from bot.handlers.location import _audit_insert, _audit_finalize, _reverse_geocode
from backend.services.client_search import search_clients

logger = logging.getLogger(__name__)
router = Router(name="driver_location")

# Module-level: id of the currently pinned menu, so /lokatsiya can unpin
# the previous one before pinning a fresh one. Resets on bot restart; a
# leftover pinned menu from before restart is harmless (it still works).
_pinned_menu_message_id: int | None = None


class DriverFlow(StatesGroup):
    client_search = State()
    awaiting_location = State()


def _is_driver_chat(message_or_cb) -> bool:
    if not DRIVER_GROUP_CHAT_ID:
        return False
    chat = getattr(message_or_cb, "chat", None) or (
        getattr(message_or_cb, "message", None) and message_or_cb.message.chat
    )
    return chat is not None and chat.id == DRIVER_GROUP_CHAT_ID


def _cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="❌ Bekor", callback_data="driver:cancel"),
        ]]
    )


def _menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="🆕 Yangi mijoz lokatsiyasi",
                                 callback_data="driver:new"),
        ]]
    )


async def _send_lokatsiya_menu(target):
    """Re-post the menu after a flow completes (or on /lokatsiya) so the next
    capture starts with a tap instead of retyping the command. Mirrors the
    cashier's _send_kassa_menu pattern."""
    await target.answer(
        "📍 <b>Lokatsiya</b>",
        parse_mode="HTML",
        reply_markup=_menu_keyboard(),
    )


async def _prompt_client_search(target, state: FSMContext, is_continuation: bool = False):
    """Set state to client_search and prompt the user for a client name."""
    await state.set_state(DriverFlow.client_search)
    text = (
        "🔎 <b>Yana mijoz nomini kiriting</b>"
        if is_continuation
        else "🔎 <b>Mijoz nomini kiriting</b>"
    )
    await target.answer(
        text,
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )


# ── Entry: /lokatsiya ───────────────────────────────────────────────

@router.message(Command("lokatsiya"))
async def cmd_lokatsiya(message: Message, state: FSMContext, bot: Bot):
    """Post the menu and pin it. Tap "🆕 Yangi mijoz lokatsiyasi" to enter the
    FSM — same flow as before, just button-first like the cashier group."""
    global _pinned_menu_message_id
    if not _is_driver_chat(message):
        return
    await state.clear()
    sent = await message.answer(
        "📍 <b>Lokatsiya</b>",
        parse_mode="HTML",
        reply_markup=_menu_keyboard(),
    )
    # Unpin the previous menu (if we tracked one) before pinning the new one,
    # so the group doesn't accumulate pinned menus across /lokatsiya calls.
    if _pinned_menu_message_id:
        try:
            await bot.unpin_chat_message(
                chat_id=message.chat.id,
                message_id=_pinned_menu_message_id,
            )
        except Exception:
            pass  # already unpinned / deleted — harmless
    try:
        await bot.pin_chat_message(
            chat_id=message.chat.id,
            message_id=sent.message_id,
            disable_notification=True,
        )
        _pinned_menu_message_id = sent.message_id
    except Exception:
        logger.warning("driver_location: pin failed (bot may lack admin rights)")


@router.callback_query(F.data == "driver:new")
async def cb_new(cb: CallbackQuery, state: FSMContext):
    """Tap on the pinned/auto-posted menu — start the flow."""
    if not _is_driver_chat(cb):
        await cb.answer()
        return
    await cb.answer()
    await state.clear()
    await _prompt_client_search(cb.message, state, is_continuation=False)


# ── Flow: client search → pick → awaiting location ──────────────────

@router.message(DriverFlow.client_search, F.text)
async def client_search_text(message: Message, state: FSMContext):
    if not _is_driver_chat(message):
        return
    q = (message.text or "").strip()
    if not q:
        await message.answer(
            "Mijoz nomini yuboring.",
            reply_markup=_cancel_keyboard(),
        )
        return
    results = search_clients(q, limit=8)
    whitelisted = results.get("whitelisted") or []
    if not whitelisted:
        await message.answer(
            f"🔍 «{html_escape(q)}» — topilmadi. Qaytadan yozing.",
            parse_mode="HTML",
            reply_markup=_cancel_keyboard(),
        )
        return
    rows = []
    for c in whitelisted[:8]:
        label = (c.get("client_id_1c") or c.get("name") or f"ID {c['id']}")[:55]
        rows.append([InlineKeyboardButton(
            text=label,
            callback_data=f"driver:pick_{c['id']}",
        )])
    rows.append([InlineKeyboardButton(text="❌ Bekor", callback_data="driver:cancel")])
    await message.answer(
        f"<b>«{html_escape(q)}»</b>:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


@router.callback_query(DriverFlow.client_search, F.data.startswith("driver:pick_"))
async def cb_pick_client(cb: CallbackQuery, state: FSMContext):
    if not _is_driver_chat(cb):
        await cb.answer()
        return
    try:
        client_id = int(cb.data.split("_", 1)[1])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri tanlov", show_alert=True)
        return
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT id, name, client_id_1c, gps_latitude, gps_longitude, "
            "gps_address, gps_set_by_name, gps_set_at "
            "FROM allowed_clients WHERE id = ?",
            (client_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        await cb.answer("Mijoz topilmadi", show_alert=True)
        return

    has_gps = row["gps_latitude"] is not None
    cname = row["client_id_1c"] or row["name"] or f"ID {row['id']}"

    await state.update_data(
        client_id=row["id"],
        client_name=cname,
        had_gps=has_gps,
    )
    await state.set_state(DriverFlow.awaiting_location)
    await cb.answer()

    if has_gps:
        prev_addr = row["gps_address"] or "—"
        prev_setter = row["gps_set_by_name"] or "—"
        prev_when = row["gps_set_at"] or "—"
        msg = (
            f"⚠️ <b>{html_escape(cname)}</b> uchun lokatsiya allaqachon mavjud:\n"
            f"📍 {html_escape(prev_addr)}\n"
            f"👤 {html_escape(prev_setter)} · {html_escape(prev_when)}\n\n"
            f"Yangi lokatsiya yuborsangiz — <b>saqlanmaydi</b> (bloklangan), "
            f"lekin admin uchun taqqoslash uchun yoziladi.\n\n"
            f"📍 <b>Lokatsiyani yuboring</b> (yoki Bekor)"
        )
    else:
        msg = (
            f"👤 {html_escape(cname)}\n\n"
            f"📍 <b>Lokatsiyani yuboring</b>"
        )
    await cb.message.answer(msg, parse_mode="HTML", reply_markup=_cancel_keyboard())


# ── Location received ────────────────────────────────────────────────

@router.message(DriverFlow.awaiting_location, F.location)
async def handle_driver_location(message: Message, state: FSMContext):
    """Audit-insert raw lat/lng, then either save canonical or reject as
    locked. Reverse-geocode runs regardless so the audit row has full
    context for admin renewal review."""
    if not _is_driver_chat(message):
        return  # FSM is per-(chat,user); double-check the chat scope

    data = await state.get_data()
    client_id = data.get("client_id")
    client_name = data.get("client_name") or ""

    if not client_id:
        await message.answer("❌ Mijoz tanlanmagan. /lokatsiya orqali boshlang.")
        await state.clear()
        return

    loc = message.location

    # Step 0 — durable audit insert (raw lat/lng preserved even on crash)
    audit_conn = get_db()
    audit_id = _audit_insert(audit_conn, message)
    audit_conn.close()

    geo = _reverse_geocode(loc.latitude, loc.longitude)
    setter_name = message.from_user.first_name or str(message.from_user.id)
    # 'agent' (not 'driver') so the admin "Agent Coverage" dashboard's
    # default "Agents only" filter includes these pins. Driver-group
    # submitters are field agents for dashboard purposes; the actual
    # telegram_id + name are preserved on gps_set_by_tg_id / gps_set_by_name.
    setter_role = "agent"

    conn = get_db()
    try:
        # Atomic conditional UPDATE — closes the SELECT/UPDATE TOCTOU window
        # where two parallel writers could both read NULL and both UPDATE,
        # silently overwriting the first-confirmed pin. The WHERE clause
        # makes SQLite serialize on the write lock and only mutate the row
        # if it is still NULL at lock-acquisition time. rowcount tells us
        # which branch we ended up in.
        cur = conn.execute(
            "UPDATE allowed_clients SET "
            "gps_latitude = ?, gps_longitude = ?, gps_address = ?, "
            "gps_region = ?, gps_district = ?, gps_set_at = datetime('now'), "
            "gps_set_by_tg_id = ?, gps_set_by_name = ?, gps_set_by_role = ? "
            "WHERE id = ? AND gps_latitude IS NULL",
            (loc.latitude, loc.longitude, geo["address"], geo["region"],
             geo["district"], message.from_user.id, setter_name, setter_role,
             client_id),
        )
        conn.commit()
        saved = cur.rowcount > 0

        if not saved:
            _audit_finalize(
                conn, audit_id, ok=False,
                error="client_already_has_gps",
                geocode_dict=geo,
                is_agent=0,
                linked_client_id=client_id,
                linked_client_1c=client_name,
            )
            await message.answer(
                f"⚠️ <b>{html_escape(client_name)}</b> uchun lokatsiya bloklangan.\n\n"
                f"Yuborgan lokatsiyangiz adminga taqqoslash uchun yoziladi.",
                parse_mode="HTML",
            )
        else:
            _audit_finalize(
                conn, audit_id, ok=True,
                geocode_dict=geo,
                is_agent=0,
                linked_client_id=client_id,
                linked_client_1c=client_name,
            )
            maps_url = f"https://maps.google.com/?q={loc.latitude},{loc.longitude}"
            display_parts = [p for p in (geo["region"], geo["address"]) if p]
            address_display = ", ".join(display_parts) if display_parts else "manzil aniqlandi"
            await message.answer(
                f"✅ <b>{html_escape(client_name)}</b> uchun lokatsiya saqlandi.\n\n"
                f"📍 {html_escape(address_display)}\n"
                f"🗺 <a href=\"{maps_url}\">Xaritada ko'rish</a>",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
    except Exception as e:
        conn.rollback()
        logger.exception("driver location save failed")
        try:
            _audit_finalize(conn, audit_id, ok=False, error=str(e)[:200],
                            geocode_dict=geo, linked_client_id=client_id,
                            linked_client_1c=client_name)
        except Exception:
            pass
        await message.answer(f"❌ Saqlashda xatolik: {html_escape(str(e))}")
    finally:
        conn.close()

    # Auto-loop: re-post the menu (button) so the next capture is a tap away.
    # Mirrors the cashier group's _send_kassa_menu pattern.
    await state.clear()
    await _send_lokatsiya_menu(message)


# ── Orphan pin (driver group, no active FSM) ─────────────────────────

@router.message(F.location, F.chat.id == DRIVER_GROUP_CHAT_ID)
async def handle_orphan_location(message: Message):
    """Catch driver-group pins from users who haven't run /lokatsiya yet.
    Without this, location.py's chat-scope guard returns early for
    DRIVER_GROUP_CHAT_ID and the pin is silently dropped — no audit row,
    raw lat/lng lost. Zero-data-loss rule requires every inbound location
    to land in `location_attempts` first. Declared after handle_driver_location
    so the FSM-filtered handler wins for users mid-flow."""
    audit_conn = get_db()
    audit_id = _audit_insert(audit_conn, message)
    _audit_finalize(audit_conn, audit_id, ok=False, error="no_active_fsm")
    audit_conn.close()
    await message.reply(
        "📍 Lokatsiya qabul qilindi (audit).\n\n"
        "Mijozga bog'lash uchun avval <b>/lokatsiya</b> yuboring, "
        "mijozni tanlang, keyin lokatsiyani qaytadan jo'nating.",
        parse_mode="HTML",
    )


# ── Cancel ───────────────────────────────────────────────────────────

@router.callback_query(F.data == "driver:cancel")
async def cb_cancel(cb: CallbackQuery, state: FSMContext):
    if not _is_driver_chat(cb):
        await cb.answer()
        return
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Bekor qilindi. /lokatsiya orqali yangidan.")
    except Exception:
        await cb.message.answer("❌ Bekor qilindi. /lokatsiya orqali yangidan.")
