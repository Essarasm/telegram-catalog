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

from aiogram import Router, F
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


async def _prompt_client_search(target, state: FSMContext, is_continuation: bool = False):
    """Set state to client_search and prompt the user for a client name.
    Used both as the initial /lokatsiya entry and as the auto-loop after each
    successful save — the cashier's menu pattern has only one option here so
    the menu step would be pure ceremony."""
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
async def cmd_lokatsiya(message: Message, state: FSMContext):
    if not _is_driver_chat(message):
        return
    await state.clear()
    await _prompt_client_search(message, state, is_continuation=False)


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
        # Re-check at write time — state could be stale if the canonical pin
        # was set between pick and location-share by a parallel writer.
        cur = conn.execute(
            "SELECT gps_latitude FROM allowed_clients WHERE id = ?",
            (client_id,),
        ).fetchone()
        still_locked = cur and cur["gps_latitude"] is not None

        if still_locked:
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
            conn.execute(
                "UPDATE allowed_clients SET "
                "gps_latitude = ?, gps_longitude = ?, gps_address = ?, "
                "gps_region = ?, gps_district = ?, gps_set_at = datetime('now'), "
                "gps_set_by_tg_id = ?, gps_set_by_name = ?, gps_set_by_role = ? "
                "WHERE id = ?",
                (loc.latitude, loc.longitude, geo["address"], geo["region"],
                 geo["district"], message.from_user.id, setter_name, setter_role,
                 client_id),
            )
            conn.commit()
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

    # Auto-loop: prompt for the next client immediately so backfill / batch
    # entry doesn't require typing /lokatsiya between each.
    await _prompt_client_search(message, state, is_continuation=True)


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
