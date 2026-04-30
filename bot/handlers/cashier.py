"""Cashier FSM (Session Z — Cashbook, Phase 1).

Lives in the dedicated cashier group (CASHIER_GROUP_CHAT_ID). Two flows
triggered from a pinned bot message with two inline buttons:

    /qabul → "💰 Klientdan pul qabul qilish" (cash_direct, more frequent)
           → "📥 Agentdan pul qabul qilish"  (confirm or reject queued
                                              agent submissions)

State is per-user, so Aunt and Uncle can run flows in parallel without
crossing wires. /bekor cancels at any step.

Notifications fire from this handler directly via bot.send_message:
    cash_direct confirmed → notify the client
    pending → confirmed   → notify the client
    pending → rejected    → notify the submitter (agent)
"""
from __future__ import annotations

import logging
from typing import Optional

from aiogram import Router, F, Bot, types
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
    Message,
)

from bot.shared import (
    CASHIER_GROUP_CHAT_ID,
    is_cashier_or_admin,
    is_cashier_or_admin_cb,
    get_user_role,
    get_db,
    html_escape,
)
from backend.services.payment_intake import (
    insert_intake_raw,
    create_intake_payment,
    lookup_client_debt,
    check_recent_duplicate,
    confirm_payment,
    summarize_today_intake,
    reject_payment,
    get_payment,
    list_pending_for_cashier,
    resolve_client_telegram_ids,
    admin_cancel_payment,
)
from backend.services.client_search import search_clients
from bot.shared import is_admin, is_admin_cb

logger = logging.getLogger(__name__)
router = Router(name="cashier")


# ── States ──────────────────────────────────────────────────────────

class CashierFlow(StatesGroup):
    # Klientdan pul qabul qilish (cash_direct: cashier records walk-in)
    direct_search = State()
    direct_uzs = State()
    direct_uzs_confirm = State()
    direct_usd = State()
    direct_usd_confirm = State()
    direct_confirm_dup = State()
    # Agentdan pul qabul qilish (confirm/reject queued submissions)
    queue = State()
    queue_reject_reason = State()


# ── Helpers ─────────────────────────────────────────────────────────

def _fmt_uzs(n) -> str:
    return f"{round(float(n or 0)):,}".replace(",", " ") + " so'm"


def _fmt_usd(n) -> str:
    return f"{float(n or 0):,.2f} $"


def _fmt_amount(amount, currency: str) -> str:
    return _fmt_uzs(amount) if currency == "UZS" else _fmt_usd(amount)


def _is_cashier_chat(message_or_cb) -> bool:
    """The FSM only runs in the configured cashier group. If the env var
    isn't set yet we stay inert (no group → no flow)."""
    if not CASHIER_GROUP_CHAT_ID:
        return False
    chat = getattr(message_or_cb, "chat", None) or (
        getattr(message_or_cb, "message", None) and message_or_cb.message.chat
    )
    return chat is not None and chat.id == CASHIER_GROUP_CHAT_ID


def _menu_keyboard() -> InlineKeyboardMarkup:
    """One button per row — wide, easy hit target. Direct first (more frequent)."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💰 Klientdan", callback_data="cashier:menu_direct")],
            [InlineKeyboardButton(text="📥 Agentdan",  callback_data="cashier:menu_queue")],
        ]
    )


def _cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="❌ Bekor", callback_data="cashier:cancel"),
        ]]
    )


def _amount_keyboard(currency: str) -> InlineKeyboardMarkup:
    """Yo'q (skip the currency) + cancel. One per row for big touch targets."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"⏭ Yo'q", callback_data=f"cashier:skip_{currency.lower()}")],
            [InlineKeyboardButton(text="❌ Bekor", callback_data="cashier:cancel")],
        ]
    )


def _confirm_amount_keyboard(currency: str) -> InlineKeyboardMarkup:
    """After typing an amount: Ha / O'zgartirish / Bekor."""
    cur = currency.lower()
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Ha, davom etamiz", callback_data=f"cashier:amt_ok_{cur}")],
            [InlineKeyboardButton(text="✏️ O'zgartirish",    callback_data=f"cashier:amt_edit_{cur}")],
            [InlineKeyboardButton(text="❌ Bekor",            callback_data="cashier:cancel")],
        ]
    )


def _dup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Ha, davom etamiz", callback_data="cashier:dup_yes")],
            [InlineKeyboardButton(text="❌ Yo'q, bekor qilish", callback_data="cashier:dup_no")],
        ]
    )


def _parse_amount(text: str) -> Optional[float]:
    """Accept '500000', '500 000', '500,000', '1.5' (USD). Returns None if
    unparseable or non-positive."""
    if not text:
        return None
    cleaned = text.replace(" ", "").replace(",", "").replace("'", "").strip()
    try:
        v = float(cleaned)
    except ValueError:
        return None
    if v <= 0:
        return None
    return v


# ── Entry: /qabul + /bekor ──────────────────────────────────────────

@router.message(Command("qabul"))
async def cmd_qabul(message: Message, state: FSMContext):
    """Open the cashier menu. Cashier group only; non-cashiers in any
    group get silently ignored to keep noise low."""
    if not _is_cashier_chat(message):
        return
    if not is_cashier_or_admin(message):
        return
    await state.clear()
    await message.answer(
        "💼 <b>Kassa</b>",
        parse_mode="HTML",
        reply_markup=_menu_keyboard(),
    )


@router.message(Command("bugun"))
async def cmd_bugun(message: Message, state: FSMContext):
    """Today's intake summary — count, totals, by channel, top clients,
    pending queue size. Cashier group only."""
    if not _is_cashier_chat(message):
        return
    if not is_cashier_or_admin(message):
        return
    conn = get_db()
    try:
        summary = summarize_today_intake(conn)
    finally:
        conn.close()
    await message.answer(_render_summary(summary), parse_mode="HTML")


def _render_summary(s: dict) -> str:
    """Format the daily intake summary as an HTML message. Used by both
    /bugun (on-demand) and the 18:00 auto-post."""
    date = s.get("date") or ""
    n = s.get("total_count", 0)
    uzs = s.get("uzs_total", 0.0)
    usd = s.get("usd_total", 0.0)
    pending = s.get("pending_count", 0)

    if n == 0:
        body = f"💼 <b>Bugungi to'lovlar — {date}</b>\n\n📭 Hozircha qabul qilingan to'lov yo'q."
        if pending:
            body += f"\n\n⏳ Kutilayotgan: <b>{pending} ta</b> agent yuboruvi tasdiq kutmoqda."
        return body

    lines = [
        f"💼 <b>Bugungi to'lovlar — {date}</b>",
        "",
        f"📥 Jami: <b>{n} ta</b>",
        f"💵 So'm: <b>{_fmt_uzs(uzs)}</b>",
        f"💵 USD: <b>{_fmt_usd(usd)}</b>",
    ]

    by_ch = s.get("by_channel") or {}
    if by_ch:
        lines.append("")
        lines.append("<b>Kanal bo'yicha:</b>")
        ch_label = {
            "cash_direct": "Klientdan",
            "cash_via_agent": "Agentdan",
            "p2p": "P2P",
        }
        for ch, label in ch_label.items():
            data = by_ch.get(ch)
            if not data:
                continue
            parts = []
            if data["uzs"] > 0:
                parts.append(_fmt_uzs(data["uzs"]))
            if data["usd"] > 0:
                parts.append(_fmt_usd(data["usd"]))
            lines.append(f"• {label}: {data['count']} ta — {' + '.join(parts) if parts else '0'}")

    top = s.get("top_clients") or []
    if top:
        lines.append("")
        lines.append("<b>Top mijozlar:</b>")
        for i, c in enumerate(top, 1):
            parts = []
            if c["uzs"] > 0:
                parts.append(_fmt_uzs(c["uzs"]))
            if c["usd"] > 0:
                parts.append(_fmt_usd(c["usd"]))
            lines.append(f"{i}. {html_escape(c['name'])} — {' + '.join(parts) if parts else '—'}")

    if pending:
        lines.append("")
        lines.append(f"⏳ Kutilayotgan: <b>{pending} ta</b> agent yuboruvi tasdiq kutmoqda.")

    return "\n".join(lines)


# ── /cashbook (admin) — list non-rejected intake_payments + cancel ─

def _cashbook_render_rows(rows):
    """Build the admin cashbook list message + inline keyboard. Each row
    has a small "✖ Bekor #N" button so admin can soft-cancel."""
    if not rows:
        return "🧾 <b>Cashbook — kutilayotgan / tasdiqlangan to'lov yo'q</b>", None
    lines = [f"🧾 <b>Cashbook — oxirgi {len(rows)} ta yozuv</b>\n"]
    kb_rows = []
    for r in rows:
        cname = (r.get("client_id_1c") or r.get("client_name") or f"ID {r['client_id']}")[:30]
        amt = _fmt_amount(r["amount"], r["currency"])
        status_icon = {
            "pending_handover": "⏳",
            "pending_review":   "🔍",
            "confirmed":        "✅",
        }.get(r["status"], "•")
        lines.append(
            f"{status_icon} #{r['id']} — <b>{html_escape(cname)}</b> · {amt} "
            f"<i>{r['status']}</i>"
        )
        kb_rows.append([InlineKeyboardButton(
            text=f"✖ Bekor #{r['id']} — {amt}",
            callback_data=f"cashier:admin_cancel_{r['id']}",
        )])
    kb_rows.append([InlineKeyboardButton(text="❌ Yopish", callback_data="cashier:admin_close")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=kb_rows)


def _cashbook_recent_rows(conn, limit: int = 15):
    """Recent non-rejected intake_payments (any status), newest first."""
    rows = conn.execute(
        """SELECT ip.id, ip.client_id, ip.amount, ip.currency, ip.status,
                  ip.submitted_at, ac.name AS client_name, ac.client_id_1c
           FROM intake_payments ip
           LEFT JOIN allowed_clients ac ON ac.id = ip.client_id
           WHERE ip.status != 'rejected'
           ORDER BY ip.submitted_at DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


@router.message(Command("cashbook"))
async def cmd_cashbook(message: Message):
    """Admin cashbook — recent intake_payments with cancel buttons.
    Works wherever the user is_admin (admin/daily/inventory groups + DM)."""
    if not is_admin(message):
        return
    conn = get_db()
    try:
        rows = _cashbook_recent_rows(conn, limit=15)
    finally:
        conn.close()
    text, kb = _cashbook_render_rows(rows)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("cashier:admin_cancel_"))
async def cb_admin_cancel(cb: CallbackQuery, bot: Bot):
    if not is_admin_cb(cb):
        await cb.answer("Faqat admin", show_alert=True)
        return
    try:
        payment_id = int(cb.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri ID", show_alert=True)
        return
    conn = get_db()
    try:
        try:
            row = admin_cancel_payment(conn, payment_id, cb.from_user.id, "admin_cancelled_via_bot")
        except ValueError as e:
            conn.rollback()
            await cb.answer(str(e), show_alert=True)
            return
        conn.commit()
        # Re-render the list with the row removed
        rows = _cashbook_recent_rows(conn, limit=15)
    finally:
        conn.close()
    text, kb = _cashbook_render_rows(rows)
    try:
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        # Edit may fail (e.g., message too old); fall back to a fresh post
        await cb.message.answer(text, parse_mode="HTML", reply_markup=kb)
    cname = row.get("client_id_1c") or row.get("client_name") or ""
    await cb.answer(
        f"✖ Bekor qilindi: #{row['id']} — {_fmt_amount(row['amount'], row['currency'])} ({cname})"[:200]
    )


@router.callback_query(F.data == "cashier:admin_close")
async def cb_admin_close(cb: CallbackQuery):
    if not is_admin_cb(cb):
        await cb.answer()
        return
    try:
        await cb.message.delete()
    except Exception:
        pass
    await cb.answer()


@router.message(Command("bekor"))
async def cmd_bekor(message: Message, state: FSMContext):
    if not _is_cashier_chat(message):
        return
    cur = await state.get_state()
    if cur is None:
        return
    await state.clear()
    await message.answer("❌ Bekor qilindi. Yangi sessiya uchun /qabul.")


# ── Cancel callback (works in any state) ────────────────────────────

@router.callback_query(F.data == "cashier:cancel")
async def cb_cancel(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await state.clear()
    try:
        await cb.message.edit_text("❌ Bekor qilindi. /qabul — yangidan boshlash.")
    except Exception:
        pass
    await cb.answer()


# ── Menu → flow start ───────────────────────────────────────────────

@router.callback_query(F.data == "cashier:menu_direct")
async def cb_menu_direct(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb) or not is_cashier_or_admin_cb(cb):
        await cb.answer()
        return
    await state.set_state(CashierFlow.direct_search)
    await state.update_data(channel="cash_direct", submitter=cb.from_user.id)
    await cb.message.answer(
        "🔎 Mijoz nomi:",
        reply_markup=_cancel_keyboard(),
    )
    await cb.answer()


@router.callback_query(F.data == "cashier:menu_queue")
async def cb_menu_queue(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb) or not is_cashier_or_admin_cb(cb):
        await cb.answer()
        return
    await state.set_state(CashierFlow.queue)
    await _render_queue(cb.message, state)
    await cb.answer()


# ── Flow 1: Klientdan pul qabul qilish ──────────────────────────────

@router.message(CashierFlow.direct_search, F.text)
async def direct_search_name(message: Message, state: FSMContext):
    if not _is_cashier_chat(message):
        return
    q = (message.text or "").strip()
    if not q:
        await message.answer("Mijoz nomini yuboring.", reply_markup=_cancel_keyboard())
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
            callback_data=f"cashier:pick_{c['id']}",
        )])
    rows.append([InlineKeyboardButton(text="❌ Bekor", callback_data="cashier:cancel")])
    await message.answer(
        f"<b>«{html_escape(q)}»</b>:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


@router.callback_query(CashierFlow.direct_search, F.data.startswith("cashier:pick_"))
async def direct_pick_client(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
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
            "SELECT id, name, client_id_1c FROM allowed_clients WHERE id = ?",
            (client_id,),
        ).fetchone()
        if not row:
            await cb.answer("Mijoz topilmadi", show_alert=True)
            return
        debt = lookup_client_debt(conn, client_id)
    finally:
        conn.close()
    await state.update_data(
        client_id=row["id"],
        client_name=row["client_id_1c"] or row["name"],
    )
    await state.set_state(CashierFlow.direct_uzs)
    debt_line = (
        f"📊 Qarz: <b>{_fmt_uzs(debt['uzs'])}</b> · <b>{_fmt_usd(debt['usd'])}</b>"
        if (debt["uzs"] or debt["usd"])
        else "✅ Qarz yo'q"
    )
    await cb.message.answer(
        f"👤 <b>{html_escape(row['client_id_1c'] or row['name'] or '')}</b>\n"
        f"{debt_line}\n\n"
        f"💵 <b>So'm</b> miqdori:",
        parse_mode="HTML",
        reply_markup=_amount_keyboard("UZS"),
    )
    await cb.answer()


async def _ask_usd(target, state: FSMContext):
    """Helper: prompt the user for the USD amount. `target` is whatever has
    a working `.answer(...)` (a Message or cb.message)."""
    await state.set_state(CashierFlow.direct_usd)
    await target.answer(
        "💵 <b>USD</b> miqdori:",
        parse_mode="HTML",
        reply_markup=_amount_keyboard("USD"),
    )


@router.message(CashierFlow.direct_uzs, F.text)
async def direct_uzs_amount(message: Message, state: FSMContext):
    if not _is_cashier_chat(message):
        return
    amount = _parse_amount(message.text or "")
    if amount is None:
        await message.answer(
            "Raqam kiriting (masalan: 500000) yoki «Yo'q».",
            reply_markup=_amount_keyboard("UZS"),
        )
        return
    await state.update_data(uzs_amount=amount)
    await state.set_state(CashierFlow.direct_uzs_confirm)
    await message.answer(
        f"💵 So'm: <b>{_fmt_uzs(amount)}</b>\n\nTo'g'rimi?",
        parse_mode="HTML",
        reply_markup=_confirm_amount_keyboard("UZS"),
    )


@router.callback_query(CashierFlow.direct_uzs, F.data == "cashier:skip_uzs")
async def direct_skip_uzs(cb: CallbackQuery, state: FSMContext):
    """Skip UZS (Yo'q). No confirmation needed — there's nothing to confirm."""
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await state.update_data(uzs_amount=0.0)
    await _ask_usd(cb.message, state)
    await cb.answer()


@router.callback_query(CashierFlow.direct_uzs_confirm, F.data == "cashier:amt_ok_uzs")
async def direct_uzs_confirm_ok(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await _ask_usd(cb.message, state)
    await cb.answer()


@router.callback_query(CashierFlow.direct_uzs_confirm, F.data == "cashier:amt_edit_uzs")
async def direct_uzs_confirm_edit(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await state.update_data(uzs_amount=None)
    await state.set_state(CashierFlow.direct_uzs)
    await cb.message.answer(
        "💵 <b>So'm</b> miqdori (qaytadan):",
        parse_mode="HTML",
        reply_markup=_amount_keyboard("UZS"),
    )
    await cb.answer()


@router.message(CashierFlow.direct_usd, F.text)
async def direct_usd_amount(message: Message, state: FSMContext):
    if not _is_cashier_chat(message):
        return
    amount = _parse_amount(message.text or "")
    if amount is None:
        await message.answer(
            "Raqam kiriting (masalan: 200) yoki «Yo'q».",
            reply_markup=_amount_keyboard("USD"),
        )
        return
    await state.update_data(usd_amount=amount)
    await state.set_state(CashierFlow.direct_usd_confirm)
    await message.answer(
        f"💵 USD: <b>{_fmt_usd(amount)}</b>\n\nTo'g'rimi?",
        parse_mode="HTML",
        reply_markup=_confirm_amount_keyboard("USD"),
    )


@router.callback_query(CashierFlow.direct_usd, F.data == "cashier:skip_usd")
async def direct_skip_usd(cb: CallbackQuery, state: FSMContext, bot: Bot):
    """Skip USD (Yo'q). Goes straight to finalize/dup-check."""
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await state.update_data(usd_amount=0.0)
    await _direct_finalize_or_dup(cb.message, state, bot)
    await cb.answer()


@router.callback_query(CashierFlow.direct_usd_confirm, F.data == "cashier:amt_ok_usd")
async def direct_usd_confirm_ok(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await _direct_finalize_or_dup(cb.message, state, bot)
    await cb.answer()


@router.callback_query(CashierFlow.direct_usd_confirm, F.data == "cashier:amt_edit_usd")
async def direct_usd_confirm_edit(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await state.update_data(usd_amount=None)
    await state.set_state(CashierFlow.direct_usd)
    await cb.message.answer(
        "💵 <b>USD</b> miqdori (qaytadan):",
        parse_mode="HTML",
        reply_markup=_amount_keyboard("USD"),
    )
    await cb.answer()


async def _direct_finalize_or_dup(message: Message, state: FSMContext, bot: Bot):
    """After both currencies collected: validate at least one >0, check
    soft-dedupe, then either prompt for confirmation or finalize."""
    data = await state.get_data()
    uzs = float(data.get("uzs_amount") or 0)
    usd = float(data.get("usd_amount") or 0)
    if uzs <= 0 and usd <= 0:
        await state.clear()
        await message.answer(
            "❌ Iltimos, kamida bittasini kiriting (so'm yoki USD).\n"
            "/qabul — yangidan boshlash.",
        )
        return

    # Dedupe check across each non-zero currency.
    conn = get_db()
    try:
        dups = []
        if uzs > 0:
            d = check_recent_duplicate(conn, data["client_id"], uzs, "UZS")
            if d:
                dups.append(("UZS", uzs, d))
        if usd > 0:
            d = check_recent_duplicate(conn, data["client_id"], usd, "USD")
            if d:
                dups.append(("USD", usd, d))
    finally:
        conn.close()

    if dups:
        await state.set_state(CashierFlow.direct_confirm_dup)
        lines = ["⚠️ <b>Yaqinda shunday to'lov bor:</b>"]
        for cur, amt, d in dups:
            lines.append(
                f"• {_fmt_amount(amt, cur)} — {d['status']} "
                f"({d['submitted_at']})"
            )
        lines.append("\nDavom etamizmi?")
        await message.answer(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=_dup_keyboard(),
        )
        return

    await _direct_finalize(message, state, bot)


@router.callback_query(CashierFlow.direct_confirm_dup, F.data == "cashier:dup_yes")
async def direct_dup_yes(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await _direct_finalize(cb.message, state, bot)
    await cb.answer()


@router.callback_query(CashierFlow.direct_confirm_dup, F.data == "cashier:dup_no")
async def direct_dup_no(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await state.clear()
    await cb.message.answer("❌ Bekor qilindi. /qabul — yangidan boshlash.")
    await cb.answer()


async def _direct_finalize(message: Message, state: FSMContext, bot: Bot):
    """Insert audit + intake_payments rows (one per currency leg), notify
    the client, clear state."""
    data = await state.get_data()
    client_id = data["client_id"]
    client_name = data.get("client_name") or ""
    uzs = float(data.get("uzs_amount") or 0)
    usd = float(data.get("usd_amount") or 0)
    # submitter is recorded in state when the flow starts, so it survives
    # callback paths where message.from_user is the bot itself.
    submitter_id = data.get("submitter") or 0
    role = "cashier"

    conn = get_db()
    payments_created = []
    try:
        for cur_code, amt in (("UZS", uzs), ("USD", usd)):
            if amt <= 0:
                continue
            raw_id = insert_intake_raw(
                conn,
                submitter_telegram_id=submitter_id,
                submitter_role=role,
                payload={
                    "channel": "cash_direct",
                    "client_id": client_id,
                    "amount": amt,
                    "currency": cur_code,
                },
            )
            pid = create_intake_payment(
                conn,
                raw_id=raw_id,
                client_id=client_id,
                amount=amt,
                currency=cur_code,
                channel="cash_direct",
                status="confirmed",
                submitter_telegram_id=submitter_id,
                submitter_role=role,
                confirmed_by_telegram_id=submitter_id,
            )
            payments_created.append((pid, cur_code, amt))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception("direct finalize failed")
        await message.answer(f"❌ Saqlashda xatolik: {html_escape(str(e))}")
        await state.clear()
        return
    finally:
        conn.close()

    # Confirmation to the cashier in the group
    legs = " + ".join(_fmt_amount(amt, cur) for _, cur, amt in payments_created)
    await message.answer(
        f"✅ Qabul qilindi: <b>{legs}</b>\n"
        f"👤 {html_escape(client_name)}",
        parse_mode="HTML",
    )

    # Notify the client (best-effort, swallow errors)
    await _notify_client_confirmed(bot, client_id, client_name, payments_created)
    await state.clear()


# ── Flow 2: Agentdan pul qabul qilish ───────────────────────────────

async def _render_queue(message: Message, state: FSMContext):
    conn = get_db()
    try:
        pending = list_pending_for_cashier(conn, limit=20)
    finally:
        conn.close()
    if not pending:
        await message.answer(
            "✅ Hozir kutilayotgan to'lov yo'q.\n\n"
            "Agent panel orqali yangi yuborilsa, shu yerda ko'rinadi.",
            reply_markup=_cancel_keyboard(),
        )
        return
    lines = [f"📥 <b>Kutilayotgan to'lovlar ({len(pending)}):</b>\n"]
    rows = []
    for p in pending[:10]:
        cname = (p.get("client_id_1c") or p.get("client_name") or f"ID {p['client_id']}")[:30]
        amt = _fmt_amount(p["amount"], p["currency"])
        ch_icon = "📦" if p["channel"] == "cash_via_agent" else "💳"
        lines.append(
            f"{ch_icon} <b>{html_escape(cname)}</b> — {amt} "
            f"(agent: <code>{p.get('handover_agent_id') or p['submitter_telegram_id']}</code>)"
        )
        rows.append([InlineKeyboardButton(
            text=f"#{p['id']} {cname[:20]} — {amt}",
            callback_data=f"cashier:pay_{p['id']}",
        )])
    rows.append([InlineKeyboardButton(text="❌ Bekor qilish", callback_data="cashier:cancel")])
    await message.answer(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


@router.callback_query(CashierFlow.queue, F.data.startswith("cashier:pay_"))
async def queue_pick(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    try:
        payment_id = int(cb.data.split("_", 1)[1])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri tanlov", show_alert=True)
        return
    conn = get_db()
    try:
        try:
            p = get_payment(conn, payment_id)
        except ValueError:
            await cb.answer("Topilmadi", show_alert=True)
            return
    finally:
        conn.close()
    if p["status"] not in ("pending_handover", "pending_review"):
        await cb.answer(f"Allaqachon {p['status']}", show_alert=True)
        return
    await state.update_data(active_payment_id=payment_id)
    cname = p.get("client_id_1c") or p.get("client_name") or f"ID {p['client_id']}"
    detail = (
        f"💰 <b>To'lov #{p['id']}</b>\n"
        f"👤 {html_escape(cname)}\n"
        f"💵 {_fmt_amount(p['amount'], p['currency'])}\n"
        f"🛣 Kanal: <code>{p['channel']}</code>\n"
        f"👨‍💼 Agent: <code>{p.get('handover_agent_id') or p['submitter_telegram_id']}</code>\n"
        f"🕒 {p['submitted_at']}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"cashier:confirm_{payment_id}"),
            InlineKeyboardButton(text="❌ Rad etish", callback_data=f"cashier:reject_{payment_id}"),
        ],
        [InlineKeyboardButton(text="↩️ Orqaga", callback_data="cashier:queue_back")],
    ])
    if p.get("screenshot_file_id"):
        try:
            await cb.message.answer_photo(p["screenshot_file_id"], caption=detail, parse_mode="HTML", reply_markup=kb)
        except Exception:
            await cb.message.answer(detail + "\n\n⚠️ Screenshot ko'rsatib bo'lmadi.", parse_mode="HTML", reply_markup=kb)
    else:
        await cb.message.answer(detail, parse_mode="HTML", reply_markup=kb)
    await cb.answer()


@router.callback_query(CashierFlow.queue, F.data == "cashier:queue_back")
async def queue_back(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await _render_queue(cb.message, state)
    await cb.answer()


@router.callback_query(CashierFlow.queue, F.data.startswith("cashier:confirm_"))
async def queue_confirm(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    try:
        payment_id = int(cb.data.split("_", 1)[1])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri tanlov", show_alert=True)
        return
    confirmer_id = cb.from_user.id
    conn = get_db()
    try:
        try:
            row = confirm_payment(conn, payment_id, confirmer_id)
        except ValueError as e:
            conn.rollback()
            await cb.answer(str(e), show_alert=True)
            return
        conn.commit()
    finally:
        conn.close()
    cname = row.get("client_id_1c") or row.get("client_name") or ""
    await cb.message.answer(
        f"✅ Tasdiqlandi: #{row['id']} — {_fmt_amount(row['amount'], row['currency'])} "
        f"({html_escape(cname)})",
        parse_mode="HTML",
    )
    await _notify_client_confirmed(
        bot,
        row["client_id"],
        cname,
        [(row["id"], row["currency"], row["amount"])],
    )
    await cb.answer("Tasdiqlandi")


@router.callback_query(CashierFlow.queue, F.data.startswith("cashier:reject_"))
async def queue_reject_start(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    try:
        payment_id = int(cb.data.split("_", 1)[1])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri tanlov", show_alert=True)
        return
    await state.update_data(reject_payment_id=payment_id)
    await state.set_state(CashierFlow.queue_reject_reason)
    await cb.message.answer(
        "✏️ Rad etish sababini yuboring (qisqa matn):",
        reply_markup=_cancel_keyboard(),
    )
    await cb.answer()


@router.message(CashierFlow.queue_reject_reason, F.text)
async def queue_reject_reason(message: Message, state: FSMContext, bot: Bot):
    if not _is_cashier_chat(message):
        return
    reason = (message.text or "").strip()
    if len(reason) < 2:
        await message.answer("Sababni biroz batafsilroq yuboring.", reply_markup=_cancel_keyboard())
        return
    data = await state.get_data()
    payment_id = data.get("reject_payment_id")
    if not payment_id:
        await state.clear()
        await message.answer("❌ Sessiya topilmadi. /qabul")
        return
    rejecter_id = message.from_user.id
    conn = get_db()
    try:
        try:
            row = reject_payment(conn, payment_id, rejecter_id, reason)
        except ValueError as e:
            conn.rollback()
            await message.answer(f"❌ {html_escape(str(e))}")
            await state.clear()
            return
        conn.commit()
    finally:
        conn.close()
    cname = row.get("client_id_1c") or row.get("client_name") or ""
    await message.answer(
        f"❌ Rad etildi: #{row['id']} — {_fmt_amount(row['amount'], row['currency'])} "
        f"({html_escape(cname)})\nSabab: {html_escape(reason)}",
        parse_mode="HTML",
    )
    # Notify the original submitter
    submitter = row.get("submitter_telegram_id") or row.get("handover_agent_id")
    if submitter:
        try:
            await bot.send_message(
                submitter,
                f"❌ <b>To'lovingiz rad etildi</b>\n"
                f"👤 {html_escape(cname)}\n"
                f"💵 {_fmt_amount(row['amount'], row['currency'])}\n"
                f"📝 Sabab: {html_escape(reason)}",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.warning(f"reject notification to {submitter} failed: {e}")
    await state.clear()


# ── Notifications ───────────────────────────────────────────────────

async def _notify_client_confirmed(
    bot: Bot,
    client_id: int,
    client_name: str,
    legs,
):
    """Send a TG receipt to every approved telegram_id linked to this client
    or its multi-phone siblings. Best-effort — never raises."""
    try:
        conn = get_db()
        try:
            recipients = resolve_client_telegram_ids(conn, client_id)
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"resolve recipients for client {client_id} failed: {e}")
        return
    if not recipients:
        return
    legs_text = ", ".join(_fmt_amount(amt, cur) for _, cur, amt in legs)
    text = (
        f"✅ <b>To'lov qabul qilindi</b>\n"
        f"💵 {legs_text}\n"
        f"👤 {html_escape(client_name)}"
    )
    for tid in recipients:
        try:
            await bot.send_message(tid, text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"client confirm notification to {tid} failed: {e}")
