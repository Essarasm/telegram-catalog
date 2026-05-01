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
import os
import re
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
    assign_supplier,
    attach_agreement,
    attach_transfer_proof,
    confirm_supplier_receipt,
    list_dedicated_cards,
    add_dedicated_card,
    retire_dedicated_card,
    format_card_number,
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


class CardsAdminFlow(StatesGroup):
    """Admin /cards CRUD — adding a new dedicated_card requires the user to
    type the card details on the next message."""
    awaiting_new_card = State()


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


# ── Admin: /cards — manage P2P destination cards ────────────────────

def _render_cards_panel(cards: list):
    """Render the /cards admin panel: list + per-row delete + add button."""
    if not cards:
        text = (
            "💳 <b>Bank kartalari</b>\n\n"
            "<i>Faol karta yo'q.</i> Yangi karta qo'shing."
        )
    else:
        lines = [f"💳 <b>Bank kartalari ({len(cards)} ta faol)</b>", ""]
        for i, c in enumerate(cards, start=1):
            num = format_card_number(c["card_number"])
            full_name = f"{c['holder_first_name']} {c['holder_last_name']}".strip()
            lines.append(f"{i}. <code>{num}</code> — {html_escape(full_name)}")
        text = "\n".join(lines)

    kb_rows = []
    for c in cards:
        full_name = f"{c['holder_first_name']} {c['holder_last_name']}".strip()
        kb_rows.append([
            InlineKeyboardButton(
                text=f"✖ {full_name[:50]}",
                callback_data=f"cards:del_ask:{c['id']}",
            )
        ])
    kb_rows.append([
        InlineKeyboardButton(text="➕ Yangi karta qo'shish", callback_data="cards:add")
    ])
    kb_rows.append([
        InlineKeyboardButton(text="❌ Yopish", callback_data="cards:close")
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


@router.message(Command("cards"))
async def cmd_cards(message: Message, state: FSMContext):
    """Admin: list/add/remove dedicated_cards used as P2P destinations."""
    if not is_admin(message):
        return
    await state.clear()  # Drop any stale CardsAdminFlow state
    conn = get_db()
    try:
        cards = list_dedicated_cards(conn, active_only=True)
    finally:
        conn.close()
    text, kb = _render_cards_panel(cards)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("cards:del_ask:"))
async def cb_cards_del_ask(cb: CallbackQuery):
    """Delete confirmation prompt — replaces buttons with [Yes][No]."""
    if not is_admin_cb(cb):
        await cb.answer("Faqat admin", show_alert=True)
        return
    try:
        card_id = int(cb.data.split(":")[2])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri ID", show_alert=True)
        return
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT card_number, holder_first_name, holder_last_name FROM dedicated_cards WHERE id = ?",
            (card_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        await cb.answer("Karta topilmadi", show_alert=True)
        return
    name = f"{row['holder_first_name']} {row['holder_last_name']}".strip()
    text = (
        f"⚠️ Kartani o'chirilsinmi?\n\n"
        f"<code>{format_card_number(row['card_number'])}</code>\n"
        f"{html_escape(name)}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Ha, o'chir", callback_data=f"cards:del_yes:{card_id}"),
        InlineKeyboardButton(text="❌ Yo'q", callback_data="cards:back"),
    ]])
    try:
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await cb.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("cards:del_yes:"))
async def cb_cards_del_yes(cb: CallbackQuery):
    if not is_admin_cb(cb):
        await cb.answer("Faqat admin", show_alert=True)
        return
    try:
        card_id = int(cb.data.split(":")[2])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri ID", show_alert=True)
        return
    conn = get_db()
    try:
        changed = retire_dedicated_card(conn, card_id)
        cards = list_dedicated_cards(conn, active_only=True)
    finally:
        conn.close()
    text, kb = _render_cards_panel(cards)
    try:
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await cb.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer("✖ O'chirildi" if changed else "Allaqachon faol emas")


@router.callback_query(F.data == "cards:back")
async def cb_cards_back(cb: CallbackQuery):
    if not is_admin_cb(cb):
        await cb.answer()
        return
    conn = get_db()
    try:
        cards = list_dedicated_cards(conn, active_only=True)
    finally:
        conn.close()
    text, kb = _render_cards_panel(cards)
    try:
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass
    await cb.answer()


@router.callback_query(F.data == "cards:close")
async def cb_cards_close(cb: CallbackQuery):
    if not is_admin_cb(cb):
        await cb.answer()
        return
    try:
        await cb.message.delete()
    except Exception:
        pass
    await cb.answer()


@router.callback_query(F.data == "cards:add")
async def cb_cards_add(cb: CallbackQuery, state: FSMContext):
    """Set FSM state and prompt for card details."""
    if not is_admin_cb(cb):
        await cb.answer("Faqat admin", show_alert=True)
        return
    await state.set_state(CardsAdminFlow.awaiting_new_card)
    await state.update_data(panel_chat_id=cb.message.chat.id, panel_message_id=cb.message.message_id)
    prompt = (
        "➕ <b>Yangi karta qo'shish</b>\n\n"
        "Kartani quyidagi formatda yuboring:\n"
        "<code>karta_raqami; Ism; Familiya</code>\n\n"
        "Masalan:\n"
        "<code>8600123412345678; Iskandar; Ibragimov</code>\n\n"
        "<i>Bekor qilish uchun /bekor.</i>"
    )
    try:
        await cb.message.edit_text(prompt, parse_mode="HTML", reply_markup=None)
    except Exception:
        await cb.message.answer(prompt, parse_mode="HTML")
    await cb.answer()


@router.message(CardsAdminFlow.awaiting_new_card, F.text)
async def cb_cards_add_input(message: Message, state: FSMContext):
    """Parse the typed card details and insert."""
    if not is_admin(message):
        return
    text = (message.text or "").strip()
    parts = [p.strip() for p in text.split(";")]
    if len(parts) != 3:
        await message.reply(
            "⚠️ Format noto'g'ri. Misol: <code>8600123412345678; Iskandar; Ibragimov</code>\n"
            "Bekor qilish uchun /bekor.",
            parse_mode="HTML",
        )
        return
    num, first, last = parts
    conn = get_db()
    try:
        try:
            result = add_dedicated_card(
                conn, card_number=num, first=first, last=last
            )
        except ValueError as e:
            await message.reply(f"⚠️ {str(e)[:200]}\nBekor qilish: /bekor.")
            return
        cards = list_dedicated_cards(conn, active_only=True)
    finally:
        conn.close()
    await state.clear()
    label = "qayta faollashtirildi" if result["reactivated"] else "qo'shildi"
    panel_text, panel_kb = _render_cards_panel(cards)
    await message.reply(f"✅ Karta {label} (id={result['id']})")
    await message.answer(panel_text, parse_mode="HTML", reply_markup=panel_kb)


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


# ── Stage 2: legal-entity transfer supplier picker ─────────────────
# Callback data: legaltx:pick:<transfer_id>:<supplier_id>

@router.callback_query(F.data.startswith("legaltx:pick:"))
async def cb_legaltx_pick_supplier(cb: CallbackQuery, bot: Bot):
    """Uncle (or any cashier) picks the supplier for a Stage 1 legal-
    entity transfer request. Atomically sets supplier_id + flips status
    submitted → supplier_assigned + logs event. Edits the original
    notification in place to remove the keyboard and append a footer
    showing which supplier was picked.
    """
    if not is_cashier_or_admin_cb(cb):
        await cb.answer("Faqat kassir/admin", show_alert=True)
        return
    try:
        parts = cb.data.split(":")
        transfer_id = int(parts[2])
        supplier_id = int(parts[3])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri tugma", show_alert=True)
        return

    conn = get_db()
    try:
        try:
            result = assign_supplier(
                conn,
                legal_transfer_id=transfer_id,
                supplier_id=supplier_id,
                actor_telegram_id=cb.from_user.id,
            )
        except ValueError as e:
            await cb.answer(str(e)[:200], show_alert=True)
            return
    finally:
        conn.close()

    supplier_name = result["supplier_name_1c"]
    # Edit the notification in place — remove keyboard, append supplier
    # footer + Stage 3 instruction (uncle replies with agreement file)
    original = cb.message.html_text or cb.message.text or ""
    new_text = (
        original
        + f"\n\n✅ → <b>{html_escape(supplier_name)}</b>"
        + "\n📎 <i>Shartnomani shu xabarga javob qilib yuboring</i>"
    )
    try:
        await cb.message.edit_text(
            new_text, parse_mode="HTML", disable_web_page_preview=True
        )
    except Exception as e:
        logger.warning(f"legaltx pick edit failed: {e}")
        await cb.message.answer(
            f"✅ #{transfer_id} → <b>{html_escape(supplier_name)}</b>\n"
            f"📎 <i>Shartnomani shu xabarga javob qilib yuboring</i>",
            parse_mode="HTML",
        )
    await cb.answer(f"✅ {supplier_name[:50]}")


# ── Stage 3: agreement document upload ──────────────────────────────
# Uncle replies to a Stage 1 notification (in the legal-transfer group)
# with a .docx (or any doc). Bot detects the reply, parses #<id> from
# the original message text, attaches the file_id, and DMs the client
# with the document so they can wire to the supplier.

_LEGALTX_ID_RE = re.compile(r"#(\d+)")


def _fmt_uzs_for_msg(amount: float) -> str:
    n = int(round(amount))
    return f"{n:,}".replace(",", " ")


@router.message(
    F.document
    & F.reply_to_message
    & F.chat.type.in_({"group", "supergroup"})
)
async def cb_legaltx_agreement_upload(message: Message, bot: Bot):
    """Stage 3: agreement file uploaded as a reply to the Stage 1 notification.

    Filters: must be in a group/supergroup (not DM — DMs are Stage 5a's
    territory), reply to one of the bot's own messages, original message
    must contain the legal-transfer header marker AND a #<id> token, and
    the user must have admin/cashier role. Otherwise silently ignored —
    the bot stays out of the way of unrelated documents in the group.
    """
    rt = message.reply_to_message
    if not rt or not rt.from_user or rt.from_user.id != bot.id:
        return
    text = rt.text or rt.caption or ""
    if "yuridik shaxs to'lov" not in text.lower():
        return
    m = _LEGALTX_ID_RE.search(text)
    if not m:
        return
    try:
        transfer_id = int(m.group(1))
    except ValueError:
        return
    if not is_cashier_or_admin(message):
        return  # Silent — not our event

    file_id = message.document.file_id
    file_name = message.document.file_name or "agreement"

    conn = get_db()
    try:
        try:
            result = attach_agreement(
                conn,
                legal_transfer_id=transfer_id,
                agreement_url=f"tg://{file_id}",
                actor_telegram_id=message.from_user.id,
            )
        except ValueError as e:
            await message.reply(f"⚠️ {str(e)[:200]}")
            return
        client_tg_ids = resolve_client_telegram_ids(conn, result["client_id"])
    finally:
        conn.close()

    # Edit the original notification: replace the Stage 3 prompt line with
    # a "Shartnoma yuklandi" confirmation + Stage 4 prompt for the cashier.
    original_html = rt.html_text or rt.text or ""
    # Strip the old Stage 3 prompt if present so we don't accumulate clutter
    original_html = original_html.replace(
        "📎 <i>Shartnomani shu xabarga javob qilib yuboring</i>", ""
    ).rstrip()
    new_text = (
        original_html
        + "\n📎 ✅ <b>Shartnoma yuklandi</b>"
        + "\n💰 <i>Klient to'lovni amalga oshirgandan keyin chek shu xabarga javob qilsin</i>"
    )
    try:
        await rt.edit_text(new_text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f"agreement upload edit failed: {e}")

    await message.reply(
        f"✅ <b>#{transfer_id}</b> Shartnoma qabul qilindi — klientga yuborildi.",
        parse_mode="HTML",
    )

    # DM client(s) — siblings (multiple phones same client_id_1c) all get a copy
    if not client_tg_ids:
        await message.reply(
            f"⚠️ Klient <b>#{result['client_id']}</b> uchun bog'langan Telegram foydalanuvchi topilmadi. "
            f"Shartnomani qo'lda yuboring.",
            parse_mode="HTML",
        )
        return

    client_caption = (
        f"🏛 <b>Yuridik shaxs to'lov #{transfer_id}</b>\n\n"
        f"Shartnoma keldi. Iltimos, faylni o'qing va to'lovni amalga oshiring.\n\n"
        f"💰 Summa: <b>{_fmt_uzs_for_msg(result['amount_uzs'])} UZS</b>\n"
        f"🏪 Yetkazib beruvchi: <b>{html_escape(result['supplier_name_1c'] or '')}</b>\n"
        f"🏢 Sizning firma: <b>{html_escape(result['legal_entity_name'] or '')}</b>\n\n"
        f"<i>To'lov amalga oshgandan keyin bank chekini bot orqali yuboring.</i>"
    )
    sent_count = 0
    for client_tg in client_tg_ids:
        try:
            await bot.send_document(
                client_tg, file_id, caption=client_caption, parse_mode="HTML"
            )
            sent_count += 1
        except Exception as e:
            logger.warning(
                f"Failed to send agreement #{transfer_id} to client tg={client_tg}: {e}"
            )
    if sent_count == 0:
        await message.reply(
            f"⚠️ Klientga yuborib bo'lmadi (botni bloklagan bo'lishi mumkin). Qo'lda yuboring."
        )


# ── Stage 5a: client uploads bank-transfer proof in DM ────────────────
# Client receives the agreement DM in Stage 3, makes the wire to the
# supplier, then replies in their DM to that agreement message with a
# photo (bank screenshot) or PDF (receipt). Bot detects, attaches the
# proof to the legal_transfer row, advances status, and forwards the
# proof to the legal-transfer group with a "supplier confirmed?" button
# for uncle (Stage 5b — wired in next commit).

@router.message(
    (F.photo | F.document)
    & F.reply_to_message
    & (F.chat.type == "private")
)
async def cb_legaltx_transfer_proof_upload(message: Message, bot: Bot):
    """Stage 5a: client replies to the agreement DM with bank-transfer proof.

    Silently ignored unless: reply target is one of the bot's own messages,
    its caption contains the legal-transfer header AND a #<id> token, the
    user is linked to that transfer's client (or admin), and the transfer
    is currently in agreement_received status.
    """
    rt = message.reply_to_message
    if not rt or not rt.from_user or rt.from_user.id != bot.id:
        return
    caption = rt.caption or rt.text or ""
    if "yuridik shaxs to'lov" not in caption.lower():
        return
    m = _LEGALTX_ID_RE.search(caption)
    if not m:
        return
    try:
        transfer_id = int(m.group(1))
    except ValueError:
        return

    # Pick the file_id (photos arrive as a list of sizes; take the largest)
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.document:
        file_id = message.document.file_id
    else:
        return

    conn = get_db()
    try:
        row = conn.execute(
            """SELECT lt.client_id, lt.status FROM legal_transfers lt WHERE lt.id = ?""",
            (transfer_id,),
        ).fetchone()
        if not row:
            await message.reply(f"⚠️ #{transfer_id} so'rov topilmadi.")
            return

        # Permission: uploader must be linked to this client OR be admin.
        # Ulu (admin) can upload on behalf during testing or for clients
        # who sent proof via WhatsApp.
        from backend.services.roles import role_in
        is_admin_role = role_in(conn, message.from_user.id, {"admin"})
        is_client_user = bool(
            conn.execute(
                """SELECT 1 FROM users
                    WHERE telegram_id = ? AND client_id = ?
                      AND COALESCE(is_approved, 0) = 1""",
                (message.from_user.id, row["client_id"]),
            ).fetchone()
        )
        if not (is_admin_role or is_client_user):
            await message.reply(
                f"⚠️ Sizda #{transfer_id} uchun chek yuborish ruxsati yo'q."
            )
            return

        try:
            result = attach_transfer_proof(
                conn,
                legal_transfer_id=transfer_id,
                transfer_proof_url=f"tg://{file_id}",
                actor_telegram_id=message.from_user.id,
            )
        except ValueError as e:
            await message.reply(f"⚠️ {str(e)[:200]}")
            return
    finally:
        conn.close()

    # Reply to the client in DM
    await message.reply(
        f"✅ Chek qabul qilindi (<b>#{transfer_id}</b>). "
        f"Kassir tekshirib yetkazib beruvchi bilan tasdiqlashi kutilmoqda.",
        parse_mode="HTML",
    )

    # Forward the proof to the legal-transfer group with a Stage-5b button
    group_id = (
        os.getenv("LEGAL_TRANSFER_GROUP_CHAT_ID", "")
        or os.getenv("CASHIER_GROUP_CHAT_ID", "")
    )
    if not group_id:
        logger.warning("No LEGAL_TRANSFER_GROUP_CHAT_ID — skipping Stage 5a forward")
        return

    g_caption = (
        f"💰 <b>#{transfer_id}</b> Klient bank chekini yubordi\n"
        f"🏪 → <b>{html_escape(result['supplier_name_1c'] or '')}</b>\n"
        f"💰 {_fmt_uzs_for_msg(result['amount_uzs'])} UZS\n"
        f"🏢 {html_escape(result['legal_entity_name'] or '')}\n\n"
        f"<i>Yetkazib beruvchi bilan tasdiqlanggandan keyin tugmani bosing:</i>"
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Yetkazib beruvchi pulni oldi",
                    callback_data=f"legaltx:confirm:{transfer_id}",
                )
            ]
        ]
    )

    try:
        if message.photo:
            await bot.send_photo(
                group_id, file_id, caption=g_caption,
                parse_mode="HTML", reply_markup=keyboard,
            )
        else:
            await bot.send_document(
                group_id, file_id, caption=g_caption,
                parse_mode="HTML", reply_markup=keyboard,
            )
    except Exception as e:
        logger.error(f"Stage 5a group forward failed for #{transfer_id}: {e}")
        await message.reply(
            "⚠️ Chek saqlandi, lekin guruhga yuborib bo'lmadi. Adminga xabar bering."
        )


# ── P2P inline confirm/reject on the cashier-group photo ────────────
# Callback data: p2p:confirm:<id> | p2p:reject:<id>
#
# Default reject reason "Rad etildi (kassir tomonidan)" is used for the
# inline button. Cashier wanting a specific reason can use the existing
# /qabul → Agentdan flow (it asks for reason via FSM).

@router.callback_query(F.data.startswith("p2p:confirm:"))
async def cb_p2p_confirm(cb: CallbackQuery, bot: Bot):
    if not is_cashier_or_admin_cb(cb):
        await cb.answer("Faqat kassir/admin", show_alert=True)
        return
    try:
        pid = int(cb.data.split(":")[2])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri ID", show_alert=True)
        return
    conn = get_db()
    try:
        try:
            payment = confirm_payment(conn, pid, cb.from_user.id)
            conn.commit()
        except ValueError as e:
            conn.rollback()
            await cb.answer(str(e)[:200], show_alert=True)
            return
        client_tg_ids = resolve_client_telegram_ids(conn, payment["client_id"])
    finally:
        conn.close()

    # Edit the photo caption to remove buttons + add confirmation footer
    original = cb.message.html_text or cb.message.caption or ""
    new_caption = original + f"\n\n✅ <b>Tasdiqlandi</b>"
    try:
        await cb.message.edit_caption(
            caption=new_caption, parse_mode="HTML", reply_markup=None
        )
    except Exception as e:
        logger.warning(f"P2P confirm edit failed for #{pid}: {e}")
    await cb.answer(f"✅ #{pid} tasdiqlandi")

    # DM client(s)
    client_msg = (
        f"✅ <b>P2P to'lov tasdiqlandi (#{pid})</b>\n\n"
        f"💰 {_fmt_uzs_for_msg(payment['amount'])} UZS qabul qilindi.\n"
        f"<i>Qarzingiz yangilandi.</i>"
    )
    for tg in client_tg_ids:
        try:
            await bot.send_message(tg, client_msg, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"P2P confirm DM #{pid} → tg={tg} failed: {e}")


@router.callback_query(F.data.startswith("p2p:reject:"))
async def cb_p2p_reject(cb: CallbackQuery, bot: Bot):
    if not is_cashier_or_admin_cb(cb):
        await cb.answer("Faqat kassir/admin", show_alert=True)
        return
    try:
        pid = int(cb.data.split(":")[2])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri ID", show_alert=True)
        return
    default_reason = "Rad etildi (kassir tomonidan)"
    conn = get_db()
    try:
        try:
            payment = reject_payment(conn, pid, cb.from_user.id, default_reason)
            conn.commit()
        except ValueError as e:
            conn.rollback()
            await cb.answer(str(e)[:200], show_alert=True)
            return
        submitter_tg = payment.get("submitter_telegram_id")
    finally:
        conn.close()

    original = cb.message.html_text or cb.message.caption or ""
    new_caption = original + f"\n\n❌ <b>Rad etildi</b>"
    try:
        await cb.message.edit_caption(
            caption=new_caption, parse_mode="HTML", reply_markup=None
        )
    except Exception as e:
        logger.warning(f"P2P reject edit failed for #{pid}: {e}")
    await cb.answer(f"❌ #{pid} rad etildi")

    # Notify submitter (agent or client)
    if submitter_tg:
        try:
            await bot.send_message(
                submitter_tg,
                f"❌ <b>P2P to'lov rad etildi (#{pid})</b>\n\n"
                f"<i>Sabab:</i> {html_escape(default_reason)}\n\n"
                f"Iltimos kassir bilan bog'laning.",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.warning(f"P2P reject DM #{pid} → tg={submitter_tg} failed: {e}")


# ── Stage 5b: uncle taps "supplier confirmed" on the proof message ─────
# Callback data: legaltx:confirm:<transfer_id>

@router.callback_query(F.data.startswith("legaltx:confirm:"))
async def cb_legaltx_supplier_confirm(cb: CallbackQuery, bot: Bot):
    """Uncle confirms (offline call to supplier) that the wire landed.
    Flips status to supplier_confirmed + edits the proof message in place
    + DMs the client a confirmation. Cabinet debt-tile integration is
    deferred to a follow-up commit (see confirm_supplier_receipt docstring).
    """
    if not is_cashier_or_admin_cb(cb):
        await cb.answer("Faqat kassir/admin", show_alert=True)
        return
    try:
        parts = cb.data.split(":")
        transfer_id = int(parts[2])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri tugma", show_alert=True)
        return

    conn = get_db()
    try:
        try:
            result = confirm_supplier_receipt(
                conn,
                legal_transfer_id=transfer_id,
                actor_telegram_id=cb.from_user.id,
            )
        except ValueError as e:
            await cb.answer(str(e)[:200], show_alert=True)
            return
        client_tg_ids = resolve_client_telegram_ids(conn, result["client_id"])
    finally:
        conn.close()

    # Edit the proof message to remove the keyboard + add confirmation footer
    original = cb.message.html_text or cb.message.caption or ""
    new_text = (
        original
        + "\n\n✅ <b>Yetkazib beruvchi pulni oldi</b> — qarz kamaytirildi"
    )
    try:
        # The proof message has the photo/document with caption — use
        # edit_caption rather than edit_text since it's not a text-only msg
        if cb.message.photo or cb.message.document:
            await cb.message.edit_caption(
                caption=new_text, parse_mode="HTML", reply_markup=None
            )
        else:
            await cb.message.edit_text(
                new_text, parse_mode="HTML", reply_markup=None,
                disable_web_page_preview=True,
            )
    except Exception as e:
        logger.warning(f"Stage 5b edit failed for #{transfer_id}: {e}")
        # Fallback: send a fresh confirmation to the group
        await cb.message.answer(
            f"✅ <b>#{transfer_id}</b> Yetkazib beruvchi pulni oldi — qarz kamaytirildi",
            parse_mode="HTML",
        )

    await cb.answer(f"✅ #{transfer_id} tasdiqlandi")

    # DM the client
    if client_tg_ids:
        client_msg = (
            f"✅ <b>To'lov tasdiqlandi (#{transfer_id})</b>\n\n"
            f"💰 {_fmt_uzs_for_msg(result['amount_uzs'])} UZS\n"
            f"🏪 Yetkazib beruvchi: <b>{html_escape(result['supplier_name_1c'] or '')}</b>\n"
            f"🏢 Sizning firma: <b>{html_escape(result['legal_entity_name'] or '')}</b>\n\n"
            f"Yetkazib beruvchi pulni qabul qildi. Qarzingiz kamaytirildi.\n\n"
            f"<i>Keyingi qadam: kabinetingizdan doverennost yuklash kerak (yaqinda).</i>"
        )
        for client_tg in client_tg_ids:
            try:
                await bot.send_message(
                    client_tg, client_msg, parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.warning(
                    f"Stage 5b client DM #{transfer_id} → tg={client_tg} failed: {e}"
                )


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
