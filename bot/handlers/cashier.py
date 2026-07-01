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
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

# Fixed offset — Uzbekistan has no DST. Avoids dependency on tzdata in the
# Railway container.
TASHKENT_TZ = timezone(timedelta(hours=5))


def _now_tashkent_hhmm() -> str:
    return datetime.now(TASHKENT_TZ).strftime("%H:%M")

from aiogram import Router, F, Bot
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
    is_cashier_role_cb,
    get_db,
    html_escape,
    chunk_message,
)
from backend.services.group_config import legal_transfer_target

LEGAL_TRANSFER_GROUP_CHAT_ID = legal_transfer_target()
from backend.services.payment_intake import (
    insert_intake_raw,
    create_intake_payment,
    lookup_client_debt,
    check_recent_duplicate,
    find_matching_pending,
    link_pending_to_cashier,
    confirm_payment,
    summarize_today_intake,
    reject_payment,
    get_payment,
    resolve_client_telegram_ids,
    admin_cancel_payment,
    edit_payment_amount,
    assign_supplier,
    attach_agreement,
    attach_transfer_proof,
    confirm_supplier_receipt,
    list_dedicated_cards,
    add_dedicated_card,
    retire_dedicated_card,
    format_card_number,
)
from backend.services.client_search import search_clients, client_display_label
from bot.shared import is_admin, is_admin_cb

logger = logging.getLogger(__name__)
router = Router(name="cashier")

AGENTS = ["Bobur", "Ibrohim", "Musobek", "Sherzod", "Umidjon", "Dilshod"]


# ── States ──────────────────────────────────────────────────────────

class CashierFlow(StatesGroup):
    # Klientdan pul qabul qilish (cash_direct: cashier records walk-in)
    direct_search = State()
    direct_uzs = State()
    direct_uzs_confirm = State()
    direct_usd = State()
    direct_usd_confirm = State()
    direct_confirm_dup = State()
    # Agentdan pul qabul qilish (cash_via_agent: cashier picks agent, then
    # falls into the same direct_search → uzs → usd → finalize chain).
    # Agent's mini-app pending row gets auto-linked at finalize when amount
    # matches; no separate queue-and-confirm step (cashier rejected that UX).
    agent_pick = State()
    # O'zgartirish — amount-only edit of an already-confirmed payment.
    # Soft-cancels old + inserts new linked via replaces_payment_id.
    edit_amount = State()
    edit_amount_confirm = State()
    # Avvalgi sanaga — pick a past date (last 7 days) before the normal
    # K/A flow. Used when drivers come back after the cashier left and one
    # of the family received the cash; she records it next morning back-
    # dated to the actual cash-flow date. kassa_date is stashed in FSM
    # state and read at finalize.
    backdate_pick = State()


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
    """One button per row — wide, easy hit target. Direct first (more frequent).
    The 3rd row opens a date picker for back-dated entries (last 7 days), used
    when drivers come back after the cashier's shift and family takes the cash."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💰 Klientdan", callback_data="cashier:menu_direct")],
            [InlineKeyboardButton(text="📥 Agentdan",  callback_data="cashier:menu_queue")],
            [InlineKeyboardButton(text="📅 Avvalgi sanaga", callback_data="cashier:menu_backdate")],
        ]
    )


# Uzbek weekday abbreviations (Mon..Sun) — first two letters of:
# Dushanba, Seshanba, Chorshanba, Payshanba, Juma, Shanba, Yakshanba.
_UZ_WEEKDAY_ABBR = ["Du", "Se", "Cho", "Pa", "Ju", "Sha", "Yak"]


def _backdate_options(days: int = 7) -> list[tuple[str, str]]:
    """Return (iso_date, label) for the last `days` days, today EXCLUDED,
    newest first. iso_date is YYYY-MM-DD (Tashkent), label is e.g.
    'Du 26.05'. Today is excluded because today's payments use the regular
    /qabul flow without back-dating."""
    today_tk = datetime.now(TASHKENT_TZ).date()
    out: list[tuple[str, str]] = []
    for i in range(1, days + 1):
        d = today_tk - timedelta(days=i)
        label = f"{_UZ_WEEKDAY_ABBR[d.weekday()]} {d.strftime('%d.%m')}"
        out.append((d.isoformat(), label))
    return out


def _backdate_keyboard() -> InlineKeyboardMarkup:
    """Last 7 days as buttons, 3 per row + cancel. Today excluded."""
    opts = _backdate_options(7)
    rows: list[list[InlineKeyboardButton]] = []
    for i in range(0, len(opts), 3):
        rows.append([
            InlineKeyboardButton(text=label, callback_data=f"cashier:bd_pick_{iso}")
            for iso, label in opts[i:i + 3]
        ])
    rows.append([InlineKeyboardButton(text="❌ Bekor", callback_data="cashier:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _format_backdate_banner(kassa_date_iso: str) -> Optional[str]:
    """Human banner for the confirmation message: '📅 Sana: 25.05 (kecha)'
    or '📅 Sana: 22.05 (4 kun oldin)'. Returns None when the date is today
    or unparseable — caller skips the banner in that case."""
    if not kassa_date_iso:
        return None
    try:
        d = datetime.strptime(kassa_date_iso, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
    today_tk = datetime.now(TASHKENT_TZ).date()
    delta = (today_tk - d).days
    if delta <= 0:
        return None
    suffix = "kecha" if delta == 1 else f"{delta} kun oldin"
    return f"📅 Sana: {d.strftime('%d.%m')} ({suffix})"


async def _send_kassa_menu(target):
    """Re-post the /qabul menu after a flow completes, so the cashier can
    start the next transaction with a tap instead of retyping the command."""
    await target.answer(
        "💼 <b>Kassa</b>",
        parse_mode="HTML",
        reply_markup=_menu_keyboard(),
    )


# ── Group-clutter cleanup ───────────────────────────────────────────
#
# Every /qabul recording used to leave ~8 bot prompts + ~2 cashier inputs
# in the group; over weeks the history became unreadable. The audit trail
# lives in `intake_payments` (+ /bugunpul re-renders today's list), so the
# in-group history has no preservation duty. We track every FSM-flow
# message id, then bulk-delete on finalize/cancel, keeping only the final
# ✅ confirmation row.

async def _track_msg(state: FSMContext, msg) -> None:
    """Append msg.message_id to FSM flow_msg_ids. Pass any Message-like
    object (returned by .answer(), or an incoming Message). Idempotent."""
    if msg is None:
        return
    mid = getattr(msg, "message_id", None)
    if mid is None:
        return
    data = await state.get_data()
    ids = list(data.get("flow_msg_ids") or [])
    if mid not in ids:
        ids.append(mid)
        await state.update_data(flow_msg_ids=ids)


async def _cleanup_flow_msgs(
    bot: Bot,
    chat_id: int,
    state: FSMContext,
    keep_ids=None,
) -> None:
    """Delete every tracked flow message except keep_ids (the final ✅
    rows). Per-message failures are swallowed — Telegram refuses deletes
    >48h old, and an already-deleted msg raises too. Clears the tracking
    list after."""
    data = await state.get_data()
    ids = data.get("flow_msg_ids") or []
    keep = set(keep_ids or [])
    deleted = 0
    failed = 0
    for mid in ids:
        if mid in keep:
            continue
        try:
            await bot.delete_message(chat_id, mid)
            deleted += 1
        except Exception as e:
            # Bumped debug→warning so silent delete failures (the symptom
            # behind the recurring group-clutter incidents) are visible in
            # `railway logs` without re-instrumenting. Telegram refuses
            # deletes >48h old and already-deleted msgs raise too — those
            # are expected and harmless, but we now see them.
            failed += 1
            logger.warning(f"cashier cleanup: delete msg {mid} failed ({e})")
    if ids:
        logger.info(
            f"cashier cleanup: chat={chat_id} tracked={len(ids)} "
            f"deleted={deleted} failed={failed} kept={len(keep)}"
        )
    await state.update_data(flow_msg_ids=[])


def _agent_keyboard() -> InlineKeyboardMarkup:
    """6 hardcoded agent names + cancel. One per row for big touch targets."""
    rows = [
        [InlineKeyboardButton(text=name, callback_data=f"cashier:agent_pick_{i}")]
        for i, name in enumerate(AGENTS)
    ]
    rows.append([InlineKeyboardButton(text="❌ Bekor", callback_data="cashier:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
    # If a previous /qabul flow was abandoned mid-step, wipe its trash
    # before starting fresh — otherwise the old prompts orphan forever.
    if await state.get_state() is not None:
        await _cleanup_flow_msgs(message.bot, message.chat.id, state)
    await state.clear()
    await _track_msg(state, message)
    sent = await message.answer(
        "💼 <b>Kassa</b>",
        parse_mode="HTML",
        reply_markup=_menu_keyboard(),
    )
    await _track_msg(state, sent)


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


# ── /bugunpul — full list of today's payments (read-only) ───────────

_CHANNEL_LABEL = {
    "cash_direct":    "Klientdan",
    "cash_via_agent": "Agentdan",
    "p2p":            "P2P",
}

_STATUS_ICON = {
    "pending_handover": "⏳",
    "pending_review":   "🔍",
    "confirmed":        "✅",
}


def _today_intake_rows(conn):
    """All non-rejected intake_payments submitted on today's Tashkent date,
    newest first. Used by /bugunpul. submitted_hhmm_tk is HH:MM in
    Tashkent (UTC+5) — display-ready, no Python-side timezone math needed.
    notes carries 'agent: <name>' for cash_via_agent rows. kassa_date is
    set only on back-dated entries (Aunt records yesterday's cash today);
    NULL for normal same-day rows."""
    today_tk = conn.execute("SELECT date('now', '+5 hours') AS d").fetchone()["d"]
    rows = conn.execute(
        """SELECT ip.id, ip.client_id, ip.amount, ip.currency, ip.channel,
                  ip.status, ip.submitted_at, ip.confirmed_at, ip.notes,
                  ip.kassa_date,
                  strftime('%H:%M', ip.submitted_at, '+5 hours') AS submitted_hhmm_tk,
                  ac.name AS client_name, ac.client_id_1c
           FROM intake_payments ip
           LEFT JOIN allowed_clients ac ON ac.id = ip.client_id
           WHERE ip.status != 'rejected'
             AND ip.channel IN ('cash_direct', 'cash_via_agent', 'p2p')
             AND date(ip.submitted_at, '+5 hours') = ?
           ORDER BY ip.submitted_at DESC""",
        (today_tk,),
    ).fetchall()
    return today_tk, [dict(r) for r in rows]


def _agent_from_notes(notes: Optional[str]) -> Optional[str]:
    """Extract agent name from notes column. Stored as 'agent: <name>' for
    cash_via_agent rows; None for cash_direct/p2p."""
    if notes and notes.startswith("agent: "):
        return notes[7:].strip() or None
    return None


def _render_today_list(date: str, rows: list) -> str:
    if not rows:
        return (
            f"💼 <b>Bugungi to'lovlar — {date}</b>\n\n"
            f"📭 Hozircha qabul qilingan to'lov yo'q."
        )
    lines = [f"💼 <b>Bugungi to'lovlar — {date}</b> ({len(rows)} ta)\n"]
    for r in rows:
        ts = r.get("submitted_hhmm_tk") or ""  # HH:MM Tashkent (UTC+5)
        ch = _CHANNEL_LABEL.get(r["channel"], r["channel"] or "—")
        cname = (r.get("client_id_1c") or r.get("client_name") or f"ID {r['client_id']}")[:30]
        amt = _fmt_amount(r["amount"], r["currency"])
        icon = _STATUS_ICON.get(r["status"], "•")
        agent = _agent_from_notes(r.get("notes"))
        agent_seg = f" · 👨‍💼 {html_escape(agent)}" if agent else ""
        # Back-dated rows get a 📅 dd.mm seg so the cashier can tell at a
        # glance which entries in today's list belong to older cash-flow
        # dates (drivers came back after shift; cash counted next morning).
        kd = r.get("kassa_date")
        kd_seg = ""
        if kd:
            try:
                kd_seg = f" · 📅 {datetime.strptime(kd, '%Y-%m-%d').strftime('%d.%m')}"
            except (ValueError, TypeError):
                pass
        lines.append(
            f"{icon} {ts} · #{r['id']} · <b>{html_escape(cname)}</b>{agent_seg}{kd_seg} · {ch} · {amt}"
        )
    lines.append("\n<i>Pastdagi tugmalar: ✏️ — summani o'zgartirish · ✖ — yozuvni bekor qilish.</i>")
    return "\n".join(lines)


def _today_list_keyboard(rows: list, max_records: int = 15) -> Optional[InlineKeyboardMarkup]:
    """Three full-width keyboard-rows per record (client name prioritized):
        [#id · CLIENT · HH:MM · 👨‍💼 agent · amount]   ← tap shows full info
        [✏️ #id · O'zgartirish]
        [✖ #id · Bekor]
    Capped at max_records (newest first). Data row is a no-data toggle
    pair below — keeps client name front-and-center per user feedback."""
    if not rows:
        return None
    kb_rows = []
    for r in rows[:max_records]:
        ts = r.get("submitted_hhmm_tk") or ""
        amt = _fmt_amount(r["amount"], r["currency"])
        cname = (r.get("client_id_1c") or r.get("client_name") or f"ID {r['client_id']}")[:24]
        agent = _agent_from_notes(r.get("notes"))
        agent_seg = f" · 👨‍💼 {agent}" if agent else ""
        data_label = f"#{r['id']} · {cname} · {ts}{agent_seg} · {amt}"[:60]
        kb_rows.append([InlineKeyboardButton(
            text=data_label,
            callback_data=f"cashier:user_info_{r['id']}",
        )])
        kb_rows.append([InlineKeyboardButton(
            text=f"✏️ #{r['id']} · O'zgartirish",
            callback_data=f"cashier:user_edit_{r['id']}",
        )])
        kb_rows.append([InlineKeyboardButton(
            text=f"✖ #{r['id']} · Bekor",
            callback_data=f"cashier:user_cancel_{r['id']}",
        )])
    if not kb_rows:
        return None
    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


def _aggregate_today_by_client(rows: list) -> list:
    """Collapse today's intake rows into one entry per client, summing
    UZS and USD separately. Ordered by each client's first submission
    time ascending (order of entrance). Used by the 18:00 cashier-group
    auto-post."""
    by_id: dict = {}
    for r in rows:
        cid = r["client_id"]
        entry = by_id.get(cid)
        if entry is None:
            entry = {
                "client_id": cid,
                "client_name": (r.get("client_id_1c") or r.get("client_name")
                                or f"ID {cid}"),
                "first_submitted_at": r["submitted_at"],
                "first_hhmm_tk": r.get("submitted_hhmm_tk") or "",
                "uzs": 0.0,
                "usd": 0.0,
                "count": 0,
            }
            by_id[cid] = entry
        if r["submitted_at"] < entry["first_submitted_at"]:
            entry["first_submitted_at"] = r["submitted_at"]
            entry["first_hhmm_tk"] = r.get("submitted_hhmm_tk") or ""
        cur = (r.get("currency") or "").upper()
        amt = float(r.get("amount") or 0)
        if cur == "UZS":
            entry["uzs"] += amt
        elif cur == "USD":
            entry["usd"] += amt
        entry["count"] += 1
    return sorted(by_id.values(), key=lambda x: x["first_submitted_at"])


def _render_today_by_client(date: str, clients: list) -> str:
    """One row per client, combined UZS + USD totals, sorted by first
    submission time. Shows (N ta) only when a client has more than one
    payment today. No status icons — this is a 'who paid us today'
    digest, not a per-row status board."""
    if not clients:
        return (
            f"💼 <b>Bugungi to'lovlar — {date}</b>\n\n"
            f"📭 Hozircha qabul qilingan to'lov yo'q."
        )
    total_count = sum(c["count"] for c in clients)
    lines = [
        f"💼 <b>Bugungi to'lovlar — {date}</b> "
        f"({total_count} ta · {len(clients)} mijoz)\n"
    ]
    num_width = len(str(len(clients)))
    for i, c in enumerate(clients, 1):
        parts = []
        if c["uzs"] > 0:
            parts.append(_fmt_uzs(c["uzs"]))
        if c["usd"] > 0:
            parts.append(_fmt_usd(c["usd"]))
        amount_str = " + ".join(parts) if parts else "—"
        count_seg = f" ({c['count']} ta)" if c["count"] > 1 else ""
        ts = c["first_hhmm_tk"]
        name = html_escape(c["client_name"])
        lines.append(
            f"{i:>{num_width}}. {ts} · <b>{name}</b> — {amount_str}{count_seg}"
        )
    return "\n".join(lines)


def _confirm_row_keyboard(payment_id: int) -> InlineKeyboardMarkup:
    """Buttons attached to the fresh '✅ Qabul qilindi' message — one
    confirmation = one payment leg = one keyboard row of [✏️] [✖]."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="✏️ O'zgartirish",
            callback_data=f"cashier:user_edit_{payment_id}",
        ),
        InlineKeyboardButton(
            text="✖ Bekor",
            callback_data=f"cashier:user_cancel_{payment_id}",
        ),
    ]])


def _edit_amount_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Saqlash", callback_data="cashier:edit_save")],
        [InlineKeyboardButton(text="❌ Bekor",   callback_data="cashier:edit_cancel")],
    ])


@router.message(Command("bugunpul"))
async def cmd_bugunpul(message: Message, state: FSMContext):
    """Today's full intake_payments list with a Bekor button per row.
    Cashier group only — cashier or admin can cancel."""
    if not _is_cashier_chat(message):
        return
    if not is_cashier_or_admin(message):
        return
    conn = get_db()
    try:
        date, rows = _today_intake_rows(conn)
    finally:
        conn.close()
    # On busy days the full list exceeds Telegram's 4096-char cap —
    # send in chunks, keyboard attached to the last one (Error Log #83).
    chunks = chunk_message(_render_today_list(date, rows))
    kb = _today_list_keyboard(rows)
    for i, chunk in enumerate(chunks):
        await message.answer(
            chunk,
            parse_mode="HTML",
            reply_markup=kb if i == len(chunks) - 1 else None,
        )


def _render_summary(s: dict) -> str:
    """Format the daily intake summary as an HTML message. Used by the
    on-demand /bugun command (cashier group)."""
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


@router.callback_query(F.data.startswith("cashier:user_cancel_"))
async def cb_user_cancel(cb: CallbackQuery, bot: Bot):
    """Cashier-side soft cancel from /bugunpul. Same soft-cancel as
    /cashbook (status flip to rejected, audit row preserved) but
    available to cashiers, not just admins."""
    if not _is_cashier_chat(cb) or not is_cashier_or_admin_cb(cb):
        await cb.answer("Faqat kassir/admin", show_alert=True)
        return
    try:
        payment_id = int(cb.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri ID", show_alert=True)
        return
    conn = get_db()
    try:
        try:
            row = admin_cancel_payment(
                conn, payment_id, cb.from_user.id, "cashier_cancelled_via_bugunpul"
            )
        except ValueError as e:
            conn.rollback()
            await cb.answer(str(e), show_alert=True)
            return
        conn.commit()
        date, rows = _today_intake_rows(conn)
    finally:
        conn.close()
    text = _render_today_list(date, rows)
    kb = _today_list_keyboard(rows)
    # Re-render may exceed the 4096-char cap on busy days (Error Log #83):
    # edit in place only when the text fits in one chunk, else repost chunked.
    chunks = chunk_message(text)
    edited = False
    if len(chunks) == 1:
        try:
            await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            edited = True
        except Exception:
            pass
    if not edited:
        for i, chunk in enumerate(chunks):
            await cb.message.answer(
                chunk,
                parse_mode="HTML",
                reply_markup=kb if i == len(chunks) - 1 else None,
            )
    cname = row.get("client_id_1c") or row.get("client_name") or ""
    amt_str = _fmt_amount(row["amount"], row["currency"])
    await cb.answer(
        f"✖ Bekor qilindi: #{row['id']} — {amt_str} ({cname})"[:200]
    )
    await _notify_client_cancelled(
        bot, row["client_id"], cname, row["currency"], row["amount"]
    )


@router.callback_query(F.data.startswith("cashier:user_info_"))
async def cb_user_info(cb: CallbackQuery):
    """Data-row tap on /bugunpul — shows the full untruncated record details
    in a popup alert. Useful when the button label gets cut off."""
    if not _is_cashier_chat(cb) or not is_cashier_or_admin_cb(cb):
        await cb.answer()
        return
    try:
        payment_id = int(cb.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri ID", show_alert=True)
        return
    conn = get_db()
    try:
        try:
            row = get_payment(conn, payment_id)
        except ValueError:
            await cb.answer("Yozuv topilmadi", show_alert=True)
            return
    finally:
        conn.close()
    cname = row.get("client_id_1c") or row.get("client_name") or f"ID {row['client_id']}"
    agent = _agent_from_notes(row.get("notes"))
    ch = _CHANNEL_LABEL.get(row["channel"], row["channel"] or "—")
    # Telegram show_alert popups are plain text, no HTML — strip tags.
    submitted_at = (row.get("submitted_at") or "")[11:16]
    # Convert UTC HH:MM → Tashkent inline (no SQL handle here)
    try:
        h, m = submitted_at.split(":")
        tk_h = (int(h) + 5) % 24
        tk_hhmm = f"{tk_h:02d}:{m}"
    except ValueError:
        tk_hhmm = submitted_at
    lines = [
        f"#{row['id']}",
        f"👤 {cname}",
        f"🕒 {tk_hhmm}",
    ]
    if agent:
        lines.append(f"👨‍💼 Agent: {agent}")
    lines.append(f"📥 {ch}")
    lines.append(f"💵 {_fmt_amount(row['amount'], row['currency'])}")
    await cb.answer("\n".join(lines)[:200], show_alert=True)


@router.callback_query(F.data.startswith("cashier:user_edit_"))
async def cb_user_edit(cb: CallbackQuery, state: FSMContext):
    """Cashier-side O'zgartirish — entry point. Loads the payment, refuses
    if not confirmed, prompts for the new amount."""
    if not _is_cashier_chat(cb) or not is_cashier_or_admin_cb(cb):
        await cb.answer("Faqat kassir/admin", show_alert=True)
        return
    try:
        payment_id = int(cb.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri ID", show_alert=True)
        return
    # Don't trample an in-flight /qabul flow
    cur_state = await state.get_state()
    if cur_state and cur_state not in (
        CashierFlow.edit_amount.state,
        CashierFlow.edit_amount_confirm.state,
    ):
        await cb.answer(
            "Avval /qabul ni tugating yoki /bekor bilan to'xtating",
            show_alert=True,
        )
        return
    conn = get_db()
    try:
        try:
            row = get_payment(conn, payment_id)
        except ValueError:
            await cb.answer("Yozuv topilmadi", show_alert=True)
            return
    finally:
        conn.close()
    if row["status"] != "confirmed":
        await cb.answer(
            "Bu yozuv allaqachon bekor qilingan yoki o'zgartirilgan",
            show_alert=True,
        )
        return
    cname = row.get("client_id_1c") or row.get("client_name") or ""
    await state.set_state(CashierFlow.edit_amount)
    await state.update_data(
        edit_payment_id=payment_id,
        edit_old_amount=float(row["amount"]),
        edit_currency=row["currency"],
        edit_client_id=row["client_id"],
        edit_client_name=cname,
    )
    cur_amt = _fmt_amount(row["amount"], row["currency"])
    hint = "(masalan: 500000)" if row["currency"] == "UZS" else "(masalan: 200)"
    sent = await cb.message.answer(
        f"✏️ <b>#{payment_id} — {html_escape(cname)}</b>\n"
        f"Joriy summa: <b>{cur_amt}</b>\n\n"
        f"Yangi summani kiriting {hint}.",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )
    # Don't track cb.message — that's the previous recording's confirmed
    # ✅ row, which must survive. Only the new prompts get tracked.
    await _track_msg(state, sent)
    await cb.answer()


@router.message(CashierFlow.edit_amount, F.text)
async def edit_amount_input(message: Message, state: FSMContext):
    if not _is_cashier_chat(message):
        return
    await _track_msg(state, message)
    new_amt = _parse_amount(message.text or "")
    if new_amt is None:
        sent = await message.answer(
            "Raqam kiriting yoki ❌ Bekor.",
            reply_markup=_cancel_keyboard(),
        )
        await _track_msg(state, sent)
        return
    data = await state.get_data()
    old_amt = float(data.get("edit_old_amount") or 0)
    currency = data.get("edit_currency") or "UZS"
    if abs(new_amt - old_amt) < 0.005:
        sent = await message.answer(
            f"Summa o'zgarmagan ({_fmt_amount(old_amt, currency)}). Boshqa raqam kiriting yoki ❌ Bekor.",
            reply_markup=_cancel_keyboard(),
        )
        await _track_msg(state, sent)
        return
    await state.update_data(edit_new_amount=new_amt)
    await state.set_state(CashierFlow.edit_amount_confirm)
    pid = data.get("edit_payment_id")
    cname = data.get("edit_client_name") or ""
    sent = await message.answer(
        f"Tasdiqlash: #{pid} — <b>{html_escape(cname)}</b>\n"
        f"<b>{_fmt_amount(old_amt, currency)}</b> → <b>{_fmt_amount(new_amt, currency)}</b>",
        parse_mode="HTML",
        reply_markup=_edit_amount_confirm_keyboard(),
    )
    await _track_msg(state, sent)


@router.callback_query(CashierFlow.edit_amount_confirm, F.data == "cashier:edit_cancel")
async def cb_edit_cancel(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    await _cleanup_flow_msgs(cb.bot, cb.message.chat.id, state)
    await state.clear()
    await _send_kassa_menu(cb.message)
    await cb.answer()


@router.callback_query(CashierFlow.edit_amount_confirm, F.data == "cashier:edit_save")
async def cb_edit_save(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not _is_cashier_chat(cb) or not is_cashier_or_admin_cb(cb):
        await cb.answer("Faqat kassir/admin", show_alert=True)
        return
    data = await state.get_data()
    payment_id = data.get("edit_payment_id")
    new_amt = data.get("edit_new_amount")
    if payment_id is None or new_amt is None:
        await _cleanup_flow_msgs(cb.bot, cb.message.chat.id, state)
        await state.clear()
        await cb.answer("Holat yo'qolgan, qaytadan urinib ko'ring", show_alert=True)
        return
    conn = get_db()
    try:
        try:
            result = edit_payment_amount(
                conn,
                int(payment_id),
                float(new_amt),
                cb.from_user.id,
            )
        except ValueError as e:
            conn.rollback()
            await _cleanup_flow_msgs(cb.bot, cb.message.chat.id, state)
            await state.clear()
            await cb.answer(str(e)[:200], show_alert=True)
            return
        conn.commit()
    finally:
        conn.close()
    # NOTE: state.clear() deferred until AFTER cleanup runs below — clearing
    # state wipes flow_msg_ids.
    old_row = result["old"]
    new_row = result["new"]
    currency = new_row["currency"]
    old_amt = float(old_row["amount"])
    new_amt_f = float(new_row["amount"])
    cname = new_row.get("client_id_1c") or new_row.get("client_name") or ""
    # In-group transparency note (cashiers rotate — Aunt + Uncle).
    # Send the new ✏️ row first so we can capture its id, then bulk-delete
    # the edit flow's prompts/inputs (keeping the new row).
    sent = await cb.message.answer(
        f"✏️ Tuzatildi: #{old_row['id']} → #{new_row['id']} · 🕒 {_now_tashkent_hhmm()}\n"
        f"<b>{_fmt_amount(old_amt, currency)}</b> → <b>{_fmt_amount(new_amt_f, currency)}</b>\n"
        f"👤 {html_escape(cname)}",
        parse_mode="HTML",
        reply_markup=_confirm_row_keyboard(new_row["id"]),
    )
    keep_ids = [sent.message_id] if sent is not None else []
    # The old ✅ row (cb.message) is NOT in flow_msg_ids, so cleanup leaves
    # it alone — we just strip its now-stale [✏️] [✖] buttons.
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await _cleanup_flow_msgs(cb.bot, cb.message.chat.id, state, keep_ids=keep_ids)
    await state.clear()
    await cb.answer(f"✏️ Saqlandi: #{new_row['id']}"[:200])
    await _notify_client_edited(
        bot, new_row["client_id"], cname, currency, old_amt, new_amt_f
    )
    await _send_kassa_menu(cb.message)


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
    client_display = (
        result.get("client_id_1c")
        or result.get("client_name")
        or f"#{result.get('client_id') or ''}"
    )
    # Stage-1 sends the notification as media+caption (extra_doc is required),
    # so we edit the caption — edit_text fails silently on media messages.
    original = cb.message.html_text or cb.message.text or ""
    new_caption = (
        original
        + f"\n\n✅ → <b>{html_escape(supplier_name)}</b>"
        + "\n📎 <i>Shartnomani shu xabarga javob qilib yuboring</i>"
    )
    try:
        await cb.message.edit_caption(caption=new_caption, parse_mode="HTML")
    except Exception as e:
        logger.warning(f"legaltx pick edit_caption failed: {e}")
        await cb.message.answer(
            f"✅ #{transfer_id} <b>{html_escape(client_display)}</b> → "
            f"<b>{html_escape(supplier_name)}</b>\n"
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
    (F.document | F.photo)
    & F.reply_to_message
    & (F.chat.id == LEGAL_TRANSFER_GROUP_CHAT_ID)
)
async def cb_legaltx_agreement_upload(message: Message, bot: Bot):
    """Stage 3: agreement file uploaded as a reply to the Stage 1 notification.

    The chat-id filter scopes this to the legal-transfer group. Inside the
    group we trust any member to post the agreement (supplier reps reply
    directly with the doc/photo); the state machine in `attach_agreement`
    rejects anything that isn't sitting at status='supplier_assigned', so a
    stray reply on the wrong message just gets a "⚠️" back without
    advancing state.
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

    if message.photo:
        is_image = True
        file_id = message.photo[-1].file_id
    else:
        is_image = False
        file_id = message.document.file_id

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
    new_caption = (
        original_html
        + "\n📎 ✅ <b>Shartnoma yuklandi</b>"
        + "\n💰 <i>Klient to'lovni amalga oshirgandan keyin chek shu xabarga javob qilsin</i>"
    )
    try:
        await rt.edit_caption(caption=new_caption, parse_mode="HTML")
    except Exception as e:
        logger.warning(f"agreement upload edit_caption failed: {e}")

    await message.reply(
        f"✅ <b>#{transfer_id}</b> Shartnoma qabul qilindi.",
        parse_mode="HTML",
    )

    # Recipient list: client siblings (all phones for the same client_id_1c)
    # PLUS the agent submitter when an agent acted on behalf. Self-submitted
    # requests already have submitter == one of the siblings, so dedup keeps
    # the DM count to one.
    recipients = list(client_tg_ids)
    submitter_tg = result.get("submitted_by_telegram_id")
    if submitter_tg and submitter_tg not in recipients:
        recipients.append(submitter_tg)

    if not recipients:
        await message.reply(
            f"⚠️ #{transfer_id}: na klient na yuboruvchi uchun Telegram topilmadi. Qo'lda yuboring.",
            parse_mode="HTML",
        )
        return
    if not client_tg_ids:
        await message.reply(
            f"ℹ️ Klient <b>#{result['client_id']}</b> botda yo'q — shartnoma faqat yuboruvchiga (agent) yuborildi.",
            parse_mode="HTML",
        )

    client_caption = (
        f"🏛 <b>Yuridik shaxs to'lov #{transfer_id}</b>\n\n"
        f"Shartnoma keldi. Iltimos, faylni o'qing va to'lovni amalga oshiring.\n\n"
        f"💰 Summa: <b>{_fmt_uzs_for_msg(result['amount_uzs'])} UZS</b>\n"
        f"🏪 Yetkazib beruvchi: <b>{html_escape(result['supplier_name_1c'] or '')}</b>\n"
        f"🏢 Sizning firma: <b>{html_escape(result['legal_entity_name'] or '')}</b>\n\n"
        f"<i>To'lov amalga oshgandan keyin bank chekini bot orqali yuboring.</i>"
    )
    sent_count = 0
    for tg in recipients:
        try:
            if is_image:
                await bot.send_photo(
                    tg, file_id, caption=client_caption, parse_mode="HTML"
                )
            else:
                await bot.send_document(
                    tg, file_id, caption=client_caption, parse_mode="HTML"
                )
            sent_count += 1
        except Exception as e:
            logger.warning(
                f"Failed to send agreement #{transfer_id} to tg={tg}: {e}"
            )
    if sent_count == 0:
        await message.reply(
            f"⚠️ Hech kimga yuborib bo'lmadi (botni bloklagan bo'lishi mumkin). Qo'lda yuboring."
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
    group_id = legal_transfer_target()
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
    if not is_cashier_role_cb(cb):
        await cb.answer("Faqat kassir tasdiqlay oladi", show_alert=True)
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
    if not is_cashier_role_cb(cb):
        await cb.answer("Faqat kassir rad eta oladi", show_alert=True)
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
    # Wipe the flow's accumulated prompts + the /bekor command itself.
    await _track_msg(state, message)
    await _cleanup_flow_msgs(message.bot, message.chat.id, state)
    await state.clear()
    await _send_kassa_menu(message)


# ── Cancel callback (works in any state) ────────────────────────────

@router.callback_query(F.data == "cashier:cancel")
async def cb_cancel(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    # Cancel via inline button — the cb.message is one of the FSM prompts
    # and is already tracked; cleanup deletes the whole flow including it.
    await _cleanup_flow_msgs(cb.bot, cb.message.chat.id, state)
    await state.clear()
    await cb.answer()
    await _send_kassa_menu(cb.message)


# ── Menu → flow start ───────────────────────────────────────────────

@router.callback_query(F.data == "cashier:menu_direct")
async def cb_menu_direct(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb) or not is_cashier_or_admin_cb(cb):
        await cb.answer()
        return
    # Abandoned-flow cleanup: if a prior /qabul attempt was left mid-step
    # (e.g. cashier typed a name, got an ambiguous picker, walked away) its
    # tracked messages still sit in the group. Starting a new action wipes
    # them. Keep the menu we just tapped — it gets re-tracked + cleaned at
    # finalize. No-op when flow_msg_ids is empty (the fresh-start case).
    await _cleanup_flow_msgs(
        cb.bot, cb.message.chat.id, state, keep_ids=[cb.message.message_id]
    )
    # Preserve kassa_date if user came via the back-date picker.
    existing = await state.get_data()
    kassa_date = existing.get("kassa_date")
    await state.set_state(CashierFlow.direct_search)
    await state.update_data(
        channel="cash_direct",
        submitter=cb.from_user.id,
        kassa_date=kassa_date,
    )
    # Track the menu we're leaving — it gets deleted on finalize so steady
    # state is one fresh menu at the bottom of the group.
    await _track_msg(state, cb.message)
    sent = await cb.message.answer(
        "🔎 Mijoz nomi:",
        reply_markup=_cancel_keyboard(),
    )
    await _track_msg(state, sent)
    await cb.answer()


@router.callback_query(F.data == "cashier:menu_queue")
async def cb_menu_queue(cb: CallbackQuery, state: FSMContext):
    """Agentdan flow — cashier picks the agent, then records the payment
    exactly like the Klientdan path (search client → enter amounts → save).
    At finalize, each leg is auto-linked to any matching pending_handover
    row the agent submitted via the mini app (exact amount, same client,
    within 24h). No queue-and-confirm step — Aunt rejected that UX."""
    if not _is_cashier_chat(cb) or not is_cashier_or_admin_cb(cb):
        await cb.answer()
        return
    # Abandoned-flow cleanup (see cb_menu_direct) — wipe any stale tracked
    # messages from a prior unfinished attempt before starting this one.
    await _cleanup_flow_msgs(
        cb.bot, cb.message.chat.id, state, keep_ids=[cb.message.message_id]
    )
    # Preserve kassa_date if user came via the back-date picker (cleared on
    # cancel / finalize, not on this transition).
    existing = await state.get_data()
    kassa_date = existing.get("kassa_date")
    await state.set_state(CashierFlow.agent_pick)
    await state.update_data(
        channel="cash_via_agent",
        submitter=cb.from_user.id,
        kassa_date=kassa_date,
    )
    await _track_msg(state, cb.message)
    sent = await cb.message.answer(
        "👨‍💼 <b>Qaysi agent?</b>",
        parse_mode="HTML",
        reply_markup=_agent_keyboard(),
    )
    await _track_msg(state, sent)
    await cb.answer()


@router.callback_query(F.data == "cashier:menu_backdate")
async def cb_menu_backdate(cb: CallbackQuery, state: FSMContext):
    """Open the date picker for back-dated intake. After the cashier picks
    a date, we re-show the K/A menu with kassa_date stashed in FSM state.
    From there the flow is identical to a normal /qabul, except the row
    written at finalize carries the chosen kassa_date and the confirmation
    message includes the 📅 Sana banner."""
    if not _is_cashier_chat(cb) or not is_cashier_or_admin_cb(cb):
        await cb.answer()
        return
    # Abandoned-flow cleanup (see cb_menu_direct) — wipe any stale tracked
    # messages from a prior unfinished attempt before starting this one.
    await _cleanup_flow_msgs(
        cb.bot, cb.message.chat.id, state, keep_ids=[cb.message.message_id]
    )
    await state.set_state(CashierFlow.backdate_pick)
    await state.update_data(submitter=cb.from_user.id)
    await _track_msg(state, cb.message)
    sent = await cb.message.answer(
        "📅 <b>Qaysi sana uchun?</b>\n\n"
        "<i>Avvalgi 7 kun. Bugungi to'lov uchun — pastdagi menyudan "
        "💰 Klientdan / 📥 Agentdan.</i>",
        parse_mode="HTML",
        reply_markup=_backdate_keyboard(),
    )
    await _track_msg(state, sent)
    await cb.answer()


@router.callback_query(CashierFlow.backdate_pick, F.data.startswith("cashier:bd_pick_"))
async def cb_backdate_pick(cb: CallbackQuery, state: FSMContext):
    """Cashier picked a past date. Validate it's within the allowed window
    (last 7 days, today excluded), stash kassa_date in FSM state, then re-
    show the K/A menu — the rest of the flow is identical to /qabul."""
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    iso = cb.data[len("cashier:bd_pick_"):]
    try:
        picked = datetime.strptime(iso, "%Y-%m-%d").date()
    except ValueError:
        await cb.answer("Noto'g'ri sana", show_alert=True)
        return
    today_tk = datetime.now(TASHKENT_TZ).date()
    delta = (today_tk - picked).days
    if delta < 1 or delta > 7:
        await cb.answer("Faqat oxirgi 7 kun (bugundan tashqari)", show_alert=True)
        return
    await state.update_data(kassa_date=iso)
    label = f"{_UZ_WEEKDAY_ABBR[picked.weekday()]} {picked.strftime('%d.%m')}"
    sent = await cb.message.answer(
        f"📅 <b>Sana:</b> {label} ({delta} kun oldin)\n\n"
        "<i>Endi pastdagi tugmadan tanlang:</i>",
        parse_mode="HTML",
        reply_markup=_menu_keyboard(),
    )
    await _track_msg(state, sent)
    # Leave the FSM in backdate_pick until the cashier taps K/A — those
    # handlers reset the state into direct_search / agent_pick and
    # preserve kassa_date from the stash above.
    await cb.answer()


@router.message(CashierFlow.backdate_pick, F.text)
async def backdate_pick_text_fallback(message: Message, state: FSMContext):
    """Nudge the cashier to use the buttons if she types instead of tapping."""
    if not _is_cashier_chat(message):
        return
    await _track_msg(state, message)
    sent = await message.answer(
        "📅 Iltimos, sanani tugmadan tanlang:",
        reply_markup=_backdate_keyboard(),
    )
    await _track_msg(state, sent)


@router.callback_query(CashierFlow.agent_pick, F.data.startswith("cashier:agent_pick_"))
async def cb_pick_agent(cb: CallbackQuery, state: FSMContext):
    if not _is_cashier_chat(cb):
        await cb.answer()
        return
    try:
        idx = int(cb.data.rsplit("_", 1)[1])
        agent_name = AGENTS[idx]
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri agent", show_alert=True)
        return
    await state.update_data(agent_name=agent_name)
    await state.set_state(CashierFlow.direct_search)
    sent = await cb.message.answer(
        f"👨‍💼 {html_escape(agent_name)}\n\n🔎 <b>Mijoz nomini kiriting</b>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )
    await _track_msg(state, sent)
    await cb.answer()


@router.message(CashierFlow.agent_pick, F.text)
async def agent_pick_text_fallback(message: Message, state: FSMContext):
    """Nudge the cashier to use the buttons if she types instead of tapping."""
    if not _is_cashier_chat(message):
        return
    await _track_msg(state, message)
    sent = await message.answer(
        "👨‍💼 Iltimos, agent nomini tugmadan tanlang:",
        reply_markup=_agent_keyboard(),
    )
    await _track_msg(state, sent)


# ── Flow 1: Klientdan pul qabul qilish ──────────────────────────────

@router.message(CashierFlow.direct_search, F.text)
async def direct_search_name(message: Message, state: FSMContext):
    if not _is_cashier_chat(message):
        return
    await _track_msg(state, message)
    q = (message.text or "").strip()
    if not q:
        sent = await message.answer("Mijoz nomini yuboring.", reply_markup=_cancel_keyboard())
        await _track_msg(state, sent)
        return
    results = search_clients(q, limit=8)
    whitelisted = results.get("whitelisted") or []
    if not whitelisted:
        sent = await message.answer(
            f"🔍 «{html_escape(q)}» — topilmadi. Qaytadan yozing.",
            parse_mode="HTML",
            reply_markup=_cancel_keyboard(),
        )
        await _track_msg(state, sent)
        return
    rows = []
    for c in whitelisted[:8]:
        label = (client_display_label(c.get("client_id_1c"), c.get("name")) or f"ID {c['id']}")[:55]
        rows.append([InlineKeyboardButton(
            text=label,
            callback_data=f"cashier:pick_{c['id']}",
        )])
    rows.append([InlineKeyboardButton(text="❌ Bekor", callback_data="cashier:cancel")])
    sent = await message.answer(
        f"<b>«{html_escape(q)}»</b>:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await _track_msg(state, sent)


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
    sent = await cb.message.answer(
        f"👤 {html_escape(row['client_id_1c'] or row['name'] or '')}\n"
        f"{debt_line}\n\n"
        f"💵 <b>So'm miqdorini kiriting</b>",
        parse_mode="HTML",
        reply_markup=_amount_keyboard("UZS"),
    )
    await _track_msg(state, sent)
    await cb.answer()


async def _ask_usd(target, state: FSMContext):
    """Helper: prompt the user for the USD amount. `target` is whatever has
    a working `.answer(...)` (a Message or cb.message)."""
    await state.set_state(CashierFlow.direct_usd)
    sent = await target.answer(
        "💵 <b>USD miqdorini kiriting</b>",
        parse_mode="HTML",
        reply_markup=_amount_keyboard("USD"),
    )
    await _track_msg(state, sent)


@router.message(CashierFlow.direct_uzs, F.text)
async def direct_uzs_amount(message: Message, state: FSMContext):
    if not _is_cashier_chat(message):
        return
    await _track_msg(state, message)
    amount = _parse_amount(message.text or "")
    if amount is None:
        sent = await message.answer(
            "Raqam kiriting (masalan: 500000) yoki «Yo'q».",
            reply_markup=_amount_keyboard("UZS"),
        )
        await _track_msg(state, sent)
        return
    await state.update_data(uzs_amount=amount)
    await state.set_state(CashierFlow.direct_uzs_confirm)
    sent = await message.answer(
        f"💵 So'm: <b>{_fmt_uzs(amount)}</b>\n\nTo'g'rimi?",
        parse_mode="HTML",
        reply_markup=_confirm_amount_keyboard("UZS"),
    )
    await _track_msg(state, sent)


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
    sent = await cb.message.answer(
        "💵 <b>So'm miqdorini qayta kiriting</b>",
        parse_mode="HTML",
        reply_markup=_amount_keyboard("UZS"),
    )
    await _track_msg(state, sent)
    await cb.answer()


@router.message(CashierFlow.direct_usd, F.text)
async def direct_usd_amount(message: Message, state: FSMContext):
    if not _is_cashier_chat(message):
        return
    await _track_msg(state, message)
    amount = _parse_amount(message.text or "")
    if amount is None:
        sent = await message.answer(
            "Raqam kiriting (masalan: 200) yoki «Yo'q».",
            reply_markup=_amount_keyboard("USD"),
        )
        await _track_msg(state, sent)
        return
    await state.update_data(usd_amount=amount)
    await state.set_state(CashierFlow.direct_usd_confirm)
    sent = await message.answer(
        f"💵 USD: <b>{_fmt_usd(amount)}</b>\n\nTo'g'rimi?",
        parse_mode="HTML",
        reply_markup=_confirm_amount_keyboard("USD"),
    )
    await _track_msg(state, sent)


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
    sent = await cb.message.answer(
        "💵 <b>USD miqdorini qayta kiriting</b>",
        parse_mode="HTML",
        reply_markup=_amount_keyboard("USD"),
    )
    await _track_msg(state, sent)
    await cb.answer()


async def _direct_finalize_or_dup(message: Message, state: FSMContext, bot: Bot):
    """After both currencies collected: validate at least one >0, then for
    each non-zero leg check (in order):

        1. find_matching_pending — agent submitted this exact amount via
           the mini app in the last 24h? Stash the pending_id and skip the
           dedup warning; finalize will UPDATE that row in place.
        2. check_recent_duplicate — confirmed/pending row with the same
           amount already exists outside the auto-link window? Warn the
           cashier before continuing."""
    data = await state.get_data()
    uzs = float(data.get("uzs_amount") or 0)
    usd = float(data.get("usd_amount") or 0)
    if uzs <= 0 and usd <= 0:
        # Empty input — full cleanup, no ✅ to keep.
        await _cleanup_flow_msgs(bot, message.chat.id, state)
        await state.clear()
        await message.answer(
            "❌ Iltimos, kamida bittasini kiriting (so'm yoki USD).",
        )
        await _send_kassa_menu(message)
        return

    # Skip the agent-pending auto-link when back-dating. An agent's
    # pending_handover row is "I'm bringing this cash in"; the 24h match
    # window catches yesterday's submission, but a back-dated row means
    # the family already received that cash — linking would silently flip
    # the agent's pending to confirmed without the cashier seeing it.
    # Better to insert a fresh row and let any leftover pending be
    # reconciled or rejected on its own. Dup-warning still fires.
    is_backdated = bool(data.get("kassa_date"))
    conn = get_db()
    try:
        auto_links = {}
        dups = []
        for cur, amt in (("UZS", uzs), ("USD", usd)):
            if amt <= 0:
                continue
            if not is_backdated:
                match = find_matching_pending(conn, data["client_id"], amt, cur)
                if match:
                    auto_links[cur] = match["id"]
                    continue
            d = check_recent_duplicate(conn, data["client_id"], amt, cur)
            if d:
                dups.append((cur, amt, d))
    finally:
        conn.close()

    await state.update_data(auto_links=auto_links)

    if dups:
        await state.set_state(CashierFlow.direct_confirm_dup)
        lines = ["⚠️ <b>Yaqinda shunday to'lov bor:</b>"]
        for cur, amt, d in dups:
            lines.append(
                f"• {_fmt_amount(amt, cur)} — {d['status']} "
                f"({d['submitted_at']})"
            )
        lines.append("\nDavom etamizmi?")
        sent = await message.answer(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=_dup_keyboard(),
        )
        await _track_msg(state, sent)
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
    await _cleanup_flow_msgs(cb.bot, cb.message.chat.id, state)
    await state.clear()
    await _send_kassa_menu(cb.message)
    await cb.answer()


async def _direct_finalize(message: Message, state: FSMContext, bot: Bot):
    """Per currency leg: if state has an auto-link pending_id (set by
    _direct_finalize_or_dup), UPDATE that agent-submitted row to confirmed
    instead of inserting a new one — preserves single-row attribution for
    one physical handover. Otherwise INSERT a fresh confirmed row.

    The audit raw row is written unconditionally so the cashier's submission
    is preserved even when the canonical row is the agent's pending."""
    data = await state.get_data()
    client_id = data["client_id"]
    client_name = data.get("client_name") or ""
    uzs = float(data.get("uzs_amount") or 0)
    usd = float(data.get("usd_amount") or 0)
    # submitter is recorded in state when the flow starts, so it survives
    # callback paths where message.from_user is the bot itself.
    submitter_id = data.get("submitter") or 0
    role = "cashier"
    channel = data.get("channel") or "cash_direct"
    agent_name = data.get("agent_name")
    auto_links = data.get("auto_links") or {}
    # Back-date stash from the date picker — NULL means today's row,
    # YYYY-MM-DD means cash actually flowed on that date but Aunt is
    # recording it now. Auto-link was already suppressed upstream in
    # _direct_finalize_or_dup when this is set.
    kassa_date = data.get("kassa_date")
    backdate_banner = _format_backdate_banner(kassa_date) if kassa_date else None

    conn = get_db()
    payments_created = []
    try:
        for cur_code, amt in (("UZS", uzs), ("USD", usd)):
            if amt <= 0:
                continue
            payload = {
                "channel": channel,
                "client_id": client_id,
                "amount": amt,
                "currency": cur_code,
            }
            if agent_name:
                payload["agent_name"] = agent_name
            if kassa_date:
                payload["kassa_date"] = kassa_date
            pending_id = auto_links.get(cur_code)
            if pending_id:
                payload["links_pending"] = pending_id
            raw_id = insert_intake_raw(
                conn,
                submitter_telegram_id=submitter_id,
                submitter_role=role,
                payload=payload,
            )
            linked = False
            if pending_id:
                try:
                    row = link_pending_to_cashier(
                        conn,
                        pending_payment_id=pending_id,
                        cashier_telegram_id=submitter_id,
                        audit_raw_id=raw_id,
                    )
                    payments_created.append((row["id"], cur_code, amt))
                    linked = True
                except ValueError as e:
                    # Race: agent's row flipped between dedup-check and now
                    # (admin cancel, double-confirm, etc.). Fall back to a
                    # fresh INSERT — the audit raw row already preserves
                    # the cashier's submission either way.
                    logger.warning(
                        f"auto-link to pending #{pending_id} failed ({e}); "
                        f"falling back to fresh insert"
                    )
            if not linked:
                pid = create_intake_payment(
                    conn,
                    raw_id=raw_id,
                    client_id=client_id,
                    amount=amt,
                    currency=cur_code,
                    channel=channel,
                    status="confirmed",
                    submitter_telegram_id=submitter_id,
                    submitter_role=role,
                    confirmed_by_telegram_id=submitter_id,
                    notes=(f"agent: {agent_name}" if agent_name else None),
                    kassa_date=kassa_date,
                )
                payments_created.append((pid, cur_code, amt))
                # Audit trail for back-dated entries — one row per leg, so
                # `grep command=backdated_intake` answers "what was back-
                # dated when, by whom, for whom" in one query without
                # touching intake_payments.
                if kassa_date:
                    try:
                        conn.execute(
                            "INSERT INTO admin_action_log "
                            "(telegram_id, chat_id, command, args) "
                            "VALUES (?, ?, ?, ?)",
                            (
                                submitter_id,
                                CASHIER_GROUP_CHAT_ID,
                                "backdated_intake",
                                f"pid={pid} client_id={client_id} "
                                f"amount={amt} cur={cur_code} "
                                f"kassa_date={kassa_date}"
                                + (f" agent={agent_name}" if agent_name else ""),
                            ),
                        )
                    except Exception as e:
                        logger.warning(f"admin_action_log write for backdated pid={pid} failed: {e}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception("direct finalize failed")
        # DB error — wipe the flow's trash, leave only the error toast.
        await _cleanup_flow_msgs(bot, message.chat.id, state)
        await message.answer(f"❌ Saqlashda xatolik: {html_escape(str(e))}")
        await state.clear()
        return
    finally:
        conn.close()

    # Confirmation to the cashier in the group — one message per leg so
    # each gets its own [✏️ #id] [✖ #id] keyboard scoped to that payment.
    # Back-dated rows lead with the 📅 banner so they're visually distinct
    # from today's rows in the group's scroll-back history.
    now_hhmm = _now_tashkent_hhmm()
    keep_ids = []
    for pid, cur, amt in payments_created:
        confirm_lines = [
            f"✅ Qabul qilindi #{pid}: <b>{_fmt_amount(amt, cur)}</b>",
            f"👤 {html_escape(client_name)}",
            f"🕒 {now_hhmm}",
        ]
        if backdate_banner:
            confirm_lines.insert(0, backdate_banner)
        if agent_name:
            confirm_lines.append(f"👨‍💼 {html_escape(agent_name)} orqali")
        if auto_links.get(cur) == pid:
            confirm_lines.append("🔗 Agent yuborgan to'lov bilan bog'landi")
        sent = await message.answer(
            "\n".join(confirm_lines),
            parse_mode="HTML",
            reply_markup=_confirm_row_keyboard(pid),
        )
        if sent is not None:
            keep_ids.append(sent.message_id)

    # Delete every FSM-flow message (menu tap, prompts, typed inputs, dup
    # warning, etc.) except the final ✅ rows we just sent. Result in the
    # group: 1 confirmation row per leg + the fresh menu below.
    await _cleanup_flow_msgs(bot, message.chat.id, state, keep_ids=keep_ids)

    # Notify the client (best-effort, swallow errors)
    await _notify_client_confirmed(bot, client_id, client_name, payments_created, agent_name)
    await state.clear()
    await _send_kassa_menu(message)


# ── Notifications ───────────────────────────────────────────────────

async def _notify_client_confirmed(
    bot: Bot,
    client_id: int,
    client_name: str,
    legs,
    agent_name: Optional[str] = None,
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
    lines = [
        f"✅ <b>To'lov qabul qilindi</b>",
        f"💵 {legs_text}",
        f"👤 {html_escape(client_name)}",
    ]
    if agent_name:
        lines.append(f"👨‍💼 Agent: {html_escape(agent_name)}")
    lines.append(f"🕒 {_now_tashkent_hhmm()}")
    text = "\n".join(lines)
    for tid in recipients:
        try:
            await bot.send_message(tid, text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"client confirm notification to {tid} failed: {e}")


async def _notify_client_cancelled(
    bot: Bot,
    client_id: int,
    client_name: str,
    currency: str,
    amount: float,
):
    """Send a cancellation receipt to every approved telegram_id linked to
    this client. Best-effort — never raises."""
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
    text = (
        f"✖ <b>Avvalgi qabul bekor qilindi</b>\n"
        f"💵 {_fmt_amount(amount, currency)}\n"
        f"👤 {html_escape(client_name)}\n"
        f"🕒 {_now_tashkent_hhmm()}"
    )
    for tid in recipients:
        try:
            await bot.send_message(tid, text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"client cancel notification to {tid} failed: {e}")


async def _notify_client_edited(
    bot: Bot,
    client_id: int,
    client_name: str,
    currency: str,
    old_amount: float,
    new_amount: float,
):
    """Corrective DM after a cashier amount-edit. Best-effort; never raises."""
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
    text = (
        f"✏️ <b>Avvalgi qabul tuzatildi</b>\n"
        f"💵 {_fmt_amount(old_amount, currency)} → <b>{_fmt_amount(new_amount, currency)}</b>\n"
        f"👤 {html_escape(client_name)}\n"
        f"🕒 {_now_tashkent_hhmm()}"
    )
    for tid in recipients:
        try:
            await bot.send_message(tid, text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"client edit notification to {tid} failed: {e}")
