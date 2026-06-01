"""Bank-transfer FSM (Session bank-transfer, 2026-05-08).

Lives in BANK_TRANSFER_GROUP_CHAT_ID — Uchqun + Shuhrat record bank
transfers received from clients. Sister to bot/handlers/cashier.py: same
intake_payments table, new channel='bank_transfer'.

Flow:
    /perevod → search client → enter gross UZS → enter accepted % →
    enter FX rate → confirm → auto-record (status=confirmed)

Net UZS (what counts as the actual payment) = gross × pct / 100. The FX
rate is stored alongside for audit + downstream USD-debt reconciliation;
it is not auto-converted to USD here per Ulugbek's choice (UZS-only display).

/perevodlar lists today's records with edit/cancel buttons. Edit walks
through a one-field-at-a-time menu (gross | pct | fx); each edit
soft-cancels the old row and inserts a new one linked via
replaces_payment_id, mirroring the cashier O'zgartirish pattern.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

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
    BANK_TRANSFER_GROUP_CHAT_ID,
    is_bank_transfer_or_admin,
    is_bank_transfer_or_admin_cb,
    get_db,
    html_escape,
)
from backend.services.payment_intake import (
    create_bank_transfer_payment,
    edit_bank_transfer_payment,
    lookup_client_debt,
    get_payment,
    admin_cancel_payment,
    resolve_client_telegram_ids,
)
from backend.services.client_search import search_clients, client_display_label

logger = logging.getLogger(__name__)
router = Router(name="bank_transfer")


# ── States ──────────────────────────────────────────────────────────

class BankTransferFlow(StatesGroup):
    search = State()
    gross_uzs = State()
    gross_uzs_confirm = State()
    pct = State()
    pct_confirm = State()
    fx = State()
    fx_confirm = State()
    edit_pick = State()
    edit_input = State()
    edit_confirm = State()


# ── Formatters / parsers ────────────────────────────────────────────

def _fmt_uzs(n) -> str:
    return f"{round(float(n or 0)):,}".replace(",", " ") + " so'm"


def _fmt_usd(n) -> str:
    return f"{float(n or 0):,.2f} $"


def _fmt_pct(p) -> str:
    val = float(p or 0)
    if val == int(val):
        return f"{int(val)}%"
    return f"{val:.1f}%"


def _fmt_fx(r) -> str:
    return f"{round(float(r or 0)):,}".replace(",", " ") + " so'm/$"


def _is_bt_chat(message_or_cb) -> bool:
    if not BANK_TRANSFER_GROUP_CHAT_ID:
        return False
    chat = getattr(message_or_cb, "chat", None) or (
        getattr(message_or_cb, "message", None) and message_or_cb.message.chat
    )
    return chat is not None and chat.id == BANK_TRANSFER_GROUP_CHAT_ID


def _parse_amount(text: str) -> Optional[float]:
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


def _parse_pct(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = text.replace("%", "").replace(",", ".").replace(" ", "").strip()
    try:
        v = float(cleaned)
    except ValueError:
        return None
    if v <= 0 or v > 100:
        return None
    return v


# ── Keyboards ───────────────────────────────────────────────────────

def _cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Bekor", callback_data="bt:cancel"),
    ]])


def _confirm_keyboard(field: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ha, davom etamiz", callback_data=f"bt:ok_{field}")],
        [InlineKeyboardButton(text="✏️ O'zgartirish",    callback_data=f"bt:redo_{field}")],
        [InlineKeyboardButton(text="❌ Bekor",            callback_data="bt:cancel")],
    ])


def _menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📨 Yangi perevod", callback_data="bt:menu_new"),
    ]])


def _confirm_row_keyboard(payment_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="✏️ O'zgartirish",
            callback_data=f"bt:user_edit_{payment_id}",
        ),
        InlineKeyboardButton(
            text="✖ Bekor",
            callback_data=f"bt:user_cancel_{payment_id}",
        ),
    ]])


def _edit_pick_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Perevod summasi", callback_data="bt:edit_field_gross")],
        [InlineKeyboardButton(text="📊 Foiz",            callback_data="bt:edit_field_pct")],
        [InlineKeyboardButton(text="💱 Kurs",            callback_data="bt:edit_field_fx")],
        [InlineKeyboardButton(text="❌ Bekor",            callback_data="bt:cancel")],
    ])


def _edit_save_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Saqlash", callback_data="bt:edit_save")],
        [InlineKeyboardButton(text="❌ Bekor",   callback_data="bt:edit_cancel")],
    ])


async def _send_menu(target):
    await target.answer(
        "📨 <b>Bank perevod</b>",
        parse_mode="HTML",
        reply_markup=_menu_keyboard(),
    )


# ── /perevod entry + cancel ─────────────────────────────────────────

@router.message(Command("perevod"))
async def cmd_perevod(message: Message, state: FSMContext):
    if not _is_bt_chat(message):
        return
    if not is_bank_transfer_or_admin(message):
        return
    await state.clear()
    await message.answer(
        "📨 <b>Bank perevod</b>",
        parse_mode="HTML",
        reply_markup=_menu_keyboard(),
    )


@router.callback_query(F.data == "bt:menu_new")
async def cb_menu_new(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb) or not is_bank_transfer_or_admin_cb(cb):
        await cb.answer()
        return
    await state.clear()
    await state.set_state(BankTransferFlow.search)
    await state.update_data(submitter=cb.from_user.id)
    await cb.message.answer(
        "🔎 Mijoz nomi:",
        reply_markup=_cancel_keyboard(),
    )
    await cb.answer()


@router.message(Command("bekor"))
async def cmd_bekor(message: Message, state: FSMContext):
    if not _is_bt_chat(message):
        return
    cur = await state.get_state()
    if cur is None:
        return
    await state.clear()
    await message.answer("❌ Bekor qilindi.")
    await _send_menu(message)


@router.callback_query(F.data == "bt:cancel")
async def cb_cancel(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb):
        await cb.answer()
        return
    await state.clear()
    try:
        await cb.message.edit_text("❌ Bekor qilindi.")
    except Exception:
        pass
    await cb.answer()
    await _send_menu(cb.message)


# ── Step 1: client search ───────────────────────────────────────────

@router.message(BankTransferFlow.search, F.text)
async def search_name(message: Message, state: FSMContext):
    if not _is_bt_chat(message):
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
        label = (client_display_label(c.get("client_id_1c"), c.get("name")) or f"ID {c['id']}")[:55]
        rows.append([InlineKeyboardButton(
            text=label,
            callback_data=f"bt:pick_{c['id']}",
        )])
    rows.append([InlineKeyboardButton(text="❌ Bekor", callback_data="bt:cancel")])
    await message.answer(
        f"<b>«{html_escape(q)}»</b>:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


@router.callback_query(BankTransferFlow.search, F.data.startswith("bt:pick_"))
async def pick_client(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb):
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
    cname = row["client_id_1c"] or row["name"] or ""
    await state.update_data(
        client_id=row["id"],
        client_name=cname,
    )
    await state.set_state(BankTransferFlow.gross_uzs)
    debt_line = (
        f"📊 Qarz: <b>{_fmt_uzs(debt['uzs'])}</b> · <b>{_fmt_usd(debt['usd'])}</b>"
        if (debt["uzs"] or debt["usd"])
        else "✅ Qarz yo'q"
    )
    await cb.message.answer(
        f"👤 {html_escape(cname)}\n"
        f"{debt_line}\n\n"
        f"💵 <b>Perevod summasi (so'm):</b>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )
    await cb.answer()


# ── Step 2: gross UZS ───────────────────────────────────────────────

@router.message(BankTransferFlow.gross_uzs, F.text)
async def gross_uzs_input(message: Message, state: FSMContext):
    if not _is_bt_chat(message):
        return
    amount = _parse_amount(message.text or "")
    if amount is None:
        await message.answer(
            "Raqam kiriting (masalan: 1000000).",
            reply_markup=_cancel_keyboard(),
        )
        return
    await state.update_data(gross_uzs=amount)
    await state.set_state(BankTransferFlow.gross_uzs_confirm)
    await message.answer(
        f"💵 Perevod summasi: <b>{_fmt_uzs(amount)}</b>\n\nTo'g'rimi?",
        parse_mode="HTML",
        reply_markup=_confirm_keyboard("gross"),
    )


@router.callback_query(BankTransferFlow.gross_uzs_confirm, F.data == "bt:ok_gross")
async def gross_ok(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb):
        await cb.answer()
        return
    await state.set_state(BankTransferFlow.pct)
    await cb.message.answer(
        "📊 <b>Necha foiz qabul qilamiz?</b>\n"
        "<i>Masalan: 100 (to'liq) yoki 96 (qisman).</i>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )
    await cb.answer()


@router.callback_query(BankTransferFlow.gross_uzs_confirm, F.data == "bt:redo_gross")
async def gross_redo(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb):
        await cb.answer()
        return
    await state.update_data(gross_uzs=None)
    await state.set_state(BankTransferFlow.gross_uzs)
    await cb.message.answer(
        "💵 <b>Perevod summasini qayta kiriting:</b>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )
    await cb.answer()


# ── Step 3: percentage ──────────────────────────────────────────────

@router.message(BankTransferFlow.pct, F.text)
async def pct_input(message: Message, state: FSMContext):
    if not _is_bt_chat(message):
        return
    pct = _parse_pct(message.text or "")
    if pct is None:
        await message.answer(
            "0 dan 100 gacha raqam kiriting (masalan: 96).",
            reply_markup=_cancel_keyboard(),
        )
        return
    await state.update_data(accepted_pct=pct)
    await state.set_state(BankTransferFlow.pct_confirm)
    data = await state.get_data()
    gross = float(data.get("gross_uzs") or 0)
    net = round(gross * pct / 100.0)
    await message.answer(
        f"📊 Foiz: <b>{_fmt_pct(pct)}</b>\n"
        f"💰 Sof summa: <b>{_fmt_uzs(net)}</b>\n\n"
        f"To'g'rimi?",
        parse_mode="HTML",
        reply_markup=_confirm_keyboard("pct"),
    )


@router.callback_query(BankTransferFlow.pct_confirm, F.data == "bt:ok_pct")
async def pct_ok(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb):
        await cb.answer()
        return
    await state.set_state(BankTransferFlow.fx)
    await cb.message.answer(
        "💱 <b>Bugungi kurs (so'm/$):</b>\n"
        "<i>Masalan: 12800.</i>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )
    await cb.answer()


@router.callback_query(BankTransferFlow.pct_confirm, F.data == "bt:redo_pct")
async def pct_redo(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb):
        await cb.answer()
        return
    await state.update_data(accepted_pct=None)
    await state.set_state(BankTransferFlow.pct)
    await cb.message.answer(
        "📊 <b>Necha foiz qabul qilamiz?</b>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )
    await cb.answer()


# ── Step 4: FX rate ─────────────────────────────────────────────────

@router.message(BankTransferFlow.fx, F.text)
async def fx_input(message: Message, state: FSMContext):
    if not _is_bt_chat(message):
        return
    rate = _parse_amount(message.text or "")
    # Sanity band: today's UZS/USD ~12,500–13,000. Accept 1,000–100,000
    # to allow drift; outside that band is almost certainly a typo.
    if rate is None or rate < 1000 or rate > 100000:
        await message.answer(
            "Kursni raqamda kiriting (masalan: 12800).",
            reply_markup=_cancel_keyboard(),
        )
        return
    await state.update_data(fx_rate=rate)
    await state.set_state(BankTransferFlow.fx_confirm)
    data = await state.get_data()
    gross = float(data.get("gross_uzs") or 0)
    pct = float(data.get("accepted_pct") or 0)
    net = round(gross * pct / 100.0)
    cname = data.get("client_name") or ""
    await message.answer(
        f"<b>Tasdiqlash</b>\n"
        f"👤 {html_escape(cname)}\n"
        f"💵 Perevod: <b>{_fmt_uzs(gross)}</b>\n"
        f"📊 Qabul: <b>{_fmt_pct(pct)}</b> → <b>{_fmt_uzs(net)}</b>\n"
        f"💱 Kurs: <b>{_fmt_fx(rate)}</b>",
        parse_mode="HTML",
        reply_markup=_confirm_keyboard("fx"),
    )


@router.callback_query(BankTransferFlow.fx_confirm, F.data == "bt:ok_fx")
async def fx_ok(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not _is_bt_chat(cb):
        await cb.answer()
        return
    await _finalize(cb.message, state, bot)
    await cb.answer()


@router.callback_query(BankTransferFlow.fx_confirm, F.data == "bt:redo_fx")
async def fx_redo(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb):
        await cb.answer()
        return
    await state.update_data(fx_rate=None)
    await state.set_state(BankTransferFlow.fx)
    await cb.message.answer(
        "💱 <b>Kursni qayta kiriting:</b>",
        parse_mode="HTML",
        reply_markup=_cancel_keyboard(),
    )
    await cb.answer()


# ── Finalize ────────────────────────────────────────────────────────

async def _finalize(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    client_id = data["client_id"]
    cname = data.get("client_name") or ""
    gross = float(data.get("gross_uzs") or 0)
    pct = float(data.get("accepted_pct") or 0)
    rate = float(data.get("fx_rate") or 0)
    submitter = data.get("submitter") or 0

    conn = get_db()
    try:
        try:
            pid = create_bank_transfer_payment(
                conn,
                client_id=client_id,
                gross_uzs=gross,
                accepted_pct=pct,
                fx_rate_uzs_per_usd=rate,
                submitter_telegram_id=submitter,
                submitter_role="bank_transfer",
            )
            conn.commit()
        except ValueError as e:
            conn.rollback()
            await message.answer(f"❌ Xatolik: {html_escape(str(e))}")
            await state.clear()
            return
    except Exception as e:
        conn.rollback()
        logger.exception("bank_transfer finalize failed")
        await message.answer(f"❌ Saqlashda xatolik: {html_escape(str(e))}")
        await state.clear()
        return
    finally:
        conn.close()

    net = round(gross * pct / 100.0)
    now = _now_tashkent_hhmm()
    await message.answer(
        f"✅ Qabul qilindi #{pid}\n"
        f"👤 {html_escape(cname)}\n"
        f"💵 {_fmt_uzs(gross)} × {_fmt_pct(pct)} = <b>{_fmt_uzs(net)}</b>\n"
        f"💱 Kurs: {_fmt_fx(rate)}\n"
        f"🕒 {now}",
        parse_mode="HTML",
        reply_markup=_confirm_row_keyboard(pid),
    )
    await _notify_client_confirmed(bot, client_id, cname, net, gross, pct, rate)
    await state.clear()
    await _send_menu(message)


# ── /perevodlar — today's list ──────────────────────────────────────

def _today_rows(conn):
    today_tk = conn.execute("SELECT date('now', '+5 hours') AS d").fetchone()["d"]
    rows = conn.execute(
        """SELECT ip.id, ip.client_id, ip.amount, ip.gross_uzs, ip.accepted_pct,
                  ip.fx_rate_uzs_per_usd, ip.status, ip.submitted_at,
                  strftime('%H:%M', ip.submitted_at, '+5 hours') AS submitted_hhmm_tk,
                  ac.name AS client_name, ac.client_id_1c
           FROM intake_payments ip
           LEFT JOIN allowed_clients ac ON ac.id = ip.client_id
           WHERE ip.channel = 'bank_transfer'
             AND ip.status != 'rejected'
             AND date(ip.submitted_at, '+5 hours') = ?
           ORDER BY ip.submitted_at DESC""",
        (today_tk,),
    ).fetchall()
    return today_tk, [dict(r) for r in rows]


def _render_today_list(date: str, rows: list) -> str:
    if not rows:
        return (
            f"📨 <b>Bugungi perevodlar — {date}</b>\n\n"
            f"📭 Hozircha qabul qilingan perevod yo'q."
        )
    total_net = sum(float(r["amount"] or 0) for r in rows)
    lines = [f"📨 <b>Bugungi perevodlar — {date}</b> ({len(rows)} ta)\n"]
    for r in rows:
        ts = r.get("submitted_hhmm_tk") or ""
        cname = (r.get("client_id_1c") or r.get("client_name") or f"ID {r['client_id']}")[:30]
        gross = _fmt_uzs(r["gross_uzs"])
        pct = _fmt_pct(r["accepted_pct"])
        net = _fmt_uzs(r["amount"])
        rate = _fmt_fx(r["fx_rate_uzs_per_usd"])
        lines.append(
            f"✅ {ts} · #{r['id']} · <b>{html_escape(cname)}</b>\n"
            f"   {gross} × {pct} = {net} · {rate}"
        )
    lines.append(f"\n💰 Jami sof: <b>{_fmt_uzs(total_net)}</b>")
    lines.append(
        "\n<i>Pastdagi tugmalar: ✏️ — yozuvni o'zgartirish · ✖ — bekor.</i>"
    )
    return "\n".join(lines)


def _today_list_keyboard(rows: list, max_records: int = 12) -> Optional[InlineKeyboardMarkup]:
    if not rows:
        return None
    kb_rows = []
    for r in rows[:max_records]:
        cname = (r.get("client_id_1c") or r.get("client_name") or f"ID {r['client_id']}")[:24]
        net = _fmt_uzs(r["amount"])
        kb_rows.append([InlineKeyboardButton(
            text=f"✏️ #{r['id']} · {cname} · {net}"[:60],
            callback_data=f"bt:user_edit_{r['id']}",
        )])
        kb_rows.append([InlineKeyboardButton(
            text=f"✖ #{r['id']} · Bekor",
            callback_data=f"bt:user_cancel_{r['id']}",
        )])
    if not kb_rows:
        return None
    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


@router.message(Command("perevodlar"))
async def cmd_perevodlar(message: Message, state: FSMContext):
    if not _is_bt_chat(message):
        return
    if not is_bank_transfer_or_admin(message):
        return
    conn = get_db()
    try:
        date, rows = _today_rows(conn)
    finally:
        conn.close()
    await message.answer(
        _render_today_list(date, rows),
        parse_mode="HTML",
        reply_markup=_today_list_keyboard(rows),
    )


# ── User-side cancel (soft, status flip) ────────────────────────────

@router.callback_query(F.data.startswith("bt:user_cancel_"))
async def cb_user_cancel(cb: CallbackQuery, bot: Bot):
    if not _is_bt_chat(cb) or not is_bank_transfer_or_admin_cb(cb):
        await cb.answer("Faqat foydalanuvchilar uchun", show_alert=True)
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
                conn, payment_id, cb.from_user.id, "cancelled_via_perevodlar"
            )
        except ValueError as e:
            conn.rollback()
            await cb.answer(str(e), show_alert=True)
            return
        conn.commit()
        date, rows = _today_rows(conn)
    finally:
        conn.close()
    text = _render_today_list(date, rows)
    kb = _today_list_keyboard(rows)
    try:
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await cb.message.answer(text, parse_mode="HTML", reply_markup=kb)
    cname = row.get("client_id_1c") or row.get("client_name") or ""
    net = _fmt_uzs(row["amount"])
    await cb.answer(f"✖ Bekor qilindi: #{row['id']} — {net} ({cname})"[:200])
    await _notify_client_cancelled(bot, row["client_id"], cname, row["amount"])


# ── Edit (one of three fields per click) ────────────────────────────

@router.callback_query(F.data.startswith("bt:user_edit_"))
async def cb_user_edit(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb) or not is_bank_transfer_or_admin_cb(cb):
        await cb.answer("Faqat foydalanuvchilar uchun", show_alert=True)
        return
    try:
        payment_id = int(cb.data.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri ID", show_alert=True)
        return
    cur_state = await state.get_state()
    if cur_state and cur_state not in (
        BankTransferFlow.edit_pick.state,
        BankTransferFlow.edit_input.state,
        BankTransferFlow.edit_confirm.state,
    ):
        await cb.answer(
            "Avval /perevod ni tugating yoki /bekor bilan to'xtating",
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
    if row["channel"] != "bank_transfer":
        await cb.answer("Bu perevod yozuvi emas", show_alert=True)
        return
    if row["status"] != "confirmed":
        await cb.answer(
            "Bu yozuv allaqachon bekor qilingan yoki o'zgartirilgan",
            show_alert=True,
        )
        return
    cname = row.get("client_id_1c") or row.get("client_name") or ""
    await state.set_state(BankTransferFlow.edit_pick)
    await state.update_data(
        edit_payment_id=payment_id,
        edit_client_id=row["client_id"],
        edit_client_name=cname,
        edit_old_gross=float(row["gross_uzs"] or 0),
        edit_old_pct=float(row["accepted_pct"] or 0),
        edit_old_fx=float(row["fx_rate_uzs_per_usd"] or 0),
        edit_old_amount=float(row["amount"] or 0),
    )
    await cb.message.answer(
        f"✏️ <b>#{payment_id} — {html_escape(cname)}</b>\n"
        f"💵 Perevod: <b>{_fmt_uzs(row['gross_uzs'])}</b>\n"
        f"📊 Foiz: <b>{_fmt_pct(row['accepted_pct'])}</b>"
        f" → {_fmt_uzs(row['amount'])}\n"
        f"💱 Kurs: <b>{_fmt_fx(row['fx_rate_uzs_per_usd'])}</b>\n\n"
        f"Qaysi qiymatni o'zgartirmoqchisiz?",
        parse_mode="HTML",
        reply_markup=_edit_pick_keyboard(),
    )
    await cb.answer()


@router.callback_query(BankTransferFlow.edit_pick, F.data.startswith("bt:edit_field_"))
async def cb_edit_field(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb):
        await cb.answer()
        return
    field = cb.data.rsplit("_", 1)[1]
    if field not in ("gross", "pct", "fx"):
        await cb.answer("Noto'g'ri tanlov", show_alert=True)
        return
    await state.update_data(edit_field=field)
    await state.set_state(BankTransferFlow.edit_input)
    prompt = {
        "gross": "💵 <b>Yangi perevod summasini kiriting (so'm):</b>",
        "pct":   "📊 <b>Yangi foizni kiriting:</b>",
        "fx":    "💱 <b>Yangi kursni kiriting (so'm/$):</b>",
    }[field]
    await cb.message.answer(prompt, parse_mode="HTML", reply_markup=_cancel_keyboard())
    await cb.answer()


@router.message(BankTransferFlow.edit_input, F.text)
async def edit_input(message: Message, state: FSMContext):
    if not _is_bt_chat(message):
        return
    data = await state.get_data()
    field = data.get("edit_field")
    raw = message.text or ""
    if field == "pct":
        v = _parse_pct(raw)
        if v is None:
            await message.answer(
                "0 dan 100 gacha raqam kiriting (masalan: 96).",
                reply_markup=_cancel_keyboard(),
            )
            return
    elif field == "fx":
        v = _parse_amount(raw)
        if v is None or v < 1000 or v > 100000:
            await message.answer(
                "Kursni raqamda kiriting (masalan: 12800).",
                reply_markup=_cancel_keyboard(),
            )
            return
    elif field == "gross":
        v = _parse_amount(raw)
        if v is None:
            await message.answer(
                "Raqam kiriting (masalan: 1000000).",
                reply_markup=_cancel_keyboard(),
            )
            return
    else:
        await state.clear()
        await message.answer("Holat yo'qolgan, qaytadan urinib ko'ring.")
        return
    await _confirm_edit_value(message, state, v)


async def _confirm_edit_value(target, state: FSMContext, new_value: float):
    data = await state.get_data()
    field = data.get("edit_field")
    pid = data.get("edit_payment_id")
    cname = data.get("edit_client_name") or ""
    old_gross = float(data.get("edit_old_gross") or 0)
    old_pct = float(data.get("edit_old_pct") or 0)
    old_fx = float(data.get("edit_old_fx") or 0)
    new_gross, new_pct, new_fx = old_gross, old_pct, old_fx
    if field == "gross":
        new_gross = new_value
    elif field == "pct":
        new_pct = new_value
    elif field == "fx":
        new_fx = new_value
    if (
        abs(new_gross - old_gross) < 0.005
        and abs(new_pct - old_pct) < 0.005
        and abs(new_fx - old_fx) < 0.005
    ):
        await target.answer(
            "Qiymat o'zgarmagan. Boshqa raqam kiriting yoki ❌ Bekor.",
            reply_markup=_cancel_keyboard(),
        )
        return
    new_net = round(new_gross * new_pct / 100.0)
    old_net = round(old_gross * old_pct / 100.0)
    await state.update_data(
        edit_new_gross=new_gross,
        edit_new_pct=new_pct,
        edit_new_fx=new_fx,
    )
    await state.set_state(BankTransferFlow.edit_confirm)
    label_map = {"gross": "Perevod", "pct": "Foiz", "fx": "Kurs"}
    fmt_map = {
        "gross": (_fmt_uzs(old_gross), _fmt_uzs(new_gross)),
        "pct":   (_fmt_pct(old_pct),   _fmt_pct(new_pct)),
        "fx":    (_fmt_fx(old_fx),     _fmt_fx(new_fx)),
    }
    old_str, new_str = fmt_map[field]
    await target.answer(
        f"Tasdiqlash: #{pid} — <b>{html_escape(cname)}</b>\n"
        f"{label_map[field]}: <b>{old_str}</b> → <b>{new_str}</b>\n"
        f"Sof summa: <b>{_fmt_uzs(old_net)}</b> → <b>{_fmt_uzs(new_net)}</b>",
        parse_mode="HTML",
        reply_markup=_edit_save_keyboard(),
    )


@router.callback_query(BankTransferFlow.edit_confirm, F.data == "bt:edit_cancel")
async def cb_edit_cancel_save(cb: CallbackQuery, state: FSMContext):
    if not _is_bt_chat(cb):
        await cb.answer()
        return
    await state.clear()
    await cb.message.answer("❌ O'zgartirish bekor qilindi.")
    await _send_menu(cb.message)
    await cb.answer()


@router.callback_query(BankTransferFlow.edit_confirm, F.data == "bt:edit_save")
async def cb_edit_save(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not _is_bt_chat(cb) or not is_bank_transfer_or_admin_cb(cb):
        await cb.answer("Faqat foydalanuvchilar uchun", show_alert=True)
        return
    data = await state.get_data()
    pid = data.get("edit_payment_id")
    new_gross = data.get("edit_new_gross")
    new_pct = data.get("edit_new_pct")
    new_fx = data.get("edit_new_fx")
    if pid is None or new_gross is None or new_pct is None or new_fx is None:
        await state.clear()
        await cb.answer("Holat yo'qolgan, qaytadan urinib ko'ring", show_alert=True)
        return
    conn = get_db()
    try:
        try:
            result = edit_bank_transfer_payment(
                conn,
                int(pid),
                new_gross_uzs=float(new_gross),
                new_accepted_pct=float(new_pct),
                new_fx_rate_uzs_per_usd=float(new_fx),
                editor_telegram_id=cb.from_user.id,
            )
            conn.commit()
        except ValueError as e:
            conn.rollback()
            await state.clear()
            await cb.answer(str(e)[:200], show_alert=True)
            return
    finally:
        conn.close()
    await state.clear()
    old_row = result["old"]
    new_row = result["new"]
    cname = new_row.get("client_id_1c") or new_row.get("client_name") or ""
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await cb.message.answer(
        f"✏️ Tuzatildi: #{old_row['id']} → #{new_row['id']}"
        f" · 🕒 {_now_tashkent_hhmm()}\n"
        f"👤 {html_escape(cname)}\n"
        f"💵 {_fmt_uzs(new_row['gross_uzs'])} × {_fmt_pct(new_row['accepted_pct'])}"
        f" = <b>{_fmt_uzs(new_row['amount'])}</b>\n"
        f"💱 Kurs: {_fmt_fx(new_row['fx_rate_uzs_per_usd'])}",
        parse_mode="HTML",
        reply_markup=_confirm_row_keyboard(new_row["id"]),
    )
    await cb.answer(f"✏️ Saqlandi: #{new_row['id']}"[:200])
    await _notify_client_edited(
        bot,
        new_row["client_id"],
        cname,
        old_amount=float(old_row["amount"] or 0),
        new_amount=float(new_row["amount"] or 0),
    )
    await _send_menu(cb.message)


# ── Notifications ───────────────────────────────────────────────────

async def _notify_client_confirmed(
    bot: Bot,
    client_id: int,
    client_name: str,
    net_uzs: float,
    gross_uzs: float,
    pct: float,
    fx_rate: float,
):
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
        f"✅ <b>Perevod qabul qilindi</b>\n"
        f"💵 {_fmt_uzs(net_uzs)} ({_fmt_uzs(gross_uzs)} × {_fmt_pct(pct)})\n"
        f"💱 Kurs: {_fmt_fx(fx_rate)}\n"
        f"👤 {html_escape(client_name)}\n"
        f"🕒 {_now_tashkent_hhmm()}"
    )
    for tid in recipients:
        try:
            await bot.send_message(tid, text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"client confirm notification to {tid} failed: {e}")


async def _notify_client_cancelled(
    bot: Bot,
    client_id: int,
    client_name: str,
    net_uzs: float,
):
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
        f"✖ <b>Avvalgi perevod bekor qilindi</b>\n"
        f"💵 {_fmt_uzs(net_uzs)}\n"
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
    *,
    old_amount: float,
    new_amount: float,
):
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
        f"✏️ <b>Avvalgi perevod tuzatildi</b>\n"
        f"💵 {_fmt_uzs(old_amount)} → <b>{_fmt_uzs(new_amount)}</b>\n"
        f"👤 {html_escape(client_name)}\n"
        f"🕒 {_now_tashkent_hhmm()}"
    )
    for tid in recipients:
        try:
            await bot.send_message(tid, text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"client edit notification to {tid} failed: {e}")
