"""Order dispatch — admin assigns a delivery agent to a placed order.

Flow:
  1. Order is posted to ORDER_GROUP_CHAT_ID with an inline "🚚 Agent ga
     biriktirish" button (added by `notify_group.send_order_to_group`).
  2. Admin taps → `disp:pick:<order_id>` → handler edits the message's
     keyboard to show a list of active delivery agents (agent_role='agent').
  3. Admin taps an agent → `disp:assign:<order_id>:<agent_tg_id>` →
     atomic UPDATE under the TOCTOU_FIRST_WRITE_WINS pattern (Error Log
     #37). On success: edit the message to show "✅ Biriktirildi: <name>"
     and DM the assigned agent. On race: toast that someone else got it.
  4. Admin taps cancel → `disp:cancel:<order_id>` → restores the original
     pick button.

Block A shipped the schema; this handler is Block B's full implementation.
No separate HTTP endpoint — bot is the sole entrypoint for v1.
"""

from aiogram import Router, F, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot.shared import ADMIN_IDS, _db_role_check, get_db, html_escape, logger


router = Router()


def _is_dispatcher(cb: types.CallbackQuery) -> bool:
    """Admin-only auth that — unlike `is_admin_cb` — does NOT short-circuit
    on ORDER_GROUP_CHAT_ID. The dispatch button lives on the order PDF
    posted in the sales group; that group is silenced for generic /admin
    commands but the dispatch flow is admin-scoped by construction (only
    admins see the button as actionable). Accept admin via either ADMIN_IDS
    env or `users.agent_role='admin'` (DB).
    """
    uid = cb.from_user.id if cb.from_user else None
    if not uid:
        return False
    if ADMIN_IDS and uid in ADMIN_IDS:
        return True
    return _db_role_check(uid, {"admin"})


def _agent_label(first_name: str | None, vehicle: str | None) -> str:
    name = (first_name or "Agent").strip() or "Agent"
    veh = (vehicle or "").strip()
    label = f"{name} ({veh})" if veh else name
    return label[:64]  # Telegram inline-button text cap


@router.callback_query(F.data.startswith("disp:pick:"))
async def on_dispatch_pick(cb: types.CallbackQuery):
    if not _is_dispatcher(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return

    parts = (cb.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=False)
        return
    order_id = int(parts[2])

    conn = get_db()
    try:
        order = conn.execute(
            "SELECT id, delivery_status FROM orders WHERE id = ?", (order_id,)
        ).fetchone()
        if not order:
            await cb.answer(f"Buyurtma #{order_id} topilmadi", show_alert=True)
            return
        if order["delivery_status"] != "open":
            await cb.answer(
                f"Buyurtma allaqachon biriktirilgan ({order['delivery_status']})",
                show_alert=True,
            )
            return

        agents = conn.execute(
            "SELECT telegram_id, first_name, vehicle FROM users "
            "WHERE agent_role = 'agent' "
            "ORDER BY first_name COLLATE NOCASE"
        ).fetchall()
    finally:
        conn.close()

    if not agents:
        await cb.answer("Faol agent topilmadi", show_alert=True)
        return

    rows: list[list[InlineKeyboardButton]] = []
    for a in agents:
        rows.append([InlineKeyboardButton(
            text=_agent_label(a["first_name"], a["vehicle"]),
            callback_data=f"disp:assign:{order_id}:{a['telegram_id']}",
        )])
    rows.append([InlineKeyboardButton(
        text="❌ Bekor",
        callback_data=f"disp:cancel:{order_id}",
    )])

    try:
        await cb.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
        )
    except Exception as e:
        logger.warning(f"dispatch:pick edit_reply_markup failed: {e}")

    await cb.answer()


@router.callback_query(F.data.startswith("disp:assign:"))
async def on_dispatch_assign(cb: types.CallbackQuery):
    if not _is_dispatcher(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return

    parts = (cb.data or "").split(":")
    if len(parts) != 4 or not parts[2].isdigit() or not parts[3].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=False)
        return
    order_id = int(parts[2])
    agent_telegram_id = int(parts[3])

    conn = get_db()
    try:
        # Atomic dispatch — TOCTOU_FIRST_WRITE_WINS (Error Log #37).
        cursor = conn.execute(
            "UPDATE orders SET assigned_agent_id = ?, "
            "  assigned_at = datetime('now'), delivery_status = 'assigned' "
            "WHERE id = ? AND delivery_status = 'open'",
            (agent_telegram_id, order_id),
        )
        if cursor.rowcount == 0:
            current = conn.execute(
                "SELECT delivery_status FROM orders WHERE id = ?", (order_id,)
            ).fetchone()
            status = current["delivery_status"] if current else "topilmadi"
            await cb.answer(f"Boshqa biriktirildi ({status})", show_alert=True)
            return
        conn.commit()

        agent = conn.execute(
            "SELECT telegram_id, first_name, vehicle FROM users WHERE telegram_id = ?",
            (agent_telegram_id,),
        ).fetchone()
        order = conn.execute(
            "SELECT id, client_name, client_phone, item_count, total_uzs, total_usd "
            "FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()
    finally:
        conn.close()

    label = _agent_label(agent["first_name"] if agent else "?",
                         agent["vehicle"] if agent else "")
    new_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"✅ Biriktirildi: {label}",
                             callback_data="disp:noop"),
    ]])
    try:
        await cb.message.edit_reply_markup(reply_markup=new_kb)
    except Exception as e:
        logger.warning(f"dispatch:assign edit_reply_markup failed: {e}")

    await cb.answer(f"Biriktirildi: {label}")

    logger.info(
        f"dispatch:assign success — order #{order_id} → agent {agent_telegram_id} "
        f"(label={label})"
    )

    # DM the agent. Best-effort — if it fails (agent never DM'd the bot,
    # blocked, etc.), the assignment still stands and they see it next
    # time they open AgentHomePage's "Mening yetkazmalarim" section.
    try:
        from bot.main import bot
        total_uzs = int(order["total_uzs"] or 0)
        total_usd = float(order["total_usd"] or 0)
        money = f"{total_uzs:,} so'm".replace(",", " ")
        if total_usd > 0:
            money += f" + ${total_usd:.2f}"
        lines = [
            "🚚 <b>Yangi yetkazma sizga</b>",
            "",
            f"#{order['id']} — <b>{html_escape(order['client_name'] or '—')}</b>",
        ]
        if order["client_phone"]:
            lines.append(f"📞 {html_escape(order['client_phone'])}")
        lines.append(f"📦 {order['item_count'] or 0} mahsulot")
        lines.append(f"💰 {money}")
        lines.append("")
        lines.append("Mini ilovada \"Mening yetkazmalarim\" bo'limida ko'ring.")
        await bot.send_message(
            chat_id=agent_telegram_id,
            text="\n".join(lines),
            parse_mode="HTML",
        )
        logger.info(f"dispatch:assign DM sent to agent {agent_telegram_id} for order #{order_id}")
    except Exception as e:
        logger.warning(
            f"dispatch:assign DM to agent {agent_telegram_id} failed: "
            f"{type(e).__name__}: {e}"
        )


@router.callback_query(F.data.startswith("disp:cancel:"))
async def on_dispatch_cancel(cb: types.CallbackQuery):
    if not _is_dispatcher(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return

    parts = (cb.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=False)
        return
    order_id = int(parts[2])

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="🚚 Agent ga biriktirish",
            callback_data=f"disp:pick:{order_id}",
        )
    ]])
    try:
        await cb.message.edit_reply_markup(reply_markup=kb)
    except Exception as e:
        logger.warning(f"dispatch:cancel edit_reply_markup failed: {e}")
    await cb.answer()


@router.callback_query(F.data == "disp:noop")
async def on_dispatch_noop(cb: types.CallbackQuery):
    """Tap on the post-assignment label — no action, just acknowledge.
    Avoids Android's no-feedback-on-noop UX bug (Error Log #2)."""
    await cb.answer()
