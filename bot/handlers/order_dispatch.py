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

from datetime import datetime, timezone, timedelta

from aiogram import Router, F, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot.shared import ADMIN_IDS, _db_role_check, get_db, html_escape, logger
from backend.services.notify_group import build_dispatch_markup


router = Router()


def _kb_from_payload(payload: dict | None) -> InlineKeyboardMarkup | None:
    """Convert build_dispatch_markup's JSON-shape dict into the aiogram
    InlineKeyboardMarkup object that edit_reply_markup expects."""
    if not payload or not payload.get("inline_keyboard"):
        return None
    rows = [
        [InlineKeyboardButton(text=btn["text"], callback_data=btn["callback_data"])
         for btn in row]
        for row in payload["inline_keyboard"]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _rebuild_order_kb(order_id: int) -> InlineKeyboardMarkup | None:
    """Look up the current dispatch state and rebuild the Sotuv-message
    keyboard. Used by the cancel-no path to restore the pre-cancel view."""
    conn = get_db()
    try:
        order = conn.execute(
            "SELECT id, delivery_status, assigned_agent_id FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()
        if not order:
            return None
        agent_dict = None
        if order["assigned_agent_id"]:
            a = conn.execute(
                "SELECT first_name, vehicle, vehicle_capacity_tons "
                "FROM users WHERE telegram_id = ?",
                (order["assigned_agent_id"],),
            ).fetchone()
            if a:
                agent_dict = {
                    "first_name": a["first_name"],
                    "vehicle": a["vehicle"],
                    "vehicle_capacity_tons": a["vehicle_capacity_tons"],
                }
        confirmed = conn.execute(
            "SELECT 1 FROM confirmed_orders WHERE wishlist_order_id = ? LIMIT 1",
            (order_id,),
        ).fetchone()
    finally:
        conn.close()
    payload = build_dispatch_markup(
        order_id, order["delivery_status"], agent_dict,
        allow_cancel=(confirmed is None),
    )
    return _kb_from_payload(payload)


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


def _agent_label(first_name: str | None, vehicle: str | None,
                 vehicle_capacity_tons: float | None = None) -> str:
    name = (first_name or "Agent").strip() or "Agent"
    veh = (vehicle or "").strip()
    cap_text = f"{vehicle_capacity_tons:.1f}t" if vehicle_capacity_tons else ""
    if veh and cap_text:
        descriptor = f"{veh}·{cap_text}"
    else:
        descriptor = veh or cap_text
    label = f"{name} ({descriptor})" if descriptor else name
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
            "SELECT telegram_id, first_name, vehicle, vehicle_capacity_tons "
            "FROM users WHERE agent_role = 'agent' "
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
            text=_agent_label(a["first_name"], a["vehicle"], a["vehicle_capacity_tons"]),
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
            "SELECT telegram_id, first_name, vehicle, vehicle_capacity_tons "
            "FROM users WHERE telegram_id = ?",
            (agent_telegram_id,),
        ).fetchone()
        order = conn.execute(
            "SELECT id, client_name, client_phone, item_count, total_uzs, total_usd "
            "FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()
    finally:
        conn.close()

    label = _agent_label(
        agent["first_name"] if agent else "?",
        agent["vehicle"] if agent else "",
        agent["vehicle_capacity_tons"] if agent else None,
    )
    agent_dict = {
        "first_name": agent["first_name"] if agent else "?",
        "vehicle": agent["vehicle"] if agent else "",
        "vehicle_capacity_tons": agent["vehicle_capacity_tons"] if agent else None,
    }
    # Use build_dispatch_markup so the "✖ Bekor qilish" row carries through
    # post-assignment — cancel stays available until 1C reply lands.
    new_kb = _kb_from_payload(
        build_dispatch_markup(order_id, "assigned", agent_dict)
    )
    try:
        if new_kb:
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


# ── Order cancellation (admin self-service for wrong-name test orders) ─
#
# Two-tap flow:
#   ord:cancel:<id>  → swap keyboard to [✅ Ha, bekor qil] [↩ Yo'q]
#   ord:yes:<id>     → re-check confirmed_orders, hard-delete order +
#                      order_items, edit message text with cancellation
#                      banner, strip keyboard, log to admin_action_log
#   ord:no:<id>      → rebuild the pre-cancel keyboard via
#                      build_dispatch_markup (preserves pick / assigned)
#
# Refuses if `confirmed_orders` already has a row for this wishlist_order
# — once 1C entry happens, the cancel is no longer safe (real_orders +
# client_balances would diverge).

@router.callback_query(F.data.startswith("ord:cancel:"))
async def on_order_cancel(cb: types.CallbackQuery):
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
            "SELECT id FROM orders WHERE id = ?", (order_id,)
        ).fetchone()
        confirmed = conn.execute(
            "SELECT 1 FROM confirmed_orders WHERE wishlist_order_id = ? LIMIT 1",
            (order_id,),
        ).fetchone()
    finally:
        conn.close()

    if not order:
        await cb.answer(f"Buyurtma #{order_id} topilmadi", show_alert=True)
        return
    if confirmed:
        await cb.answer(
            "1C ga kiritilgan — bekor qilib bo'lmaydi.",
            show_alert=True,
        )
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="✅ Ha, bekor qil",
            callback_data=f"ord:yes:{order_id}",
        ),
        InlineKeyboardButton(
            text="↩ Yo'q",
            callback_data=f"ord:no:{order_id}",
        ),
    ]])
    try:
        await cb.message.edit_reply_markup(reply_markup=kb)
    except Exception as e:
        logger.warning(f"ord:cancel edit_reply_markup failed: {e}")
    await cb.answer()


@router.callback_query(F.data.startswith("ord:yes:"))
async def on_order_cancel_yes(cb: types.CallbackQuery):
    if not _is_dispatcher(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return

    parts = (cb.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=False)
        return
    order_id = int(parts[2])

    conn = get_db()
    order_row = None
    try:
        # Re-check at execution time — 1C reply could have landed between
        # taps. confirmed_orders row = no-go.
        confirmed = conn.execute(
            "SELECT 1 FROM confirmed_orders WHERE wishlist_order_id = ? LIMIT 1",
            (order_id,),
        ).fetchone()
        if confirmed:
            await cb.answer(
                "1C ga kiritildi — bekor qilib bo'lmaydi.",
                show_alert=True,
            )
            return

        order_row = conn.execute(
            "SELECT id, client_name, sales_group_message_text "
            "FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()
        if not order_row:
            await cb.answer(f"#{order_id} topilmadi", show_alert=True)
            return

        try:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM order_items WHERE order_id = ?", (order_id,))
            conn.execute("DELETE FROM orders WHERE id = ?", (order_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    except Exception as e:
        logger.error(f"order cancel #{order_id} failed: {e}", exc_info=True)
        await cb.answer(f"Xatolik: {str(e)[:80]}", show_alert=True)
        return
    finally:
        conn.close()

    # Audit log — fire-and-forget. CallbackQuery doesn't fit log_admin_action's
    # Message-shape contract, so insert directly.
    try:
        conn2 = get_db()
        conn2.execute(
            "INSERT INTO admin_action_log "
            "(telegram_id, user_name, chat_id, command, args) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                cb.from_user.id if cb.from_user else None,
                (cb.from_user.full_name if cb.from_user else None) or "",
                cb.message.chat.id if cb.message and cb.message.chat else None,
                "cancelorder",
                f"order_id={order_id} client={order_row['client_name']}"[:500],
            ),
        )
        conn2.commit()
        conn2.close()
    except Exception as e:
        logger.warning(f"admin_action_log failed for cancel #{order_id}: {e}")

    # Edit Sotuv message: prepend cancellation banner, drop keyboard.
    ts = datetime.now(timezone(timedelta(hours=5))).strftime("%H:%M")
    who_raw = ""
    if cb.from_user:
        who_raw = cb.from_user.first_name or cb.from_user.username or "admin"
    who = html_escape(who_raw or "admin")
    original = order_row["sales_group_message_text"] or ""
    banner = f"❌ <b>BEKOR QILINDI</b> — {who}, {ts} (Toshkent)"
    new_text = f"{banner}\n\n{original}" if original else banner
    if len(new_text) > 4000:
        new_text = new_text[:3997] + "…"
    try:
        await cb.message.edit_text(
            new_text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.warning(
            f"ord:yes edit_text failed for #{order_id}: {e}; "
            f"falling back to keyboard strip"
        )
        try:
            await cb.message.edit_reply_markup(reply_markup=None)
        except Exception as e2:
            logger.warning(f"ord:yes keyboard strip also failed: {e2}")

    logger.info(
        f"order cancelled: #{order_id} by user="
        f"{cb.from_user.id if cb.from_user else '?'}"
    )
    await cb.answer(f"#{order_id} bekor qilindi")


@router.callback_query(F.data.startswith("ord:no:"))
async def on_order_cancel_no(cb: types.CallbackQuery):
    if not _is_dispatcher(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return

    parts = (cb.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=False)
        return
    order_id = int(parts[2])

    kb = _rebuild_order_kb(order_id)
    if kb is None:
        # Order vanished between cancel-prompt and decline — nothing to restore.
        await cb.answer(f"#{order_id} topilmadi", show_alert=True)
        return
    try:
        await cb.message.edit_reply_markup(reply_markup=kb)
    except Exception as e:
        logger.warning(f"ord:no edit_reply_markup failed: {e}")
    await cb.answer()
