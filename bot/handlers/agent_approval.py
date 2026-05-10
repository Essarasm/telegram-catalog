"""Admin approves / rejects agent self-registrations from the Admin group.

Pairs with `backend/services/agent_signup.py`:
  - submit_agent_application posts an inline-keyboarded message to ADMIN_
    GROUP_CHAT_ID with callback_data `appr:yes:<id>` / `appr:no:<id>`.
  - These handlers consume the callback, run the approval/rejection
    helper, edit the original message to show the decision, and DM the
    applicant with the outcome.

Auth: is_admin_cb. ADMIN_GROUP is in is_admin_cb's whitelist, so this
fires for any tap by an admin in the admin group (or any admin via
ADMIN_IDS env / DB role 'admin' in any chat).
"""

from aiogram import Router, F, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot.shared import get_db, is_admin_cb, logger


router = Router()


@router.callback_query(F.data.startswith("appr:yes:"))
async def on_approve(cb: types.CallbackQuery):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return

    parts = (cb.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=False)
        return
    application_id = int(parts[2])
    approver_id = cb.from_user.id if cb.from_user else 0

    from backend.services.agent_signup import approve_application
    conn = get_db()
    try:
        result = approve_application(conn, application_id, approver_id)
    finally:
        conn.close()

    if not result.get("ok"):
        err = result.get("error", "")
        if err == "not_pending":
            cur = result.get("current_status", "?")
            await cb.answer(
                f"Allaqachon ko'rib chiqilgan ({cur})", show_alert=True
            )
        elif err == "not_found":
            await cb.answer("Ariza topilmadi", show_alert=True)
        else:
            await cb.answer(f"Xatolik: {err}", show_alert=True)
        return

    name = f"{result.get('first_name', '')} {result.get('last_name', '')}".strip()
    await cb.answer(f"Tasdiqlandi: {name}")

    # Edit the admin-group message: replace the keyboard with a static
    # outcome label.
    new_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"✅ Tasdiqlandi: {name}",
                             callback_data="appr:noop"),
    ]])
    try:
        await cb.message.edit_reply_markup(reply_markup=new_kb)
    except Exception as e:
        logger.warning(f"approve edit_reply_markup failed: {e}")

    # DM the new agent — best-effort.
    try:
        from bot.main import bot
        await bot.send_message(
            chat_id=result["telegram_id"],
            text=(
                "✅ <b>Siz agent sifatida tasdiqlandingiz!</b>\n\n"
                "Endi <b>Katalog</b> tugmasini bosib agent panelidan "
                "foydalanishingiz mumkin."
            ),
            parse_mode="HTML",
        )
        logger.info(
            f"appr:yes — application #{application_id} approved, "
            f"user {result['telegram_id']} promoted to agent"
        )
    except Exception as e:
        logger.warning(
            f"appr:yes DM to user {result.get('telegram_id')} failed: "
            f"{type(e).__name__}: {e}"
        )


@router.callback_query(F.data.startswith("appr:no:"))
async def on_reject(cb: types.CallbackQuery):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return

    parts = (cb.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cb.answer("Noto'g'ri tugma", show_alert=False)
        return
    application_id = int(parts[2])
    rejector_id = cb.from_user.id if cb.from_user else 0

    from backend.services.agent_signup import reject_application
    conn = get_db()
    try:
        result = reject_application(conn, application_id, rejector_id, None)
    finally:
        conn.close()

    if not result.get("ok"):
        err = result.get("error", "")
        if err == "not_pending":
            cur = result.get("current_status", "?")
            await cb.answer(
                f"Allaqachon ko'rib chiqilgan ({cur})", show_alert=True
            )
        elif err == "not_found":
            await cb.answer("Ariza topilmadi", show_alert=True)
        else:
            await cb.answer(f"Xatolik: {err}", show_alert=True)
        return

    name = f"{result.get('first_name', '')} {result.get('last_name', '')}".strip()
    await cb.answer(f"Rad qilindi: {name}")

    new_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"❌ Rad qilindi: {name}",
                             callback_data="appr:noop"),
    ]])
    try:
        await cb.message.edit_reply_markup(reply_markup=new_kb)
    except Exception as e:
        logger.warning(f"reject edit_reply_markup failed: {e}")

    try:
        from bot.main import bot
        await bot.send_message(
            chat_id=result["telegram_id"],
            text=(
                "❌ <b>Sizning agent arizangiz rad etildi.</b>\n\n"
                "Savollar uchun admin bilan bog'laning."
            ),
            parse_mode="HTML",
        )
        logger.info(
            f"appr:no — application #{application_id} rejected, "
            f"user {result['telegram_id']} notified"
        )
    except Exception as e:
        logger.warning(
            f"appr:no DM to user {result.get('telegram_id')} failed: "
            f"{type(e).__name__}: {e}"
        )


@router.callback_query(F.data == "appr:noop")
async def on_noop(cb: types.CallbackQuery):
    """Tap on the post-decision label — no action, just acknowledge.
    Avoids Android's no-feedback-on-noop UX bug (Error Log #2)."""
    await cb.answer()
