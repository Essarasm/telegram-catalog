"""Role-toggle bot command for admin self-impersonation.

/role (inside the agent-approval group only) opens an inline keyboard
letting a real admin temporarily view the panel + Mini App as another
role (admin / agent / cashier / worker) for testing.

The override lives in `users.view_as_role`; `get_role()` in
backend/services/roles.py honors it everywhere — Mini App API, panel,
callbacks — so what the admin sees is what an actual user with that role
would see.

The real `users.agent_role` is never modified by this command, so admins
can always reset back. Authorization checks `agent_role` directly (not
`get_role`) plus `ADMIN_IDS` env, so an admin who has flipped themselves
to a non-admin role can still re-tap /role to flip back.

The "🧹 Hammasini tozalash" button clears both `view_as_role` AND
`client_id` in one tap — the natural "exit testing mode" action that
removes the test-client link set by /testclient too.
"""
from __future__ import annotations

from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot.shared import (
    ADMIN_GROUP_CHAT_ID, AGENT_APPROVAL_GROUP_CHAT_ID,
    ADMIN_IDS, get_db, html_escape, logger,
)

router = Router()

# Chats where /role and its callbacks are accepted. Keep this tight —
# the override flips effective role globally, so we don't want random
# group members triggering it. Both groups are admin-only.
_ALLOWED_CHATS = {ADMIN_GROUP_CHAT_ID, AGENT_APPROVAL_GROUP_CHAT_ID}


_ROLE_LABEL = {
    "admin":   "🛡 Admin",
    "agent":   "👔 Agent",
    "cashier": "💰 Kassir",
    "worker":  "🚚 Ishchi",
    "observer": "👁 Kuzatuvchi",
}
_ROLE_ORDER = ("admin", "agent", "cashier", "worker", "observer")


def _is_real_admin(conn, telegram_id: int) -> bool:
    """True iff the user is a *real* admin — DB `agent_role='admin'` OR
    in `ADMIN_IDS` env. Bypasses `view_as_role` so a self-impersonating
    admin can always reset back."""
    if ADMIN_IDS and telegram_id in ADMIN_IDS:
        return True
    row = conn.execute(
        "SELECT agent_role FROM users WHERE telegram_id = ?", (telegram_id,)
    ).fetchone()
    return bool(row and (row["agent_role"] or "").lower() == "admin")


def _ensure_user_row(conn, telegram_id: int, first_name: str | None) -> None:
    """Insert a placeholder users row if missing — mirrors the bootstrap
    pattern in /testclient so admins who haven't opened the Mini App yet
    can still use /role."""
    row = conn.execute(
        "SELECT 1 FROM users WHERE telegram_id = ?", (telegram_id,)
    ).fetchone()
    if not row:
        conn.execute(
            "INSERT INTO users (telegram_id, first_name, is_approved) VALUES (?, ?, 1)",
            (telegram_id, first_name or "Admin"),
        )


def _render_state(conn, telegram_id: int) -> tuple[str, InlineKeyboardMarkup]:
    """Build the status text + 5-button keyboard reflecting current
    `view_as_role` + `client_id` for this user."""
    row = conn.execute(
        "SELECT agent_role, view_as_role, client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    real_role = (row["agent_role"] if row else None) or None
    override = (row["view_as_role"] if row else None) or None
    client_id = row["client_id"] if row else None

    effective = override or real_role
    effective_lbl = _ROLE_LABEL.get(effective, effective or "—")
    real_lbl = _ROLE_LABEL.get(real_role, real_role or "—")

    if client_id:
        ct = conn.execute(
            "SELECT name FROM allowed_clients WHERE id = ?", (client_id,)
        ).fetchone()
        if ct:
            client_line = f"#{client_id} — {html_escape(ct['name'] or '—')}"
        else:
            client_line = f"#{client_id} <i>(topilmadi)</i>"
    else:
        client_line = "<i>yo'q</i>"

    role_line = f"• Rol: {effective_lbl}"
    if override and override != real_role:
        role_line += f"  <i>(haqiqiy: {real_lbl})</i>"

    text = "\n".join([
        "<b>🔁 Test rejimi</b>",
        "",
        role_line,
        f"• Test mijoz: {client_line}",
    ])

    # 4 role buttons (2×2) + reset; mark the currently-active role with "•"
    active = override or real_role
    buttons: list[list[InlineKeyboardButton]] = []
    for i in range(0, len(_ROLE_ORDER), 2):
        kb_row: list[InlineKeyboardButton] = []
        for r in _ROLE_ORDER[i:i + 2]:
            prefix = "• " if r == active else ""
            kb_row.append(InlineKeyboardButton(
                text=f"{prefix}{_ROLE_LABEL[r]}",
                callback_data=f"role:set:{r}",
            ))
        buttons.append(kb_row)
    buttons.append([InlineKeyboardButton(
        text="🧹 Hammasini tozalash (rol + mijoz)",
        callback_data="role:reset",
    )])

    return text, InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(Command("role"))
async def cmd_role(message: types.Message):
    """Open the role-switch menu. Only works inside admin-type groups
    (admin group + agent-approval group) and only for real admins."""
    chat_id = message.chat.id if message.chat else None
    if chat_id not in _ALLOWED_CHATS:
        return  # silent — /role is scoped to admin chats by design

    uid = message.from_user.id if message.from_user else 0
    if not uid:
        return

    conn = get_db()
    try:
        if not _is_real_admin(conn, uid):
            await message.reply("Ruxsat yo'q — faqat adminlar.")
            return
        _ensure_user_row(conn, uid, message.from_user.first_name)
        conn.commit()
        text, kb = _render_state(conn, uid)
    finally:
        conn.close()

    await message.reply(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("role:"))
async def on_role_callback(cb: types.CallbackQuery):
    """Apply a role override or reset, then refresh the menu in place."""
    if not cb.from_user:
        await cb.answer("Foydalanuvchi aniqlanmadi", show_alert=False)
        return
    uid = cb.from_user.id

    chat_id = cb.message.chat.id if cb.message and cb.message.chat else None
    if chat_id not in _ALLOWED_CHATS:
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return

    parts = (cb.data or "").split(":", 2)
    if len(parts) < 2:
        await cb.answer("Noma'lum amal", show_alert=False)
        return
    action = parts[1]

    conn = get_db()
    try:
        if not _is_real_admin(conn, uid):
            await cb.answer("Ruxsat yo'q — faqat adminlar.", show_alert=True)
            return
        _ensure_user_row(conn, uid, cb.from_user.first_name)

        if action == "set" and len(parts) == 3 and parts[2] in _ROLE_ORDER:
            new_role = parts[2]
            conn.execute(
                "UPDATE users SET view_as_role = ? WHERE telegram_id = ?",
                (new_role, uid),
            )
            conn.commit()
            await cb.answer(f"✅ {_ROLE_LABEL[new_role]}", show_alert=False)
            logger.info(f"/role: {uid} → view_as_role={new_role}")

        elif action == "reset":
            conn.execute(
                "UPDATE users SET view_as_role = NULL, client_id = NULL "
                "WHERE telegram_id = ?",
                (uid,),
            )
            conn.commit()
            await cb.answer("🧹 Tozalandi", show_alert=False)
            logger.info(f"/role: {uid} → reset (view_as + client_id cleared)")

        else:
            await cb.answer("Noma'lum amal", show_alert=False)
            return

        text, kb = _render_state(conn, uid)
    finally:
        conn.close()

    try:
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        # Same text (e.g. tapped current role twice) → Telegram returns
        # "message is not modified". Safe to swallow.
        logger.debug(f"/role edit_text noop: {e}")
