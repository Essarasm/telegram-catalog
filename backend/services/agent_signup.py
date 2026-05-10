"""Agent self-registration — audit-first, admin-vetted.

Flow:
  1. Prospective agent fills form in mini-app (first/last + Telegram-
     verified contact phone + free-text vehicle). POST /api/users/register-
     agent calls `submit_agent_application`.
  2. We insert a `pending_agents` row immediately (audit-first), then post
     an inline-keyboarded message to ADMIN_GROUP_CHAT_ID and stash the
     resulting message_id on the row so the approval handler can edit it.
  3. Admin taps ✅ Tasdiqlash or ❌ Rad qilish in the group → callback in
     `bot/handlers/agent_approval.py` calls `approve_application` /
     `reject_application` here. Approval grants users.agent_role='agent'.
  4. Approved agent gets a DM "✅ Siz agent sifatida tasdiqlandingiz" via
     the bot. Best-effort — failure does not roll back the role grant.

Audit-first per zero-data-loss rule. Hard rules from Agent charter:
  - Role resolution stays in `roles.py`; this module only writes the
    `users.agent_role` column directly when granting the role.
  - Admin-group notification uses BOT_TOKEN over httpx (no aiogram
    dependency in backend). Mirrors `notify_group.py`'s pattern.
"""
import logging
import os
from typing import Optional

import httpx

from backend.services.phone_slots import _normalize

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
from backend.services.group_config import AGENT_APPROVAL_GROUP_CHAT_ID


def submit_agent_application(
    conn,
    telegram_id: int,
    first_name: str,
    last_name: str,
    phone_raw: str,
    vehicle: Optional[str] = None,
) -> dict:
    """Insert a pending_agents row + post to admin group. Returns:

        {ok: True, application_id: int, status: 'pending'}        on success
        {ok: False, error: str, application_id: Optional[int]}    on failure

    Idempotency: a telegram_id with a row in status='pending' gets that
    application_id back instead of a new row (form re-submit handling).
    A telegram_id already on users.agent_role='agent' is rejected as
    'already_agent'. Workers can re-apply (no role-block); admin reviews.
    """
    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()
    phone_raw = (phone_raw or "").strip()
    vehicle = (vehicle or "").strip()[:60]
    phone_norm = _normalize(phone_raw)

    if not first_name or not last_name:
        return {"ok": False, "error": "name_required"}
    if not phone_norm or len(phone_norm) < 9:
        return {"ok": False, "error": "phone_invalid"}

    existing_role = conn.execute(
        "SELECT agent_role FROM users WHERE telegram_id = ?", (telegram_id,)
    ).fetchone()
    if existing_role and (existing_role["agent_role"] or "") == "agent":
        return {"ok": False, "error": "already_agent"}

    pending = conn.execute(
        "SELECT id FROM pending_agents "
        "WHERE telegram_id = ? AND status = 'pending' LIMIT 1",
        (telegram_id,),
    ).fetchone()
    if pending:
        return {
            "ok": True,
            "application_id": pending["id"],
            "status": "pending",
            "deduped": True,
        }

    cur = conn.execute(
        "INSERT INTO pending_agents "
        "(telegram_id, first_name, last_name, phone_raw, phone_normalized, "
        " vehicle, status) "
        "VALUES (?, ?, ?, ?, ?, ?, 'pending')",
        (telegram_id, first_name, last_name, phone_raw, phone_norm,
         vehicle or None),
    )
    application_id = cur.lastrowid
    conn.commit()

    # Post to admin group + stash message_id on the row for later editing.
    msg_id = _post_to_admin_group(
        application_id=application_id,
        telegram_id=telegram_id,
        first_name=first_name,
        last_name=last_name,
        phone_raw=phone_raw,
        phone_norm=phone_norm,
        vehicle=vehicle,
    )
    if msg_id is not None:
        conn.execute(
            "UPDATE pending_agents SET notify_message_id = ? WHERE id = ?",
            (msg_id, application_id),
        )
        conn.commit()

    return {
        "ok": True,
        "application_id": application_id,
        "status": "pending",
        "deduped": False,
    }


def _post_to_admin_group(
    application_id: int,
    telegram_id: int,
    first_name: str,
    last_name: str,
    phone_raw: str,
    phone_norm: str,
    vehicle: Optional[str],
) -> Optional[int]:
    """Send the inline-keyboarded approval prompt. Returns message_id on
    success, None on failure (logged but non-fatal — admin can still
    approve via direct DB / API call)."""
    if not BOT_TOKEN or not AGENT_APPROVAL_GROUP_CHAT_ID:
        logger.warning("BOT_TOKEN / AGENT_APPROVAL_GROUP_CHAT_ID missing — agent app #%s "
                       "not announced to approval group", application_id)
        return None

    veh_line = f"\n🚚 Transport: <b>{_h(vehicle)}</b>" if vehicle else ""
    text = (
        f"🆕 <b>Yangi agent arizasi</b>\n\n"
        f"👤 Ism: <b>{_h(first_name)} {_h(last_name)}</b>\n"
        f"📞 Telefon: <code>{_h(phone_norm or phone_raw)}</code>\n"
        f"🆔 Telegram: <code>{telegram_id}</code>{veh_line}\n\n"
        f"Tasdiqlash uchun pastdagi tugmalardan birini bosing."
    )
    payload = {
        "chat_id": AGENT_APPROVAL_GROUP_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": {
            "inline_keyboard": [[
                {"text": "✅ Tasdiqlash",
                 "callback_data": f"appr:yes:{application_id}"},
                {"text": "❌ Rad qilish",
                 "callback_data": f"appr:no:{application_id}"},
            ]]
        },
    }
    try:
        resp = httpx.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json=payload,
            timeout=10,
        )
        j = resp.json()
        if j.get("ok"):
            return (j.get("result") or {}).get("message_id")
        logger.warning("Approval-group post failed for agent app #%s: %s",
                       application_id, j.get("description"))
        return None
    except Exception as e:
        logger.error("Approval-group post exception for agent app #%s: %s",
                     application_id, e)
        return None


def approve_application(
    conn,
    application_id: int,
    approver_telegram_id: int,
) -> dict:
    """Grant agent_role='agent' atomically. Returns:

        {ok: True, telegram_id: int, first_name: str, ...}
        {ok: False, error: 'not_pending' | 'not_found'}
    """
    row = conn.execute(
        "SELECT id, telegram_id, first_name, last_name, vehicle, status "
        "FROM pending_agents WHERE id = ?",
        (application_id,),
    ).fetchone()
    if not row:
        return {"ok": False, "error": "not_found"}
    if row["status"] != "pending":
        return {"ok": False, "error": "not_pending", "current_status": row["status"]}

    cursor = conn.execute(
        "UPDATE pending_agents "
        "SET status = 'approved', approved_by_telegram_id = ?, "
        "    approved_at = datetime('now') "
        "WHERE id = ? AND status = 'pending'",
        (approver_telegram_id, application_id),
    )
    if cursor.rowcount == 0:
        return {"ok": False, "error": "not_pending"}

    # Insert or update the users row. INSERT OR IGNORE first so a freshly-
    # signed-up agent gets a row even if they never opened the mini-app
    # before. Then UPDATE to set the role + vehicle.
    conn.execute(
        "INSERT OR IGNORE INTO users (telegram_id, is_approved) VALUES (?, 1)",
        (row["telegram_id"],),
    )
    conn.execute(
        "UPDATE users SET agent_role = 'agent', is_agent = 1, is_approved = 1, "
        "  first_name = COALESCE(first_name, ?), "
        "  last_name = COALESCE(last_name, ?), "
        "  vehicle = COALESCE(vehicle, ?) "
        "WHERE telegram_id = ?",
        (row["first_name"], row["last_name"], row["vehicle"] or None,
         row["telegram_id"]),
    )
    conn.commit()

    return {
        "ok": True,
        "telegram_id": row["telegram_id"],
        "first_name": row["first_name"],
        "last_name": row["last_name"],
        "vehicle": row["vehicle"] or "",
    }


def reject_application(
    conn,
    application_id: int,
    rejector_telegram_id: int,
    reason: Optional[str] = None,
) -> dict:
    """Mark pending_agents row as rejected. No users-row mutation."""
    cursor = conn.execute(
        "UPDATE pending_agents "
        "SET status = 'rejected', rejected_by_telegram_id = ?, "
        "    rejected_at = datetime('now'), reject_reason = ? "
        "WHERE id = ? AND status = 'pending'",
        (rejector_telegram_id, (reason or "").strip()[:200] or None,
         application_id),
    )
    if cursor.rowcount == 0:
        row = conn.execute(
            "SELECT status FROM pending_agents WHERE id = ?",
            (application_id,),
        ).fetchone()
        if not row:
            return {"ok": False, "error": "not_found"}
        return {"ok": False, "error": "not_pending", "current_status": row["status"]}
    conn.commit()

    row = conn.execute(
        "SELECT telegram_id, first_name, last_name FROM pending_agents WHERE id = ?",
        (application_id,),
    ).fetchone()
    return {
        "ok": True,
        "telegram_id": row["telegram_id"] if row else None,
        "first_name": row["first_name"] if row else "",
        "last_name": row["last_name"] if row else "",
    }


def _h(text: str) -> str:
    """HTML escape — local helper to avoid backend↔bot import cycle."""
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
