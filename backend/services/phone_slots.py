"""Mini-app captured phones → first empty raqam_02/raqam_03 slot.

Implements the policy that mini-app data is authoritative: when a Telegram
user registers with a phone that differs from their linked client's primary
number, the new number is parked on the client row (slot 2 → slot 3) so it
shows up in the agent panel and survives the next Master export round-trip.

If both raqam_02 and raqam_03 are already taken by other numbers, the row
is flagged needs_review = 1 instead of silently dropping the new contact.

Writer guards (added 2026-05-12 after the Нажмиддин/Умиджон incident):
  - never park a phone that is already the primary line of another
    tg-bound allowed_clients row — that phone belongs to a real person;
  - never park a phone owned by a staff role (admin/cashier/agent/worker)
    — agent-switch flows leave users.client_id pointing at the serviced
    shop, which would otherwise leak the staffer's phone onto the client.
"""
from __future__ import annotations

import re

from backend.database import get_db


_STAFF_ROLES = {"admin", "cashier", "agent", "worker"}


def _normalize(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    return digits[-9:] if len(digits) >= 9 else digits


def _is_owned_by_other_tg_user(conn, norm: str, client_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM allowed_clients "
        "WHERE phone_normalized = ? AND id != ? "
        "  AND matched_telegram_id IS NOT NULL "
        "  AND COALESCE(status,'active') = 'active' "
        "LIMIT 1",
        (norm, client_id),
    ).fetchone()
    return row is not None


def _user_has_staff_role(conn, telegram_id: int) -> bool:
    if not telegram_id:
        return False
    from backend.services.roles import role_in
    return role_in(conn, telegram_id, _STAFF_ROLES)


def fill_empty_slot(conn, client_id: int, new_phone_raw: str,
                    *, user_telegram_id: int | None = None) -> str:
    """Park a new phone on an allowed_clients row in the first empty contact slot.

    Returns one of: 'already_present', 'filled_02', 'filled_03', 'no_slot',
                    'owned_by_other_user', 'staff_phone', 'noop'.
    Caller is responsible for committing the connection.
    """
    norm = _normalize(new_phone_raw)
    if not norm or not client_id:
        return "noop"
    row = conn.execute(
        "SELECT phone_normalized, raqam_02, raqam_03 FROM allowed_clients WHERE id = ?",
        (client_id,),
    ).fetchone()
    if not row:
        return "noop"
    known = {
        (row["phone_normalized"] or "").strip(),
        (row["raqam_02"] or "").strip(),
        (row["raqam_03"] or "").strip(),
    }
    if norm in known:
        return "already_present"
    if _is_owned_by_other_tg_user(conn, norm, client_id):
        return "owned_by_other_user"
    if _user_has_staff_role(conn, user_telegram_id or 0):
        return "staff_phone"
    if not (row["raqam_02"] or "").strip():
        conn.execute(
            "UPDATE allowed_clients SET raqam_02 = ? WHERE id = ?",
            (norm, client_id),
        )
        return "filled_02"
    if not (row["raqam_03"] or "").strip():
        conn.execute(
            "UPDATE allowed_clients SET raqam_03 = ? WHERE id = ?",
            (norm, client_id),
        )
        return "filled_03"
    conn.execute(
        "UPDATE allowed_clients SET needs_review = 1 WHERE id = ?",
        (client_id,),
    )
    return "no_slot"


def backfill_from_users() -> dict:
    """One-shot pass: for every users row with a phone different from the
    linked client's known numbers, park the user's phone on the client row.
    Idempotent — safe to re-run. Staff phones and phones already owned by
    another tg-bound client are skipped (see writer guards above)."""
    conn = get_db()
    rows = conn.execute(
        """SELECT u.telegram_id, u.phone, u.client_id
           FROM users u
           WHERE u.phone IS NOT NULL AND u.phone != ''
             AND u.client_id IS NOT NULL"""
    ).fetchall()
    totals = {"scanned": 0, "filled_02": 0, "filled_03": 0,
              "already_present": 0, "no_slot": 0,
              "owned_by_other_user": 0, "staff_phone": 0, "noop": 0}
    for r in rows:
        totals["scanned"] += 1
        result = fill_empty_slot(
            conn, r["client_id"], r["phone"],
            user_telegram_id=r["telegram_id"],
        )
        totals[result] = totals.get(result, 0) + 1
    conn.commit()
    conn.close()
    return totals
