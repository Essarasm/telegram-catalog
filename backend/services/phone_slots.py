"""Mini-app captured phones → first empty raqam_02/raqam_03 slot.

Implements the policy that mini-app data is authoritative: when a Telegram
user registers with a phone that differs from their linked client's primary
number, the new number is parked on the client row (slot 2 → slot 3) so it
shows up in the agent panel and survives the next Master export round-trip.

If both raqam_02 and raqam_03 are already taken by other numbers, the row
is flagged needs_review = 1 instead of silently dropping the new contact.
"""
import re

from backend.database import get_db


def _normalize(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    return digits[-9:] if len(digits) >= 9 else digits


def fill_empty_slot(conn, client_id: int, new_phone_raw: str) -> str:
    """Park a new phone on an allowed_clients row in the first empty contact slot.

    Returns one of: 'already_present', 'filled_02', 'filled_03', 'no_slot', 'noop'.
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
    Idempotent — safe to re-run."""
    conn = get_db()
    rows = conn.execute(
        """SELECT u.telegram_id, u.phone, u.client_id
           FROM users u
           WHERE u.phone IS NOT NULL AND u.phone != ''
             AND u.client_id IS NOT NULL"""
    ).fetchall()
    totals = {"scanned": 0, "filled_02": 0, "filled_03": 0,
              "already_present": 0, "no_slot": 0, "noop": 0}
    for r in rows:
        totals["scanned"] += 1
        result = fill_empty_slot(conn, r["client_id"], r["phone"])
        totals[result] = totals.get(result, 0) + 1
    conn.commit()
    conn.close()
    return totals
