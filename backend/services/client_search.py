"""Shared client search used by both the bot's /testclient command and the
agent panel mini app.

Two-tier result:
  • whitelisted — already in allowed_clients, tap to link (fast path)
  • new_1c      — only in client_balances, needs allowed_clients insert first
"""
import unicodedata
from collections import OrderedDict
from typing import Optional

from backend.database import get_db


def _normalize(q: str) -> str:
    return unicodedata.normalize("NFC", q).strip().lower()


def search_clients(query: str, limit: int = 15, new_limit: int = 5) -> dict:
    """Search allowed_clients + client_balances by name / client_id_1c.

    Returns:
        {
            "whitelisted": [
                {id, name, client_id_1c, phone, balance_count}
            ],
            "new_1c": [
                {client_name_1c, balance_count, latest_period}
            ],
        }

    Whitelisted results are deduplicated by client_id_1c so multi-phone
    siblings roll up into one entry (the first matching allowed_clients row
    is returned as the canonical anchor).
    """
    q = _normalize(query)
    if not q:
        return {"whitelisted": [], "new_1c": []}
    search = f"%{q}%"

    conn = get_db()
    try:
        matches = conn.execute(
            """SELECT ac.id, ac.name, ac.client_id_1c, ac.phone_normalized,
                      (SELECT COUNT(*) FROM client_balances
                       WHERE client_id = ac.id) as bal_count
               FROM allowed_clients ac
               WHERE (LOWER(ac.client_id_1c) LIKE ? OR LOWER(ac.name) LIKE ?
                  OR ac.id IN (
                      SELECT DISTINCT client_id FROM client_balances
                      WHERE LOWER(client_name_1c) LIKE ? AND client_id IS NOT NULL
                  ))
                 AND COALESCE(ac.status, 'active') != 'merged'
                 AND ac.client_id_1c IS NOT NULL AND ac.client_id_1c != ''
               ORDER BY bal_count DESC
               LIMIT ?""",
            (search, search, search, limit),
        ).fetchall()

        cb_only = conn.execute(
            """SELECT DISTINCT cb.client_name_1c,
                      COUNT(*) as bal_count,
                      MAX(cb.period_end) as latest_period
               FROM client_balances cb
               WHERE LOWER(cb.client_name_1c) LIKE ?
                 AND (cb.client_id IS NULL
                      OR cb.client_id NOT IN (SELECT id FROM allowed_clients))
               GROUP BY cb.client_name_1c
               LIMIT ?""",
            (search, new_limit),
        ).fetchall()
    finally:
        conn.close()

    grouped = OrderedDict()
    for m in matches:
        cid = (m["client_id_1c"] or "").strip()
        key = cid if cid else f"__no1c_{m['id']}"
        if key in grouped:
            continue
        grouped[key] = {
            "id": m["id"],
            "name": m["name"],
            "client_id_1c": m["client_id_1c"],
            "phone": m["phone_normalized"] or "",
            "balance_count": m["bal_count"],
        }

    new_1c = [
        {
            "client_name_1c": r["client_name_1c"],
            "balance_count": r["bal_count"],
            "latest_period": r["latest_period"],
        }
        for r in cb_only
    ]
    return {"whitelisted": list(grouped.values()), "new_1c": new_1c}


def create_and_link_new_1c_client(
    client_name_1c: str, agent_telegram_id: int
) -> Optional[dict]:
    """Insert a new allowed_clients row for a 1C-only client and link every
    financial table to it (mirrors the `tc:add:` callback path).

    Returns the new {id, name, client_id_1c} or None if client_name_1c has
    no corresponding client_balances rows.
    """
    conn = get_db()
    try:
        cb_exists = conn.execute(
            "SELECT COUNT(*) FROM client_balances WHERE client_name_1c = ?",
            (client_name_1c,),
        ).fetchone()[0]
        if not cb_exists:
            return None

        existing = conn.execute(
            "SELECT id FROM allowed_clients WHERE client_id_1c = ? LIMIT 1",
            (client_name_1c,),
        ).fetchone()
        if existing:
            new_id = existing["id"]
        else:
            conn.execute(
                "INSERT INTO allowed_clients (phone_normalized, name, "
                "client_id_1c, source_sheet, status) "
                "VALUES (?, ?, ?, ?, ?)",
                ("", client_name_1c, client_name_1c, "agent_panel", "active"),
            )
            new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            for table in ("client_balances", "real_orders",
                          "client_payments", "client_debts"):
                conn.execute(
                    f"UPDATE {table} SET client_id = ? "
                    f"WHERE client_name_1c = ? AND client_id IS NULL",
                    (new_id, client_name_1c),
                )
        conn.commit()
        return {
            "id": new_id,
            "name": client_name_1c,
            "client_id_1c": client_name_1c,
        }
    finally:
        conn.close()
