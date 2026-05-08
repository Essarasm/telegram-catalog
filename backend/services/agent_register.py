"""Agent-initiated shop registration — audit-first, phone-collision-safe.

Agents register new shops on-site (visit, fill first/last name + venue +
phone + GPS, switch into acting-as that shop, place an order). The flow
is gated to non-worker roles in `routers/agent.py`.

Audit-first per the project's zero-data-loss rule: every attempt writes a
row to `agent_client_registrations` BEFORE we touch `allowed_clients`. If
the phone collides with an existing whitelisted shop (any of the three
contact slots), we link to the existing row instead of creating a duplicate.

Storage in `allowed_clients`:
    - `name`        = "<first_name> <last_name>" (matches 1C single-name convention)
    - `moljal`      = venue/orientir (existing Master-owned column)
    - `source_sheet`= 'agent_panel'
    - `segment`     = 'shop'
    - `gps_*`       = captured location, set_by_role='agent', set_by_tg_id=<agent>
"""
from typing import Optional

from backend.services.phone_slots import _normalize


def _check_phone_collision(conn, phone_norm: str) -> Optional[dict]:
    """Find an existing allowed_clients row with this phone in any of the
    three contact slots. Returns {id, name, client_id_1c} or None."""
    if not phone_norm:
        return None
    row = conn.execute(
        """SELECT id, name, client_id_1c
           FROM allowed_clients
           WHERE COALESCE(status, 'active') != 'merged'
             AND (phone_normalized = ? OR raqam_02 = ? OR raqam_03 = ?)
           ORDER BY id LIMIT 1""",
        (phone_norm, phone_norm, phone_norm),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "name": row["name"],
        "client_id_1c": row["client_id_1c"],
    }


def register_new_shop(
    conn,
    agent_tg_id: int,
    first_name: str,
    last_name: str,
    venue: str,
    phone_raw: str,
    lat: Optional[float],
    lng: Optional[float],
) -> dict:
    """Register a new shop initiated by an agent in the field.

    Returns one of:
        {"status": "created",         "client_id": <id>, "client": {...}}
        {"status": "linked_existing", "client_id": <id>, "client": {...}}
        {"status": "failed",          "error": "<reason>"}

    Caller is responsible for committing the connection.
    """
    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()
    venue = (venue or "").strip()
    phone_norm = _normalize(phone_raw)

    if len(first_name) < 2:
        return {"status": "failed", "error": "first_name must be at least 2 characters"}
    if len(last_name) < 2:
        return {"status": "failed", "error": "last_name must be at least 2 characters"}
    if len(venue) < 2:
        return {"status": "failed", "error": "venue must be at least 2 characters"}
    if len(phone_norm) < 9:
        return {"status": "failed", "error": "phone must contain 9+ digits"}
    if lat is None or lng is None:
        return {"status": "failed", "error": "location (lat, lng) required"}

    full_name = f"{first_name} {last_name}"

    # Audit row first — every attempt is recorded before we touch the
    # whitelist. Status flips to 'created' or 'linked_existing' below.
    cur = conn.execute(
        """INSERT INTO agent_client_registrations
           (agent_telegram_id, shop_name, first_name, last_name, venue,
            phone_raw, phone_normalized,
            gps_latitude, gps_longitude, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')""",
        (agent_tg_id, full_name, first_name, last_name, venue,
         phone_raw or "", phone_norm, lat, lng),
    )
    audit_id = cur.lastrowid

    existing = _check_phone_collision(conn, phone_norm)
    if existing:
        conn.execute(
            "UPDATE agent_client_registrations SET status = 'linked_existing', "
            "linked_client_id = ? WHERE id = ?",
            (existing["id"], audit_id),
        )
        return {
            "status": "linked_existing",
            "client_id": existing["id"],
            "client": existing,
        }

    # Clean phone — create a new whitelist row tagged as agent-panel-sourced.
    # `moljal` holds the venue/orientir (existing Master-owned column).
    conn.execute(
        """INSERT INTO allowed_clients
           (phone_normalized, name, moljal, source_sheet, status, segment,
            gps_latitude, gps_longitude, gps_set_at, gps_set_by_tg_id, gps_set_by_role)
           VALUES (?, ?, ?, 'agent_panel', 'active', 'shop',
                   ?, ?, datetime('now'), ?, 'agent')""",
        (phone_norm, full_name, venue, lat, lng, agent_tg_id),
    )
    new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        "UPDATE agent_client_registrations SET status = 'created', "
        "linked_client_id = ? WHERE id = ?",
        (new_id, audit_id),
    )
    return {
        "status": "created",
        "client_id": new_id,
        "client": {
            "id": new_id,
            "name": full_name,
            "client_id_1c": None,
        },
    }
