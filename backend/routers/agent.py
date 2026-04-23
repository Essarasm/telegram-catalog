"""Agent-only dashboard endpoints (Phase 1: simple stats).

The MVP returns today / this-month realization volume for orders the agent
physically placed (orders.placed_by_telegram_id = agent.telegram_id). No
commission math yet — once we wire the Session T 3-tier producer table
into the DB, we'll add a /earnings endpoint that applies the rates.
"""
from datetime import date
from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from backend.admin_auth import check_admin_key
from backend.database import get_db
from backend.services.client_search import (
    create_and_link_new_1c_client,
    relink_orphan_finance_rows,
    search_clients,
)

router = APIRouter(prefix="/api/agent", tags=["agent"])


def _is_agent(conn, telegram_id: int) -> bool:
    row = conn.execute(
        "SELECT is_agent FROM users WHERE telegram_id = ?", (telegram_id,),
    ).fetchone()
    return bool(row and row["is_agent"])


@router.get("/stats")
def agent_stats(telegram_id: int = Query(...)):
    """Return today + this-month order counts and totals placed by this agent."""
    conn = get_db()
    try:
        if not _is_agent(conn, telegram_id):
            return JSONResponse({"ok": False, "error": "not an agent"}, status_code=403)

        today_str = date.today().isoformat()
        month_prefix = today_str[:7]  # 'YYYY-MM'

        today = conn.execute(
            """SELECT COUNT(*) AS n,
                      COALESCE(SUM(total_uzs), 0) AS uzs,
                      COALESCE(SUM(total_usd), 0) AS usd
               FROM orders
               WHERE placed_by_telegram_id = ?
                 AND date(created_at) = ?""",
            (telegram_id, today_str),
        ).fetchone()

        month = conn.execute(
            """SELECT COUNT(*) AS n,
                      COALESCE(SUM(total_uzs), 0) AS uzs,
                      COALESCE(SUM(total_usd), 0) AS usd,
                      COUNT(DISTINCT client_id) AS clients
               FROM orders
               WHERE placed_by_telegram_id = ?
                 AND substr(created_at, 1, 7) = ?""",
            (telegram_id, month_prefix),
        ).fetchone()

        # Recent orders (last 5 placed by this agent)
        recent = conn.execute(
            """SELECT o.id, o.created_at, o.total_uzs, o.total_usd,
                      o.item_count, ac.client_id_1c
               FROM orders o
               LEFT JOIN allowed_clients ac ON ac.id = o.client_id
               WHERE o.placed_by_telegram_id = ?
               ORDER BY o.created_at DESC
               LIMIT 5""",
            (telegram_id,),
        ).fetchall()

        return {
            "ok": True,
            "is_agent": True,
            "today": {
                "order_count": today["n"],
                "total_uzs": round(float(today["uzs"]) or 0),
                "total_usd": round(float(today["usd"]) or 0, 2),
            },
            "month": {
                "order_count": month["n"],
                "total_uzs": round(float(month["uzs"]) or 0),
                "total_usd": round(float(month["usd"]) or 0, 2),
                "unique_clients": month["clients"],
            },
            "recent_orders": [
                {
                    "id": r["id"],
                    "created_at": r["created_at"],
                    "client_1c": r["client_id_1c"] or "—",
                    "item_count": r["item_count"],
                    "total_uzs": round(float(r["total_uzs"]) or 0),
                    "total_usd": round(float(r["total_usd"]) or 0, 2),
                }
                for r in recent
            ],
        }
    finally:
        conn.close()


# ── Agent panel: client switcher ─────────────────────────────────────────

@router.get("/search-clients")
def agent_search_clients(
    telegram_id: int = Query(...),
    q: str = Query(..., min_length=1),
    limit: int = Query(30, ge=1, le=100),
):
    """Search allowed_clients + 1C client_balances by name / client_id_1c.
    Used by the agent panel's search bar. Gated on is_agent=1.
    """
    conn = get_db()
    try:
        if not _is_agent(conn, telegram_id):
            return JSONResponse({"ok": False, "error": "not an agent"}, status_code=403)
    finally:
        conn.close()
    results = search_clients(q, limit=limit)
    return {"ok": True, **results}


@router.post("/switch-client")
def agent_switch_client(payload: dict = Body(...)):
    """Link the agent's account to a client (acting-as). Mirrors /testclient.

    Payload:
        {telegram_id: int, client_id: int}       — link to an allowed_clients row
        {telegram_id: int, client_name_1c: str}  — auto-create allowed_clients
                                                    from 1C, then link
        {telegram_id: int, clear: true}          — unlink (return to agent home)

    Every successful switch writes an agent_client_switches audit row so the
    recent-clients list on the agent home stays fresh.
    """
    telegram_id = payload.get("telegram_id")
    if not isinstance(telegram_id, int):
        return JSONResponse({"ok": False, "error": "telegram_id required"}, status_code=400)

    conn = get_db()
    try:
        if not _is_agent(conn, telegram_id):
            return JSONResponse({"ok": False, "error": "not an agent"}, status_code=403)

        if payload.get("clear"):
            conn.execute(
                "UPDATE users SET client_id = NULL WHERE telegram_id = ?",
                (telegram_id,),
            )
            conn.commit()
            return {"ok": True, "cleared": True}

        client_id = payload.get("client_id")
        client_name_1c = payload.get("client_name_1c")

        if client_id is None and client_name_1c:
            created = create_and_link_new_1c_client(client_name_1c, telegram_id)
            if not created:
                return JSONResponse(
                    {"ok": False, "error": f"'{client_name_1c}' not found in 1C"},
                    status_code=404,
                )
            client_id = created["id"]

        if not isinstance(client_id, int):
            return JSONResponse(
                {"ok": False, "error": "client_id or client_name_1c required"},
                status_code=400,
            )

        target = conn.execute(
            "SELECT id, name, client_id_1c, phone_normalized "
            "FROM allowed_clients WHERE id = ?",
            (client_id,),
        ).fetchone()
        if not target:
            return JSONResponse(
                {"ok": False, "error": f"client {client_id} not found"},
                status_code=404,
            )

        conn.execute(
            "UPDATE users SET client_id = ? WHERE telegram_id = ?",
            (client_id, telegram_id),
        )
        conn.execute(
            "INSERT INTO agent_client_switches (agent_telegram_id, client_id) "
            "VALUES (?, ?)",
            (telegram_id, client_id),
        )
        # Heal orphan finance rows for this client on every switch — some
        # 1C imports leave client_id NULL on new rows; relinking here makes
        # the cabinet show the full picture.
        relink_counts = {}
        if target["client_id_1c"]:
            relink_counts = relink_orphan_finance_rows(
                conn, client_id, target["client_id_1c"]
            )
        conn.commit()
        return {
            "ok": True,
            "client": {
                "id": target["id"],
                "name": target["name"],
                "client_id_1c": target["client_id_1c"],
                "phone": target["phone_normalized"] or "",
            },
            "relinked": relink_counts,
        }
    finally:
        conn.close()


@router.get("/debug/client-lookup")
def agent_debug_client_lookup(
    q: str = Query(..., min_length=1),
    admin_key: str = Query(""),
):
    """TEMPORARY diagnostic endpoint — dumps every row touching the query
    across allowed_clients / client_balances / client_debts / real_orders
    so the duplicate-dedup can be planned safely. Remove after repair."""
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "bad admin_key"}, status_code=403)

    pat = f"%{q}%"
    conn = get_db()
    conn.create_function("LOWER", 1, lambda s: s.lower() if s else s)
    try:
        allowed = [dict(r) for r in conn.execute(
            """SELECT id, client_id_1c, name, phone_normalized, source_sheet,
                      status, matched_telegram_id, source_1c, source_master,
                      last_master_synced_at
               FROM allowed_clients
               WHERE LOWER(client_id_1c) LIKE LOWER(?)
                  OR LOWER(name) LIKE LOWER(?)
               ORDER BY id""",
            (pat, pat),
        ).fetchall()]

        balances = [dict(r) for r in conn.execute(
            """SELECT client_id, client_name_1c, COUNT(*) AS rows,
                      SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) AS orphan_rows,
                      MAX(period_end) AS latest_period
               FROM client_balances
               WHERE LOWER(client_name_1c) LIKE LOWER(?)
               GROUP BY client_id, client_name_1c
               ORDER BY client_id""",
            (pat,),
        ).fetchall()]

        debts = [dict(r) for r in conn.execute(
            """SELECT client_id, client_name_1c, COUNT(*) AS rows,
                      SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) AS orphan_rows
               FROM client_debts
               WHERE LOWER(client_name_1c) LIKE LOWER(?)
               GROUP BY client_id, client_name_1c
               ORDER BY client_id""",
            (pat,),
        ).fetchall()]

        real_orders = [dict(r) for r in conn.execute(
            """SELECT client_id, client_name_1c, COUNT(*) AS rows,
                      SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) AS orphan_rows
               FROM real_orders
               WHERE LOWER(client_name_1c) LIKE LOWER(?)
               GROUP BY client_id, client_name_1c
               ORDER BY client_id""",
            (pat,),
        ).fetchall()]

        payments = [dict(r) for r in conn.execute(
            """SELECT client_id, client_name_1c, COUNT(*) AS rows,
                      SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) AS orphan_rows
               FROM client_payments
               WHERE LOWER(client_name_1c) LIKE LOWER(?)
               GROUP BY client_id, client_name_1c
               ORDER BY client_id""",
            (pat,),
        ).fetchall()]

        users_linked = [dict(r) for r in conn.execute(
            """SELECT u.telegram_id, u.phone, u.first_name, u.client_id,
                      u.is_agent, ac.client_id_1c, ac.name
               FROM users u
               LEFT JOIN allowed_clients ac ON ac.id = u.client_id
               WHERE u.client_id IN (SELECT id FROM allowed_clients
                                     WHERE LOWER(client_id_1c) LIKE LOWER(?)
                                        OR LOWER(name) LIKE LOWER(?))""",
            (pat, pat),
        ).fetchall()]

        recent_switches = [dict(r) for r in conn.execute(
            """SELECT s.*, ac.client_id_1c, ac.name
               FROM agent_client_switches s
               LEFT JOIN allowed_clients ac ON ac.id = s.client_id
               WHERE s.client_id IN (SELECT id FROM allowed_clients
                                     WHERE LOWER(client_id_1c) LIKE LOWER(?)
                                        OR LOWER(name) LIKE LOWER(?))
               ORDER BY s.switched_at DESC LIMIT 20""",
            (pat, pat),
        ).fetchall()]
    finally:
        conn.close()

    return {
        "ok": True,
        "query": q,
        "allowed_clients": allowed,
        "client_balances_grouped": balances,
        "client_debts_grouped": debts,
        "real_orders_grouped": real_orders,
        "client_payments_grouped": payments,
        "users_linked_to_these_clients": users_linked,
        "recent_agent_switches": recent_switches,
    }


@router.post("/debug/relink-orphans")
def agent_debug_relink_orphans(
    admin_key: str = Query(""),
    dry_run: int = Query(1, ge=0, le=1),
):
    """TEMPORARY admin endpoint: relink orphan finance rows to
    allowed_clients by exact client_name_1c = client_id_1c match.

    Skips structural non-client entries (Наличка/Организации/etc) per the
    1C exclusion list. Safe: only touches rows where client_id IS NULL.

    dry_run=1 (default): report what WOULD be relinked, no writes.
    dry_run=0: actually execute the relink.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "bad admin_key"}, status_code=403)

    exclude_patterns = [
        "%Наличка%", "%НАЛИЧКА%", "%наличка%",
        "%Организации%", "%ОРГАНИЗАЦИИ%",
        "%Исправление%", "%ИСПРАВЛЕНИЕ%",
        "%Стройка%", "%СТРОЙКА%",
    ]
    exclude_where = " AND ".join(["client_name_1c NOT LIKE ?"] * len(exclude_patterns))

    conn = get_db()
    results = {}
    try:
        for table in ("client_balances", "real_orders",
                      "client_payments", "client_debts"):
            # Find orphan rows that have an exact allowed_clients match
            rows = conn.execute(
                f"""SELECT t.client_name_1c, ac.id AS target_client_id,
                           COUNT(*) AS orphan_rows
                    FROM {table} t
                    JOIN allowed_clients ac
                      ON ac.client_id_1c = t.client_name_1c
                     AND COALESCE(ac.status, 'active') != 'merged'
                    WHERE t.client_id IS NULL
                      AND {exclude_where}
                    GROUP BY t.client_name_1c, ac.id
                    ORDER BY orphan_rows DESC""",
                tuple(exclude_patterns),
            ).fetchall()

            matched_names = len(rows)
            matched_rows = sum(r["orphan_rows"] for r in rows)
            top = [dict(r) for r in rows[:5]]

            if not dry_run and rows:
                for r in rows:
                    conn.execute(
                        f"UPDATE {table} SET client_id = ? "
                        f"WHERE client_name_1c = ? AND client_id IS NULL",
                        (r["target_client_id"], r["client_name_1c"]),
                    )
            results[table] = {
                "unique_clients_matched": matched_names,
                "rows_to_relink" if dry_run else "rows_relinked": matched_rows,
                "top_samples": top,
            }
        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    return {"ok": True, "dry_run": bool(dry_run), "results": results}


@router.get("/recent-clients")
def agent_recent_clients(
    telegram_id: int = Query(...),
    limit: int = Query(5, ge=1, le=20),
):
    """Return the agent's most-recently-switched-to distinct clients."""
    conn = get_db()
    try:
        if not _is_agent(conn, telegram_id):
            return JSONResponse({"ok": False, "error": "not an agent"}, status_code=403)

        rows = conn.execute(
            """SELECT s.client_id,
                      MAX(s.switched_at) AS last_switch,
                      ac.name, ac.client_id_1c, ac.phone_normalized
               FROM agent_client_switches s
               LEFT JOIN allowed_clients ac ON ac.id = s.client_id
               WHERE s.agent_telegram_id = ?
                 AND ac.id IS NOT NULL
               GROUP BY s.client_id
               ORDER BY last_switch DESC
               LIMIT ?""",
            (telegram_id, limit),
        ).fetchall()

        return {
            "ok": True,
            "recent": [
                {
                    "client_id": r["client_id"],
                    "name": r["name"],
                    "client_id_1c": r["client_id_1c"],
                    "phone": r["phone_normalized"] or "",
                    "last_switch": r["last_switch"],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()
