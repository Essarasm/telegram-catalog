"""Agent-panel endpoints, role-gated.

Roles: admin / cashier / agent / worker (`users.agent_role`).
- admin / cashier / agent: full panel access.
- worker: minimal — fxrate, client search, switch-client, debt readout,
  location-save. Stats / cash-handover / order timeline are blocked.

The MVP returns today / this-month realization volume for orders the agent
physically placed (orders.placed_by_telegram_id = agent.telegram_id). No
commission math yet — once we wire the Session T 3-tier producer table
into the DB, we'll add a /earnings endpoint that applies the rates.
"""
from datetime import date
from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from backend.database import get_db
from backend.services.agent_register import register_new_shop
from backend.services.client_search import (
    create_and_link_new_1c_client,
    relink_orphan_finance_rows,
    search_clients,
)
from backend.services.roles import role_in

router = APIRouter(prefix="/api/agent", tags=["agent"])


# Roles that may use the panel at all (any non-worker still has full access).
_PANEL_ROLES = {"admin", "cashier", "agent", "worker"}
_NON_WORKER_ROLES = {"admin", "cashier", "agent"}


def _is_agent(conn, telegram_id: int) -> bool:
    """Backwards-compat: any non-null panel role counts as 'agent' for the
    legacy is_agent gate."""
    return role_in(conn, telegram_id, _PANEL_ROLES)


@router.get("/stats")
def agent_stats(telegram_id: int = Query(...)):
    """Return today + this-month order counts and totals placed by this agent.
    Workers are excluded — they have no need for placed-order metrics.
    """
    conn = get_db()
    try:
        if not role_in(conn, telegram_id, _NON_WORKER_ROLES):
            return JSONResponse({"ok": False, "error": "not allowed"}, status_code=403)

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


# ── Agent panel: commission (Phase 1: flat 0.5%) ─────────────────────────

# Flat placeholder rate for Phase 1. Per-supplier tiered calc (0.5/1/2%) is
# blocked on Session T's "Walk all 43 active suppliers + assign supplier→tier"
# TODO — see `obsidian-vault/session-logs/Session T — log.md`. Don't move
# this constant into env or DB until Phase 2 lands; keeping it inline + grep-
# able makes the placeholder-vs-tiered pivot a single-PR change.
_PHASE1_COMMISSION_RATE = 0.005


@router.get("/commission")
def agent_commission(telegram_id: int = Query(...)):
    """Phase 1: flat 0.5% on collected money from this agent's registered
    shops, current month (Tashkent).

    Attribution: payments rows in `intake_payments` (status='confirmed') for
    any `client_id` that this agent registered via `agent_client_registrations`
    (status='created', linked_client_id NOT NULL). Legacy agents (Шерзод /
    Дилшод / Шухрат) with no panel-registered shops will see zero until an
    `agent_client_assignments` table is populated — captured as a follow-up.

    Returns dual-currency totals + a per-channel breakdown so the agent can
    see which intake stream (cash-handover / P2P / bank-transfer / legal-
    transfer) drove their commission this month.
    """
    conn = get_db()
    try:
        if not role_in(conn, telegram_id, _NON_WORKER_ROLES):
            return JSONResponse({"ok": False, "error": "not allowed"}, status_code=403)

        today = date.today()
        period_start = today.replace(day=1).isoformat()  # YYYY-MM-01
        period_label = today.strftime("%Y-%m")

        client_ids = [
            row["linked_client_id"]
            for row in conn.execute(
                "SELECT linked_client_id FROM agent_client_registrations "
                "WHERE agent_telegram_id = ? "
                "  AND linked_client_id IS NOT NULL "
                "  AND status = 'created'",
                (telegram_id,),
            ).fetchall()
        ]

        if not client_ids:
            return {
                "ok": True,
                "period": period_label,
                "rate_pct": _PHASE1_COMMISSION_RATE * 100,
                "rate_basis": "phase1_flat",
                "client_count": 0,
                "collected_uzs": 0,
                "collected_usd": 0.0,
                "commission_uzs": 0,
                "commission_usd": 0.0,
                "by_channel": [],
            }

        placeholders = ",".join("?" * len(client_ids))
        rows = conn.execute(
            f"""SELECT channel, currency, COALESCE(SUM(amount), 0) AS total
                FROM intake_payments
                WHERE status = 'confirmed'
                  AND date(COALESCE(confirmed_at, submitted_at)) >= ?
                  AND client_id IN ({placeholders})
                GROUP BY channel, currency""",
            [period_start, *client_ids],
        ).fetchall()

        collected_uzs = 0.0
        collected_usd = 0.0
        per_channel: dict = {}
        for r in rows:
            ch = r["channel"]
            cur = r["currency"]
            total = float(r["total"] or 0)
            slot = per_channel.setdefault(ch, {"uzs": 0.0, "usd": 0.0})
            if cur == "UZS":
                slot["uzs"] += total
                collected_uzs += total
            else:
                slot["usd"] += total
                collected_usd += total

        return {
            "ok": True,
            "period": period_label,
            "rate_pct": _PHASE1_COMMISSION_RATE * 100,
            "rate_basis": "phase1_flat",
            "client_count": len(client_ids),
            "collected_uzs": round(collected_uzs),
            "collected_usd": round(collected_usd, 2),
            "commission_uzs": round(collected_uzs * _PHASE1_COMMISSION_RATE),
            "commission_usd": round(collected_usd * _PHASE1_COMMISSION_RATE, 2),
            "by_channel": [
                {
                    "channel": ch,
                    "uzs": round(v["uzs"]),
                    "usd": round(v["usd"], 2),
                }
                for ch, v in sorted(per_channel.items())
            ],
        }
    finally:
        conn.close()


# ── Agent panel: vehicle profile (Block A) ───────────────────────────────

@router.get("/vehicle")
def agent_vehicle_get(telegram_id: int = Query(...)):
    """Return this agent's vehicle string (free-text). Empty/null = office-
    only agent (no delivery vehicle). Workers blocked."""
    conn = get_db()
    try:
        if not role_in(conn, telegram_id, _NON_WORKER_ROLES):
            return JSONResponse({"ok": False, "error": "not allowed"}, status_code=403)
        row = conn.execute(
            "SELECT vehicle FROM users WHERE telegram_id = ?", (telegram_id,)
        ).fetchone()
        return {"ok": True, "vehicle": (row["vehicle"] if row else "") or ""}
    finally:
        conn.close()


@router.post("/vehicle")
def agent_vehicle_set(payload: dict = Body(...)):
    """Agent updates their own vehicle descriptor. Free-text, max 60 chars.
    Empty string clears the field (office-only)."""
    telegram_id = payload.get("telegram_id")
    if not isinstance(telegram_id, int):
        return JSONResponse({"ok": False, "error": "telegram_id required"}, status_code=400)
    vehicle = (payload.get("vehicle") or "").strip()[:60]
    conn = get_db()
    try:
        if not role_in(conn, telegram_id, _NON_WORKER_ROLES):
            return JSONResponse({"ok": False, "error": "not allowed"}, status_code=403)
        conn.execute(
            "UPDATE users SET vehicle = ? WHERE telegram_id = ?",
            (vehicle or None, telegram_id),
        )
        conn.commit()
        return {"ok": True, "vehicle": vehicle}
    finally:
        conn.close()


# ── Agent panel: my deliveries (Block A — read-only; dispatch ships in Block B) ──

@router.get("/my-deliveries")
def agent_my_deliveries(telegram_id: int = Query(...)):
    """List orders dispatched to this agent. Active = assigned/in_transit;
    History = recent delivered/cancelled (last 30 days). Workers blocked.
    """
    conn = get_db()
    try:
        if not role_in(conn, telegram_id, _NON_WORKER_ROLES):
            return JSONResponse({"ok": False, "error": "not allowed"}, status_code=403)

        rows = conn.execute(
            """SELECT o.id, o.client_name, o.client_phone, o.item_count,
                      o.total_uzs, o.total_usd, o.delivery_status,
                      o.assigned_at, o.created_at,
                      ac.name AS whitelist_name, ac.client_id_1c
               FROM orders o
               LEFT JOIN allowed_clients ac ON ac.id = o.client_id
               WHERE o.assigned_agent_id = ?
                 AND o.delivery_status IN ('assigned', 'in_transit',
                                           'delivered', 'cancelled')
                 AND (o.delivery_status IN ('assigned', 'in_transit')
                      OR date(o.assigned_at) >= date('now', '-30 days'))
               ORDER BY
                 CASE o.delivery_status
                   WHEN 'in_transit' THEN 0
                   WHEN 'assigned' THEN 1
                   WHEN 'delivered' THEN 2
                   WHEN 'cancelled' THEN 3
                 END,
                 o.assigned_at DESC""",
            (telegram_id,),
        ).fetchall()

        active = []
        history = []
        for r in rows:
            entry = {
                "order_id": r["id"],
                "client_name": (r["whitelist_name"] or r["client_name"] or "—"),
                "client_1c": r["client_id_1c"] or None,
                "client_phone": r["client_phone"] or "",
                "item_count": r["item_count"] or 0,
                "total_uzs": round(float(r["total_uzs"]) or 0),
                "total_usd": round(float(r["total_usd"]) or 0, 2),
                "delivery_status": r["delivery_status"],
                "assigned_at": r["assigned_at"],
                "created_at": r["created_at"],
            }
            if r["delivery_status"] in ("assigned", "in_transit"):
                active.append(entry)
            else:
                history.append(entry)

        return {"ok": True, "active": active, "history": history}
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


@router.post("/register-client")
def agent_register_client(payload: dict = Body(...)):
    """Register a brand-new shop and immediately switch the agent into
    acting-as it. One round trip — registration + switch are atomic.

    Payload:
        {telegram_id: int, first_name: str, last_name: str, venue: str,
         phone: str, lat: float, lng: float}

    Roles: admin / cashier / agent. Workers are blocked — they don't
    register clients per the Agent charter's no-money-flow rule.

    Returns on success:
        {ok: True, registration_status: "created"|"linked_existing",
         client: {id, name, client_id_1c, phone}, relinked: {...}}

    Phone collision (any of phone_normalized / raqam_02 / raqam_03 on an
    existing allowed_clients row) → registration_status="linked_existing"
    and we switch into the pre-existing shop instead of creating a dupe.
    """
    telegram_id = payload.get("telegram_id")
    first_name = payload.get("first_name")
    last_name = payload.get("last_name")
    venue = payload.get("venue")
    phone = payload.get("phone")
    lat = payload.get("lat")
    lng = payload.get("lng")

    if not isinstance(telegram_id, int):
        return JSONResponse(
            {"ok": False, "error": "telegram_id required"}, status_code=400
        )

    conn = get_db()
    try:
        if not role_in(conn, telegram_id, _NON_WORKER_ROLES):
            return JSONResponse(
                {"ok": False, "error": "not allowed"}, status_code=403
            )

        result = register_new_shop(
            conn, telegram_id,
            first_name or "", last_name or "", venue or "",
            phone or "", lat, lng,
        )
        if result["status"] == "failed":
            return JSONResponse(
                {"ok": False, "error": result["error"]}, status_code=400
            )

        client_id = result["client_id"]
        target = conn.execute(
            "SELECT id, name, client_id_1c, phone_normalized "
            "FROM allowed_clients WHERE id = ?",
            (client_id,),
        ).fetchone()

        conn.execute(
            "UPDATE users SET client_id = ? WHERE telegram_id = ?",
            (client_id, telegram_id),
        )
        conn.execute(
            "INSERT INTO agent_client_switches (agent_telegram_id, client_id) "
            "VALUES (?, ?)",
            (telegram_id, client_id),
        )
        relink_counts = {}
        if target["client_id_1c"]:
            relink_counts = relink_orphan_finance_rows(
                conn, client_id, target["client_id_1c"]
            )
        conn.commit()
        return {
            "ok": True,
            "registration_status": result["status"],
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
