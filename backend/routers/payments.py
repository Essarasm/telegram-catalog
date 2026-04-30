"""Cashbook intake — Mini App agent endpoints (Session Z, Phase 1).

The cashier surface lives entirely in the bot FSM (bot/handlers/cashier.py).
Agents continue to use the Mini App agent panel; these endpoints let them
record cash handovers (status: pending_handover) that show up in the
cashier's queue for confirmation. P2P is Phase 2.
"""
import os

from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from backend.database import get_db, get_sibling_client_ids
from backend.services.payment_intake import (
    admin_cancel_payment,
    check_recent_duplicate,
    create_intake_payment,
    insert_intake_raw,
    list_my_pending,
    list_pending_for_client,
)

router = APIRouter(prefix="/api/payments", tags=["payments"])


def _admin_ids() -> set:
    raw = os.getenv("ADMIN_IDS", "")
    return {int(x.strip()) for x in raw.split(",") if x.strip().isdigit()}


def _format_name(first, last, username, fallback_id) -> str:
    parts = [(first or "").strip(), (last or "").strip()]
    name = " ".join(p for p in parts if p)
    if name:
        return name
    if username:
        return f"@{username}"
    return f"#{fallback_id}"


def _is_agent(conn, telegram_id: int) -> bool:
    """Cash-handover is for non-worker panel roles only. Workers can pick a
    client and read debt, but they do not record collections — that belongs
    to the agent / cashier flow."""
    from backend.services.roles import role_in
    return role_in(conn, telegram_id, {"admin", "cashier", "agent"})


@router.post("/agent-cash-handover")
def agent_cash_handover(payload: dict = Body(...)):
    """Agent records a cash handover destined for the cashier. One call may
    create up to two intake_payments rows (UZS leg + USD leg) — both
    with status 'pending_handover'. Soft-dedupe: if a similar row exists
    within the last hour and force is not set, return 409 with the match
    so the frontend can prompt 'submit anyway'.

    Payload:
        {
            telegram_id: int,
            client_id: int,
            uzs_amount: float (>=0),
            usd_amount: float (>=0),
            force: bool (optional, default False)
        }
    """
    try:
        telegram_id = int(payload.get("telegram_id") or 0)
        client_id = int(payload.get("client_id") or 0)
        uzs = float(payload.get("uzs_amount") or 0)
        usd = float(payload.get("usd_amount") or 0)
        force = bool(payload.get("force") or False)
    except (TypeError, ValueError):
        return JSONResponse(
            {"ok": False, "error": "invalid payload"}, status_code=400
        )
    if not telegram_id or not client_id:
        return JSONResponse(
            {"ok": False, "error": "telegram_id and client_id required"},
            status_code=400,
        )
    if uzs < 0 or usd < 0:
        return JSONResponse(
            {"ok": False, "error": "amounts must be non-negative"},
            status_code=400,
        )
    if uzs <= 0 and usd <= 0:
        return JSONResponse(
            {"ok": False, "error": "at least one of uzs/usd must be > 0"},
            status_code=400,
        )

    conn = get_db()
    try:
        if not _is_agent(conn, telegram_id):
            return JSONResponse(
                {"ok": False, "error": "not an agent"}, status_code=403
            )
        client = conn.execute(
            "SELECT id, name, client_id_1c FROM allowed_clients WHERE id = ?",
            (client_id,),
        ).fetchone()
        if not client:
            return JSONResponse(
                {"ok": False, "error": "client not found"}, status_code=404
            )

        # Dedupe — check each non-zero leg independently. Surface the first
        # match so the frontend can render a single confirmation prompt.
        if not force:
            for cur, amt in (("UZS", uzs), ("USD", usd)):
                if amt <= 0:
                    continue
                dup = check_recent_duplicate(conn, client_id, amt, cur)
                if dup:
                    return JSONResponse(
                        {
                            "ok": False,
                            "error": "duplicate",
                            "duplicate": dup,
                            "currency": cur,
                            "amount": amt,
                        },
                        status_code=409,
                    )

        created = []
        for cur, amt in (("UZS", uzs), ("USD", usd)):
            if amt <= 0:
                continue
            raw_id = insert_intake_raw(
                conn,
                submitter_telegram_id=telegram_id,
                submitter_role="agent",
                payload={
                    "channel": "cash_via_agent",
                    "client_id": client_id,
                    "amount": amt,
                    "currency": cur,
                    "source": "mini_app",
                },
            )
            pid = create_intake_payment(
                conn,
                raw_id=raw_id,
                client_id=client_id,
                amount=amt,
                currency=cur,
                channel="cash_via_agent",
                status="pending_handover",
                submitter_telegram_id=telegram_id,
                submitter_role="agent",
                handover_agent_id=telegram_id,
            )
            created.append({"id": pid, "currency": cur, "amount": amt})
        conn.commit()
    except Exception as e:
        conn.rollback()
        return JSONResponse(
            {"ok": False, "error": str(e)}, status_code=500
        )
    finally:
        conn.close()

    return {
        "ok": True,
        "created": created,
        "client_id": client_id,
        "client_name": client["client_id_1c"] or client["name"],
    }


@router.get("/pending-for-client")
def pending_for_client(
    telegram_id: int = Query(...),
    client_id: int = Query(None),
):
    """Pending + recent-confirmed intake_payments for a specific client.
    Used by the cabinet's Hisob-kitob pending section.

    If `client_id` is omitted, resolves to the requester's own linked
    client (users.client_id). With it, authorized for agents, admins,
    and the client owner (siblings of users.client_id)."""
    conn = get_db()
    try:
        user = conn.execute(
            "SELECT is_agent, client_id FROM users WHERE telegram_id = ?",
            (telegram_id,),
        ).fetchone()
        if client_id is None:
            if not user or not user["client_id"]:
                return {"ok": True, "items": []}
            client_id = user["client_id"]
        else:
            is_admin = telegram_id in _admin_ids()
            is_agent = bool(user and user["is_agent"])
            is_owner = False
            if user and user["client_id"]:
                siblings = get_sibling_client_ids(conn, user["client_id"]) or [user["client_id"]]
                is_owner = client_id in siblings
            if not (is_admin or is_agent or is_owner):
                return JSONResponse(
                    {"ok": False, "error": "not authorized"}, status_code=403
                )
        rows = list_pending_for_client(conn, client_id, days=14)
    finally:
        conn.close()

    items = []
    for r in rows:
        submitter_name = _format_name(
            r.get("sub_first"), r.get("sub_last"), r.get("sub_username"),
            r["submitter_telegram_id"],
        )
        agent_name = None
        if r.get("handover_agent_id"):
            agent_name = _format_name(
                r.get("agent_first"), r.get("agent_last"), r.get("agent_username"),
                r["handover_agent_id"],
            )
        items.append({
            "id": r["id"],
            "amount": r["amount"],
            "currency": r["currency"],
            "channel": r["channel"],
            "status": r["status"],
            "submitted_at": r["submitted_at"],
            "confirmed_at": r["confirmed_at"],
            "submitter_role": r["submitter_role"],
            "submitter_name": submitter_name,
            "agent_name": agent_name,
        })
    return {"ok": True, "items": items}


@router.get("/my-pending")
def my_pending(telegram_id: int = Query(...)):
    """Pending submissions made by this agent — pending_handover or
    pending_review. Used by the agent panel's "Mening yuborganlarim"
    widget so they know what the cashier hasn't acted on yet."""
    conn = get_db()
    try:
        if not _is_agent(conn, telegram_id):
            return JSONResponse(
                {"ok": False, "error": "not an agent"}, status_code=403
            )
        items = list_my_pending(conn, telegram_id, limit=30)
    finally:
        conn.close()
    return {
        "ok": True,
        "items": [
            {
                "id": p["id"],
                "client_id": p["client_id"],
                "client_name": p.get("client_id_1c") or p.get("client_name"),
                "amount": p["amount"],
                "currency": p["currency"],
                "channel": p["channel"],
                "status": p["status"],
                "submitted_at": p["submitted_at"],
            }
            for p in items
        ],
    }


@router.post("/cancel")
def admin_cancel(payload: dict = Body(...)):
    """Admin-only soft-cancel of any non-rejected intake_payment row.
    Flips status → 'rejected' (audit row in payment_intake_raw stays;
    no actual deletion). Used by the Mini-App admin button + the
    `/cashbook` bot command.

    Payload: {telegram_id: int, payment_id: int, reason?: str}
    """
    try:
        telegram_id = int(payload.get("telegram_id") or 0)
        payment_id = int(payload.get("payment_id") or 0)
        reason = (payload.get("reason") or "").strip() or None
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "invalid payload"}, status_code=400)
    if not telegram_id or not payment_id:
        return JSONResponse(
            {"ok": False, "error": "telegram_id and payment_id required"},
            status_code=400,
        )
    if telegram_id not in _admin_ids():
        return JSONResponse({"ok": False, "error": "admin only"}, status_code=403)

    conn = get_db()
    try:
        try:
            row = admin_cancel_payment(conn, payment_id, telegram_id, reason)
        except ValueError as e:
            conn.rollback()
            return JSONResponse({"ok": False, "error": str(e)}, status_code=404)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "payment": row}
