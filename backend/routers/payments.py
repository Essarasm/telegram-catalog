"""Cashbook intake — Mini App agent endpoints (Session Z, Phase 1+2).

The cashier surface lives entirely in the bot FSM (bot/handlers/cashier.py).
Agents continue to use the Mini App agent panel; these endpoints let them
record cash handovers (status: pending_handover) that show up in the
cashier's queue for confirmation. P2P is Phase 2.

Phase 2 also adds the legal-entity bank transfer flow: agent submits Stage 1
data (amount, category, client's legal entity), creating a legal_transfers
row + cashier-group notification. Uncle picks the supplier in Stage 2 (next
commit's inline keyboard).
"""
import logging
import os

import httpx
from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from backend.database import get_db, get_sibling_client_ids
from backend.services.payment_intake import (
    admin_cancel_payment,
    check_recent_duplicate,
    create_intake_payment,
    create_legal_transfer,
    get_category,
    insert_intake_raw,
    list_active_categories,
    list_my_pending,
    list_pending_for_client,
    list_suppliers_in_category,
)

logger = logging.getLogger(__name__)
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CASHIER_GROUP_CHAT_ID = os.getenv("CASHIER_GROUP_CHAT_ID", "")
# Dedicated group for legal-entity bank transfer flow (Stage 1 notifications,
# Stage 2 supplier picker, future stages). Falls back to the cashier group if
# unset so older deployments don't break.
LEGAL_TRANSFER_GROUP_CHAT_ID = (
    os.getenv("LEGAL_TRANSFER_GROUP_CHAT_ID", "") or CASHIER_GROUP_CHAT_ID
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


def _fmt_uzs(amount: float) -> str:
    """30000000 → '30 000 000'. Space-separated thousands, no decimals."""
    n = int(round(amount))
    s = f"{n:,}".replace(",", " ")
    return s


def _notify_cashier_group_legal_transfer(
    *,
    transfer_id: int,
    client_name: str,
    amount_uzs: float,
    category_id: int,
    category_label: str,
    is_freetext: bool,
    category_freetext: str,
    legal_entity_name: str,
    legal_entity_inn: str,
    guvohnoma_photo_url: str,
    agent_name: str,
    suppliers: list,
) -> bool:
    """Send a structured notification to the cashier group when an agent
    submits a Stage 1 legal-entity transfer request. Includes an inline
    keyboard for uncle's Stage 2 supplier pick (one button per active
    supplier in the chosen category). Returns True on Telegram 200, False
    otherwise. Failures are logged but don't break the request — the row
    is already in the DB and uncle can find it via /cashbook later.
    """
    if not BOT_TOKEN or not LEGAL_TRANSFER_GROUP_CHAT_ID:
        logger.warning(
            "BOT_TOKEN or LEGAL_TRANSFER_GROUP_CHAT_ID missing; skipping legal-transfer notify"
        )
        return False

    if is_freetext and category_freetext:
        category_line = f"📦 Toifa: <b>Boshqa</b> — <i>{category_freetext}</i>"
    else:
        category_line = f"📦 Toifa: <b>{category_label}</b>"

    lines = [
        f"🏛 <b>Yangi yuridik shaxs to'lov so'rovi #{transfer_id}</b>",
        "",
        f"👤 Mijoz: <b>{client_name}</b>",
        f"💰 Summa: <b>{_fmt_uzs(amount_uzs)} UZS</b>",
        category_line,
        "",
        f"🏢 Firma: <b>{legal_entity_name}</b>",
        f"🆔 INN: <code>{legal_entity_inn}</code>",
    ]
    if guvohnoma_photo_url:
        lines.append(f"📷 Guvohnoma: {guvohnoma_photo_url}")
    lines += ["", f"🤵 Agent: {agent_name}"]

    # Inline keyboard: one button per active supplier in this category
    reply_markup = None
    if suppliers:
        lines += ["", "👇 <i>Yetkazib beruvchini tanlang:</i>"]
        reply_markup = {
            "inline_keyboard": [
                [
                    {
                        "text": s["name_1c"][:60],
                        "callback_data": f"legaltx:pick:{transfer_id}:{s['id']}",
                    }
                ]
                for s in suppliers
            ]
        }
    else:
        # Boshqa or empty category — no buttons; uncle handles manually
        lines += ["", "⚠️ <i>Bu toifa uchun yetkazib beruvchi ro'yxatda yo'q. Qo'lda hal qiling.</i>"]

    payload = {
        "chat_id": LEGAL_TRANSFER_GROUP_CHAT_ID,
        "text": "\n".join(lines),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    try:
        r = httpx.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json=payload,
            timeout=10,
        )
        if r.status_code != 200:
            logger.error(
                f"legal-transfer notify failed: {r.status_code} {r.text[:200]}"
            )
            return False
        return True
    except Exception as e:
        logger.error(f"legal-transfer notify exception: {e}")
        return False


@router.post("/legal-transfer")
def submit_legal_transfer(payload: dict = Body(...)):
    """Agent submits Stage 1 of a legal-entity bank transfer request.

    Writes one legal_transfers row (status='submitted') + initial audit event,
    fires a cashier-group notification with all collected data so uncle can
    pick a supplier in Stage 2 (next commit).

    Payload:
        {
            telegram_id: int (the agent),
            client_id: int,
            amount_uzs: float (>0),
            category_id: int (FK procurement_categories),
            category_freetext: str (required iff category.is_freetext),
            legal_entity_name: str (non-empty),
            legal_entity_inn: str (9 digits),
            guvohnoma_photo_url: str (optional)
        }
    """
    try:
        telegram_id = int(payload.get("telegram_id") or 0)
        client_id = int(payload.get("client_id") or 0)
        amount_uzs = float(payload.get("amount_uzs") or 0)
        category_id = int(payload.get("category_id") or 0)
        category_freetext = (payload.get("category_freetext") or "").strip()
        legal_entity_name = (payload.get("legal_entity_name") or "").strip()
        legal_entity_inn = (payload.get("legal_entity_inn") or "").strip()
        guvohnoma_photo_url = (payload.get("guvohnoma_photo_url") or "").strip()
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "invalid payload"}, status_code=400)

    if not telegram_id or not client_id:
        return JSONResponse(
            {"ok": False, "error": "telegram_id and client_id required"},
            status_code=400,
        )
    if amount_uzs <= 0:
        return JSONResponse(
            {"ok": False, "error": "amount_uzs must be > 0"}, status_code=400
        )
    if not category_id:
        return JSONResponse({"ok": False, "error": "category_id required"}, status_code=400)
    if not legal_entity_name:
        return JSONResponse(
            {"ok": False, "error": "legal_entity_name required"}, status_code=400
        )
    if not legal_entity_inn.isdigit() or len(legal_entity_inn) != 9:
        return JSONResponse(
            {"ok": False, "error": "legal_entity_inn must be 9 digits"}, status_code=400
        )

    conn = get_db()
    try:
        if not _is_agent(conn, telegram_id):
            return JSONResponse(
                {"ok": False, "error": "not an agent"}, status_code=403
            )

        cat = get_category(conn, category_id)
        if not cat:
            return JSONResponse(
                {"ok": False, "error": "category not found"}, status_code=400
            )
        if cat["is_freetext"] and not category_freetext:
            return JSONResponse(
                {"ok": False, "error": "category_freetext required for Boshqa"},
                status_code=400,
            )

        # Verify client exists (FK enforcement happens at insert; surface a
        # clean 400 instead of a SQLite IntegrityError)
        client_row = conn.execute(
            "SELECT id, name, client_id_1c FROM allowed_clients WHERE id = ?",
            (client_id,),
        ).fetchone()
        if not client_row:
            return JSONResponse(
                {"ok": False, "error": "client not found"}, status_code=400
            )

        agent_row = conn.execute(
            "SELECT first_name, last_name, username FROM users WHERE telegram_id = ?",
            (telegram_id,),
        ).fetchone()
        agent_name = (
            _format_name(
                agent_row["first_name"] if agent_row else None,
                agent_row["last_name"] if agent_row else None,
                agent_row["username"] if agent_row else None,
                telegram_id,
            )
            if agent_row
            else f"#{telegram_id}"
        )

        transfer_id = create_legal_transfer(
            conn,
            client_id=client_id,
            submitted_by_telegram_id=telegram_id,
            amount_uzs=amount_uzs,
            category_id=category_id,
            legal_entity_name=legal_entity_name,
            legal_entity_inn=legal_entity_inn,
            category_freetext=category_freetext if cat["is_freetext"] else None,
            guvohnoma_photo_url=guvohnoma_photo_url or None,
        )
        # Look up active suppliers for the cashier-group inline picker
        suppliers = list_suppliers_in_category(conn, category_id)
    finally:
        conn.close()

    notify_ok = _notify_cashier_group_legal_transfer(
        transfer_id=transfer_id,
        client_name=client_row["client_id_1c"] or client_row["name"] or f"#{client_id}",
        amount_uzs=amount_uzs,
        category_id=category_id,
        category_label=cat["label_uz"],
        is_freetext=bool(cat["is_freetext"]),
        category_freetext=category_freetext,
        legal_entity_name=legal_entity_name,
        legal_entity_inn=legal_entity_inn,
        guvohnoma_photo_url=guvohnoma_photo_url,
        agent_name=agent_name,
        suppliers=suppliers,
    )

    return {"ok": True, "transfer_id": transfer_id, "notified": notify_ok}


@router.get("/categories")
def categories(telegram_id: int = Query(...)):
    """Active procurement categories for the Stage 1 picker of the
    legal-entity bank transfer flow. Agent-gated (matches the rest of
    /api/payments/*). Returns 13 rows in display order; the 'Boshqa'
    free-text fallback has is_freetext=1.
    """
    conn = get_db()
    try:
        if not _is_agent(conn, telegram_id):
            return JSONResponse(
                {"ok": False, "error": "not an agent"}, status_code=403
            )
        items = list_active_categories(conn)
    finally:
        conn.close()
    return {
        "ok": True,
        "items": [
            {
                "id": c["id"],
                "label_uz": c["label_uz"],
                "label_ru": c["label_ru"],
                "label_en": c["label_en"],
                "is_freetext": bool(c["is_freetext"]),
            }
            for c in items
        ],
    }


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

    conn = get_db()
    try:
        # Admin role lives in users.agent_role now (with env-var fallback).
        # Env-only check would miss admins promoted through /makeagent.
        from backend.services.roles import role_in
        if not role_in(conn, telegram_id, {"admin"}):
            return JSONResponse({"ok": False, "error": "admin only"}, status_code=403)
        try:
            row = admin_cancel_payment(conn, payment_id, telegram_id, reason)
        except ValueError as e:
            conn.rollback()
            return JSONResponse({"ok": False, "error": str(e)}, status_code=404)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "payment": row}
