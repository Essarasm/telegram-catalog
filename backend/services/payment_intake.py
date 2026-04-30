"""Cashbook intake service (Session Z — Cashbook, Phase 1).

Shared helpers used by both the bot FSM (cashier group) and the Mini App
agent panel router. The flow is the same regardless of surface:

    1. insert_intake_raw()  — audit row, written before any matching
    2. create_intake_payment() — canonical row in intake_payments, links
       back to the raw audit row via source_intake_raw_id
    3. confirm_payment() / reject_payment() — status transitions

Sits parallel to client_payments (the 1C kassa import) until reconciliation
is clean for ~2 weeks (Phase 3), then becomes source of truth for collected
money. UZS and USD are tracked independently — never converted.
"""
from __future__ import annotations

import json
import logging
from typing import Dict, List, Optional

from backend.database import get_sibling_client_ids

logger = logging.getLogger(__name__)


# ── Audit-first ─────────────────────────────────────────────────────

def insert_intake_raw(
    conn,
    submitter_telegram_id: int,
    submitter_role: str,
    payload: dict,
    notes: Optional[str] = None,
) -> int:
    """Insert an audit row BEFORE any matching/validation. Per the
    zero-data-loss rule, every submission lands here even if the
    downstream intake_payments insert later fails."""
    cur = conn.execute(
        """INSERT INTO payment_intake_raw
           (submitter_telegram_id, submitter_role, raw_payload, notes)
           VALUES (?, ?, ?, ?)""",
        (
            submitter_telegram_id,
            submitter_role,
            json.dumps(payload, ensure_ascii=False),
            notes,
        ),
    )
    return cur.lastrowid


def create_intake_payment(
    conn,
    *,
    raw_id: int,
    client_id: int,
    amount: float,
    currency: str,
    channel: str,
    status: str,
    submitter_telegram_id: int,
    submitter_role: str,
    handover_agent_id: Optional[int] = None,
    card_id: Optional[int] = None,
    screenshot_file_id: Optional[str] = None,
    confirmed_by_telegram_id: Optional[int] = None,
    notes: Optional[str] = None,
) -> int:
    """Create the canonical intake_payments row and back-link the audit row.
    Caller is responsible for the transaction (commit on success)."""
    if amount <= 0:
        raise ValueError(f"amount must be > 0, got {amount}")
    if currency not in ("UZS", "USD"):
        raise ValueError(f"currency must be UZS or USD, got {currency}")
    if channel not in ("cash_direct", "cash_via_agent", "p2p"):
        raise ValueError(f"unknown channel: {channel}")
    if status not in ("pending_handover", "pending_review", "confirmed", "rejected"):
        raise ValueError(f"unknown status: {status}")

    confirmed_at = "datetime('now')" if status == "confirmed" else None
    cur = conn.execute(
        """INSERT INTO intake_payments
           (client_id, amount, currency, channel, card_id, handover_agent_id,
            submitter_telegram_id, submitter_role, confirmed_by_telegram_id,
            confirmed_at, status, screenshot_file_id, notes,
            source_intake_raw_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, """
        + ("datetime('now')" if confirmed_at else "NULL")
        + """, ?, ?, ?, ?)""",
        (
            client_id,
            float(amount),
            currency,
            channel,
            card_id,
            handover_agent_id,
            submitter_telegram_id,
            submitter_role,
            confirmed_by_telegram_id,
            status,
            screenshot_file_id,
            notes,
            raw_id,
        ),
    )
    payment_id = cur.lastrowid
    conn.execute(
        "UPDATE payment_intake_raw SET processed_payment_id = ? WHERE id = ?",
        (payment_id, raw_id),
    )
    return payment_id


# ── Debt lookup ─────────────────────────────────────────────────────

def lookup_client_debt(conn, client_id: int) -> Dict[str, float]:
    """Return {'uzs': ..., 'usd': ...} for this client. Sums across multi-
    phone siblings sharing one client_id_1c. Mirrors the helper in
    payment_notifications._lookup_debt — duplicated here to keep services
    decoupled."""
    ids = get_sibling_client_ids(conn, client_id) or [client_id]
    placeholders = ",".join("?" * len(ids))
    row = conn.execute(
        f"""SELECT COALESCE(SUM(debt_uzs), 0) AS uzs,
                   COALESCE(SUM(debt_usd), 0) AS usd
            FROM client_debts
            WHERE client_id IN ({placeholders})""",
        tuple(ids),
    ).fetchone()
    return {
        "uzs": float(row["uzs"] or 0) if row else 0.0,
        "usd": float(row["usd"] or 0) if row else 0.0,
    }


# ── Soft dedupe ─────────────────────────────────────────────────────

def check_recent_duplicate(
    conn,
    client_id: int,
    amount: float,
    currency: str,
    window_hours: int = 1,
) -> Optional[dict]:
    """Return the most recent matching intake_payments row in the window
    (status pending_*/confirmed) or None. Caller decides whether to warn
    the user before submitting again — we intentionally don't block."""
    row = conn.execute(
        """SELECT id, status, submitter_telegram_id, submitted_at, channel
           FROM intake_payments
           WHERE client_id = ?
             AND amount = ?
             AND currency = ?
             AND status IN ('pending_handover', 'pending_review', 'confirmed')
             AND submitted_at >= datetime('now', ?)
           ORDER BY submitted_at DESC
           LIMIT 1""",
        (client_id, float(amount), currency, f"-{int(window_hours)} hours"),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "status": row["status"],
        "submitter_telegram_id": row["submitter_telegram_id"],
        "submitted_at": row["submitted_at"],
        "channel": row["channel"],
    }


# ── Status transitions ──────────────────────────────────────────────

def confirm_payment(conn, payment_id: int, confirmer_telegram_id: int) -> dict:
    """Flip a pending row to confirmed. Returns the updated row as dict, or
    raises ValueError if the row isn't in a pending state."""
    row = conn.execute(
        "SELECT status FROM intake_payments WHERE id = ?", (payment_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"payment {payment_id} not found")
    if row["status"] not in ("pending_handover", "pending_review"):
        raise ValueError(
            f"cannot confirm payment {payment_id}: status={row['status']}"
        )
    conn.execute(
        """UPDATE intake_payments
           SET status = 'confirmed',
               confirmed_at = datetime('now'),
               confirmed_by_telegram_id = ?
           WHERE id = ?""",
        (confirmer_telegram_id, payment_id),
    )
    return get_payment(conn, payment_id)


def reject_payment(
    conn,
    payment_id: int,
    rejecter_telegram_id: int,
    reason: str,
) -> dict:
    """Flip a pending row to rejected. Reason is required (the submitter
    sees it via TG notification)."""
    if not reason or not reason.strip():
        raise ValueError("reject reason cannot be empty")
    row = conn.execute(
        "SELECT status FROM intake_payments WHERE id = ?", (payment_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"payment {payment_id} not found")
    if row["status"] not in ("pending_handover", "pending_review"):
        raise ValueError(
            f"cannot reject payment {payment_id}: status={row['status']}"
        )
    conn.execute(
        """UPDATE intake_payments
           SET status = 'rejected',
               rejected_at = datetime('now'),
               confirmed_by_telegram_id = ?,
               reject_reason = ?
           WHERE id = ?""",
        (rejecter_telegram_id, reason.strip(), payment_id),
    )
    return get_payment(conn, payment_id)


def admin_cancel_payment(
    conn,
    payment_id: int,
    admin_telegram_id: int,
    reason: Optional[str] = None,
) -> dict:
    """Admin-only soft cancel — flips status to 'rejected' regardless of
    current state (pending_handover/pending_review/confirmed all OK).
    No-op if already rejected. Audit row in payment_intake_raw is
    preserved; the intake_payments row itself is also kept (status flip,
    not deletion) per the zero-data-loss rule."""
    row = conn.execute(
        "SELECT status FROM intake_payments WHERE id = ?", (payment_id,)
    ).fetchone()
    if not row:
        raise ValueError(f"payment {payment_id} not found")
    if row["status"] == "rejected":
        return get_payment(conn, payment_id)
    final_reason = (reason or "").strip() or "admin_cancelled"
    conn.execute(
        """UPDATE intake_payments
           SET status = 'rejected',
               rejected_at = datetime('now'),
               confirmed_by_telegram_id = ?,
               reject_reason = ?
           WHERE id = ?""",
        (admin_telegram_id, final_reason, payment_id),
    )
    return get_payment(conn, payment_id)


def get_payment(conn, payment_id: int) -> dict:
    row = conn.execute(
        """SELECT ip.*, ac.name AS client_name, ac.client_id_1c
           FROM intake_payments ip
           LEFT JOIN allowed_clients ac ON ac.id = ip.client_id
           WHERE ip.id = ?""",
        (payment_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"payment {payment_id} not found")
    return dict(row)


# ── Queues ──────────────────────────────────────────────────────────

def list_pending_for_cashier(conn, limit: int = 50) -> List[dict]:
    """Pending submissions awaiting cashier action (handover or P2P review).
    Ordered oldest first so the queue feels FIFO."""
    rows = conn.execute(
        """SELECT ip.id, ip.client_id, ip.amount, ip.currency, ip.channel,
                  ip.handover_agent_id, ip.submitter_telegram_id,
                  ip.submitted_at, ip.status, ip.screenshot_file_id,
                  ac.name AS client_name, ac.client_id_1c
           FROM intake_payments ip
           LEFT JOIN allowed_clients ac ON ac.id = ip.client_id
           WHERE ip.status IN ('pending_handover', 'pending_review')
           ORDER BY ip.submitted_at ASC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def list_my_pending(conn, telegram_id: int, limit: int = 30) -> List[dict]:
    """Pending submissions made by this agent — so they can see what the
    cashier hasn't acted on yet."""
    rows = conn.execute(
        """SELECT ip.id, ip.client_id, ip.amount, ip.currency, ip.channel,
                  ip.submitted_at, ip.status,
                  ac.name AS client_name, ac.client_id_1c
           FROM intake_payments ip
           LEFT JOIN allowed_clients ac ON ac.id = ip.client_id
           WHERE ip.submitter_telegram_id = ?
             AND ip.status IN ('pending_handover', 'pending_review')
           ORDER BY ip.submitted_at DESC
           LIMIT ?""",
        (telegram_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def list_pending_for_client(conn, client_id: int, days: int = 14) -> List[dict]:
    """Pending + recent-confirmed intake_payments for a single client (and
    its multi-phone siblings). Joins users for submitter name + handover
    agent name. Used by the cabinet's Hisob-kitob pending section.

    Window: pending_handover/pending_review rows are kept for `days` days
    (default 14 — after that, the cashier needs admin attention). Confirmed
    rows are kept indefinitely — the frontend flips them to "Tekshirish
    kerak" red after 48h with no 1C match, so mismatches stay visible
    rather than silently disappearing. Phase 3 reconciliation will be the
    source-of-truth once it ships.
    """
    ids = get_sibling_client_ids(conn, client_id) or [client_id]
    placeholders = ",".join("?" * len(ids))
    # Pending rows: bounded by `days`. Confirmed rows: no time cap — they
    # need to stay visible until Phase 3 reconciliation marks them matched.
    rows = conn.execute(
        f"""SELECT ip.id, ip.client_id, ip.amount, ip.currency, ip.channel,
                   ip.status, ip.submitted_at, ip.confirmed_at,
                   ip.submitter_telegram_id, ip.submitter_role,
                   ip.handover_agent_id,
                   u_sub.first_name  AS sub_first,
                   u_sub.last_name   AS sub_last,
                   u_sub.username    AS sub_username,
                   u_agent.first_name AS agent_first,
                   u_agent.last_name  AS agent_last,
                   u_agent.username   AS agent_username
            FROM intake_payments ip
            LEFT JOIN users u_sub
                   ON u_sub.telegram_id = ip.submitter_telegram_id
            LEFT JOIN users u_agent
                   ON u_agent.telegram_id = ip.handover_agent_id
            WHERE ip.client_id IN ({placeholders})
              AND (
                  (ip.status IN ('pending_handover', 'pending_review')
                   AND ip.submitted_at >= datetime('now', ?))
                  OR ip.status = 'confirmed'
              )
            ORDER BY ip.submitted_at DESC""",
        tuple(ids) + (f"-{int(days)} days",),
    ).fetchall()
    return [dict(r) for r in rows]


def list_today_confirmed(conn, limit: int = 100) -> List[dict]:
    """Today's confirmed intake — for the cashier's "today" view."""
    rows = conn.execute(
        """SELECT ip.id, ip.client_id, ip.amount, ip.currency, ip.channel,
                  ip.submitter_telegram_id, ip.confirmed_at,
                  ac.name AS client_name, ac.client_id_1c
           FROM intake_payments ip
           LEFT JOIN allowed_clients ac ON ac.id = ip.client_id
           WHERE ip.status = 'confirmed'
             AND date(ip.confirmed_at, '+5 hours') = date('now', '+5 hours')
           ORDER BY ip.confirmed_at DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def summarize_today_intake(conn) -> dict:
    """Tashkent-day summary of confirmed intake_payments. Drives both the
    /bugun command and the 18:00 auto-post in the cashier group.

    Returns (always populated, even on a quiet day):
        {
            'date': 'YYYY-MM-DD',           # Tashkent calendar date
            'total_count': int,
            'uzs_total': float,
            'usd_total': float,
            'by_channel': {                  # only channels with rows today
                'cash_direct':    {'count': N, 'uzs': X, 'usd': Y},
                'cash_via_agent': {'count': N, 'uzs': X, 'usd': Y},
                'p2p':            {'count': N, 'uzs': X, 'usd': Y},
            },
            'top_clients': [
                {'name': '...', 'uzs': X, 'usd': Y, 'count': N},
                ...
            ],
            'pending_count': int,            # still-unactioned, for context
        }
    """
    today_tk = conn.execute(
        "SELECT date('now', '+5 hours') AS d"
    ).fetchone()["d"]

    confirmed = conn.execute(
        """SELECT ip.id, ip.amount, ip.currency, ip.channel, ip.client_id,
                  ac.client_id_1c, ac.name AS ac_name
           FROM intake_payments ip
           LEFT JOIN allowed_clients ac ON ac.id = ip.client_id
           WHERE ip.status = 'confirmed'
             AND date(ip.confirmed_at, '+5 hours') = ?""",
        (today_tk,),
    ).fetchall()

    total_count = len(confirmed)
    uzs_total = 0.0
    usd_total = 0.0
    by_channel: dict = {}
    by_client: dict = {}
    for r in confirmed:
        amt = float(r["amount"] or 0)
        cur = r["currency"]
        ch = r["channel"]
        if cur == "UZS":
            uzs_total += amt
        elif cur == "USD":
            usd_total += amt
        slot = by_channel.setdefault(ch, {"count": 0, "uzs": 0.0, "usd": 0.0})
        slot["count"] += 1
        if cur == "UZS":
            slot["uzs"] += amt
        elif cur == "USD":
            slot["usd"] += amt
        cname = r["client_id_1c"] or r["ac_name"] or f"#{r['client_id']}"
        c_slot = by_client.setdefault(cname, {"name": cname, "uzs": 0.0, "usd": 0.0, "count": 0})
        c_slot["count"] += 1
        if cur == "UZS":
            c_slot["uzs"] += amt
        elif cur == "USD":
            c_slot["usd"] += amt

    # Top 5 clients ranked by UZS-equivalent (UZS amount + USD amount * latest fx).
    fx = conn.execute(
        "SELECT rate FROM daily_fx_rates WHERE currency_pair = 'USD_UZS' ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    fx_rate = float(fx["rate"]) if fx and fx["rate"] else 0.0
    def _rank_value(c):
        return c["uzs"] + (c["usd"] * fx_rate if fx_rate else 0)
    top_clients = sorted(by_client.values(), key=_rank_value, reverse=True)[:5]

    pending_row = conn.execute(
        """SELECT COUNT(*) AS n FROM intake_payments
           WHERE status IN ('pending_handover', 'pending_review')
             AND submitted_at >= datetime('now', '-14 days')"""
    ).fetchone()
    pending_count = int(pending_row["n"] or 0) if pending_row else 0

    return {
        "date": today_tk,
        "total_count": total_count,
        "uzs_total": uzs_total,
        "usd_total": usd_total,
        "by_channel": by_channel,
        "top_clients": top_clients,
        "pending_count": pending_count,
    }


# ── Resolve telegram IDs for a client (for confirm-notification) ────

def resolve_client_telegram_ids(conn, client_id: int) -> List[int]:
    """Every approved telegram_id bound to this client or its multi-phone
    siblings. Used to notify the client on payment confirmation."""
    ids = get_sibling_client_ids(conn, client_id)
    if not ids:
        return []
    placeholders = ",".join("?" * len(ids))
    rows = conn.execute(
        f"""SELECT DISTINCT u.telegram_id
            FROM users u
            WHERE u.client_id IN ({placeholders})
              AND u.telegram_id IS NOT NULL
              AND COALESCE(u.is_approved, 0) = 1""",
        tuple(ids),
    ).fetchall()
    return [r["telegram_id"] for r in rows]
