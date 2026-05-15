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
    gross_uzs: Optional[float] = None,
    accepted_pct: Optional[float] = None,
    fx_rate_uzs_per_usd: Optional[float] = None,
) -> int:
    """Create the canonical intake_payments row and back-link the audit row.
    Caller is responsible for the transaction (commit on success).

    For channel='bank_transfer', pass gross_uzs/accepted_pct/fx_rate_uzs_per_usd
    alongside `amount` (the net UZS = gross × pct/100). Other channels leave
    these three as None."""
    if amount <= 0:
        raise ValueError(f"amount must be > 0, got {amount}")
    if currency not in ("UZS", "USD"):
        raise ValueError(f"currency must be UZS or USD, got {currency}")
    if channel not in ("cash_direct", "cash_via_agent", "p2p", "bank_transfer"):
        raise ValueError(f"unknown channel: {channel}")
    if status not in ("pending_handover", "pending_review", "confirmed", "rejected"):
        raise ValueError(f"unknown status: {status}")

    confirmed_at = "datetime('now')" if status == "confirmed" else None
    cur = conn.execute(
        """INSERT INTO intake_payments
           (client_id, amount, currency, channel, card_id, handover_agent_id,
            submitter_telegram_id, submitter_role, confirmed_by_telegram_id,
            confirmed_at, status, screenshot_file_id, notes,
            source_intake_raw_id, gross_uzs, accepted_pct, fx_rate_uzs_per_usd)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, """
        + ("datetime('now')" if confirmed_at else "NULL")
        + """, ?, ?, ?, ?, ?, ?, ?)""",
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
            float(gross_uzs) if gross_uzs is not None else None,
            float(accepted_pct) if accepted_pct is not None else None,
            float(fx_rate_uzs_per_usd) if fx_rate_uzs_per_usd is not None else None,
        ),
    )
    payment_id = cur.lastrowid
    conn.execute(
        "UPDATE payment_intake_raw SET processed_payment_id = ? WHERE id = ?",
        (payment_id, raw_id),
    )
    return payment_id


# ── Bank-transfer helpers ───────────────────────────────────────────

def compute_bank_transfer_net(gross_uzs: float, accepted_pct: float) -> float:
    """net_uzs = gross × pct/100, rounded to whole so'm. The pct is what we
    accept as payment; the rest is left in the bank for other use."""
    if gross_uzs is None or float(gross_uzs) <= 0:
        raise ValueError(f"gross_uzs must be > 0, got {gross_uzs}")
    if accepted_pct is None or float(accepted_pct) <= 0 or float(accepted_pct) > 100:
        raise ValueError(f"accepted_pct must be in (0, 100], got {accepted_pct}")
    return round(float(gross_uzs) * float(accepted_pct) / 100.0)


def create_bank_transfer_payment(
    conn,
    *,
    client_id: int,
    gross_uzs: float,
    accepted_pct: float,
    fx_rate_uzs_per_usd: float,
    submitter_telegram_id: int,
    submitter_role: str = "bank_transfer",
    notes: Optional[str] = None,
) -> int:
    """End-to-end bank-transfer write: audit row + canonical row, auto-confirmed.
    Caller commits the transaction. Returns the payment_id."""
    if fx_rate_uzs_per_usd is None or float(fx_rate_uzs_per_usd) <= 0:
        raise ValueError(f"fx_rate_uzs_per_usd must be > 0, got {fx_rate_uzs_per_usd}")
    net_uzs = compute_bank_transfer_net(gross_uzs, accepted_pct)
    payload = {
        "channel": "bank_transfer",
        "client_id": client_id,
        "gross_uzs": float(gross_uzs),
        "accepted_pct": float(accepted_pct),
        "fx_rate_uzs_per_usd": float(fx_rate_uzs_per_usd),
        "net_uzs": net_uzs,
    }
    raw_id = insert_intake_raw(
        conn,
        submitter_telegram_id=submitter_telegram_id,
        submitter_role=submitter_role,
        payload=payload,
    )
    return create_intake_payment(
        conn,
        raw_id=raw_id,
        client_id=client_id,
        amount=net_uzs,
        currency="UZS",
        channel="bank_transfer",
        status="confirmed",
        submitter_telegram_id=submitter_telegram_id,
        submitter_role=submitter_role,
        confirmed_by_telegram_id=submitter_telegram_id,
        notes=notes,
        gross_uzs=float(gross_uzs),
        accepted_pct=float(accepted_pct),
        fx_rate_uzs_per_usd=float(fx_rate_uzs_per_usd),
    )


def edit_bank_transfer_payment(
    conn,
    payment_id: int,
    *,
    new_gross_uzs: float,
    new_accepted_pct: float,
    new_fx_rate_uzs_per_usd: float,
    editor_telegram_id: int,
    reason: str = "bank_transfer_edited_via_ozgartirish",
) -> dict:
    """Soft-cancel old + insert-new linked by replaces_payment_id, mirroring
    edit_payment_amount but for the three-field bank-transfer record. Refuses
    on non-confirmed rows or no-op edits. Caller commits."""
    old = conn.execute(
        """SELECT id, client_id, amount, currency, channel, status,
                  submitter_telegram_id, submitter_role, notes,
                  gross_uzs, accepted_pct, fx_rate_uzs_per_usd
           FROM intake_payments WHERE id = ?""",
        (payment_id,),
    ).fetchone()
    if not old:
        raise ValueError(f"payment {payment_id} not found")
    if old["channel"] != "bank_transfer":
        raise ValueError(f"payment {payment_id} is not a bank_transfer record")
    if old["status"] != "confirmed":
        raise ValueError(
            f"faqat tasdiqlangan yozuvni o'zgartirish mumkin (joriy: {old['status']})"
        )

    new_net = compute_bank_transfer_net(new_gross_uzs, new_accepted_pct)
    if (
        abs(float(old["gross_uzs"] or 0) - float(new_gross_uzs)) < 0.005
        and abs(float(old["accepted_pct"] or 0) - float(new_accepted_pct)) < 0.005
        and abs(float(old["fx_rate_uzs_per_usd"] or 0) - float(new_fx_rate_uzs_per_usd)) < 0.005
    ):
        raise ValueError("yozuv o'zgarmagan")

    cur = conn.execute(
        """UPDATE intake_payments
           SET status = 'rejected',
               rejected_at = datetime('now'),
               confirmed_by_telegram_id = ?,
               reject_reason = ?
           WHERE id = ? AND status = 'confirmed'""",
        (editor_telegram_id, reason, payment_id),
    )
    if cur.rowcount == 0:
        raise ValueError("yozuv holati o'zgargan, qaytadan urinib ko'ring")

    payload = {
        "channel": "bank_transfer",
        "client_id": old["client_id"],
        "gross_uzs": float(new_gross_uzs),
        "accepted_pct": float(new_accepted_pct),
        "fx_rate_uzs_per_usd": float(new_fx_rate_uzs_per_usd),
        "net_uzs": new_net,
        "edits_payment_id": payment_id,
    }
    raw_id = insert_intake_raw(
        conn,
        submitter_telegram_id=editor_telegram_id,
        submitter_role="bank_transfer",
        payload=payload,
        notes=f"edits #{payment_id}",
    )
    new_pid = create_intake_payment(
        conn,
        raw_id=raw_id,
        client_id=old["client_id"],
        amount=new_net,
        currency="UZS",
        channel="bank_transfer",
        status="confirmed",
        submitter_telegram_id=old["submitter_telegram_id"],
        submitter_role=old["submitter_role"] or "bank_transfer",
        confirmed_by_telegram_id=editor_telegram_id,
        notes=old["notes"],
        gross_uzs=float(new_gross_uzs),
        accepted_pct=float(new_accepted_pct),
        fx_rate_uzs_per_usd=float(new_fx_rate_uzs_per_usd),
    )
    conn.execute(
        "UPDATE intake_payments SET replaces_payment_id = ? WHERE id = ?",
        (payment_id, new_pid),
    )
    return {
        "old": get_payment(conn, payment_id),
        "new": get_payment(conn, new_pid),
    }


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


def edit_payment_amount(
    conn,
    payment_id: int,
    new_amount: float,
    editor_telegram_id: int,
    reason: str = "cashier_edited_via_ozgartirish",
) -> dict:
    """Soft-cancel old + insert-new linked by replaces_payment_id. Single
    transaction; caller commits on success. Refuses on non-confirmed rows
    or no-op edits. Currency, client, channel, agent stay the same — only
    amount changes."""
    if new_amount is None or float(new_amount) <= 0:
        raise ValueError(f"new_amount must be > 0, got {new_amount}")
    old = conn.execute(
        """SELECT id, client_id, amount, currency, channel, card_id,
                  handover_agent_id, screenshot_file_id, notes, status,
                  submitter_telegram_id, submitter_role
           FROM intake_payments WHERE id = ?""",
        (payment_id,),
    ).fetchone()
    if not old:
        raise ValueError(f"payment {payment_id} not found")
    if old["status"] != "confirmed":
        raise ValueError(f"faqat tasdiqlangan yozuvni o'zgartirish mumkin (joriy: {old['status']})")
    if abs(float(old["amount"]) - float(new_amount)) < 0.005:
        raise ValueError("summa o'zgarmagan")

    cur = conn.execute(
        """UPDATE intake_payments
           SET status = 'rejected',
               rejected_at = datetime('now'),
               confirmed_by_telegram_id = ?,
               reject_reason = ?
           WHERE id = ? AND status = 'confirmed'""",
        (editor_telegram_id, reason, payment_id),
    )
    if cur.rowcount == 0:
        # status flipped between SELECT and UPDATE — abort to avoid creating
        # an orphan replacement row (Error Log #37 pattern)
        raise ValueError("yozuv holati o'zgargan, qaytadan urinib ko'ring")

    payload = {
        "channel": old["channel"],
        "client_id": old["client_id"],
        "amount": float(new_amount),
        "currency": old["currency"],
        "edits_payment_id": payment_id,
    }
    raw_id = insert_intake_raw(
        conn,
        submitter_telegram_id=editor_telegram_id,
        submitter_role="cashier",
        payload=payload,
        notes=f"edits #{payment_id}",
    )
    new_pid = create_intake_payment(
        conn,
        raw_id=raw_id,
        client_id=old["client_id"],
        amount=float(new_amount),
        currency=old["currency"],
        channel=old["channel"],
        status="confirmed",
        submitter_telegram_id=old["submitter_telegram_id"],
        submitter_role=old["submitter_role"],
        handover_agent_id=old["handover_agent_id"],
        card_id=old["card_id"],
        screenshot_file_id=old["screenshot_file_id"],
        confirmed_by_telegram_id=editor_telegram_id,
        notes=old["notes"],
    )
    conn.execute(
        "UPDATE intake_payments SET replaces_payment_id = ? WHERE id = ?",
        (payment_id, new_pid),
    )
    return {
        "old": get_payment(conn, payment_id),
        "new": get_payment(conn, new_pid),
    }


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


# ── Phase 2.5: cashier ↔ 1C reconciliation gate ─────────────────────
#
# Muqaddas records actual currency mix received; Alisher reshapes the same
# payment in 1C to fit dual-pricing legs, so per-currency splits differ
# even though USD-equivalent totals agree. Without this check, every
# cashier row stayed "Tekshirish kerak (48h)" red forever, double-counting
# in the client's Kabinet view.
#
# Tolerance was sampled across 97 active clients on 2026-05-13 — clean
# matches clustered ≤2% relative; real mismatches started at ≥17%. The
# 2%/$2 floor sits safely inside the gap. Per-row pairing is deferred to
# the full Phase 3 reconciler that will populate `payment_reconciliation`.

_FX_FALLBACK_UZS_PER_USD = 12200.0
_RECONCILE_TOLERANCE_PCT = 0.02
_RECONCILE_TOLERANCE_FLOOR_USD = 2.0


def client_reconciliation_check(conn, client_id: int, days: int = 14) -> dict:
    """Compare cashier (intake_payments) USD-eq vs 1C (client_payments)
    USD-eq totals for a client over the last `days`. Used to decide
    whether to suppress the "Tekshirish kerak" red flag on the Kabinet.

    The 1C window extends one day past the cashier window because Alisher
    routinely codes the previous day's cash on the next morning.

    Returns:
        {
            "reconciled": bool,
            "cashier_usdeq": float,
            "onec_usdeq": float,
            "diff_usdeq": float,       # cashier - 1C
            "tolerance_usdeq": float,  # the threshold used
        }
    """
    ids = get_sibling_client_ids(conn, client_id) or [client_id]
    placeholders = ",".join("?" * len(ids))

    cashier_row = conn.execute(
        f"""SELECT COALESCE(SUM(
                CASE WHEN ip.currency = 'USD' THEN ip.amount
                     WHEN ip.currency = 'UZS'
                       THEN ip.amount / COALESCE(fx.rate, ?)
                     ELSE 0 END
            ), 0) AS total
            FROM intake_payments ip
            LEFT JOIN daily_fx_rates fx
                   ON fx.rate_date = date(ip.submitted_at)
                  AND fx.currency_pair = 'USD_UZS'
            WHERE ip.client_id IN ({placeholders})
              AND ip.status = 'confirmed'
              AND ip.submitted_at >= datetime('now', ?)""",
        (_FX_FALLBACK_UZS_PER_USD,) + tuple(ids) + (f"-{int(days)} days",),
    ).fetchone()

    onec_row = conn.execute(
        f"""SELECT COALESCE(SUM(
                CASE WHEN cp.currency = 'USD' THEN cp.amount_currency
                     WHEN cp.currency = 'UZS'
                       THEN cp.amount_local / COALESCE(fx.rate, ?)
                     ELSE 0 END
            ), 0) AS total
            FROM client_payments cp
            LEFT JOIN daily_fx_rates fx
                   ON fx.rate_date = date(cp.doc_date)
                  AND fx.currency_pair = 'USD_UZS'
            WHERE cp.client_id IN ({placeholders})
              AND cp.doc_date >= date('now', ?)
              AND cp.doc_date <= date('now', '+1 day')""",
        (_FX_FALLBACK_UZS_PER_USD,) + tuple(ids) + (f"-{int(days)} days",),
    ).fetchone()

    cashier = float(cashier_row["total"] or 0)
    onec = float(onec_row["total"] or 0)
    diff = cashier - onec
    tolerance = max(
        _RECONCILE_TOLERANCE_FLOOR_USD,
        _RECONCILE_TOLERANCE_PCT * max(cashier, onec),
    )
    # Edge case: no cashier rows at all → nothing to reconcile, trivially
    # "reconciled" so callers don't false-flag. Same when both sides are
    # zero.
    if cashier == 0:
        reconciled = True
    else:
        reconciled = abs(diff) <= tolerance

    return {
        "reconciled": reconciled,
        "cashier_usdeq": round(cashier, 2),
        "onec_usdeq": round(onec, 2),
        "diff_usdeq": round(diff, 2),
        "tolerance_usdeq": round(tolerance, 2),
    }


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

    # Cashier-scope only — bank_transfer rows live in their own surface
    # (/perevodlar). Including them here would pollute Aunt's UZS/USD
    # totals with net-after-percentage values.
    confirmed = conn.execute(
        """SELECT ip.id, ip.amount, ip.currency, ip.channel, ip.client_id,
                  ac.client_id_1c, ac.name AS ac_name
           FROM intake_payments ip
           LEFT JOIN allowed_clients ac ON ac.id = ip.client_id
           WHERE ip.status = 'confirmed'
             AND ip.channel IN ('cash_direct', 'cash_via_agent', 'p2p')
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


def format_card_number(num: str) -> str:
    """8600123412345678 → '8600 1234 1234 5678'. Cosmetic only; storage stays
    as a digits-only string."""
    digits = "".join(c for c in (num or "") if c.isdigit())
    if not digits:
        return num or ""
    chunks = [digits[i : i + 4] for i in range(0, len(digits), 4)]
    return " ".join(chunks)


def list_dedicated_cards(conn, active_only: bool = True) -> List[dict]:
    """Return P2P destination cards (cards Rassvet gives to clients to wire
    money to). active_only filters out retired cards."""
    where = "WHERE active = 1" if active_only else ""
    rows = conn.execute(
        f"""SELECT id, card_number, holder_first_name, holder_last_name,
                   active, created_at, retired_at
              FROM dedicated_cards
              {where}
              ORDER BY id"""
    ).fetchall()
    return [dict(r) for r in rows]


def add_dedicated_card(conn, *, card_number: str, first: str, last: str) -> dict:
    """Insert a new dedicated card OR reactivate an existing one (matched by
    card_number). On reactivation, holder name is updated to the latest values.
    Returns the row id + a flag indicating whether the row was newly created
    or reactivated.

    Raises ValueError if card_number is empty / non-digit / wrong length, or
    if first/last is empty.
    """
    digits = "".join(c for c in (card_number or "") if c.isdigit())
    if len(digits) != 16:
        raise ValueError(
            f"Card number must be 16 digits (got {len(digits)})"
        )
    first = (first or "").strip()
    last = (last or "").strip()
    if not first or not last:
        raise ValueError("Holder first and last name required")

    cur = conn.cursor()
    existing = cur.execute(
        "SELECT id, active FROM dedicated_cards WHERE card_number = ?",
        (digits,),
    ).fetchone()
    if existing:
        cur.execute(
            """UPDATE dedicated_cards
                  SET active = 1, retired_at = NULL,
                      holder_first_name = ?, holder_last_name = ?
                WHERE id = ?""",
            (first, last, existing["id"]),
        )
        conn.commit()
        return {"id": existing["id"], "reactivated": True}

    cur.execute(
        """INSERT INTO dedicated_cards
              (card_number, holder_first_name, holder_last_name)
           VALUES (?, ?, ?)""",
        (digits, first, last),
    )
    conn.commit()
    return {"id": cur.lastrowid, "reactivated": False}


def retire_dedicated_card(conn, card_id: int) -> bool:
    """Soft-delete: set active=0, retired_at=now. Returns True if a row
    actually changed (i.e., it existed and was active before)."""
    cur = conn.cursor()
    cur.execute(
        """UPDATE dedicated_cards
              SET active = 0, retired_at = datetime('now')
            WHERE id = ? AND active = 1""",
        (card_id,),
    )
    changed = cur.rowcount > 0
    conn.commit()
    return changed


def list_active_categories(conn) -> List[dict]:
    """Active procurement categories for the Stage 1 dropdown of the
    legal-entity bank transfer flow. Returned in display order.

    The 'Boshqa' free-text fallback row has is_freetext=1 — frontend
    should render it last and surface a free-text input when picked.
    """
    rows = conn.execute(
        """SELECT id, label_uz, label_ru, label_en, sort_order, is_freetext
           FROM procurement_categories
           WHERE is_active = 1
           ORDER BY sort_order"""
    ).fetchall()
    return [dict(r) for r in rows]


def get_category(conn, category_id: int) -> Optional[dict]:
    """Single category lookup — used to decide if free-text is required."""
    row = conn.execute(
        "SELECT id, label_uz, label_ru, is_freetext FROM procurement_categories WHERE id = ?",
        (category_id,),
    ).fetchone()
    return dict(row) if row else None


def create_legal_transfer(
    conn,
    *,
    client_id: int,
    submitted_by_telegram_id: int,
    amount_uzs: float,
    category_id: int,
    legal_entity_name: str,
    legal_entity_inn: str,
    category_freetext: Optional[str] = None,
    guvohnoma_photo_url: Optional[str] = None,
    extra_doc_url: Optional[str] = None,
) -> int:
    """Create a legal_transfers row + the initial 'submitted' audit event.
    Returns the new row id. Caller is responsible for input validation;
    this function trusts its arguments (matches the create_intake_payment
    pattern at line 156).
    """
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO legal_transfers
              (client_id, submitted_by_telegram_id, amount_uzs,
               category_id, category_freetext,
               legal_entity_name, legal_entity_inn, guvohnoma_photo_url,
               extra_doc_url, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'submitted')""",
        (
            client_id,
            submitted_by_telegram_id,
            amount_uzs,
            category_id,
            category_freetext,
            legal_entity_name,
            legal_entity_inn,
            guvohnoma_photo_url,
            extra_doc_url,
        ),
    )
    transfer_id = cur.lastrowid
    cur.execute(
        """INSERT INTO legal_transfer_events
              (legal_transfer_id, from_status, to_status, actor_telegram_id, note)
           VALUES (?, NULL, 'submitted', ?, NULL)""",
        (transfer_id, submitted_by_telegram_id),
    )
    conn.commit()
    return transfer_id


def record_legal_transfer_event(
    conn,
    *,
    legal_transfer_id: int,
    from_status: str,
    to_status: str,
    actor_telegram_id: int,
    note: Optional[str] = None,
) -> int:
    """Append a legal_transfer_events row + atomically flip
    legal_transfers.status + bump updated_at. Returns the event row id.
    Caller validates the from→to transition is legal for the state machine
    (a thin wrapper service in a later commit will own that).
    """
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO legal_transfer_events
              (legal_transfer_id, from_status, to_status, actor_telegram_id, note)
           VALUES (?, ?, ?, ?, ?)""",
        (legal_transfer_id, from_status, to_status, actor_telegram_id, note),
    )
    event_id = cur.lastrowid
    cur.execute(
        """UPDATE legal_transfers
              SET status = ?, updated_at = datetime('now')
            WHERE id = ?""",
        (to_status, legal_transfer_id),
    )
    conn.commit()
    return event_id


def list_pending_legal_transfers_for_client(
    conn, client_id: int, days: int = 14
) -> List[dict]:
    """Active legal-entity transfer requests for a client — every status
    that still has a pending action by SOMEONE in the chain. Hides only
    terminal states (closed = faktura received, cancelled = aborted).

    Includes 'supplier_confirmed' (waiting for client to upload
    doverennost) and 'doverennost_received' (waiting for supplier
    faktura). The cabinet tile's per-status label tells the client what
    action — if any — is on them. Ordered newest first.

    The 14-day cap applies to status='submitted' only — once uncle has
    progressed the row, it stays visible until terminal regardless of
    age, so a long-running document chase never disappears.
    """
    rows = conn.execute(
        """SELECT lt.id, lt.amount_uzs, lt.status, lt.created_at, lt.updated_at,
                  lt.legal_entity_name, lt.legal_entity_inn,
                  lt.category_freetext,
                  pc.label_uz AS category_label,
                  pc.is_freetext AS category_is_freetext,
                  s.name_1c AS supplier_name_1c,
                  u.first_name AS submitter_first_name,
                  u.last_name AS submitter_last_name,
                  u.username AS submitter_username
             FROM legal_transfers lt
             LEFT JOIN procurement_categories pc ON pc.id = lt.category_id
             LEFT JOIN suppliers s ON s.id = lt.supplier_id
             LEFT JOIN users u ON u.telegram_id = lt.submitted_by_telegram_id
            WHERE lt.client_id = ?
              AND lt.status NOT IN ('closed', 'cancelled')
              AND (lt.status != 'submitted'
                   OR lt.created_at >= datetime('now', ?))
            ORDER BY lt.created_at DESC""",
        (client_id, f'-{days} days'),
    ).fetchall()
    return [dict(r) for r in rows]


def list_suppliers_in_category(conn, category_id: int) -> List[dict]:
    """All active suppliers mapped to this category, alphabetically by name_1c.
    Used to build the cashier-group inline keyboard for Stage 2 supplier
    picking. Returns [] for the Boshqa category (no rows in supplier_categories
    point at it) — caller should render a manual-handling note in that case.
    """
    rows = conn.execute(
        """SELECT s.id, s.name_1c, s.legal_name
           FROM suppliers s
           JOIN supplier_categories sc ON sc.supplier_id = s.id
           WHERE sc.category_id = ?
             AND s.is_active = 1
           ORDER BY s.name_1c COLLATE NOCASE""",
        (category_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def attach_doverennost(
    conn,
    *,
    legal_transfer_id: int,
    doverennost_url: str,
    actor_telegram_id: int,
) -> dict:
    """Atomic Stage 6 transition: store doverennost_url + flip status
    supplier_confirmed → doverennost_received + log event.

    The client (or staff on their behalf) uploads the power-of-attorney
    document via the cabinet pending tile's 📎 button. Backend forwards
    the file to the Перечисление group so uncle can pass it to the
    supplier (truck pickup gate).

    Raises ValueError if transfer not found or not in supplier_confirmed.
    """
    cur = conn.cursor()
    row = cur.execute(
        """SELECT lt.status, lt.client_id, lt.amount_uzs, lt.legal_entity_name,
                  s.name_1c AS supplier_name_1c
             FROM legal_transfers lt
             LEFT JOIN suppliers s ON s.id = lt.supplier_id
            WHERE lt.id = ?""",
        (legal_transfer_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Transfer #{legal_transfer_id} not found")
    if row["status"] != "supplier_confirmed":
        raise ValueError(
            f"Transfer is '{row['status']}', not supplier_confirmed (Stage 6 expects supplier_confirmed)"
        )

    cur.execute(
        """UPDATE legal_transfers
              SET doverennost_url = ?, status = 'doverennost_received',
                  updated_at = datetime('now')
            WHERE id = ?""",
        (doverennost_url, legal_transfer_id),
    )
    cur.execute(
        """INSERT INTO legal_transfer_events
              (legal_transfer_id, from_status, to_status, actor_telegram_id, note)
           VALUES (?, 'supplier_confirmed', 'doverennost_received', ?, NULL)""",
        (legal_transfer_id, actor_telegram_id),
    )
    conn.commit()
    return {
        "id": legal_transfer_id,
        "client_id": row["client_id"],
        "amount_uzs": row["amount_uzs"],
        "legal_entity_name": row["legal_entity_name"],
        "supplier_name_1c": row["supplier_name_1c"],
    }


def confirm_supplier_receipt(
    conn,
    *,
    legal_transfer_id: int,
    actor_telegram_id: int,
) -> dict:
    """Atomic Stage 5b transition: flip status transfer_proof_uploaded →
    supplier_confirmed + log event.

    NOTE — Cabinet debt-tile integration is deferred to a follow-up commit.
    The legal-transfer payment is currently NOT mirrored into intake_payments
    because the channel CHECK constraint would need a SQLite table recreation
    to add 'legal_transfer'. For v1 the legal_transfer row IS the canonical
    record; the cabinet pending tile will get a separate query branch later.

    Returns transfer info (amount, supplier, client linkage) so the caller
    can DM the client + edit the cashier-group message.

    Raises ValueError on missing transfer or wrong-status.
    """
    cur = conn.cursor()
    row = cur.execute(
        """SELECT lt.status, lt.client_id, lt.amount_uzs, lt.legal_entity_name,
                  s.name_1c AS supplier_name_1c
             FROM legal_transfers lt
             LEFT JOIN suppliers s ON s.id = lt.supplier_id
            WHERE lt.id = ?""",
        (legal_transfer_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Transfer #{legal_transfer_id} not found")
    if row["status"] != "transfer_proof_uploaded":
        raise ValueError(
            f"Transfer is '{row['status']}', not transfer_proof_uploaded (Stage 5b expects transfer_proof_uploaded)"
        )

    cur.execute(
        """UPDATE legal_transfers
              SET status = 'supplier_confirmed', updated_at = datetime('now')
            WHERE id = ?""",
        (legal_transfer_id,),
    )
    cur.execute(
        """INSERT INTO legal_transfer_events
              (legal_transfer_id, from_status, to_status, actor_telegram_id, note)
           VALUES (?, 'transfer_proof_uploaded', 'supplier_confirmed', ?, NULL)""",
        (legal_transfer_id, actor_telegram_id),
    )
    conn.commit()
    return {
        "id": legal_transfer_id,
        "client_id": row["client_id"],
        "amount_uzs": row["amount_uzs"],
        "legal_entity_name": row["legal_entity_name"],
        "supplier_name_1c": row["supplier_name_1c"],
    }


def attach_transfer_proof(
    conn,
    *,
    legal_transfer_id: int,
    transfer_proof_url: str,
    actor_telegram_id: int,
) -> dict:
    """Atomic Stage 5a transition: store transfer_proof_url (typically
    'tg://<file_id>' for v1) + flip status agreement_received →
    transfer_proof_uploaded + log event.

    Returns transfer info for downstream notification (forwarding the proof
    to the legal-transfer group with a "supplier confirmed?" button).

    Raises ValueError if transfer not found or not in agreement_received status.
    """
    cur = conn.cursor()
    row = cur.execute(
        """SELECT lt.status, lt.client_id, lt.amount_uzs, lt.legal_entity_name,
                  s.name_1c AS supplier_name_1c
             FROM legal_transfers lt
             LEFT JOIN suppliers s ON s.id = lt.supplier_id
            WHERE lt.id = ?""",
        (legal_transfer_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Transfer #{legal_transfer_id} not found")
    if row["status"] != "agreement_received":
        raise ValueError(
            f"Transfer is '{row['status']}', not agreement_received (Stage 5a expects agreement_received)"
        )

    cur.execute(
        """UPDATE legal_transfers
              SET transfer_proof_url = ?, status = 'transfer_proof_uploaded',
                  updated_at = datetime('now')
            WHERE id = ?""",
        (transfer_proof_url, legal_transfer_id),
    )
    cur.execute(
        """INSERT INTO legal_transfer_events
              (legal_transfer_id, from_status, to_status, actor_telegram_id, note)
           VALUES (?, 'agreement_received', 'transfer_proof_uploaded', ?, NULL)""",
        (legal_transfer_id, actor_telegram_id),
    )
    conn.commit()
    return {
        "id": legal_transfer_id,
        "client_id": row["client_id"],
        "amount_uzs": row["amount_uzs"],
        "legal_entity_name": row["legal_entity_name"],
        "supplier_name_1c": row["supplier_name_1c"],
    }


def attach_agreement(
    conn,
    *,
    legal_transfer_id: int,
    agreement_url: str,
    actor_telegram_id: int,
) -> dict:
    """Atomic Stage 3 transition: store agreement_url (typically 'tg://<file_id>'
    for v1 — Telegram-mediated storage) + flip status supplier_assigned →
    agreement_received + log event.

    Returns transfer info (id, client_id, amount_uzs, legal_entity_name,
    supplier_name_1c) for downstream notification — caller uses this to
    DM the client and edit the cashier-group message.

    Raises ValueError if transfer not found or not in supplier_assigned status.
    """
    cur = conn.cursor()
    row = cur.execute(
        """SELECT lt.status, lt.client_id, lt.amount_uzs, lt.legal_entity_name,
                  lt.submitted_by_telegram_id,
                  s.name_1c AS supplier_name_1c
             FROM legal_transfers lt
             LEFT JOIN suppliers s ON s.id = lt.supplier_id
            WHERE lt.id = ?""",
        (legal_transfer_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Transfer #{legal_transfer_id} not found")
    if row["status"] != "supplier_assigned":
        raise ValueError(
            f"Transfer is '{row['status']}', not supplier_assigned (Stage 3 expects supplier_assigned)"
        )

    cur.execute(
        """UPDATE legal_transfers
              SET agreement_url = ?, status = 'agreement_received',
                  updated_at = datetime('now')
            WHERE id = ?""",
        (agreement_url, legal_transfer_id),
    )
    cur.execute(
        """INSERT INTO legal_transfer_events
              (legal_transfer_id, from_status, to_status, actor_telegram_id, note)
           VALUES (?, 'supplier_assigned', 'agreement_received', ?, NULL)""",
        (legal_transfer_id, actor_telegram_id),
    )
    conn.commit()
    return {
        "id": legal_transfer_id,
        "client_id": row["client_id"],
        "amount_uzs": row["amount_uzs"],
        "legal_entity_name": row["legal_entity_name"],
        "supplier_name_1c": row["supplier_name_1c"],
        "submitted_by_telegram_id": row["submitted_by_telegram_id"],
    }


def assign_supplier(
    conn,
    *,
    legal_transfer_id: int,
    supplier_id: int,
    actor_telegram_id: int,
) -> dict:
    """Atomic Stage 2 transition: set supplier_id + flip status
    submitted → supplier_assigned + log event, all in one commit.

    Raises ValueError if transfer not found, already past 'submitted',
    or supplier doesn't exist / is inactive.

    Returns dict with the post-transition row's key fields for the caller
    to use in confirmation messages.
    """
    cur = conn.cursor()
    row = cur.execute(
        """SELECT lt.status, lt.client_id,
                  ac.name AS client_name, ac.client_id_1c AS client_id_1c
             FROM legal_transfers lt
             LEFT JOIN allowed_clients ac ON ac.id = lt.client_id
            WHERE lt.id = ?""",
        (legal_transfer_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Transfer #{legal_transfer_id} not found")
    if row["status"] != "submitted":
        raise ValueError(f"Transfer is '{row['status']}', not submittable")

    sup = cur.execute(
        "SELECT id, name_1c, is_active FROM suppliers WHERE id = ?",
        (supplier_id,),
    ).fetchone()
    if not sup:
        raise ValueError(f"Supplier #{supplier_id} not found")
    if not sup["is_active"]:
        raise ValueError(f"Supplier #{supplier_id} is inactive")

    cur.execute(
        """UPDATE legal_transfers
              SET supplier_id = ?, status = 'supplier_assigned',
                  updated_at = datetime('now')
            WHERE id = ?""",
        (supplier_id, legal_transfer_id),
    )
    cur.execute(
        """INSERT INTO legal_transfer_events
              (legal_transfer_id, from_status, to_status, actor_telegram_id, note)
           VALUES (?, 'submitted', 'supplier_assigned', ?, NULL)""",
        (legal_transfer_id, actor_telegram_id),
    )
    conn.commit()
    return {
        "supplier_name_1c": sup["name_1c"],
        "supplier_id": sup["id"],
        "client_id": row["client_id"],
        "client_name": row["client_name"],
        "client_id_1c": row["client_id_1c"],
    }
