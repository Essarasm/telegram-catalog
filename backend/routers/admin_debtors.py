"""Admin debtors / receivables endpoints — aging buckets, manager's printed
debtors list, callback scheduling, receivables trend, per-client history.

Extracted from `admin.py` to keep that file under the 2,000-line god-module
canary. Endpoints kept on their original `/api/admin/...` URLs so the admin
dashboard frontend and any external scripts do not have to change.
"""
from fastapi import APIRouter, Form, Query, HTTPException

from backend.admin_auth import check_admin_key
from backend.database import get_db


router = APIRouter(prefix="/api/admin", tags=["admin"])

# Fallback rate when no daily_fx_rates row covers the period being looked at.
# Duplicated with admin_revenue.py / admin.py — keep in sync if tuned.
FX_FALLBACK = 12000.0


def _check_admin(admin_key: str):
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _latest_fxrate(conn, fallback: float = 12000.0) -> float:
    row = conn.execute(
        """SELECT rate FROM daily_fx_rates
           WHERE currency_pair = 'USD_UZS'
           ORDER BY rate_date DESC LIMIT 1"""
    ).fetchone()
    return float(row["rate"]) if row and row["rate"] else fallback


_AGING_BUCKETS_UZS = ("0_30", "31_60", "61_90", "91_120", "120_plus")

# Proposal B client-size buckets (monthly USD volume) — the SOLE client-facing
# bucketing scheme (see memory `bucketing_schemes`). Deliberately NOT the
# Session-G thresholds stored in `client_scores.volume_bucket` (300/1500/5000/
# 12000) — those are an internal credit_scoring.py detail. We re-classify each
# debtor's `monthly_volume_usd` against these edges instead.
_PROPOSAL_B_EDGES = (125.0, 621.0, 1721.0, 4120.0)
_PROPOSAL_B_LABELS = ("Micro", "Small", "Medium", "Large", "Heavy")


def _proposal_b_bucket(monthly_volume_usd):
    """Map a monthly-volume USD figure to a Proposal B size bucket.

    Returns None when the client has no score row at all (debtor not in
    `client_scores`) — surfaced as 'Unscored' so it's never silently dropped.
    A scored client with 0 volume is a real Micro client, not Unscored.
    """
    if monthly_volume_usd is None:
        return None
    v = float(monthly_volume_usd)
    if v < _PROPOSAL_B_EDGES[0]:
        return "Micro"
    if v < _PROPOSAL_B_EDGES[1]:
        return "Small"
    if v < _PROPOSAL_B_EDGES[2]:
        return "Medium"
    if v < _PROPOSAL_B_EDGES[3]:
        return "Large"
    return "Heavy"


@router.get("/receivables")
def receivables(
    admin_key: str = Query(...),
    currency: str = Query("UZS"),
):
    """Receivables + aging from 1C `client_debts` (debtor report).

    Source: latest `/debtors` snapshot in `client_debts`. Real day-aged
    buckets are taken straight from 1C for UZS. USD has no aging in the
    1C report — only a total.

    Pseudo-account exclusion (cash registers, structural ledger accounts,
    return markers, etc.) is applied via `pseudo_clients`.
    """
    from backend.services.pseudo_clients import (
        sql_exclusion_clause,
        sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()

    report_date = conn.execute(
        "SELECT MAX(report_date) FROM client_debts"
    ).fetchone()[0]

    if not report_date:
        conn.close()
        return {
            "ok": True,
            "currency": currency,
            "as_of": None,
            "total_receivable": 0,
            "total_clients_with_debt": 0,
            "aging": {},
            "aging_client_count": {},
            "aging_top_clients": {},
            "usd_total": 0,
            "usd_client_count": 0,
            "usd_aging_available": False,
            "methodology": "No /debtors data imported yet.",
        }

    excl_clause = sql_exclusion_clause("client_name_1c")
    excl_params = sql_exclusion_params()

    if currency == "USD":
        # USD has no aging in /debtors — return totals + top clients only.
        rows = conn.execute(
            f"""SELECT client_name_1c, debt_usd
                  FROM client_debts
                 WHERE report_date = ? AND debt_usd > 0 AND {excl_clause}
                 ORDER BY debt_usd DESC""",
            (report_date, *excl_params),
        ).fetchall()
        total = sum(r["debt_usd"] for r in rows)
        top = [
            {"name": r["client_name_1c"], "balance": round(r["debt_usd"], 2)}
            for r in rows[:10]
        ]
        conn.close()
        return {
            "ok": True,
            "currency": "USD",
            "as_of": report_date,
            "total_receivable": round(total, 2),
            "total_clients_with_debt": len(rows),
            "aging": {},
            "aging_client_count": {},
            "aging_top_clients": {"all": top},
            "usd_total": round(total, 2),
            "usd_client_count": len(rows),
            "usd_aging_available": False,
            "methodology": (
                "USD totals from latest 1C debtor report. 1C does not provide "
                "per-bucket aging for USD — only total outstanding per client."
            ),
        }

    # UZS path — real aging buckets from 1C
    rows = conn.execute(
        f"""SELECT client_name_1c, debt_uzs, debt_usd, last_transaction_date,
                   aging_0_30, aging_31_60, aging_61_90, aging_91_120, aging_120_plus
              FROM client_debts
             WHERE report_date = ? AND debt_uzs > 0 AND {excl_clause}
             ORDER BY debt_uzs DESC""",
        (report_date, *excl_params),
    ).fetchall()

    aging = {b: 0.0 for b in _AGING_BUCKETS_UZS}
    client_count = {b: 0 for b in _AGING_BUCKETS_UZS}
    bucket_clients: dict[str, list[dict]] = {b: [] for b in _AGING_BUCKETS_UZS}
    bucket_col = {
        "0_30": "aging_0_30",
        "31_60": "aging_31_60",
        "61_90": "aging_61_90",
        "91_120": "aging_91_120",
        "120_plus": "aging_120_plus",
    }
    total_receivable = 0.0
    for r in rows:
        total_receivable += r["debt_uzs"]
        for b, col in bucket_col.items():
            amt = r[col] or 0
            if amt > 0:
                aging[b] += amt
                client_count[b] += 1
                bucket_clients[b].append({
                    "name": r["client_name_1c"],
                    "balance": round(amt, 2),
                    "total_debt": round(r["debt_uzs"], 2),
                    "last_tx": r["last_transaction_date"],
                })

    # USD side-panel summary (computed even on UZS calls so frontend can show it)
    usd_rows = conn.execute(
        f"""SELECT client_name_1c, debt_usd
              FROM client_debts
             WHERE report_date = ? AND debt_usd > 0 AND {excl_clause}
             ORDER BY debt_usd DESC""",
        (report_date, *excl_params),
    ).fetchall()
    usd_total = sum(r["debt_usd"] for r in usd_rows)

    conn.close()

    # Top 10 per bucket (by amount in that bucket)
    for b in bucket_clients:
        bucket_clients[b].sort(key=lambda x: x["balance"], reverse=True)
        bucket_clients[b] = bucket_clients[b][:10]

    return {
        "ok": True,
        "currency": "UZS",
        "as_of": report_date,
        "total_receivable": round(total_receivable, 2),
        "total_clients_with_debt": len(rows),
        "aging": {k: round(v, 2) for k, v in aging.items()},
        "aging_client_count": client_count,
        "aging_top_clients": bucket_clients,
        "usd_total": round(usd_total, 2),
        "usd_client_count": len(usd_rows),
        "usd_aging_available": False,
        "methodology": (
            "Aging from 1C 'Дебиторская задолженность' report (per-day FIFO "
            "bucketed by 1C). UZS only — 1C does not provide USD aging. "
            "Pseudo-accounts (cash registers, structural ledger entries, "
            "return markers) excluded via pseudo_clients.SYSTEM_NON_CLIENT_NAMES."
        ),
    }


@router.get("/debtors-list")
def debtors_list(admin_key: str = Query(...)):
    """Per-client debtors list — mirrors the manager's printed report.

    Latest `client_debts` snapshot, real clients only (pseudo-accounts
    excluded via `pseudo_clients.SYSTEM_NON_CLIENT_NAMES`). Sorted by
    combined USD-equivalent debt DESC. Includes `last_transaction_date`
    and computed `days_since_last_tx` so the manager can spot stuck debt.

    Footer totals (count, sum_uzs, sum_usd) are returned alongside the
    rows for paper-list reconciliation.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from backend.services.pseudo_clients import (
        sql_exclusion_clause, sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()

    report_date = conn.execute(
        "SELECT MAX(report_date) FROM client_debts"
    ).fetchone()[0]

    if not report_date:
        conn.close()
        return {
            "ok": True,
            "as_of": None,
            "fxrate_used": 0,
            "count": 0,
            "total_uzs": 0,
            "total_usd": 0,
            "items": [],
        }

    fxrate = _latest_fxrate(conn)
    excl_clause = sql_exclusion_clause("cd.client_name_1c")

    rows = conn.execute(
        f"""WITH max_pay AS (
                SELECT client_name_1c, MAX(doc_date) AS max_date
                  FROM client_payments
                 GROUP BY client_name_1c
            ),
            last_pay AS (
                SELECT cp.client_name_1c,
                       cp.doc_date,
                       COALESCE(SUM(CASE WHEN cp.currency = 'UZS'
                                         THEN cp.amount_local ELSE 0 END), 0) AS last_pay_uzs,
                       COALESCE(SUM(CASE WHEN cp.currency = 'USD'
                                         THEN cp.amount_currency ELSE 0 END), 0) AS last_pay_usd
                  FROM client_payments cp
                  JOIN max_pay mp ON mp.client_name_1c = cp.client_name_1c
                                 AND mp.max_date = cp.doc_date
                 GROUP BY cp.client_name_1c, cp.doc_date
            ),
            max_cb AS (
                SELECT client_name_1c, MAX(id) AS max_id
                  FROM client_callbacks
                 GROUP BY client_name_1c
            ),
            last_cb AS (
                SELECT cc.client_name_1c, cc.callback_date,
                       cc.set_by_name, cc.set_by_telegram_id, cc.set_at, cc.note
                  FROM client_callbacks cc
                  JOIN max_cb mc ON mc.client_name_1c = cc.client_name_1c
                                AND mc.max_id = cc.id
            ),
            cb_stats AS (
                -- Reschedule signal: count of DISTINCT non-null callback
                -- dates the client was ever given. Note-only edits reuse the
                -- same date, so they don't inflate this. Reschedules =
                -- distinct_dates - 1 (computed in Python below).
                SELECT client_name_1c,
                       COUNT(DISTINCT callback_date) AS cb_date_count
                  FROM client_callbacks
                 WHERE callback_date IS NOT NULL
                 GROUP BY client_name_1c
            ),
            latest_score AS (
                -- Latest score row per client → monthly_volume_usd for the
                -- Proposal B size bucket. We take only the volume; the stored
                -- volume_bucket uses Session-G thresholds and is NOT used.
                SELECT cs.client_id, cs.monthly_volume_usd
                  FROM client_scores cs
                  JOIN (SELECT client_id, MAX(recalc_date) AS md
                          FROM client_scores GROUP BY client_id) m
                    ON m.client_id = cs.client_id AND m.md = cs.recalc_date
            )
            -- Agent column is purely manual — sourced from
            -- allowed_clients.assigned_agent_tg_id only. No auto-derive
            -- from acting-as switches or any other signal.
            SELECT cd.client_name_1c, cd.client_id, cd.debt_uzs, cd.debt_usd,
                   cd.last_transaction_date, cd.last_transaction_no,
                   cd.aging_0_30, cd.aging_31_60, cd.aging_61_90,
                   cd.aging_91_120, cd.aging_120_plus,
                   lp.doc_date AS last_payment_date,
                   lp.last_pay_uzs AS last_payment_uzs,
                   lp.last_pay_usd AS last_payment_usd,
                   lcb.callback_date, lcb.set_by_name AS callback_set_by,
                   lcb.set_at AS callback_set_at, lcb.note AS callback_note,
                   cbs.cb_date_count AS cb_date_count,
                   ac.phone_normalized AS anchor_phone,
                   sib.phones AS sibling_phones,
                   ac.assigned_agent_tg_id,
                   ac.assigned_agent_set_at,
                   ac.assigned_agent_set_by_name,
                   ma.first_name AS assigned_agent_first_name,
                   ma.last_name  AS assigned_agent_last_name,
                   ls.monthly_volume_usd AS monthly_volume_usd
              FROM client_debts cd
              LEFT JOIN last_pay lp ON lp.client_name_1c = cd.client_name_1c
              LEFT JOIN last_cb lcb ON lcb.client_name_1c = cd.client_name_1c
              LEFT JOIN cb_stats cbs ON cbs.client_name_1c = cd.client_name_1c
              LEFT JOIN latest_score ls ON ls.client_id = cd.client_id
              LEFT JOIN allowed_clients ac ON ac.id = cd.client_id
              LEFT JOIN users ma ON ma.telegram_id = ac.assigned_agent_tg_id
              LEFT JOIN (
                  SELECT client_id_1c,
                         GROUP_CONCAT(DISTINCT phone_normalized) AS phones
                    FROM allowed_clients
                   WHERE phone_normalized IS NOT NULL
                     AND phone_normalized != ''
                     AND client_id_1c IS NOT NULL
                   GROUP BY client_id_1c
              ) sib ON sib.client_id_1c = ac.client_id_1c
             WHERE cd.report_date = ?
               AND (cd.debt_uzs > 0 OR cd.debt_usd > 0)
               AND {excl_clause}""",
        (report_date, *sql_exclusion_params()),
    ).fetchall()

    # Agent dropdown options — every user with agent_role='agent' is selectable.
    agent_rows = conn.execute(
        """SELECT telegram_id, first_name, last_name
             FROM users
            WHERE agent_role = 'agent'
            ORDER BY COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')"""
    ).fetchall()
    conn.close()

    available_agents = []
    for ag in agent_rows:
        first = (ag["first_name"] or "").strip()
        last = (ag["last_name"] or "").strip()
        full = " ".join(p for p in (first, last) if p) or f"#{ag['telegram_id']}"
        available_agents.append({
            "telegram_id": ag["telegram_id"],
            "name": full,
        })

    today_tk = datetime.now(ZoneInfo("Asia/Tashkent")).date()

    def _days_since(date_str):
        if not date_str:
            return None
        try:
            return (today_tk - datetime.strptime(date_str, "%Y-%m-%d").date()).days
        except (ValueError, TypeError):
            return None

    items = []
    total_uzs = 0.0
    total_usd = 0.0
    for r in rows:
        debt_uzs = float(r["debt_uzs"] or 0)
        debt_usd = float(r["debt_usd"] or 0)
        usd_eq = debt_usd + (debt_uzs / fxrate if fxrate > 0 else 0)
        total_uzs += debt_uzs
        total_usd += debt_usd
        # Phones: sibling group via shared client_id_1c if available, else
        # fall back to the anchor row's own phone. Keep digits-only —
        # frontend prefixes +998 for tel: links + display formatting.
        phones_raw = r["sibling_phones"] or r["anchor_phone"] or ""
        phones = sorted({
            p.strip() for p in str(phones_raw).split(",")
            if p and p.strip()
        }) if phones_raw else []
        manual_first = (r["assigned_agent_first_name"] or "").strip()
        manual_last = (r["assigned_agent_last_name"] or "").strip()
        agent_name = " ".join(p for p in (manual_first, manual_last) if p) or None
        monthly_volume_usd = r["monthly_volume_usd"]
        size_bucket = _proposal_b_bucket(monthly_volume_usd)
        items.append({
            "client_name": r["client_name_1c"],
            "client_id": r["client_id"],
            "debt_uzs": round(debt_uzs, 2),
            "debt_usd": round(debt_usd, 2),
            "debt_usd_eq": round(usd_eq, 2),
            "last_transaction_date": r["last_transaction_date"],
            "last_transaction_no": r["last_transaction_no"],
            "days_since_last_tx": _days_since(r["last_transaction_date"]),
            "last_payment_date": r["last_payment_date"],
            "days_since_last_payment": _days_since(r["last_payment_date"]),
            "last_payment_uzs": round(float(r["last_payment_uzs"] or 0), 2),
            "last_payment_usd": round(float(r["last_payment_usd"] or 0), 2),
            "callback_date": r["callback_date"],
            "callback_set_by": r["callback_set_by"],
            "callback_set_at": r["callback_set_at"],
            "callback_note": r["callback_note"],
            "callback_reschedules": max(0, (r["cb_date_count"] or 0) - 1),
            "agent_name": agent_name,
            "agent_telegram_id": r["assigned_agent_tg_id"],
            "assigned_agent_set_at": r["assigned_agent_set_at"],
            "assigned_agent_set_by": r["assigned_agent_set_by_name"],
            "phones": phones,
            "aging_uzs": {
                "0_30": round(r["aging_0_30"] or 0, 2),
                "31_60": round(r["aging_31_60"] or 0, 2),
                "61_90": round(r["aging_61_90"] or 0, 2),
                "91_120": round(r["aging_91_120"] or 0, 2),
                "120_plus": round(r["aging_120_plus"] or 0, 2),
            },
            # Proposal B client-size bucket (re-classified from monthly_volume_usd;
            # NOT the Session-G client_scores.volume_bucket). None → "Unscored".
            "size_bucket": size_bucket,
            "monthly_volume_usd": round(float(monthly_volume_usd), 2) if monthly_volume_usd is not None else None,
        })

    # Sort by combined USD-eq debt DESC
    items.sort(key=lambda x: x["debt_usd_eq"], reverse=True)
    for idx, it in enumerate(items, start=1):
        it["rank"] = idx

    return {
        "ok": True,
        "as_of": report_date,
        "fxrate_used": fxrate,
        "count": len(items),
        "total_uzs": round(total_uzs, 2),
        "total_usd": round(total_usd, 2),
        "items": items,
        "available_agents": available_agents,
    }


@router.post("/client-assign-agent")
def client_assign_agent(
    admin_key: str = Form(...),
    client_id: int = Form(...),
    agent_telegram_id: int = Form(None),
):
    """Manually assign (or clear) the responsible agent for a client.

    Persists to `allowed_clients.assigned_agent_tg_id`. The Debtors List
    Agent column reads this in preference to the auto-derived latest-switch
    agent. Pass `agent_telegram_id` empty/null to clear the override and
    fall back to auto-derive.

    Admin role only. The setter's identity is captured in
    `assigned_agent_set_by_*` so the next operator can see who made the
    change. The env-var admin path (no session identity) records
    `set_by_name='admin'`.
    """
    from backend.admin_auth import resolve_auth

    auth = resolve_auth(admin_key)
    if not auth or auth.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    conn = get_db()
    try:
        if not conn.execute(
            "SELECT 1 FROM allowed_clients WHERE id = ?", (client_id,)
        ).fetchone():
            raise HTTPException(status_code=404, detail="client_id not found")

        if agent_telegram_id is not None:
            agent_row = conn.execute(
                "SELECT telegram_id, first_name, last_name, agent_role "
                "FROM users WHERE telegram_id = ?",
                (agent_telegram_id,),
            ).fetchone()
            if not agent_row:
                raise HTTPException(status_code=404, detail="agent not found")
            if agent_row["agent_role"] != "agent":
                raise HTTPException(
                    status_code=400,
                    detail="user is not an agent (agent_role != 'agent')",
                )

        set_by_name = auth.get("name") or "admin"
        set_by_tg = auth.get("telegram_id")

        if agent_telegram_id is None:
            conn.execute(
                """UPDATE allowed_clients
                      SET assigned_agent_tg_id = NULL,
                          assigned_agent_set_at = datetime('now'),
                          assigned_agent_set_by_tg_id = ?,
                          assigned_agent_set_by_name = ?
                    WHERE id = ?""",
                (set_by_tg, set_by_name, client_id),
            )
        else:
            conn.execute(
                """UPDATE allowed_clients
                      SET assigned_agent_tg_id = ?,
                          assigned_agent_set_at = datetime('now'),
                          assigned_agent_set_by_tg_id = ?,
                          assigned_agent_set_by_name = ?
                    WHERE id = ?""",
                (agent_telegram_id, set_by_tg, set_by_name, client_id),
            )
        conn.commit()

        # Re-read for response (and to surface the joined agent name).
        row = conn.execute(
            """SELECT ac.assigned_agent_tg_id,
                      ac.assigned_agent_set_at,
                      ac.assigned_agent_set_by_name,
                      u.first_name, u.last_name
                 FROM allowed_clients ac
                 LEFT JOIN users u ON u.telegram_id = ac.assigned_agent_tg_id
                WHERE ac.id = ?""",
            (client_id,),
        ).fetchone()
    finally:
        conn.close()

    first = (row["first_name"] or "").strip() if row else ""
    last = (row["last_name"] or "").strip() if row else ""
    agent_name = " ".join(p for p in (first, last) if p) or None

    return {
        "ok": True,
        "client_id": client_id,
        "assigned_agent_tg_id": row["assigned_agent_tg_id"] if row else None,
        "agent_name": agent_name,
        "set_at": row["assigned_agent_set_at"] if row else None,
        "set_by_name": row["assigned_agent_set_by_name"] if row else None,
    }


@router.post("/debtors-callback")
def debtors_callback_set(
    admin_key: str = Form(...),
    client_name_1c: str = Form(...),
    callback_date: str = Form(None),
    note: str = Form(None),
):
    """Schedule (or clear) a callback for a debtor client.

    Append-only: every save writes a new row in `client_callbacks`. The
    latest row per client wins on read (joined back into `/debtors-list`).
    Passing `callback_date=""` (or omitting it) records an explicit clear
    so the audit trail captures who cleared and when.

    Admin role only — captures `set_by_telegram_id` + `set_by_name` from
    the dashboard session so the next employee can see who scheduled the
    callback. The env-var admin path (no session identity) records
    `set_by_name='admin'`; use the bot's /dashboard button for full
    attribution.
    """
    from backend.admin_auth import resolve_auth

    auth = resolve_auth(admin_key)
    if not auth or auth.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    name = (client_name_1c or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="client_name_1c required")

    cb_date = (callback_date or "").strip() or None
    if cb_date:
        # Light validation — YYYY-MM-DD only.
        from datetime import datetime
        try:
            datetime.strptime(cb_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="callback_date must be YYYY-MM-DD")

    set_by_name = auth.get("name") or "admin"
    set_by_tg = auth.get("telegram_id")
    cb_note = (note or "").strip() or None

    conn = get_db()
    cur = conn.execute(
        """INSERT INTO client_callbacks
                  (client_name_1c, callback_date, set_by_telegram_id, set_by_name, note)
           VALUES (?, ?, ?, ?, ?)""",
        (name, cb_date, set_by_tg, set_by_name, cb_note),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id, callback_date, set_by_name, set_at, note FROM client_callbacks WHERE id = ?",
        (cur.lastrowid,),
    ).fetchone()
    conn.close()

    return {
        "ok": True,
        "client_name_1c": name,
        "callback_date": row["callback_date"],
        "set_by_name": row["set_by_name"],
        "set_at": row["set_at"],
        "note": row["note"],
    }


@router.get("/debtors-callback-history")
def debtors_callback_history(
    admin_key: str = Query(...),
    client_name_1c: str = Query(...),
    limit: int = Query(20, ge=1, le=200),
):
    """Per-client callback history (newest first). Read-only audit view."""
    _check_admin(admin_key)
    conn = get_db()
    rows = conn.execute(
        """SELECT id, callback_date, set_by_name, set_by_telegram_id, set_at, note
             FROM client_callbacks
            WHERE client_name_1c = ?
         ORDER BY id DESC
            LIMIT ?""",
        (client_name_1c, limit),
    ).fetchall()
    conn.close()
    return {
        "ok": True,
        "client_name_1c": client_name_1c,
        "count": len(rows),
        "items": [
            {
                "id": r["id"],
                "callback_date": r["callback_date"],
                "set_by_name": r["set_by_name"],
                "set_by_telegram_id": r["set_by_telegram_id"],
                "set_at": r["set_at"],
                "note": r["note"],
            }
            for r in rows
        ],
    }


@router.get("/receivables-trend")
def receivables_trend(
    admin_key: str = Query(...),
    currency: str = Query("UZS"),
):
    """Month-end total receivables per period — excludes suppliers.

    Sums (closing_debit - closing_credit) across real-client rows for each
    period_start. Negative closings (client overpayments / credits) are netted
    against positive ones to give the true trade-receivable figure.
    """
    from backend.services.pseudo_clients import (
        sql_exclusion_clause, sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()
    excl_clause = sql_exclusion_clause('cb.client_name_1c')
    rows = conn.execute(f"""
        SELECT cb.period_start,
               SUM(cb.closing_debit - cb.closing_credit) AS net_receivable,
               SUM(CASE WHEN (cb.closing_debit - cb.closing_credit) > 0
                        THEN (cb.closing_debit - cb.closing_credit) ELSE 0 END) AS positive_only,
               SUM(cb.period_debit) AS shipments,
               SUM(cb.period_credit) AS collections,
               COUNT(DISTINCT cb.client_name_1c) AS clients_with_row
          FROM client_balances cb
         WHERE cb.currency = ?
           AND cb.period_start >= '2025-01-01'
           AND {excl_clause}
         GROUP BY cb.period_start
         ORDER BY cb.period_start ASC
    """, (currency, *sql_exclusion_params())).fetchall()
    conn.close()

    from datetime import date
    today = date.today().replace(day=1).isoformat()
    return {
        "ok": True,
        "currency": currency,
        "periods": [
            {
                "period": r["period_start"],
                "month": r["period_start"][:7],
                "net_receivable": round(r["net_receivable"] or 0, 2),
                "positive_only": round(r["positive_only"] or 0, 2),
                "shipments": round(r["shipments"] or 0, 2),
                "collections": round(r["collections"] or 0, 2),
                "clients": r["clients_with_row"],
                "partial": r["period_start"] == today,
            }
            for r in rows
        ],
    }


# ── Client History (drill-down) ──────────────────────────────────


@router.get("/client/{client_name}/history")
def client_history(
    client_name: str,
    admin_key: str = Query(...),
):
    """Per-client balance history — monthly bars, with USD-equivalent merge.

    Returns native UZS and USD legs (back-compat for any other caller) plus a
    pre-merged `history_usd_eq` series that converts UZS via the month's
    average daily_fx_rates (FX_FALLBACK 12,000 when no rate covers the month).
    """
    _check_admin(admin_key)
    conn = get_db()

    rows = conn.execute("""
        SELECT currency, period_start, period_end,
               opening_debit, opening_credit,
               period_debit, period_credit,
               closing_debit, closing_credit
        FROM client_balances
        WHERE client_name_1c = ?
        ORDER BY currency, period_start ASC
    """, (client_name,)).fetchall()

    if not rows:
        conn.close()
        return {
            "ok": True,
            "client_name": client_name,
            "history": {},
            "history_usd_eq": [],
        }

    history: dict = {}
    months: set = set()
    for r in rows:
        cur = r["currency"]
        history.setdefault(cur, []).append({
            "period": r["period_start"],
            "period_end": r["period_end"],
            "period_debit": round(r["period_debit"] or 0, 2),
            "period_credit": round(r["period_credit"] or 0, 2),
            "closing_debit": round(r["closing_debit"] or 0, 2),
            "closing_credit": round(r["closing_credit"] or 0, 2),
            "balance": round((r["closing_debit"] or 0) - (r["closing_credit"] or 0), 2),
        })
        months.add(r["period_start"])

    # Per-month average FX for the UZS leg
    fx_by_month: dict = {}
    if months:
        ms = min(months)
        fx_rows = conn.execute(
            """SELECT strftime('%Y-%m-01', rate_date) AS month, AVG(rate) AS r
                 FROM daily_fx_rates
                WHERE currency_pair = 'USD_UZS'
                  AND rate_date >= ?
                GROUP BY month""",
            (ms,),
        ).fetchall()
        for row in fx_rows:
            fx_by_month[row["month"]] = float(row["r"] or FX_FALLBACK)

    def _fx_for(month: str) -> float:
        return fx_by_month.get(month, FX_FALLBACK)

    # Merge UZS + USD per period into USD-eq
    uzs_by_period = {p["period"]: p for p in history.get("UZS", [])}
    usd_by_period = {p["period"]: p for p in history.get("USD", [])}
    all_periods = sorted(set(uzs_by_period) | set(usd_by_period))

    history_usd_eq = []
    for period in all_periods:
        u = uzs_by_period.get(period, {})
        d = usd_by_period.get(period, {})
        fx = _fx_for(period) or FX_FALLBACK
        debit_usd_eq = (d.get("period_debit") or 0) + (u.get("period_debit") or 0) / fx
        credit_usd_eq = (d.get("period_credit") or 0) + (u.get("period_credit") or 0) / fx
        balance_usd_eq = (d.get("balance") or 0) + (u.get("balance") or 0) / fx
        history_usd_eq.append({
            "period": period,
            "period_end": u.get("period_end") or d.get("period_end"),
            "period_debit": round(debit_usd_eq, 2),
            "period_credit": round(credit_usd_eq, 2),
            "balance": round(balance_usd_eq, 2),
            "fx_rate": round(fx, 2),
        })

    conn.close()
    return {
        "ok": True,
        "client_name": client_name,
        "history": history,
        "history_usd_eq": history_usd_eq,
    }


# ─── client_balance_overrides — admin-managed authoritative balance values ────
#
# Solves the structural gap where 1C's Дебиторская задолженность report omits
# credit-balance clients (Бахтиёр case): cabinet shows 0 when the client
# actually has money in their favor. Admin verifies the real balance in 1C
# акт сверки, POSTs it here, and get_effective_debt() reads it BEFORE the
# daily-upload picker. Override is per-client_id; older row for the same
# client is upserted (so an updated override replaces the previous value).


@router.get("/balance-overrides")
def list_balance_overrides(admin_key: str = Query(...)):
    _check_admin(admin_key)
    conn = get_db()
    rows = conn.execute(
        """SELECT o.client_id, o.debt_uzs, o.debt_usd, o.source, o.reason,
                  o.set_by_user_id, o.set_by_name, o.set_at, o.expires_at, o.notes,
                  ac.name AS client_name, ac.client_id_1c, ac.phone_normalized
           FROM client_balance_overrides o
           LEFT JOIN allowed_clients ac ON ac.id = o.client_id
           ORDER BY o.set_at DESC"""
    ).fetchall()
    conn.close()
    return {"ok": True, "count": len(rows), "overrides": [dict(r) for r in rows]}


@router.post("/balance-override")
def set_balance_override(
    admin_key: str = Form(...),
    client_id: int = Form(...),
    debt_uzs: float = Form(0.0),
    debt_usd: float = Form(0.0),
    source: str = Form(""),
    reason: str = Form(""),
    set_by_name: str = Form(""),
    set_by_user_id: int = Form(0),
    expires_at: str = Form(""),
    notes: str = Form(""),
):
    """Upsert an override. Semantics match get_effective_debt: positive
    debt_uzs/debt_usd = client owes us; negative = we owe the client.
    `source` should cite the 1C document/report verified against
    (e.g. 'akt_sverki_2026-05-19')."""
    _check_admin(admin_key)
    conn = get_db()
    ac = conn.execute("SELECT id, name FROM allowed_clients WHERE id=?", (client_id,)).fetchone()
    if not ac:
        conn.close()
        raise HTTPException(status_code=404, detail=f"client_id={client_id} not in allowed_clients")
    conn.execute(
        """INSERT INTO client_balance_overrides
              (client_id, debt_uzs, debt_usd, source, reason,
               set_by_user_id, set_by_name, set_at, expires_at, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?)
           ON CONFLICT(client_id) DO UPDATE SET
              debt_uzs=excluded.debt_uzs,
              debt_usd=excluded.debt_usd,
              source=excluded.source,
              reason=excluded.reason,
              set_by_user_id=excluded.set_by_user_id,
              set_by_name=excluded.set_by_name,
              set_at=datetime('now'),
              expires_at=excluded.expires_at,
              notes=excluded.notes""",
        (client_id, debt_uzs, debt_usd, source or None, reason or None,
         set_by_user_id or None, set_by_name or None,
         expires_at or None, notes or None),
    )
    conn.commit()
    conn.close()
    return {
        "ok": True,
        "client_id": client_id,
        "client_name": ac["name"],
        "debt_uzs": debt_uzs,
        "debt_usd": debt_usd,
        "source": source,
    }


@router.delete("/balance-override/{client_id}")
def delete_balance_override(client_id: int, admin_key: str = Query(...)):
    _check_admin(admin_key)
    conn = get_db()
    cur = conn.execute("DELETE FROM client_balance_overrides WHERE client_id=?", (client_id,))
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    return {"ok": True, "client_id": client_id, "deleted": deleted}
