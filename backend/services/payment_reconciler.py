"""Phase 3 — nightly cashier ↔ 1C reconciler.

Populates `payment_reconciliation` with per-row pairing of `intake_payments`
(cashier-recorded) against `client_payments` (Alisher's 1C Касса), so the
Kabinet's "Tekshirish kerak" red flag becomes per-row accurate rather than
per-client-aggregate (Phase 2.5).

The matcher runs aggregate-first per client:

  Pass 1 — window aggregate. If the cashier USD-eq sum over the window
           matches the 1C USD-eq sum within tolerance, trust the
           aggregate and mark every row 'matched'. This captures Bahrom's
           case: Alisher's internal FX rate (~10,650) drifts from market
           (~12,150), so per-day the FX gap looks like a mismatch, but
           across the window it averages out within 2%.

  Pass 2 — per-day pairing. Only reached when Pass 1 fails (real
           cashier↔1C divergence). For each cashier-day D, sum cashier
           USD-eq and compare against 1C summed over [D, D+1] (Alisher
           routinely codes the previous day's cash on the next morning).
           If they agree, mark involved rows 'matched'.

Anything still unmatched after both passes is recorded as 'bot_only' (the
cashier saw it but 1C didn't) or 'kassa_only' (1C has it but cashier
didn't — typically wire transfers that bypassed the cashier flow).

Tolerance: max($2, 2% of the larger side). Validated against 97 active
clients on 2026-05-13 — clean matches clustered ≤2%, real mismatches
started at ≥17%; the gap leaves wide safety margin.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

_FX_FALLBACK_UZS_PER_USD = 12200.0
_TOLERANCE_PCT = 0.02
_TOLERANCE_FLOOR_USD = 2.0


def _to_usd_eq(amount: float, currency: str, fx_rate: float | None) -> float:
    if currency == "USD":
        return float(amount or 0)
    if currency == "UZS":
        rate = fx_rate if fx_rate and fx_rate > 0 else _FX_FALLBACK_UZS_PER_USD
        return float(amount or 0) / rate
    return 0.0


def _within_tolerance(a: float, b: float) -> bool:
    tol = max(_TOLERANCE_FLOOR_USD, _TOLERANCE_PCT * max(abs(a), abs(b)))
    return abs(a - b) <= tol


def _fetch_intake(conn, client_id: int, lookback_days: int) -> list[dict]:
    rows = conn.execute(
        """SELECT ip.id, ip.amount, ip.currency, date(ip.submitted_at) AS d,
                  fx.rate AS fx_rate
           FROM intake_payments ip
           LEFT JOIN daily_fx_rates fx
                  ON fx.rate_date = date(ip.submitted_at)
                 AND fx.currency_pair = 'USD_UZS'
           WHERE ip.client_id = ?
             AND ip.status = 'confirmed'
             AND ip.submitted_at >= datetime('now', ?)
           ORDER BY ip.submitted_at""",
        (client_id, f"-{int(lookback_days)} days"),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "usdeq": _to_usd_eq(r["amount"], r["currency"], r["fx_rate"]),
            "date": r["d"],
        }
        for r in rows
    ]


def _fetch_onec(conn, client_id: int, lookback_days: int) -> list[dict]:
    # +1 day buffer on the upper end because Alisher codes today's cash
    # tomorrow morning.
    rows = conn.execute(
        """SELECT cp.doc_number_1c, cp.currency, cp.amount_local,
                  cp.amount_currency, cp.doc_date,
                  fx.rate AS fx_rate
           FROM client_payments cp
           LEFT JOIN daily_fx_rates fx
                  ON fx.rate_date = cp.doc_date
                 AND fx.currency_pair = 'USD_UZS'
           WHERE cp.client_id = ?
             AND cp.doc_date >= date('now', ?)
             AND cp.doc_date <= date('now', '+1 day')
           ORDER BY cp.doc_date""",
        (client_id, f"-{int(lookback_days)} days"),
    ).fetchall()
    out = []
    for r in rows:
        if r["currency"] == "USD":
            usdeq = float(r["amount_currency"] or 0)
        else:
            usdeq = _to_usd_eq(r["amount_local"], "UZS", r["fx_rate"])
        out.append({"doc_no": r["doc_number_1c"], "usdeq": usdeq, "date": r["doc_date"]})
    return out


def _candidate_client_ids(conn, lookback_days: int) -> list[int]:
    """Every client_id with confirmed intake OR client_payments in window."""
    rows = conn.execute(
        """SELECT DISTINCT client_id FROM (
               SELECT client_id FROM intake_payments
               WHERE status = 'confirmed'
                 AND submitted_at >= datetime('now', ?)
                 AND client_id IS NOT NULL
               UNION
               SELECT client_id FROM client_payments
               WHERE doc_date >= date('now', ?)
                 AND client_id IS NOT NULL
           )""",
        (f"-{int(lookback_days)} days", f"-{int(lookback_days)} days"),
    ).fetchall()
    return [r["client_id"] for r in rows]


def _match_client(intake: list[dict], onec: list[dict]) -> dict:
    """Run the aggregate-first matcher and return per-id outcomes.

    Returns:
        {
            "matched_intake":     dict[int, str]   (id -> notes)
            "matched_docs":       set[str]
            "used_aggregate":     bool             (Pass 1 succeeded)
            "bot_only_ids":       set[int]
            "kassa_only_doc_nos": set[str]
        }
    """
    matched_intake: dict[int, str] = {}
    matched_docs: set[str] = set()

    # Restrict 1C to dates plausibly tied to cashier activity (±1 day around
    # each cashier-day). 1C rows outside this band are wire transfers or
    # pre-cashier-system payments — they get pre-classified as kassa_only
    # rather than skewing the aggregate.
    cashier_active_dates: set[str] = set()
    for r in intake:
        try:
            y, m, dd = (int(p) for p in r["date"].split("-"))
            base = date(y, m, dd)
            cashier_active_dates.add((base - timedelta(days=1)).isoformat())
            cashier_active_dates.add(base.isoformat())
            cashier_active_dates.add((base + timedelta(days=1)).isoformat())
        except (ValueError, AttributeError):
            cashier_active_dates.add(r["date"])
    onec_in_band = [r for r in onec if r["date"] in cashier_active_dates]

    # Pass 1 — window aggregate against the in-band 1C subset.
    intake_total = sum(r["usdeq"] for r in intake)
    onec_total = sum(r["usdeq"] for r in onec_in_band)
    used_aggregate = (
        intake_total > 0 and onec_total > 0
        and _within_tolerance(intake_total, onec_total)
    )
    if used_aggregate:
        for r in intake:
            matched_intake[r["id"]] = "window-aggregate"
        for r in onec_in_band:
            matched_docs.add(r["doc_no"])
    else:
        # Pass 2 — per-day matching.
        intake_by_day: dict[str, list[dict]] = defaultdict(list)
        for r in intake:
            intake_by_day[r["date"]].append(r)
        onec_by_day: dict[str, list[dict]] = defaultdict(list)
        for r in onec:
            onec_by_day[r["date"]].append(r)

        for d, intake_rows in sorted(intake_by_day.items()):
            same_day_docs = [r for r in onec_by_day.get(d, []) if r["doc_no"] not in matched_docs]
            try:
                y, m, dd = (int(p) for p in d.split("-"))
                nd = (date(y, m, dd) + timedelta(days=1)).isoformat()
            except (ValueError, AttributeError):
                nd = d
            next_day_docs = [r for r in onec_by_day.get(nd, []) if r["doc_no"] not in matched_docs]
            window_docs = same_day_docs + next_day_docs

            cashier_total = sum(r["usdeq"] for r in intake_rows)
            day_onec_total = sum(r["usdeq"] for r in window_docs)
            if cashier_total > 0 and _within_tolerance(cashier_total, day_onec_total):
                for r in intake_rows:
                    matched_intake[r["id"]] = "day-match"
                for r in window_docs:
                    matched_docs.add(r["doc_no"])

    bot_only = {r["id"] for r in intake if r["id"] not in matched_intake}
    kassa_only = {r["doc_no"] for r in onec if r["doc_no"] not in matched_docs}
    return {
        "matched_intake": matched_intake,
        "matched_docs": matched_docs,
        "used_aggregate": used_aggregate,
        "bot_only_ids": bot_only,
        "kassa_only_doc_nos": kassa_only,
    }


def reconcile_payments(
    conn,
    lookback_days: int = 30,
    reconcile_date: Optional[str] = None,
) -> dict:
    """Run the full reconciliation and rewrite the day's
    `payment_reconciliation` rows.

    Idempotent within a day: deletes any rows for `reconcile_date` (default
    today, Tashkent date) before re-inserting, so re-running mid-day
    refreshes the snapshot rather than duplicating.

    Returns a summary:
        {
            "reconcile_date": "YYYY-MM-DD",
            "clients_examined": int,
            "matched_rows": int,         # intake rows marked matched
            "bot_only_rows": int,
            "kassa_only_rows": int,
            "aggregate_matches": int,    # clients where Pass 1 succeeded
        }
    """
    if reconcile_date is None:
        reconcile_date = conn.execute(
            "SELECT date('now', '+5 hours') AS d"
        ).fetchone()["d"]

    conn.execute(
        "DELETE FROM payment_reconciliation WHERE reconcile_date = ?",
        (reconcile_date,),
    )

    client_ids = _candidate_client_ids(conn, lookback_days)
    matched = 0
    bot_only = 0
    kassa_only = 0
    fallbacks = 0

    for cid in client_ids:
        intake = _fetch_intake(conn, cid, lookback_days)
        onec = _fetch_onec(conn, cid, lookback_days)
        if not intake and not onec:
            continue

        result = _match_client(intake, onec)
        if result["used_aggregate"]:
            fallbacks += 1

        for iid, notes in result["matched_intake"].items():
            # Pair each matched intake row with the doc_no list of its day
            # cohort (joined as a comma-separated audit string — kassa_doc_no
            # is TEXT, multiple cashier rows can share a 1C doc).
            conn.execute(
                """INSERT INTO payment_reconciliation
                   (reconcile_date, bot_payment_id, kassa_doc_no, match_status, notes)
                   VALUES (?, ?, ?, 'matched', ?)""",
                (
                    reconcile_date,
                    iid,
                    ",".join(sorted(result["matched_docs"])) or None,
                    notes,
                ),
            )
            matched += 1

        for iid in result["bot_only_ids"]:
            conn.execute(
                """INSERT INTO payment_reconciliation
                   (reconcile_date, bot_payment_id, kassa_doc_no, match_status, notes)
                   VALUES (?, ?, NULL, 'bot_only', NULL)""",
                (reconcile_date, iid),
            )
            bot_only += 1

        for doc_no in result["kassa_only_doc_nos"]:
            conn.execute(
                """INSERT INTO payment_reconciliation
                   (reconcile_date, bot_payment_id, kassa_doc_no, match_status, notes)
                   VALUES (?, NULL, ?, 'kassa_only', NULL)""",
                (reconcile_date, doc_no),
            )
            kassa_only += 1

    conn.commit()
    summary = {
        "reconcile_date": reconcile_date,
        "clients_examined": len(client_ids),
        "matched_rows": matched,
        "bot_only_rows": bot_only,
        "kassa_only_rows": kassa_only,
        "aggregate_matches": fallbacks,
    }
    logger.info("payment_reconciler summary: %s", summary)
    return summary


def get_intake_match_status(conn, intake_ids: list[int]) -> dict[int, str]:
    """Look up the latest payment_reconciliation match_status for each
    intake_payments.id. Returns id -> 'matched' / 'bot_only' / 'unknown'.

    Used by /api/payments/pending-for-client to decide the per-row
    `reconciled` flag. When no row exists yet (first run hasn't happened,
    or the intake row was created since the last run), the caller should
    fall back to the Phase 2.5 client_reconciliation_check aggregate.
    """
    if not intake_ids:
        return {}
    placeholders = ",".join("?" * len(intake_ids))
    rows = conn.execute(
        f"""SELECT bot_payment_id AS id, match_status
            FROM payment_reconciliation
            WHERE bot_payment_id IN ({placeholders})
              AND reconcile_date = (
                  SELECT MAX(reconcile_date)
                  FROM payment_reconciliation
                  WHERE bot_payment_id IN ({placeholders})
              )""",
        tuple(intake_ids) + tuple(intake_ids),
    ).fetchall()
    return {r["id"]: r["match_status"] for r in rows}
