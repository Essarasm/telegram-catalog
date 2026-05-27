"""Canonical revenue queries against real_orders.

Why this module exists
======================

The `real_orders.currency` column is a 1C export quirk: every row in the
Реализация export comes through tagged `'USD'`, regardless of whether the
underlying doc is UZS-denominated, USD-denominated, or dual-currency. The
column looks like a per-row denomination tag (and sibling tables like
`client_payments.currency` legitimately work that way), but on real_orders
it is structurally **always 'USD'**.

Consumers that write `WHERE currency = 'UZS'` against real_orders silently
return zero for the UZS leg. This shape has now hit at four boundary
layers:
  - #20 (import-skip, schema-drift)
  - #49 (display, UI render)
  - #57 (import-skip, pre-parse filter)
  - now (query-filter on real_orders)

The dual-currency truth lives in the **line items**:
  - `total_sum`           — UZS leg (sum of `real_order_items.total_local`)
  - `total_sum_currency`  — USD leg (sum of `real_order_items.total_currency`)

Both are always present and always honest. Single-currency docs simply
have one leg = 0. Dual-currency docs have both > 0.

This module is the **only** sanctioned way to compute revenue from
real_orders. The pre-commit guard `SKIP_REALORDERS_CURRENCY_FILTER_CHECK`
rejects new code that bypasses this helper with `WHERE currency =`
filters against real_orders. See `.claude/skills/recurring-family-
investigation.md` for the discipline that produced this module.
"""

from __future__ import annotations
from typing import Optional, Sequence
from backend.database import get_db


def realorders_revenue(
    date_min: Optional[str] = None,
    date_max: Optional[str] = None,
    client_id: Optional[int | Sequence[int]] = None,
    only_approved: bool = False,
    exclude_pseudo: bool = False,
    conn=None,
) -> dict:
    """Compute dual-currency revenue from real_orders.

    Sums `total_sum` (UZS leg) and `total_sum_currency` (USD leg) directly,
    bypassing the misleading `currency` column entirely. Both legs are
    independent — a doc may contribute to both, one, or neither.

    Args:
        date_min, date_max: ISO date strings; inclusive. Either can be None.
        client_id: int or iterable of ints; if provided, filter to these.
                   For multi-phone clients use `get_sibling_client_ids`.
        only_approved: True → only V-marked rows (is_approved=1 or legacy
                       NULL treated as approved). False → all rows.
        exclude_pseudo: True → exclude Наличка / СТРОЙКА / Возврат /
                        Организации / Исправление clients.
        conn: optional existing connection. Caller-supplied conn is NOT
              closed by this function.

    Returns:
        {
            "uzs": float,        # UZS leg revenue
            "usd": float,        # USD leg revenue
            "doc_count": int,
            "uzs_only_docs": int,    # docs with only UZS leg > 0
            "usd_only_docs": int,    # docs with only USD leg > 0
            "dual_docs": int,        # docs with both legs > 0
        }
    """
    owned = conn is None
    if owned:
        conn = get_db()
    try:
        where = ["1=1"]
        params: list = []
        if date_min:
            where.append("doc_date >= ?")
            params.append(date_min)
        if date_max:
            where.append("doc_date <= ?")
            params.append(date_max)
        if client_id is not None:
            if isinstance(client_id, (list, tuple, set)):
                ids = list(client_id)
                if not ids:
                    return _empty()
                ph = ",".join("?" * len(ids))
                where.append(f"client_id IN ({ph})")
                params.extend(ids)
            else:
                where.append("client_id = ?")
                params.append(int(client_id))
        if only_approved:
            # NULL is treated as approved for legacy compatibility (v18 schema
            # change captures the marker forward; old rows are NULL).
            where.append("COALESCE(is_approved, 1) = 1")
        if exclude_pseudo:
            from backend.services.pseudo_clients import SYSTEM_NON_CLIENT_NAMES
            ph = ",".join("?" * len(SYSTEM_NON_CLIENT_NAMES))
            where.append(f"COALESCE(client_name_1c, '') NOT IN ({ph})")
            params.extend(SYSTEM_NON_CLIENT_NAMES)

        row = conn.execute(
            f"""SELECT
                    COALESCE(SUM(total_sum), 0)                AS uzs,
                    COALESCE(SUM(total_sum_currency), 0)       AS usd,
                    COUNT(*)                                   AS doc_count,
                    SUM(CASE WHEN COALESCE(total_sum,0) > 0
                              AND COALESCE(total_sum_currency,0) = 0
                             THEN 1 ELSE 0 END)                AS uzs_only_docs,
                    SUM(CASE WHEN COALESCE(total_sum,0) = 0
                              AND COALESCE(total_sum_currency,0) > 0
                             THEN 1 ELSE 0 END)                AS usd_only_docs,
                    SUM(CASE WHEN COALESCE(total_sum,0) > 0
                              AND COALESCE(total_sum_currency,0) > 0
                             THEN 1 ELSE 0 END)                AS dual_docs
                FROM real_orders
                WHERE {' AND '.join(where)}""",
            tuple(params),
        ).fetchone()
        return {
            "uzs": float(row["uzs"] or 0),
            "usd": float(row["usd"] or 0),
            "doc_count": int(row["doc_count"] or 0),
            "uzs_only_docs": int(row["uzs_only_docs"] or 0),
            "usd_only_docs": int(row["usd_only_docs"] or 0),
            "dual_docs": int(row["dual_docs"] or 0),
        }
    finally:
        if owned:
            conn.close()


def realorders_revenue_by_client(
    date_min: str,
    date_max: str,
    only_approved: bool = False,
    exclude_pseudo: bool = True,
    limit: Optional[int] = None,
    conn=None,
) -> list[dict]:
    """Per-client revenue rollup; same dual-currency contract as the
    aggregate helper. Sorted by UZS desc, USD desc as tiebreaker.

    Returns a list of dicts with `client_name_1c, uzs, usd, doc_count`.
    """
    owned = conn is None
    if owned:
        conn = get_db()
    try:
        where = ["doc_date BETWEEN ? AND ?",
                "client_name_1c IS NOT NULL",
                "client_name_1c != ''"]
        params: list = [date_min, date_max]
        if only_approved:
            where.append("COALESCE(is_approved, 1) = 1")
        if exclude_pseudo:
            from backend.services.pseudo_clients import SYSTEM_NON_CLIENT_NAMES
            ph = ",".join("?" * len(SYSTEM_NON_CLIENT_NAMES))
            where.append(f"client_name_1c NOT IN ({ph})")
            params.extend(SYSTEM_NON_CLIENT_NAMES)

        sql = f"""SELECT client_name_1c,
                         COALESCE(SUM(total_sum), 0)          AS uzs,
                         COALESCE(SUM(total_sum_currency), 0) AS usd,
                         COUNT(*)                             AS doc_count
                  FROM real_orders
                  WHERE {' AND '.join(where)}
                  GROUP BY client_name_1c
                  ORDER BY uzs DESC, usd DESC"""
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = conn.execute(sql, tuple(params)).fetchall()
        return [
            {
                "client_name_1c": r["client_name_1c"],
                "uzs": float(r["uzs"] or 0),
                "usd": float(r["usd"] or 0),
                "doc_count": int(r["doc_count"] or 0),
            }
            for r in rows
        ]
    finally:
        if owned:
            conn.close()


def _empty() -> dict:
    return {
        "uzs": 0.0, "usd": 0.0, "doc_count": 0,
        "uzs_only_docs": 0, "usd_only_docs": 0, "dual_docs": 0,
    }
