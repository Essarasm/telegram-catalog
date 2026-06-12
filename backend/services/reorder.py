"""Per-supplier reorder recommendation engine — drives /zakazlar.

Formula (lead-time aware, with year-over-year seasonality):
    sold_window        = real sales over last `window_days` (default 30)
    sold_window       += demand_signals.quantity over same window (lost demand)
    daily_rate         = sold_window / window_days
    seasonal_mult      = (last-year same-month daily) / (last-year prior-60d daily)
                         fallback 1.0 if last-year same-month has <30 units sold
    seasoned_daily     = daily_rate × seasonal_mult
    lead_time          = median inter-delivery gap in supply_orders for the
                         (supplier, product) pair, ≥3 events required.
                         Fallback to supplier-level median, then global 14d.
    reorder_point      = seasoned_daily × lead_time × safety_factor   (1.5)
    target_qty         = seasoned_daily × (lead_time + review) × safety
    suggested_buy      = max(0, ceil(target_qty − stock))
    days_of_cover      = stock / seasoned_daily

CHRONIC-STOCKOUT RECONSTRUCTION (the "silent stockout" fix):
    A product that has been out of stock for the whole window sells 0 units
    (you can't sell what you don't have), so sold_window≈0 → seasoned_daily≈0
    → suggested_buy=0. The engine files it as no_recent_demand and recommends
    buying nothing — circular: chronic stockouts make themselves invisible.
    Fix: for stock<=0 AND zero recent demand, reconstruct daily velocity from
    the `window_days` ending at the product's LAST real sale (real_order_items
    — NOT demand_signals, which is structurally sparse for agent-placed B2B).
    Those rows get a real suggested_buy and the `chronic_stockout` status, and
    carry demand_source='pre_stockout' so the UI can flag the lower-confidence
    (reconstructed) number.

Status bucket (drives sort order, lower = more urgent):
    1 stockout            — seasoned_daily>0 AND stock<=0 (live demand)
    2 chronic_stockout    — stock<=0, no recent demand, velocity reconstructed
                            from pre-stockout sales
    3 order_now           — stock < reorder_point
    4 order_soon          — stock < target_qty
    5 ok                  — stock ≥ target_qty
    6 no_recent_demand    — seasoned_daily≈0 AND never sold (truly dead)

NO PRICES surfaced (per memory feedback_order_prep_no_prices).
"""
from __future__ import annotations

import datetime as _dt
import math
from statistics import median
from typing import List, Optional, Tuple

from backend.database import get_db


DEFAULT_WINDOW_DAYS = 30
DEFAULT_REVIEW_PERIOD_DAYS = 7
DEFAULT_GLOBAL_LEAD_TIME_DAYS = 14
DEFAULT_SAFETY_FACTOR = 1.5
MIN_SUPPLY_EVENTS_FOR_MEDIAN = 3
MIN_YOY_UNITS = 30
DAILY_EPSILON = 1e-6


def list_suppliers_with_products(conn=None) -> List[dict]:
    """Active suppliers with at least one product mapped via latest_supplier_id,
    plus a synthetic '(noma'lum)' bucket for products with NULL latest_supplier_id.
    Sorted: oos_count desc, product_count desc, name asc.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT s.id, s.name_1c,
                   COUNT(p.id) AS product_count,
                   SUM(CASE WHEN COALESCE(p.stock_quantity,0) <= 0 THEN 1 ELSE 0 END) AS oos_count
              FROM suppliers s
              JOIN products p ON p.latest_supplier_id = s.id AND p.is_active = 1
             WHERE s.is_active = 1
             GROUP BY s.id, s.name_1c
             ORDER BY oos_count DESC, product_count DESC, s.name_1c
            """
        ).fetchall()
        result = [dict(r) for r in rows]

        unknown = conn.execute(
            """SELECT COUNT(*) AS n,
                      SUM(CASE WHEN COALESCE(stock_quantity,0) <= 0 THEN 1 ELSE 0 END) AS oos
                 FROM products
                WHERE is_active = 1 AND latest_supplier_id IS NULL"""
        ).fetchone()
        if unknown and unknown["n"] > 0:
            result.append({
                "id": None,
                "name_1c": "(noma'lum supplier)",
                "product_count": unknown["n"],
                "oos_count": unknown["oos"] or 0,
            })
        return result
    finally:
        if own_conn:
            conn.close()


def recent_sales_map(conn, window_start: str) -> dict:
    """{product_id: (units_sold_in_window, last_sale_date)} over all products.

    Computed ONCE and reused across every supplier — this aggregation scans the
    140k-row real_order_items and is supplier-independent, so running it inside
    the per-supplier loop (43×) was the dominant cost (~337ms × 43 ≈ 15s →
    intermittent 502s). Callers that loop suppliers must pass the result into
    list_supplier_full(sales_map=...).
    """
    rows = conn.execute(
        """SELECT roi.product_id AS pid,
                  SUM(roi.quantity) AS sold,
                  MAX(ro.doc_date) AS last_sale
             FROM real_order_items roi
             JOIN real_orders ro ON ro.id = roi.real_order_id
            WHERE ro.doc_date >= ? AND roi.product_id IS NOT NULL
            GROUP BY roi.product_id""",
        (window_start,),
    ).fetchall()
    return {r["pid"]: (float(r["sold"] or 0), r["last_sale"]) for r in rows}


def _median_gap_days(sorted_dates: List[str]) -> Optional[float]:
    """Median gap in days between consecutive ISO-date strings. None if <2 dates."""
    if len(sorted_dates) < 2:
        return None
    gaps = []
    prev = _dt.date.fromisoformat(sorted_dates[0])
    for d_str in sorted_dates[1:]:
        cur = _dt.date.fromisoformat(d_str)
        delta = (cur - prev).days
        if delta > 0:
            gaps.append(delta)
        prev = cur
    if not gaps:
        return None
    return float(median(gaps))


def _classify_status(seasoned_daily: float, stock: float,
                     reorder_point: float, target_qty: float) -> str:
    if seasoned_daily <= DAILY_EPSILON:
        return "no_recent_demand"
    if stock <= 0:
        return "stockout"
    if stock < reorder_point:
        return "order_now"
    if stock < target_qty:
        return "order_soon"
    return "ok"


STATUS_ORDER = {
    "stockout": 1,
    "chronic_stockout": 2,
    "order_now": 3,
    "order_soon": 4,
    "ok": 5,
    "no_recent_demand": 6,
}


def list_supplier_full(
    supplier_id: Optional[int],
    window_days: int = DEFAULT_WINDOW_DAYS,
    review_period_days: int = DEFAULT_REVIEW_PERIOD_DAYS,
    safety_factor: float = DEFAULT_SAFETY_FACTOR,
    today: Optional[_dt.date] = None,
    conn=None,
    sales_map: Optional[dict] = None,
) -> List[dict]:
    """All active products for a supplier (or unmapped bucket if supplier_id=None)
    with computed forecast fields. Sorted by status priority then days_of_cover asc.

    `sales_map` ({pid: (sold, last_sale)} from recent_sales_map): pass it in when
    looping suppliers so the expensive sales aggregation runs once, not per call.
    If None it's computed here (fine for one-off / standalone / test callers).
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db()
    try:
        if today is None:
            today = _dt.date.today()
        window_start = (today - _dt.timedelta(days=window_days)).isoformat()

        if sales_map is None:
            sales_map = recent_sales_map(conn, window_start)

        if supplier_id is None:
            sup_clause = "p.latest_supplier_id IS NULL"
            sup_params: Tuple = ()
        else:
            sup_clause = "p.latest_supplier_id = ?"
            sup_params = (supplier_id,)

        prod_rows = conn.execute(
            f"""
            SELECT p.id AS product_id,
                   p.name,
                   p.lifecycle,
                   COALESCE(p.stock_quantity, 0) AS stock
              FROM products p
             WHERE p.is_active = 1
               AND {sup_clause}
            """,
            sup_params,
        ).fetchall()

        if not prod_rows:
            return []

        product_ids = [r["product_id"] for r in prod_rows]
        placeholders = ",".join(["?"] * len(product_ids))

        ds_rows = conn.execute(
            f"""SELECT product_id, COALESCE(SUM(quantity), 0) AS qty
                  FROM demand_signals
                 WHERE product_id IN ({placeholders})
                   AND date(created_at) >= date(?)
                 GROUP BY product_id""",
            (*product_ids, window_start),
        ).fetchall()
        demand_signal_qty = {r["product_id"]: float(r["qty"]) for r in ds_rows}

        supply_events_by_product: dict[int, List[str]] = {}
        supplier_supply_dates: List[str] = []
        if supplier_id is not None:
            ev_rows = conn.execute(
                f"""SELECT soi.matched_product_id AS pid, so.doc_date
                      FROM supply_orders so
                      JOIN supply_order_items soi ON soi.supply_order_id = so.id
                      JOIN suppliers sup ON sup.name_1c = so.counterparty_name
                     WHERE sup.id = ?
                       AND so.doc_type = 'supply'
                       AND soi.matched_product_id IN ({placeholders})
                     ORDER BY soi.matched_product_id, so.doc_date""",
                (supplier_id, *product_ids),
            ).fetchall()
            for r in ev_rows:
                supply_events_by_product.setdefault(r["pid"], []).append(r["doc_date"])

            sup_rows = conn.execute(
                """SELECT so.doc_date
                     FROM supply_orders so
                     JOIN suppliers sup ON sup.name_1c = so.counterparty_name
                    WHERE sup.id = ? AND so.doc_type = 'supply'
                    ORDER BY so.doc_date""",
                (supplier_id,),
            ).fetchall()
            supplier_supply_dates = [r["doc_date"] for r in sup_rows]

        supplier_median_gap = _median_gap_days(supplier_supply_dates) \
            if len(supplier_supply_dates) >= MIN_SUPPLY_EVENTS_FOR_MEDIAN else None

        same_month_start = today.replace(year=today.year - 1, day=1)
        if same_month_start.month == 12:
            same_month_end = same_month_start.replace(day=31)
        else:
            next_m = same_month_start.replace(month=same_month_start.month + 1, day=1)
            same_month_end = next_m - _dt.timedelta(days=1)
        sm_days = (same_month_end - same_month_start).days + 1
        prior_60_end = same_month_start - _dt.timedelta(days=1)
        prior_60_start = prior_60_end - _dt.timedelta(days=59)

        yoy_rows = conn.execute(
            f"""SELECT roi.product_id AS pid,
                       SUM(CASE WHEN ro.doc_date BETWEEN ? AND ?
                                THEN roi.quantity ELSE 0 END) AS sm_qty,
                       SUM(CASE WHEN ro.doc_date BETWEEN ? AND ?
                                THEN roi.quantity ELSE 0 END) AS prior_qty
                  FROM real_order_items roi
                  JOIN real_orders ro ON ro.id = roi.real_order_id
                 WHERE roi.product_id IN ({placeholders})
                   AND ro.doc_date BETWEEN ? AND ?
                 GROUP BY roi.product_id""",
            (
                same_month_start.isoformat(), same_month_end.isoformat(),
                prior_60_start.isoformat(), prior_60_end.isoformat(),
                *product_ids,
                prior_60_start.isoformat(), same_month_end.isoformat(),
            ),
        ).fetchall()
        yoy_by_product = {r["pid"]: (float(r["sm_qty"]), float(r["prior_qty"]))
                          for r in yoy_rows}

        # ── Chronic-stockout velocity reconstruction ──────────────────
        # Products out of stock with zero recent demand are blind spots:
        # they sell 0 only because there's nothing to sell. Reconstruct
        # their velocity from the window ending at their last real sale so
        # they get a real suggested_buy instead of vanishing.
        recon_daily: dict[int, float] = {}
        recon_last_sale: dict[int, str] = {}
        candidate_pids = [
            r["product_id"] for r in prod_rows
            if float(r["stock"] or 0) <= 0
            and (sales_map.get(r["product_id"], (0.0, None))[0]
                 + demand_signal_qty.get(r["product_id"], 0.0)) <= DAILY_EPSILON
        ]
        if candidate_pids:
            cph = ",".join(["?"] * len(candidate_pids))
            window_modifier = f"-{int(window_days)} days"
            # Last sale per candidate (grouped ONCE in the subquery), then sum
            # sales in the window ending at it. Grouped join, NOT a per-row
            # correlated subquery — the correlated form was ~25× slower on the
            # 140k-row real_order_items and made hot-list/scoreboard/seasonal
            # take ~18s → intermittent 502s + blank dashboard.
            recon_rows = conn.execute(
                f"""SELECT lsq.pid AS pid, lsq.ls AS last_sale,
                           SUM(CASE WHEN ro.doc_date > date(lsq.ls, ?)
                                    THEN roi.quantity ELSE 0 END) AS presold
                      FROM (SELECT roi2.product_id AS pid, MAX(ro2.doc_date) AS ls
                              FROM real_order_items roi2
                              JOIN real_orders ro2 ON ro2.id = roi2.real_order_id
                             WHERE roi2.product_id IN ({cph})
                             GROUP BY roi2.product_id) lsq
                      JOIN real_order_items roi ON roi.product_id = lsq.pid
                      JOIN real_orders ro ON ro.id = roi.real_order_id
                     GROUP BY lsq.pid""",
                (window_modifier, *candidate_pids),
            ).fetchall()
            for rr in recon_rows:
                recon_last_sale[rr["pid"]] = rr["last_sale"] or ""
                presold = float(rr["presold"] or 0)
                if presold > 0 and window_days > 0:
                    recon_daily[rr["pid"]] = presold / window_days

        result = []
        for r in prod_rows:
            pid = r["product_id"]
            stock = float(r["stock"])
            sm_sold, sm_last_sale = sales_map.get(pid, (0.0, None))
            base_sold = float(sm_sold or 0)
            ds_qty = demand_signal_qty.get(pid, 0.0)
            adjusted_sold = base_sold + ds_qty

            daily_rate = adjusted_sold / window_days if window_days > 0 else 0.0

            # Chronic stockout: no live demand, but it sold before running
            # out — use the reconstructed pre-stockout velocity instead of 0.
            demand_source = "live"
            if pid in recon_daily and stock <= 0 and adjusted_sold <= DAILY_EPSILON:
                daily_rate = recon_daily[pid]
                demand_source = "pre_stockout"

            sm_qty, prior_qty = yoy_by_product.get(pid, (0.0, 0.0))
            if sm_qty >= MIN_YOY_UNITS and prior_qty > 0 and sm_days > 0:
                sm_daily = sm_qty / sm_days
                prior_daily = prior_qty / 60.0
                seasonal_mult = sm_daily / prior_daily if prior_daily > 0 else 1.0
                seasonal_source = "yoy"
            else:
                seasonal_mult = 1.0
                seasonal_source = "fallback"

            seasoned_daily = daily_rate * seasonal_mult

            prod_dates = supply_events_by_product.get(pid, [])
            prod_gap = _median_gap_days(prod_dates) \
                if len(prod_dates) >= MIN_SUPPLY_EVENTS_FOR_MEDIAN else None
            if prod_gap is not None:
                lead_time = prod_gap
                lead_source = "product"
            elif supplier_median_gap is not None:
                lead_time = supplier_median_gap
                lead_source = "supplier"
            else:
                lead_time = float(DEFAULT_GLOBAL_LEAD_TIME_DAYS)
                lead_source = "global"

            reorder_point = seasoned_daily * lead_time * safety_factor
            target_qty = seasoned_daily * (lead_time + review_period_days) * safety_factor
            suggested_buy = max(0, math.ceil(target_qty - stock))
            days_of_cover = (stock / seasoned_daily) if seasoned_daily > DAILY_EPSILON else None
            status = _classify_status(seasoned_daily, stock, reorder_point, target_qty)
            # Reconstructed-demand stockouts get their own bucket so the UI
            # can flag them as lower-confidence than live-demand stockouts.
            if demand_source == "pre_stockout" and seasoned_daily > DAILY_EPSILON:
                status = "chronic_stockout"

            result.append({
                "product_id": pid,
                "name": r["name"],
                "lifecycle": r["lifecycle"] or "",
                "stock": stock,
                "sold_window": base_sold,
                "demand_signal_qty": ds_qty,
                "demand_source": demand_source,
                "daily_rate": round(daily_rate, 3),
                "seasonal_mult": round(seasonal_mult, 2),
                "seasonal_source": seasonal_source,
                "seasoned_daily": round(seasoned_daily, 3),
                "lead_time_days": round(lead_time, 1),
                "lead_time_source": lead_source,
                "reorder_point": round(reorder_point, 1),
                "target_qty": round(target_qty, 1),
                "suggested_buy": suggested_buy,
                "days_of_cover": round(days_of_cover, 1) if days_of_cover is not None else None,
                "last_sale": sm_last_sale or recon_last_sale.get(pid, "") or "",
                "status": status,
            })

        def _sort_key(it):
            doc = it["days_of_cover"]
            return (STATUS_ORDER.get(it["status"], 99),
                    float("inf") if doc is None else doc)
        result.sort(key=_sort_key)
        return result
    finally:
        if own_conn:
            conn.close()


def compute_supplier_reorder(
    supplier_id: Optional[int],
    window_days: int = DEFAULT_WINDOW_DAYS,
    review_period_days: int = DEFAULT_REVIEW_PERIOD_DAYS,
    safety_factor: float = DEFAULT_SAFETY_FACTOR,
    today: Optional[_dt.date] = None,
    conn=None,
) -> List[dict]:
    """Subset of list_supplier_full: only rows where suggested_buy > 0."""
    full = list_supplier_full(
        supplier_id, window_days, review_period_days, safety_factor, today, conn
    )
    return [r for r in full if r["suggested_buy"] > 0]
