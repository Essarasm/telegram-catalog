# -*- coding: utf-8 -*-
"""Daily supply-order decision — fuses delivery backlog + inventory need.

Computed on demand (dashboard load, after the day's stock + realorders uploads).
Answers, in plain terms: "tomorrow, order from supplier X these items" OR
"hold — the delivery backlog is piled up, clear it first."

Two inputs:
  - Delivery backlog: unshipped (X / is_approved=0) orders' tonnage
    (real_order_items.total_weight is in KG → /1000; same source + conversion
    as x_queue.py). The "are deliveries piled up?" signal.
  - Inventory need: the reorder engine (reorder.list_supplier_full) — what's
    below reorder point per supplier, with quantities + $ value.

Framed by the weekly fixed-supplier schedule (Uncle/Plan_Sklad_Savdo): each
weekday has an assigned supplier (ЭЛЕРОН Wed/Thu/Fri = the ~⅓-of-intake anchor).
The schedule decides *whose turn*; this daily pass decides *go/no-go* (backlog)
and *quantities* (live need).

HOLD rule (data-backed, self-adjusting): if backlog > HOLD_MULTIPLIER × the
trailing-30d median daily delivery tonnage (~37t now), recommend clearing
deliveries first so the crew isn't slammed both directions (the Jun-4 jam).
"""
from __future__ import annotations

import datetime as _dt
from collections import defaultdict
from statistics import median
from typing import Optional

from backend.database import get_db
from backend.services import reorder

# Weekday (Mon=0 … Sun=6) → supplier name patterns (substring match on name_1c).
# Source: Uncle/Plan_Sklad_Savdo_RU_UZ.docx fixed-day proposal. Editable as the
# owner locks suppliers into real slots.
SUPPLY_SCHEDULE = {
    0: ["ГВОЗДИ"],               # Mon
    1: ["УЗКАБЕЛЬ"],             # Tue
    2: ["ЭЛЕРОН"],              # Wed   (anchor 1/3)
    3: ["ЭЛЕРОН", "ЛИНОЛЕУМ"],  # Thu   (anchor + linoleum)
    4: ["ЭЛЕРОН", "DELUX"],     # Fri   (anchor + delux)
    5: ["LAMA"],                # Sat   (+ others, catch-all via overdue list)
    6: [],                       # Sun
}
_WD_UZ = ["Dushanba", "Seshanba", "Chorshanba", "Payshanba",
          "Juma", "Shanba", "Yakshanba"]

HOLD_MULTIPLIER = 2.0       # backlog > N days of delivery → hold ordering
DELIVERY_WINDOW_DAYS = 30


def _backlog_tonnes(conn) -> float:
    r = conn.execute(
        """SELECT COALESCE(SUM(CAST(COALESCE(roi.total_weight, 0) AS REAL)), 0)
             FROM real_order_items roi
             JOIN real_orders ro ON ro.id = roi.real_order_id
            WHERE COALESCE(ro.is_approved, 1) = 0"""
    ).fetchone()
    return float(r[0] or 0) / 1000.0


def _median_daily_delivery_tonnes(conn) -> float:
    rows = conn.execute(
        """SELECT ro.doc_date, SUM(CAST(COALESCE(roi.total_weight, 0) AS REAL)) AS t
             FROM real_order_items roi
             JOIN real_orders ro ON ro.id = roi.real_order_id
            WHERE COALESCE(ro.is_approved, 1) = 1
              AND ro.doc_date >= date('now', ?)
            GROUP BY ro.doc_date HAVING t > 0""",
        (f"-{DELIVERY_WINDOW_DAYS} days",),
    ).fetchall()
    tons = [float(r[1] or 0) / 1000.0 for r in rows]
    return float(median(tons)) if tons else 0.0


def _resolve_suppliers(conn, patterns):
    out, seen = [], set()
    for pat in patterns:
        row = conn.execute(
            """SELECT id, name_1c FROM suppliers
                WHERE is_active = 1 AND name_1c LIKE ?
                ORDER BY id LIMIT 1""",
            (f"%{pat}%",),
        ).fetchone()
        if row and row["id"] not in seen:
            seen.add(row["id"])
            out.append({"id": row["id"], "name_1c": row["name_1c"], "pattern": pat})
    return out


def _supplier_order(conn, supplier_id, sales_map, fx_rate):
    """Reorder list (suggested_buy>0) for a supplier + roll-up totals + est tonnes."""
    items = [it for it in reorder.list_supplier_full(
        supplier_id, conn=conn, sales_map=sales_map, fx_rate=fx_rate)
        if it["suggested_buy"] > 0]
    pids = [it["product_id"] for it in items]
    wmap = {}
    if pids:
        ph = ",".join(["?"] * len(pids))
        wmap = {r["id"]: float(r["w"] or 0) for r in conn.execute(
            f"SELECT id, COALESCE(weight, 0) AS w FROM products WHERE id IN ({ph})", pids)}
    est_t = sum(wmap.get(it["product_id"], 0) * it["suggested_buy"] for it in items) / 1000.0
    return {
        "n_items": len(items),
        "total_buy_units": sum(it["suggested_buy"] for it in items),
        "total_value_usd": sum(it.get("order_value_usd", 0) for it in items),
        "est_tonnes": round(est_t, 1),
        "items": items,
    }


def _overdue_suppliers(conn, today, limit=8):
    """Suppliers past their own delivery cadence (days_since ≥ median gap)."""
    ev = conn.execute(
        """SELECT counterparty_name AS nm, doc_date FROM supply_orders
            WHERE doc_type = 'supply' AND doc_date >= date('now', '-180 days')
            ORDER BY counterparty_name, doc_date"""
    ).fetchall()
    by = defaultdict(list)
    for r in ev:
        by[r["nm"]].append(r["doc_date"])
    res = []
    for nm, dates in by.items():
        u = sorted(set(dates))
        if len(u) < 3:
            continue
        gaps = [(_dt.date.fromisoformat(u[i]) - _dt.date.fromisoformat(u[i - 1])).days
                for i in range(1, len(u))]
        gaps = [g for g in gaps if g > 0]
        if not gaps:
            continue
        mg = float(median(gaps))
        dsl = (today - _dt.date.fromisoformat(u[-1])).days
        if dsl >= mg:
            res.append({"supplier_name": nm, "median_gap_days": round(mg),
                        "days_since": dsl, "last_supply": u[-1],
                        "overdue_by": round(dsl - mg)})
    res.sort(key=lambda r: -r["overdue_by"])
    return res[:limit]


def snapshot_backlog(conn, today: Optional[_dt.date] = None) -> dict:
    """Record today's delivery backlog (forward-capture). real_orders overwrites
    its V/X flag each upload, so without this ledger the day-by-day backlog is
    lost. Idempotent per date — call at the end of each realorders import."""
    if today is None:
        today = _dt.date.today()
    backlog = round(_backlog_tonnes(conn), 1)
    n_orders = int(conn.execute(
        "SELECT COUNT(*) FROM real_orders WHERE COALESCE(is_approved, 1) = 0"
    ).fetchone()[0] or 0)
    med = _median_daily_delivery_tonnes(conn)
    threshold = round(HOLD_MULTIPLIER * med, 1)
    decision = "HOLD" if (threshold > 0 and backlog > threshold) else "GO"
    conn.execute(
        """INSERT OR REPLACE INTO supply_backlog_daily
             (snapshot_date, backlog_tonnes, backlog_orders, hold_threshold_tonnes,
              decision, captured_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))""",
        (today.isoformat(), backlog, n_orders, threshold, decision),
    )
    conn.commit()
    return {"date": today.isoformat(), "backlog_tonnes": backlog,
            "orders": n_orders, "decision": decision}


def load_backtest(conn, days: int = 56) -> dict:
    """Per-day supply-in vs delivery-out tonnage over the window — reconstructable
    from the dated rows we keep (supply_orders, real_orders). Joins the captured
    backlog snapshots where they exist (they accrue going forward). Flags the
    overload (slammed-both-directions) and zero-supply days."""
    win = f"-{int(days)} days"
    deliv = {r["d"]: float(r["t"] or 0) / 1000.0 for r in conn.execute(
        """SELECT ro.doc_date AS d, SUM(CAST(COALESCE(roi.total_weight, 0) AS REAL)) AS t
             FROM real_order_items roi JOIN real_orders ro ON ro.id = roi.real_order_id
            WHERE COALESCE(ro.is_approved, 1) = 1 AND ro.doc_date >= date('now', ?)
            GROUP BY ro.doc_date""", (win,))}
    sup = {}
    for r in conn.execute(
        """SELECT so.doc_date AS d,
                  SUM(CASE WHEN lower(COALESCE(soi.unit, '')) IN ('кг', 'kg')
                           THEN CAST(COALESCE(soi.quantity, 0) AS REAL)
                           ELSE CAST(COALESCE(soi.quantity, 0) AS REAL) * COALESCE(p.weight, 0)
                      END) AS kg
             FROM supply_orders so
             JOIN supply_order_items soi ON soi.supply_order_id = so.id
             LEFT JOIN products p ON p.id = soi.matched_product_id
            WHERE so.doc_type = 'supply' AND so.doc_date >= date('now', ?)
            GROUP BY so.doc_date""", (win,)):
        sup[r["d"]] = float(r["kg"] or 0) / 1000.0
    snaps = {r["snapshot_date"]: r for r in conn.execute(
        "SELECT * FROM supply_backlog_daily WHERE snapshot_date >= date('now', ?)", (win,))}

    med = _median_daily_delivery_tonnes(conn)
    overload_ceiling = round(3 * med, 1)   # combined in+out beyond ~3 days of flow = slammed
    today = _dt.date.today()
    rows, overload_days, zero_supply_days = [], 0, 0
    for i in range(days, -1, -1):
        d = today - _dt.timedelta(days=i)
        if d.weekday() == 6:               # skip Sundays
            continue
        ds = d.isoformat()
        si = round(sup.get(ds, 0.0), 1)
        do = round(deliv.get(ds, 0.0), 1)
        total = round(si + do, 1)
        is_overload = total > overload_ceiling
        if is_overload:
            overload_days += 1
        if si == 0:
            zero_supply_days += 1
        snap = snaps.get(ds)
        rows.append({
            "date": ds, "weekday": d.weekday(),
            "supply_in_t": si, "delivery_out_t": do, "total_t": total,
            "overload": is_overload,
            "backlog_t": snap["backlog_tonnes"] if snap else None,
            "decision": snap["decision"] if snap else None,
        })
    return {
        "days": days,
        "median_delivery_tonnes": round(med, 1),
        "overload_ceiling_tonnes": overload_ceiling,
        "overload_days": overload_days,
        "zero_supply_days": zero_supply_days,
        "rows": rows,
    }


def compute_daily_plan(conn=None, today: Optional[_dt.date] = None) -> dict:
    own = conn is None
    if own:
        conn = get_db()
    try:
        if today is None:
            today = _dt.date.today()
        backlog = round(_backlog_tonnes(conn), 1)
        med = _median_daily_delivery_tonnes(conn)
        threshold = round(HOLD_MULTIPLIER * med, 1)
        hold = bool(threshold > 0 and backlog > threshold)

        # tomorrow's working day (skip Sunday)
        tomorrow = today + _dt.timedelta(days=1)
        if tomorrow.weekday() == 6:
            tomorrow += _dt.timedelta(days=1)
        wd = tomorrow.weekday()

        sales_map = reorder.recent_sales_map(
            conn, (today - _dt.timedelta(days=reorder.DEFAULT_WINDOW_DAYS)).isoformat())
        fxrow = conn.execute(
            """SELECT rate FROM daily_fx_rates WHERE currency_pair='USD_UZS'
                AND rate > 0 ORDER BY rate_date DESC LIMIT 1""").fetchone()
        fx = float(fxrow["rate"]) if fxrow else None

        scheduled = _resolve_suppliers(conn, SUPPLY_SCHEDULE.get(wd, []))
        scheduled_ids = {s["id"] for s in scheduled}

        # Priority order list — every supplier with items needing reorder, ranked
        # by total money-velocity ($/day), schedule-agnostic. The owner's "what to
        # order, most-urgent-first, regardless of whose day it is" view. Each item
        # list lives behind the per-supplier Excel; here we ship roll-ups + a top-3.
        priority_orders = []
        for s in reorder.list_suppliers_with_products(conn=conn):
            o = _supplier_order(conn, s["id"], sales_map, fx)
            if o["n_items"] == 0:
                continue
            items = o.pop("items")
            o["supplier_id"] = s["id"]
            o["supplier_name"] = s["name_1c"]
            o["scheduled_tomorrow"] = s["id"] in scheduled_ids
            o["total_throughput_usd"] = round(
                sum(it.get("daily_throughput_usd", 0) for it in items), 1)
            o["top_items"] = [
                {"name": it["name"], "throughput_usd": it.get("daily_throughput_usd", 0),
                 "suggested_buy": it["suggested_buy"], "status": it["status"]}
                for it in sorted(items, key=lambda x: -(x.get("daily_throughput_usd") or 0))[:3]
            ]
            priority_orders.append(o)
        priority_orders.sort(key=lambda o: -o["total_throughput_usd"])

        overdue = _overdue_suppliers(conn, today)

        if hold:
            decision = "HOLD"
            reason = (f"Yetkazish navbati {backlog} t (≈{round(backlog / med, 1) if med else '?'} "
                      f"kunlik) — chegaradan ({threshold} t) yuqori. Avval yetkazishni tugating; "
                      f"faqat shoshilinch tugagan tovarlarni buyurtma qiling.")
        else:
            decision = "GO"
            reason = (f"Yetkazish navbati {backlog} t — chegaradan ({threshold} t) past. "
                      f"Ertaga buyurtma berish mumkin.")

        return {
            "as_of": today.isoformat(),
            "decision": decision,
            "reason": reason,
            "backlog_tonnes": backlog,
            "median_daily_delivery_tonnes": round(med, 1),
            "hold_threshold_tonnes": threshold,
            "tomorrow": {
                "date": tomorrow.isoformat(),
                "weekday_uz": _WD_UZ[wd],
                "scheduled": [s["name_1c"] for s in scheduled],
            },
            "priority_orders": priority_orders,
            "overdue_suppliers": overdue,
            "schedule": {str(k): v for k, v in SUPPLY_SCHEDULE.items()},
        }
    finally:
        if own:
            conn.close()
