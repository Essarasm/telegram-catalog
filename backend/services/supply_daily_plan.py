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
            WHERE COALESCE(ro.is_approved, 1) = 0
              AND ro.stale_expired_at IS NULL"""
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
    """Reorder list (suggested_buy>0) for a supplier + roll-ups, money-velocity,
    and the urgency tier/mix. `urgency_tier` = the most-urgent item's status rank
    (stockout=1 … order_soon=4) so callers can sort URGENCY-first, velocity-within
    — i.e. "what's most critically empty, and among those what moves most money.\""""
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
    status_counts = {}
    for it in items:
        status_counts[it["status"]] = status_counts.get(it["status"], 0) + 1
    urgency_tier = min(
        (reorder.STATUS_ORDER.get(it["status"], 99) for it in items), default=99)
    # Urgency DEPTH × velocity: $/day of the items that are actually OUT of stock
    # (stockout + chronic). Ranking by this floats a supplier with many high-
    # velocity stockouts above one with higher TOTAL velocity but few stockouts.
    urgent_throughput = round(sum(
        it.get("daily_throughput_usd", 0) for it in items
        if it["status"] in ("stockout", "chronic_stockout")), 1)
    top_items = [
        {"name": it["name"], "throughput_usd": it.get("daily_throughput_usd", 0),
         "suggested_buy": it["suggested_buy"], "status": it["status"]}
        for it in sorted(items, key=lambda x: -(x.get("daily_throughput_usd") or 0))[:3]
    ]
    return {
        "n_items": len(items),
        "total_buy_units": sum(it["suggested_buy"] for it in items),
        "total_value_usd": sum(it.get("order_value_usd", 0) for it in items),
        "est_tonnes": round(est_t, 1),
        "total_throughput_usd": round(
            sum(it.get("daily_throughput_usd", 0) for it in items), 1),
        "urgent_throughput_usd": urgent_throughput,
        "status_counts": status_counts,
        "urgency_tier": urgency_tier,
        "top_items": top_items,
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
        "SELECT COUNT(*) FROM real_orders "
        "WHERE COALESCE(is_approved, 1) = 0 AND stale_expired_at IS NULL"
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


def capture_unshipped_daily(conn) -> int:
    """Persist per-day NOT-DELIVERED (X) recorded-sales tonnage + USD-eq revenue.
    real_orders overwrites the V/X flag on every re-import, so the undelivered
    backlog has to be captured the moment it's seen. Reads the CURRENT X rows
    (is_approved=0) grouped by doc_date and upserts into unshipped_daily, keeping
    the MAX tonnes per day — a later (mostly-delivered) re-upload reports fewer X
    and must NOT erase the original peak. Returns the number of days touched.

    USD-eq = USD leg + UZS leg / 12000 (canonical realorders_revenue formula).
    KG → /1000 for tonnes. See `.claude/skills/operational-resource-balancing.md`
    and the unshipped_daily table in database.py."""
    _FX = 12000.0
    # Scope to the recent window only. Without this the query sweeps ALL-TIME
    # is_approved=0 and rakes in year-old single-doc orphans (X marks never
    # updated) that aren't an active daily backlog. 90 days comfortably covers
    # the backtest chart range (14/30) and any daily upload's dates.
    rows = conn.execute(
        """SELECT doc_date AS d,
                  SUM(CAST(COALESCE(total_weight, 0) AS REAL)) / 1000.0 AS t,
                  SUM(COALESCE(total_sum_currency, 0) + COALESCE(total_sum, 0) / ?) AS rev,
                  COUNT(*) AS n
             FROM real_orders
            WHERE is_approved = 0 AND doc_date IS NOT NULL
              AND doc_date >= date('now', '-90 days')
            GROUP BY doc_date HAVING t > 0""", (_FX,)).fetchall()
    for r in rows:
        conn.execute(
            """INSERT INTO unshipped_daily (doc_date, tonnes, revenue_usd, doc_count, source, updated_at)
               VALUES (?, ?, ?, ?, 'realorders_import', datetime('now'))
               ON CONFLICT(doc_date) DO UPDATE SET
                   revenue_usd = CASE WHEN excluded.tonnes > unshipped_daily.tonnes
                                      THEN excluded.revenue_usd ELSE unshipped_daily.revenue_usd END,
                   doc_count   = CASE WHEN excluded.tonnes > unshipped_daily.tonnes
                                      THEN excluded.doc_count ELSE unshipped_daily.doc_count END,
                   tonnes      = MAX(unshipped_daily.tonnes, excluded.tonnes),
                   updated_at  = datetime('now')""",
            (r["d"], round(float(r["t"] or 0), 2), round(float(r["rev"] or 0), 1), int(r["n"] or 0)))
    conn.commit()
    return len(rows)


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
    # Delivered revenue, USD-eq = USD leg + UZS leg / 12000 (canonical realorders_revenue
    # formula; the `currency` column is always 'USD' on real_orders — a 1C export quirk).
    _FX = 12000.0
    rev = {r["d"]: float(r["usd"] or 0) for r in conn.execute(
        """SELECT doc_date AS d,
                  SUM(COALESCE(total_sum_currency, 0) + COALESCE(total_sum, 0) / ?) AS usd
             FROM real_orders
            WHERE COALESCE(is_approved, 1) = 1 AND doc_date >= date('now', ?)
            GROUP BY doc_date""", (_FX, win))}
    # Not-delivered (X) recorded-sales tonnage, backfilled from the dated X exports.
    unship = {r["doc_date"]: float(r["tonnes"] or 0) for r in conn.execute(
        "SELECT doc_date, tonnes FROM unshipped_daily WHERE doc_date >= date('now', ?)", (win,))}

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
            "revenue_k": round(rev.get(ds, 0.0) / 1000.0, 1),
            "unshipped_x_t": round(unship.get(ds, 0.0), 1),
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


def _unshipped_summary(conn) -> dict:
    """The 'orders not yet shipped' (delivery backlog) by zone — JSON-safe
    reshape of x_queue.compute_x_queue (which returns un-serialisable sets).
    The sales/delivery side of the weekly plan: what's pending to deliver, so
    the owner can sequence the next few days. Now accurate post Error Log #97."""
    from backend.services.x_queue import _suggest_truck, compute_x_queue
    q = compute_x_queue(conn)
    zones = []
    if q["city"]["orders"]:
        c = q["city"]
        zones.append({"zone": "Samarqand shahar", "tonnes": round(c["tonnes"], 1),
                      "orders": c["orders"], "no_pin": c["no_pin"],
                      "truck": _suggest_truck(c["tonnes"])})
    for z, v in q["districts"].items():
        zones.append({"zone": z, "tonnes": round(v["tonnes"], 1),
                      "orders": v["orders"], "no_pin": v["no_pin"],
                      "truck": _suggest_truck(v["tonnes"])})
    if q["unlocated"]["orders"]:
        u = q["unlocated"]
        zones.append({"zone": "(joylashuvsiz)", "tonnes": round(u["tonnes"], 1),
                      "orders": u["orders"], "no_pin": u["orders"], "truck": "—"})
    zones.sort(key=lambda x: -x["tonnes"])
    # Per-order list (client name + tonnage), heaviest first — drives the
    # dashboard's "Joʻnatilmagan buyurtmalar" table (zones kept for callers
    # like the chart that still aggregate by zone).
    orders = [{"name": o["name"] or "(nomsiz)", "tonnes": round(o["tonnes"], 1)}
              for o in q.get("orders", [])]
    return {"total_orders": q["total_orders"], "total_tonnes": q["total_t"],
            "zones": zones, "orders": orders, "latest_day": q.get("latest_day")}


# Delivery-day schedule (Mon=0 … Sat=5) → district keyword(s), from the load-
# balancing session's Heavy-Client Call List (steer whales to their district's
# day to even the peaks). Latin keyword match on allowed_clients.tuman/viloyat.
DELIVERY_SCHEDULE = {
    0: ["payariq", "chelak"],        # Dushanba — Челак/Паярик
    1: ["urgut"],                    # Seshanba — Ургут
    2: ["oqdaryo", "jomboy"],        # Chorshanba — Окдарье/Жамбай
    3: ["pastdarg"],                 # Payshanba — Пастдаргом/Жума
    4: ["kattaqo", "nurobod"],       # Juma — Каттакурган/Нурабад
    5: ["bulung", "samarqand sh"],   # Shanba — Булунгур/город
}


def _assigned_delivery_day(tuman, viloyat) -> Optional[int]:
    blob = f"{tuman or ''} {viloyat or ''}".lower()
    for wd, keys in DELIVERY_SCHEDULE.items():
        if any(k in blob for k in keys):
            return wd
    return None


def _delivery_distribution(conn) -> dict:
    """Heavy/Large clients mapped to their district's delivery day — the live,
    data-driven Heavy-Client Call List. Reuses client_portfolio's 12-mo USD-eq
    bucketing; flags clients ordering OFF their assigned day (the scatter to
    steer). Steering is human; this shows WHO to call onto WHICH day."""
    from backend.services.client_portfolio import FX_FALLBACK, _bucket
    from backend.services.pseudo_clients import (
        sql_exclusion_clause, sql_exclusion_params)

    fx_rows = conn.execute(
        "SELECT rate FROM daily_fx_rates WHERE currency_pair='USD_UZS' AND rate>0").fetchall()
    avg_fx = (sum(float(r["rate"]) for r in fx_rows) / len(fx_rows)) if fx_rows else FX_FALLBACK
    level_start = (_dt.date.today() - _dt.timedelta(days=365)).isoformat()
    excl = sql_exclusion_clause("client_name_1c")
    excl_params = sql_exclusion_params()

    rows = conn.execute(
        f"""SELECT ro.client_id AS cid, MAX(ac.client_id_1c) AS name,
                   MAX(ac.tuman) AS tuman, MAX(ac.viloyat) AS viloyat,
                   SUM(ro.total_sum) AS uzs, SUM(ro.total_sum_currency) AS usd
              FROM real_orders ro JOIN allowed_clients ac ON ac.id = ro.client_id
             WHERE ro.doc_date >= ? AND COALESCE(ro.is_approved, 1) = 1
               AND ro.client_id IS NOT NULL AND {excl}
             GROUP BY ro.client_id""",
        (level_start, *excl_params),
    ).fetchall()

    clients = {}
    for r in rows:
        monthly = (float(r["uzs"] or 0) / avg_fx + float(r["usd"] or 0)) / 12.0
        tier = _bucket(monthly)
        if tier not in ("Heavy", "Large"):
            continue
        clients[r["cid"]] = {
            "name": r["name"], "tuman": r["tuman"], "tier": tier,
            "monthly_usd": round(monthly),
            "assigned_day": _assigned_delivery_day(r["tuman"], r["viloyat"]),
            "weekdays": set(),
        }

    if clients:
        ph = ",".join(["?"] * len(clients))
        for r in conn.execute(
            f"""SELECT client_id AS cid, strftime('%w', doc_date) AS w
                  FROM real_orders WHERE client_id IN ({ph})
                   AND COALESCE(is_approved, 1) = 1 AND doc_date >= date('now', '-56 days')""",
                tuple(clients)):
            clients[r["cid"]]["weekdays"].add((int(r["w"]) - 1) % 7)  # %w Sun=0 → Mon=0

    days = []
    for wd in range(6):
        cl = []
        for c in clients.values():
            if c["assigned_day"] != wd:
                continue
            wds = c["weekdays"]
            cl.append({
                "name": c["name"], "tier": c["tier"], "tuman": c["tuman"],
                "monthly_usd": c["monthly_usd"],
                "scattered": bool(wds and (len(wds) > 1 or wd not in wds)),
                "order_weekdays": sorted(wds),
            })
        cl.sort(key=lambda x: -x["monthly_usd"])
        days.append({"weekday": wd, "weekday_uz": _WD_UZ[wd], "clients": cl})

    unassigned = sorted(
        ({"name": c["name"], "tier": c["tier"], "tuman": c["tuman"], "monthly_usd": c["monthly_usd"]}
         for c in clients.values() if c["assigned_day"] is None),
        key=lambda x: -x["monthly_usd"])
    return {
        "heavy_threshold": 4120, "large_threshold": 1721,
        "total_heavy": sum(1 for c in clients.values() if c["tier"] == "Heavy"),
        "total_large": sum(1 for c in clients.values() if c["tier"] == "Large"),
        "days": days,
        "unassigned": unassigned,
    }


def compute_weekly_plan(conn=None) -> dict:
    """7-day (Mon–Sat) forward view of the fixed supplier schedule with each
    scheduled supplier's CURRENT order (items/$/tonnes). v1 = current need (not
    forward-projected). Multi-day anchors (ЭЛЕРОН Wed/Thu/Fri) are computed once
    and their order spread across their delivery days so per-day tonnage is
    realistic and the week total isn't multiplied. Target ~22 t/day (one truck)."""
    own = conn is None
    if own:
        conn = get_db()
    try:
        today = _dt.date.today()
        sales_map = reorder.recent_sales_map(
            conn, (today - _dt.timedelta(days=reorder.DEFAULT_WINDOW_DAYS)).isoformat())
        fxrow = conn.execute(
            """SELECT rate FROM daily_fx_rates WHERE currency_pair='USD_UZS'
                AND rate > 0 ORDER BY rate_date DESC LIMIT 1""").fetchone()
        fx = float(fxrow["rate"]) if fxrow else None

        resolved_by_wd, sched_days = {}, {}
        for wd in range(6):                       # Mon..Sat
            sups = _resolve_suppliers(conn, SUPPLY_SCHEDULE.get(wd, []))
            resolved_by_wd[wd] = sups
            for s in sups:
                sched_days.setdefault(s["id"], {"name": s["name_1c"], "days": 0})
                sched_days[s["id"]]["days"] += 1

        orders = {}
        for sid, info in sched_days.items():
            o = _supplier_order(conn, sid, sales_map, fx)
            o.pop("items")
            o["supplier_id"] = sid
            o["supplier_name"] = info["name"]
            o["n_days"] = info["days"]
            orders[sid] = o

        days, week_value, week_tonnes, counted = [], 0.0, 0.0, set()
        for wd in range(6):
            day_sups, day_t, day_v = [], 0.0, 0.0
            for s in resolved_by_wd[wd]:
                o = orders[s["id"]]
                nd = max(1, o["n_days"])
                per_t = round(o["est_tonnes"] / nd, 1)
                per_v = round(o["total_value_usd"] / nd)
                day_sups.append({**o, "per_day_tonnes": per_t, "per_day_value": per_v})
                day_t += per_t
                day_v += per_v
                if s["id"] not in counted:
                    week_value += o["total_value_usd"]
                    week_tonnes += o["est_tonnes"]
                    counted.add(s["id"])
            # urgency depth ($/day out-of-stock) first, then total velocity
            day_sups.sort(key=lambda x: (-x["urgent_throughput_usd"], -x["total_throughput_usd"]))
            days.append({
                "weekday": wd, "weekday_uz": _WD_UZ[wd],
                "suppliers": day_sups,
                "day_tonnes": round(day_t, 1), "day_value": round(day_v),
                "overload": day_t > 22,
            })
        return {
            "target_tonnes_per_day": 22,
            "days": days,
            "week_total_value_usd": round(week_value),
            "week_total_tonnes": round(week_tonnes, 1),
            "unshipped": _unshipped_summary(conn),
            "delivery_distribution": _delivery_distribution(conn),
        }
    finally:
        if own:
            conn.close()


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
            o.pop("items")
            o["supplier_id"] = s["id"]
            o["supplier_name"] = s["name_1c"]
            o["scheduled_tomorrow"] = s["id"] in scheduled_ids
            priority_orders.append(o)
        # Urgency DEPTH first — $/day stuck in out-of-stock items — then total
        # velocity as tiebreak. Surfaces "most money sitting in empty shelves".
        priority_orders.sort(
            key=lambda o: (-o["urgent_throughput_usd"], -o["total_throughput_usd"]))

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
