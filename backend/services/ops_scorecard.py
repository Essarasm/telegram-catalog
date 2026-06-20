# -*- coding: utf-8 -*-
"""Weekly ops scorecard — the learning engine for operational-resource-balancing.

Four KPIs that measure even-distribution of human resource + revenue and how
effectively the resources at hand are used:

  1. Crew-load spread (t)  — peak−trough of daily (supply-IN + delivery-OUT) tonnes.
                             Lower = more even human-resource load across the week.
  2. Revenue spread (×)    — peak ÷ median daily revenue (USD-eq). Lower = steadier.
  3. Truck fill (%)        — avg delivered load ÷ vehicle capacity per trip.
                             Higher = trucks run fuller (resource effectiveness).
  4. Order aging (days)    — how long X (recorded-but-not-shipped) orders wait now.

Read-only; reuses validated data sources (real_order_items.total_weight is the
authoritative 1C weight; supply weight via products.weight). See the skill
.claude/skills/operational-resource-balancing.md for the methodology.
"""
from __future__ import annotations

import re
import statistics
from datetime import date, timedelta

from backend.database import get_db

FX_FALLBACK = 12050.0  # UZS per USD; UZS leg is small vs USD leg, so exactness doesn't matter
TRUCK_CAP = {"isuzu": 7.0, "foton": 3.0, "jac": 2.5, "labo": 1.5}
_WD_UZ = ["Du", "Se", "Cho", "Pa", "Ju", "Sha"]

# comment-tag → truck type (tags mix vehicle names with driver names; both map here)
_VEH_KEYS = (
    ("isuzu", "isuzu"), ("исуз", "isuzu"),
    ("foton", "foton"), ("фотон", "foton"),
    ("jac", "jac"), ("жак", "jac"),
    ("labo", "labo"), ("лабо", "labo"),
    ("sherzod", "labo"), ("шерзод", "labo"),   # Sherzod drives a Labo
    ("umid", "labo"), ("умид", "labo"),         # bare "Labo" tag = Umid
)


def _vehicle(comment: str):
    s = (comment or "").lower()
    if "dostavka" not in s:
        return None
    s = re.sub(r"[^a-zа-я]", "", s.replace("dostavka", ""))
    for key, truck in _VEH_KEYS:
        if key in s:
            return truck
    return None


def _last_full_week_monday(ref: date | None = None) -> date:
    """Monday of the most recently completed work week (Mon..Sat) before `ref`."""
    ref = ref or date.today()
    this_monday = ref - timedelta(days=ref.weekday())
    return this_monday - timedelta(days=7)


def compute_scorecard(conn, week_monday: date) -> dict:
    """Compute the four KPIs for the Mon..Sat week starting `week_monday`."""
    cur = conn.cursor()
    a = week_monday.isoformat()
    b = (week_monday + timedelta(days=5)).isoformat()  # Saturday

    # 1. crew-load spread — daily supply-IN + delivery-OUT tonnes
    in_by, out_by = {}, {}
    for r in cur.execute(
        "SELECT so.doc_date, SUM(si.quantity*COALESCE(p.weight,0)) "
        "FROM supply_orders so JOIN supply_order_items si ON si.supply_order_id=so.id "
        "LEFT JOIN products p ON p.id=si.matched_product_id "
        "WHERE so.doc_type='supply' AND so.doc_date BETWEEN ? AND ? GROUP BY so.doc_date", (a, b)
    ):
        in_by[r[0]] = (r[1] or 0) / 1000.0
    for r in cur.execute(
        "SELECT ro.doc_date, SUM(ri.total_weight) FROM real_orders ro "
        "JOIN real_order_items ri ON ri.real_order_id=ro.id "
        "WHERE COALESCE(ro.is_approved,1)=1 AND ro.doc_date BETWEEN ? AND ? GROUP BY ro.doc_date", (a, b)
    ):
        out_by[r[0]] = (r[1] or 0) / 1000.0
    days = [(week_monday + timedelta(days=i)).isoformat() for i in range(6)]
    combined = [(d, in_by.get(d, 0) + out_by.get(d, 0)) for d in days]
    active = [(d, v) for d, v in combined if v > 0]
    if active:
        peak = max(active, key=lambda x: x[1])
        trough = min(active, key=lambda x: x[1])
        crew_spread = peak[1] - trough[1]
        peak_lbl = (_WD_UZ[date.fromisoformat(peak[0]).weekday()], round(peak[1], 1))
        trough_lbl = (_WD_UZ[date.fromisoformat(trough[0]).weekday()], round(trough[1], 1))
    else:
        crew_spread = 0.0
        peak_lbl = trough_lbl = ("-", 0.0)

    # 2. revenue spread — peak ÷ median daily revenue (USD-eq)
    rev = [
        r[1] or 0
        for r in cur.execute(
            f"SELECT doc_date, SUM(total_sum/{FX_FALLBACK}+total_sum_currency) FROM real_orders "
            "WHERE COALESCE(is_approved,1)=1 AND doc_date BETWEEN ? AND ? GROUP BY doc_date", (a, b)
        )
    ]
    rev_med = statistics.median(rev) if rev else 0
    rev_peak = max(rev) if rev else 0
    rev_spread = (rev_peak / rev_med) if rev_med else 0.0

    # 3. truck fill % — avg delivered load ÷ capacity per (vehicle, day)
    veh_day = {}
    for r in cur.execute(
        "SELECT comment, doc_date, total_weight FROM real_orders "
        "WHERE COALESCE(is_approved,1)=1 AND doc_date BETWEEN ? AND ? "
        "AND LOWER(comment) LIKE '%dostavka%'", (a, b)
    ):
        v = _vehicle(r[0])
        if v:
            veh_day[(v, r[1])] = veh_day.get((v, r[1]), 0.0) + (r[2] or 0) / 1000.0
    fills = [min(load / TRUCK_CAP[v], 1.5) for (v, _d), load in veh_day.items() if v in TRUCK_CAP]
    truck_fill = (sum(fills) / len(fills) * 100) if fills else 0

    return {
        "week": (a, b),
        "crew_spread_t": round(crew_spread, 1),
        "crew_peak": peak_lbl,
        "crew_trough": trough_lbl,
        "rev_spread_x": round(rev_spread, 2),
        "rev_peak_k": round(rev_peak / 1000, 1),
        "rev_med_k": round(rev_med / 1000, 1),
        "truck_fill_pct": round(truck_fill),
    }


def compute_order_aging(conn, ref: date | None = None) -> dict:
    """Current X-backlog aging snapshot (recorded-but-not-shipped orders)."""
    ref = ref or date.today()
    cur = conn.cursor()
    # stale_expired_at IS NULL: exclude X orders auto-resolved as stale (abandoned
    # >7d, 1C never re-exported them as shipped). Without this the aging count +
    # max blow up — 49 phantom rows back to 2025-04 (Error Log: stale-X). See
    # x_queue.expire_stale_unshipped.
    ages = [
        (ref - date.fromisoformat(r[0])).days
        for r in cur.execute(
            "SELECT doc_date FROM real_orders "
            "WHERE COALESCE(is_approved,1)=0 AND stale_expired_at IS NULL")
        if r[0]
    ]
    if not ages:
        return {"avg": 0.0, "max": 0, "count": 0}
    return {"avg": round(sum(ages) / len(ages), 1), "max": max(ages), "count": len(ages)}


def _arrow(cur_v, prev_v) -> str:
    if prev_v is None or round(cur_v, 2) == round(prev_v, 2):
        return "→"
    return "↓" if cur_v < prev_v else "↑"


def format_scorecard(conn=None, ref: date | None = None) -> str:
    """Telegram-ready weekly scorecard (Uzbek), with last-week trend arrows."""
    own = conn is None
    if own:
        conn = get_db()
    try:
        mon = _last_full_week_monday(ref)
        c = compute_scorecard(conn, mon)
        p = compute_scorecard(conn, mon - timedelta(days=7))
        ag = compute_order_aging(conn, ref)
        a, b = c["week"]
        lines = [
            f"📊 <b>Haftalik ko'rsatkichlar</b> ({a[5:]} — {b[5:]})",
            "",
            f"⚖️ Brigada yuki tarqalishi: <b>{c['crew_spread_t']}t</b> "
            f"{_arrow(c['crew_spread_t'], p['crew_spread_t'])} (o'tgan {p['crew_spread_t']}t)",
            f"     band: {c['crew_peak'][0]} {c['crew_peak'][1]}t · "
            f"yengil: {c['crew_trough'][0]} {c['crew_trough'][1]}t",
            f"💰 Tushum tarqalishi: <b>{c['rev_spread_x']}×</b> "
            f"{_arrow(c['rev_spread_x'], p['rev_spread_x'])} (o'tgan {p['rev_spread_x']}×)",
            f"     peak ${c['rev_peak_k']}k · median ${c['rev_med_k']}k",
            f"🚚 Mashina to'ldirilishi: <b>{c['truck_fill_pct']}%</b> "
            f"{_arrow(c['truck_fill_pct'], p['truck_fill_pct'])} (o'tgan {p['truck_fill_pct']}%)",
            f"📦 Kutilayotgan buyurtma: o'rtacha <b>{ag['avg']}</b> kun · "
            f"eng uzun {ag['max']} kun ({ag['count']} ta)",
            "",
            "🎯 Maqsad: tarqalish kamaytirish · to'ldirishni oshirish",
        ]
        return "\n".join(lines)
    finally:
        if own:
            conn.close()
