# -*- coding: utf-8 -*-
"""X-queue — today's delivery manifest from recorded-but-not-shipped orders.

The lean daily-execution layer beneath the weekly ops scorecard. In the daily
`realorders` 1C export the first column marks each doc V (shipped) or X
(recorded, not yet shipped). X rows = `real_orders.is_approved = 0` = the live
delivery queue. This groups the current X backlog by zone and suggests a truck
so the dispatcher sees what must go out, by direction. Read-only — no scoring,
no multi-day planning (that's a deferred v2). See the operational-resource-
balancing skill.
"""
from __future__ import annotations

from backend.database import get_db

# smallest-first so the suggestion picks the smallest truck that fits
TRUCK_CAP = [("Labo", 1.5), ("Jac", 2.5), ("Foton", 3.0), ("Isuzu", 7.0)]
_BIGGEST = TRUCK_CAP[-1]

# rows that are not real delivery clients (cash/warehouse/returns/org buckets)
_PSEUDO = ("наличк", "склад", "возврат", "организаци", "прочие")


def _suggest_truck(tonnes: float) -> str:
    for name, cap in TRUCK_CAP:
        if tonnes <= cap:
            return name
    trips = int(tonnes // _BIGGEST[1]) + 1
    return f"{_BIGGEST[0]} ({trips} reys)"  # exceeds one truck → N trips


def _is_city(zone: str, viloyat: str) -> bool:
    blob = f"{zone} {viloyat}".lower()
    return "samarqand sh" in blob or "samarkand" in blob


def compute_x_queue(conn) -> dict:
    """Group the current X backlog by zone. Returns city/districts/unlocated buckets."""
    cur = conn.cursor()
    rows = cur.execute(
        # CAST: real_orders.total_weight is sometimes stored as TEXT (SQLite dynamic
        # typing) — row-level read needs explicit coercion (SUM would coerce, this won't)
        "SELECT ro.client_name_1c, CAST(COALESCE(ro.total_weight, 0) AS REAL), "
        "       ac.tuman, ac.gps_district, ac.viloyat, "
        "       CASE WHEN ac.gps_latitude IS NOT NULL THEN 1 ELSE 0 END AS has_pin "
        "FROM real_orders ro LEFT JOIN allowed_clients ac ON ac.id = ro.client_id "
        "WHERE COALESCE(ro.is_approved, 1) = 0"
    ).fetchall()

    city = {"tonnes": 0.0, "orders": 0, "clients": set(), "no_pin": 0}
    districts: dict[str, dict] = {}
    unlocated = {"tonnes": 0.0, "orders": 0, "clients": set()}
    total_t = 0.0
    total_orders = 0
    orders: list[dict] = []  # one entry per X doc — flat per-order list for /navbat

    for row in rows:
        # NB: get_db()'s _DictRow iterates KEYS, not values — positional unpack
        # (`for a, b, ... in rows`) would bind COLUMN NAMES, not data (Error Log
        # #98). Access by index instead.
        name, wt, tuman, gdist, viloyat, has_pin = (
            row[0], row[1], row[2], row[3], row[4], row[5])
        if name and any(k in name.lower() for k in _PSEUDO):
            continue
        # get_db() can return numeric columns as str (SQLite text affinity through
        # this connection) — coerce in Python, don't trust SQL CAST/typeof here.
        try:
            t = (float(wt) if wt not in (None, "") else 0.0) / 1000.0
        except (TypeError, ValueError):
            t = 0.0
        pinned = str(has_pin) in ("1", "1.0")
        total_t += t
        total_orders += 1
        orders.append({"name": name, "tonnes": t})
        zone = (tuman or gdist or viloyat or "").strip()
        if not zone:
            unlocated["tonnes"] += t
            unlocated["orders"] += 1
            unlocated["clients"].add(name)
        elif _is_city(zone, viloyat or ""):
            city["tonnes"] += t
            city["orders"] += 1
            city["clients"].add(name)
            if not pinned:
                city["no_pin"] += 1
        else:
            z = districts.setdefault(
                zone, {"tonnes": 0.0, "orders": 0, "clients": set(), "no_pin": 0}
            )
            z["tonnes"] += t
            z["orders"] += 1
            z["clients"].add(name)
            if not pinned:
                z["no_pin"] += 1

    return {
        "total_t": round(total_t, 1),
        "total_orders": total_orders,
        "city": city,
        "districts": districts,
        "unlocated": unlocated,
        "orders": sorted(orders, key=lambda o: -o["tonnes"]),  # heaviest first
    }


def format_x_queue(conn=None) -> str:
    """Telegram-ready daily delivery queue (Uzbek)."""
    own = conn is None
    if own:
        conn = get_db()
    try:
        q = compute_x_queue(conn)
        if q["total_orders"] == 0:
            return "📦 <b>Bugungi navbat</b>\n\nHali joʻnatilmagan buyurtma yoʻq (X belgisi)."

        lines = [
            f"📦 <b>Bugungi navbat</b> — {q['total_t']}t / {q['total_orders']} buyurtma "
            f"(X belgisi)",
            "",
        ]
        for o in q["orders"]:
            name = o["name"] or "(nomsiz)"
            lines.append(f"   • {name} — {round(o['tonnes'], 1)}t")
        return "\n".join(lines)
    finally:
        if own:
            conn.close()
