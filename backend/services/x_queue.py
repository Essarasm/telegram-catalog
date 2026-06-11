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
        "SELECT ro.client_name_1c, COALESCE(ro.total_weight, 0), "
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

    for name, wt, tuman, gdist, viloyat, has_pin in rows:
        if name and any(k in name.lower() for k in _PSEUDO):
            continue
        t = (wt or 0) / 1000.0
        total_t += t
        total_orders += 1
        zone = (tuman or gdist or viloyat or "").strip()
        if not zone:
            unlocated["tonnes"] += t
            unlocated["orders"] += 1
            unlocated["clients"].add(name)
        elif _is_city(zone, viloyat or ""):
            city["tonnes"] += t
            city["orders"] += 1
            city["clients"].add(name)
            if not has_pin:
                city["no_pin"] += 1
        else:
            z = districts.setdefault(
                zone, {"tonnes": 0.0, "orders": 0, "clients": set(), "no_pin": 0}
            )
            z["tonnes"] += t
            z["orders"] += 1
            z["clients"].add(name)
            if not has_pin:
                z["no_pin"] += 1

    return {
        "total_t": round(total_t, 1),
        "total_orders": total_orders,
        "city": city,
        "districts": districts,
        "unlocated": unlocated,
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

        n_zones = len(q["districts"]) + (1 if q["city"]["orders"] else 0)
        lines = [
            f"📦 <b>Bugungi navbat</b> — {q['total_t']}t / {q['total_orders']} buyurtma / "
            f"{n_zones} zona (X belgisi)",
        ]

        c = q["city"]
        if c["orders"]:
            tag = f"  ⚠ {c['no_pin']} joylashuvsiz" if c["no_pin"] else ""
            lines += [
                "",
                "🏙 <b>Shahar</b> (Shuxrat):",
                f"   • Samarqand {round(c['tonnes'], 1)}t → {_suggest_truck(c['tonnes'])} "
                f"({len(c['clients'])} mijoz){tag}",
            ]

        if q["districts"]:
            lines += ["", "🗺 <b>Tumanlar</b> (Alisher/Ibrat):"]
            for zone, z in sorted(q["districts"].items(), key=lambda kv: -kv[1]["tonnes"]):
                tag = f"  ⚠ {z['no_pin']} joylashuvsiz" if z["no_pin"] else ""
                lines.append(
                    f"   • {zone} {round(z['tonnes'], 1)}t → {_suggest_truck(z['tonnes'])} "
                    f"({len(z['clients'])} mijoz){tag}"
                )

        u = q["unlocated"]
        if u["orders"]:
            lines += [
                "",
                f"❓ <b>Joylashuvsiz</b>: {round(u['tonnes'], 1)}t / {len(u['clients'])} mijoz "
                f"— pin kerak (/lokatsiya)",
            ]
        return "\n".join(lines)
    finally:
        if own:
            conn.close()
