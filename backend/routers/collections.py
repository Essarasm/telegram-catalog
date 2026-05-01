"""Collection Route Planner — dispatcher tool.

Lets ops point at where a truck is heading and see debtors on the way:

  POST /api/collections/candidates  — corridor match for one or more routes
  POST /api/collections/attempts    — log a phone-call outcome
  GET  /api/collections/attempts    — call history for a client
  POST /api/collections/route       — finalize → Google Maps URL + driver text

Corridor model: straight-line from origin → destination, with a
perpendicular buffer (default 5 km). A client is "on the way" if its
GPS pin is within `buffer_km` of the segment AND the foot of the
perpendicular falls between the endpoints (i.e. not past origin or
destination). Clients without GPS are included only when their tuman
matches a destination tuman explicitly chosen by the dispatcher.

For each truck (route) we run the same match; the response merges
debtors across routes, deduped, with `matched_routes: [0,1,...]`
indicating which corridor(s) the client lies on.
"""
import math
import os
from typing import Optional, List
from urllib.parse import quote_plus

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field

from backend.database import get_db
from backend.admin_auth import check_admin_key

router = APIRouter(prefix="/api/collections", tags=["collections"])


# ── Models ──────────────────────────────────────────────────

class CorridorRoute(BaseModel):
    origin_lat: float
    origin_lng: float
    dest_lat: float
    dest_lng: float
    dest_tuman_id: Optional[int] = None  # if set, also include all clients in this tuman
    buffer_km: float = 5.0
    label: Optional[str] = None  # dispatcher-chosen, e.g. "Truck 1"


class CandidatesRequest(BaseModel):
    routes: List[CorridorRoute] = Field(..., min_length=1)
    min_debt_uzs: float = 0.0
    min_debt_usd: float = 0.0
    exclude_aging_0_30: bool = False  # hide clients whose oldest debt is still in 0-30 window


class AttemptCreate(BaseModel):
    client_id: int
    outcome: str  # 'no_answer' | 'refused' | 'will_call_later' | 'agreed_full' | 'agreed_partial'
    dispatcher_name: Optional[str] = None
    dispatcher_tg_id: Optional[int] = None
    agreed_amount_uzs: Optional[float] = None
    agreed_amount_usd: Optional[float] = None
    notes: Optional[str] = None
    destination_tuman_id: Optional[int] = None
    destination_lat: Optional[float] = None
    destination_lng: Optional[float] = None


class RouteStop(BaseModel):
    lat: float
    lng: float
    label: str  # for the driver-facing message


class BuildRouteRequest(BaseModel):
    origin_lat: float
    origin_lng: float
    delivery_stops: List[RouteStop] = []
    collection_attempt_ids: List[int] = []  # rows in collection_attempts to mark included_in_route=1


_VALID_OUTCOMES = {
    "no_answer", "refused", "will_call_later",
    "agreed_full", "agreed_partial",
}


# ── Geometry helpers ─────────────────────────────────────────

_EARTH_R_KM = 6371.0


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return _EARTH_R_KM * c


def _point_to_segment_km(plat: float, plng: float,
                         lat1: float, lng1: float,
                         lat2: float, lng2: float) -> tuple:
    """Distance (km) from P to segment AB, plus parameter t in [0,1] for the
    perpendicular foot (clamped). Uses an equirectangular projection around
    the segment midpoint — accurate to ~0.5% for routes <200 km, which all
    real Rassvet trips are."""
    mid_lat = (lat1 + lat2) / 2
    deg_to_km_lat = 110.574  # km per degree latitude
    deg_to_km_lng = 111.320 * math.cos(math.radians(mid_lat))

    ax = lng1 * deg_to_km_lng
    ay = lat1 * deg_to_km_lat
    bx = lng2 * deg_to_km_lng
    by = lat2 * deg_to_km_lat
    px = plng * deg_to_km_lng
    py = plat * deg_to_km_lat

    abx = bx - ax
    aby = by - ay
    seg_len_sq = abx * abx + aby * aby
    if seg_len_sq < 1e-9:
        # Origin == destination; treat as point distance
        return _haversine_km(plat, plng, lat1, lng1), 0.0

    apx = px - ax
    apy = py - ay
    t = (apx * abx + apy * aby) / seg_len_sq
    t_clamped = max(0.0, min(1.0, t))

    foot_x = ax + t_clamped * abx
    foot_y = ay + t_clamped * aby
    dx = px - foot_x
    dy = py - foot_y
    return math.sqrt(dx * dx + dy * dy), t


# ── Aging helpers ────────────────────────────────────────────

_AGING_ORDER = ["120+", "91-120", "61-90", "31-60", "0-30"]
_AGING_DAYS = {"0-30": 30, "31-60": 60, "61-90": 90, "91-120": 120, "120+": 180}


def _oldest_aging_bucket(row) -> Optional[str]:
    """Return the most-aged non-zero bucket name for a client_debts row."""
    if (row["aging_120_plus"] or 0) > 0:
        return "120+"
    if (row["aging_91_120"] or 0) > 0:
        return "91-120"
    if (row["aging_61_90"] or 0) > 0:
        return "61-90"
    if (row["aging_31_60"] or 0) > 0:
        return "31-60"
    if (row["aging_0_30"] or 0) > 0:
        return "0-30"
    return None


# ── Helper: tuman list with computed centroids ───────────────

@router.get("/tumans")
def tumans_with_centroids(admin_key: str = Query(...)):
    """List of tumans + average GPS centroid (computed from pinned clients
    in that tuman) for the destination dropdown. Tumans with no pinned
    clients return centroid=NULL; the frontend then asks the dispatcher
    to click on the map instead.
    """
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    conn = get_db()
    rows = conn.execute(
        """
        SELECT l.id AS tuman_id,
               l.name AS name,
               vil.id AS viloyat_id,
               vil.name AS viloyat_name,
               AVG(ac.gps_latitude) AS lat,
               AVG(ac.gps_longitude) AS lng,
               COUNT(CASE WHEN ac.gps_latitude IS NOT NULL THEN 1 END) AS pin_count
        FROM locations l
        LEFT JOIN locations vil ON vil.id = l.parent_id
        LEFT JOIN allowed_clients ac ON ac.location_district_id = l.id
            AND COALESCE(ac.status, 'active') != 'merged'
        WHERE l.type = 'district' AND l.is_active = 1
        GROUP BY l.id
        ORDER BY vil.sort_order, l.sort_order, l.name
        """
    ).fetchall()
    conn.close()
    return {"tumans": [dict(r) for r in rows]}


# ── Candidates endpoint ──────────────────────────────────────

@router.post("/candidates")
def get_candidates(req: CandidatesRequest, admin_key: str = Query(...)):
    """Return debtor candidates whose location lies inside any route's
    corridor, OR who belong to a destination tuman explicitly listed.

    Sorted by `aging_days × (debt_uzs + debt_usd × 12500) / (min_distance_km + 1)`.
    The 12500 factor is a rough UZS-per-USD stand-in; the API also returns
    raw values so the frontend can re-sort if needed.
    """
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")

    conn = get_db()

    # Latest debt snapshot per client. The `client_debts` table keeps every
    # report; we want the most recent one per client.
    debts_rows = conn.execute(
        """
        SELECT cd.*
        FROM client_debts cd
        JOIN (
            SELECT client_id, MAX(report_date) AS rd
            FROM client_debts
            WHERE client_id IS NOT NULL
            GROUP BY client_id
        ) latest ON cd.client_id = latest.client_id AND cd.report_date = latest.rd
        WHERE (cd.debt_uzs > 0 OR cd.debt_usd > 0)
        """
    ).fetchall()

    if not debts_rows:
        conn.close()
        return {"routes": [r.model_dump() for r in req.routes], "candidates": [], "total": 0}

    # Clients
    debt_by_client = {r["client_id"]: r for r in debts_rows}
    client_ids = list(debt_by_client.keys())
    placeholders = ",".join("?" * len(client_ids))
    client_rows = conn.execute(
        f"""
        SELECT id, name, client_id_1c, company_name, phone_normalized,
               gps_latitude, gps_longitude, gps_address,
               viloyat, tuman, location_district_id
        FROM allowed_clients
        WHERE id IN ({placeholders})
          AND COALESCE(status, 'active') != 'merged'
        """,
        client_ids,
    ).fetchall()

    # Latest call attempt per client (for inline history display)
    last_attempts = conn.execute(
        f"""
        SELECT ca.* FROM collection_attempts ca
        JOIN (
            SELECT client_id, MAX(call_at) AS last_at
            FROM collection_attempts
            WHERE client_id IN ({placeholders})
            GROUP BY client_id
        ) lt ON ca.client_id = lt.client_id AND ca.call_at = lt.last_at
        """,
        client_ids,
    ).fetchall()
    last_attempt_by_client = {r["client_id"]: r for r in last_attempts}

    conn.close()

    # Apply currency thresholds + aging filter
    candidates = []
    dest_tuman_ids = {r.dest_tuman_id for r in req.routes if r.dest_tuman_id}

    for c in client_rows:
        debt = debt_by_client[c["id"]]
        d_uzs = float(debt["debt_uzs"] or 0)
        d_usd = float(debt["debt_usd"] or 0)
        # Include if EITHER currency has positive debt that meets its threshold.
        # Defaults (min=0/0) include any client with any positive debt.
        passes = (d_uzs > 0 and d_uzs >= req.min_debt_uzs) or (
            d_usd > 0 and d_usd >= req.min_debt_usd
        )
        if not passes:
            continue
        oldest = _oldest_aging_bucket(debt)
        if oldest is None:
            continue
        if req.exclude_aging_0_30 and oldest == "0-30":
            continue

        lat = c["gps_latitude"]
        lng = c["gps_longitude"]

        matched_routes = []
        nearest_distance = None
        nearest_t = None

        if lat is not None and lng is not None:
            for idx, route in enumerate(req.routes):
                dist_km, t = _point_to_segment_km(
                    lat, lng,
                    route.origin_lat, route.origin_lng,
                    route.dest_lat, route.dest_lng,
                )
                # On-segment test: t must be within [0,1]; otherwise the foot
                # is past origin or destination — debtor is "behind us" or
                # "past the destination", not on the way.
                if 0.0 <= t <= 1.0 and dist_km <= route.buffer_km:
                    matched_routes.append(idx)
                    if nearest_distance is None or dist_km < nearest_distance:
                        nearest_distance = dist_km
                        nearest_t = t

        # Tuman fallback (clients with no GPS, or GPS-pinned but in the dest tuman)
        in_tuman = False
        if c["location_district_id"] and c["location_district_id"] in dest_tuman_ids:
            in_tuman = True
        if in_tuman:
            for idx, route in enumerate(req.routes):
                if route.dest_tuman_id == c["location_district_id"] and idx not in matched_routes:
                    matched_routes.append(idx)

        if not matched_routes:
            continue

        candidates.append({
            "client_id": c["id"],
            "client_name": c["client_id_1c"] or c["company_name"] or c["name"] or f"#{c['id']}",
            "phone": c["phone_normalized"],
            "lat": lat,
            "lng": lng,
            "address": c["gps_address"],
            "viloyat": c["viloyat"],
            "tuman": c["tuman"],
            "tuman_id": c["location_district_id"],
            "debt_uzs": d_uzs,
            "debt_usd": d_usd,
            "oldest_aging": oldest,
            "last_transaction_date": debt["last_transaction_date"],
            "matched_routes": matched_routes,
            "distance_km": nearest_distance,
            "segment_t": nearest_t,
            "last_attempt": (
                {
                    "outcome": last_attempt_by_client[c["id"]]["outcome"],
                    "notes": last_attempt_by_client[c["id"]]["notes"],
                    "called_at": last_attempt_by_client[c["id"]]["call_at"],
                    "agreed_uzs": last_attempt_by_client[c["id"]]["agreed_amount_uzs"],
                    "agreed_usd": last_attempt_by_client[c["id"]]["agreed_amount_usd"],
                }
                if c["id"] in last_attempt_by_client else None
            ),
        })

    # Score & sort: aging_days × debt_total_normalised / (distance_km + 1)
    # Distance defaults to 0 for tuman-fallback (no GPS) so they still rank.
    UZS_PER_USD = 12500.0  # rough; only used for ranking, not display
    for cand in candidates:
        aging_days = _AGING_DAYS.get(cand["oldest_aging"], 30)
        debt_total = cand["debt_uzs"] + cand["debt_usd"] * UZS_PER_USD
        dist = cand["distance_km"] if cand["distance_km"] is not None else 0.0
        cand["_score"] = aging_days * debt_total / (dist + 1.0)
    candidates.sort(key=lambda x: -x["_score"])
    for cand in candidates:
        cand.pop("_score", None)

    return {
        "routes": [r.model_dump() for r in req.routes],
        "candidates": candidates,
        "total": len(candidates),
    }


# ── Attempts endpoints ───────────────────────────────────────

@router.post("/attempts")
def create_attempt(att: AttemptCreate, admin_key: str = Query(...)):
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    if att.outcome not in _VALID_OUTCOMES:
        raise HTTPException(status_code=400, detail=f"outcome must be one of {sorted(_VALID_OUTCOMES)}")
    if att.notes and len(att.notes) > 140:
        raise HTTPException(status_code=400, detail="notes must be ≤140 chars")

    conn = get_db()
    # Snapshot current debt + aging at call time
    debt = conn.execute(
        """SELECT cd.* FROM client_debts cd
           WHERE cd.client_id = ?
           ORDER BY cd.report_date DESC
           LIMIT 1""",
        (att.client_id,),
    ).fetchone()
    debt_uzs = float(debt["debt_uzs"] or 0) if debt else None
    debt_usd = float(debt["debt_usd"] or 0) if debt else None
    oldest = _oldest_aging_bucket(debt) if debt else None

    cur = conn.execute(
        """INSERT INTO collection_attempts
            (client_id, dispatcher_name, dispatcher_tg_id, outcome,
             agreed_amount_uzs, agreed_amount_usd, notes,
             debt_uzs_at_call, debt_usd_at_call, oldest_aging_at_call,
             destination_tuman_id, destination_lat, destination_lng)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (att.client_id, att.dispatcher_name, att.dispatcher_tg_id, att.outcome,
         att.agreed_amount_uzs, att.agreed_amount_usd, att.notes,
         debt_uzs, debt_usd, oldest,
         att.destination_tuman_id, att.destination_lat, att.destination_lng),
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"ok": True, "id": new_id}


@router.get("/attempts")
def list_attempts(client_id: int = Query(...), admin_key: str = Query(...)):
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    conn = get_db()
    rows = conn.execute(
        """SELECT id, client_id, dispatcher_name, dispatcher_tg_id, call_at,
                  outcome, agreed_amount_uzs, agreed_amount_usd, notes,
                  debt_uzs_at_call, debt_usd_at_call, oldest_aging_at_call,
                  included_in_route, actual_collected_uzs, actual_collected_usd
           FROM collection_attempts
           WHERE client_id = ?
           ORDER BY call_at DESC
           LIMIT 50""",
        (client_id,),
    ).fetchall()
    conn.close()
    return {"attempts": [dict(r) for r in rows]}


# ── Build-route endpoint ─────────────────────────────────────

@router.post("/route")
def build_route(req: BuildRouteRequest, admin_key: str = Query(...)):
    """Compose a Google Maps directions URL with every stop as a waypoint
    (origin → delivery stops → collection stops → back to origin), plus a
    Telegram-ready text block summarising each stop with phone + debt.
    Marks the listed collection_attempts as `included_in_route=1`.
    """
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")

    conn = get_db()

    # Gather collection-attempt details with client coords + debt snapshot
    coll_rows = []
    if req.collection_attempt_ids:
        ph = ",".join("?" * len(req.collection_attempt_ids))
        coll_rows = conn.execute(
            f"""SELECT ca.id AS attempt_id, ca.client_id, ca.outcome,
                       ca.agreed_amount_uzs, ca.agreed_amount_usd,
                       ca.debt_uzs_at_call, ca.debt_usd_at_call, ca.notes,
                       ac.gps_latitude AS lat, ac.gps_longitude AS lng,
                       ac.gps_address AS address,
                       ac.client_id_1c, ac.company_name, ac.name,
                       ac.phone_normalized
                FROM collection_attempts ca
                JOIN allowed_clients ac ON ac.id = ca.client_id
                WHERE ca.id IN ({ph})""",
            req.collection_attempt_ids,
        ).fetchall()

        conn.execute(
            f"UPDATE collection_attempts SET included_in_route = 1 WHERE id IN ({ph})",
            req.collection_attempt_ids,
        )
        conn.commit()
    conn.close()

    # Build waypoints: delivery first (already-confirmed orders), then collection
    waypoints = []
    for s in req.delivery_stops:
        waypoints.append((s.lat, s.lng, s.label))
    for r in coll_rows:
        if r["lat"] is not None and r["lng"] is not None:
            label = r["client_id_1c"] or r["company_name"] or r["name"] or f"client #{r['client_id']}"
            waypoints.append((r["lat"], r["lng"], f"{label} (debt)"))

    if not waypoints:
        raise HTTPException(status_code=400, detail="No stops to route")

    # Google Maps URL — origin + waypoints + destination=origin (round trip)
    origin_str = f"{req.origin_lat},{req.origin_lng}"
    wp_str = "|".join(f"{lat},{lng}" for lat, lng, _ in waypoints)
    gmaps_url = (
        "https://www.google.com/maps/dir/?api=1"
        f"&origin={quote_plus(origin_str)}"
        f"&waypoints={quote_plus(wp_str)}"
        f"&destination={quote_plus(origin_str)}"
        "&travelmode=driving"
    )

    # Telegram-ready driver brief
    lines = ["🚚 <b>Маршрут (Route)</b>", f"📍 Old: {origin_str} (склад)"]
    for i, (lat, lng, label) in enumerate(waypoints, start=1):
        lines.append(f"\n<b>{i}.</b> {label}")
        lines.append(f"   📍 {lat:.5f}, {lng:.5f}")
    # Per-stop debt detail for the collection rows
    for r in coll_rows:
        label = r["client_id_1c"] or r["company_name"] or r["name"] or f"client #{r['client_id']}"
        owe_uzs = r["debt_uzs_at_call"] or 0
        owe_usd = r["debt_usd_at_call"] or 0
        agreed_uzs = r["agreed_amount_uzs"] or 0
        agreed_usd = r["agreed_amount_usd"] or 0
        phone = r["phone_normalized"] or "—"
        owe_parts = []
        if owe_uzs > 0:
            owe_parts.append(f"{int(owe_uzs):,} UZS")
        if owe_usd > 0:
            owe_parts.append(f"${owe_usd:,.2f}")
        agreed_parts = []
        if agreed_uzs > 0:
            agreed_parts.append(f"{int(agreed_uzs):,} UZS")
        if agreed_usd > 0:
            agreed_parts.append(f"${agreed_usd:,.2f}")
        lines.append(
            f"\n💰 <b>{label}</b>: owes {' + '.join(owe_parts) or '—'}"
            + (f"; agreed {' + '.join(agreed_parts)}" if agreed_parts else "")
            + f"\n   ☎ {phone}"
            + (f"\n   📝 {r['notes']}" if r['notes'] else "")
        )
    telegram_text = "\n".join(lines)

    return {
        "google_maps_url": gmaps_url,
        "telegram_message": telegram_text,
        "waypoint_count": len(waypoints),
    }
