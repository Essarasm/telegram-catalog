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
from typing import Optional, List
from urllib.parse import quote_plus

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field

from backend.database import get_db
from backend.admin_auth import check_admin_key, resolve_auth
from backend.services.client_search import (
    CLIENT_FUZZY_MIN_SCORE,
    _best_trigram,
    _query_variants,
)

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
            AND COALESCE(ac.status, 'active') NOT LIKE 'merged%'
        WHERE l.type = 'district' AND l.is_active = 1
        GROUP BY l.id
        ORDER BY vil.sort_order, l.sort_order, l.name
        """
    ).fetchall()
    conn.close()
    return {"tumans": [dict(r) for r in rows]}


# ── Debt-by-client (Agent Coverage map overlay) ──────────────

@router.get("/debt-by-client")
def get_debt_by_client(admin_key: str = Query(...)):
    """Lightweight debt map keyed by `allowed_clients.id`.

    Returns one entry per client with positive debt in the latest
    `client_debts` snapshot, including the most-aged non-zero bucket.
    Used by the Agent Coverage map to color GPS pins by debt tier.
    """
    auth = resolve_auth(admin_key)
    if not auth or auth["role"] not in ("admin", "agent"):
        raise HTTPException(status_code=403, detail="Invalid admin key")

    conn = get_db()
    rows = conn.execute(
        """
        SELECT cd.client_id, cd.debt_uzs, cd.debt_usd, cd.report_date,
               cd.aging_0_30, cd.aging_31_60, cd.aging_61_90,
               cd.aging_91_120, cd.aging_120_plus
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
    conn.close()

    by_client: dict = {}
    snapshot_dates: set = set()
    for r in rows:
        oldest = _oldest_aging_bucket(r)
        if oldest is None:
            continue
        by_client[r["client_id"]] = {
            "debt_uzs": float(r["debt_uzs"] or 0),
            "debt_usd": float(r["debt_usd"] or 0),
            "oldest_aging": oldest,
        }
        if r["report_date"]:
            snapshot_dates.add(r["report_date"])

    return {
        "by_client": by_client,
        "snapshot_date": max(snapshot_dates) if snapshot_dates else None,
        "total": len(by_client),
    }


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
          AND COALESCE(status, 'active') NOT LIKE 'merged%'
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


# ── Session M Phase 3: Auto-planned delivery + return-leg route ──
#
# A complete planning flow:
#   GET  /drivers                       — agents available as drivers
#   POST /route/plan                    — preview a route (NN-ordered delivery +
#                                          greedy-picked return-leg debtors).
#                                          No DB writes.
#   POST /route/save                    — persist + optionally dispatch
#   GET  /routes                        — recent saved routes
#   GET  /routes/{route_id}             — single route detail
#
# Algorithm:
#   • Delivery sequence = nearest-neighbor from warehouse on haversine.
#     ~80–90% of optimal for ≤8 stops; no external API.
#   • Return-leg candidates = debtors within `return_buffer_km` of the
#     last_delivery → warehouse segment AND whose perpendicular foot lies
#     on the segment (t ∈ [0,1]). Detour cost = round-trip perp_dist ÷
#     city_speed; greedy-fill by (debt × aging_days / detour_min) while
#     sum ≤ return_time_budget_min.

# Hardcoded truck catalogue. Free-text `users.vehicle` may contain other
# values ("Жигули", "boshqa"); we accept those as truck_type=="other".
TRUCK_CATALOGUE = {
    "labo": ("Labo", 1.0),
    "jac": ("Jac", 2.5),
    "foton": ("Foton", 3.0),
    "isuzu": ("Isuzu", 7.0),
}

CITY_SPEED_KMH_DEFAULT = 25.0  # rough Tashkent/Samarkand mixed-city avg
MAX_DELIVERY_STOPS = 8


class PlanRouteRequest(BaseModel):
    truck_type: str                                  # 'labo' | 'jac' | 'foton' | 'isuzu' | 'other'
    driver_tg_id: int
    driver_name: Optional[str] = None
    origin_lat: float
    origin_lng: float
    delivery_client_ids: List[int] = Field(default_factory=list, max_length=MAX_DELIVERY_STOPS)
    return_buffer_km: float = 5.0
    return_time_budget_min: float = 30.0
    city_speed_kmh: float = CITY_SPEED_KMH_DEFAULT
    # If non-empty, dispatcher has explicitly chosen these debtors for the
    # return leg (overrides greedy pick). Order is ignored — return-leg
    # sequence is computed by NN from the last delivery stop.
    return_client_ids: Optional[List[int]] = None
    dispatcher_name: Optional[str] = None
    dispatcher_tg_id: Optional[int] = None


class SaveRouteRequest(PlanRouteRequest):
    dispatch: bool = False  # if True, also DM the driver via bot


def _nearest_neighbor(stops: list, start_lat: float, start_lng: float) -> list:
    """Greedy NN ordering. Mutates each chosen stop with `leg_distance_km`
    (distance from the previous point — warehouse for stop #1).
    Returns the stops in visit order. Empty input → []."""
    remaining = list(stops)
    cur_lat, cur_lng = start_lat, start_lng
    ordered = []
    while remaining:
        best_i, best_d = 0, None
        for i, s in enumerate(remaining):
            d = _haversine_km(cur_lat, cur_lng, s["lat"], s["lng"])
            if best_d is None or d < best_d:
                best_i, best_d = i, d
        chosen = remaining.pop(best_i)
        chosen["leg_distance_km"] = best_d
        ordered.append(chosen)
        cur_lat, cur_lng = chosen["lat"], chosen["lng"]
    return ordered


def _fetch_clients_by_ids(conn, ids: list) -> dict:
    """Bulk lookup of `allowed_clients` rows by id, with the fields we need
    for route planning. Returns {id: dict}."""
    if not ids:
        return {}
    ph = ",".join("?" * len(ids))
    rows = conn.execute(
        f"""SELECT id, name, client_id_1c, company_name, phone_normalized,
                   gps_latitude, gps_longitude, gps_address,
                   viloyat, tuman
            FROM allowed_clients
            WHERE id IN ({ph})
              AND COALESCE(status, 'active') NOT LIKE 'merged%'""",
        ids,
    ).fetchall()
    return {r["id"]: r for r in rows}


def _latest_debt_for(conn, client_ids: list) -> dict:
    """Most recent `client_debts` row per client_id. Returns {id: row}."""
    if not client_ids:
        return {}
    ph = ",".join("?" * len(client_ids))
    rows = conn.execute(
        f"""SELECT cd.*
            FROM client_debts cd
            JOIN (
                SELECT client_id, MAX(report_date) AS rd
                FROM client_debts
                WHERE client_id IN ({ph})
                GROUP BY client_id
            ) latest ON cd.client_id = latest.client_id AND cd.report_date = latest.rd""",
        client_ids,
    ).fetchall()
    return {r["client_id"]: r for r in rows}


def _yandex_route_url(origin: tuple, stops: list, return_to_origin: bool = True) -> str:
    """Build a Yandex Maps directions URL.

    `stops` is a list of (lat, lng) tuples. Yandex's URL scheme:
      https://yandex.com/maps/?rtext=lat1,lng1~lat2,lng2~...&rtt=auto
    `rtt=auto` = driving. We end at the origin (round trip) when
    `return_to_origin` is True so the driver gets a closed loop.
    """
    pts = [origin] + list(stops)
    if return_to_origin:
        pts.append(origin)
    rtext = "~".join(f"{lat:.6f},{lng:.6f}" for lat, lng in pts)
    return f"https://yandex.com/maps/?rtext={rtext}&rtt=auto"


def _truck_label(truck_type: str, capacity_t: Optional[float]) -> str:
    name = TRUCK_CATALOGUE.get(truck_type, (truck_type.title(), None))[0]
    if capacity_t:
        return f"{name} ({capacity_t:g}т)"
    return name


def _plan_route_payload(conn, req: PlanRouteRequest) -> dict:
    """Shared core for /route/plan and /route/save. Returns the planned-route
    payload (sequence + url + brief + totals). No DB writes."""
    truck_type = (req.truck_type or "").lower()
    if truck_type not in TRUCK_CATALOGUE and truck_type != "other":
        raise HTTPException(status_code=400, detail=f"truck_type must be one of {sorted(TRUCK_CATALOGUE)} or 'other'")
    capacity = TRUCK_CATALOGUE.get(truck_type, (None, None))[1]

    # Driver lookup (and a sane fallback for driver_name)
    driver_row = conn.execute(
        """SELECT telegram_id, first_name, last_name, username, phone,
                  vehicle, vehicle_capacity_tons, agent_role, is_agent
           FROM users WHERE telegram_id = ?""",
        (req.driver_tg_id,),
    ).fetchone()
    if not driver_row:
        raise HTTPException(status_code=400, detail=f"driver {req.driver_tg_id} not found in users")
    # Allow any user for now (some drivers may not be flagged is_agent); the
    # frontend dropdown restricts to agents, but we don't enforce here.
    driver_name = req.driver_name or (
        (driver_row["first_name"] or "") + (f" {driver_row['last_name']}" if driver_row["last_name"] else "")
    ).strip() or driver_row["username"] or f"#{driver_row['telegram_id']}"

    # Delivery stops — must all have GPS
    deliveries = []
    if req.delivery_client_ids:
        client_map = _fetch_clients_by_ids(conn, req.delivery_client_ids)
        missing = [cid for cid in req.delivery_client_ids if cid not in client_map]
        if missing:
            raise HTTPException(status_code=400, detail=f"client(s) not found: {missing}")
        for cid in req.delivery_client_ids:
            c = client_map[cid]
            if c["gps_latitude"] is None or c["gps_longitude"] is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"client {cid} ({c['client_id_1c'] or c['name']}) has no GPS — drop or pin first",
                )
            deliveries.append({
                "client_id": c["id"],
                "client_id_1c": c["client_id_1c"],
                "client_display_name": c["client_id_1c"] or c["company_name"] or c["name"] or f"#{c['id']}",
                "phone_normalized": c["phone_normalized"],
                "lat": float(c["gps_latitude"]),
                "lng": float(c["gps_longitude"]),
                "address": c["gps_address"],
                "kind": "delivery",
            })

    # Order deliveries by nearest-neighbor from warehouse
    ordered_deliveries = _nearest_neighbor(deliveries, req.origin_lat, req.origin_lng)

    # Return-leg pivot = last delivery stop, or warehouse itself if no deliveries
    if ordered_deliveries:
        last_lat = ordered_deliveries[-1]["lat"]
        last_lng = ordered_deliveries[-1]["lng"]
    else:
        last_lat, last_lng = req.origin_lat, req.origin_lng

    direct_return_km = _haversine_km(last_lat, last_lng, req.origin_lat, req.origin_lng)

    # Return-leg debtor candidates
    # If `return_client_ids` is explicitly provided, use those; else, search
    # the buffer + greedy-fill by score until time budget exhausted.
    UZS_PER_USD = 12500.0
    candidates_raw = []

    if req.return_client_ids is not None:
        # Explicit list — fetch + compute detour, no budget filter
        cmap = _fetch_clients_by_ids(conn, req.return_client_ids)
        debts = _latest_debt_for(conn, req.return_client_ids)
        for cid in req.return_client_ids:
            c = cmap.get(cid)
            if not c or c["gps_latitude"] is None or c["gps_longitude"] is None:
                continue
            dist_km, t = _point_to_segment_km(
                float(c["gps_latitude"]), float(c["gps_longitude"]),
                last_lat, last_lng, req.origin_lat, req.origin_lng,
            )
            d = debts.get(cid)
            d_uzs = float(d["debt_uzs"] or 0) if d else 0.0
            d_usd = float(d["debt_usd"] or 0) if d else 0.0
            aging = _oldest_aging_bucket(d) if d else None
            detour_min = (2.0 * dist_km / max(req.city_speed_kmh, 1.0)) * 60.0
            candidates_raw.append({
                "client": c, "debt_uzs": d_uzs, "debt_usd": d_usd,
                "aging": aging, "perp_km": dist_km, "detour_min": detour_min,
                "client_id": cid,
            })
    else:
        # Buffer search across all positive-debt clients
        all_debts_rows = conn.execute(
            """SELECT cd.*
               FROM client_debts cd
               JOIN (
                   SELECT client_id, MAX(report_date) AS rd
                   FROM client_debts
                   WHERE client_id IS NOT NULL
                   GROUP BY client_id
               ) latest ON cd.client_id = latest.client_id AND cd.report_date = latest.rd
               WHERE (cd.debt_uzs > 0 OR cd.debt_usd > 0)"""
        ).fetchall()
        debt_by_client = {r["client_id"]: r for r in all_debts_rows}
        if debt_by_client:
            ph = ",".join("?" * len(debt_by_client))
            client_rows = conn.execute(
                f"""SELECT id, name, client_id_1c, company_name, phone_normalized,
                           gps_latitude, gps_longitude, gps_address, viloyat, tuman
                    FROM allowed_clients
                    WHERE id IN ({ph})
                      AND gps_latitude IS NOT NULL AND gps_longitude IS NOT NULL
                      AND COALESCE(status, 'active') NOT LIKE 'merged%'""",
                list(debt_by_client.keys()),
            ).fetchall()
            # Exclude the delivery clients from return-leg candidates (already on the route)
            already_on_route = {s["client_id"] for s in ordered_deliveries}
            for c in client_rows:
                if c["id"] in already_on_route:
                    continue
                debt = debt_by_client[c["id"]]
                dist_km, t = _point_to_segment_km(
                    float(c["gps_latitude"]), float(c["gps_longitude"]),
                    last_lat, last_lng, req.origin_lat, req.origin_lng,
                )
                if not (0.0 <= t <= 1.0 and dist_km <= req.return_buffer_km):
                    continue
                aging = _oldest_aging_bucket(debt)
                if aging is None:
                    continue
                d_uzs = float(debt["debt_uzs"] or 0)
                d_usd = float(debt["debt_usd"] or 0)
                detour_min = (2.0 * dist_km / max(req.city_speed_kmh, 1.0)) * 60.0
                candidates_raw.append({
                    "client": c, "debt_uzs": d_uzs, "debt_usd": d_usd,
                    "aging": aging, "perp_km": dist_km, "detour_min": detour_min,
                    "client_id": c["id"],
                })

    # Score + greedy budget fill (skipped when explicit list given)
    for cand in candidates_raw:
        aging_days = _AGING_DAYS.get(cand["aging"], 30) if cand["aging"] else 30
        debt_total = cand["debt_uzs"] + cand["debt_usd"] * UZS_PER_USD
        cand["_score"] = (aging_days * debt_total) / max(cand["detour_min"], 1.0)
    candidates_raw.sort(key=lambda x: -x["_score"])

    picked = []
    if req.return_client_ids is not None:
        picked = candidates_raw  # all explicit picks, no budget filter
    else:
        budget = req.return_time_budget_min
        used = 0.0
        for cand in candidates_raw:
            if used + cand["detour_min"] <= budget:
                picked.append(cand)
                used += cand["detour_min"]

    # NN-order the picked collection stops on the return segment
    coll_stops_unordered = []
    for cand in picked:
        c = cand["client"]
        coll_stops_unordered.append({
            "client_id": c["id"],
            "client_id_1c": c["client_id_1c"],
            "client_display_name": c["client_id_1c"] or c["company_name"] or c["name"] or f"#{c['id']}",
            "phone_normalized": c["phone_normalized"],
            "lat": float(c["gps_latitude"]),
            "lng": float(c["gps_longitude"]),
            "address": c["gps_address"],
            "debt_uzs": cand["debt_uzs"],
            "debt_usd": cand["debt_usd"],
            "oldest_aging_bucket": cand["aging"],
            "detour_minutes": cand["detour_min"],
            "perp_km": cand["perp_km"],
            "kind": "collection",
        })
    ordered_collections = _nearest_neighbor(coll_stops_unordered, last_lat, last_lng)

    # Compose full sequence (origin → deliveries → collections → return)
    all_stops = ordered_deliveries + ordered_collections
    waypoints = [(s["lat"], s["lng"]) for s in all_stops]

    # Distance + time totals (haversine; rough)
    total_km = sum(s["leg_distance_km"] for s in all_stops)
    # Plus return leg from last collection (or last delivery if no collections) to warehouse
    if all_stops:
        total_km += _haversine_km(all_stops[-1]["lat"], all_stops[-1]["lng"],
                                  req.origin_lat, req.origin_lng)
    estimated_min = (total_km / max(req.city_speed_kmh, 1.0)) * 60.0

    maps_url = _yandex_route_url((req.origin_lat, req.origin_lng), waypoints, return_to_origin=True)

    # Driver brief (Telegram HTML)
    truck_label = _truck_label(truck_type, capacity)
    lines = [
        f"🚚 <b>Маршрут — {truck_label}</b>",
        f"🧑‍💼 Driver: {driver_name}",
        f"📦 {len(ordered_deliveries)} delivery, {len(ordered_collections)} collection",
        f"⏱ ~{int(round(estimated_min))} min · {total_km:.1f} km",
        f"📍 Old: склад ({req.origin_lat:.5f}, {req.origin_lng:.5f})",
    ]
    step = 1
    for s in ordered_deliveries:
        lines.append("")
        lines.append(f"<b>{step}. {s['client_display_name']}</b> — delivery")
        lines.append(f"   📍 {s['lat']:.5f}, {s['lng']:.5f}")
        if s["phone_normalized"]:
            lines.append(f"   ☎ {s['phone_normalized']}")
        if s["address"]:
            lines.append(f"   🏠 {s['address']}")
        step += 1
    for s in ordered_collections:
        lines.append("")
        owe_parts = []
        if s["debt_uzs"] > 0:
            owe_parts.append(f"{int(s['debt_uzs']):,} UZS")
        if s["debt_usd"] > 0:
            owe_parts.append(f"${s['debt_usd']:,.2f}")
        lines.append(f"<b>{step}. {s['client_display_name']}</b> — collection")
        lines.append(f"   💰 owes {' + '.join(owe_parts) or '—'} ({s['oldest_aging_bucket']})")
        lines.append(f"   📍 {s['lat']:.5f}, {s['lng']:.5f} (+{s['detour_minutes']:.0f} min detour)")
        if s["phone_normalized"]:
            lines.append(f"   ☎ {s['phone_normalized']}")
        step += 1
    lines.append("")
    lines.append("📍 Назад: склад")
    driver_brief = "\n".join(lines)

    return {
        "truck_type": truck_type,
        "truck_label": truck_label,
        "truck_capacity_t": capacity,
        "driver_tg_id": req.driver_tg_id,
        "driver_name": driver_name,
        "origin": {"lat": req.origin_lat, "lng": req.origin_lng},
        "return_buffer_km": req.return_buffer_km,
        "return_time_budget_min": req.return_time_budget_min,
        "city_speed_kmh": req.city_speed_kmh,
        "delivery_stops": ordered_deliveries,
        "collection_stops": ordered_collections,
        "total_distance_km": round(total_km, 2),
        "estimated_minutes": round(estimated_min, 1),
        "direct_return_km": round(direct_return_km, 2),
        "maps_url": maps_url,
        "driver_brief": driver_brief,
    }


@router.get("/clients/search")
def search_clients_for_route(q: str = Query(..., min_length=1),
                             limit: int = 15,
                             admin_key: str = Query(...)):
    """Tiny client search for the delivery-stop picker. Matches against
    `client_id_1c`, `name`, `company_name`, `phone_normalized`. Returns
    GPS-status so the UI can warn before the dispatcher adds a pinless
    client (which the /route/plan endpoint will reject anyway)."""
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    q_clean = q.strip()
    needle = f"%{q_clean}%"
    capped_limit = max(1, min(limit, 50))
    conn = get_db()
    rows = conn.execute(
        """SELECT id, client_id_1c, name, company_name, phone_normalized,
                  gps_latitude, gps_longitude, gps_address, tuman, viloyat
           FROM allowed_clients
           WHERE COALESCE(status,'active') NOT LIKE 'merged%'
             AND (client_id_1c LIKE ? OR name LIKE ?
                  OR company_name LIKE ? OR phone_normalized LIKE ?)
           ORDER BY (gps_latitude IS NULL),   -- pinned first
                    client_id_1c
           LIMIT ?""",
        (needle, needle, needle, needle, capped_limit),
    ).fetchall()

    def to_item(r, match_type, similarity=None):
        item = {
            "id": r["id"],
            "client_id_1c": r["client_id_1c"],
            "display_name": r["client_id_1c"] or r["company_name"] or r["name"] or f"#{r['id']}",
            "phone": r["phone_normalized"],
            "has_gps": r["gps_latitude"] is not None and r["gps_longitude"] is not None,
            "lat": r["gps_latitude"], "lng": r["gps_longitude"],
            "address": r["gps_address"],
            "tuman": r["tuman"], "viloyat": r["viloyat"],
            "match_type": match_type,
        }
        if similarity is not None:
            item["similarity"] = round(similarity, 3)
        return item

    results = [to_item(r, "exact") for r in rows]

    fuzzy_count = 0
    # Skip fuzzy on digit-only or very short queries — trigram is meaningless
    # on phone fragments / numeric IDs, and short queries collide with too
    # many unrelated names.
    need = capped_limit - len(results)
    if need > 0 and len(q_clean) >= 3 and not q_clean.isdigit():
        variants = _query_variants(q_clean.lower())
        exclude_ids = {r["id"] for r in rows}
        candidates = conn.execute(
            """SELECT id, client_id_1c, name, company_name, phone_normalized,
                      gps_latitude, gps_longitude, gps_address, tuman, viloyat
               FROM allowed_clients
               WHERE COALESCE(status,'active') NOT LIKE 'merged%'"""
        ).fetchall()
        scored = []
        for r in candidates:
            if r["id"] in exclude_ids:
                continue
            sim = _best_trigram(
                variants,
                r["name"] or "",
                r["client_id_1c"] or "",
                r["company_name"] or "",
            )
            if sim >= CLIENT_FUZZY_MIN_SCORE:
                scored.append((sim, r))
        scored.sort(key=lambda x: (-x[0], 0 if x[1]["gps_latitude"] is not None else 1))
        for sim, r in scored[:need]:
            results.append(to_item(r, "fuzzy", similarity=sim))
            fuzzy_count += 1

    conn.close()
    return {"results": results, "fuzzy_count": fuzzy_count}


@router.get("/drivers")
def list_drivers(admin_key: str = Query(...)):
    """Drivers picker = `users.agent_role='agent'`. Returns each agent's
    name, telegram_id, phone, declared vehicle + capacity (so the frontend
    can auto-fill the truck dropdown when a driver is picked)."""
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    conn = get_db()
    rows = conn.execute(
        """SELECT telegram_id, first_name, last_name, username, phone,
                  vehicle, vehicle_capacity_tons
           FROM users
           WHERE agent_role = 'agent' OR is_agent = 1
           ORDER BY COALESCE(first_name, username, '')"""
    ).fetchall()
    conn.close()
    drivers = []
    for r in rows:
        name = ((r["first_name"] or "") + (f" {r['last_name']}" if r["last_name"] else "")).strip() \
            or r["username"] or f"#{r['telegram_id']}"
        drivers.append({
            "telegram_id": r["telegram_id"],
            "name": name,
            "phone": r["phone"],
            "vehicle": r["vehicle"],
            "vehicle_capacity_tons": r["vehicle_capacity_tons"],
        })
    return {"drivers": drivers, "trucks": [
        {"key": k, "label": v[0], "capacity_t": v[1]} for k, v in TRUCK_CATALOGUE.items()
    ]}


@router.post("/route/plan")
def plan_route(req: PlanRouteRequest, admin_key: str = Query(...)):
    """Preview a planned route. No DB writes — pure compute. Frontend
    shows the sequenced stops on the map; dispatcher tweaks return params
    or removes/adds collection stops; then calls /route/save to persist."""
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    conn = get_db()
    try:
        return _plan_route_payload(conn, req)
    finally:
        conn.close()


@router.post("/route/save")
def save_route(req: SaveRouteRequest, admin_key: str = Query(...)):
    """Persist a route to `delivery_routes` + `route_stops`. If
    `dispatch=true`, also DM the driver the Yandex URL + brief via the bot.
    Returns the saved `route_id` plus the same payload as `/route/plan`."""
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")

    conn = get_db()
    try:
        payload = _plan_route_payload(conn, req)

        cur = conn.execute(
            """INSERT INTO delivery_routes
                (created_by_tg_id, created_by_name, truck_type, truck_capacity_t,
                 driver_tg_id, driver_name, status, origin_lat, origin_lng,
                 return_buffer_km, return_time_budget_min,
                 total_distance_km, estimated_minutes, maps_url, driver_brief)
               VALUES (?, ?, ?, ?, ?, ?, 'planned', ?, ?, ?, ?, ?, ?, ?, ?)""",
            (req.dispatcher_tg_id, req.dispatcher_name,
             payload["truck_type"], payload["truck_capacity_t"],
             payload["driver_tg_id"], payload["driver_name"],
             req.origin_lat, req.origin_lng,
             req.return_buffer_km, req.return_time_budget_min,
             payload["total_distance_km"], payload["estimated_minutes"],
             payload["maps_url"], payload["driver_brief"]),
        )
        route_id = cur.lastrowid

        seq = 0
        conn.execute(
            """INSERT INTO route_stops
                (route_id, sequence_order, kind, lat, lng)
               VALUES (?, ?, 'origin', ?, ?)""",
            (route_id, seq, req.origin_lat, req.origin_lng),
        )
        seq += 1
        for s in payload["delivery_stops"]:
            conn.execute(
                """INSERT INTO route_stops
                    (route_id, sequence_order, kind, client_id, client_id_1c,
                     client_display_name, phone_normalized, lat, lng, address,
                     leg_distance_km)
                   VALUES (?, ?, 'delivery', ?, ?, ?, ?, ?, ?, ?, ?)""",
                (route_id, seq, s["client_id"], s["client_id_1c"],
                 s["client_display_name"], s["phone_normalized"],
                 s["lat"], s["lng"], s["address"], s["leg_distance_km"]),
            )
            seq += 1
        for s in payload["collection_stops"]:
            conn.execute(
                """INSERT INTO route_stops
                    (route_id, sequence_order, kind, client_id, client_id_1c,
                     client_display_name, phone_normalized, lat, lng, address,
                     leg_distance_km, detour_minutes, debt_uzs, debt_usd,
                     oldest_aging_bucket)
                   VALUES (?, ?, 'collection', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (route_id, seq, s["client_id"], s["client_id_1c"],
                 s["client_display_name"], s["phone_normalized"],
                 s["lat"], s["lng"], s["address"],
                 s["leg_distance_km"], s["detour_minutes"],
                 s["debt_uzs"], s["debt_usd"], s["oldest_aging_bucket"]),
            )
            seq += 1
        # Closing 'return' marker
        conn.execute(
            """INSERT INTO route_stops
                (route_id, sequence_order, kind, lat, lng)
               VALUES (?, ?, 'return', ?, ?)""",
            (route_id, seq, req.origin_lat, req.origin_lng),
        )
        conn.commit()

        dispatch_result = None
        if req.dispatch:
            dispatch_result = _dispatch_route_to_driver(conn, route_id, payload)
            conn.commit()

        payload["route_id"] = route_id
        payload["dispatch_result"] = dispatch_result
        return payload
    finally:
        conn.close()


def _dispatch_route_to_driver(conn, route_id: int, payload: dict) -> dict:
    """Send the driver a Telegram DM with the Yandex URL + brief.
    Fire-and-forget over httpx; on success mark `dispatched_at` + status.
    Returns {ok, telegram_response|error}."""
    import os
    import httpx as _httpx
    bot_token = os.getenv("BOT_TOKEN", "")
    if not bot_token:
        return {"ok": False, "error": "BOT_TOKEN not set"}
    if not payload.get("driver_tg_id"):
        return {"ok": False, "error": "no driver_tg_id"}

    text = payload["driver_brief"] + f"\n\n🗺 <a href=\"{payload['maps_url']}\">Yandex Maps</a>"
    try:
        r = _httpx.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={
                "chat_id": payload["driver_tg_id"],
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            },
            timeout=10,
        )
        ok = r.status_code == 200 and r.json().get("ok") is True
        if ok:
            conn.execute(
                "UPDATE delivery_routes SET status='dispatched', dispatched_at=datetime('now') WHERE id=?",
                (route_id,),
            )
        return {"ok": ok, "telegram_response": r.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/route/{route_id}/dispatch")
def dispatch_existing_route(route_id: int, admin_key: str = Query(...)):
    """Re-dispatch a previously-saved route to its driver."""
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM delivery_routes WHERE id = ?", (route_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="route not found")
        payload = {
            "driver_tg_id": row["driver_tg_id"],
            "maps_url": row["maps_url"],
            "driver_brief": row["driver_brief"],
        }
        result = _dispatch_route_to_driver(conn, route_id, payload)
        conn.commit()
        return {"route_id": route_id, "dispatch_result": result}
    finally:
        conn.close()


@router.get("/routes")
def list_routes(limit: int = 20, admin_key: str = Query(...)):
    """Recent saved routes (most-recent first), for the Collections-tab history."""
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    conn = get_db()
    rows = conn.execute(
        """SELECT id, created_at, created_by_name, truck_type, truck_capacity_t,
                  driver_name, driver_tg_id, status, total_distance_km,
                  estimated_minutes, dispatched_at, completed_at
           FROM delivery_routes
           ORDER BY created_at DESC
           LIMIT ?""",
        (max(1, min(limit, 200)),),
    ).fetchall()
    conn.close()
    return {"routes": [dict(r) for r in rows]}


@router.get("/routes/{route_id}")
def get_route(route_id: int, admin_key: str = Query(...)):
    """Full detail (route + stops) for a single saved route."""
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    conn = get_db()
    route = conn.execute(
        "SELECT * FROM delivery_routes WHERE id = ?", (route_id,)
    ).fetchone()
    if not route:
        conn.close()
        raise HTTPException(status_code=404, detail="route not found")
    stops = conn.execute(
        "SELECT * FROM route_stops WHERE route_id = ? ORDER BY sequence_order",
        (route_id,),
    ).fetchall()
    conn.close()
    return {"route": dict(route), "stops": [dict(s) for s in stops]}
