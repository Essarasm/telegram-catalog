"""Location hierarchy API for delivery logistics (Session M).

Endpoints:
  GET /api/locations/tree        — full hierarchy (viloyat → district → mo'ljal)
  GET /api/locations?parent_id=X — children of a given location
  GET /api/locations/{id}        — single location detail
  POST /api/locations            — admin: add a new location entry
  PUT /api/locations/{id}        — admin: update a location entry
  GET /api/client-location       — get a client's saved location
  POST /api/client-location      — save/update a client's location
"""
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
from typing import Optional
from backend.database import get_db
from backend.admin_auth import check_admin_key

router = APIRouter(prefix="/api/locations", tags=["locations"])


# ── Models ──────────────────────────────────────────────────

class LocationCreate(BaseModel):
    name: str
    type: str  # 'viloyat', 'district', 'moljal'
    parent_id: Optional[int] = None
    sort_order: Optional[int] = 0


class LocationUpdate(BaseModel):
    name: Optional[str] = None
    sort_order: Optional[int] = None
    is_active: Optional[int] = None


class ClientLocationSave(BaseModel):
    telegram_id: int
    district_id: int
    moljal_id: Optional[int] = None


class GpsLocationSave(BaseModel):
    telegram_id: int
    latitude: float
    longitude: float
    address: str = ""
    region: str = ""
    district: str = ""


# ── Read endpoints ──────────────────────────────────────────

@router.get("/tree")
def get_location_tree():
    """Full hierarchy: viloyats → districts → mo'ljals.

    Returns a nested structure optimized for the frontend dropdown.
    Only active locations are included.
    """
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, type, parent_id, sort_order FROM locations WHERE is_active = 1 ORDER BY sort_order, name"
    ).fetchall()
    conn.close()

    # Build lookup
    by_id = {r["id"]: dict(r) for r in rows}
    viloyats = []
    districts_by_viloyat = {}
    moljals_by_district = {}

    for r in rows:
        d = dict(r)
        if d["type"] == "viloyat":
            d["districts"] = []
            viloyats.append(d)
        elif d["type"] == "district":
            d["moljals"] = []
            pid = d["parent_id"]
            districts_by_viloyat.setdefault(pid, []).append(d)
        elif d["type"] == "moljal":
            pid = d["parent_id"]
            moljals_by_district.setdefault(pid, []).append(d)

    # Assemble tree
    for v in viloyats:
        v["districts"] = districts_by_viloyat.get(v["id"], [])
        for d in v["districts"]:
            d["moljals"] = moljals_by_district.get(d["id"], [])

    return {"viloyats": viloyats}


@router.get("")
def get_locations(
    parent_id: Optional[int] = Query(None),
    type: Optional[str] = Query(None),
):
    """List locations, optionally filtered by parent_id and/or type."""
    conn = get_db()
    query = "SELECT id, name, type, parent_id, sort_order, client_count FROM locations WHERE is_active = 1"
    params = []

    if parent_id is not None:
        query += " AND parent_id = ?"
        params.append(parent_id)
    if type:
        query += " AND type = ?"
        params.append(type)

    query += " ORDER BY sort_order, name"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return {"locations": [dict(r) for r in rows]}


@router.get("/heatmap")
def get_agent_heatmap(
    admin_key: str = Query(...),
    role: str = Query("agent"),
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    agent_tg_id: Optional[int] = Query(None),
):
    """Admin: GPS points submitted by agents (or any role).

    Reads from the canonical `allowed_clients.gps_*` columns — the same
    rows the bot location handler writes when an agent tags a client's
    location. `role='agent'` (default) filters to non-client setters
    (`gps_set_by_role = 'agent'`); `role='client'` returns self-shared
    points; `role='all'` returns both.

    Optional `from` / `to` are ISO date strings filtered on `gps_set_at`.
    Optional `agent_tg_id` scopes to a single setter.

    Returns:
        points: [{lat, lng, client_id, client_name, agent_tg_id,
                  agent_name, role, set_at, address}]
        agents: [{tg_id, name, count}] — all setters in the result, for
                the dashboard's per-agent filter dropdown
        total:  count of points
    """
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")

    conn = get_db()
    where = [
        "ac.gps_latitude IS NOT NULL",
        "ac.gps_longitude IS NOT NULL",
        "COALESCE(ac.status, 'active') != 'merged'",
    ]
    params: list = []
    if role == "agent":
        where.append("ac.gps_set_by_role = 'agent'")
    elif role == "client":
        where.append("ac.gps_set_by_role = 'client'")
    elif role != "all":
        raise HTTPException(status_code=400, detail="role must be agent, client, or all")

    if date_from:
        where.append("ac.gps_set_at >= ?")
        params.append(date_from)
    if date_to:
        where.append("ac.gps_set_at <= ?")
        params.append(date_to + " 23:59:59" if len(date_to) == 10 else date_to)
    if agent_tg_id is not None:
        where.append("ac.gps_set_by_tg_id = ?")
        params.append(agent_tg_id)

    rows = conn.execute(
        f"""
        SELECT
            ac.id              AS client_id,
            ac.client_id_1c    AS client_1c,
            ac.name            AS client_name,
            ac.company_name    AS company_name,
            ac.gps_latitude    AS lat,
            ac.gps_longitude   AS lng,
            ac.gps_address     AS address,
            ac.gps_set_at      AS set_at,
            ac.gps_set_by_tg_id AS agent_tg_id,
            ac.gps_set_by_name  AS agent_name,
            ac.gps_set_by_role  AS role
        FROM allowed_clients ac
        WHERE {' AND '.join(where)}
        ORDER BY ac.gps_set_at DESC
        """,
        params,
    ).fetchall()
    conn.close()

    points = []
    agent_counts: dict = {}
    for r in rows:
        d = dict(r)
        d["client_label"] = d.get("client_1c") or d.get("company_name") or d.get("client_name") or f"#{d['client_id']}"
        points.append(d)
        tg = d.get("agent_tg_id")
        if tg is None:
            continue
        if tg not in agent_counts:
            agent_counts[tg] = {"tg_id": tg, "name": d.get("agent_name") or str(tg), "count": 0}
        agent_counts[tg]["count"] += 1

    agents = sorted(agent_counts.values(), key=lambda a: -a["count"])
    return {"points": points, "agents": agents, "total": len(points)}


@router.get("/{location_id}")
def get_location(location_id: int):
    """Get a single location with its parent chain."""
    conn = get_db()
    row = conn.execute(
        "SELECT id, name, type, parent_id, sort_order, client_count FROM locations WHERE id = ?",
        (location_id,),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Location not found")
    return dict(row)


# ── Admin endpoints ─────────────────────────────────────────

@router.post("")
def create_location(loc: LocationCreate, admin_key: str = Query(...)):
    """Admin: add a new location entry."""
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    if loc.type not in ("viloyat", "district", "moljal"):
        raise HTTPException(status_code=400, detail="type must be viloyat, district, or moljal")

    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO locations (name, type, parent_id, sort_order) VALUES (?, ?, ?, ?)",
            (loc.name, loc.type, loc.parent_id, loc.sort_order or 0),
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except Exception as e:
        conn.close()
        if "UNIQUE" in str(e):
            raise HTTPException(status_code=409, detail="Location already exists")
        raise HTTPException(status_code=500, detail=str(e))
    conn.close()
    return {"ok": True, "id": new_id}


@router.put("/{location_id}")
def update_location(location_id: int, loc: LocationUpdate, admin_key: str = Query(...)):
    """Admin: update a location entry."""
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")

    conn = get_db()
    updates = []
    params = []
    if loc.name is not None:
        updates.append("name = ?")
        params.append(loc.name)
    if loc.sort_order is not None:
        updates.append("sort_order = ?")
        params.append(loc.sort_order)
    if loc.is_active is not None:
        updates.append("is_active = ?")
        params.append(loc.is_active)

    if not updates:
        conn.close()
        return {"ok": True, "changed": False}

    params.append(location_id)
    conn.execute(f"UPDATE locations SET {', '.join(updates)} WHERE id = ?", params)
    conn.commit()
    conn.close()
    return {"ok": True, "changed": True}


# ── Client location endpoints ───────────────────────────────

client_router = APIRouter(prefix="/api/client-location", tags=["client-location"])


@client_router.get("")
def get_client_location(telegram_id: int = Query(...)):
    """Get a client's saved delivery location (GPS + manual).

    GPS resolution order:
      1. `allowed_clients.gps_*` — the canonical client-level GPS, written
         by the bot location handler. Read-only here. Importers never touch
         these columns, so this value survives every Master/CSV re-import.
      2. Requester's own `users.latitude/longitude` — only used when the
         requester has no client link, or when the linked client has no
         GPS yet (so a self-shared GPS still shows up before someone tags
         on their behalf).

    Why no fallback to `allowed_clients.location`: until Apr 2026 that
    column was overloaded as both free-text address (1C/Master imports)
    AND canonical "lat,lng|addr" (bot). Any importer would smash the bot's
    write. The new `gps_*` columns end the overload — the legacy `location`
    field is now treated as free-text only and intentionally ignored here.
    """
    conn = get_db()

    user = conn.execute(
        "SELECT client_id, latitude, longitude, location_address, location_region, location_district, location_updated FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user:
        conn.close()
        return {"has_location": False, "has_gps": False}

    has_gps = False
    gps_data = None

    if user["client_id"]:
        ac_row = conn.execute(
            "SELECT gps_latitude, gps_longitude, gps_address, gps_region, "
            "gps_district, gps_set_at FROM allowed_clients WHERE id = ?",
            (user["client_id"],),
        ).fetchone()
        if ac_row and ac_row["gps_latitude"] is not None and ac_row["gps_longitude"] is not None:
            has_gps = True
            gps_data = {
                "latitude": ac_row["gps_latitude"],
                "longitude": ac_row["gps_longitude"],
                "address": ac_row["gps_address"] or "",
                "region": ac_row["gps_region"] or "",
                "district": ac_row["gps_district"] or "",
                "updated": ac_row["gps_set_at"] or "",
            }

    if not has_gps and user["latitude"] and user["longitude"]:
        has_gps = True
        gps_data = {
            "latitude": user["latitude"],
            "longitude": user["longitude"],
            "address": user["location_address"] or "",
            "region": user["location_region"] or "",
            "district": user["location_district"] or "",
            "updated": user["location_updated"] or "",
        }

    # Manual location from allowed_clients
    manual_data = None
    if user["client_id"]:
        client = conn.execute(
            "SELECT location_district_id, location_moljal_id FROM allowed_clients WHERE id = ?",
            (user["client_id"],),
        ).fetchone()

        if client and client["location_district_id"]:
            district = conn.execute(
                "SELECT id, name, parent_id FROM locations WHERE id = ?",
                (client["location_district_id"],),
            ).fetchone()
            moljal = None
            if client["location_moljal_id"]:
                moljal = conn.execute(
                    "SELECT id, name FROM locations WHERE id = ?",
                    (client["location_moljal_id"],),
                ).fetchone()
            viloyat = None
            if district and district["parent_id"]:
                viloyat = conn.execute(
                    "SELECT id, name FROM locations WHERE id = ?",
                    (district["parent_id"],),
                ).fetchone()

            manual_data = {
                "district_id": client["location_district_id"],
                "district_name": district["name"] if district else None,
                "moljal_id": client["location_moljal_id"],
                "moljal_name": moljal["name"] if moljal else None,
                "viloyat_id": viloyat["id"] if viloyat else None,
                "viloyat_name": viloyat["name"] if viloyat else None,
            }

    conn.close()

    return {
        "has_location": has_gps or manual_data is not None,
        "has_gps": has_gps,
        "gps": gps_data,
        "manual": manual_data,
        # Backward compatibility
        "district_id": manual_data["district_id"] if manual_data else None,
        "district_name": manual_data["district_name"] if manual_data else None,
        "moljal_id": manual_data["moljal_id"] if manual_data else None,
        "moljal_name": manual_data["moljal_name"] if manual_data else None,
        "viloyat_id": manual_data["viloyat_id"] if manual_data else None,
        "viloyat_name": manual_data["viloyat_name"] if manual_data else None,
    }


@client_router.post("")
def save_client_location(data: ClientLocationSave):
    """Save or update a client's delivery location."""
    conn = get_db()

    # Find client_id from telegram_id
    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?", (data.telegram_id,)
    ).fetchone()
    if not user or not user["client_id"]:
        conn.close()
        raise HTTPException(status_code=404, detail="Client not found for this telegram_id")

    # Validate district exists
    district = conn.execute(
        "SELECT id, type FROM locations WHERE id = ? AND is_active = 1",
        (data.district_id,),
    ).fetchone()
    if not district or district["type"] != "district":
        conn.close()
        raise HTTPException(status_code=400, detail="Invalid district_id")

    # Validate moljal if provided
    if data.moljal_id:
        moljal = conn.execute(
            "SELECT id, type, parent_id FROM locations WHERE id = ? AND is_active = 1",
            (data.moljal_id,),
        ).fetchone()
        if not moljal or moljal["type"] != "moljal":
            conn.close()
            raise HTTPException(status_code=400, detail="Invalid moljal_id")
        if moljal["parent_id"] != data.district_id:
            conn.close()
            raise HTTPException(status_code=400, detail="Mo'ljal does not belong to this district")

    conn.execute(
        "UPDATE allowed_clients SET location_district_id = ?, location_moljal_id = ? WHERE id = ?",
        (data.district_id, data.moljal_id, user["client_id"]),
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@client_router.post("/gps")
def save_gps_location(data: GpsLocationSave):
    """Save GPS coordinates + address from the in-app map picker.

    Also propagates the reverse-geocoded region/district back into the
    matched allowed_clients row (viloyat/tuman), but only when those
    fields are currently empty — never overrides operator-curated values.
    This is the Phase 1f "Mini App → Client Master backward flow" hook.
    """
    conn = get_db()
    user = conn.execute(
        "SELECT telegram_id, client_id, first_name FROM users WHERE telegram_id = ?",
        (data.telegram_id,),
    ).fetchone()
    if not user:
        conn.close()
        raise HTTPException(status_code=404, detail="User not found")

    conn.execute(
        "UPDATE users SET latitude = ?, longitude = ?, location_address = ?, "
        "location_region = ?, location_district = ?, "
        "location_updated = datetime('now') WHERE telegram_id = ?",
        (data.latitude, data.longitude, data.address, data.region, data.district, data.telegram_id),
    )

    # Mirror to the canonical client-level GPS so the Cabinet xaritada toggle
    # (and any agent viewing this client) sees the in-app map pick too.
    if user["client_id"]:
        setter_name = user["first_name"] or str(data.telegram_id)
        conn.execute(
            "UPDATE allowed_clients SET "
            "gps_latitude = ?, gps_longitude = ?, gps_address = ?, "
            "gps_region = ?, gps_district = ?, gps_set_at = datetime('now'), "
            "gps_set_by_tg_id = ?, gps_set_by_name = ?, gps_set_by_role = 'client' "
            "WHERE id = ?",
            (data.latitude, data.longitude, data.address, data.region,
             data.district, data.telegram_id, setter_name, user["client_id"]),
        )

    # Backward flow: if this Telegram user is linked to an allowed_clients row,
    # fill in Viloyat/Tuman from the reverse-geocode when those are empty.
    # Never overwrite operator-curated values (empty-only rule).
    try:
        ac_row = conn.execute(
            "SELECT id, viloyat, tuman FROM allowed_clients "
            "WHERE matched_telegram_id = ? LIMIT 1",
            (data.telegram_id,),
        ).fetchone()
        if ac_row:
            updates, params = [], []
            if data.region and not ac_row["viloyat"]:
                updates.append("viloyat = ?")
                params.append(data.region)
            if data.district and not ac_row["tuman"]:
                updates.append("tuman = ?")
                params.append(data.district)
            if updates:
                params.append(ac_row["id"])
                conn.execute(
                    f"UPDATE allowed_clients SET {', '.join(updates)}, "
                    f"last_master_synced_at = datetime('now') WHERE id = ?",
                    params,
                )
    except Exception:
        # Never block a user's GPS save on a backward-flow hiccup
        pass

    conn.commit()
    conn.close()
    return {"ok": True}
