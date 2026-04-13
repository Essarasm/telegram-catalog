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
    if admin_key != "rassvet2026":
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
    if admin_key != "rassvet2026":
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
    """Get a client's saved delivery location."""
    conn = get_db()

    # Find client_id from telegram_id
    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?", (telegram_id,)
    ).fetchone()
    if not user or not user["client_id"]:
        conn.close()
        return {"has_location": False}

    client = conn.execute(
        "SELECT location_district_id, location_moljal_id FROM allowed_clients WHERE id = ?",
        (user["client_id"],),
    ).fetchone()
    conn.close()

    if not client or not client["location_district_id"]:
        return {"has_location": False}

    # Resolve names
    conn = get_db()
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
    conn.close()

    return {
        "has_location": True,
        "district_id": client["location_district_id"],
        "district_name": district["name"] if district else None,
        "moljal_id": client["location_moljal_id"],
        "moljal_name": moljal["name"] if moljal else None,
        "viloyat_id": viloyat["id"] if viloyat else None,
        "viloyat_name": viloyat["name"] if viloyat else None,
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
