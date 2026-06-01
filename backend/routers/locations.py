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
from backend.admin_auth import check_admin_key, resolve_auth

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
    auth = resolve_auth(admin_key)
    if not auth or auth["role"] not in ("admin", "agent"):
        raise HTTPException(status_code=403, detail="Invalid admin key")

    conn = get_db()
    where = [
        "ac.gps_latitude IS NOT NULL",
        "ac.gps_longitude IS NOT NULL",
        "COALESCE(ac.status, 'active') = 'active'",
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


# ── Customer Coverage (route planning) ──────────────────────────────────────
#
# DIRECTIONS schedule (Mon–Sat × geography) — sales managers' weekly route.
# Centroids are approximate (used for non-pinned client map-placement only).
# Source: parent root `Client Master 13.05.26.xlsx` sheet `Directions`.

_DIRECTIONS = [
    # (day_key,    tuman,                   approx_lat, approx_lng)
    ("Dushanba",   "Chelak",                40.0530, 66.3000),
    ("Dushanba",   "Payariq tumani",        40.0260, 66.6920),
    ("Dushanba",   "Narimon",               40.0050, 66.7100),
    ("Dushanba",   "Motrid",                39.6720, 66.9100),
    ("Dushanba",   "Al-Buxoriy",            39.6610, 66.8400),
    ("Seshanba",   "Juma",                  39.6900, 66.7000),
    ("Seshanba",   "Pastdarg'om",           39.5800, 66.7800),
    ("Seshanba",   "Charxin",               39.5300, 66.9800),
    ("Seshanba",   "Xazora",                39.6100, 67.0400),
    ("Seshanba",   "Super",                 39.6500, 66.9700),
    ("Seshanba",   "Dal lager",             39.6300, 66.8800),
    ("Chorshanba", "Jomboy",                39.7250, 67.1400),
    ("Chorshanba", "Bulung'ur",             39.9100, 67.2500),
    ("Payshanba",  "Urgut",                 39.4100, 67.2400),
    ("Payshanba",  "Toyloq",                39.6200, 66.7900),
    ("Payshanba",  "Jartepa",               39.4500, 67.1400),
    ("Payshanba",  "Juma bozor",            39.6900, 66.7100),
    ("Juma",       "Ishtixon",              39.9700, 66.4900),
    ("Juma",       "Oqdaryo",               39.8500, 66.8500),
    ("Juma",       "Mitan shaharchasi",     39.8400, 66.8800),
    ("Juma",       "Dahbet",                39.7600, 66.8700),
    ("Shanba",     "Kattaqo'rg'on",         39.8990, 66.2620),
    ("Shanba",     "Payshanba shaharchasi", 39.7800, 66.3200),
]

# Normalized tuman keyword → day. Substring-match on normalized (lower, ASCII-stripped) text.
_NAME_MATCH_DAY = {
    "pastdargom": "Seshanba", "pastdaron": "Seshanba", "charxin": "Seshanba",
    "xazora": "Seshanba",     "hazora": "Seshanba",
    "jomboy": "Chorshanba",   "jombay": "Chorshanba", "bulungur": "Chorshanba",
    "urgut": "Payshanba",     "ugrut": "Payshanba",   "toyloq": "Payshanba",
    "tayloq": "Payshanba",    "jartepa": "Payshanba",
    "ishtixon": "Juma",       "ishtihon": "Juma",     "oqdaryo": "Juma",
    "akdarya": "Juma",        "dahbet": "Juma",       "dagbet": "Juma",
    "mitan": "Juma",
    "kattaqorgon": "Shanba",  "kattakurgan": "Shanba", "payshanba": "Shanba",
    "payariq": "Dushanba",    "paiariq": "Dushanba",  "chelak": "Dushanba",
    "chalak": "Dushanba",     "narimon": "Dushanba",
}

_CITY_KEYS = ("samarqandshahri", "samarqandshahar", "samarkand")
_OUTSIDE_TUMANS = {  # NOT on the weekly schedule
    "nurobod": (39.7950, 66.1500),
    "qoshrabot": (40.2000, 66.7000),
    "qoshrabod": (40.2000, 66.7000),
    "narpay": (39.7400, 66.0300),
}

# Proposal B bucket thresholds, monthly USD.
_BUCKET_THRESHOLDS = [
    ("Heavy",  4120.0),
    ("Large",  1721.0),
    ("Medium",  621.0),
    ("Small",   125.0),
    ("Micro",     0.0),
]

# Fallback FX rate (UZS per USD). Matches memory `reference_fx_rate_coverage.md` —
# ±2% of actual across the whole coverage window. For dashboard buckets this is fine.
_USD_FALLBACK = 12000.0

# Bucketing window for Proposal B comparison (months).
_BUCKET_WINDOW_MONTHS = 12


def _normalize_name(s: Optional[str]) -> str:
    if not s:
        return ""
    import re
    import unicodedata
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    pairs = [
        ("а","a"),("б","b"),("в","v"),("г","g"),("д","d"),("е","e"),("ё","yo"),
        ("ж","j"),("з","z"),("и","i"),("й","y"),("к","k"),("л","l"),("м","m"),
        ("н","n"),("о","o"),("п","p"),("р","r"),("с","s"),("т","t"),("у","u"),
        ("ф","f"),("х","kh"),("ц","ts"),("ч","ch"),("ш","sh"),("щ","shch"),
        ("ъ",""),("ы","y"),("ь",""),("э","e"),("ю","yu"),("я","ya"),
    ]
    out = []
    for ch in s.lower():
        repl = next((r for c, r in pairs if ch == c), ch)
        out.append(repl)
    return re.sub(r"[^a-z0-9]+", "", "".join(out))


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    import math
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def _hash_jitter(seed: str, scale_deg: float = 0.012) -> tuple[float, float]:
    """Deterministic ±~1 km offset so master-inferred clients in the same tuman don't overlap."""
    import hashlib
    h = hashlib.md5((seed or "").encode("utf-8", errors="replace")).digest()
    return ((h[0] - 128) / 128.0) * scale_deg, ((h[1] - 128) / 128.0) * scale_deg


def _assign_day_pinned(lat: float, lng: float, blob: str) -> tuple[str, str]:
    """For a GPS-pinned client: returns (day_key, matched_tuman). Name-match first, then nearest centroid."""
    norm = _normalize_name(blob)
    for key, day in _NAME_MATCH_DAY.items():
        if key in norm:
            cands = [(t, la, ln) for d, t, la, ln in _DIRECTIONS if d == day]
            t, _, _ = min(cands, key=lambda x: _haversine_km(lat, lng, x[1], x[2])) if cands else (key, 0, 0)
            return day, t
    for ck in _CITY_KEYS:
        if ck in norm:
            return "CITY", "Samarqand shahri"
    best = None
    for d, t, la, ln in _DIRECTIONS:
        dist = _haversine_km(lat, lng, la, ln)
        if best is None or dist < best[2]:
            best = (d, t, dist)
    return (best[0], best[1]) if best else ("UNMAPPED", "")


def _assign_day_master(c1c: str, tuman: str, moljal: str, viloyat: str) -> tuple[str, str, float, float]:
    """For a non-pinned client: returns (day_key, matched_tuman, approx_lat, approx_lng) from master text."""
    blob = " ".join(s for s in (tuman, moljal, viloyat) if s)
    norm = _normalize_name(blob)
    for ck in _CITY_KEYS:
        if ck in norm:
            lat0, lng0 = 39.6542, 66.9597
            jit_lat, jit_lng = _hash_jitter(c1c, scale_deg=0.015)
            return "CITY", "Samarqand shahri", lat0 + jit_lat, lng0 + jit_lng
    for key, (lat0, lng0) in _OUTSIDE_TUMANS.items():
        if key in norm:
            jit_lat, jit_lng = _hash_jitter(c1c)
            return "OUTSIDE", (tuman or key), lat0 + jit_lat, lng0 + jit_lng
    for key, day in _NAME_MATCH_DAY.items():
        if key in norm:
            cands = [(t, la, ln) for d, t, la, ln in _DIRECTIONS if d == day]
            if not cands:
                continue
            pref = [x for x in cands if key in _normalize_name(x[0])]
            t, lat0, lng0 = pref[0] if pref else cands[0]
            jit_lat, jit_lng = _hash_jitter(c1c)
            return day, t, lat0 + jit_lat, lng0 + jit_lng
    return "UNMAPPED", (tuman or ""), 0.0, 0.0


def _classify_bucket(monthly_usd: float) -> str:
    for name, threshold in _BUCKET_THRESHOLDS:
        if monthly_usd >= threshold:
            return name
    return "Micro"


@router.get("/customer-coverage")
def get_customer_coverage(
    admin_key: str = Query(...),
    since: Optional[str] = Query("2026-01-01", description="ISO date — trade-active cutoff"),
    bucket: Optional[str] = Query(None, description="Heavy/Large/Medium/Small/Micro — filter to one bucket"),
    day: Optional[str] = Query(None, description="Dushanba..Shanba or CITY/OUTSIDE — filter to one day"),
    source: Optional[str] = Query(None, description="pinned/master/dormant — filter to one source"),
    include_dormant: bool = Query(False, description="Include pinned-but-not-trading clients as location_source='dormant'"),
):
    """Return every trade-active client with: precise GPS pin OR master-inferred approximate location.

    For each client returns: lat/lng, bucket (Proposal B thresholds on rolling 12-month USD-eq trade),
    day-of-week assignment based on the Directions schedule, location source flag, last trade date.

    Designed for the admin Customer Coverage tab. Read-only; ~500 clients, ~200ms warm.
    """
    auth = resolve_auth(admin_key)
    if not auth or auth["role"] not in ("admin", "agent"):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    # Defensive: when this function is called outside FastAPI dependency injection
    # (e.g., test harness), the Query(None) defaults arrive as fastapi.params.Query
    # objects rather than None. Normalize so the optional-filter branches behave.
    if not isinstance(bucket, str): bucket = None
    if not isinstance(day, str): day = None
    if not isinstance(source, str): source = None
    if not isinstance(since, str): since = "2026-01-01"
    if len(since) < 10:
        raise HTTPException(status_code=400, detail="since must be ISO YYYY-MM-DD")

    conn = get_db()

    # 1) Universe of trade-active clients + last trade date
    rows = conn.execute(
        """
        SELECT client_name_1c AS c1c, MAX(doc_date) AS last_date, 'realizatsiya' AS kind
        FROM real_orders WHERE doc_date >= ? AND client_name_1c IS NOT NULL
        GROUP BY client_name_1c
        UNION ALL
        SELECT client_name_1c, MAX(doc_date), 'kassa'
        FROM client_payments WHERE doc_date >= ? AND client_name_1c IS NOT NULL
        GROUP BY client_name_1c
        """,
        (since, since),
    ).fetchall()
    active_last: dict[str, tuple[str, str]] = {}
    for r in rows:
        c1c = r["c1c"] if "c1c" in r.keys() else r[0]
        d = r[1]; kind = r[2]
        if c1c is None:
            continue
        if c1c not in active_last or (d and d > active_last[c1c][0]):
            active_last[c1c] = (d, kind)

    if not active_last:
        conn.close()
        return {"clients": [], "totals": {"universe": 0, "pinned": 0, "master": 0, "unmapped": 0}, "tumans": []}

    # 2) Compute USD-eq monthly volume per 1C name (rolling _BUCKET_WINDOW_MONTHS)
    from datetime import datetime, timedelta
    bucket_since = (datetime.fromisoformat(since[:10]) - timedelta(days=30 * _BUCKET_WINDOW_MONTHS)).date().isoformat()
    # USD-eq = USD leg directly + UZS leg converted via FX. Both legs are
    # honest columns at the doc level; no `currency` filter (always 'USD' on
    # real_orders due to 1C export quirk — see realorders_revenue.py).
    bucket_rows = conn.execute(
        """
        SELECT client_name_1c AS c1c,
               SUM(COALESCE(total_sum_currency, 0)
                   + COALESCE(total_sum, 0) / ?) AS total_usd
        FROM real_orders
        WHERE doc_date >= ? AND client_name_1c IS NOT NULL
        GROUP BY client_name_1c
        """,
        (_USD_FALLBACK, bucket_since),
    ).fetchall()
    usd_by_1c: dict[str, float] = {}
    for r in bucket_rows:
        c1c = r["c1c"] if "c1c" in r.keys() else r[0]
        if c1c is None:
            continue
        usd_by_1c[c1c] = float(r[1] or 0) / max(_BUCKET_WINDOW_MONTHS, 1)

    # 3) Pull allowed_clients rows for active 1Cs
    placeholders = ",".join(["?"] * len(active_last))
    ac_rows = conn.execute(
        f"""
        SELECT ac.id, ac.client_id_1c, ac.name, ac.company_name, ac.phone_normalized,
               ac.gps_latitude, ac.gps_longitude, ac.gps_address, ac.gps_region, ac.gps_district,
               ac.gps_set_by_role, ac.gps_set_at, ac.gps_set_by_name,
               ac.viloyat, ac.tuman, ac.moljal
        FROM allowed_clients ac
        WHERE ac.client_id_1c IN ({placeholders})
          AND COALESCE(ac.status, 'active') = 'active'
        """,
        list(active_last.keys()),
    ).fetchall()
    by_1c: dict[str, list] = {}
    for r in ac_rows:
        c1c = r["client_id_1c"]
        by_1c.setdefault(c1c, []).append(r)

    # 3b) Optionally pull "lost" pinned clients — agent/driver pins on rows
    # whose client_id_1c is NOT in the trade-active universe. Operationally
    # useful for ops: surfaces real dormancy + latent 1C-card-rename leaks
    # (where the field visit is real but the 1C invoices land under a new
    # name).
    dormant_rows: list = []
    if include_dormant:
        not_in_active = "ac.client_id_1c IS NULL OR ac.client_id_1c NOT IN ({})".format(
            ",".join("?" * len(active_last))
        )
        dormant_rows = conn.execute(
            f"""
            SELECT ac.id, ac.client_id_1c, ac.name, ac.company_name, ac.phone_normalized,
                   ac.gps_latitude, ac.gps_longitude, ac.gps_address, ac.gps_region, ac.gps_district,
                   ac.gps_set_by_role, ac.gps_set_at, ac.gps_set_by_name, ac.gps_set_by_tg_id,
                   ac.viloyat, ac.tuman, ac.moljal
            FROM allowed_clients ac
            WHERE ac.gps_latitude IS NOT NULL
              AND ac.gps_longitude IS NOT NULL
              AND COALESCE(ac.status, 'active') = 'active'
              AND ac.gps_set_by_role IN ('agent', 'driver')
              AND ({not_in_active})
            """,
            list(active_last.keys()),
        ).fetchall()

    conn.close()

    # 4) Pick representative + assign location/day/bucket
    clients = []
    counts = {"pinned": 0, "master": 0, "unmapped": 0, "dormant": 0}
    tuman_agg: dict[str, dict] = {}

    for c1c, (last_date, last_kind) in active_last.items():
        rows_for_1c = by_1c.get(c1c, [])
        # Prefer a row that has a pin; fall back to first row
        best = None
        for r in rows_for_1c:
            has_pin = r["gps_latitude"] is not None and r["gps_longitude"] is not None and (r["gps_set_by_role"] or "") in ("agent", "driver")
            if has_pin and (best is None or _is_newer(r, best)):
                best = r
        if best is None and rows_for_1c:
            best = rows_for_1c[0]

        monthly_usd = usd_by_1c.get(c1c, 0.0)
        bkt = _classify_bucket(monthly_usd)

        if best is None:
            # 1C name exists in trade tables but no allowed_clients row at all
            location_source = "unmapped"
            day_key = "UNMAPPED"
            matched_tuman = ""
            lat = 0.0; lng = 0.0
            name = ""; phone = None
            address = None; gps_district = None
            master_v = master_t = master_m = ""
        else:
            has_pin = best["gps_latitude"] is not None and best["gps_longitude"] is not None and (best["gps_set_by_role"] or "") in ("agent", "driver")
            master_v = (best["viloyat"] or "").strip() if isinstance(best["viloyat"], str) else ""
            master_t = (best["tuman"] or "").strip() if isinstance(best["tuman"], str) else ""
            master_m = (best["moljal"] or "").strip() if isinstance(best["moljal"], str) else ""

            if has_pin:
                location_source = "pinned"
                lat = float(best["gps_latitude"]); lng = float(best["gps_longitude"])
                blob = " ".join(filter(None, [best["gps_district"], best["gps_address"], best["gps_region"]]))
                day_key, matched_tuman = _assign_day_pinned(lat, lng, blob)
            elif master_t:
                location_source = "master"
                day_key, matched_tuman, lat, lng = _assign_day_master(c1c, master_t, master_m, master_v)
            else:
                location_source = "unmapped"
                day_key = "UNMAPPED"
                matched_tuman = ""
                lat = 0.0; lng = 0.0
            name = best["name"] or ""
            phone = best["phone_normalized"]
            address = best["gps_address"]
            gps_district = best["gps_district"]

        counts[location_source] += 1

        # Optional filters
        if bucket and bkt != bucket: continue
        if day and day_key != day: continue
        if source and location_source != source: continue

        # Skip unmapped from the map response (but their count is preserved in `counts`)
        if location_source == "unmapped":
            continue

        clients.append({
            "client_id_1c": c1c,
            "name": name,
            "phone": phone,
            "lat": lat,
            "lng": lng,
            "location_source": location_source,
            "day": day_key,
            "matched_tuman": matched_tuman,
            "bucket": bkt,
            "monthly_usd": round(monthly_usd, 0),
            "last_trade_date": last_date,
            "last_trade_kind": last_kind,
            "address": address,
            "gps_district": gps_district,
            "master_viloyat": master_v if best is not None else "",
            "master_tuman":   master_t if best is not None else "",
            "master_moljal":  master_m if best is not None else "",
            "pinned_by":      (best["gps_set_by_name"] if best is not None else None) if location_source == "pinned" else None,
            "pinned_at":      (best["gps_set_at"]      if best is not None else None) if location_source == "pinned" else None,
        })

        # Tuman aggregate (for the Top Revenue Tumans table)
        key = matched_tuman or "?"
        agg = tuman_agg.setdefault(key, {"tuman": key, "day": day_key, "clients": 0, "monthly_usd": 0.0,
                                         "heavy": 0, "large": 0, "medium": 0, "small": 0, "micro": 0})
        agg["clients"] += 1
        agg["monthly_usd"] += monthly_usd
        agg[bkt.lower()] += 1

    # 4b) Append dormant pinned clients (location_source='dormant'). Skipped
    # by tuman aggregation since they have no monthly_usd, but rendered on
    # the map so ops can see the gap between agent coverage and trade activity.
    for r in dormant_rows:
        lat = float(r["gps_latitude"])
        lng = float(r["gps_longitude"])
        blob = " ".join(filter(None, [r["gps_district"], r["gps_address"], r["gps_region"]]))
        day_key, matched_tuman = _assign_day_pinned(lat, lng, blob)
        counts["dormant"] += 1
        if bucket: continue  # dormant has no bucket
        if day and day_key != day: continue
        if source and source != "dormant": continue
        clients.append({
            "client_id_1c": r["client_id_1c"],
            "name": r["name"] or "",
            "phone": r["phone_normalized"],
            "lat": lat,
            "lng": lng,
            "location_source": "dormant",
            "day": day_key,
            "matched_tuman": matched_tuman,
            "bucket": None,
            "monthly_usd": 0,
            "last_trade_date": None,
            "last_trade_kind": None,
            "address": r["gps_address"],
            "gps_district": r["gps_district"],
            "master_viloyat": r["viloyat"] or "",
            "master_tuman":   r["tuman"] or "",
            "master_moljal":  r["moljal"] or "",
            "pinned_by":      r["gps_set_by_name"],
            "pinned_at":      r["gps_set_at"],
        })

    tuman_list = sorted(tuman_agg.values(), key=lambda x: -x["monthly_usd"])
    for t in tuman_list:
        t["monthly_usd"] = round(t["monthly_usd"], 0)

    return {
        "clients": clients,
        "totals": {
            "universe": len(active_last),
            "pinned": counts["pinned"],
            "master": counts["master"],
            "unmapped": counts["unmapped"],
            "dormant": counts["dormant"],
            "shown": len(clients),
        },
        "tumans": tuman_list,
        "filters": {"since": since, "bucket": bucket, "day": day, "source": source, "include_dormant": include_dormant},
    }


def _is_newer(a, b) -> bool:
    """Compare two allowed_clients rows: prefer the one with more recent gps_set_at."""
    a_has = a["gps_latitude"] is not None and a["gps_longitude"] is not None and (a["gps_set_by_role"] or "") in ("agent", "driver")
    b_has = b["gps_latitude"] is not None and b["gps_longitude"] is not None and (b["gps_set_by_role"] or "") in ("agent", "driver")
    if a_has and not b_has:
        return True
    if not a_has:
        return False
    return (a["gps_set_at"] or "") > (b["gps_set_at"] or "")


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


@router.post("/clear-pin")
def clear_client_pin(
    client_id: int = Query(..., description="allowed_clients.id"),
    admin_key: str = Query(...),
    reason: Optional[str] = Query(None, max_length=500),
):
    """Admin: clear an agent/client-tagged GPS pin on `allowed_clients`.

    Snapshots the prior `gps_*` columns into `admin_action_log` before
    NULLing them, so the original pin (lat/lng, address, who set it, when)
    is forensically recoverable from the audit row's `args` JSON. The raw
    `location_attempts` row from when the pin was first shared is never
    touched — that table is the immutable source-of-truth.

    Admin role only. Agents cannot self-clear pins they set.
    """
    auth = resolve_auth(admin_key)
    if not auth or auth["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin-only")

    conn = get_db()
    prior = conn.execute(
        "SELECT id, name, client_id_1c, gps_latitude, gps_longitude, gps_address, "
        "gps_region, gps_district, gps_set_at, gps_set_by_tg_id, gps_set_by_name, "
        "gps_set_by_role FROM allowed_clients WHERE id = ?",
        (client_id,),
    ).fetchone()
    if not prior:
        conn.close()
        raise HTTPException(status_code=404, detail="Client not found")
    if prior["gps_latitude"] is None and prior["gps_longitude"] is None:
        conn.close()
        return {"ok": True, "cleared": False, "reason": "no_pin_to_clear",
                "client_id": client_id, "client_name": prior["name"]}

    import json as _json
    args_payload = _json.dumps({
        "client_id": prior["id"],
        "client_name": prior["name"],
        "client_id_1c": prior["client_id_1c"],
        "prior_gps_latitude": prior["gps_latitude"],
        "prior_gps_longitude": prior["gps_longitude"],
        "prior_gps_address": prior["gps_address"],
        "prior_gps_region": prior["gps_region"],
        "prior_gps_district": prior["gps_district"],
        "prior_gps_set_at": prior["gps_set_at"],
        "prior_gps_set_by_tg_id": prior["gps_set_by_tg_id"],
        "prior_gps_set_by_name": prior["gps_set_by_name"],
        "prior_gps_set_by_role": prior["gps_set_by_role"],
        "reason": (reason or "").strip(),
    }, ensure_ascii=False)
    conn.execute(
        "INSERT INTO admin_action_log (telegram_id, user_name, command, args) "
        "VALUES (?, ?, ?, ?)",
        (auth.get("telegram_id"), auth.get("name"), "clear_client_gps", args_payload),
    )
    conn.execute(
        "UPDATE allowed_clients SET "
        "gps_latitude = NULL, gps_longitude = NULL, gps_address = NULL, "
        "gps_region = NULL, gps_district = NULL, gps_set_at = NULL, "
        "gps_set_by_tg_id = NULL, gps_set_by_name = NULL, gps_set_by_role = NULL "
        "WHERE id = ?",
        (client_id,),
    )
    conn.commit()
    conn.close()
    return {
        "ok": True,
        "cleared": True,
        "client_id": client_id,
        "client_name": prior["name"],
        "prior": {
            "lat": prior["gps_latitude"],
            "lng": prior["gps_longitude"],
            "address": prior["gps_address"],
            "set_by_name": prior["gps_set_by_name"],
            "set_at": prior["gps_set_at"],
        },
    }


@router.post("/restore-pin")
def restore_client_pin(
    client_id: int = Query(..., description="allowed_clients.id"),
    admin_key: str = Query(...),
):
    """Admin: restore a client's GPS from the most recent snapshot in
    `admin_action_log`. Recognises two snapshot sources:

    - `clear_client_gps`: written by the admin-initiated POST /clear-pin
      before it NULLs the columns. Restores the cleared pin.
    - `auto_overwrite_snapshot`: written by `bot/handlers/location.py`
      before an agent's location share overwrites an existing pin. Restores
      whoever was the prior owner — useful when a stale `users.client_id`
      caused a wrong agent to clobber a good pin.

    Picks the newest snapshot for this client across either source. Logs
    the restore as a new `admin_action_log` row with
    `command='restore_client_gps'` so every round-trip is fully auditable.
    """
    auth = resolve_auth(admin_key)
    if not auth or auth["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin-only")

    import json as _json
    conn = get_db()
    rows = conn.execute(
        "SELECT id, args FROM admin_action_log "
        "WHERE command IN ('clear_client_gps', 'auto_overwrite_snapshot') "
        "ORDER BY id DESC"
    ).fetchall()
    snap = None
    snap_audit_id = None
    for row in rows:
        try:
            parsed = _json.loads(row["args"])
        except Exception:
            continue
        if int(parsed.get("client_id", 0)) == client_id:
            snap = parsed
            snap_audit_id = row["id"]
            break
    if not snap:
        conn.close()
        raise HTTPException(status_code=404,
                            detail="No prior clear-pin snapshot for this client")

    conn.execute(
        "UPDATE allowed_clients SET "
        "gps_latitude = ?, gps_longitude = ?, gps_address = ?, "
        "gps_region = ?, gps_district = ?, gps_set_at = ?, "
        "gps_set_by_tg_id = ?, gps_set_by_name = ?, gps_set_by_role = ? "
        "WHERE id = ?",
        (snap.get("prior_gps_latitude"), snap.get("prior_gps_longitude"),
         snap.get("prior_gps_address"), snap.get("prior_gps_region"),
         snap.get("prior_gps_district"), snap.get("prior_gps_set_at"),
         snap.get("prior_gps_set_by_tg_id"), snap.get("prior_gps_set_by_name"),
         snap.get("prior_gps_set_by_role"), client_id),
    )
    audit_args = _json.dumps({
        "client_id": client_id,
        "client_name": snap.get("client_name"),
        "restored_from_audit_id": snap_audit_id,
        "restored_lat": snap.get("prior_gps_latitude"),
        "restored_lng": snap.get("prior_gps_longitude"),
        "restored_set_by_name": snap.get("prior_gps_set_by_name"),
        "restored_set_at": snap.get("prior_gps_set_at"),
    }, ensure_ascii=False)
    conn.execute(
        "INSERT INTO admin_action_log (telegram_id, user_name, command, args) "
        "VALUES (?, ?, ?, ?)",
        (auth.get("telegram_id"), auth.get("name"), "restore_client_gps", audit_args),
    )
    conn.commit()
    conn.close()
    return {
        "ok": True,
        "client_id": client_id,
        "client_name": snap.get("client_name"),
        "restored_from_audit_id": snap_audit_id,
        "restored": {
            "lat": snap.get("prior_gps_latitude"),
            "lng": snap.get("prior_gps_longitude"),
            "address": snap.get("prior_gps_address"),
            "set_by_name": snap.get("prior_gps_set_by_name"),
            "set_at": snap.get("prior_gps_set_at"),
        },
    }


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

    # Fallback to users.latitude/longitude ONLY when the requester has no
    # client link. Otherwise the agent's own self-shared pin would surface
    # on every acted-as client whose canonical gps_* is NULL, painting the
    # cabinet green while the bot picker (which reads gps_* directly) says
    # "no location set". See Error Log #31 AGENT_GPS_FALLBACK_MASKS_NULL.
    if not has_gps and not user["client_id"] and user["latitude"] and user["longitude"]:
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
