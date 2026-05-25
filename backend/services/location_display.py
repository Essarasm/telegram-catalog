"""Canonical display-location helper for allowed_clients rows.

allowed_clients has three independent location sources (history in Error Log #NN):
  1. GPS columns (gps_latitude/longitude/address/region/district) — written by
     agent /lokatsiya pins + Mini App location share. Reverse-geocoded.
  2. Text columns (viloyat/tuman/moljal) — written by Client Master xlsx sync.
  3. Legacy `location` column — pre-Apr-2026 single column; importers + bot both
     wrote here before the split (commit 7f0bebd). Still populated for many rows.

Each writer touches its own column set; no cross-population at write time.
Readers that pick only one set go blind to clients with location in another set
(e.g. /reviewclients showed "joy yo'q" for clients with GPS pins set by agents).

Precedence (user-confirmed 2026-05-25): GPS > text > legacy > "(joy yo'q)".
GPS wins because the pin is ground-truth observed by an agent; text is curated
but may lag reality; legacy is free-text and pre-split, used only when nothing
else has data.

Use this function in EVERY human-facing reader that needs to display a client's
location. Do not write inline `r["viloyat"] or r["tuman"]`-style fallback chains
in handlers — they drift from this function over time.
"""

import re

_LATLNG_RE = re.compile(r"^\s*-?\d+\.\d+\s*,\s*-?\d+\.\d+")

JOY_YOQ = "(joy yo'q)"


def get_display_location(row) -> str:
    """Return the canonical display string for a client's location.

    Args:
        row: a dict-like (sqlite3.Row, dict, or anything with __getitem__/get)
             holding any subset of: gps_region, gps_district, gps_address,
             viloyat, tuman, moljal, location.

    Returns:
        Human-readable location string, or "(joy yo'q)" if no source has data.
    """
    def _get(key):
        try:
            v = row[key] if hasattr(row, "__getitem__") else None
        except (KeyError, IndexError):
            v = None
        if v is None and hasattr(row, "get"):
            v = row.get(key)
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    # 1. GPS reverse-geocode (agent ground-truth)
    gps_region = _get("gps_region")
    gps_district = _get("gps_district")
    gps_address = _get("gps_address")
    if gps_region or gps_district:
        parts = [p for p in (gps_region, gps_district) if p]
        return " → ".join(parts)
    if gps_address:
        return gps_address

    # 2. Text address (Master xlsx curated)
    viloyat = _get("viloyat")
    tuman = _get("tuman")
    moljal = _get("moljal")
    text_parts = [p for p in (viloyat, tuman, moljal) if p]
    if text_parts:
        return " → ".join(text_parts)

    # 3. Legacy `location` column (filter out pre-split "lat,lng|addr" packing)
    legacy = _get("location")
    if legacy and not _LATLNG_RE.match(legacy):
        return legacy

    return JOY_YOQ


def backfill_text_from_gps(conn, client_id: int, geo: dict) -> None:
    """Fill-only backfill of viloyat/tuman from GPS reverse-geocode.

    Call after every GPS write to allowed_clients.gps_*. Idempotent and safe:
    only fills text columns that are currently NULL or empty — never overwrites
    Master-xlsx-curated text addresses.

    This is the write-side complement to get_display_location: it ensures every
    GPS-set client gets minimal text-address coverage, so even readers that
    haven't been migrated to get_display_location can still display SOMETHING
    for the client. Defense-in-depth against future blind readers (Layer 2 of
    the 2026-05-25 dual-source-column fix).

    Args:
        conn: open sqlite3 connection (caller is responsible for commit)
        client_id: allowed_clients.id of the row that just got its GPS updated
        geo: reverse-geocode dict with keys "region", "district" (str or None)

    Does NOT backfill moljal — reverse-geocode address is too coarse to derive
    a landmark/orientir from.
    """
    conn.execute(
        "UPDATE allowed_clients SET "
        "viloyat = COALESCE(NULLIF(viloyat, ''), ?), "
        "tuman = COALESCE(NULLIF(tuman, ''), ?) "
        "WHERE id = ?",
        (geo.get("region"), geo.get("district"), client_id),
    )
