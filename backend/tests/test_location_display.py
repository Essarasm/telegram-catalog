"""Tests for backend.services.location_display.

Validates the 3-source precedence in get_display_location (GPS > text > legacy
> JOY_YOQ) and the fill-only behavior of backfill_text_from_gps.
"""
import sqlite3
import pytest

from backend.services.location_display import (
    backfill_text_from_gps,
    get_display_location,
    JOY_YOQ,
)


def _row(**kwargs):
    """Build a sqlite-Row-like dict with all expected keys defaulting to None."""
    keys = ("gps_region", "gps_district", "gps_address",
            "viloyat", "tuman", "moljal", "location")
    return {k: kwargs.get(k) for k in keys}


# --- Single-source cases ---

def test_gps_region_and_district():
    row = _row(gps_region="Samarqand viloyati", gps_district="Kattaqo'rg'on shahri")
    assert get_display_location(row) == "Samarqand viloyati → Kattaqo'rg'on shahri"


def test_gps_address_only_when_region_district_empty():
    """Mirrors #679-style row where only gps_address is set without region/district."""
    row = _row(gps_address="Kattaqo'rg'on shahri")
    assert get_display_location(row) == "Kattaqo'rg'on shahri"


def test_text_only():
    row = _row(viloyat="Samarqand", tuman="Pastdarg'om tuman", moljal="Juma")
    assert get_display_location(row) == "Samarqand → Pastdarg'om tuman → Juma"


def test_legacy_only():
    """Mirrors #1350/#1877 — no GPS, no text, but legacy `location` populated."""
    row = _row(location="Urgut tuman, Jartepa")
    assert get_display_location(row) == "Urgut tuman, Jartepa"


def test_empty_returns_joy_yoq():
    assert get_display_location(_row()) == JOY_YOQ


# --- Precedence cases ---

def test_gps_wins_over_text():
    row = _row(gps_region="Samarqand viloyati", gps_district="Kattaqo'rg'on shahri",
               viloyat="X", tuman="Y", moljal="Z")
    assert get_display_location(row) == "Samarqand viloyati → Kattaqo'rg'on shahri"


def test_text_wins_over_legacy():
    row = _row(viloyat="Samarqand", tuman="Pastdarg'om tuman",
               location="some legacy junk")
    assert get_display_location(row) == "Samarqand → Pastdarg'om tuman"


def test_gps_wins_over_everything():
    row = _row(gps_region="R", gps_district="D",
               viloyat="V", tuman="T", moljal="M",
               location="L")
    assert get_display_location(row) == "R → D"


# --- Legacy lat/lng filter (Apr-2026 pre-split bot format) ---

def test_legacy_latlng_packed_is_filtered():
    """Pre-split bot wrote 'lat,lng|addr' into `location`. Don't display as text."""
    row = _row(location="39.922847,66.283873|Kattaqo'rg'on")
    assert get_display_location(row) == JOY_YOQ


def test_legacy_latlng_no_addr_is_filtered():
    row = _row(location="39.922847,66.283873")
    assert get_display_location(row) == JOY_YOQ


def test_legacy_latlng_with_spaces_is_filtered():
    row = _row(location="  39.922847 , 66.283873  ")
    assert get_display_location(row) == JOY_YOQ


def test_legacy_text_starting_with_number_is_kept():
    """Don't over-filter — a real address starting with a number is valid."""
    row = _row(location="12 Mustaqillik ko'chasi, Samarqand")
    assert get_display_location(row) == "12 Mustaqillik ko'chasi, Samarqand"


# --- Partial / incomplete cases ---

def test_gps_lat_lng_set_but_no_geocode_falls_back_to_text():
    """Mirrors agent_register.py case — pin coords saved but reverse-geocode skipped.
    Helper doesn't read raw lat/lng, only the reverse-geocoded fields, so it falls
    through to text/legacy."""
    row = _row(viloyat="Samarqand", tuman="Pastdarg'om tuman")
    # GPS coords would be present in a real row but we don't display them directly
    assert get_display_location(row) == "Samarqand → Pastdarg'om tuman"


def test_partial_gps_district_only():
    row = _row(gps_district="Samarqand shahri")
    assert get_display_location(row) == "Samarqand shahri"


def test_partial_text_moljal_only():
    row = _row(moljal="Juma")
    assert get_display_location(row) == "Juma"


# --- Whitespace handling ---

def test_empty_strings_treated_as_null():
    row = _row(viloyat="", tuman="   ", moljal=None, location="Real fallback")
    assert get_display_location(row) == "Real fallback"


def test_whitespace_trimmed_from_values():
    row = _row(viloyat="  Samarqand  ", tuman="Tuman  ")
    assert get_display_location(row) == "Samarqand → Tuman"


# --- Row types ---

def test_accepts_sqlite_row_like_object():
    """Helper must work with sqlite3.Row (dict-like via __getitem__, no .get)."""
    class FakeSqliteRow:
        def __init__(self, data):
            self._data = data
        def __getitem__(self, key):
            return self._data.get(key)
    row = FakeSqliteRow({"gps_region": "R", "gps_district": "D"})
    assert get_display_location(row) == "R → D"


# --- backfill_text_from_gps (Layer 2 write-side helper) ---

@pytest.fixture
def db():
    """Minimal allowed_clients table for backfill tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE allowed_clients (
            id INTEGER PRIMARY KEY,
            viloyat TEXT, tuman TEXT, moljal TEXT
        )
    """)
    yield conn
    conn.close()


def _select(conn, cid):
    return dict(conn.execute(
        "SELECT viloyat, tuman, moljal FROM allowed_clients WHERE id = ?", (cid,)
    ).fetchone())


def test_backfill_fills_empty_columns(db):
    db.execute("INSERT INTO allowed_clients (id) VALUES (1)")
    backfill_text_from_gps(db, 1, {"region": "Samarqand viloyati", "district": "Kattaqo'rg'on shahri"})
    row = _select(db, 1)
    assert row["viloyat"] == "Samarqand viloyati"
    assert row["tuman"] == "Kattaqo'rg'on shahri"
    assert row["moljal"] is None  # not backfilled by design


def test_backfill_preserves_existing_non_empty_columns(db):
    db.execute("INSERT INTO allowed_clients (id, viloyat, tuman, moljal) "
               "VALUES (1, 'OperatorViloyat', 'OperatorTuman', 'OperatorMoljal')")
    backfill_text_from_gps(db, 1, {"region": "GpsViloyat", "district": "GpsTuman"})
    row = _select(db, 1)
    assert row["viloyat"] == "OperatorViloyat"  # preserved
    assert row["tuman"] == "OperatorTuman"      # preserved
    assert row["moljal"] == "OperatorMoljal"    # never touched


def test_backfill_treats_empty_string_as_null(db):
    db.execute("INSERT INTO allowed_clients (id, viloyat, tuman) VALUES (1, '', '   ')")
    backfill_text_from_gps(db, 1, {"region": "Reg", "district": "Dist"})
    row = _select(db, 1)
    assert row["viloyat"] == "Reg"
    # Note: NULLIF only checks for exact empty string '', not whitespace.
    # 'tuman' was '   ' which NULLIF won't strip, so it stays.
    assert row["tuman"] == "   "


def test_backfill_partial_geo_only_fills_what_it_has(db):
    db.execute("INSERT INTO allowed_clients (id) VALUES (1)")
    backfill_text_from_gps(db, 1, {"region": "OnlyReg", "district": None})
    row = _select(db, 1)
    assert row["viloyat"] == "OnlyReg"
    assert row["tuman"] is None


def test_backfill_empty_geo_is_noop(db):
    db.execute("INSERT INTO allowed_clients (id) VALUES (1)")
    backfill_text_from_gps(db, 1, {})
    row = _select(db, 1)
    assert row["viloyat"] is None
    assert row["tuman"] is None


def test_backfill_idempotent(db):
    db.execute("INSERT INTO allowed_clients (id) VALUES (1)")
    geo = {"region": "Samarqand", "district": "Kattaqo'rg'on"}
    backfill_text_from_gps(db, 1, geo)
    backfill_text_from_gps(db, 1, geo)
    backfill_text_from_gps(db, 1, geo)
    row = _select(db, 1)
    assert row["viloyat"] == "Samarqand"
    assert row["tuman"] == "Kattaqo'rg'on"
