"""Regression — GET /api/client-location must return the CLIENT's saved GPS
when the requester is linked to a client, not the requester's own coords.

The Apr 2026 fix moved canonical client GPS into dedicated `allowed_clients.gps_*`
columns (see Session M follow-up). The legacy `allowed_clients.location` column
is now treated as free-text only and intentionally NOT consulted by this
endpoint, because importers (1C/CSV/Master) overwrite it freely. Reading from
`gps_*` is immune to those imports.

Resolution order in `get_client_location`:
  1. `allowed_clients.gps_*` — canonical client-level GPS (bot or self-share)
  2. `users.latitude/longitude` — only when (1) is missing
"""
from __future__ import annotations

import os
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _client(db) -> TestClient:
    from backend.routers.locations import client_router
    app = FastAPI()
    app.include_router(client_router)
    return TestClient(app)


def _setup_client_row(
    db,
    *,
    client_id: int,
    client_id_1c: str = "TEST",
    location: str | None = None,
    gps_lat: float | None = None,
    gps_lng: float | None = None,
    gps_address: str = "",
) -> None:
    db.execute(
        "INSERT INTO allowed_clients (id, phone_normalized, name, client_id_1c, "
        "source_sheet, status, location, gps_latitude, gps_longitude, gps_address) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (client_id, "+998900000000", client_id_1c, client_id_1c,
         "test", "active", location, gps_lat, gps_lng, gps_address),
    )
    db.commit()


def _setup_user(db, *, telegram_id: int, client_id: int | None,
                 lat: float | None = None, lng: float | None = None,
                 addr: str = "", region: str = "", district: str = "") -> None:
    db.execute(
        "INSERT INTO users (telegram_id, phone, first_name, is_approved, "
        "client_id, latitude, longitude, location_address, location_region, "
        "location_district, location_updated) "
        "VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, datetime('now'))",
        (telegram_id, "+998900000001", "Tester", client_id,
         lat, lng, addr, region, district),
    )
    db.commit()


def test_client_linked_user_sees_client_gps_not_own(db):
    """Admin/agent linked to a client sees the client's saved coords, not their own."""
    _setup_client_row(db, client_id=1, client_id_1c="Акрам",
                       gps_lat=39.64550, gps_lng=66.93900,
                       gps_address="Samarqand, shop")
    # Admin's own GPS is somewhere else entirely (their office).
    _setup_user(db, telegram_id=100, client_id=1,
                 lat=41.3000, lng=69.2000, addr="Admin office")

    resp = _client(db).get("/api/client-location?telegram_id=100")
    assert resp.status_code == 200
    data = resp.json()
    assert data["has_gps"] is True
    assert data["gps"]["latitude"] == pytest.approx(39.64550)
    assert data["gps"]["longitude"] == pytest.approx(66.93900)
    assert data["gps"]["address"] == "Samarqand, shop"


def test_client_linked_falls_back_to_user_row_when_client_has_no_gps(db):
    """No gps_* on the client → fall back to requester's own users row."""
    _setup_client_row(db, client_id=2, client_id_1c="Legacy",
                       location="Samarqand shahar, Titova")  # legacy free-text
    _setup_user(db, telegram_id=200, client_id=2,
                 lat=39.1234, lng=66.5678, addr="From registration")

    resp = _client(db).get("/api/client-location?telegram_id=200")
    data = resp.json()
    assert data["has_gps"] is True
    assert data["gps"]["latitude"] == pytest.approx(39.1234)
    assert data["gps"]["longitude"] == pytest.approx(66.5678)
    assert data["gps"]["address"] == "From registration"


def test_legacy_location_string_is_ignored_for_gps(db):
    """Even if `allowed_clients.location` carries a stale "lat,lng|addr" string
    from before the schema split, we ignore it — `gps_*` is the only source of
    truth. (Otherwise importers could re-introduce the original bug.)"""
    _setup_client_row(db, client_id=3, client_id_1c="Legacy2",
                       location="39.999,66.999|Stale shop")  # NOT trusted
    _setup_user(db, telegram_id=250, client_id=3,
                 lat=39.1234, lng=66.5678, addr="Fallback")

    resp = _client(db).get("/api/client-location?telegram_id=250")
    data = resp.json()
    assert data["gps"]["latitude"] == pytest.approx(39.1234)
    assert data["gps"]["longitude"] == pytest.approx(66.5678)


def test_unlinked_user_returns_own_gps(db):
    """Regular user with no client_id — existing behaviour unchanged."""
    _setup_user(db, telegram_id=300, client_id=None,
                 lat=38.9, lng=65.1, addr="Home")

    resp = _client(db).get("/api/client-location?telegram_id=300")
    data = resp.json()
    assert data["has_gps"] is True
    assert data["gps"]["latitude"] == pytest.approx(38.9)
    assert data["gps"]["longitude"] == pytest.approx(65.1)


def test_unknown_user_returns_no_location(db):
    resp = _client(db).get("/api/client-location?telegram_id=999999")
    data = resp.json()
    assert data["has_location"] is False
    assert data["has_gps"] is False


def test_client_gps_wins_over_stale_requester_gps(db):
    """Agent relinked from client A (prior save) to client B — must not show A's coords.

    This is the exact shape of the production bug: the agent's users row
    still holds client A's coords. Now linked to client B, the Cabinet must
    read client B's saved location from `gps_*`, not the agent's stale row.
    """
    _setup_client_row(db, client_id=10, client_id_1c="ClientB",
                       gps_lat=40.0, gps_lng=67.5, gps_address="B shop")
    _setup_user(db, telegram_id=500, client_id=10,
                 lat=39.64550, lng=66.93900, addr="Old — client A's shop")

    resp = _client(db).get("/api/client-location?telegram_id=500")
    data = resp.json()
    assert data["gps"]["latitude"] == pytest.approx(40.0)
    assert data["gps"]["longitude"] == pytest.approx(67.5)
