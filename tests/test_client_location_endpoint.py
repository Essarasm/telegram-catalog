"""Regression — GET /api/client-location must return the CLIENT's saved GPS
when the requester is linked to a client, not the requester's own coords.

Before the April 2026 fix the endpoint always read `users.latitude/longitude`
for the requester. When an admin used /testclient to inspect a client, they
saw their own GPS attributed to the client. When an agent relinked from
client A to client B, the agent's users row still held A's coords until the
next successful Yangilash write, so client B looked like client A in the
Cabinet.

The fix: when `users.client_id` is set, prefer `allowed_clients.location`
(stored as "lat,lng|address" by the bot handler) and only fall back to the
requester's own users row when the client-level entry is missing or is the
legacy free-text format from the 1C master import.
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


def _setup_client_row(db, *, client_id: int, client_id_1c: str = "TEST",
                       location: str | None = None) -> None:
    db.execute(
        "INSERT INTO allowed_clients (id, phone_normalized, name, client_id_1c, "
        "source_sheet, status, location) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (client_id, "+998900000000", client_id_1c, client_id_1c,
         "test", "active", location),
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
                       location="39.64550,66.93900|Samarqand, shop")
    # Admin's own GPS is somewhere else entirely (their office).
    _setup_user(db, telegram_id=100, client_id=1,
                 lat=41.3000, lng=69.2000, addr="Admin office")

    resp = _client(db).get("/api/client-location?telegram_id=100")
    assert resp.status_code == 200
    data = resp.json()
    assert data["has_gps"] is True
    assert data["gps"]["latitude"] == pytest.approx(39.64550)
    assert data["gps"]["longitude"] == pytest.approx(66.93900)
    # Address can come from the users-row metadata or from the allowed_clients
    # tail — either way it must not silently drop to empty.
    assert data["gps"]["address"]


def test_client_linked_falls_back_to_user_row_when_client_has_no_gps(db):
    """Legacy allowed_clients.location ("Samarqand shahar, Titova") → fall back."""
    _setup_client_row(db, client_id=2, client_id_1c="Legacy",
                       location="Samarqand shahar, Titova")
    _setup_user(db, telegram_id=200, client_id=2,
                 lat=39.1234, lng=66.5678, addr="From registration")

    resp = _client(db).get("/api/client-location?telegram_id=200")
    data = resp.json()
    assert data["has_gps"] is True
    assert data["gps"]["latitude"] == pytest.approx(39.1234)
    assert data["gps"]["longitude"] == pytest.approx(66.5678)
    assert data["gps"]["address"] == "From registration"


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
    still holds client A's coords from the previous Yangilash. Now linked to
    client B, the Cabinet must read client B's saved location, not the
    agent's stale row.
    """
    _setup_client_row(db, client_id=10, client_id_1c="ClientB",
                       location="40.0,67.5|B shop")
    _setup_user(db, telegram_id=500, client_id=10,
                 lat=39.64550, lng=66.93900, addr="Old — client A's shop")

    resp = _client(db).get("/api/client-location?telegram_id=500")
    data = resp.json()
    assert data["gps"]["latitude"] == pytest.approx(40.0)
    assert data["gps"]["longitude"] == pytest.approx(67.5)
