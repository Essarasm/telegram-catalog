"""Auth-gate regression tests for endpoints fixed in Error Log #86
(UI_SURFACE_AUTH_DIVERGE #42).

`/api/users/export-map` (bulk PII + GPS) and the three `/api/reports*` admin
endpoints — including the destructive `PATCH .../status` that deletes a catalog
photo — were all unauthenticated. After the fix: a missing key → 422 (required
Query param), a wrong key → 403. These tests fail loudly if any gate regresses,
since the rest of the suite stays green even when an endpoint goes open.
"""
import os

from fastapi import FastAPI
from fastapi.testclient import TestClient

ADMIN_KEY = os.environ["ADMIN_API_KEY"]


def _client(db) -> TestClient:
    from backend.routers.users import router as users_router
    from backend.routers.reports import router as reports_router
    app = FastAPI()
    app.include_router(users_router)
    app.include_router(reports_router)
    return TestClient(app)


def test_export_map_requires_admin_key(db):
    c = _client(db)
    assert c.get("/api/users/export-map").status_code == 422               # missing key
    assert c.get("/api/users/export-map", params={"admin_key": "junk"}).status_code == 403


def test_export_map_valid_key_passes_gate(db):
    # Positive control: a valid key reaches the handler (200 even with no users).
    c = _client(db)
    assert c.get("/api/users/export-map", params={"admin_key": ADMIN_KEY}).status_code == 200


def test_list_reports_requires_admin_key(db):
    c = _client(db)
    assert c.get("/api/reports").status_code == 422
    assert c.get("/api/reports", params={"admin_key": "junk"}).status_code == 403


def test_wrong_photos_requires_admin_key(db):
    c = _client(db)
    assert c.get("/api/reports/wrong-photos").status_code == 422
    assert c.get("/api/reports/wrong-photos", params={"admin_key": "junk"}).status_code == 403


def test_update_report_status_requires_admin_key(db):
    # The destructive one: an anonymous PATCH could mark a wrong_photo 'fixed'
    # and unlink the catalog photo. Gate must reject before any lookup/delete.
    c = _client(db)
    body = {"status": "fixed"}
    assert c.patch("/api/reports/1/status", json=body).status_code == 422            # missing key
    assert c.patch("/api/reports/1/status", params={"admin_key": "junk"},
                   json=body).status_code == 403                                      # wrong key
