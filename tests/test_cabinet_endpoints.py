"""Cabinet endpoint contract tests — dual-currency UZS/USD shape.

These tests guard against the regression class that produced the 2026-04-23
incident: a frontend chart silently hiding the UZS series because the
backend response shape changed (or the underlying aggregation produced zeros
where real data should exist).

Each test seeds a synthetic client with known per-month UZS/USD totals and
asserts the response payload contains both currency keys for every month
plus the expected non-zero values for the seeded months.

Add a new test here whenever a Cabinet endpoint gains a currency-bearing
field. The existing pre-commit hook runs `pytest tests/`, so any commit
that breaks dual-currency rendering will be blocked at commit time.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _app(db) -> TestClient:
    from backend.routers.cabinet import router
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _ymd(year: int, month: int, day: int = 15) -> str:
    return f"{year:04d}-{month:02d}-{day:02d}"


def _months_back(n: int) -> tuple[int, int]:
    """Return (year, month) for n months before the current month."""
    today = date.today()
    y, m = today.year, today.month - n
    while m <= 0:
        m += 12
        y -= 1
    return y, m


@pytest.fixture
def seed_dual_currency_client(db):
    """Seed one client with mixed UZS-only / USD-only / dual-currency history.

    Returns (telegram_id, client_id, expected_per_month) where
    expected_per_month maps "YYYY-MM" -> {"uzs": int, "usd": float}.

    Layout (relative to today):
      - month -10  : UZS-only shipment (1_000_000 UZS)
      - month  -8  : USD-only shipment (250 USD)
      - month  -5  : dual-currency shipment (500_000 UZS + 100 USD on the
                     SAME doc — mirrors how 1C exports a foreign-priced line
                     with a UZS conversion)
      - month  -2  : another UZS-only shipment (2_500_000 UZS)
    """
    telegram_id = 999001
    client_id = 1

    db.execute(
        "INSERT INTO allowed_clients (id, phone_normalized, name, client_id_1c, "
        "source_sheet, status) VALUES (?, ?, ?, ?, ?, ?)",
        (client_id, "+998900000099", "Test Dual-Currency Client",
         "TEST_DUAL_1C", "test", "active"),
    )
    db.execute(
        "INSERT INTO users (telegram_id, phone, first_name, client_id) "
        "VALUES (?, ?, ?, ?)",
        (telegram_id, "+998900000099", "Tester", client_id),
    )

    seed_plan = [
        # (months_back, doc_no, uzs_total, usd_total)
        (10, "DOC-UZS-A", 1_000_000, 0.0),
        (8,  "DOC-USD-A", 0,         250.0),
        (5,  "DOC-DUAL",  500_000,   100.0),
        (2,  "DOC-UZS-B", 2_500_000, 0.0),
    ]
    expected: dict[str, dict] = {}

    for offset, doc_no, uzs, usd in seed_plan:
        y, m = _months_back(offset)
        doc_date = _ymd(y, m, 15)
        currency = "USD" if usd > 0 and uzs == 0 else "UZS"

        cur = db.execute(
            "INSERT INTO real_orders (doc_number_1c, doc_date, client_name_1c, "
            "client_id, currency, total_sum, total_sum_currency) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (doc_no, doc_date, "TEST_DUAL_1C", client_id, currency, uzs, usd),
        )
        real_order_id = cur.lastrowid
        db.execute(
            "INSERT INTO real_order_items (real_order_id, line_no, "
            "product_name_1c, quantity, price, total_local, total_currency) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (real_order_id, 1, "Test product", 1.0, uzs or usd, uzs, usd),
        )
        expected[f"{y:04d}-{m:02d}"] = {"uzs": uzs, "usd": usd}

    db.commit()
    return telegram_id, client_id, expected


def test_spend_trend_returns_16_months_with_dual_currency_keys(
    db, seed_dual_currency_client
):
    """Every month in the requested window has both total_uzs AND total_usd.

    The 2026-04-23 regression hid the UZS chart because total_uzs was missing
    or zero for months the user expected to see. This test asserts the
    payload contract: every month object has both keys, every time.
    """
    telegram_id, _, _ = seed_dual_currency_client
    resp = _app(db).get(f"/api/cabinet/spend-trend?telegram_id={telegram_id}&months=16")
    assert resp.status_code == 200

    body = resp.json()
    assert body["ok"] is True
    assert body["linked"] is True
    assert len(body["months"]) == 16

    for entry in body["months"]:
        assert "month" in entry, f"month key missing: {entry}"
        assert "total_uzs" in entry, f"total_uzs missing for {entry['month']}"
        assert "total_usd" in entry, f"total_usd missing for {entry['month']}"
        # Even zero-filled months must carry both keys (frontend gates on key
        # presence + value, never on key existence — but if the key vanishes
        # the chart silently disappears).
        assert isinstance(entry["total_uzs"], (int, float))
        assert isinstance(entry["total_usd"], (int, float))


def test_spend_trend_uzs_only_month_has_uzs_nonzero_usd_zero(
    db, seed_dual_currency_client
):
    telegram_id, _, expected = seed_dual_currency_client
    resp = _app(db).get(f"/api/cabinet/spend-trend?telegram_id={telegram_id}&months=16")
    months = {m["month"]: m for m in resp.json()["months"]}

    uzs_only_keys = [k for k, v in expected.items() if v["uzs"] > 0 and v["usd"] == 0]
    assert uzs_only_keys, "fixture must seed at least one UZS-only month"

    for key in uzs_only_keys:
        assert key in months, f"seeded month {key} missing from response"
        assert months[key]["total_uzs"] == expected[key]["uzs"]
        assert months[key]["total_usd"] == 0


def test_spend_trend_usd_only_month_has_usd_nonzero_uzs_zero(
    db, seed_dual_currency_client
):
    telegram_id, _, expected = seed_dual_currency_client
    resp = _app(db).get(f"/api/cabinet/spend-trend?telegram_id={telegram_id}&months=16")
    months = {m["month"]: m for m in resp.json()["months"]}

    usd_only_keys = [k for k, v in expected.items() if v["usd"] > 0 and v["uzs"] == 0]
    assert usd_only_keys, "fixture must seed at least one USD-only month"

    for key in usd_only_keys:
        assert months[key]["total_usd"] == expected[key]["usd"]
        assert months[key]["total_uzs"] == 0


def test_spend_trend_dual_currency_month_has_both_nonzero(
    db, seed_dual_currency_client
):
    telegram_id, _, expected = seed_dual_currency_client
    resp = _app(db).get(f"/api/cabinet/spend-trend?telegram_id={telegram_id}&months=16")
    months = {m["month"]: m for m in resp.json()["months"]}

    dual_keys = [k for k, v in expected.items() if v["uzs"] > 0 and v["usd"] > 0]
    assert dual_keys, "fixture must seed at least one dual-currency month"

    for key in dual_keys:
        assert months[key]["total_uzs"] == expected[key]["uzs"]
        assert months[key]["total_usd"] == expected[key]["usd"]


def test_spend_trend_unlinked_user_returns_empty_months(db):
    """An unknown telegram_id returns the safe empty shape, not an error."""
    resp = _app(db).get("/api/cabinet/spend-trend?telegram_id=42424242&months=16")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["linked"] is False
    assert body["months"] == []


def test_activity_summary_lifetime_dual_currency_keys(
    db, seed_dual_currency_client
):
    """Lifetime aggregates carry both currencies — same shape contract."""
    telegram_id, _, expected = seed_dual_currency_client
    resp = _app(db).get(f"/api/cabinet/activity-summary?telegram_id={telegram_id}")
    assert resp.status_code == 200

    body = resp.json()
    assert body["ok"] is True
    assert body["linked"] is True
    lifetime = body["summary"]["lifetime"]

    for key in ("total_uzs", "total_usd", "avg_order_uzs", "avg_order_usd"):
        assert key in lifetime, f"lifetime.{key} missing"

    expected_uzs_total = sum(v["uzs"] for v in expected.values())
    expected_usd_total = sum(v["usd"] for v in expected.values())
    assert lifetime["total_uzs"] == expected_uzs_total
    assert lifetime["total_usd"] == expected_usd_total
    assert lifetime["total_orders"] == len(expected)


def test_top_products_returns_separate_uzs_and_usd_lists(
    db, seed_dual_currency_client
):
    """top_uzs and top_usd are independent lists — both must be present."""
    telegram_id, _, _ = seed_dual_currency_client
    resp = _app(db).get(f"/api/cabinet/top-products?telegram_id={telegram_id}&limit=5")
    assert resp.status_code == 200

    body = resp.json()
    assert body["ok"] is True
    assert body["linked"] is True
    assert "top_uzs" in body, "top_uzs list missing"
    assert "top_usd" in body, "top_usd list missing"
    assert isinstance(body["top_uzs"], list)
    assert isinstance(body["top_usd"], list)
    assert len(body["top_uzs"]) > 0, "UZS-bearing fixture rows should produce top_uzs"
    assert len(body["top_usd"]) > 0, "USD-bearing fixture rows should produce top_usd"


def test_client_info_resolves_linked_client(db, seed_dual_currency_client):
    """Linked user gets back the client identity with 1C name."""
    telegram_id, client_id, _ = seed_dual_currency_client
    resp = _app(db).get(f"/api/cabinet/client-info?telegram_id={telegram_id}")
    assert resp.status_code == 200

    body = resp.json()
    assert body["ok"] is True
    assert body["client"] is not None
    assert body["client"]["id"] == client_id
    assert body["client"]["client_id_1c"] == "TEST_DUAL_1C"
    # Non-agent must not receive the phone field
    assert "phone" not in body["client"]


def test_client_info_unlinked_returns_null(db):
    resp = _app(db).get("/api/cabinet/client-info?telegram_id=42424242")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "client": None}
