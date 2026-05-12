"""Admin Supply dashboard endpoint tests.

Verifies the 6 endpoints under /api/admin/supply/* wire up correctly,
respect admin_key auth, and return the expected JSON shapes.
"""
from __future__ import annotations

import datetime as _dt
import os

from fastapi import FastAPI
from fastapi.testclient import TestClient


ADMIN_KEY = os.environ["ADMIN_API_KEY"]
TODAY = _dt.date(2026, 5, 15)


def _client(db) -> TestClient:
    from backend.routers.admin_supply import router
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _seed_supplier(db, sid: int, name: str):
    db.execute(
        "INSERT OR REPLACE INTO suppliers (id, name_1c, is_active, periods, activity_uzs, activity_usd) "
        "VALUES (?, ?, 1, 12, 0, 0)",
        (sid, name),
    )


def _set_supplier(db, pid: int, sid: int, stock: float = 0):
    db.execute(
        "UPDATE products SET latest_supplier_id = ?, stock_quantity = ? WHERE id = ?",
        (sid, stock, pid),
    )


def _seed_sale(db, ro_id: int, product_id: int, qty: float, doc_date: str):
    db.execute(
        """INSERT OR REPLACE INTO real_orders
              (id, doc_number_1c, doc_date, client_name_1c, currency, total_sum, item_count)
           VALUES (?, ?, ?, 'Test', 'UZS', 0, 1)""",
        (ro_id, f"D-{ro_id}", doc_date),
    )
    db.execute(
        """INSERT INTO real_order_items
              (real_order_id, line_no, product_name_1c, product_id, quantity, price)
           VALUES (?, 1, '', ?, ?, 0)""",
        (ro_id, product_id, qty),
    )


def _seed_supply(db, so_id: int, supplier_name: str, doc_date: str, product_id: int):
    db.execute(
        """INSERT OR REPLACE INTO supply_orders
              (id, doc_number, doc_date, counterparty_name, doc_type, currency)
           VALUES (?, ?, ?, ?, 'supply', 'UZS')""",
        (so_id, f"S-{so_id}", doc_date, supplier_name),
    )
    db.execute(
        """INSERT INTO supply_order_items
              (supply_order_id, line_no, product_name_raw, matched_product_id, quantity, total_local)
           VALUES (?, 1, '', ?, 1.0, 100.0)""",
        (so_id, product_id),
    )


def _seed_demand_signal(db, ds_id: int, product_id: int, qty: int, telegram_id: int, created_at: str):
    db.execute(
        """INSERT OR REPLACE INTO demand_signals
              (id, order_id, order_item_id, product_id, telegram_id, quantity, created_at)
           VALUES (?, 1, 1, ?, ?, ?, ?)""",
        (ds_id, product_id, telegram_id, qty, created_at),
    )


# ── Auth ────────────────────────────────────────────────────────────────

class TestAuth:
    def test_hot_list_rejects_bad_key(self, seed_products):
        c = _client(seed_products)
        assert c.get("/api/admin/supply/hot-list?admin_key=wrong").status_code == 401

    def test_each_endpoint_requires_key(self, seed_products):
        c = _client(seed_products)
        for path in [
            "/api/admin/supply/hot-list",
            "/api/admin/supply/supplier-scoreboard",
            "/api/admin/supply/lost-demand",
            "/api/admin/supply/recent-deliveries",
            "/api/admin/supply/unmapped-count",
            "/api/admin/supply/seasonal-alerts",
        ]:
            assert c.get(f"{path}?admin_key=wrong").status_code == 401


# ── Endpoint shapes ─────────────────────────────────────────────────────

class TestHotList:
    def test_empty_db_returns_empty_list(self, seed_products):
        c = _client(seed_products)
        r = c.get(f"/api/admin/supply/hot-list?admin_key={ADMIN_KEY}")
        assert r.status_code == 200
        body = r.json()
        assert "items" in body
        assert "total_with_buy" in body
        assert isinstance(body["items"], list)

    def test_includes_supplier_name(self, seed_products):
        db = seed_products
        _seed_supplier(db, 100, "ACME")
        _set_supplier(db, 1, 100, stock=0)
        today = _dt.date.today()
        for i in range(60):
            _seed_sale(db, 1000 + i, 1, 2.0,
                       (today - _dt.timedelta(days=i)).isoformat())
        db.commit()

        c = _client(db)
        r = c.get(f"/api/admin/supply/hot-list?admin_key={ADMIN_KEY}&limit=10")
        body = r.json()
        assert body["items"], "expected at least one item"
        item = body["items"][0]
        assert item["supplier_name"] == "ACME"
        assert item["status"] == "stockout"
        assert item["suggested_buy"] > 0


class TestSupplierScoreboard:
    def test_supplier_with_products_appears(self, seed_products):
        db = seed_products
        _seed_supplier(db, 200, "BRAVO")
        _set_supplier(db, 1, 200, stock=10)
        _set_supplier(db, 2, 200, stock=0)
        _seed_supply(db, 500, "BRAVO", "2026-04-01", 1)
        _seed_supply(db, 501, "BRAVO", "2026-04-15", 1)
        _seed_supply(db, 502, "BRAVO", "2026-04-29", 1)
        db.commit()

        c = _client(db)
        r = c.get(f"/api/admin/supply/supplier-scoreboard?admin_key={ADMIN_KEY}")
        body = r.json()
        bravo = next((s for s in body["suppliers"] if s["supplier_name"] == "BRAVO"), None)
        assert bravo is not None
        assert bravo["product_count"] == 2
        assert bravo["last_supply_date"] == "2026-04-29"
        assert bravo["supply_events_ytd"] == 3

    def test_unmapped_bucket_included(self, seed_products):
        c = _client(seed_products)
        r = c.get(f"/api/admin/supply/supplier-scoreboard?admin_key={ADMIN_KEY}")
        body = r.json()
        unmapped = next((s for s in body["suppliers"] if s["supplier_id"] is None), None)
        assert unmapped is not None
        assert unmapped["supplier_name"].startswith("(noma")


class TestLostDemand:
    def test_returns_top_signals(self, seed_products):
        db = seed_products
        _seed_demand_signal(db, 1, 1, 5, 100, "2026-05-10 12:00:00")
        _seed_demand_signal(db, 2, 1, 3, 101, "2026-05-11 12:00:00")
        _seed_demand_signal(db, 3, 2, 1, 100, "2026-05-12 12:00:00")
        db.commit()

        c = _client(db)
        r = c.get(f"/api/admin/supply/lost-demand?admin_key={ADMIN_KEY}&days=60&limit=10")
        body = r.json()
        items = body["items"]
        assert items[0]["product_id"] == 1
        assert items[0]["lost_qty"] == 8
        assert items[0]["unique_clients"] == 2

    def test_excludes_inactive_products(self, seed_products):
        db = seed_products
        db.execute("UPDATE products SET is_active = 0 WHERE id = 1")
        _seed_demand_signal(db, 1, 1, 99, 100, "2026-05-10 12:00:00")
        db.commit()
        c = _client(db)
        r = c.get(f"/api/admin/supply/lost-demand?admin_key={ADMIN_KEY}")
        assert all(it["product_id"] != 1 for it in r.json()["items"])


class TestRecentDeliveries:
    def test_returns_supply_orders_in_window(self, seed_products):
        db = seed_products
        today = _dt.date.today()
        _seed_supply(db, 1, "ALPHA", today.isoformat(), 1)
        _seed_supply(db, 2, "ALPHA",
                     (today - _dt.timedelta(days=5)).isoformat(), 1)
        # Out-of-window event
        _seed_supply(db, 3, "ALPHA",
                     (today - _dt.timedelta(days=400)).isoformat(), 1)
        db.commit()

        c = _client(db)
        r = c.get(f"/api/admin/supply/recent-deliveries?admin_key={ADMIN_KEY}&days=30")
        body = r.json()
        doc_dates = [d["doc_date"] for d in body["deliveries"]]
        assert today.isoformat() in doc_dates
        assert (today - _dt.timedelta(days=400)).isoformat() not in doc_dates


class TestUnmappedCount:
    def test_unmapped_count_matches_db(self, seed_products):
        # All 5 seed_products start unmapped
        c = _client(seed_products)
        r = c.get(f"/api/admin/supply/unmapped-count?admin_key={ADMIN_KEY}")
        body = r.json()
        assert body["total_active"] == 5
        assert body["mapped"] == 0
        assert body["unmapped"] == 5
        assert body["pct_mapped"] == 0.0

    def test_mapped_products_drop_off_count(self, seed_products):
        db = seed_products
        _seed_supplier(db, 300, "MAPPED")
        _set_supplier(db, 1, 300, stock=5)
        db.commit()
        c = _client(db)
        body = c.get(f"/api/admin/supply/unmapped-count?admin_key={ADMIN_KEY}").json()
        assert body["mapped"] == 1
        assert body["unmapped"] == 4


class TestSeasonalAlerts:
    def test_empty_when_no_yoy_data(self, seed_products):
        c = _client(seed_products)
        r = c.get(f"/api/admin/supply/seasonal-alerts?admin_key={ADMIN_KEY}")
        body = r.json()
        assert body["peak"] == []
        assert body["low"] == []
        assert "peak_threshold" in body
        assert "low_threshold" in body
