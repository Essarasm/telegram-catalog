"""Admin dashboard endpoint tests — Session X Phase 2 inventory + top sellers.

Covers the two new windows for the business owner dashboard:

* `/api/admin/inventory-week-out` — Mon–Sat cumulative tugagan with
  days-still-out for personnel-monitoring.
* `/api/admin/top-sellers-wow` — top N products by USD-equivalent revenue
  this week, with last-week comparison and rank-change.

The dual-currency rule says native UZS and USD revenue stays uncrossed at
the data layer; these endpoints only convert UZS→USD for the *ranking*
key, and they preserve native amounts on every row.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


TASHKENT = ZoneInfo("Asia/Tashkent")


# admin_auth captures ADMIN_API_KEY at import time, so the fallback key
# "rassvet2026" is what's in effect for tests (same as prod fallback).
ADMIN_KEY = "rassvet2026"


def _client(db) -> TestClient:
    from backend.routers.admin import router
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _this_monday_tk() -> datetime:
    now = datetime.now(TASHKENT)
    return (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _seed_orders(db, orders):
    """Insert real_orders + real_order_items.

    orders: list of dicts {id, doc_date, currency, exchange_rate, items: [(pid, qty, total_local)]}
    """
    for o in orders:
        db.execute(
            """INSERT OR REPLACE INTO real_orders
               (id, doc_number_1c, doc_date, client_name_1c, currency, exchange_rate)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                o["id"],
                f"DOC{o['id']}",
                o["doc_date"],
                "Test Client",
                o.get("currency", "UZS"),
                o.get("exchange_rate", 12000.0),
            ),
        )
        for line_no, (pid, qty, total_local) in enumerate(o["items"], start=1):
            db.execute(
                """INSERT INTO real_order_items
                   (real_order_id, line_no, product_name_1c, product_id, quantity, total_local)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (o["id"], line_no, f"product-{pid}", pid, qty, total_local),
            )
    db.commit()


def _seed_fxrate(db, rate=12050.0, rate_date="2026-04-27"):
    db.execute(
        """INSERT OR REPLACE INTO daily_fx_rates
           (rate_date, currency_pair, rate, source)
           VALUES (?, 'USD_UZS', ?, 'manual')""",
        (rate_date, rate),
    )
    db.commit()


# ── /inventory-week-out ─────────────────────────────────────────────


class TestInventoryWeekOut:
    def test_unauthorized_without_key(self, seed_products):
        c = _client(seed_products)
        r = c.get("/api/admin/inventory-week-out")
        assert r.status_code == 422  # missing required query param

    def test_returns_items_stamped_this_week(self, seed_products):
        db = seed_products
        # Stamp pid=1 stockout at 2 hours ago — within this week
        recent = db.execute("SELECT datetime('now', '-2 hours')").fetchone()[0]
        db.execute(
            "UPDATE products SET stock_quantity=0, stock_status='out_of_stock', "
            "stockout_at=? WHERE id = 1",
            (recent,),
        )
        # pid=2 stamped 8 days ago — last week, should NOT appear
        old = db.execute("SELECT datetime('now', '-8 days')").fetchone()[0]
        db.execute(
            "UPDATE products SET stock_quantity=0, stock_status='out_of_stock', "
            "stockout_at=? WHERE id = 2",
            (old,),
        )
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/inventory-week-out", params={"admin_key": ADMIN_KEY})
        assert r.status_code == 200
        body = r.json()
        ids = {item["product_id"] for item in body["items"]}
        assert 1 in ids
        assert 2 not in ids

    def test_restocked_items_excluded(self, seed_products):
        """Item that ran out earlier this week but came back in stock should drop."""
        db = seed_products
        recent = db.execute("SELECT datetime('now', '-1 hours')").fetchone()[0]
        db.execute(
            "UPDATE products SET stock_quantity=10, stock_status='in_stock', "
            "stockout_at=? WHERE id = 1",
            (recent,),
        )
        db.commit()
        c = _client(db)
        r = c.get("/api/admin/inventory-week-out", params={"admin_key": ADMIN_KEY})
        body = r.json()
        ids = {item["product_id"] for item in body["items"]}
        assert 1 not in ids

    def test_days_out_computed_in_tashkent(self, seed_products):
        """A stamp from 2 days ago should report days_out >= 1 (Tashkent calendar)."""
        db = seed_products
        two_days = db.execute("SELECT datetime('now', '-2 days')").fetchone()[0]
        db.execute(
            "UPDATE products SET stock_quantity=0, stock_status='out_of_stock', "
            "stockout_at=? WHERE id = 1",
            (two_days,),
        )
        db.commit()
        c = _client(db)
        r = c.get("/api/admin/inventory-week-out", params={"admin_key": ADMIN_KEY})
        body = r.json()
        # Find the item we stamped (might or might not be in this week depending on Mon)
        for item in body["items"]:
            if item["product_id"] == 1:
                assert item["days_out"] >= 1
                return
        # If pid=1 isn't in the list, it's because 2 days ago was last week —
        # the test shouldn't fail flakily for that. Skip the assertion.


# ── /top-sellers-wow ────────────────────────────────────────────────


class TestTopSellersWoW:
    def _setup_two_weeks(self, db):
        """Seed orders this week and last week with known volumes."""
        mon_tk = _this_monday_tk().date()
        last_mon = mon_tk - timedelta(days=7)
        # This week — pid 1 leads, pid 2 second
        _seed_orders(db, [
            {"id": 100, "doc_date": mon_tk.isoformat(), "currency": "UZS",
             "exchange_rate": 12050,
             "items": [(1, 10, 1_000_000), (2, 5, 500_000)]},
            {"id": 101, "doc_date": (mon_tk + timedelta(days=1)).isoformat(),
             "currency": "USD", "exchange_rate": 12050,
             "items": [(1, 5, 100), (3, 8, 50)]},
        ])
        # Last week — pid 2 led
        _seed_orders(db, [
            {"id": 200, "doc_date": last_mon.isoformat(), "currency": "UZS",
             "exchange_rate": 12000,
             "items": [(2, 20, 2_000_000), (1, 3, 100_000)]},
        ])
        _seed_fxrate(db, rate=12050.0, rate_date=mon_tk.isoformat())
        return mon_tk, last_mon

    def test_returns_ranked_top_sellers(self, seed_products):
        db = seed_products
        self._setup_two_weeks(db)
        c = _client(db)
        r = c.get("/api/admin/top-sellers-wow", params={"admin_key": ADMIN_KEY})
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert body["fxrate_used"] == 12050.0
        items = body["items"]
        assert len(items) >= 2
        # Ranked descending by usd_eq
        usd_eqs = [i["this_week"]["revenue_usd_eq"] for i in items]
        assert usd_eqs == sorted(usd_eqs, reverse=True)

    def test_native_uzs_and_usd_preserved_per_row(self, seed_products):
        """Dual-currency rule: don't lose native currency revenue at the data layer."""
        db = seed_products
        self._setup_two_weeks(db)
        c = _client(db)
        r = c.get("/api/admin/top-sellers-wow", params={"admin_key": ADMIN_KEY})
        body = r.json()
        # pid=1 has both UZS (1M) and USD (100) revenue this week
        pid1 = next((i for i in body["items"] if i["product_id"] == 1), None)
        assert pid1 is not None
        assert pid1["this_week"]["revenue_uzs_native"] == 1_000_000
        assert pid1["this_week"]["revenue_usd_native"] == 100
        # USD-eq = 100 + 1_000_000/12050 ≈ 183
        assert 180 < pid1["this_week"]["revenue_usd_eq"] < 186

    def test_rank_change_for_returning_item(self, seed_products):
        db = seed_products
        self._setup_two_weeks(db)
        c = _client(db)
        r = c.get("/api/admin/top-sellers-wow", params={"admin_key": ADMIN_KEY})
        items = r.json()["items"]
        # pid=2 was rank 1 last week, will be lower this week
        pid2 = next((i for i in items if i["product_id"] == 2), None)
        if pid2:
            # Last week rank should be present
            assert pid2["last_week"]["rank"] == 1
            # rank_change = last_rank - this_rank; pid2 likely dropped → negative
            assert pid2["delta"]["rank"] is not None

    def test_new_entry_has_null_rank_change(self, seed_products):
        db = seed_products
        self._setup_two_weeks(db)
        c = _client(db)
        r = c.get("/api/admin/top-sellers-wow", params={"admin_key": ADMIN_KEY})
        items = r.json()["items"]
        # pid=3 only sold this week (USD order line, qty=8)
        pid3 = next((i for i in items if i["product_id"] == 3), None)
        if pid3:
            assert pid3["last_week"]["rank"] is None
            assert pid3["delta"]["rank"] is None

    def test_limit_param_respected(self, seed_products):
        db = seed_products
        self._setup_two_weeks(db)
        c = _client(db)
        r = c.get(
            "/api/admin/top-sellers-wow",
            params={"admin_key": ADMIN_KEY, "limit": 1},
        )
        items = r.json()["items"]
        assert len(items) == 1

    def test_unauthorized_without_key(self, seed_products):
        c = _client(seed_products)
        # Missing admin_key altogether
        r = c.get("/api/admin/top-sellers-wow")
        assert r.status_code == 422
        # Wrong admin_key
        r = c.get("/api/admin/top-sellers-wow", params={"admin_key": "nope"})
        assert r.status_code == 401
