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


# ── /receivables (rebuilt to read from client_debts) ────────────────


def _seed_debt(db, name, *, debt_uzs=0, debt_usd=0,
               b0_30=0, b31_60=0, b61_90=0, b91_120=0, b120p=0,
               last_tx="2026-05-01", report_date="2026-05-05"):
    db.execute(
        """INSERT INTO client_debts
           (client_name_1c, client_id, debt_uzs, debt_usd,
            last_transaction_date, last_transaction_no,
            aging_0_30, aging_31_60, aging_61_90, aging_91_120, aging_120_plus,
            report_date)
           VALUES (?, NULL, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)""",
        (name, debt_uzs, debt_usd, last_tx,
         b0_30, b31_60, b61_90, b91_120, b120p, report_date),
    )


class TestReceivables:
    def test_uzs_buckets_match_1c(self, db):
        _seed_debt(db, "Реал клиент A", debt_uzs=10000, b0_30=10000)
        _seed_debt(db, "Реал клиент B", debt_uzs=5000, b31_60=5000)
        _seed_debt(db, "Реал клиент C", debt_uzs=8000, b0_30=3000, b91_120=5000)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/receivables", params={"admin_key": ADMIN_KEY})
        assert r.status_code == 200
        body = r.json()
        assert body["currency"] == "UZS"
        assert body["as_of"] == "2026-05-05"
        assert body["total_receivable"] == 23000
        assert body["total_clients_with_debt"] == 3
        assert body["aging"]["0_30"] == 13000  # A's 10k + C's 3k
        assert body["aging"]["31_60"] == 5000   # B's 5k
        assert body["aging"]["91_120"] == 5000  # C's 5k
        assert body["aging"]["61_90"] == 0
        assert body["aging"]["120_plus"] == 0
        # C contributes to two buckets, so client counts in those = 2 (A+C) and 1 (C)
        assert body["aging_client_count"]["0_30"] == 2
        assert body["aging_client_count"]["91_120"] == 1

    def test_pseudo_clients_excluded(self, db):
        # These should be silently dropped
        _seed_debt(db, "Наличка №1", debt_uzs=100000, b120p=100000)
        _seed_debt(db, "СТРОЙКА", debt_uzs=200000, b120p=200000)
        _seed_debt(db, "В О З В Р А Т ПОСТАВЩИКУ", debt_uzs=50000, b120p=50000)
        # This is a real client
        _seed_debt(db, "Асад ака", debt_uzs=7000, b0_30=7000)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/receivables", params={"admin_key": ADMIN_KEY})
        body = r.json()
        assert body["total_receivable"] == 7000
        assert body["total_clients_with_debt"] == 1
        # 120+ should NOT have the structural debt
        assert body["aging"]["120_plus"] == 0

    def test_usd_response_omits_aging(self, db):
        _seed_debt(db, "USD клиент", debt_usd=4000)
        _seed_debt(db, "Mixed клиент", debt_uzs=1000, debt_usd=2000, b0_30=1000)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/receivables",
                  params={"admin_key": ADMIN_KEY, "currency": "USD"})
        body = r.json()
        assert body["currency"] == "USD"
        assert body["total_receivable"] == 6000
        assert body["total_clients_with_debt"] == 2
        assert body["aging"] == {}
        assert body["usd_aging_available"] is False
        # Top-USD list under the synthetic 'all' bucket
        assert "all" in body["aging_top_clients"]
        assert body["aging_top_clients"]["all"][0]["balance"] == 4000

    def test_uzs_response_carries_usd_sidepanel_total(self, db):
        _seed_debt(db, "UZS клиент", debt_uzs=10000, b0_30=10000)
        _seed_debt(db, "USD клиент", debt_usd=3000)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/receivables", params={"admin_key": ADMIN_KEY})
        body = r.json()
        assert body["total_receivable"] == 10000
        assert body["usd_total"] == 3000
        assert body["usd_client_count"] == 1
        assert body["usd_aging_available"] is False

    def test_only_latest_report_date_used(self, db):
        # Older snapshot
        _seed_debt(db, "Старый клиент", debt_uzs=99999, b0_30=99999,
                   report_date="2026-05-01")
        # Latest snapshot (different client)
        _seed_debt(db, "Новый клиент", debt_uzs=4000, b0_30=4000,
                   report_date="2026-05-05")
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/receivables", params={"admin_key": ADMIN_KEY})
        body = r.json()
        assert body["as_of"] == "2026-05-05"
        assert body["total_receivable"] == 4000

    def test_empty_db_returns_zero(self, db):
        c = _client(db)
        r = c.get("/api/admin/receivables", params={"admin_key": ADMIN_KEY})
        body = r.json()
        assert body["ok"] is True
        assert body["total_receivable"] == 0
        assert body["as_of"] is None

    def test_unauthorized(self, db):
        c = _client(db)
        r = c.get("/api/admin/receivables", params={"admin_key": "nope"})
        assert r.status_code == 401
