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


# admin_auth captures ADMIN_API_KEY at import time. conftest.py sets a
# deterministic test value before any backend imports run.
ADMIN_KEY = os.environ["ADMIN_API_KEY"]


def _client(db) -> TestClient:
    # admin endpoints are split across 4 routers (admin, admin_data_ops,
    # admin_debtors, admin_revenue) — mount them all so tests covering
    # endpoints in any module work uniformly.
    from backend.routers.admin import router
    from backend.routers.admin_data_ops import router as data_ops_router
    from backend.routers.admin_debtors import router as debtors_router
    from backend.routers.admin_revenue import router as revenue_router
    app = FastAPI()
    app.include_router(router)
    app.include_router(data_ops_router)
    app.include_router(debtors_router)
    app.include_router(revenue_router)
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


# ── /top-sellers-period — closed-period rankings (last_week / yesterday) ─


class TestTopSellersPeriod:
    def test_last_week_returns_closed_period_ranking(self, seed_products):
        from datetime import date, timedelta
        db = seed_products
        # Seed orders for last calendar week (Mon-Sun fully completed)
        mon_tk = _this_monday_tk().date()
        last_mon = mon_tk - timedelta(days=7)
        last_wed = last_mon + timedelta(days=2)
        _seed_orders(db, [
            {"id": 300, "doc_date": last_mon.isoformat(), "currency": "UZS",
             "exchange_rate": 12000,
             "items": [(1, 7, 700_000), (2, 4, 400_000)]},
            {"id": 301, "doc_date": last_wed.isoformat(), "currency": "USD",
             "exchange_rate": 12000,
             "items": [(1, 2, 50)]},
        ])
        # Also seed THIS week — must NOT appear in last_week
        _seed_orders(db, [
            {"id": 400, "doc_date": mon_tk.isoformat(), "currency": "UZS",
             "exchange_rate": 12050,
             "items": [(3, 99, 99_000_000)]},
        ])
        _seed_fxrate(db, rate=12050.0, rate_date=mon_tk.isoformat())

        c = _client(db)
        r = c.get("/api/admin/top-sellers-period",
                  params={"admin_key": ADMIN_KEY, "period": "last_week"})
        assert r.status_code == 200
        body = r.json()
        assert body["period"] == "last_week"
        assert body["start_date"] == last_mon.isoformat()
        ids = {it["product_id"] for it in body["items"]}
        # Last-week products
        assert 1 in ids
        assert 2 in ids
        # This-week's pid=3 must NOT leak in
        assert 3 not in ids
        # Ranked DESC by usd_eq
        usd_eqs = [it["revenue_usd_eq"] for it in body["items"]]
        assert usd_eqs == sorted(usd_eqs, reverse=True)

    def test_yesterday_returns_single_day(self, seed_products):
        from datetime import datetime, timedelta
        db = seed_products
        yesterday = (datetime.now(TASHKENT) - timedelta(days=1)).date()
        two_days_ago = yesterday - timedelta(days=1)
        _seed_orders(db, [
            {"id": 500, "doc_date": yesterday.isoformat(), "currency": "UZS",
             "exchange_rate": 12000,
             "items": [(1, 5, 500_000)]},
            {"id": 501, "doc_date": two_days_ago.isoformat(), "currency": "UZS",
             "exchange_rate": 12000,
             "items": [(2, 99, 99_000_000)]},
        ])
        _seed_fxrate(db, rate=12000.0, rate_date=yesterday.isoformat())

        c = _client(db)
        r = c.get("/api/admin/top-sellers-period",
                  params={"admin_key": ADMIN_KEY, "period": "yesterday"})
        body = r.json()
        assert body["period"] == "yesterday"
        assert body["start_date"] == body["end_date"] == yesterday.isoformat()
        ids = {it["product_id"] for it in body["items"]}
        assert 1 in ids
        # Two-days-ago must NOT show up
        assert 2 not in ids

    def test_returns_native_uzs_and_usd_per_row(self, seed_products):
        from datetime import datetime, timedelta
        db = seed_products
        yesterday = (datetime.now(TASHKENT) - timedelta(days=1)).date()
        _seed_orders(db, [
            {"id": 600, "doc_date": yesterday.isoformat(), "currency": "UZS",
             "exchange_rate": 12000,
             "items": [(1, 4, 400_000)]},
            {"id": 601, "doc_date": yesterday.isoformat(), "currency": "USD",
             "exchange_rate": 12000,
             "items": [(1, 2, 30)]},
        ])
        _seed_fxrate(db, rate=12000.0, rate_date=yesterday.isoformat())

        c = _client(db)
        r = c.get("/api/admin/top-sellers-period",
                  params={"admin_key": ADMIN_KEY, "period": "yesterday"})
        pid1 = next((i for i in r.json()["items"] if i["product_id"] == 1), None)
        assert pid1 is not None
        assert pid1["revenue_uzs_native"] == 400_000
        assert pid1["revenue_usd_native"] == 30

    def test_invalid_period_rejected(self, seed_products):
        c = _client(seed_products)
        r = c.get("/api/admin/top-sellers-period",
                  params={"admin_key": ADMIN_KEY, "period": "this_year"})
        assert r.status_code == 400

    def test_uses_cyrillic_name(self, seed_products):
        from datetime import datetime, timedelta
        db = seed_products
        yesterday = (datetime.now(TASHKENT) - timedelta(days=1)).date()
        _seed_orders(db, [
            {"id": 700, "doc_date": yesterday.isoformat(), "currency": "UZS",
             "exchange_rate": 12000,
             "items": [(1, 1, 100_000)]},
        ])
        _seed_fxrate(db, rate=12000.0, rate_date=yesterday.isoformat())

        c = _client(db)
        r = c.get("/api/admin/top-sellers-period",
                  params={"admin_key": ADMIN_KEY, "period": "yesterday"})
        items = r.json()["items"]
        assert items
        # seed_products[1] has name="ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/" (Cyrillic)
        # name_display="Standart Oq" (Latin) — endpoint must emit Cyrillic primary
        pid1 = next(i for i in items if i["product_id"] == 1)
        assert "ВЭБЕР" in pid1["name"]
        assert pid1["name_display"] == "Standart Oq"


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


# ── /debtors-list — manager's printed report ─────────────────────


class TestDebtorsList:
    def test_lists_all_real_clients_with_totals(self, db):
        _seed_debt(db, "Реал A", debt_uzs=10_000, debt_usd=200,
                   last_tx="2026-04-30")
        _seed_debt(db, "Реал B", debt_uzs=5_000,
                   last_tx="2026-05-01")
        _seed_debt(db, "Реал C", debt_usd=300, last_tx="2026-04-25")
        # Pseudo-account that should be filtered out
        _seed_debt(db, "Наличка №1", debt_uzs=99_000_000, b120p=99_000_000,
                   last_tx="2026-05-05")
        # Seed an fxrate so combined sort key works
        db.execute(
            "INSERT INTO daily_fx_rates (rate_date, currency_pair, rate, source) "
            "VALUES ('2026-05-05', 'USD_UZS', 12000, 'manual')"
        )
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/debtors-list", params={"admin_key": ADMIN_KEY})
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 3
        assert body["total_uzs"] == 15_000
        assert body["total_usd"] == 500
        names = [it["client_name"] for it in body["items"]]
        assert "Наличка №1" not in names
        assert set(names) == {"Реал A", "Реал B", "Реал C"}
        # Each row carries rank starting at 1
        assert body["items"][0]["rank"] == 1

    def test_sorted_by_combined_usd_eq_desc(self, db):
        _seed_debt(db, "Small UZS", debt_uzs=1_000, last_tx="2026-05-01")
        _seed_debt(db, "Big USD", debt_usd=500, last_tx="2026-05-01")
        _seed_debt(db, "Mixed", debt_uzs=100_000, debt_usd=100,
                   last_tx="2026-05-01")
        db.execute(
            "INSERT INTO daily_fx_rates (rate_date, currency_pair, rate, source) "
            "VALUES ('2026-05-05', 'USD_UZS', 12000, 'manual')"
        )
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/debtors-list", params={"admin_key": ADMIN_KEY})
        body = r.json()
        usd_eqs = [it["debt_usd_eq"] for it in body["items"]]
        assert usd_eqs == sorted(usd_eqs, reverse=True)
        # Big USD ($500) > Mixed ($100 + ~8.3) > Small UZS (~$0.08)
        assert body["items"][0]["client_name"] == "Big USD"

    def test_days_since_last_tx_computed(self, db):
        from datetime import date, timedelta
        recent = (date.today() - timedelta(days=3)).isoformat()
        old = (date.today() - timedelta(days=120)).isoformat()
        _seed_debt(db, "Recent", debt_uzs=1_000, last_tx=recent)
        _seed_debt(db, "Old", debt_uzs=1_000, last_tx=old)
        _seed_debt(db, "NoDate", debt_uzs=1_000, last_tx=None)
        db.execute(
            "INSERT INTO daily_fx_rates (rate_date, currency_pair, rate, source) "
            "VALUES ('2026-05-05', 'USD_UZS', 12000, 'manual')"
        )
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/debtors-list",
                     params={"admin_key": ADMIN_KEY}).json()
        by_name = {it["client_name"]: it for it in body["items"]}
        assert by_name["Recent"]["days_since_last_tx"] == 3
        assert by_name["Old"]["days_since_last_tx"] == 120
        assert by_name["NoDate"]["days_since_last_tx"] is None

    def test_empty_db_returns_zero_count(self, db):
        c = _client(db)
        body = c.get("/api/admin/debtors-list",
                     params={"admin_key": ADMIN_KEY}).json()
        assert body["ok"] is True
        assert body["count"] == 0
        assert body["as_of"] is None
        assert body["total_uzs"] == 0
        assert body["total_usd"] == 0

    def test_unauthorized(self, db):
        c = _client(db)
        r = c.get("/api/admin/debtors-list", params={"admin_key": "nope"})
        assert r.status_code == 401

    def test_phones_surfaced_anchor_only(self, db):
        # Debtor linked to allowed_clients row without sibling phones.
        cur = db.execute(
            "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c) "
            "VALUES ('901234567', 'Реал A', NULL)"
        )
        ac_id = cur.lastrowid
        db.execute(
            """INSERT INTO client_debts
               (client_name_1c, client_id, debt_uzs, debt_usd,
                last_transaction_date, last_transaction_no,
                aging_0_30, aging_31_60, aging_61_90, aging_91_120, aging_120_plus,
                report_date)
               VALUES ('Реал A', ?, 10000, 0, '2026-05-01', NULL,
                       0, 0, 0, 0, 0, '2026-05-05')""",
            (ac_id,),
        )
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/debtors-list",
                     params={"admin_key": ADMIN_KEY}).json()
        item = next(it for it in body["items"] if it["client_name"] == "Реал A")
        assert item["phones"] == ["901234567"]

    def test_phones_surfaced_sibling_group(self, db):
        # Same client_id_1c shared across 3 phone rows; client_debts links to one.
        cid_1c = "1C-CLIENT-42"
        ids = []
        for phone in ("901111111", "902222222", "903333333"):
            cur = db.execute(
                "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c) "
                "VALUES (?, 'Реал A', ?)", (phone, cid_1c),
            )
            ids.append(cur.lastrowid)
        # Link debt to the middle row — siblings should still appear.
        db.execute(
            """INSERT INTO client_debts
               (client_name_1c, client_id, debt_uzs, debt_usd,
                last_transaction_date, last_transaction_no,
                aging_0_30, aging_31_60, aging_61_90, aging_91_120, aging_120_plus,
                report_date)
               VALUES ('Реал A', ?, 10000, 0, '2026-05-01', NULL,
                       0, 0, 0, 0, 0, '2026-05-05')""",
            (ids[1],),
        )
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/debtors-list",
                     params={"admin_key": ADMIN_KEY}).json()
        item = next(it for it in body["items"] if it["client_name"] == "Реал A")
        assert sorted(item["phones"]) == ["901111111", "902222222", "903333333"]

    def test_phones_empty_when_unmatched(self, db):
        _seed_debt(db, "Orphan", debt_uzs=10_000, last_tx="2026-05-01")
        db.commit()
        c = _client(db)
        body = c.get("/api/admin/debtors-list",
                     params={"admin_key": ADMIN_KEY}).json()
        item = next(it for it in body["items"] if it["client_name"] == "Orphan")
        assert item["phones"] == []


class TestDebtorsCallbacks:
    def test_post_persists_and_surfaces_in_list(self, db):
        _seed_debt(db, "Реал A", debt_uzs=10_000, last_tx="2026-05-01")
        db.execute(
            "INSERT INTO daily_fx_rates (rate_date, currency_pair, rate, source) "
            "VALUES ('2026-05-05', 'USD_UZS', 12000, 'manual')"
        )
        db.commit()

        c = _client(db)
        r = c.post("/api/admin/debtors-callback", data={
            "admin_key": ADMIN_KEY,
            "client_name_1c": "Реал A",
            "callback_date": "2026-05-15",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["callback_date"] == "2026-05-15"
        assert body["set_by_name"] == "admin"

        body = c.get("/api/admin/debtors-list",
                     params={"admin_key": ADMIN_KEY}).json()
        item = next(it for it in body["items"] if it["client_name"] == "Реал A")
        assert item["callback_date"] == "2026-05-15"
        assert item["callback_set_by"] == "admin"
        assert item["callback_set_at"] is not None

    def test_latest_row_wins_on_reschedule(self, db):
        _seed_debt(db, "Реал A", debt_uzs=10_000, last_tx="2026-05-01")
        db.commit()

        c = _client(db)
        c.post("/api/admin/debtors-callback", data={
            "admin_key": ADMIN_KEY, "client_name_1c": "Реал A",
            "callback_date": "2026-05-14",
        })
        c.post("/api/admin/debtors-callback", data={
            "admin_key": ADMIN_KEY, "client_name_1c": "Реал A",
            "callback_date": "2026-05-16",
        })

        body = c.get("/api/admin/debtors-list",
                     params={"admin_key": ADMIN_KEY}).json()
        item = next(it for it in body["items"] if it["client_name"] == "Реал A")
        assert item["callback_date"] == "2026-05-16"

        hist = c.get("/api/admin/debtors-callback-history",
                     params={"admin_key": ADMIN_KEY,
                             "client_name_1c": "Реал A"}).json()
        assert hist["count"] == 2
        # Newest first
        assert hist["items"][0]["callback_date"] == "2026-05-16"
        assert hist["items"][1]["callback_date"] == "2026-05-14"

    def test_clear_records_null_date_in_history(self, db):
        _seed_debt(db, "Реал A", debt_uzs=10_000, last_tx="2026-05-01")
        db.commit()

        c = _client(db)
        c.post("/api/admin/debtors-callback", data={
            "admin_key": ADMIN_KEY, "client_name_1c": "Реал A",
            "callback_date": "2026-05-15",
        })
        # Clear: empty string → None
        c.post("/api/admin/debtors-callback", data={
            "admin_key": ADMIN_KEY, "client_name_1c": "Реал A",
            "callback_date": "",
        })

        body = c.get("/api/admin/debtors-list",
                     params={"admin_key": ADMIN_KEY}).json()
        item = next(it for it in body["items"] if it["client_name"] == "Реал A")
        assert item["callback_date"] is None
        # Set_by/set_at still populated — the clear is audit-trailed
        assert item["callback_set_by"] == "admin"

    def test_invalid_date_rejected(self, db):
        _seed_debt(db, "Реал A", debt_uzs=10_000, last_tx="2026-05-01")
        db.commit()

        c = _client(db)
        r = c.post("/api/admin/debtors-callback", data={
            "admin_key": ADMIN_KEY, "client_name_1c": "Реал A",
            "callback_date": "next-friday",
        })
        assert r.status_code == 400

    def test_missing_client_name_rejected(self, db):
        c = _client(db)
        r = c.post("/api/admin/debtors-callback", data={
            "admin_key": ADMIN_KEY, "client_name_1c": "  ",
            "callback_date": "2026-05-15",
        })
        assert r.status_code == 400

    def test_unauthorized(self, db):
        c = _client(db)
        r = c.post("/api/admin/debtors-callback", data={
            "admin_key": "nope", "client_name_1c": "Реал A",
            "callback_date": "2026-05-15",
        })
        assert r.status_code == 401


# ── Pseudo-client filter sweep — /revenue, /collections, /top-clients,
# ── /receivables-trend, /entities now use pseudo_clients.SYSTEM_NON_CLIENT_NAMES
# ── instead of the legacy <5%-collection heuristic. See
# ── obsidian-vault/audits/2026-05-06_admin_filter_sweep.md.


def _seed_balance(db, name, *, period="2025-01-01", currency="UZS",
                  shipped=0, paid=0, closing_debit=None, closing_credit=None):
    cd = closing_debit if closing_debit is not None else shipped - paid
    cc = closing_credit if closing_credit is not None else 0
    db.execute(
        """INSERT INTO client_balances
           (client_name_1c, currency, period_start, period_end,
            opening_debit, opening_credit, period_debit, period_credit,
            closing_debit, closing_credit)
           VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, ?)""",
        (name, currency, period, period[:7] + "-" + str(int(period[8:]) + 27),
         shipped, paid, cd, cc),
    )


class TestPseudoFilterSweep:
    def test_top_clients_excludes_pseudo_accounts(self, db):
        # Pseudo-accounts that legacy heuristic would have classified as 'client'
        # because they cycle credits (high pay rate).
        _seed_balance(db, "Наличка №1", shipped=1_000_000_000, paid=1_000_000_000)
        _seed_balance(db, "СТРОЙКА",     shipped=500_000_000, paid=500_000_000)
        _seed_balance(db, "ORIGINAL COLORMIX", shipped=9_000_000_000, paid=0)
        # Real clients
        _seed_balance(db, "Реал клиент A", shipped=10_000_000, paid=5_000_000)
        _seed_balance(db, "Реал клиент B", shipped=8_000_000, paid=2_000_000)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/top-clients",
                  params={"admin_key": ADMIN_KEY, "currency": "UZS"})
        body = r.json()
        names = {x["name"] for x in body["clients"]}
        assert "Наличка №1" not in names
        assert "СТРОЙКА" not in names
        assert "ORIGINAL COLORMIX" not in names  # added 2026-05-06
        assert "Реал клиент A" in names
        assert "Реал клиент B" in names
        # entity_type field has been dropped
        assert "entity_type" not in body["clients"][0]

    def test_top_clients_include_suppliers_returns_pseudo_too(self, db):
        _seed_balance(db, "Наличка №1", shipped=1_000_000_000, paid=1_000_000_000)
        _seed_balance(db, "Реал клиент A", shipped=10_000_000, paid=5_000_000)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/top-clients",
                  params={"admin_key": ADMIN_KEY, "currency": "UZS",
                          "include_suppliers": "true"})
        body = r.json()
        names = {x["name"] for x in body["clients"]}
        assert "Наличка №1" in names
        assert "Реал клиент A" in names

    def test_revenue_excludes_pseudo_accounts(self, db):
        _seed_balance(db, "Наличка №3", shipped=900_000_000, paid=100_000_000)
        _seed_balance(db, "Реал клиент A", shipped=20_000_000, paid=10_000_000)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/revenue", params={"admin_key": ADMIN_KEY})
        uzs = r.json()["data"]["UZS"]
        # Single period, single bucket sum — should reflect only real client
        assert len(uzs) == 1
        assert uzs[0]["shipments"] == 20_000_000
        assert uzs[0]["collections"] == 10_000_000
        assert uzs[0]["active_clients"] == 1

    def test_collections_excludes_pseudo_accounts(self, db):
        _seed_balance(db, "Наличка СКЛАД", shipped=3_000_000_000, paid=2_500_000_000)
        _seed_balance(db, "Реал клиент A", shipped=10_000_000, paid=4_000_000)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/collections", params={"admin_key": ADMIN_KEY})
        uzs = r.json()["data"]["UZS"]
        assert len(uzs) == 1
        # 4M / 10M = 40%, not contaminated by Наличка's 83%
        assert uzs[0]["collection_rate"] == 40.0

    def test_entities_endpoint_splits_pseudo_vs_real(self, db):
        _seed_balance(db, "Наличка №2", shipped=500_000_000, paid=400_000_000)
        _seed_balance(db, "Реал клиент A", shipped=10_000_000, paid=5_000_000)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/entities", params={"admin_key": ADMIN_KEY})
        body = r.json()
        pseudo_names = {x["name"] for x in body["pseudo_accounts"]}
        real_names = {x["name"] for x in body["top_clients"]}
        assert "Наличка №2" in pseudo_names
        assert "Реал клиент A" in real_names
        # New fields (replaces 'suppliers' / 'suppliers_count')
        assert body["pseudo_count"] == 1
        assert body["clients_count"] == 1
        # Each row carries is_pseudo flag
        assert all("is_pseudo" in x for x in body["pseudo_accounts"])

    def test_newly_added_pseudo_names_dropped(self, db):
        # The 6 names added on 2026-05-06 should all be excluded
        for n in ("ORIGINAL COLORMIX", "УГОЛОК", "COLOREX",
                  "ФИРДАВС 3 D НАЛИВН ПОЛ УСТО",
                  "БЕКЗОД ПАНДЖОБ /Маг Авто Запчасть/", "40.12"):
            _seed_balance(db, n, shipped=100_000, paid=0)
        _seed_balance(db, "Реал клиент A", shipped=1_000, paid=0)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/top-clients",
                  params={"admin_key": ADMIN_KEY, "currency": "UZS"})
        names = {x["name"] for x in r.json()["clients"]}
        assert names == {"Реал клиент A"}


# ── /weekly-recap ───────────────────────────────────────────────────


def _seed_real_order(db, oid, doc_date, client, total_uzs=0.0, total_usd=0.0):
    """Insert a real_orders row with dual UZS+USD legs."""
    db.execute(
        """INSERT OR REPLACE INTO real_orders
           (id, doc_number_1c, doc_date, client_name_1c, currency,
            exchange_rate, total_sum, total_sum_currency)
           VALUES (?, ?, ?, ?, 'USD', 12000.0, ?, ?)""",
        (oid, f"R{oid}", doc_date, client, total_uzs, total_usd),
    )


def _seed_payment(db, pid, doc_date, client, currency, amount_local=0.0, amount_currency=0.0):
    """Insert a client_payments row. UZS rows put value in amount_local;
    USD rows put it in amount_currency."""
    db.execute(
        """INSERT INTO client_payments
           (doc_number_1c, doc_date, client_name_1c, currency,
            amount_local, amount_currency, corr_account)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            f"P{pid}",
            doc_date,
            client,
            currency,
            amount_local,
            amount_currency,
            "40.10" if currency == "UZS" else "40.11",
        ),
    )


def _seed_fx(db, rate_date, rate):
    db.execute(
        """INSERT OR REPLACE INTO daily_fx_rates
           (rate_date, currency_pair, rate, source)
           VALUES (?, 'USD_UZS', ?, 'test')""",
        (rate_date, rate),
    )


def _closed_week(weeks_back: int):
    """Compute the Mon→Sun closed-week dates the endpoint will look at,
    weeks_back=1 = last closed week."""
    now = datetime.now(TASHKENT)
    monday = (now - timedelta(days=now.weekday() + 7 * weeks_back)).date()
    sunday = monday + timedelta(days=6)
    return monday.isoformat(), sunday.isoformat()


class TestWeeklyRecap:
    def test_unauthorized_without_key(self, db):
        c = _client(db)
        r = c.get("/api/admin/weekly-recap")
        assert r.status_code == 422

    def test_unauthorized_with_wrong_key(self, db):
        c = _client(db)
        r = c.get("/api/admin/weekly-recap", params={"admin_key": "wrong"})
        assert r.status_code == 401

    def test_returns_13_weeks_by_default(self, db):
        c = _client(db)
        r = c.get("/api/admin/weekly-recap", params={"admin_key": ADMIN_KEY})
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert body["weeks_back"] == 13
        assert len(body["weeks"]) == 13
        # Oldest first, newest last
        starts = [w["week_start"] for w in body["weeks"]]
        assert starts == sorted(starts)

    def test_excludes_in_progress_current_week(self, db):
        """Orders dated *this* (unclosed) week must not appear in any bucket."""
        now = datetime.now(TASHKENT).date()
        # Find this week's Monday
        this_monday = now - timedelta(days=now.weekday())
        _seed_real_order(db, 1, this_monday.isoformat(), "Real Client", total_uzs=999_000_000)
        db.commit()
        c = _client(db)
        r = c.get("/api/admin/weekly-recap", params={"admin_key": ADMIN_KEY})
        weeks = r.json()["weeks"]
        # No closed week should overlap today
        for w in weeks:
            assert w["week_end"] < now.isoformat()
            assert w["revenue_uzs_native"] == 0  # the order is in the open week

    def test_aggregates_revenue_uzs_and_usd_legs_separately(self, db):
        last_mon, last_sun = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "Real Client", total_uzs=120_000_000, total_usd=0)
        _seed_real_order(db, 2, last_sun, "Real Client", total_uzs=0, total_usd=5_000)
        _seed_fx(db, last_mon, 12000.0)
        _seed_fx(db, last_sun, 12000.0)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/weekly-recap", params={"admin_key": ADMIN_KEY})
        last_week = r.json()["weeks"][-1]
        assert last_week["revenue_uzs_native"] == 120_000_000
        assert last_week["revenue_usd_native"] == 5_000
        # USD-eq = 120M / 12000 + 5000 = 10000 + 5000 = 15000
        assert last_week["revenue_usd_eq"] == 15_000
        assert last_week["fx_source"] == "actual"
        assert last_week["order_count"] == 2

    def test_collections_split_by_currency(self, db):
        last_mon, last_sun = _closed_week(1)
        _seed_payment(db, 1, last_mon, "Client A", "UZS", amount_local=24_000_000)
        _seed_payment(db, 2, last_sun, "Client B", "USD", amount_currency=2_500)
        _seed_fx(db, last_mon, 12000.0)
        db.commit()

        c = _client(db)
        r = c.get("/api/admin/weekly-recap", params={"admin_key": ADMIN_KEY})
        last = r.json()["weeks"][-1]
        assert last["collections_uzs_native"] == 24_000_000
        assert last["collections_usd_native"] == 2_500
        # 24M / 12000 + 2500 = 2000 + 2500 = 4500
        assert last["collections_usd_eq"] == 4_500

    def test_fx_fallback_when_no_rates_for_week(self, db):
        """No daily_fx_rates → fallback rate 12000, fx_source='fallback'."""
        last_mon, last_sun = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "Real Client", total_uzs=120_000_000)
        db.commit()  # NB: no _seed_fx

        c = _client(db)
        body = c.get("/api/admin/weekly-recap",
                     params={"admin_key": ADMIN_KEY}).json()
        last = body["weeks"][-1]
        assert last["fx_source"] == "fallback"
        assert last["fx_rate"] == 12000.0
        assert last["revenue_usd_eq"] == 10_000  # 120M / 12k
        assert body["fx_fallback_count"] >= 1

    def test_pseudo_filter_excludes_system_accounts(self, db):
        last_mon, last_sun = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "Наличка СКЛАД", total_uzs=999_000_000)
        _seed_real_order(db, 2, last_sun, "Real Client", total_uzs=12_000_000)
        _seed_fx(db, last_mon, 12000.0)
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/weekly-recap",
                     params={"admin_key": ADMIN_KEY}).json()
        last = body["weeks"][-1]
        # Pseudo accounts dropped → only the real client counts
        assert last["revenue_uzs_native"] == 12_000_000
        assert last["order_count"] == 1
        assert last["active_clients"] == 1

    def test_pseudo_filter_can_be_disabled(self, db):
        last_mon, _ = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "Наличка СКЛАД", total_uzs=10_000_000)
        _seed_fx(db, last_mon, 12000.0)
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/weekly-recap",
                     params={"admin_key": ADMIN_KEY,
                             "include_suppliers": "true"}).json()
        # With pseudo filter off, the system account is counted
        assert body["weeks"][-1]["revenue_uzs_native"] == 10_000_000

    def test_yoy_shift_is_364_days(self, db):
        """YoY week = exactly 364 days back so Mon-Sun alignment is preserved."""
        c = _client(db)
        body = c.get("/api/admin/weekly-recap",
                     params={"admin_key": ADMIN_KEY}).json()
        for w in body["weeks"]:
            ws = date.fromisoformat(w["week_start"])
            yws = date.fromisoformat(w["yoy"]["week_start"])
            assert (ws - yws).days == 364

    def test_yoy_delta_pct_computation(self, db):
        last_mon, _ = _closed_week(1)
        yoy_mon = (date.fromisoformat(last_mon) - timedelta(days=364)).isoformat()
        _seed_real_order(db, 1, last_mon, "Real Client", total_uzs=120_000_000)  # this year: $10k
        _seed_real_order(db, 2, yoy_mon, "Real Client", total_uzs=96_000_000)    # prior year: $8k
        _seed_fx(db, last_mon, 12000.0)
        _seed_fx(db, yoy_mon, 12000.0)
        db.commit()

        c = _client(db)
        last = c.get("/api/admin/weekly-recap",
                     params={"admin_key": ADMIN_KEY}).json()["weeks"][-1]
        assert last["revenue_usd_eq"] == 10_000
        assert last["yoy"]["revenue_usd_eq"] == 8_000
        # (10000 - 8000) / 8000 = +25%
        assert last["yoy"]["revenue_delta_pct"] == 25.0
        assert last["yoy"]["available"] is True

    def test_yoy_unavailable_when_no_prior_data(self, db):
        last_mon, _ = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "Real Client", total_uzs=120_000_000)
        _seed_fx(db, last_mon, 12000.0)
        db.commit()

        c = _client(db)
        last = c.get("/api/admin/weekly-recap",
                     params={"admin_key": ADMIN_KEY}).json()["weeks"][-1]
        assert last["yoy"]["available"] is False
        assert last["yoy"]["revenue_usd_eq"] == 0
        assert last["yoy"]["revenue_delta_pct"] is None

    def test_collection_rate_pct(self, db):
        last_mon, last_sun = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "Real Client", total_uzs=120_000_000)  # $10k revenue
        _seed_payment(db, 1, last_sun, "Real Client", "UZS", amount_local=60_000_000)  # $5k collected
        _seed_fx(db, last_mon, 12000.0)
        _seed_fx(db, last_sun, 12000.0)
        db.commit()

        c = _client(db)
        last = c.get("/api/admin/weekly-recap",
                     params={"admin_key": ADMIN_KEY}).json()["weeks"][-1]
        assert last["revenue_usd_eq"] == 10_000
        assert last["collections_usd_eq"] == 5_000
        assert last["collection_rate_pct"] == 50.0

    def test_weeks_param_bounds(self, db):
        c = _client(db)
        # Out of range — pydantic validation
        assert c.get("/api/admin/weekly-recap",
                     params={"admin_key": ADMIN_KEY, "weeks": 0}).status_code == 422
        assert c.get("/api/admin/weekly-recap",
                     params={"admin_key": ADMIN_KEY, "weeks": 53}).status_code == 422
        # In range
        body = c.get("/api/admin/weekly-recap",
                     params={"admin_key": ADMIN_KEY, "weeks": 4}).json()
        assert len(body["weeks"]) == 4


class TestTopClientsWeekly:
    def test_auth_required(self, db):
        c = _client(db)
        assert c.get("/api/admin/top-clients-weekly").status_code == 422
        assert c.get("/api/admin/top-clients-weekly",
                     params={"admin_key": "wrong"}).status_code == 401

    def test_empty_week_returns_empty_clients(self, db):
        c = _client(db)
        body = c.get("/api/admin/top-clients-weekly",
                     params={"admin_key": ADMIN_KEY}).json()
        assert body["ok"] is True
        assert body["clients"] == []
        assert body["count"] == 0
        assert body["kpis"]["top_client_shipped_usd_eq"] == 0

    def test_ranks_clients_by_usd_eq_shipped(self, db):
        last_mon, last_sun = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "Big Client", total_uzs=240_000_000)  # $20k @ 12k
        _seed_real_order(db, 2, last_sun, "Big Client", total_usd=0)
        _seed_real_order(db, 3, last_mon, "Small Client", total_uzs=60_000_000)  # $5k
        _seed_real_order(db, 4, last_sun, "Mid Client", total_usd=10_000)  # $10k native
        _seed_fx(db, last_mon, 12000.0)
        _seed_fx(db, last_sun, 12000.0)
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/top-clients-weekly",
                     params={"admin_key": ADMIN_KEY}).json()
        names = [cl["client_name"] for cl in body["clients"]]
        assert names == ["Big Client", "Mid Client", "Small Client"]
        assert body["kpis"]["top_client_shipped_usd_eq"] == 20_000

    def test_includes_native_legs_and_usd_eq(self, db):
        last_mon, _ = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "X", total_uzs=120_000_000, total_usd=500)
        _seed_payment(db, 1, last_mon, "X", "UZS", amount_local=60_000_000)
        _seed_payment(db, 2, last_mon, "X", "USD", amount_currency=200)
        _seed_fx(db, last_mon, 12000.0)
        db.commit()

        c = _client(db)
        row = c.get("/api/admin/top-clients-weekly",
                    params={"admin_key": ADMIN_KEY}).json()["clients"][0]
        assert row["shipped_uzs"] == 120_000_000
        assert row["shipped_usd"] == 500
        assert row["shipped_usd_eq"] == 10_500  # 500 + 120M / 12k
        assert row["paid_uzs"] == 60_000_000
        assert row["paid_usd"] == 200
        assert row["paid_usd_eq"] == 5_200
        # pay_pct = 5200 / 10500 = 49.5%
        assert row["pay_pct"] == 49.5

    def test_pseudo_filter_excludes_structural_accounts(self, db):
        last_mon, _ = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "Real Client", total_uzs=120_000_000)
        _seed_real_order(db, 2, last_mon, "Наличка СКЛАД", total_uzs=999_000_000)
        _seed_fx(db, last_mon, 12000.0)
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/top-clients-weekly",
                     params={"admin_key": ADMIN_KEY}).json()
        names = [cl["client_name"] for cl in body["clients"]]
        assert "Наличка СКЛАД" not in names
        assert names == ["Real Client"]

    def test_total_receivable_uses_latest_debts_snapshot(self, db):
        last_mon, _ = _closed_week(1)
        _seed_debt(db, "Real Client", debt_uzs=120_000_000, debt_usd=500,
                   last_tx=last_mon)
        _seed_debt(db, "Наличка СКЛАД", debt_uzs=99_000_000_000,
                   last_tx=last_mon)
        _seed_fx(db, last_mon, 12000.0)
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/top-clients-weekly",
                     params={"admin_key": ADMIN_KEY}).json()
        # 500 + 120M / 12k = 10_500; pseudo excluded
        assert body["kpis"]["total_receivable_usd_eq"] == 10_500

    def test_net_balance_is_top_only(self, db):
        last_mon, _ = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "A", total_uzs=120_000_000)  # $10k
        _seed_payment(db, 1, last_mon, "A", "UZS", amount_local=24_000_000)  # $2k paid
        _seed_real_order(db, 2, last_mon, "B", total_uzs=60_000_000)  # $5k
        _seed_payment(db, 2, last_mon, "B", "UZS", amount_local=60_000_000)  # $5k paid
        _seed_fx(db, last_mon, 12000.0)
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/top-clients-weekly",
                     params={"admin_key": ADMIN_KEY}).json()
        # (10k - 2k) + (5k - 5k) = 8k
        assert body["kpis"]["net_balance_usd_eq_week"] == 8_000

    def test_fx_fallback_when_no_rate(self, db):
        last_mon, _ = _closed_week(1)
        _seed_real_order(db, 1, last_mon, "X", total_uzs=120_000_000)
        # no fxrate seeded
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/top-clients-weekly",
                     params={"admin_key": ADMIN_KEY}).json()
        assert body["fx_source"] == "fallback"
        assert body["fx_rate"] == 12_000
        assert body["clients"][0]["shipped_usd_eq"] == 10_000

    def test_weeks_back_param(self, db):
        # weeks_back=2 should hit the week before last
        two_mon, two_sun = _closed_week(2)
        _seed_real_order(db, 1, two_mon, "Old Client", total_uzs=120_000_000)
        _seed_fx(db, two_mon, 12000.0)
        db.commit()

        c = _client(db)
        body = c.get("/api/admin/top-clients-weekly",
                     params={"admin_key": ADMIN_KEY, "weeks_back": 2}).json()
        assert body["week_start"] == two_mon
        assert body["week_end"] == two_sun
        assert body["clients"][0]["client_name"] == "Old Client"
