"""Tests for backend.services.reorder — /zakazlar engine.

Covers: list_suppliers_with_products, list_supplier_full, compute_supplier_reorder.
Helper coverage (post-v1): median gap, status classifier, lead-time fallbacks,
YoY multiplier, demand-signal augmentation, sort order.
"""
from __future__ import annotations

import datetime as _dt

import pytest

from backend.services.reorder import (
    DEFAULT_GLOBAL_LEAD_TIME_DAYS,
    DEFAULT_REVIEW_PERIOD_DAYS,
    DEFAULT_SAFETY_FACTOR,
    DEFAULT_WINDOW_DAYS,
    MIN_YOY_UNITS,
    _classify_status,
    _median_gap_days,
    compute_supplier_reorder,
    list_supplier_full,
    list_suppliers_with_products,
)


FIXED_TODAY = _dt.date(2026, 5, 15)


def _seed_supplier(db, sid: int, name: str, is_active: int = 1):
    db.execute(
        "INSERT OR REPLACE INTO suppliers (id, name_1c, is_active, periods, activity_uzs, activity_usd) "
        "VALUES (?, ?, ?, 12, 0, 0)",
        (sid, name, is_active),
    )


def _seed_product_stock(db, pid: int, stock: float, supplier_id=None):
    db.execute(
        "UPDATE products SET stock_quantity = ?, latest_supplier_id = ? WHERE id = ?",
        (stock, supplier_id, pid),
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


def _seed_supply(db, so_id: int, supplier_name: str, doc_date: str,
                 product_id: int, qty: float = 1.0):
    db.execute(
        """INSERT OR REPLACE INTO supply_orders
              (id, doc_number, doc_date, counterparty_name, doc_type)
           VALUES (?, ?, ?, ?, 'supply')""",
        (so_id, f"S-{so_id}", doc_date, supplier_name),
    )
    db.execute(
        """INSERT INTO supply_order_items
              (supply_order_id, line_no, product_name_raw, matched_product_id, quantity)
           VALUES (?, 1, '', ?, ?)""",
        (so_id, product_id, qty),
    )


def _seed_demand_signal(db, ds_id: int, product_id: int, qty: int, created_at: str):
    db.execute(
        """INSERT OR REPLACE INTO demand_signals
              (id, order_id, order_item_id, product_id, telegram_id, quantity, created_at)
           VALUES (?, 1, 1, ?, 1, ?, ?)""",
        (ds_id, product_id, qty, created_at),
    )


class TestMedianGapDays:
    def test_returns_none_when_too_few(self):
        assert _median_gap_days([]) is None
        assert _median_gap_days(["2025-01-01"]) is None

    def test_median_of_uniform_gaps(self):
        assert _median_gap_days([
            "2025-01-01", "2025-01-08", "2025-01-15", "2025-01-22"
        ]) == 7.0

    def test_robust_to_outlier(self):
        assert _median_gap_days([
            "2025-01-01", "2025-01-08", "2025-01-15", "2025-04-15"
        ]) == 7.0

    def test_ignores_same_day_duplicates(self):
        assert _median_gap_days([
            "2025-01-01", "2025-01-08", "2025-01-08", "2025-01-15"
        ]) == 7.0


class TestClassifyStatus:
    def test_no_demand_overrides_stock(self):
        assert _classify_status(0.0, 0.0, 0.0, 0.0) == "no_recent_demand"
        assert _classify_status(0.0, 100.0, 0.0, 0.0) == "no_recent_demand"

    def test_stockout_when_demand_and_no_stock(self):
        assert _classify_status(1.0, 0.0, 10.0, 20.0) == "stockout"

    def test_order_now_when_below_reorder_point(self):
        assert _classify_status(1.0, 5.0, 10.0, 20.0) == "order_now"

    def test_order_soon_when_between_reorder_and_target(self):
        assert _classify_status(1.0, 15.0, 10.0, 20.0) == "order_soon"

    def test_ok_when_above_target(self):
        assert _classify_status(1.0, 25.0, 10.0, 20.0) == "ok"


class TestListSuppliers:
    def test_only_active_with_mapped_products_appear(self, seed_products):
        db = seed_products
        _seed_supplier(db, 100, "ALPHA")
        _seed_supplier(db, 101, "BETA")
        _seed_supplier(db, 102, "GAMMA_RETIRED", is_active=0)
        db.execute("UPDATE products SET latest_supplier_id = 100 WHERE id = 1")
        db.execute("UPDATE products SET latest_supplier_id = 102 WHERE id = 2")
        db.commit()

        names = [s["name_1c"] for s in list_suppliers_with_products()]
        assert "ALPHA" in names
        assert "BETA" not in names
        assert "GAMMA_RETIRED" not in names

    def test_unknown_bucket_appears(self, seed_products):
        result = list_suppliers_with_products()
        unknown = [s for s in result if s["id"] is None]
        assert len(unknown) == 1
        assert unknown[0]["product_count"] == 5
        assert unknown[0]["name_1c"].startswith("(noma'lum")

    def test_oos_count_drives_sort_order(self, seed_products):
        db = seed_products
        _seed_supplier(db, 200, "LOW_OOS")
        _seed_supplier(db, 201, "HIGH_OOS")
        db.execute("UPDATE products SET latest_supplier_id = 200, stock_quantity = 5 WHERE id = 1")
        db.execute("UPDATE products SET latest_supplier_id = 200, stock_quantity = 0 WHERE id = 2")
        db.execute("UPDATE products SET latest_supplier_id = 201, stock_quantity = 0 WHERE id = 3")
        db.execute("UPDATE products SET latest_supplier_id = 201, stock_quantity = 0 WHERE id = 4")
        db.commit()
        named = [s for s in list_suppliers_with_products() if s["id"] in (200, 201)]
        assert named[0]["name_1c"] == "HIGH_OOS"


class TestForecastCore:
    def test_global_lead_fallback_no_supply_events(self, seed_products):
        db = seed_products
        _seed_supplier(db, 300, "SUP1")
        _seed_product_stock(db, 1, stock=10, supplier_id=300)
        for i in range(60):
            d = (FIXED_TODAY - _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 1000 + i, 1, 1.0, d)
        db.commit()

        items = list_supplier_full(300, window_days=60, today=FIXED_TODAY)
        assert len(items) == 1
        it = items[0]
        assert it["sold_window"] == 60
        assert it["daily_rate"] == 1.0
        assert it["seasonal_mult"] == 1.0
        assert it["lead_time_days"] == DEFAULT_GLOBAL_LEAD_TIME_DAYS
        assert it["lead_time_source"] == "global"
        assert it["suggested_buy"] == 22
        assert it["status"] == "order_now"

    def test_product_level_lead_time_overrides_global(self, seed_products):
        db = seed_products
        _seed_supplier(db, 310, "SUP_LEAD")
        _seed_product_stock(db, 1, stock=100, supplier_id=310)
        for i, days_back in enumerate([0, 7, 14, 21]):
            d = (FIXED_TODAY - _dt.timedelta(days=days_back + 30)).isoformat()
            _seed_supply(db, 500 + i, "SUP_LEAD", d, 1)
        for i in range(60):
            d = (FIXED_TODAY - _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 2000 + i, 1, 1.0, d)
        db.commit()

        items = list_supplier_full(310, today=FIXED_TODAY)
        it = items[0]
        assert it["lead_time_days"] == 7.0
        assert it["lead_time_source"] == "product"

    def test_supplier_level_lead_time_when_product_thin(self, seed_products):
        db = seed_products
        _seed_supplier(db, 320, "SUP_FALLBACK")
        _seed_product_stock(db, 1, stock=50, supplier_id=320)
        _seed_product_stock(db, 2, stock=50, supplier_id=320)
        _seed_supply(db, 600, "SUP_FALLBACK", "2026-03-01", 1)
        for i, d in enumerate(["2026-02-01", "2026-03-15", "2026-04-29"]):
            _seed_supply(db, 601 + i, "SUP_FALLBACK", d, 2)
        for i in range(60):
            d = (FIXED_TODAY - _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 3000 + i, 1, 1.0, d)
        db.commit()

        items = list_supplier_full(320, today=FIXED_TODAY)
        p1 = next(i for i in items if i["product_id"] == 1)
        assert p1["lead_time_source"] == "supplier"
        assert p1["lead_time_days"] == 28.0

    def test_yoy_seasonal_multiplier_applied(self, seed_products):
        db = seed_products
        _seed_supplier(db, 330, "SUP_YOY")
        _seed_product_stock(db, 1, stock=10, supplier_id=330)
        for i in range(31):
            d = (_dt.date(2025, 5, 1) + _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 4000 + i, 1, 2.0, d)
        for i in range(60):
            d = (_dt.date(2025, 3, 2) + _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 4100 + i, 1, 1.0, d)
        for i in range(60):
            d = (FIXED_TODAY - _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 4200 + i, 1, 1.0, d)
        db.commit()

        items = list_supplier_full(330, window_days=60, today=FIXED_TODAY)
        it = items[0]
        assert it["seasonal_source"] == "yoy"
        assert it["seasonal_mult"] == 2.0
        assert it["seasoned_daily"] == 2.0

    def test_yoy_falls_back_when_under_minimum(self, seed_products):
        db = seed_products
        _seed_supplier(db, 331, "SUP_YOY_THIN")
        _seed_product_stock(db, 1, stock=10, supplier_id=331)
        thin = MIN_YOY_UNITS - 1
        for i in range(thin):
            d = (_dt.date(2025, 5, 1) + _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 5000 + i, 1, 1.0, d)
        for i in range(60):
            d = (FIXED_TODAY - _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 5100 + i, 1, 1.0, d)
        db.commit()

        items = list_supplier_full(331, today=FIXED_TODAY)
        it = items[0]
        assert it["seasonal_source"] == "fallback"
        assert it["seasonal_mult"] == 1.0

    def test_demand_signal_augments_sold_window(self, seed_products):
        db = seed_products
        _seed_supplier(db, 340, "SUP_DS")
        _seed_product_stock(db, 1, stock=0, supplier_id=340)
        for i in range(30):
            d = (FIXED_TODAY - _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 6000 + i, 1, 1.0, d)
        for i in range(30):
            ts = (FIXED_TODAY - _dt.timedelta(days=i)).isoformat() + " 12:00:00"
            _seed_demand_signal(db, 7000 + i, 1, 1, ts)
        db.commit()

        items = list_supplier_full(340, window_days=60, today=FIXED_TODAY)
        it = items[0]
        assert it["sold_window"] == 30
        assert it["demand_signal_qty"] == 30
        assert it["daily_rate"] == 1.0

    def test_status_sort_order(self, seed_products):
        db = seed_products
        _seed_supplier(db, 350, "SUP_SORT")
        _seed_product_stock(db, 1, stock=0, supplier_id=350)
        for i in range(60):
            _seed_sale(db, 8000 + i, 1, 1.0,
                       (FIXED_TODAY - _dt.timedelta(days=i)).isoformat())
        _seed_product_stock(db, 2, stock=50, supplier_id=350)
        _seed_product_stock(db, 3, stock=10000, supplier_id=350)
        for i in range(60):
            _seed_sale(db, 8100 + i, 3, 1.0,
                       (FIXED_TODAY - _dt.timedelta(days=i)).isoformat())
        db.commit()

        items = list_supplier_full(350, today=FIXED_TODAY)
        statuses = [it["status"] for it in items]
        assert statuses.index("stockout") < statuses.index("ok")
        assert statuses.index("ok") < statuses.index("no_recent_demand")

    def test_compute_supplier_reorder_filters_zero(self, seed_products):
        db = seed_products
        _seed_supplier(db, 360, "SUP_FILTER")
        _seed_product_stock(db, 1, stock=0, supplier_id=360)
        _seed_product_stock(db, 2, stock=0, supplier_id=360)
        for i in range(60):
            _seed_sale(db, 9000 + i, 2, 1.0,
                       (FIXED_TODAY - _dt.timedelta(days=i)).isoformat())
        db.commit()

        items = compute_supplier_reorder(360, today=FIXED_TODAY)
        product_ids = [it["product_id"] for it in items]
        assert 1 not in product_ids
        assert 2 in product_ids

    def test_inactive_products_excluded(self, seed_products):
        db = seed_products
        _seed_supplier(db, 370, "SUP_X")
        _seed_product_stock(db, 1, stock=0, supplier_id=370)
        db.execute("UPDATE products SET is_active = 0 WHERE id = 1")
        for i in range(60):
            _seed_sale(db, 10000 + i, 1, 5.0,
                       (FIXED_TODAY - _dt.timedelta(days=i)).isoformat())
        db.commit()
        assert compute_supplier_reorder(370, today=FIXED_TODAY) == []

    def test_unmapped_bucket_uses_global_lead_time(self, seed_products):
        db = seed_products
        _seed_product_stock(db, 1, stock=0, supplier_id=None)
        for i in range(60):
            _seed_sale(db, 11000 + i, 1, 2.0,
                       (FIXED_TODAY - _dt.timedelta(days=i)).isoformat())
        db.commit()

        items = compute_supplier_reorder(None, window_days=60, today=FIXED_TODAY)
        p1 = next(it for it in items if it["product_id"] == 1)
        assert p1["lead_time_source"] == "global"
        assert p1["suggested_buy"] == 63
