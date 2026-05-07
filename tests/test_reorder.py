"""Tests for backend.services.reorder — /zakazlar Phase 1 engine."""
from __future__ import annotations

import datetime as _dt

import pytest

from backend.services.reorder import (
    DEFAULT_BUFFER_DAYS,
    DEFAULT_WINDOW_DAYS,
    compute_supplier_reorder,
    list_suppliers_with_products,
)


def _seed_supplier(db, sid: int, name: str, is_active: int = 1):
    db.execute(
        "INSERT OR REPLACE INTO suppliers (id, name_1c, is_active, periods, activity_uzs, activity_usd) "
        "VALUES (?, ?, ?, 12, 0, 0)",
        (sid, name, is_active),
    )


def _seed_product(db, pid: int, name: str, stock: float, supplier_id=None,
                  supplied_at: str = None, is_active: int = 1):
    db.execute(
        """INSERT OR REPLACE INTO products
              (id, name, name_display, category_id, producer_id,
               price_usd, price_uzs, unit, weight, is_active,
               stock_quantity, latest_supplier_id, latest_supplied_at)
           VALUES (?, ?, ?, 1, 1, 1.0, NULL, 'шт', 1.0, ?, ?, ?, ?)""",
        (pid, name, name, is_active, stock, supplier_id, supplied_at),
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


class TestListSuppliers:
    def test_only_active_with_mapped_products_appear(self, seed_products):
        db = seed_products
        # Insert 2 suppliers, only one mapped to a product
        _seed_supplier(db, 100, "ALPHA", is_active=1)
        _seed_supplier(db, 101, "BETA", is_active=1)
        _seed_supplier(db, 102, "GAMMA_RETIRED", is_active=0)
        db.execute("UPDATE products SET latest_supplier_id = 100 WHERE id = 1")
        # Product 2 mapped to retired GAMMA → shouldn't show
        db.execute("UPDATE products SET latest_supplier_id = 102 WHERE id = 2")
        db.commit()

        result = list_suppliers_with_products()
        names = [s["name_1c"] for s in result]
        assert "ALPHA" in names
        assert "BETA" not in names  # no products mapped
        assert "GAMMA_RETIRED" not in names  # is_active=0

    def test_unknown_bucket_appears_when_unmapped_products_exist(self, seed_products):
        db = seed_products
        # Products 1-5 unmapped by default in seed_products
        result = list_suppliers_with_products()
        unknown = [s for s in result if s["id"] is None]
        assert len(unknown) == 1
        assert unknown[0]["product_count"] == 5
        assert unknown[0]["name_1c"].startswith("(noma'lum")

    def test_oos_count_drives_sort_order(self, seed_products):
        db = seed_products
        _seed_supplier(db, 200, "LOW_OOS")
        _seed_supplier(db, 201, "HIGH_OOS")
        # 1 OOS for LOW, 2 OOS for HIGH — HIGH should rank first
        db.execute("UPDATE products SET latest_supplier_id = 200, stock_quantity = 5 WHERE id = 1")
        db.execute("UPDATE products SET latest_supplier_id = 200, stock_quantity = 0 WHERE id = 2")
        db.execute("UPDATE products SET latest_supplier_id = 201, stock_quantity = 0 WHERE id = 3")
        db.execute("UPDATE products SET latest_supplier_id = 201, stock_quantity = 0 WHERE id = 4")
        db.commit()

        result = list_suppliers_with_products()
        named = [s for s in result if s["id"] in (200, 201)]
        assert named[0]["name_1c"] == "HIGH_OOS"
        assert named[0]["oos_count"] == 2


class TestComputeReorder:
    def test_suggested_buy_uses_window_and_buffer(self, seed_products):
        db = seed_products
        _seed_supplier(db, 300, "SUP1")
        # Product 1: stock=10, sold 90 over 90d → daily=1.0 → target=30 → buy=20
        db.execute("UPDATE products SET latest_supplier_id = 300, stock_quantity = 10 WHERE id = 1")
        today = _dt.date.today()
        for i in range(90):
            d = (today - _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 1000 + i, 1, 1.0, d)
        db.commit()

        items = compute_supplier_reorder(300)
        assert len(items) == 1
        assert items[0]["product_id"] == 1
        assert items[0]["sold_window"] == 90
        assert items[0]["daily_rate"] == 1.0
        assert items[0]["suggested_buy"] == 20  # 30 target - 10 stock

    def test_no_buy_when_stock_covers_buffer(self, seed_products):
        db = seed_products
        _seed_supplier(db, 301, "SUP_OK")
        # Stock 100, daily 1.0 → 100-day cover, buffer 30 → no buy
        db.execute("UPDATE products SET latest_supplier_id = 301, stock_quantity = 100 WHERE id = 1")
        today = _dt.date.today()
        for i in range(90):
            d = (today - _dt.timedelta(days=i)).isoformat()
            _seed_sale(db, 2000 + i, 1, 1.0, d)
        db.commit()

        items = compute_supplier_reorder(301)
        assert items == []

    def test_zero_sales_means_no_buy(self, seed_products):
        db = seed_products
        _seed_supplier(db, 302, "SUP_DEAD")
        db.execute("UPDATE products SET latest_supplier_id = 302, stock_quantity = 0 WHERE id = 1")
        # No sales seeded
        db.commit()

        items = compute_supplier_reorder(302)
        assert items == []  # daily=0 → target=0 → no suggested buy

    def test_unknown_bucket_returns_unmapped(self, seed_products):
        db = seed_products
        # Product 1 stays unmapped, simulate sales
        today = _dt.date.today()
        for i in range(90):
            _seed_sale(db, 3000 + i, 1, 2.0, (today - _dt.timedelta(days=i)).isoformat())
        # Set stock=0 so it qualifies
        db.execute("UPDATE products SET stock_quantity = 0 WHERE id = 1")
        db.commit()

        items = compute_supplier_reorder(None)
        product_ids = [i["product_id"] for i in items]
        assert 1 in product_ids
        # 180 sold / 90d = 2.0 daily * 30 buffer = 60 target
        item = next(i for i in items if i["product_id"] == 1)
        assert item["suggested_buy"] == 60

    def test_only_active_products_considered(self, seed_products):
        db = seed_products
        _seed_supplier(db, 303, "SUP_X")
        db.execute("UPDATE products SET latest_supplier_id = 303, stock_quantity = 0, is_active = 0 WHERE id = 1")
        today = _dt.date.today()
        for i in range(90):
            _seed_sale(db, 4000 + i, 1, 5.0, (today - _dt.timedelta(days=i)).isoformat())
        db.commit()

        items = compute_supplier_reorder(303)
        assert items == []  # is_active=0 excluded
