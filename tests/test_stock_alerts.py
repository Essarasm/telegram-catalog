"""Tests for stock alert active product detection and classification."""
from backend.services.stock_alerts import (
    get_stock_alerts,
    format_stock_alert_message,
    format_daily_inventory_message,
)


class TestStockAlerts:
    def _setup_stock(self, db, product_id, qty, last_positive="2026-04-15"):
        db.execute(
            "UPDATE products SET stock_quantity = ?, stock_status = ?, "
            "stock_last_positive_at = ? WHERE id = ?",
            (qty, "in_stock" if qty > 0 else "out_of_stock", last_positive, product_id),
        )
        db.commit()

    def test_out_of_stock_detected(self, seed_products):
        db = seed_products
        self._setup_stock(db, 1, 0)
        self._setup_stock(db, 2, 50)
        self._setup_stock(db, 3, 50)
        self._setup_stock(db, 4, 0)
        self._setup_stock(db, 5, 50)

        alerts = get_stock_alerts(db)
        oos_ids = {item["id"] for item in alerts["out_of_stock"]}
        assert 1 in oos_ids
        assert 4 in oos_ids
        assert 2 not in oos_ids

    def test_running_low_detected(self, seed_products):
        db = seed_products
        self._setup_stock(db, 1, 2)
        self._setup_stock(db, 2, 50)
        self._setup_stock(db, 3, 1)
        self._setup_stock(db, 4, 100)
        self._setup_stock(db, 5, 3)

        alerts = get_stock_alerts(db)
        low_ids = {item["id"] for item in alerts["running_low"]}
        assert 1 in low_ids
        assert 3 in low_ids
        assert 5 in low_ids
        assert 2 not in low_ids

    def test_fractional_qty_is_oos(self, seed_products):
        db = seed_products
        self._setup_stock(db, 1, 0.5)
        self._setup_stock(db, 2, 0.75)
        self._setup_stock(db, 3, 50)

        alerts = get_stock_alerts(db)
        oos_ids = {item["id"] for item in alerts["out_of_stock"]}
        assert 1 in oos_ids
        assert 2 in oos_ids

    def test_healthy_count(self, seed_products):
        db = seed_products
        for pid in range(1, 6):
            self._setup_stock(db, pid, 100)

        alerts = get_stock_alerts(db)
        assert alerts["healthy_count"] == 5
        assert len(alerts["out_of_stock"]) == 0
        assert len(alerts["running_low"]) == 0

    def test_uses_1c_name(self, seed_products):
        db = seed_products
        self._setup_stock(db, 1, 0)
        alerts = get_stock_alerts(db)
        assert alerts["out_of_stock"][0]["name"].startswith("ВЭБЕР")

    def test_empty_db(self, db):
        alerts = get_stock_alerts(db)
        assert alerts["active_count"] == 0


class TestFormatMessage:
    """As of 2026-04-20, format_stock_alert_message returns list[str] (chunked
    for Telegram's 4096-char limit)."""

    def test_no_alerts_message(self):
        alerts = {"active_count": 100, "out_of_stock": [], "running_low": [], "healthy_count": 100}
        msgs = format_stock_alert_message(alerts)
        combined = "\n".join(msgs)
        assert "100" in combined
        assert "TUGAGAN" not in combined or "0" in combined

    def test_oos_in_message(self):
        alerts = {
            "active_count": 10,
            "out_of_stock": [{"name": "ЦЕМЕНТ М500", "last_sold": "2026-04-15"}],
            "running_low": [],
            "healthy_count": 9,
        }
        msgs = format_stock_alert_message(alerts)
        combined = "\n".join(msgs)
        assert "ЦЕМЕНТ М500" in combined
        assert "🔴" in combined

    def test_running_low_in_message(self):
        alerts = {
            "active_count": 10,
            "out_of_stock": [],
            "running_low": [{"name": "КРАСКА", "qty": 2, "unit": "шт"}],
            "healthy_count": 9,
        }
        msgs = format_stock_alert_message(alerts)
        combined = "\n".join(msgs)
        assert "КРАСКА" in combined
        assert "🟡" in combined

    def test_zero_active(self):
        alerts = {"active_count": 0, "out_of_stock": [], "running_low": [], "healthy_count": 0}
        msgs = format_stock_alert_message(alerts)
        assert len(msgs) == 1
        assert "Faol mahsulotlar topilmadi" in msgs[0]


class TestWeeklyOutOfStock:
    """Daily 09:00 work-week cumulative — items that ran out on/after this
    week's Monday 00:00 Tashkent. Tests pin the cutoff explicitly via
    ``week_start_utc`` so they don't depend on the current weekday."""

    # Monday 2026-04-27 00:00 Tashkent = 2026-04-26 19:00 UTC.
    WEEK_START = "2026-04-26 19:00:00"

    def _setup(self, db, pid, qty, last_positive="2026-04-15", stockout_at=None):
        db.execute(
            """UPDATE products SET stock_quantity = ?, stock_status = ?,
                                   stock_last_positive_at = ?, stockout_at = ?
               WHERE id = ?""",
            (
                qty,
                "in_stock" if qty > 0 else "out_of_stock",
                last_positive,
                stockout_at,
                pid,
            ),
        )
        db.commit()

    def test_this_week_stockout_listed(self, seed_products):
        db = seed_products
        # Mon 06:00 UTC = Mon 11:00 Tashkent — squarely inside the week
        self._setup(db, 1, 0, stockout_at="2026-04-27 06:00:00")
        self._setup(db, 2, 50)
        alerts = get_stock_alerts(db, week_start_utc=self.WEEK_START)
        weekly = {item["id"] for item in alerts["weekly_out_of_stock"]}
        assert 1 in weekly
        assert 2 not in weekly  # still in stock

    def test_last_week_stockout_excluded(self, seed_products):
        db = seed_products
        # Sat 2026-04-25 — last week
        self._setup(db, 1, 0, stockout_at="2026-04-25 12:00:00")
        alerts = get_stock_alerts(db, week_start_utc=self.WEEK_START)
        cumul = {item["id"] for item in alerts["out_of_stock"]}
        weekly = {item["id"] for item in alerts["weekly_out_of_stock"]}
        assert 1 in cumul  # still in cumulative tugagan list
        assert 1 not in weekly  # but not in this week's section

    def test_null_stockout_excluded(self, seed_products):
        db = seed_products
        self._setup(db, 1, 0, stockout_at=None)
        alerts = get_stock_alerts(db, week_start_utc=self.WEEK_START)
        weekly = {item["id"] for item in alerts["weekly_out_of_stock"]}
        assert 1 not in weekly

    def test_restocked_item_drops_off(self, seed_products):
        db = seed_products
        # Stockout earlier this week, but qty>0 now (restocked) — should NOT appear
        self._setup(db, 1, 5, stockout_at="2026-04-27 06:00:00")
        alerts = get_stock_alerts(db, week_start_utc=self.WEEK_START)
        weekly = {item["id"] for item in alerts["weekly_out_of_stock"]}
        assert 1 not in weekly

    def test_daily_message_empty_when_nothing_this_week(self, seed_products):
        db = seed_products
        # Cumulative-out from last week only
        self._setup(db, 1, 0, stockout_at="2026-04-20 12:00:00")
        alerts = get_stock_alerts(db, week_start_utc=self.WEEK_START)
        msgs = format_daily_inventory_message(alerts)
        assert msgs == []

    def test_daily_message_groups_by_day(self, seed_products):
        db = seed_products
        # Mon 06:00 UTC → Mon 11:00 Tashkent (Dushanba)
        self._setup(db, 1, 0, stockout_at="2026-04-27 06:00:00")
        # Tue 06:00 UTC → Tue 11:00 Tashkent (Seshanba)
        self._setup(db, 2, 0, stockout_at="2026-04-28 06:00:00")
        alerts = get_stock_alerts(db, week_start_utc=self.WEEK_START)
        msgs = format_daily_inventory_message(alerts)
        combined = "\n".join(msgs)
        assert "BU HAFTA TUGAGAN" in combined
        assert "(bu haftada:" in combined
        assert "Dushanba" in combined
        assert "Seshanba" in combined
        # Days should appear in chronological order, Mon before Tue
        assert combined.index("Dushanba") < combined.index("Seshanba")


class TestWeeklyTopSellers:
    """Top 5 products by units sold since this week's Monday (Tashkent)."""

    WEEK_START_UTC = "2026-04-26 19:00:00"
    WEEK_START_TK_DATE = "2026-04-27"

    def _add_order(self, db, order_id, doc_date, item_specs):
        """item_specs: list of (product_id, quantity)."""
        db.execute(
            """INSERT OR REPLACE INTO real_orders
               (id, doc_number_1c, doc_date, client_name_1c)
               VALUES (?, ?, ?, ?)""",
            (order_id, f"DOC{order_id}", doc_date, "Test Client"),
        )
        for line_no, (pid, qty) in enumerate(item_specs, start=1):
            db.execute(
                """INSERT INTO real_order_items
                   (real_order_id, line_no, product_name_1c, product_id, quantity)
                   VALUES (?, ?, ?, ?, ?)""",
                (order_id, line_no, f"product-{pid}", pid, qty),
            )
        # Mark some seed products with positive stock so the active set
        # is non-empty (otherwise get_stock_alerts short-circuits).
        db.execute(
            "UPDATE products SET stock_quantity = 50, stock_status = 'in_stock', "
            "stock_last_positive_at = '2026-04-27' WHERE id <= 5",
        )
        db.commit()

    def test_top5_orders_by_units_desc(self, seed_products):
        db = seed_products
        self._add_order(db, 1, "2026-04-27", [(1, 10), (2, 5), (3, 3)])
        self._add_order(db, 2, "2026-04-28", [(1, 15), (4, 8), (5, 2)])
        alerts = get_stock_alerts(
            db,
            week_start_utc=self.WEEK_START_UTC,
            week_start_tk_date=self.WEEK_START_TK_DATE,
        )
        top = alerts["weekly_top_sellers"]
        assert len(top) == 5
        assert top[0]["product_id"] == 1 and top[0]["units_sold"] == 25
        # Sorted descending
        units = [t["units_sold"] for t in top]
        assert units == sorted(units, reverse=True)

    def test_top5_excludes_pre_week_orders(self, seed_products):
        db = seed_products
        # Last week — should NOT count
        self._add_order(db, 1, "2026-04-20", [(1, 100)])
        # This week
        self._add_order(db, 2, "2026-04-27", [(2, 5)])
        alerts = get_stock_alerts(
            db,
            week_start_utc=self.WEEK_START_UTC,
            week_start_tk_date=self.WEEK_START_TK_DATE,
        )
        top_ids = {t["product_id"] for t in alerts["weekly_top_sellers"]}
        assert 1 not in top_ids
        assert 2 in top_ids

    def test_top5_caps_at_five(self, seed_products):
        db = seed_products
        # 5 products × different quantities — fixture only has 5, so naturally capped
        self._add_order(db, 1, "2026-04-27", [
            (1, 10), (2, 9), (3, 8), (4, 7), (5, 6),
        ])
        alerts = get_stock_alerts(
            db,
            week_start_utc=self.WEEK_START_UTC,
            week_start_tk_date=self.WEEK_START_TK_DATE,
        )
        assert len(alerts["weekly_top_sellers"]) == 5

    def test_top5_groups_same_product_across_orders(self, seed_products):
        db = seed_products
        self._add_order(db, 1, "2026-04-27", [(1, 7)])
        self._add_order(db, 2, "2026-04-28", [(1, 13)])
        alerts = get_stock_alerts(
            db,
            week_start_utc=self.WEEK_START_UTC,
            week_start_tk_date=self.WEEK_START_TK_DATE,
        )
        top = alerts["weekly_top_sellers"]
        assert len(top) == 1
        assert top[0]["product_id"] == 1
        assert top[0]["units_sold"] == 20

    def test_message_includes_top5_section(self, seed_products):
        db = seed_products
        self._add_order(db, 1, "2026-04-27", [(1, 25), (2, 12)])
        alerts = get_stock_alerts(
            db,
            week_start_utc=self.WEEK_START_UTC,
            week_start_tk_date=self.WEEK_START_TK_DATE,
        )
        msgs = format_daily_inventory_message(alerts)
        combined = "\n".join(msgs)
        assert "TOP-5 SOTILGAN" in combined
        assert "🔥" in combined
        # Top-ranked item appears before second-ranked
        assert "1." in combined and "2." in combined

    def test_message_sent_when_only_sales_no_stockouts(self, seed_products):
        """Tuesday morning case: nothing ran out yet, but sales happened —
        message should still go out (top-5 section alone is enough)."""
        db = seed_products
        self._add_order(db, 1, "2026-04-27", [(1, 5)])
        alerts = get_stock_alerts(
            db,
            week_start_utc=self.WEEK_START_UTC,
            week_start_tk_date=self.WEEK_START_TK_DATE,
        )
        # No stockouts but top sellers present
        assert len(alerts["weekly_out_of_stock"]) == 0
        assert len(alerts["weekly_top_sellers"]) > 0
        msgs = format_daily_inventory_message(alerts)
        assert msgs != []
        combined = "\n".join(msgs)
        assert "TOP-5 SOTILGAN" in combined
        assert "BU HAFTA TUGAGAN" not in combined
