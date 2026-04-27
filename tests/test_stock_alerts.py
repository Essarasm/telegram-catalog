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


class TestNewlyOutOfStock:
    """Daily 09:00 delta — items that flipped positive→0 within NEWLY_OUT_HOURS."""

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

    def test_recent_stockout_in_delta(self, seed_products):
        db = seed_products
        # Use SQLite's own clock so the cutoff math matches.
        recent = db.execute("SELECT datetime('now', '-1 hours')").fetchone()[0]
        self._setup(db, 1, 0, stockout_at=recent)
        self._setup(db, 2, 50)
        alerts = get_stock_alerts(db)
        newly = {item["id"] for item in alerts["newly_out_of_stock"]}
        assert 1 in newly
        assert 2 not in newly

    def test_old_stockout_excluded(self, seed_products):
        db = seed_products
        # 48h ago — outside the 24h window
        old = db.execute("SELECT datetime('now', '-48 hours')").fetchone()[0]
        self._setup(db, 1, 0, stockout_at=old)
        alerts = get_stock_alerts(db)
        cumul = {item["id"] for item in alerts["out_of_stock"]}
        newly = {item["id"] for item in alerts["newly_out_of_stock"]}
        assert 1 in cumul  # still in cumulative tugagan
        assert 1 not in newly  # but not in today's delta

    def test_null_stockout_excluded(self, seed_products):
        db = seed_products
        # qty=0 but stockout_at NULL (e.g. pre-migration historical zero)
        self._setup(db, 1, 0, stockout_at=None)
        alerts = get_stock_alerts(db)
        newly = {item["id"] for item in alerts["newly_out_of_stock"]}
        assert 1 not in newly

    def test_daily_message_empty_when_no_delta(self, seed_products):
        db = seed_products
        # All cumulative-out, none recent → 09:00 cron should be silent
        old = db.execute("SELECT datetime('now', '-72 hours')").fetchone()[0]
        self._setup(db, 1, 0, stockout_at=old)
        alerts = get_stock_alerts(db)
        msgs = format_daily_inventory_message(alerts)
        assert msgs == []

    def test_daily_message_includes_bugun_section(self, seed_products):
        db = seed_products
        recent = db.execute("SELECT datetime('now', '-2 hours')").fetchone()[0]
        self._setup(db, 1, 0, stockout_at=recent)
        alerts = get_stock_alerts(db)
        msgs = format_daily_inventory_message(alerts)
        combined = "\n".join(msgs)
        assert "BUGUN TUGAGAN" in combined
        assert "ВЭБЕР" in combined
        # cumulative count is shown for context
        assert "(bugun:" in combined
