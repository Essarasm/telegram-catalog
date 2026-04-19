"""Tests for stock alert active product detection and classification."""
from backend.services.stock_alerts import get_stock_alerts, format_stock_alert_message


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
    def test_no_alerts_message(self):
        alerts = {"active_count": 100, "out_of_stock": [], "running_low": [], "healthy_count": 100}
        msg = format_stock_alert_message(alerts)
        assert "100" in msg
        assert "TUGAGAN" not in msg or "0" in msg

    def test_oos_in_message(self):
        alerts = {
            "active_count": 10,
            "out_of_stock": [{"name": "ЦЕМЕНТ М500", "last_sold": "2026-04-15"}],
            "running_low": [],
            "healthy_count": 9,
        }
        msg = format_stock_alert_message(alerts)
        assert "ЦЕМЕНТ М500" in msg
        assert "🔴" in msg

    def test_running_low_in_message(self):
        alerts = {
            "active_count": 10,
            "out_of_stock": [],
            "running_low": [{"name": "КРАСКА", "qty": 2, "unit": "шт"}],
            "healthy_count": 9,
        }
        msg = format_stock_alert_message(alerts)
        assert "КРАСКА" in msg
        assert "🟡" in msg

    def test_zero_active(self):
        alerts = {"active_count": 0, "out_of_stock": [], "running_low": [], "healthy_count": 0}
        msg = format_stock_alert_message(alerts)
        assert "Faol mahsulotlar topilmadi" in msg
