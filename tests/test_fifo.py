"""Tests for FIFO allocation logic in akt-sverki."""
from backend.services.akt_sverki import _as_float, _allocate_fifo, _state_for


class TestAsFloat:
    def test_normal(self):
        assert _as_float(42.5) == 42.5

    def test_none(self):
        assert _as_float(None) == 0.0

    def test_zero(self):
        assert _as_float(0) == 0.0

    def test_string_number(self):
        assert _as_float("123.45") == 123.45

    def test_empty_string(self):
        assert _as_float("") == 0.0

    def test_non_numeric(self):
        assert _as_float("abc") == 0.0


class TestAllocateFifo:
    def _order(self, id, date, uzs=0, usd=0):
        return {"type": "order", "id": id, "doc_number": f"DOC-{id}",
                "date": date, "uzs_amount": uzs, "usd_amount": usd}

    def _payment(self, id, date, uzs=0, usd=0):
        return {"type": "payment", "id": f"pay-{id}", "ids": [id],
                "doc_number": "", "date": date, "uzs_amount": uzs, "usd_amount": usd}

    def test_single_order_single_payment_exact(self):
        events = [self._order(1, "2026-04-01", uzs=100000), self._payment(1, "2026-04-05", uzs=100000)]
        result = _allocate_fifo(events)
        assert result["uzs_debt"] == 0
        assert result["uzs_advance"] == 0
        assert result["uzs_balance"] == 0

    def test_partial_payment_leaves_debt(self):
        events = [self._order(1, "2026-04-01", uzs=100000), self._payment(1, "2026-04-05", uzs=60000)]
        result = _allocate_fifo(events)
        assert result["uzs_debt"] == 40000
        assert result["uzs_advance"] == 0

    def test_overpayment_creates_advance(self):
        events = [self._order(1, "2026-04-01", uzs=100000), self._payment(1, "2026-04-05", uzs=150000)]
        result = _allocate_fifo(events)
        assert result["uzs_debt"] == 0
        assert result["uzs_advance"] == 50000

    def test_fifo_order_oldest_first(self):
        events = [
            self._order(1, "2026-04-01", uzs=50000),
            self._order(2, "2026-04-03", uzs=30000),
            self._payment(1, "2026-04-05", uzs=60000),
        ]
        result = _allocate_fifo(events)
        order1 = result["events"][0]
        order2 = result["events"][1]
        assert order1["uzs_remaining"] == 0
        assert order2["uzs_remaining"] == 20000

    def test_payment_before_order_creates_advance(self):
        events = [self._payment(1, "2026-04-01", uzs=50000), self._order(1, "2026-04-03", uzs=30000)]
        result = _allocate_fifo(events)
        assert result["uzs_advance"] == 20000
        order = result["events"][1]
        assert order["uzs_remaining"] == 0

    def test_dual_currency_independent(self):
        events = [
            self._order(1, "2026-04-01", uzs=100000, usd=500),
            self._payment(1, "2026-04-05", uzs=100000, usd=0),
        ]
        result = _allocate_fifo(events)
        assert result["uzs_debt"] == 0
        assert result["usd_debt"] == 500

    def test_no_events(self):
        result = _allocate_fifo([])
        assert result["uzs_debt"] == 0
        assert result["usd_debt"] == 0
        assert result["uzs_advance"] == 0

    def test_multiple_payments_close_one_order(self):
        events = [
            self._order(1, "2026-04-01", uzs=100000),
            self._payment(1, "2026-04-03", uzs=40000),
            self._payment(2, "2026-04-05", uzs=60000),
        ]
        result = _allocate_fifo(events)
        assert result["uzs_debt"] == 0
        order = result["events"][0]
        assert len(order["uzs_paid_by"]) == 2

    def test_running_balance_tracked(self):
        events = [
            self._order(1, "2026-04-01", uzs=100000),
            self._payment(1, "2026-04-05", uzs=100000),
        ]
        result = _allocate_fifo(events)
        assert result["events"][0]["uzs_balance"] < 0
        assert result["events"][1]["uzs_balance"] == 0


class TestStateFor:
    def test_clean_no_debt(self):
        state = _state_for(0, 0, None)
        assert state["code"] == "clean"

    def test_advance(self):
        state = _state_for(0, 50000, None)
        assert state["code"] == "advance"
        assert state["advance"] == 50000

    def test_debt_recent(self):
        from datetime import date, timedelta
        recent = (date.today() - timedelta(days=5)).isoformat()
        state = _state_for(10000, 0, recent)
        assert state["code"] == "debt_0_14"

    def test_debt_15_29(self):
        from datetime import date, timedelta
        d = (date.today() - timedelta(days=20)).isoformat()
        state = _state_for(10000, 0, d)
        assert state["code"] == "debt_15_29"

    def test_debt_30_plus(self):
        from datetime import date, timedelta
        d = (date.today() - timedelta(days=45)).isoformat()
        state = _state_for(10000, 0, d)
        assert state["code"] == "debt_30_plus"
