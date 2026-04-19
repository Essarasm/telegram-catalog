"""Tests for FIFO allocation logic in akt-sverki."""
from backend.services.akt_sverki import _as_float


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
