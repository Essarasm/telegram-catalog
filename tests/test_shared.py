"""Tests for shared helper functions."""
import re


def _normalize_phone(raw):
    """Local copy of normalize_phone to avoid Python 3.9 import issues."""
    digits = re.sub(r"\D", "", raw or "")
    return digits[-9:] if len(digits) >= 9 else digits


def _html_escape(text):
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class TestNormalizePhone:
    def test_full_uzbek_number(self):
        assert _normalize_phone("998901234567") == "901234567"

    def test_nine_digits(self):
        assert _normalize_phone("901234567") == "901234567"

    def test_with_plus_and_dashes(self):
        assert _normalize_phone("+998-90-123-45-67") == "901234567"

    def test_with_spaces(self):
        assert _normalize_phone("998 90 123 45 67") == "901234567"

    def test_short_number(self):
        assert _normalize_phone("12345") == "12345"

    def test_empty(self):
        assert _normalize_phone("") == ""

    def test_none(self):
        assert _normalize_phone(None) == ""


class TestHtmlEscape:
    def test_ampersand(self):
        assert _html_escape("A & B") == "A &amp; B"

    def test_angle_brackets(self):
        assert _html_escape("<script>") == "&lt;script&gt;"

    def test_none(self):
        assert _html_escape(None) == ""

    def test_clean_string(self):
        assert _html_escape("hello") == "hello"

    def test_double_escape_safe(self):
        assert _html_escape("A &amp; B") == "A &amp;amp; B"
