"""Canonical phone normalizer (Error Log #86, audit M2) + the one intentional
variant that must NOT be collapsed into it."""
from backend.phone_utils import normalize_phone
from backend.services.import_client_master import _normalize_phone as master_norm


def test_normalize_phone_basics():
    assert normalize_phone("+998 90 123 45 67") == "901234567"   # strip + last 9
    assert normalize_phone("901234567") == "901234567"
    assert normalize_phone(None) == ""                           # None-safe
    assert normalize_phone(998901234567) == "901234567"          # non-str (xlrd float/int)
    assert normalize_phone("123") == "123"                       # <9 → partial


def test_master_wrapper_blanks_sub9():
    # The master importer deliberately returns "" for sub-9-digit input: an empty
    # phone is exempt from the active-phone partial UNIQUE index, a 7-digit partial
    # is not. This distinction must survive the M2 unification.
    assert master_norm("12345") == ""
    assert master_norm("901234567") == "901234567"
    assert master_norm(None) == ""
