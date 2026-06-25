"""Regression matrix for the client-portfolio identity-folding hardening.

The Portfolio matrix groups orders by a raw key but DISPLAYS the 1C name, so a
single shop could surface in two Rising/Stable/Sliding cells whenever two raw
keys resolved to the same name (Error Log #75/#82 identity family). The fix
(`_build_canon` + fold + display disambiguation in `client_portfolio.py`) keys
by the stable `onec_card_id` so one shop = one matrix row, while keeping
genuinely-different same-name shops separate and visually distinct.

These tests pin the three mechanisms + the unchanged baseline.
"""
import sqlite3
from datetime import datetime, timedelta

import pytest

from backend.services.client_portfolio import (
    _build_canon,
    _fold_hist,
    _fold_window,
    _norm,
    compute_portfolio,
)
from backend.services.client_portfolio import TK


def _conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(
        """
        CREATE TABLE allowed_clients (
            id INTEGER PRIMARY KEY, onec_card_id TEXT, client_id_1c TEXT,
            tuman TEXT, moljal TEXT, viloyat TEXT);
        CREATE TABLE real_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, doc_date TEXT,
            client_name_1c TEXT, client_id INTEGER,
            total_sum REAL, total_sum_currency REAL, is_approved INTEGER DEFAULT 1);
        CREATE TABLE daily_fx_rates (currency_pair TEXT, rate REAL);
        CREATE TABLE client_scores (recalc_date TEXT, client_id INTEGER, score REAL);
        INSERT INTO daily_fx_rates VALUES ('USD_UZS', 12000);
        """
    )
    return c


def _ac(c, cid, card, name, tuman=None):
    c.execute(
        "INSERT INTO allowed_clients (id, onec_card_id, client_id_1c, tuman) VALUES (?,?,?,?)",
        (cid, card, name, tuman),
    )


def _today():
    return datetime.now(TK).date()


def _ord(c, name, cid, days_ago, usd):
    d = (_today() - timedelta(days=days_ago)).isoformat()
    c.execute(
        "INSERT INTO real_orders (doc_date, client_name_1c, client_id, total_sum, total_sum_currency) "
        "VALUES (?,?,?,0,?)",
        (d, name, cid, usd),
    )


def _eligible_orders(c, name, cid, usd):
    """Seed orders that make a client Established + active + trajectory-eligible."""
    _ord(c, name, cid, 10, usd)    # current window + active (last order ≤60d)
    _ord(c, name, cid, 400, usd)   # prior window [365..485] order #1 + establishes
    _ord(c, name, cid, 450, usd)   # prior window order #2 (MIN_PRIOR_ORDERS=2)


# ---------------------------------------------------------------- unit: canon

def test_canon_folds_two_ids_sharing_one_card():
    c = _conn()
    _ac(c, 1, "Покупатели:10", "ALPHA")
    _ac(c, 2, "Покупатели:10", "ALPHA")   # duplicate row, same shop
    canon = _build_canon(c)
    assert canon(1) == canon(2) == 1       # both → representative (min) id


def test_canon_keeps_distinct_cards_separate():
    c = _conn()
    _ac(c, 3, "Покупатели:30", "BETA")
    _ac(c, 4, "Покупатели:40", "BETA")     # same name, different shop
    canon = _build_canon(c)
    assert canon(3) != canon(4)


def test_canon_resolves_unambiguous_null_name():
    c = _conn()
    _ac(c, 5, "Покупатели:50", "GAMMA")
    canon = _build_canon(c)
    assert canon("NAME:gamma") == 5        # NULL-id order folds into the known shop


def test_canon_leaves_ambiguous_null_name_unmerged():
    c = _conn()
    _ac(c, 6, "Покупатели:60", "DELTA")
    _ac(c, 7, "Покупатели:70", "DELTA")    # name maps to 2 ids → cannot fold
    canon = _build_canon(c)
    assert canon("NAME:delta") == "NAME:delta"


def test_canon_no_card_falls_back_to_id():
    c = _conn()
    _ac(c, 8, None, "EPSILON")
    canon = _build_canon(c)
    assert canon(8) == 8


# ---------------------------------------------------------------- unit: fold

def test_fold_window_sums_volume_and_keeps_max_name():
    canon = lambda k: 1  # noqa: E731 — both raw keys collapse to one shop
    raw = {1: {"name": "Azim", "usd_eq": 100.0, "n": 2},
           2: {"name": "AZIM URGUT", "usd_eq": 50.0, "n": 1}}
    out = _fold_window(raw, canon)
    assert set(out) == {1}
    assert out[1]["usd_eq"] == 150.0 and out[1]["n"] == 3
    assert out[1]["name"] == "Azim"        # MAX() of the two strings


def test_fold_hist_widens_first_and_last():
    canon = lambda k: 1  # noqa: E731
    raw = {1: ("2025-03-01", "2025-09-01"), 2: ("2025-01-15", "2025-12-20")}
    out = _fold_hist(raw, canon)
    assert out[1] == ("2025-01-15", "2025-12-20")


# ------------------------------------------------------- integration: compute

def test_duplicate_shop_appears_once():
    c = _conn()
    _ac(c, 1, "Покупатели:10", "ALPHA")
    _ac(c, 2, "Покупатели:10", "ALPHA")
    _eligible_orders(c, "ALPHA", 1, 300)   # half the orders on each duplicate row
    _eligible_orders(c, "ALPHA", 2, 300)
    data = compute_portfolio(c)
    names = [e["name"] for lst in data["cells"].values() for e in lst]
    assert names.count("ALPHA") == 1       # folded, not duplicated across cells


def test_two_same_name_shops_both_present_and_disambiguated():
    c = _conn()
    _ac(c, 3, "Покупатели:30", "BETA", tuman="Urgut")
    _ac(c, 4, "Покупатели:40", "BETA", tuman="Bulungur")
    _eligible_orders(c, "BETA", 3, 500)
    _eligible_orders(c, "BETA", 4, 200)
    data = compute_portfolio(c)
    names = sorted(e["name"] for lst in data["cells"].values() for e in lst)
    assert names == ["BETA (Bulungur)", "BETA (Urgut)"]   # distinct + locatable


def test_baseline_single_shop_name_unchanged():
    c = _conn()
    _ac(c, 5, "Покупатели:50", "GAMMA")
    _eligible_orders(c, "GAMMA", 5, 400)
    data = compute_portfolio(c)
    names = [e["name"] for lst in data["cells"].values() for e in lst]
    assert names == ["GAMMA"]              # no fold, no disambiguation noise


def test_no_internal_key_leaks_into_payload():
    c = _conn()
    _ac(c, 5, "Покупатели:50", "GAMMA")
    _eligible_orders(c, "GAMMA", 5, 400)
    data = compute_portfolio(c)
    for lst in data["cells"].values():
        for e in lst:
            assert "_key" not in e
    for lst in data["cohort_lists"].values():
        for e in lst:
            assert "_key" not in e


def test_norm_folds_case_whitespace_apostrophe():
    assert _norm("  Mo'ljal ") == _norm("mo'ljal") == _norm("MOʼLJAL")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
