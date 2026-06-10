"""Regression matrix for the authoritative weight helper (Error Log #89, rule #12).

Covers the precedence between the three weight compartments — kg-unit
definitional, sales-derived, and name-parse — for every empty/full combination.
"""
from backend.services.product_weight import (
    is_kg_unit, within_factor, suggest_weight, authoritative_weight,
)


# ── kg-unit is definitional and overrides every other source ──
def test_kg_unit_forces_one_regardless_of_sources():
    assert authoritative_weight(20.0, "кг", 1.0, name="Гвозди /20 кг/") == 1.0
    assert authoritative_weight(None, "kg", None, name="X /50кг/", excel_candidate=50) == 1.0
    assert authoritative_weight(25.0, "кг", None, name="Гвозди /25 кг/") == 1.0  # never shipped
    assert suggest_weight("кг", None, "Гвозди /20 кг/") == (1.0, "kg_unit")


def test_is_kg_unit_variants():
    assert is_kg_unit("кг") and is_kg_unit("KG") and is_kg_unit(" kg ")
    assert not is_kg_unit("шт") and not is_kg_unit(None) and not is_kg_unit("рул")


# ── sales-derived wins over Excel/name-parse for non-kg units ──
def test_sales_snaps_gross_error():
    # plinth end-cap: Excel 48 garbage, sales says 0.01 → snap to sales
    assert authoritative_weight(48.0, "шт", 0.01, name="Заглушка", excel_candidate=48) == 0.01


def test_sales_wins_outright():
    # sales always wins when present — even a sub-2x disagreement (Линолеум /25м/
    # 100→63, 1.59x) must be corrected, and a noisy existing is overridden.
    assert authoritative_weight(0.25, "шт", 0.25) == 0.25
    assert authoritative_weight(0.4, "шт", 0.25) == 0.25      # not kept
    assert authoritative_weight(100.0, "рул", 63.0, excel_candidate=100) == 63.0


# ── name-parse only for never-shipped, non-kg products ──
def test_name_parse_fallback_only_without_sales():
    assert authoritative_weight(None, "шт", None, name="Грунтовка 0.75 кг") == 0.75
    assert suggest_weight("шт", None, "Грунтовка 0.75 кг") == (0.75, "name_parse")


def test_excel_candidate_beats_name_parse_without_sales():
    assert authoritative_weight(None, "шт", None, name="X 0.75 кг", excel_candidate=5.0) == 5.0


# ── all-empty → no signal, leave as-is ──
def test_no_signal_leaves_existing():
    assert authoritative_weight(None, "шт", None, name="Безымянный товар") is None
    assert authoritative_weight(3.0, "шт", None, name="Безымянный товар") == 3.0
    assert suggest_weight("шт", None, "Безымянный товар") == (None, "none")


# ── idempotency: re-resolving a settled value is a fixed point ──
def test_idempotent():
    for args in [(1.0, "кг", 1.0), (0.01, "шт", 0.01), (63.0, "рул", 63.0)]:
        assert authoritative_weight(*args) == authoritative_weight(args[0], args[1], args[2])
        # second pass on the result equals itself
        once = authoritative_weight(*args)
        assert authoritative_weight(once, args[1], args[2]) == once


def test_within_factor():
    assert within_factor(1.0, 1.5) and within_factor(1.0, 2.0)
    assert not within_factor(1.0, 2.1) and not within_factor(20.0, 1.0)
    assert not within_factor(0, 1.0) and not within_factor(1.0, None)
