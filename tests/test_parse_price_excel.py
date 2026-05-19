"""Smoke test for the product-price (1C Номенклатура / Справочник) parser.

Pins `parse_price_excel`'s shape and filtering rules:
  - Column indices: NAME=1, TYPE=2, UNIT=5, UZS=6, USD=15, WEIGHT=18
  - Only rows where TYPE == 'Товар' are emitted (services, sub-categories
    dropped)
  - Rows with USD <= 0 are dropped (price list filter)
  - Names shorter than 3 chars are dropped
  - Returns Dict[name → {usd, uzs, weight, unit}] keyed by Cyrillic name
  - Weight: if column 18 is missing/zero, parse from name via parse_weight_from_name
  - Unit: defaults to 'sht' if blank/'none'/'nan'
  - .xls + cp1251 (xlrd) primary path; pandas + xlrd-default are fallbacks

Per `04-data-handling` rule (1C is single source of truth) and the
zero-data-loss red line — a silent column shift here would invert
every product's USD price across the catalog.

Uses xlwt (test-only) to synthesize .xls fixtures in memory.
"""
import io

import xlwt

from backend.services.update_prices import (
    COL_NAME,
    COL_TYPE,
    COL_UNIT,
    COL_USD,
    COL_UZS,
    COL_WEIGHT,
    parse_price_excel,
)


def _new_xls() -> tuple[xlwt.Workbook, xlwt.Worksheet]:
    wb = xlwt.Workbook(encoding="cp1251")
    ws = wb.add_sheet("Sheet1")
    return wb, ws


def _to_bytes(wb: xlwt.Workbook) -> bytes:
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# Minimum row width = max(NAME, TYPE, UNIT, UZS, USD, WEIGHT) + 1.
_ROW_WIDTH = max(COL_NAME, COL_TYPE, COL_UNIT, COL_UZS, COL_USD, COL_WEIGHT) + 1


def _write_price_xls(rows_of_cells: list[dict]) -> bytes:
    """Build a minimal price .xls fixture.

    Each dict may set: name, type, unit, uzs, usd, weight.
    xlwt strips trailing empty cells when serializing, which would shrink
    the row below the parser's 19-column requirement; we force col 18 to a
    numeric 0 sentinel so xlrd reports ncols == 19 for every row.
    (Numeric 0 triggers the parser's name-based weight fallback per its
    `weight <= 0` check.)
    """
    wb, ws = _new_xls()
    for r, row in enumerate(rows_of_cells):
        for c in range(_ROW_WIDTH):
            if c == COL_NAME:
                ws.write(r, c, row.get("name", ""))
            elif c == COL_TYPE:
                ws.write(r, c, row.get("type", ""))
            elif c == COL_UNIT:
                ws.write(r, c, row.get("unit", ""))
            elif c == COL_UZS:
                ws.write(r, c, row.get("uzs", 0))
            elif c == COL_USD:
                ws.write(r, c, row.get("usd", 0))
            elif c == COL_WEIGHT:
                # Force numeric 0 (not "") so xlrd preserves the row width.
                ws.write(r, c, row.get("weight", 0))
            else:
                ws.write(r, c, "")
    return _to_bytes(wb)


def test_parse_price_excel_basic_filters():
    """A mix of valid products and rows that must be filtered out:
       services (TYPE != Товар), short names, blank rows.
       UZS-only Собственный products are valid and must survive.
    """
    file_bytes = _write_price_xls(
        [
            # Header row (no TYPE='Товар') — filtered
            {"name": "Наименование", "type": "Группа"},
            # Real product — kept
            {"name": "Цемент М400 50кг", "type": "Товар",
             "unit": "шт", "uzs": 50000, "usd": 4.5, "weight": 50},
            # Service (not Товар) — filtered
            {"name": "Доставка по городу", "type": "Услуга",
             "unit": "шт", "uzs": 100000, "usd": 8, "weight": 0},
            # Short name (<3 chars) — filtered
            {"name": "AB", "type": "Товар",
             "unit": "шт", "uzs": 1, "usd": 1, "weight": 1},
            # UZS-only Собственный product (no USD) — KEPT
            {"name": "Мегамикс сатин 51 /20 кг/", "type": "Товар",
             "unit": "шт", "uzs": 37000, "usd": 0, "weight": 20},
            # No prices at all (uzs=0, usd=0) — filtered
            {"name": "Товар без цены", "type": "Товар",
             "unit": "шт", "uzs": 0, "usd": 0, "weight": 1},
            # Another real product — kept
            {"name": "Кирпич красный 250x120", "type": "Товар",
             "unit": "шт", "uzs": 800, "usd": 0.07, "weight": 3.5},
        ]
    )

    result = parse_price_excel(file_bytes)
    assert isinstance(result, dict)
    expected = {
        "Цемент М400 50кг",
        "Мегамикс сатин 51 /20 кг/",
        "Кирпич красный 250x120",
    }
    assert set(result.keys()) == expected, (
        f"unexpected keys: {sorted(result.keys())}"
    )
    # Sanity: UZS-only product carries uzs but zero usd
    satin = result["Мегамикс сатин 51 /20 кг/"]
    assert satin["uzs"] == 37000
    assert satin["usd"] == 0

    cement = result["Цемент М400 50кг"]
    assert cement["usd"] == 4.5
    assert cement["uzs"] == 50000
    assert cement["weight"] == 50
    assert cement["unit"] == "шт"

    brick = result["Кирпич красный 250x120"]
    assert brick["usd"] == 0.07
    assert brick["uzs"] == 800
    assert brick["weight"] == 3.5


def test_parse_price_excel_weight_falls_back_to_name():
    """If COL_WEIGHT is blank/zero, weight must be derived from the name
    via parse_weight_from_name. Catches the silent-NULL-weight regression
    that would leak '0 kg' shipping calculations downstream.
    """
    file_bytes = _write_price_xls(
        [
            # Weight column 0 — parser's `weight <= 0` branch triggers the
            # name-based fallback, which should extract "25" from the name.
            {"name": "Клей плиточный 25кг", "type": "Товар",
             "unit": "мешок", "usd": 5, "uzs": 60000, "weight": 0},
        ]
    )
    result = parse_price_excel(file_bytes)
    p = result["Клей плиточный 25кг"]
    assert p["weight"] == 25.0, f"expected weight 25 from name, got {p['weight']}"


def test_parse_price_excel_unit_default_sht():
    """Blank / 'none' / 'nan' unit must default to 'sht' (Latin-script Uzbek
    for 'pcs'). Catches a UI regression that would render literal 'none' text.
    """
    file_bytes = _write_price_xls(
        [
            {"name": "Изделие без единицы", "type": "Товар",
             "unit": "", "usd": 1, "uzs": 1, "weight": 1},
            {"name": "Изделие с явной единицей", "type": "Товар",
             "unit": "комплект", "usd": 2, "uzs": 2, "weight": 2},
        ]
    )
    result = parse_price_excel(file_bytes)
    assert result["Изделие без единицы"]["unit"] == "sht"
    assert result["Изделие с явной единицей"]["unit"] == "комплект"


def test_parse_price_excel_zero_uzs_is_normalized_to_zero():
    """UZS missing/<=0 must serialize as 0 (not None or negative) — keeps
    downstream display-time `?? 0` checks honest.
    """
    file_bytes = _write_price_xls(
        [
            {"name": "USD-only product", "type": "Товар",
             "unit": "шт", "uzs": 0, "usd": 3.5, "weight": 1},
        ]
    )
    result = parse_price_excel(file_bytes)
    assert result["USD-only product"]["uzs"] == 0
    assert result["USD-only product"]["usd"] == 3.5


def test_parse_price_excel_empty_returns_empty_dict():
    """Empty file (or one with only filtered-out rows) returns {} —
    NOT a raise, NOT a None. Lets the caller's `for name, data in ...`
    loop be a no-op naturally.
    """
    file_bytes = _write_price_xls([])
    assert parse_price_excel(file_bytes) == {}
