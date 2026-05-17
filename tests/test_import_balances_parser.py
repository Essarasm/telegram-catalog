"""Smoke test for the оборотно-сальдовая (1C balance) parser.

Pins the parsing rules captured in the import_balances module:
  - .xls + cp1251 (parser uses xlrd with encoding_override='cp1251')
  - Period parsed from row 3: 'за DD.MM.YY - DD.MM.YY' or 'за <Month> YYYY г.'
  - Three file formats, all dispatched by `parse_balance_xls`:
      1. Single-account UZS (header contains '40.10')   → `_parse_clients_simple`
      2. Single-account USD (header contains '40.11')   → `_parse_clients_usd`
      3. Combined (header contains '40' with '40.10'+'40.11' section dividers)
  - Indented rows (per-contract breakdowns, prefix '   ') are sub-details,
    NOT clients — must be dropped silently
  - 'Итого' / 'Итого развернутое' totals rows must be dropped
  - Section header rows ('40.10', '40.11') in combined files act only as
    dividers — never emitted as clients

Per memory `no_data_loss` and the dual-currency rule in `04-data-handling`,
a silent drop of the USD leg here would silently lose the USD picture for
every client. This test would fail loudly long before that ships.

Uses xlwt (test-only dependency in requirements-dev.txt) to synthesize
.xls fixtures in memory — no PII committed.
"""
import io

import xlwt

from backend.services.import_balances import parse_balance_xls


def _new_xls() -> tuple[xlwt.Workbook, xlwt.Worksheet]:
    wb = xlwt.Workbook(encoding="cp1251")
    ws = wb.add_sheet("Sheet1")
    return wb, ws


def _to_bytes(wb: xlwt.Workbook) -> bytes:
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _write_balance_xls(
    *,
    header_text: str,
    period_text: str,
    data_rows: list[tuple],
) -> bytes:
    """Build a minimal balance .xls fixture.

    Row layout (0-indexed) matches what `parse_balance_xls` expects:
        Row 0: blank
        Row 1: header (xlrd reads `sh.cell_value(1, 0)`; must contain '40.10',
               '40.11', or '40' to drive format detection)
        Row 2: blank
        Row 3: period text (parsed by `_parse_period`)
        Row 4: blank
        Row 5: column header row (ignored by parser; data starts at row 6)
        Row 6+: `data_rows` — each tuple is (col0, col1, col2, col3, col4, col5, col6)
                col0 = client name (or '   <subdetail>' for skip, or section divider)
                col1..col6 = opening_debit, opening_credit, period_debit,
                             period_credit, closing_debit, closing_credit
    """
    wb, ws = _new_xls()
    ws.write(1, 0, header_text)
    ws.write(3, 0, period_text)
    ws.write(5, 0, "Контрагент")
    for r_offset, row in enumerate(data_rows):
        r = 6 + r_offset
        for c, val in enumerate(row):
            ws.write(r, c, val)
    return _to_bytes(wb)


def test_parse_balance_single_account_uzs():
    """Header contains '40.10' → UZS single-account format, simple client rows."""
    file_bytes = _write_balance_xls(
        header_text="Оборотно-сальдовая ведомость по счету 40.10",
        period_text="за 01.03.26 - 31.03.26",
        data_rows=[
            ("ООО АЛЬФА",  1000000, 0, 500000,  300000, 1200000, 0),
            ("   договор-1", 0, 0, 0, 0, 0, 0),   # indented sub-row → skip
            ("ИП БЕТА",    0, 200000, 0, 100000, 0, 100000),
            ("Итого",      0, 0, 0, 0, 0, 0),     # totals row → skip
        ],
    )
    result = parse_balance_xls(file_bytes)
    assert result["ok"] is True, f"parse failed: {result.get('error')}"
    assert result["currency"] == "UZS"
    assert result["period_start"] == "2026-03-01"
    assert result["period_end"] == "2026-03-31"
    clients = result["clients"]
    assert len(clients) == 2, f"expected 2 clients, got {len(clients)}: {[c['client_name_1c'] for c in clients]}"

    c1 = clients[0]
    assert c1["client_name_1c"] == "ООО АЛЬФА"
    assert c1["opening_debit"] == 1000000
    assert c1["period_debit"] == 500000
    assert c1["closing_debit"] == 1200000

    c2 = clients[1]
    assert c2["client_name_1c"] == "ИП БЕТА"
    assert c2["opening_credit"] == 200000
    assert c2["closing_credit"] == 100000


def test_parse_balance_single_account_usd():
    """Header contains '40.11' → USD format with 'В валюте' aggregate rows.

    In this layout each client name row is followed by a 'В валюте' row that
    carries the actual values; the parser must capture only the FIRST
    'В валюте' per client (subsequent ones are sub-account breakdowns).
    """
    wb, ws = _new_xls()
    ws.write(1, 0, "Оборотно-сальдовая по счету 40.11 (USD)")
    ws.write(3, 0, "за Март 2026 г.")
    ws.write(5, 0, "Контрагент")
    # Row 6: client name (only col 0)
    ws.write(6, 0, "USD CLIENT ONE")
    # Row 7: aggregate values in 'В валюте' row
    ws.write(7, 0, "В валюте")
    ws.write(7, 1, 100); ws.write(7, 2, 0)
    ws.write(7, 3, 50);  ws.write(7, 4, 20)
    ws.write(7, 5, 130); ws.write(7, 6, 0)
    # Row 8: a second 'В валюте' row (sub-account detail) — must be skipped
    ws.write(8, 0, "В валюте")
    ws.write(8, 1, 999)  # bogus values — must NOT overwrite the captured aggregate
    # Row 9: another client
    ws.write(9, 0, "USD CLIENT TWO")
    ws.write(10, 0, "В валюте")
    ws.write(10, 5, 75)  # closing_debit only
    buf = io.BytesIO()
    wb.save(buf)

    result = parse_balance_xls(buf.getvalue())
    assert result["ok"] is True, f"parse failed: {result.get('error')}"
    assert result["currency"] == "USD"
    assert result["period_start"] == "2026-03-01"
    assert result["period_end"] == "2026-03-31"
    clients = result["clients"]
    assert len(clients) == 2, f"expected 2, got {len(clients)}: {[c['client_name_1c'] for c in clients]}"

    c1 = clients[0]
    assert c1["client_name_1c"] == "USD CLIENT ONE"
    assert c1["opening_debit"] == 100  # from first 'В валюте' row
    assert c1["period_debit"] == 50
    assert c1["period_credit"] == 20
    assert c1["closing_debit"] == 130  # not 999 — second 'В валюте' must be ignored

    c2 = clients[1]
    assert c2["client_name_1c"] == "USD CLIENT TWO"
    assert c2["closing_debit"] == 75


def test_parse_balance_combined_two_sections():
    """Combined file (header '40') with '40.10' + '40.11' section dividers.
    Both sections must land in `sections`; the dividers themselves must NOT
    appear as clients in either section.
    """
    wb, ws = _new_xls()
    ws.write(1, 0, "Оборотно-сальдовая по счету 40")
    ws.write(3, 0, "за 01.03.26 - 31.03.26")
    ws.write(5, 0, "Контрагент")
    # Helper to pad every data row to width 7 (xlrd raises IndexError on truncated rows).
    # xlwt forbids overwriting cells, so write each column exactly once.
    def _row(r: int, name: str, closing_debit: float = 0):
        ws.write(r, 0, name)
        for c in range(1, 7):
            ws.write(r, c, closing_debit if c == 5 else 0)
    # UZS section
    _row(6, "40.10")                       # section divider
    _row(7, "ALPHA UZS", 500000)
    _row(8, "BETA UZS",  250000)
    # USD section
    _row(9, "40.11")                       # section divider
    _row(10, "GAMMA USD", 100)
    # Totals rows (must be skipped — end_row = nrows - 2 trims them)
    _row(11, "Итого развернутое")
    _row(12, "Итого")
    buf = io.BytesIO()
    wb.save(buf)

    result = parse_balance_xls(buf.getvalue())
    assert result["ok"] is True, f"parse failed: {result.get('error')}"
    sections = result.get("sections")
    assert sections is not None, "combined file must populate 'sections'"
    assert len(sections) == 2, f"expected 2 sections, got {len(sections)}"

    uzs = sections[0]
    assert uzs["currency"] == "UZS"
    uzs_names = [c["client_name_1c"] for c in uzs["clients"]]
    assert uzs_names == ["ALPHA UZS", "BETA UZS"], uzs_names
    assert "40.10" not in uzs_names  # divider must never become a client

    usd = sections[1]
    assert usd["currency"] == "USD"
    usd_names = [c["client_name_1c"] for c in usd["clients"]]
    assert usd_names == ["GAMMA USD"], usd_names
    assert "40.11" not in usd_names


def test_parse_balance_invalid_period_returns_error():
    """A file whose row 3 doesn't match any period format must return
    ok=False with a diagnostic — never silently emit zero clients.
    """
    file_bytes = _write_balance_xls(
        header_text="Оборотно-сальдовая по счету 40.10",
        period_text="this is not a period line",
        data_rows=[("CLIENT", 0, 0, 0, 0, 100, 0)],
    )
    result = parse_balance_xls(file_bytes)
    assert result["ok"] is False
    assert result.get("error"), "ok=False with no error message is silent failure"
    assert "period" in result["error"].lower()
