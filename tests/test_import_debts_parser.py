"""Smoke test for the дебиторская задолженность (1C debtors) parser
and its regression guard.

Pins the parsing rules:
  - .xls + cp1251 (xlrd with encoding_override)
  - Title parsed from row 1 col 1 (or row 1 col 0 fallback)
  - Data starts at row 4; col 2 = name, col 5 = UZS debt, col 6 = USD debt
  - Aging buckets at cols 7-11 (0-30, 31-60, 61-90, 91-120, 120+)
  - Rows with both currencies = 0 are dropped silently
  - Parsing stops at row whose name starts with 'ВСЕГО'

Plus pins the SCHEMA_DRIFT regression guard (`_check_debtors_regression`)
from Error Log #20. Thresholds chosen to catch the Apr 2026 incident
(«В Валюте» column vanished, $187,991 USD silently dropped to $0).

Uses xlwt + xlrd (test-only via requirements-dev.txt).
"""
import io

import xlwt

from backend.services.import_debts import (
    _check_debtors_regression,
    parse_debtors_xls,
)


def _new_xls() -> tuple[xlwt.Workbook, xlwt.Worksheet]:
    wb = xlwt.Workbook(encoding="cp1251")
    ws = wb.add_sheet("Sheet1")
    return wb, ws


def _to_bytes(wb: xlwt.Workbook) -> bytes:
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _write_debtors_xls(*, title: str, data_rows: list[dict]) -> bytes:
    """Build a minimal debtors .xls fixture.

    Row layout:
        Row 0: blank
        Row 1: title in col 1 ('Дебиторская задолженность на DD Month YYYY г.')
        Row 2: blank
        Row 3: column headers (parser starts reading at row 4)
        Row 4+: data — col2=name, col3=trans_date_ddmmyy, col4=trans_no,
                col5=UZS, col6=USD, col7..11=aging buckets

    Each dict in `data_rows` may set: name, debt_uzs, debt_usd,
    trans_date (DDMMYY str/int), trans_no, aging_0_30, aging_31_60,
    aging_61_90, aging_91_120, aging_120_plus.
    """
    wb, ws = _new_xls()
    ws.write(1, 1, title)
    ws.write(3, 2, "Контрагент")
    for offset, row in enumerate(data_rows):
        r = 4 + offset
        # xlwt forbids overwriting cells, so write each column exactly once.
        for c in range(12):
            if c == 2:
                ws.write(r, c, row.get("name", ""))
            elif c == 3:
                ws.write(r, c, row.get("trans_date", 0))
            elif c == 4:
                ws.write(r, c, row.get("trans_no", ""))
            elif c == 5:
                ws.write(r, c, row.get("debt_uzs", 0))
            elif c == 6:
                ws.write(r, c, row.get("debt_usd", 0))
            elif c == 7:
                ws.write(r, c, row.get("aging_0_30", 0))
            elif c == 8:
                ws.write(r, c, row.get("aging_31_60", 0))
            elif c == 9:
                ws.write(r, c, row.get("aging_61_90", 0))
            elif c == 10:
                ws.write(r, c, row.get("aging_91_120", 0))
            elif c == 11:
                ws.write(r, c, row.get("aging_120_plus", 0))
            else:
                ws.write(r, c, "")
    return _to_bytes(wb)


# ── Parser tests ─────────────────────────────────────────────────────────


def test_parse_debtors_dual_currency():
    """Three rows: UZS-only, USD-only, dual. All must land in `clients`."""
    file_bytes = _write_debtors_xls(
        title="Дебиторская задолженность на 31 марта 2026 г.",
        data_rows=[
            {
                "name": "ALPHA",
                "debt_uzs": 5_000_000,
                "debt_usd": 0,
                "trans_date": 150326,           # 2026-03-15
                "trans_no": "R-100",
                "aging_0_30": 5_000_000,
            },
            {
                "name": "BETA",
                "debt_uzs": 0,
                "debt_usd": 1_200.50,
                "trans_date": 200326,
                "aging_31_60": 1_200.50,
            },
            {
                "name": "GAMMA",
                "debt_uzs": 2_500_000,
                "debt_usd": 300,
                "aging_120_plus": 2_500_000,
            },
        ],
    )
    result = parse_debtors_xls(file_bytes)
    assert result["ok"] is True, f"parse failed: {result.get('error')}"
    assert result["report_date"] == "2026-03-31"
    clients = result["clients"]
    assert len(clients) == 3, f"expected 3 clients, got {len(clients)}: {[c['client_name_1c'] for c in clients]}"

    alpha = clients[0]
    assert alpha["client_name_1c"] == "ALPHA"
    assert alpha["debt_uzs"] == 5_000_000
    assert alpha["debt_usd"] == 0
    assert alpha["last_transaction_date"] == "2026-03-15"
    assert alpha["last_transaction_no"] == "R-100"
    assert alpha["aging_0_30"] == 5_000_000

    beta = clients[1]
    assert beta["debt_uzs"] == 0
    assert beta["debt_usd"] == 1_200.50
    assert beta["aging_31_60"] == 1_200.50
    # last_transaction_no left blank — parser must yield None, not "0"
    assert beta["last_transaction_no"] is None

    gamma = clients[2]
    assert gamma["debt_uzs"] == 2_500_000
    assert gamma["debt_usd"] == 300


def test_parse_debtors_skips_zero_rows():
    """A client with both debts = 0 (defensive case) must be dropped silently."""
    file_bytes = _write_debtors_xls(
        title="Дебиторская задолженность на 1 апреля 2026 г.",
        data_rows=[
            {"name": "ZERO ROW", "debt_uzs": 0, "debt_usd": 0},
            {"name": "REAL DEBTOR", "debt_uzs": 100, "debt_usd": 0},
        ],
    )
    result = parse_debtors_xls(file_bytes)
    assert result["ok"] is True
    assert len(result["clients"]) == 1
    assert result["clients"][0]["client_name_1c"] == "REAL DEBTOR"


def test_parse_debtors_stops_at_vsego():
    """The 'ВСЕГО:' totals row terminates parsing — anything after it
    (including the totals values themselves) must NOT be parsed as a client.
    """
    file_bytes = _write_debtors_xls(
        title="Дебиторская задолженность на 1 апреля 2026 г.",
        data_rows=[
            {"name": "CLIENT ONE", "debt_uzs": 100, "debt_usd": 0},
            {"name": "ВСЕГО:", "debt_uzs": 999_999, "debt_usd": 9999},  # totals
            {"name": "POST-TOTALS LEAK", "debt_uzs": 50, "debt_usd": 0},
        ],
    )
    result = parse_debtors_xls(file_bytes)
    assert result["ok"] is True
    names = [c["client_name_1c"] for c in result["clients"]]
    assert names == ["CLIENT ONE"], f"unexpected clients past ВСЕГО: {names}"


def test_parse_debtors_invalid_title_returns_error():
    """Title row without a parseable date must return ok=False — never a
    silent ok=True with empty clients (operator would think 'no debtors today').
    """
    file_bytes = _write_debtors_xls(
        title="this is not a debtors report title",
        data_rows=[{"name": "X", "debt_uzs": 100, "debt_usd": 0}],
    )
    result = parse_debtors_xls(file_bytes)
    assert result["ok"] is False
    assert result.get("error"), "ok=False with no error is silent failure"
    assert "date" in result["error"].lower() or "title" in result["error"].lower() or "parse" in result["error"].lower()


# ── Regression guard tests (Error Log #20 SCHEMA_DRIFT_SILENT_LOSS) ─────


def test_regression_guard_catches_usd_collapse():
    """USD>$1000 collapsing to <10% triggers — the exact shape of the
    Apr 2026 incident when «В Валюте» column vanished from the 1C export.
    """
    reasons = _check_debtors_regression(
        prev={"uzs": 5_000_000, "usd": 187_991, "rows": 140},
        new_uzs=5_000_000,
        new_usd=0,           # collapsed
        new_rows=140,
    )
    assert reasons, "expected regression to fire on 100% USD drop"
    assert any("USD" in r for r in reasons)
    assert any("В Валюте" in r for r in reasons)


def test_regression_guard_catches_uzs_collapse():
    """UZS>1M collapsing to <30% triggers."""
    reasons = _check_debtors_regression(
        prev={"uzs": 100_000_000, "usd": 0, "rows": 100},
        new_uzs=10_000_000,   # 10% of prev → 90% drop, well past the 70% threshold
        new_usd=0,
        new_rows=100,
    )
    assert reasons, "expected regression to fire on 90% UZS drop"
    assert any("UZS" in r for r in reasons)


def test_regression_guard_catches_row_collapse():
    """Row count >50 collapsing to <50% triggers — catches accidental filter."""
    reasons = _check_debtors_regression(
        prev={"uzs": 5_000_000, "usd": 1000, "rows": 200},
        new_uzs=5_000_000,
        new_usd=1000,
        new_rows=80,          # 40% of prev → 60% drop, past the 50% threshold
    )
    assert reasons
    assert any("Row count" in r or "row" in r.lower() for r in reasons)


def test_regression_guard_lets_normal_fluctuation_through():
    """Day-over-day swings within the expected band must NOT flag."""
    reasons = _check_debtors_regression(
        prev={"uzs": 100_000_000, "usd": 50_000, "rows": 150},
        new_uzs=95_000_000,   # 5% UZS drop
        new_usd=48_000,       # 4% USD drop
        new_rows=148,         # 1% row drop
    )
    assert reasons == [], f"unexpected regression on normal fluctuation: {reasons}"


def test_regression_guard_ignores_tiny_previous_values():
    """Thresholds gate on prev_usd>1000 / prev_uzs>1M / prev_rows>50 so a
    tiny historical base doesn't flag every test run or fresh deploy.
    """
    reasons = _check_debtors_regression(
        prev={"uzs": 500_000, "usd": 500, "rows": 10},
        new_uzs=0, new_usd=0, new_rows=0,   # absolute collapse, but base too small
    )
    assert reasons == [], "should not flag on tiny historical base"
