"""Import Касса (cash receipts journal) from 1C "Приходный кассовый ордер" export.

This is the payment-dates register — the source of truth for when and how
much a client actually paid us. Session G's credit-score system will layer
on top of this table.

File format (XLS, cp1251 or XLSX):
    Flat journal — one row per payment document. Each row is marked with a
    "V" (or similar single-char) in column 0. Pre-header metadata rows and
    trailing totals rows have a blank column 0.

Columns (after the V marker):
    Номер, Дата, Время, Автор, Принято от, Основание, Приложение,
    Корреспондирующий счет, Субконто1, Субконто2, Субконто3,
    Формировать проводки, Сумма, Валютный, Валюта,
    Движение денежных средств, Курс, ВалСумма

Currency detection:
    Корреспондирующий счет = 40.10 → UZS payment (use Сумма)
    Корреспондирующий счет = 40.11 → USD payment (use ВалСумма)

Idempotency: UNIQUE on `doc_number_1c`. Re-uploading the same file replaces
rows via INSERT OR REPLACE. Morning and evening cash files have disjoint
document numbers, so both persist.
"""
from __future__ import annotations

import io
import re
import logging
from datetime import datetime, time
from typing import Dict, List, Optional, Tuple

from backend.database import get_db

# Reuse the well-tested workbook loader + helpers from Session E's parser.
from backend.services.import_real_orders import (
    _load_workbook,
    _norm,
    _parse_number,
    _parse_doc_date,
    _parse_doc_time,
    _dump_first_rows,
    _build_column_map,
    _Sheet,
)
from backend.services.import_balances import _try_match_client

logger = logging.getLogger(__name__)


_CASH_FIELDS: Dict[str, List[str]] = {
    "doc_number_1c":   ["номер", "№"],
    "doc_date":        ["дата"],
    "doc_time":        ["время"],
    "author":          ["автор"],
    "received_from":   ["принято от", "принято"],
    "basis":           ["основание"],
    "attachment":      ["приложение"],
    "corr_account":    ["корреспондирующий счет", "корр. счет", "корсчет", "счет"],
    "subconto1":       ["субконто1", "субконто 1"],
    "subconto2":       ["субконто2", "субконто 2"],
    "subconto3":       ["субконто3", "субконто 3"],
    "amount_local":    ["сумма"],
    "currency_code":   ["валюта"],
    "cashflow_category":["движение денежных средств", "движение дс"],
    "fx_rate":         ["курс"],
    "amount_currency": ["валсумма", "вал. сумма", "сумма вал", "суммавал"],
}


def _find_cash_header_row(sh: _Sheet) -> Optional[int]:
    """Locate the column-header row.

    Heuristic: within the first ~30 rows, find a row that contains both
    "Субконто1" and "Корреспондирующий счет" (or at least one of them along
    with "Номер" and "Дата").
    """
    max_scan = min(30, sh.nrows)
    for r in range(max_scan):
        cells = [_norm(sh.cell(r, c)) for c in range(sh.ncols)]
        joined = " | ".join(cells)
        has_subconto = "субконто1" in joined or "субконто 1" in joined
        has_corr = "корреспондирующий" in joined or "корсчет" in joined
        has_basic = "номер" in joined and "дата" in joined and "сумма" in joined
        if (has_subconto and has_basic) or (has_corr and has_basic):
            return r
    return None


def _is_data_row(sh: _Sheet, r: int) -> bool:
    """A payment row has a non-empty marker in column 0 (usually 'V')."""
    v = sh.cell(r, 0)
    if v is None:
        return False
    s = str(v).strip()
    return len(s) > 0


def _detect_currency(corr_account: str) -> str:
    """Derive 'UZS' or 'USD' from the corresponding-account string.

    1C uses 40.10 for UZS cash receipts and 40.11 for USD. Some files print
    the code as '40.10', '4010', '40.10.0', etc. We only care about the
    first few digits after stripping punctuation.
    """
    if not corr_account:
        return "UZS"
    s = re.sub(r"[^\d]", "", str(corr_account))
    if s.startswith("4011") or s.startswith("4 011"):
        return "USD"
    if s.startswith("4010"):
        return "UZS"
    # Fall back to the currency-code column interpretation done by the caller
    return "UZS"


def parse_cash_xls(file_bytes: bytes, filename_hint: str = "") -> dict:
    """Parse a 1C Касса file into a list of payment rows.

    Returns dict with keys:
        ok (bool), error (str, optional),
        payments (list of dict), stats (dict)
    """
    sh, err = _load_workbook(file_bytes, filename_hint)
    if err:
        return {"ok": False, "error": err}
    if sh.nrows < 3:
        return {"ok": False, "error": "File too short (< 3 rows)"}

    header_row = _find_cash_header_row(sh)
    if header_row is None:
        return {
            "ok": False,
            "error": (
                "Could not find Касса header row — expected columns like "
                "'Субконто1', 'Корреспондирующий счет', 'Сумма', 'Номер', 'Дата'."
            ),
            "diagnostics": _dump_first_rows(sh, 20),
        }

    col_map = _build_column_map(sh, header_row, _CASH_FIELDS)

    required = ["doc_number_1c", "doc_date", "amount_local"]
    missing = [f for f in required if f not in col_map]
    if missing:
        return {
            "ok": False,
            "error": f"Missing required columns: {', '.join(missing)}",
            "diagnostics": _dump_first_rows(sh, 20),
        }

    payments: List[dict] = []
    total_uzs = 0.0
    total_usd = 0.0
    dates: List[str] = []
    clients_seen: set = set()

    for r in range(header_row + 1, sh.nrows):
        if not _is_data_row(sh, r):
            continue

        doc_number = sh.cell(r, col_map["doc_number_1c"])
        if doc_number is None or str(doc_number).strip() == "":
            continue
        doc_number = str(doc_number).strip()

        doc_date = _parse_doc_date(sh.cell(r, col_map["doc_date"]))
        if not doc_date:
            continue  # rows without a date are junk (totals, separators)

        corr_account = sh.cell(r, col_map.get("corr_account", -1)) if col_map.get("corr_account") is not None else None
        corr_account_str = str(corr_account).strip() if corr_account is not None else ""

        amount_local = _parse_number(sh.cell(r, col_map["amount_local"]))
        amount_currency = _parse_number(sh.cell(r, col_map["amount_currency"])) if "amount_currency" in col_map else 0.0

        currency = _detect_currency(corr_account_str)

        subconto1 = sh.cell(r, col_map.get("subconto1", -1)) if col_map.get("subconto1") is not None else None
        client_name_1c = str(subconto1).strip() if subconto1 is not None else ""

        received_from = sh.cell(r, col_map.get("received_from", -1)) if col_map.get("received_from") is not None else None

        p = {
            "doc_number_1c": doc_number,
            "doc_date": doc_date,
            "doc_time": _parse_doc_time(sh.cell(r, col_map.get("doc_time", -1))) if "doc_time" in col_map else None,
            "author": _str_or_none(sh.cell(r, col_map.get("author", -1))) if "author" in col_map else None,
            "received_from": _str_or_none(received_from),
            "basis": _str_or_none(sh.cell(r, col_map.get("basis", -1))) if "basis" in col_map else None,
            "attachment": _str_or_none(sh.cell(r, col_map.get("attachment", -1))) if "attachment" in col_map else None,
            "corr_account": corr_account_str or None,
            "client_name_1c": client_name_1c or None,
            "subconto2": _str_or_none(sh.cell(r, col_map.get("subconto2", -1))) if "subconto2" in col_map else None,
            "subconto3": _str_or_none(sh.cell(r, col_map.get("subconto3", -1))) if "subconto3" in col_map else None,
            "currency": currency,
            "amount_local": amount_local,
            "amount_currency": amount_currency,
            "fx_rate": _parse_number(sh.cell(r, col_map.get("fx_rate", -1))) if "fx_rate" in col_map else 0.0,
            "cashflow_category": _str_or_none(sh.cell(r, col_map.get("cashflow_category", -1))) if "cashflow_category" in col_map else None,
        }
        payments.append(p)
        dates.append(doc_date)
        if client_name_1c:
            clients_seen.add(client_name_1c)
        if currency == "USD":
            total_usd += amount_currency or 0.0
        else:
            total_uzs += amount_local or 0.0

    stats = {
        "row_count": len(payments),
        "date_min": min(dates) if dates else None,
        "date_max": max(dates) if dates else None,
        "client_count": len(clients_seen),
        "total_uzs": total_uzs,
        "total_usd": total_usd,
    }
    return {"ok": True, "payments": payments, "stats": stats}


def _str_or_none(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def apply_cash_import(file_bytes: bytes, filename_hint: str = "") -> dict:
    """Parse a Касса file and upsert every payment row.

    Idempotent on doc_number_1c. Re-uploading the same file is a no-op at
    the row level (INSERT OR REPLACE). Morning and evening files have
    disjoint document numbers so both sets of rows persist.
    """
    parsed = parse_cash_xls(file_bytes, filename_hint)
    if not parsed.get("ok"):
        return parsed

    payments = parsed["payments"]
    stats = parsed["stats"]

    if not payments:
        return {"ok": False, "error": "No payment rows found in file"}

    conn = get_db()
    try:
        inserted = 0
        updated = 0
        matched_clients = 0
        client_cache: Dict[str, Optional[int]] = {}

        for p in payments:
            client_name = p.get("client_name_1c") or ""
            client_id = None
            if client_name:
                if client_name not in client_cache:
                    client_cache[client_name] = _try_match_client(client_name, conn)
                client_id = client_cache[client_name]
                if client_id is not None:
                    matched_clients += 1

            # Use composite (doc_number_1c, doc_date) — doc numbers cycle per year
            existing = conn.execute(
                "SELECT id FROM client_payments WHERE doc_number_1c = ? AND doc_date = ?",
                (p["doc_number_1c"], p["doc_date"]),
            ).fetchone()

            if existing:
                conn.execute(
                    """UPDATE client_payments SET
                        doc_date=?, doc_time=?, author=?, received_from=?,
                        basis=?, attachment=?, corr_account=?,
                        client_name_1c=?, client_id=?,
                        subconto2=?, subconto3=?,
                        currency=?, amount_local=?, amount_currency=?,
                        fx_rate=?, cashflow_category=?,
                        imported_at=datetime('now')
                      WHERE id=?""",
                    (
                        p["doc_date"], p.get("doc_time"), p.get("author"), p.get("received_from"),
                        p.get("basis"), p.get("attachment"), p.get("corr_account"),
                        client_name or None, client_id,
                        p.get("subconto2"), p.get("subconto3"),
                        p.get("currency"), p.get("amount_local"), p.get("amount_currency"),
                        p.get("fx_rate"), p.get("cashflow_category"),
                        existing[0],
                    ),
                )
                updated += 1
            else:
                conn.execute(
                    """INSERT INTO client_payments
                       (doc_number_1c, doc_date, doc_time, author, received_from,
                        basis, attachment, corr_account,
                        client_name_1c, client_id, subconto2, subconto3,
                        currency, amount_local, amount_currency,
                        fx_rate, cashflow_category)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        p["doc_number_1c"], p["doc_date"], p.get("doc_time"),
                        p.get("author"), p.get("received_from"),
                        p.get("basis"), p.get("attachment"), p.get("corr_account"),
                        client_name or None, client_id,
                        p.get("subconto2"), p.get("subconto3"),
                        p.get("currency"), p.get("amount_local"), p.get("amount_currency"),
                        p.get("fx_rate"), p.get("cashflow_category"),
                    ),
                )
                inserted += 1

        # Post-import orphan heal — see import_balances.py for rationale.
        from backend.services.client_search import heal_finance_orphans_by_1c_name
        orphans_healed = heal_finance_orphans_by_1c_name(conn, "client_payments")

        conn.commit()

        db_total = conn.execute("SELECT COUNT(*) FROM client_payments").fetchone()[0]

        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "matched_clients": matched_clients,
            "orphans_healed": orphans_healed,
            "stats": stats,
            "db_total": db_total,
        }
    finally:
        conn.close()
