"""Import client balances from 1C оборотно-сальдовая ведомость (turnover balance sheet).

Parses XLS files exported from 1C account 40.10 and upserts client balance
snapshots into the client_balances table.

File format (7 columns):
    Row 0-5: Headers
    Row 3: Period line "за DD.MM.YY - DD.MM.YY"
    Row 6+: Data rows
        - Non-indented = client aggregate row
        - Indented (starts with spaces) = per-contract breakdown (ignored)
    Last 2 rows: Totals (Итого развернутое, Итого)

Columns: name | opening_debit | opening_credit | period_debit | period_credit | closing_debit | closing_credit

1C is the single source of truth — all financial data is accepted as-is.
Uses cp1251 encoding override for XLS files from 1C.
"""
import io
import re
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from backend.database import get_db

logger = logging.getLogger(__name__)

# Months in Russian (abbreviated as they appear in filenames)
_RU_MONTHS = {
    'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6,
    'июл': 7, 'авг': 8, 'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12,
}

# Full month names for "за Март 2026 г." format
_RU_MONTH_FULL = {
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
    'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12,
}

# Account to currency mapping
_ACCOUNT_CURRENCY = {
    '40.10': 'UZS',
    '40.11': 'USD',
}


def _parse_date_ru(date_str: str) -> Optional[str]:
    """Parse date like '01.03.26' to '2026-03-01'."""
    m = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})', date_str.strip())
    if not m:
        return None
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if year < 100:
        year += 2000
    return f"{year:04d}-{month:02d}-{day:02d}"


def _parse_period(period_text: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse period line. Supports two formats:
    1. 'за 01.03.26 - 30.03.26' (UZS format)
    2. 'за Март 2026 г.' (USD format)
    """
    import calendar

    # Format 1: explicit date range
    m = re.search(r'за\s+(\d{1,2}\.\d{1,2}\.\d{2,4})\s*-\s*(\d{1,2}\.\d{1,2}\.\d{2,4})', period_text)
    if m:
        return _parse_date_ru(m.group(1)), _parse_date_ru(m.group(2))

    # Format 2: "за Март 2026 г." — derive first/last day of month
    for month_name, month_num in _RU_MONTH_FULL.items():
        if month_name.lower() in period_text.lower():
            year_m = re.search(r'(\d{4})', period_text)
            if year_m:
                year = int(year_m.group(1))
                last_day = calendar.monthrange(year, month_num)[1]
                return f"{year:04d}-{month_num:02d}-01", f"{year:04d}-{month_num:02d}-{last_day:02d}"

    return None, None


def _parse_number(val) -> float:
    """Parse a cell value to float. Returns 0 for empty/space values."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s or s == ' ':
        return 0.0
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def parse_balance_xls(file_bytes: bytes) -> dict:
    """Parse оборотно-сальдовая XLS file and return structured data.

    Returns dict with:
        - period_start, period_end: ISO date strings
        - clients: list of dicts with client balance data
        - totals: summary row
    """
    try:
        import xlrd
    except ImportError:
        return {"ok": False, "error": "xlrd module not installed (needed for .xls files)"}

    try:
        wb = xlrd.open_workbook(file_contents=file_bytes, encoding_override='cp1251')
    except Exception as e:
        return {"ok": False, "error": f"Failed to open XLS file: {e}"}

    sh = wb.sheet_by_index(0)

    if sh.nrows < 7:
        return {"ok": False, "error": "File too short — expected оборотно-сальдовая format"}

    # Detect currency from account number in row 1
    header_text = str(sh.cell_value(1, 0))
    currency = 'UZS'  # default
    for account, cur in _ACCOUNT_CURRENCY.items():
        if account in header_text:
            currency = cur
            break

    # Parse period from row 3
    period_text = str(sh.cell_value(3, 0))
    period_start, period_end = _parse_period(period_text)
    if not period_start or not period_end:
        return {"ok": False, "error": f"Could not parse period from: '{period_text}'"}

    # Skip rows that are currency breakdown labels (in USD files)
    _SKIP_LABELS = {'Валюта USD', 'Валюта EUR', 'В валюте', 'Итого', 'Итого развернутое', ''}

    # For USD files, we need to aggregate "В валюте" rows per client
    # For UZS files, just read client-level rows directly
    clients = []

    if currency == 'UZS':
        # Simple format: each non-indented row is a client
        for i in range(6, sh.nrows):
            name = str(sh.cell_value(i, 0))
            if name.startswith('   '):
                continue
            if name.strip() in _SKIP_LABELS:
                continue

            client_name = name.strip()
            if not client_name:
                continue

            clients.append({
                "client_name_1c": client_name,
                "opening_debit": _parse_number(sh.cell_value(i, 1)),
                "opening_credit": _parse_number(sh.cell_value(i, 2)),
                "period_debit": _parse_number(sh.cell_value(i, 3)),
                "period_credit": _parse_number(sh.cell_value(i, 4)),
                "closing_debit": _parse_number(sh.cell_value(i, 5)),
                "closing_credit": _parse_number(sh.cell_value(i, 6)),
            })
    else:
        # USD format structure per client:
        #   ClientName       (non-indented, all values empty)
        #   Валюта USD       (currency label)
        #   В валюте         (AGGREGATE values - this is what we capture)
        #      <contract>    (indented sub-contract)
        #   Валюта USD       (sub-contract currency)
        #   В валюте         (sub-contract values - skip these)
        #
        # Strategy: capture the FIRST "В валюте" after each client name.
        current_client = None
        got_aggregate = False  # True after capturing first "В валюте" for current client

        for i in range(6, sh.nrows):
            raw_name = str(sh.cell_value(i, 0))
            name = raw_name.strip()

            if not name or name in ('Итого', 'Итого развернутое'):
                continue

            # "В валюте" row — capture only the first one per client (aggregate)
            if name == 'В валюте' and current_client and not got_aggregate:
                current_client["opening_debit"] = _parse_number(sh.cell_value(i, 1))
                current_client["opening_credit"] = _parse_number(sh.cell_value(i, 2))
                current_client["period_debit"] = _parse_number(sh.cell_value(i, 3))
                current_client["period_credit"] = _parse_number(sh.cell_value(i, 4))
                current_client["closing_debit"] = _parse_number(sh.cell_value(i, 5))
                current_client["closing_credit"] = _parse_number(sh.cell_value(i, 6))
                got_aggregate = True
                continue

            # Skip currency labels, sub-row "В валюте", and indented rows
            if name == 'В валюте' or name.startswith('Валюта') or raw_name.startswith('   '):
                continue

            # New client name row (non-indented, not a label)
            if current_client and current_client["client_name_1c"]:
                clients.append(current_client)

            current_client = {
                "client_name_1c": name,
                "opening_debit": 0, "opening_credit": 0,
                "period_debit": 0, "period_credit": 0,
                "closing_debit": 0, "closing_credit": 0,
            }
            got_aggregate = False

        # Don't forget the last client
        if current_client and current_client["client_name_1c"]:
            clients.append(current_client)

    return {
        "ok": True,
        "period_start": period_start,
        "period_end": period_end,
        "period_text": period_text.strip(),
        "currency": currency,
        "clients": clients,
    }


def _try_match_client(client_name_1c: str, conn) -> Optional[int]:
    """Try to match a 1C client name to an allowed_clients record.

    Matching strategy:
    1. Exact match on client_id_1c (if populated)
    2. Normalized name match on allowed_clients.name
    Returns allowed_clients.id or None.
    """
    # 1. Check if any allowed_client has this as their client_id_1c
    row = conn.execute(
        "SELECT id FROM allowed_clients WHERE client_id_1c = ? LIMIT 1",
        (client_name_1c,),
    ).fetchone()
    if row:
        return row[0]

    # 2. Normalized name matching (lowercase, stripped)
    normalized = client_name_1c.strip().lower()
    row = conn.execute(
        "SELECT id FROM allowed_clients WHERE LOWER(TRIM(name)) = ? LIMIT 1",
        (normalized,),
    ).fetchone()
    if row:
        return row[0]

    return None


def apply_balance_import(file_bytes: bytes) -> dict:
    """Parse balance XLS and upsert into client_balances table.

    Returns detailed summary of the import.
    """
    parsed = parse_balance_xls(file_bytes)
    if not parsed.get("ok"):
        return parsed

    period_start = parsed["period_start"]
    period_end = parsed["period_end"]
    currency = parsed.get("currency", "UZS")
    clients = parsed["clients"]

    if not clients:
        return {"ok": False, "error": "No client data found in file"}

    conn = get_db()

    inserted = 0
    updated = 0
    matched = 0
    unmatched_names = []

    for c in clients:
        # Try to match client to allowed_clients
        client_id = _try_match_client(c["client_name_1c"], conn)
        if client_id:
            matched += 1
        else:
            unmatched_names.append(c["client_name_1c"])

        # Upsert balance record (unique on client_name + period + currency)
        existing = conn.execute(
            "SELECT id FROM client_balances WHERE client_name_1c = ? AND period_start = ? AND currency = ?",
            (c["client_name_1c"], period_start, currency),
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE client_balances SET
                    client_id = ?,
                    period_end = ?,
                    opening_debit = ?, opening_credit = ?,
                    period_debit = ?, period_credit = ?,
                    closing_debit = ?, closing_credit = ?,
                    imported_at = datetime('now')
                   WHERE id = ?""",
                (
                    client_id, period_end,
                    c["opening_debit"], c["opening_credit"],
                    c["period_debit"], c["period_credit"],
                    c["closing_debit"], c["closing_credit"],
                    existing[0],
                ),
            )
            updated += 1
        else:
            conn.execute(
                """INSERT INTO client_balances
                   (client_name_1c, client_id, currency, period_start, period_end,
                    opening_debit, opening_credit, period_debit, period_credit,
                    closing_debit, closing_credit)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    c["client_name_1c"], client_id, currency, period_start, period_end,
                    c["opening_debit"], c["opening_credit"],
                    c["period_debit"], c["period_credit"],
                    c["closing_debit"], c["closing_credit"],
                ),
            )
            inserted += 1

    conn.commit()

    # Count total unique clients in DB
    total_clients = conn.execute(
        "SELECT COUNT(DISTINCT client_name_1c) FROM client_balances"
    ).fetchone()[0]
    total_periods = conn.execute(
        "SELECT COUNT(DISTINCT period_start) FROM client_balances"
    ).fetchone()[0]

    conn.close()

    return {
        "ok": True,
        "period": parsed["period_text"],
        "period_start": period_start,
        "period_end": period_end,
        "currency": currency,
        "total_clients_in_file": len(clients),
        "inserted": inserted,
        "updated": updated,
        "matched_to_app": matched,
        "unmatched_count": len(unmatched_names),
        "unmatched_sample": unmatched_names[:20],
        "db_total_clients": total_clients,
        "db_total_periods": total_periods,
    }


def get_client_balance(client_id: int) -> Optional[dict]:
    """Get the latest balance for a client by their allowed_clients.id.

    Returns balances per currency (UZS and/or USD), using the most recent
    period for each currency.
    """
    conn = get_db()

    # Get latest balance per currency
    rows = conn.execute(
        """SELECT cb.client_name_1c, cb.currency, cb.period_start, cb.period_end,
                  cb.opening_debit, cb.opening_credit,
                  cb.period_debit, cb.period_credit,
                  cb.closing_debit, cb.closing_credit,
                  cb.imported_at
           FROM client_balances cb
           INNER JOIN (
               SELECT client_id, currency, MAX(period_start) as max_period
               FROM client_balances
               WHERE client_id = ?
               GROUP BY client_id, currency
           ) latest ON cb.client_id = latest.client_id
                   AND cb.currency = latest.currency
                   AND cb.period_start = latest.max_period""",
        (client_id,),
    ).fetchall()
    conn.close()

    if not rows:
        return None

    balances = {}
    imported_at = None
    client_name_1c = None

    for row in rows:
        cur = row["currency"]
        closing_balance = (row["closing_debit"] or 0) - (row["closing_credit"] or 0)
        client_name_1c = row["client_name_1c"]
        imported_at = row["imported_at"]

        balances[cur] = {
            "currency": cur,
            "period_start": row["period_start"],
            "period_end": row["period_end"],
            "closing_debit": row["closing_debit"],
            "closing_credit": row["closing_credit"],
            "balance": closing_balance,
            "period_debit": row["period_debit"],
            "period_credit": row["period_credit"],
        }

    # For backward compatibility, also include top-level UZS balance
    uzs = balances.get("UZS", {})
    return {
        "client_name_1c": client_name_1c,
        "period_start": uzs.get("period_start", ""),
        "period_end": uzs.get("period_end", ""),
        "closing_debit": uzs.get("closing_debit", 0),
        "closing_credit": uzs.get("closing_credit", 0),
        "balance": uzs.get("balance", 0),
        "period_debit": uzs.get("period_debit", 0),
        "period_credit": uzs.get("period_credit", 0),
        "imported_at": imported_at,
        "balances_by_currency": balances,
    }


def get_client_balance_history(client_id: int, limit: int = 12) -> List[dict]:
    """Get balance history for a client (last N periods)."""
    conn = get_db()
    rows = conn.execute(
        """SELECT period_start, period_end,
                  opening_debit, opening_credit,
                  period_debit, period_credit,
                  closing_debit, closing_credit
           FROM client_balances
           WHERE client_id = ?
           ORDER BY period_start DESC
           LIMIT ?""",
        (client_id, limit),
    ).fetchall()
    conn.close()

    return [
        {
            "period_start": r["period_start"],
            "period_end": r["period_end"],
            "period_debit": r["period_debit"],
            "period_credit": r["period_credit"],
            "balance": (r["closing_debit"] or 0) - (r["closing_credit"] or 0),
        }
        for r in rows
    ]
