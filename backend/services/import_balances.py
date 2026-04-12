"""Import client balances from 1C оборотно-сальдовая ведомость (turnover balance sheet).

Parses XLS files exported from 1C and upserts client balance snapshots
into the client_balances table.

Supports three file formats:
    1. Single account file (40.10 or 40.11) — one currency per file
    2. Combined file (счет 40) with sub-account sections — "40.10" and "40.11"
       rows act as section dividers, each section parsed with its own currency

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


def _parse_clients_simple(sh, start_row: int, end_row: int) -> list:
    """Parse client rows in simple (UZS / combined) format.

    Each non-indented row is a client; indented rows are sub-details (skipped).
    """
    _SKIP = {'Валюта USD', 'Валюта EUR', 'В валюте', 'Итого', 'Итого развернутое', ''}
    clients = []
    for i in range(start_row, end_row):
        name = str(sh.cell_value(i, 0))
        if name.startswith('   '):
            continue
        client_name = name.strip()
        if not client_name or client_name in _SKIP:
            continue
        # Skip sub-account section headers like "40.10", "40.11"
        if client_name in _ACCOUNT_CURRENCY:
            continue
        # Skip <...> and <> marker rows at section level
        if client_name.startswith('<'):
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
    return clients


def _parse_clients_usd(sh, start_row: int, end_row: int) -> list:
    """Parse client rows in USD format (with 'В валюте' aggregate rows)."""
    clients = []
    current_client = None
    got_aggregate = False

    for i in range(start_row, end_row):
        raw_name = str(sh.cell_value(i, 0))
        name = raw_name.strip()

        if not name or name in ('Итого', 'Итого развернутое'):
            continue
        if name in _ACCOUNT_CURRENCY:
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

        # New client name row
        if current_client and current_client["client_name_1c"]:
            clients.append(current_client)

        current_client = {
            "client_name_1c": name,
            "opening_debit": 0, "opening_credit": 0,
            "period_debit": 0, "period_credit": 0,
            "closing_debit": 0, "closing_credit": 0,
        }
        got_aggregate = False

    if current_client and current_client["client_name_1c"]:
        clients.append(current_client)

    return clients


def parse_balance_xls(file_bytes: bytes) -> dict:
    """Parse оборотно-сальдовая XLS file and return structured data.

    Supports three formats:
    1. Single-account file (header contains "40.10" or "40.11")
    2. Combined file (header contains "40") with "40.10" and "40.11" section rows
    3. Combined file without section markers — treated as UZS

    Returns dict with:
        - sections: list of {currency, clients} dicts (one per currency found)
        - period_start, period_end: ISO date strings
    For backward compatibility, also includes top-level 'currency' and 'clients'
    (from the first/only section).
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

    # Parse period from row 3
    period_text = str(sh.cell_value(3, 0))
    period_start, period_end = _parse_period(period_text)
    if not period_start or not period_end:
        return {"ok": False, "error": f"Could not parse period from: '{period_text}'"}

    # Detect format: single account or combined
    header_text = str(sh.cell_value(1, 0))

    # Check for sub-account section rows in the data (combined format)
    section_rows = {}  # account -> row number
    for r in range(6, min(sh.nrows, sh.nrows)):
        val = str(sh.cell_value(r, 0)).strip()
        if val in _ACCOUNT_CURRENCY and val not in section_rows:
            section_rows[val] = r

    if section_rows:
        # Combined file with section dividers (e.g. "40.10" at row 6, "40.11" at row 1238)
        logger.info(f"Combined balance file detected. Sections: {section_rows}")
        sections = []

        # Sort sections by row number
        sorted_sections = sorted(section_rows.items(), key=lambda x: x[1])

        for idx, (account, start_row) in enumerate(sorted_sections):
            currency = _ACCOUNT_CURRENCY[account]
            # End row is the next section's start, or end of file (minus totals)
            if idx + 1 < len(sorted_sections):
                end_row = sorted_sections[idx + 1][1]
            else:
                end_row = sh.nrows - 2  # skip Итого rows

            # In combined files, both sections use the simple format
            # (values directly on client rows, no "В валюте" intermediary)
            clients = _parse_clients_simple(sh, start_row + 1, end_row)

            logger.info(f"Section {account} ({currency}): {len(clients)} clients")
            sections.append({"currency": currency, "clients": clients})

        # Flatten all clients for backward compatibility
        all_clients = []
        for s in sections:
            all_clients.extend(s["clients"])

        return {
            "ok": True,
            "period_start": period_start,
            "period_end": period_end,
            "period_text": period_text.strip(),
            "currency": sections[0]["currency"] if sections else "UZS",
            "clients": all_clients,  # backward compat (not used when sections present)
            "sections": sections,
        }

    # Single-account file
    currency = 'UZS'  # default
    for account, cur in _ACCOUNT_CURRENCY.items():
        if account in header_text:
            currency = cur
            break

    if currency == 'USD':
        clients = _parse_clients_usd(sh, 6, sh.nrows)
    else:
        clients = _parse_clients_simple(sh, 6, sh.nrows)

    return {
        "ok": True,
        "period_start": period_start,
        "period_end": period_end,
        "period_text": period_text.strip(),
        "currency": currency,
        "clients": clients,
        "sections": [{"currency": currency, "clients": clients}],
    }


def _try_match_client(client_name_1c: str, conn) -> Optional[int]:
    """Try to match a 1C client name to an allowed_clients record.

    Always returns the lowest ID (deterministic) and skips merged records.
    Matching strategy:
    1. Exact match on client_id_1c (if populated)
    2. Normalized name match on allowed_clients.name
    Returns allowed_clients.id or None.
    """
    # 1. Check if any allowed_client has this as their client_id_1c
    row = conn.execute(
        "SELECT id FROM allowed_clients WHERE client_id_1c = ? AND COALESCE(status, 'active') != 'merged' ORDER BY id LIMIT 1",
        (client_name_1c,),
    ).fetchone()
    if row:
        return row[0]

    # 2. Normalized name matching (lowercase, stripped)
    normalized = client_name_1c.strip().lower()
    row = conn.execute(
        "SELECT id FROM allowed_clients WHERE LOWER(TRIM(name)) = ? AND COALESCE(status, 'active') != 'merged' ORDER BY id LIMIT 1",
        (normalized,),
    ).fetchone()
    if row:
        return row[0]

    return None


def apply_balance_import(file_bytes: bytes) -> dict:
    """Parse balance XLS and upsert into client_balances table.

    Handles both single-currency and combined (multi-section) files.
    Returns detailed summary of the import.
    """
    parsed = parse_balance_xls(file_bytes)
    if not parsed.get("ok"):
        return parsed

    period_start = parsed["period_start"]
    period_end = parsed["period_end"]
    sections = parsed.get("sections", [])

    if not sections:
        return {"ok": False, "error": "No client data found in file"}

    # Check that at least one section has clients
    total_clients_in_file = sum(len(s["clients"]) for s in sections)
    if total_clients_in_file == 0:
        return {"ok": False, "error": "No client data found in file"}

    conn = get_db()

    inserted = 0
    updated = 0
    matched = 0
    unmatched_names = []
    section_summaries = []

    skipped_zero = 0

    for section in sections:
        currency = section["currency"]
        clients = section["clients"]
        sec_inserted = 0
        sec_updated = 0
        sec_matched = 0

        for c in clients:
            # Skip rows where ALL 6 financial columns are 0 — no information.
            # This prevents daily сальдо imports from creating zero-balance
            # records that mask real debt from monthly imports.
            if all(
                c[k] == 0
                for k in (
                    "opening_debit", "opening_credit",
                    "period_debit", "period_credit",
                    "closing_debit", "closing_credit",
                )
            ):
                skipped_zero += 1
                continue

            # Try to match client to allowed_clients
            client_id = _try_match_client(c["client_name_1c"], conn)
            if client_id:
                matched += 1
                sec_matched += 1
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
                sec_updated += 1
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
                sec_inserted += 1

        section_summaries.append({
            "currency": currency,
            "clients": len(clients),
            "inserted": sec_inserted,
            "updated": sec_updated,
            "matched": sec_matched,
        })

    conn.commit()

    # Count total unique clients in DB
    total_clients = conn.execute(
        "SELECT COUNT(DISTINCT client_name_1c) FROM client_balances"
    ).fetchone()[0]
    total_periods = conn.execute(
        "SELECT COUNT(DISTINCT period_start) FROM client_balances"
    ).fetchone()[0]

    conn.close()

    # Primary currency for display (first section)
    primary_currency = sections[0]["currency"] if sections else "UZS"

    return {
        "ok": True,
        "period": parsed["period_text"],
        "period_start": period_start,
        "period_end": period_end,
        "currency": primary_currency,
        "total_clients_in_file": total_clients_in_file,
        "inserted": inserted,
        "updated": updated,
        "matched_to_app": matched,
        "skipped_zero": skipped_zero,
        "unmatched_count": len(unmatched_names),
        "unmatched_sample": unmatched_names[:20],
        "db_total_clients": total_clients,
        "db_total_periods": total_periods,
        "sections": section_summaries,
    }


def get_client_balance(client_id) -> Optional[dict]:
    """Get the latest balance for a client by their allowed_clients.id.

    client_id can be a single int or a list of ints (sibling IDs for
    multi-phone clients sharing the same client_id_1c).

    Returns balances per currency (UZS and/or USD), using the most recent
    period for each currency.

    Balance = period_debit − period_credit (cumulative shipped − paid).
    This matches the Акт сверки from 1C, which is the source of truth
    for client debt. We use the middle columns (period activity) rather
    than the closing columns because the opening/closing balances in the
    оборотка are unreliable due to historical 1C configuration.
    """
    conn = get_db()

    # Normalize to list of IDs
    if isinstance(client_id, (list, tuple)):
        ids = list(client_id)
    else:
        ids = [client_id]
    placeholders = ",".join("?" * len(ids))

    # Get latest balance per currency — use MAX(period_end) so cumulative
    # records (period_end = today) take priority over older monthly ones
    rows = conn.execute(
        f"""SELECT cb.client_name_1c, cb.currency, cb.period_start, cb.period_end,
                  cb.opening_debit, cb.opening_credit,
                  cb.period_debit, cb.period_credit,
                  cb.closing_debit, cb.closing_credit,
                  cb.imported_at
           FROM client_balances cb
           INNER JOIN (
               SELECT currency, MAX(period_end) as max_end
               FROM client_balances
               WHERE client_id IN ({placeholders})
               GROUP BY currency
           ) latest ON cb.currency = latest.currency
                   AND cb.period_end = latest.max_end
           WHERE cb.client_id IN ({placeholders})""",
        (*ids, *ids),
    ).fetchall()
    conn.close()

    if not rows:
        return None

    balances = {}
    imported_at = None
    client_name_1c = None

    for row in rows:
        cur = row["currency"]
        # Balance = cumulative shipped − cumulative paid (matches Акт сверки)
        balance = (row["period_debit"] or 0) - (row["period_credit"] or 0)
        client_name_1c = row["client_name_1c"]
        imported_at = row["imported_at"]

        balances[cur] = {
            "currency": cur,
            "period_start": row["period_start"],
            "period_end": row["period_end"],
            "balance": balance,
            "period_debit": row["period_debit"],
            "period_credit": row["period_credit"],
        }

    # For backward compatibility, also include top-level UZS balance
    uzs = balances.get("UZS", {})
    return {
        "client_name_1c": client_name_1c,
        "period_start": uzs.get("period_start", ""),
        "period_end": uzs.get("period_end", ""),
        "balance": uzs.get("balance", 0),
        "period_debit": uzs.get("period_debit", 0),
        "period_credit": uzs.get("period_credit", 0),
        "imported_at": imported_at,
        "balances_by_currency": balances,
    }


def get_client_balance_history(client_id, limit: int = 24) -> dict:
    """Get balance history for a client, separated by currency.

    client_id can be a single int or a list of ints (sibling IDs for
    multi-phone clients sharing the same client_id_1c).

    Returns dict with 'UZS' and 'USD' keys, each containing a list of
    period snapshots sorted chronologically (oldest first, for charting).
    """
    conn = get_db()
    # Normalize to list of IDs
    if isinstance(client_id, (list, tuple)):
        ids = list(client_id)
    else:
        ids = [client_id]
    placeholders = ",".join("?" * len(ids))
    rows = conn.execute(
        f"""SELECT currency, period_start, period_end,
                  opening_debit, opening_credit,
                  period_debit, period_credit,
                  closing_debit, closing_credit
           FROM client_balances
           WHERE client_id IN ({placeholders})
           ORDER BY currency, period_start ASC""",
        tuple(ids),
    ).fetchall()
    conn.close()

    history = {}
    for r in rows:
        cur = r["currency"]
        if cur not in history:
            history[cur] = []
        history[cur].append({
            "period_start": r["period_start"],
            "period_end": r["period_end"],
            "period_debit": r["period_debit"],
            "period_credit": r["period_credit"],
            "closing_debit": r["closing_debit"],
            "closing_credit": r["closing_credit"],
            "balance": (r["closing_debit"] or 0) - (r["closing_credit"] or 0),
        })

    # Trim to limit per currency
    for cur in history:
        if len(history[cur]) > limit:
            history[cur] = history[cur][-limit:]

    return history


def bulk_import_balances(file_list: List[tuple]) -> dict:
    """Import multiple balance files at once.

    Args:
        file_list: List of (filename, file_bytes) tuples.

    Returns summary of all imports.
    """
    results = []
    total_inserted = 0
    total_updated = 0
    total_matched = 0
    errors = []

    for filename, file_bytes in file_list:
        result = apply_balance_import(file_bytes)
        if result.get("ok"):
            total_inserted += result.get("inserted", 0)
            total_updated += result.get("updated", 0)
            total_matched += result.get("matched_to_app", 0)
            results.append({
                "file": filename,
                "currency": result.get("currency"),
                "period": result.get("period"),
                "clients": result.get("total_clients_in_file", 0),
                "matched": result.get("matched_to_app", 0),
            })
        else:
            errors.append({"file": filename, "error": result.get("error", "Unknown")})

    conn = get_db()
    db_clients = conn.execute("SELECT COUNT(DISTINCT client_name_1c) FROM client_balances").fetchone()[0]
    db_periods = conn.execute("SELECT COUNT(DISTINCT period_start || currency) FROM client_balances").fetchone()[0]
    conn.close()

    return {
        "ok": True,
        "files_processed": len(results),
        "files_failed": len(errors),
        "total_inserted": total_inserted,
        "total_updated": total_updated,
        "total_matched": total_matched,
        "results": results,
        "errors": errors,
        "db_total_clients": db_clients,
        "db_total_periods": db_periods,
    }
