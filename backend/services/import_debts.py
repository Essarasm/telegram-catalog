"""Import client debts from 1C "Дебиторская задолженность на дату" report.

This is the source of truth for client debt — it matches the Акт сверки exactly.
Only clients with positive debt appear in the report; absence = settled (0 balance).

File format (XLS, cp1251):
    Row 0: empty
    Row 1: "Дебиторская задолженность на DD Month YYYY г."
    Row 2: empty
    Row 3: headers
    Row 4+: data — col2=name, col5=UZS debt, col6=USD debt, col7-11=aging buckets
    Last 2 rows: totals
"""
import re
import logging
from typing import Optional

from backend.database import get_db

logger = logging.getLogger(__name__)

# Russian months for parsing the report title date
_RU_MONTHS_GENI = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
    'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
    'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
}


def _parse_report_date(title: str) -> Optional[str]:
    """Parse report date from title like 'Дебиторская задолженность на 2 Апреля 2026 г.'"""
    m = re.search(r'на\s+(\d{1,2})\s+(\w+)\s+(\d{4})', title, re.IGNORECASE)
    if not m:
        return None
    day = int(m.group(1))
    month_name = m.group(2).lower()
    year = int(m.group(3))
    month = _RU_MONTHS_GENI.get(month_name)
    if not month:
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def _parse_transaction_date(val) -> Optional[str]:
    """Parse transaction date from float like 300326.0 → '2026-03-30' (DDMMYY format)."""
    if val is None:
        return None
    s = str(val).replace('.0', '').strip()
    if not s or not s.isdigit():
        return None
    # Pad to 6 digits (e.g. 20426 → 020426)
    s = s.zfill(6)
    try:
        day = int(s[0:2])
        month = int(s[2:4])
        year = int(s[4:6])
        if year < 100:
            year += 2000
        return f"{year:04d}-{month:02d}-{day:02d}"
    except (ValueError, IndexError):
        return None


def _parse_number(val) -> float:
    """Parse cell value to float, returning 0 for empty/invalid."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _try_match_client(client_name_1c: str, conn) -> Optional[int]:
    """Match 1C client name to allowed_clients.id.
    Same logic as import_balances._try_match_client.
    """
    row = conn.execute(
        "SELECT id FROM allowed_clients WHERE client_id_1c = ? LIMIT 1",
        (client_name_1c,),
    ).fetchone()
    if row:
        return row[0]

    normalized = client_name_1c.strip().lower()
    row = conn.execute(
        "SELECT id FROM allowed_clients WHERE LOWER(TRIM(name)) = ? LIMIT 1",
        (normalized,),
    ).fetchone()
    if row:
        return row[0]

    return None


def parse_debtors_xls(file_bytes: bytes) -> dict:
    """Parse дебиторская задолженность XLS and return structured data."""
    try:
        import xlrd
    except ImportError:
        return {"ok": False, "error": "xlrd not installed"}

    try:
        wb = xlrd.open_workbook(file_contents=file_bytes, encoding_override='cp1251')
    except Exception as e:
        return {"ok": False, "error": f"Failed to open XLS: {e}"}

    sh = wb.sheet_by_index(0)
    if sh.nrows < 5:
        return {"ok": False, "error": "File too short"}

    # Parse report date from Row 1
    title = str(sh.cell_value(1, 1)).strip()
    if not title:
        title = str(sh.cell_value(1, 0)).strip()
    report_date = _parse_report_date(title)
    if not report_date:
        return {"ok": False, "error": f"Could not parse report date from: '{title}'"}

    # Parse data rows (Row 4 until ВСЕГО:)
    clients = []
    for i in range(4, sh.nrows):
        name = str(sh.cell_value(i, 2)).strip()
        if not name or name.startswith('ВСЕГО'):
            break

        debt_uzs = _parse_number(sh.cell_value(i, 5))
        debt_usd = _parse_number(sh.cell_value(i, 6))

        # Skip if both debts are 0 (shouldn't happen in this report, but safety)
        if debt_uzs == 0 and debt_usd == 0:
            continue

        clients.append({
            "client_name_1c": name,
            "debt_uzs": debt_uzs,
            "debt_usd": debt_usd,
            "last_transaction_date": _parse_transaction_date(sh.cell_value(i, 3)),
            "last_transaction_no": str(sh.cell_value(i, 4)).replace('.0', '').strip() or None,
            "aging_0_30": _parse_number(sh.cell_value(i, 7)),
            "aging_31_60": _parse_number(sh.cell_value(i, 8)),
            "aging_61_90": _parse_number(sh.cell_value(i, 9)),
            "aging_91_120": _parse_number(sh.cell_value(i, 10)),
            "aging_120_plus": _parse_number(sh.cell_value(i, 11)),
        })

    return {
        "ok": True,
        "report_date": report_date,
        "title": title,
        "clients": clients,
    }


def apply_debtors_import(file_bytes: bytes) -> dict:
    """Parse debtors XLS and replace all records in client_debts table."""
    parsed = parse_debtors_xls(file_bytes)
    if not parsed.get("ok"):
        return parsed

    report_date = parsed["report_date"]
    clients = parsed["clients"]

    if not clients:
        return {"ok": False, "error": "No client data found"}

    conn = get_db()

    # Clear existing records — full replacement
    conn.execute("DELETE FROM client_debts")

    matched = 0
    unmatched_names = []
    total_uzs = 0.0
    total_usd = 0.0

    for c in clients:
        client_id = _try_match_client(c["client_name_1c"], conn)
        if client_id:
            matched += 1
        else:
            unmatched_names.append(c["client_name_1c"])

        total_uzs += c["debt_uzs"]
        total_usd += c["debt_usd"]

        conn.execute(
            """INSERT INTO client_debts
               (client_name_1c, client_id, debt_uzs, debt_usd,
                last_transaction_date, last_transaction_no,
                aging_0_30, aging_31_60, aging_61_90, aging_91_120, aging_120_plus,
                report_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                c["client_name_1c"], client_id,
                c["debt_uzs"], c["debt_usd"],
                c["last_transaction_date"], c["last_transaction_no"],
                c["aging_0_30"], c["aging_31_60"], c["aging_61_90"],
                c["aging_91_120"], c["aging_120_plus"],
                report_date,
            ),
        )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "report_date": report_date,
        "total_clients": len(clients),
        "matched_to_app": matched,
        "unmatched_count": len(unmatched_names),
        "unmatched_sample": unmatched_names[:15],
        "total_uzs": total_uzs,
        "total_usd": total_usd,
    }


def get_client_debt(client_id: int) -> Optional[dict]:
    """Get current debt for a client from the debtors snapshot.

    Returns debt data if client is in the table, or a zero-balance result
    if the table has data but this client isn't in it (= settled).
    Returns None if no debtors report has been imported yet.
    """
    conn = get_db()

    # Check if any debtors data exists
    has_data = conn.execute("SELECT COUNT(*) FROM client_debts").fetchone()[0]
    if not has_data:
        conn.close()
        return None  # No debtors report imported — fall back to old system

    # Look for this client
    row = conn.execute(
        """SELECT client_name_1c, debt_uzs, debt_usd,
                  last_transaction_date, last_transaction_no,
                  aging_0_30, aging_31_60, aging_61_90, aging_91_120, aging_120_plus,
                  report_date, imported_at
           FROM client_debts WHERE client_id = ?""",
        (client_id,),
    ).fetchone()

    # Get report metadata
    meta = conn.execute(
        "SELECT report_date, imported_at FROM client_debts LIMIT 1"
    ).fetchone()
    conn.close()

    if row:
        return {
            "client_name_1c": row["client_name_1c"],
            "debt_uzs": row["debt_uzs"],
            "debt_usd": row["debt_usd"],
            "report_date": row["report_date"],
            "last_transaction_date": row["last_transaction_date"],
            "aging": {
                "0_30": row["aging_0_30"],
                "31_60": row["aging_31_60"],
                "61_90": row["aging_61_90"],
                "91_120": row["aging_91_120"],
                "120_plus": row["aging_120_plus"],
            },
            "imported_at": row["imported_at"],
        }

    # Client not in debtors table = settled (0 balance)
    return {
        "client_name_1c": None,
        "debt_uzs": 0,
        "debt_usd": 0,
        "report_date": meta["report_date"] if meta else None,
        "last_transaction_date": None,
        "aging": {"0_30": 0, "31_60": 0, "61_90": 0, "91_120": 0, "120_plus": 0},
        "imported_at": meta["imported_at"] if meta else None,
    }
