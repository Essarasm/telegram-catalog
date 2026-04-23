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

    Always returns the lowest ID (deterministic) and skips merged records.
    Same logic as import_balances._try_match_client.
    """
    row = conn.execute(
        "SELECT id FROM allowed_clients WHERE client_id_1c = ? AND COALESCE(status, 'active') != 'merged' ORDER BY id LIMIT 1",
        (client_name_1c,),
    ).fetchone()
    if row:
        return row[0]

    normalized = client_name_1c.strip().lower()
    row = conn.execute(
        "SELECT id FROM allowed_clients WHERE LOWER(TRIM(name)) = ? AND COALESCE(status, 'active') != 'merged' ORDER BY id LIMIT 1",
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


def _check_debtors_regression(
    prev: dict, new_uzs: float, new_usd: float, new_rows: int
) -> list[str]:
    """Compare incoming debtors totals against current DB state. Returns a
    list of human-readable regression reasons (empty if no regression).

    Thresholds chosen to catch schema-drift class incidents (like the
    "В Валюте" column vanishing from the 1C report between 15-22 Apr 2026
    which silently zero'd out $187,991 USD debt across 140 clients) while
    letting legitimate day-over-day fluctuations through.
    """
    reasons: list[str] = []
    prev_uzs = float(prev["uzs"] or 0)
    prev_usd = float(prev["usd"] or 0)
    prev_rows = int(prev["rows"] or 0)

    if prev_usd > 1000 and new_usd < prev_usd * 0.1:
        reasons.append(
            f"USD total collapsed: ${prev_usd:,.2f} → ${new_usd:,.2f} "
            f"(≥90% drop). Verify the 1C report includes the «В Валюте» column."
        )
    if prev_uzs > 1_000_000 and new_uzs < prev_uzs * 0.3:
        reasons.append(
            f"UZS total collapsed: {prev_uzs:,.0f} → {new_uzs:,.0f} so'm "
            f"(≥70% drop). Verify the 1C report currency filter."
        )
    if prev_rows > 50 and new_rows < prev_rows * 0.5:
        reasons.append(
            f"Row count collapsed: {prev_rows} → {new_rows} "
            f"(≥50% drop). Verify the 1C report isn't filtered to a subset."
        )
    return reasons


def apply_debtors_import(file_bytes: bytes, force: bool = False) -> dict:
    """Parse debtors XLS and replace all records in client_debts table.

    When force=False (default), runs a regression guard against the current
    client_debts totals and refuses the upload if USD/UZS/row-count
    collapse beyond safe thresholds. Pass force=True (or caption
    '/debtors force') to bypass when the regression is known-legitimate.
    """
    parsed = parse_debtors_xls(file_bytes)
    if not parsed.get("ok"):
        return parsed

    report_date = parsed["report_date"]
    clients = parsed["clients"]

    if not clients:
        return {"ok": False, "error": "No client data found"}

    # Pre-compute incoming totals so we can diff vs current before DELETE
    incoming_total_uzs = sum(c["debt_uzs"] for c in clients)
    incoming_total_usd = sum(c["debt_usd"] for c in clients)

    conn = get_db()

    prev = conn.execute(
        """SELECT COALESCE(SUM(debt_uzs), 0) AS uzs,
                  COALESCE(SUM(debt_usd), 0) AS usd,
                  COUNT(*) AS rows
           FROM client_debts"""
    ).fetchone()

    if not force:
        regression_reasons = _check_debtors_regression(
            prev, incoming_total_uzs, incoming_total_usd, len(clients)
        )
        if regression_reasons:
            conn.close()
            return {
                "ok": False,
                "regression_blocked": True,
                "reasons": regression_reasons,
                "previous": {
                    "total_uzs": float(prev["uzs"] or 0),
                    "total_usd": float(prev["usd"] or 0),
                    "rows": int(prev["rows"] or 0),
                },
                "incoming": {
                    "total_uzs": incoming_total_uzs,
                    "total_usd": incoming_total_usd,
                    "rows": len(clients),
                },
            }

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

    # Post-import orphan heal — see import_balances.py for rationale.
    from backend.services.client_search import heal_finance_orphans_by_1c_name
    orphans_healed = heal_finance_orphans_by_1c_name(conn, "client_debts")

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "report_date": report_date,
        "total_clients": len(clients),
        "matched_to_app": matched,
        "unmatched_count": len(unmatched_names),
        "unmatched_sample": unmatched_names[:15],
        "orphans_healed": orphans_healed,
        "total_uzs": total_uzs,
        "total_usd": total_usd,
    }


def get_client_debt(client_id) -> Optional[dict]:
    """Get current debt for a client from the debtors snapshot.

    client_id can be a single int or a list of ints (sibling IDs for
    multi-phone clients sharing the same client_id_1c).

    Returns debt data if client is in the table, or a zero-balance result
    if the table has data but this client isn't in it (= settled).
    Returns None if no debtors report has been imported yet.
    """
    conn = get_db()

    # Normalize to list of IDs
    if isinstance(client_id, (list, tuple)):
        ids = list(client_id)
    else:
        ids = [client_id]

    # Check if any debtors data exists
    has_data = conn.execute("SELECT COUNT(*) FROM client_debts").fetchone()[0]
    if not has_data:
        conn.close()
        return None  # No debtors report imported — fall back to old system

    # Look for this client (any sibling ID)
    placeholders = ",".join("?" * len(ids))
    row = conn.execute(
        f"""SELECT client_name_1c, debt_uzs, debt_usd,
                  last_transaction_date, last_transaction_no,
                  aging_0_30, aging_31_60, aging_61_90, aging_91_120, aging_120_plus,
                  report_date, imported_at
           FROM client_debts WHERE client_id IN ({placeholders})""",
        tuple(ids),
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


def _latest_ob_closing(conn, ids: list) -> dict:
    """Return the most recent оборотка closing per currency for a client.

    Returns a dict keyed by currency: {UZS: {debt, period_start, imported_at}, USD: {...}}.
    "debt" = closing_debit − closing_credit on the row with the latest period_start.
    Used as the fallback when client_debts has structural gaps (e.g., upstream 1C
    template dropped the В Валюте column, leaving debt_usd = 0 for every row).
    """
    placeholders = ",".join("?" * len(ids))
    rows = conn.execute(
        f"""SELECT cb.currency, cb.period_start, cb.period_end,
                   cb.closing_debit, cb.closing_credit, cb.imported_at
            FROM client_balances cb
            INNER JOIN (
                SELECT currency, MAX(period_start) AS pmax
                FROM client_balances
                WHERE client_id IN ({placeholders})
                GROUP BY currency
            ) latest ON cb.currency = latest.currency
                    AND cb.period_start = latest.pmax
            WHERE cb.client_id IN ({placeholders})""",
        (*ids, *ids),
    ).fetchall()
    out = {}
    for r in rows:
        ccy = (r["currency"] or "").upper()
        if not ccy:
            continue
        out[ccy] = {
            "debt": (r["closing_debit"] or 0) - (r["closing_credit"] or 0),
            "period_start": r["period_start"],
            "imported_at": r["imported_at"],
        }
    return out


def get_effective_debt(client_id) -> Optional[dict]:
    """Resolve current debt per currency, picking the best raw source per leg.

    Algorithm (per currency, independently):
        1. If client_debts has structurally-valid data for this currency
           (≥1 row globally with a non-zero value), use client_debts.
           Preserves "client not listed = settled" semantics.
        2. Otherwise fall back to the most recent client_balances closing
           (closing_debit − closing_credit on MAX(period_start)).

    Self-healing: when the 1C template for Дебиторская задолженность restores
    its В Валюте column and a good /debtors upload lands, the USD leg will
    have non-zero rows again and this function switches USD back to debts
    automatically. Same logic covers any currency leg going dark in the future.

    Returns None when no data is available from either source.
    """
    conn = get_db()
    if isinstance(client_id, (list, tuple)):
        ids = list(client_id)
    else:
        ids = [client_id]

    # Fetch the client's debtors row (if any) and оборотка closings
    placeholders = ",".join("?" * len(ids))
    debts_row = conn.execute(
        f"""SELECT client_name_1c, debt_uzs, debt_usd, report_date,
                   last_transaction_date,
                   aging_0_30, aging_31_60, aging_61_90, aging_91_120, aging_120_plus,
                   imported_at
            FROM client_debts WHERE client_id IN ({placeholders})""",
        tuple(ids),
    ).fetchone()

    debts_meta = conn.execute(
        "SELECT report_date, imported_at FROM client_debts LIMIT 1"
    ).fetchone()

    # Structural column validity: is there ≥1 non-zero value globally?
    has_debts_table = debts_meta is not None
    uzs_valid_in_debts = has_debts_table and bool(conn.execute(
        "SELECT EXISTS(SELECT 1 FROM client_debts WHERE COALESCE(debt_uzs,0) != 0)"
    ).fetchone()[0])
    usd_valid_in_debts = has_debts_table and bool(conn.execute(
        "SELECT EXISTS(SELECT 1 FROM client_debts WHERE COALESCE(debt_usd,0) != 0)"
    ).fetchone()[0])

    ob_closings = _latest_ob_closing(conn, ids)
    conn.close()

    if not has_debts_table and not ob_closings:
        return None

    def pick(ccy_lower: str, column_valid: bool):
        if has_debts_table and column_valid:
            if debts_row:
                return float(debts_row[f"debt_{ccy_lower}"] or 0), "debts"
            return 0.0, "debts_settled"  # Client absent from debtors = settled
        ob = ob_closings.get(ccy_lower.upper())
        if ob is not None:
            return float(ob["debt"] or 0), "ob_closing"
        return 0.0, "empty"

    uzs_val, uzs_src = pick("uzs", uzs_valid_in_debts)
    usd_val, usd_src = pick("usd", usd_valid_in_debts)

    # Prefer debtors metadata when available; else use latest оборотка snapshot
    report_date = debts_meta["report_date"] if debts_meta else None
    imported_at = debts_meta["imported_at"] if debts_meta else None
    if not has_debts_table:
        for ccy in ("USD", "UZS"):
            ob = ob_closings.get(ccy)
            if ob is not None:
                imported_at = ob["imported_at"]
                break

    client_name_1c = debts_row["client_name_1c"] if debts_row else None
    aging = None
    last_tx_date = None
    if debts_row:
        aging = {
            "0_30": debts_row["aging_0_30"],
            "31_60": debts_row["aging_31_60"],
            "61_90": debts_row["aging_61_90"],
            "91_120": debts_row["aging_91_120"],
            "120_plus": debts_row["aging_120_plus"],
        }
        last_tx_date = debts_row["last_transaction_date"]

    return {
        "client_name_1c": client_name_1c,
        "debt_uzs": uzs_val,
        "debt_usd": usd_val,
        "debt_uzs_source": uzs_src,
        "debt_usd_source": usd_src,
        "report_date": report_date,
        "last_transaction_date": last_tx_date,
        "aging": aging or {"0_30": 0, "31_60": 0, "61_90": 0, "91_120": 0, "120_plus": 0},
        "imported_at": imported_at,
    }
