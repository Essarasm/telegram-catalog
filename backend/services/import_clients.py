"""
Import allowed clients from the CSV file into the allowed_clients table.
Normalizes phone numbers for matching against Telegram contacts.
Supports client_id_1c and company_name columns for 1C integration.
"""
import io
import sqlite3
import os
import re
from typing import Iterable

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")
CLIENTS_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "clients_data.csv")


def normalize_phone(raw: str) -> str:
    """Strip to last 9 digits (Uzbek local number without country code)."""
    if not raw or not isinstance(raw, str):
        return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) >= 9:
        return digits[-9:]
    return digits


def import_clients():
    if not os.path.exists(CLIENTS_FILE):
        print("[import_clients] No clients_data.csv found, skipping.")
        return

    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    # Read CSV
    import csv
    rows_inserted = 0
    rows_updated = 0
    seen_phones = set()

    with open(CLIENTS_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            phone = normalize_phone(row.get("phone", ""))
            if not phone or phone in seen_phones:
                continue
            seen_phones.add(phone)

            name = row.get("name", "").strip()
            location = row.get("location", "").strip()
            source = row.get("source", "").strip()
            client_id_1c = row.get("client_id_1c", "").strip()
            company_name = row.get("company_name", "").strip()

            # Check if this phone already exists (skip merged records for updates)
            existing = conn.execute(
                "SELECT id, COALESCE(status, 'active') as status FROM allowed_clients WHERE phone_normalized = ? LIMIT 1",
                (phone,),
            ).fetchone()

            if existing and existing[1] == 'merged':
                # Skip merged records — don't override dedup decisions
                continue

            if existing:
                # Update existing record with new data (preserving non-empty fields)
                updates = []
                params = []
                if name:
                    updates.append("name = ?")
                    params.append(name)
                if location:
                    updates.append("location = ?")
                    params.append(location)
                if source:
                    updates.append("source_sheet = ?")
                    params.append(source)
                if client_id_1c:
                    updates.append("client_id_1c = ?")
                    params.append(client_id_1c)
                if company_name:
                    updates.append("company_name = ?")
                    params.append(company_name)
                if updates:
                    params.append(existing[0])
                    conn.execute(
                        f"UPDATE allowed_clients SET {', '.join(updates)} WHERE id = ?",
                        params,
                    )
                    rows_updated += 1
            else:
                conn.execute(
                    """INSERT INTO allowed_clients
                       (phone_normalized, name, location, source_sheet, client_id_1c, company_name)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (phone, name, location, source, client_id_1c, company_name),
                )
                rows_inserted += 1

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]

    # Retroactively approve existing registered users whose phone matches whitelist
    existing_users = conn.execute(
        "SELECT telegram_id, phone FROM users WHERE phone IS NOT NULL"
    ).fetchall()
    approved_count = 0
    for u in existing_users:
        phone_norm = normalize_phone(u[1])
        match = conn.execute(
            "SELECT id FROM allowed_clients WHERE phone_normalized = ? AND COALESCE(status, 'active') != 'merged' LIMIT 1",
            (phone_norm,),
        ).fetchone()
        if match:
            conn.execute(
                "UPDATE users SET is_approved = 1, client_id = ? WHERE telegram_id = ?",
                (match[0], u[0]),
            )
            approved_count += 1

    # Set is_approved to 0 for any NULL values (from migration)
    conn.execute("UPDATE users SET is_approved = 0 WHERE is_approved IS NULL")
    conn.commit()
    conn.close()

    print(f"[import_clients] Inserted {rows_inserted}, updated {rows_updated}. Total: {total}")
    if approved_count:
        print(f"[import_clients] Retroactively approved {approved_count} existing users.")


_HEADER_ALIAS = {
    # phone variants
    "phone": "phone", "tel": "phone", "tel.": "phone",
    "telefon": "phone", "telefon raqam": "phone",
    "телефон": "phone", "тел": "phone", "тел.": "phone",
    "phone number": "phone", "телефон номер": "phone",
    "mobile": "phone", "мобильный": "phone", "nomer": "phone", "номер": "phone",
    # name variants
    "name": "name", "ism": "name", "имя": "name", "nom": "name",
    "fish": "name", "fio": "name", "фио": "name",
    "klient": "name", "клиент": "name", "mijoz": "name",
    "ф.и.о": "name", "ф.и.о.": "name", "ф и о": "name",
    # location
    "location": "location", "manzil": "location", "адрес": "location",
    "address": "location",
    # source
    "source": "source", "manba": "source", "источник": "source",
    # 1c name
    "client_id_1c": "client_id_1c", "1c": "client_id_1c",
    "1c nomi": "client_id_1c", "1с nomi": "client_id_1c",
    "1c ismi": "client_id_1c", "1c name": "client_id_1c",
    "client 1c": "client_id_1c", "1c клиент": "client_id_1c",
    "1с клиент": "client_id_1c", "контрагент": "client_id_1c",
    "kontragent": "client_id_1c",
    # company
    "company": "company_name", "company_name": "company_name",
    "kompaniya": "company_name", "компания": "company_name",
    "firma": "company_name", "фирма": "company_name",
}


def _normalize_headers(raw_headers: list) -> list:
    return [_HEADER_ALIAS.get(
        str(h or "").strip().lower().replace("  ", " "),
        str(h or "").strip().lower(),
    ) for h in raw_headers]


def _score_header_row(raw_row) -> int:
    """Return the number of canonical fields this row hits."""
    known = {"phone", "name", "client_id_1c", "company_name", "location", "source"}
    return sum(1 for h in _normalize_headers(raw_row) if h in known)


def _find_header_row(table_rows, max_scan: int = 10) -> int:
    """1C exports often put the sheet title / metadata on rows 1-3 and the
    real header on row 2-5. Pick the earliest row with the most alias hits,
    preferring the first row that scores >= 2."""
    best_idx, best_score = 0, -1
    for i, row in enumerate(table_rows[:max_scan]):
        s = _score_header_row(row)
        if s > best_score:
            best_score = s
            best_idx = i
        if s >= 2:
            return i
    return best_idx


def _iter_rows_from_xlsx(file_bytes: bytes):
    """Return (headers_raw, list of dicts) from an xlsx, auto-detecting the
    real header row (1C exports often have a title row above it)."""
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return [], []
    hdr_idx = _find_header_row(rows)
    header_row = rows[hdr_idx]
    header_raw = [("" if c is None else str(c)) for c in header_row]
    headers = _normalize_headers(header_raw)
    out = []
    for row in rows[hdr_idx + 1:]:
        data = {headers[i]: (row[i] if i < len(row) else None)
                for i in range(len(headers))}
        out.append(data)
    return header_raw, out


def _iter_rows_from_xls(file_bytes: bytes):
    """Return (headers_raw, list of dicts) from a legacy .xls, auto-detecting
    the real header row."""
    import xlrd
    wb = xlrd.open_workbook(file_contents=file_bytes, encoding_override="cp1251")
    sh = wb.sheet_by_index(0)
    if sh.nrows < 1:
        return [], []
    all_rows = [
        [sh.cell_value(r, c) for c in range(sh.ncols)]
        for r in range(min(sh.nrows, 10))
    ]
    hdr_idx = _find_header_row(all_rows)
    header_raw = [str(sh.cell_value(hdr_idx, c) or "") for c in range(sh.ncols)]
    headers = _normalize_headers(header_raw)
    out = []
    for r in range(hdr_idx + 1, sh.nrows):
        row = {}
        for c in range(sh.ncols):
            v = sh.cell_value(r, c)
            if isinstance(v, float) and v.is_integer():
                v = str(int(v))
            row[headers[c]] = v
        out.append(row)
    return header_raw, out


def apply_clients_upload(file_bytes: bytes, filename_hint: str = "") -> dict:
    """Bot entry point: upsert allowed_clients from an uploaded xls/xlsx file.

    Required column: phone. Optional: name, location, source, client_id_1c,
    company_name. Matching behavior mirrors the CSV import.
    """
    name = (filename_hint or "").lower()
    try:
        if name.endswith(".xlsx"):
            header_raw, rows = _iter_rows_from_xlsx(file_bytes)
        else:
            header_raw, rows = _iter_rows_from_xls(file_bytes)
    except Exception as e:
        return {"ok": False, "error": f"Fayl o'qib bo'lmadi: {e}"}

    if not rows:
        return {"ok": False, "error": "Faylda ma'lumot topilmadi"}

    # If no row produced a valid phone, surface the header list so the operator
    # (and we) can see which columns the file actually has. Aliases can then
    # be added to _HEADER_ALIAS without guessing.
    any_phone = any(normalize_phone(str(r.get("phone") or "")) for r in rows)

    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    inserted = updated = skipped = 0
    seen = set()
    for raw in rows:
        phone = normalize_phone(str(raw.get("phone") or ""))
        if not phone or phone in seen:
            skipped += 1
            continue
        seen.add(phone)

        client_name = str(raw.get("name") or "").strip()
        location = str(raw.get("location") or "").strip()
        source = str(raw.get("source") or "clients_upload").strip()
        cid_1c = str(raw.get("client_id_1c") or "").strip()
        company = str(raw.get("company_name") or "").strip()

        existing = conn.execute(
            "SELECT id, COALESCE(status, 'active') FROM allowed_clients "
            "WHERE phone_normalized = ? LIMIT 1",
            (phone,),
        ).fetchone()

        if existing and existing[1] == "merged":
            skipped += 1
            continue

        if existing:
            updates, params = [], []
            if client_name:
                updates.append("name = ?"); params.append(client_name)
            if location:
                updates.append("location = ?"); params.append(location)
            if source:
                updates.append("source_sheet = ?"); params.append(source)
            if cid_1c:
                updates.append("client_id_1c = ?"); params.append(cid_1c)
            if company:
                updates.append("company_name = ?"); params.append(company)
            if updates:
                params.append(existing[0])
                conn.execute(
                    f"UPDATE allowed_clients SET {', '.join(updates)} WHERE id = ?",
                    params,
                )
                updated += 1
            else:
                skipped += 1
        else:
            conn.execute(
                "INSERT INTO allowed_clients (phone_normalized, name, location, "
                "source_sheet, status, client_id_1c, company_name) "
                "VALUES (?, ?, ?, ?, 'active', ?, ?)",
                (phone, client_name, location, source, cid_1c, company),
            )
            inserted += 1

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]
    conn.close()

    return {
        "ok": True,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "total_clients": total,
        "headers_seen": header_raw,
        "phone_column_detected": any_phone,
    }


if __name__ == "__main__":
    import_clients()
