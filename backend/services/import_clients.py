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


def _iter_rows_from_xlsx(file_bytes: bytes) -> Iterable[dict]:
    """Yield row dicts from an xlsx file by column header."""
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=True)
    try:
        header = [str(c).strip().lower() if c is not None else "" for c in next(rows)]
    except StopIteration:
        return
    # Normalize header variants
    alias = {
        "phone": "phone", "tel": "phone", "telefon": "phone", "телефон": "phone",
        "name": "name", "ism": "name", "имя": "name", "nom": "name",
        "location": "location", "manzil": "location", "адрес": "location",
        "source": "source", "manba": "source", "источник": "source",
        "client_id_1c": "client_id_1c", "1c": "client_id_1c", "client 1c": "client_id_1c",
        "company": "company_name", "company_name": "company_name",
        "kompaniya": "company_name", "компания": "company_name",
    }
    headers = [alias.get(h, h) for h in header]
    for row in rows:
        data = {headers[i]: (row[i] if i < len(row) else None)
                for i in range(len(headers))}
        yield data


def _iter_rows_from_xls(file_bytes: bytes) -> Iterable[dict]:
    """Yield row dicts from a legacy .xls file via xlrd (cp1251 1C export)."""
    import xlrd
    wb = xlrd.open_workbook(file_contents=file_bytes, encoding_override="cp1251")
    sh = wb.sheet_by_index(0)
    if sh.nrows < 2:
        return
    header = [str(sh.cell_value(0, c) or "").strip().lower()
              for c in range(sh.ncols)]
    alias = {
        "phone": "phone", "tel": "phone", "telefon": "phone", "телефон": "phone",
        "name": "name", "ism": "name", "имя": "name", "nom": "name",
        "location": "location", "manzil": "location", "адрес": "location",
        "source": "source", "manba": "source", "источник": "source",
        "client_id_1c": "client_id_1c", "1c": "client_id_1c", "client 1c": "client_id_1c",
        "company": "company_name", "company_name": "company_name",
        "kompaniya": "company_name", "компания": "company_name",
    }
    headers = [alias.get(h, h) for h in header]
    for r in range(1, sh.nrows):
        row = {}
        for c in range(sh.ncols):
            v = sh.cell_value(r, c)
            if isinstance(v, float) and v.is_integer():
                v = str(int(v))
            row[headers[c]] = v
        yield row


def apply_clients_upload(file_bytes: bytes, filename_hint: str = "") -> dict:
    """Bot entry point: upsert allowed_clients from an uploaded xls/xlsx file.

    Required column: phone. Optional: name, location, source, client_id_1c,
    company_name. Matching behavior mirrors the CSV import.
    """
    name = (filename_hint or "").lower()
    try:
        if name.endswith(".xlsx"):
            rows = list(_iter_rows_from_xlsx(file_bytes))
        else:
            rows = list(_iter_rows_from_xls(file_bytes))
    except Exception as e:
        return {"ok": False, "error": f"Fayl o'qib bo'lmadi: {e}"}

    if not rows:
        return {"ok": False, "error": "Faylda ma'lumot topilmadi"}

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
    }


if __name__ == "__main__":
    import_clients()
