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
    "телефон": "phone", "телефоны": "phone",
    "телефон контрагента": "phone", "телефоны контрагента": "phone",
    "тел": "phone", "тел.": "phone",
    "тел. номер": "phone", "тел.номер": "phone", "тел номер": "phone",
    "контактный телефон": "phone", "контакт": "phone", "контакты": "phone",
    "phone number": "phone", "телефон номер": "phone",
    "mobile": "phone", "мобильный": "phone", "nomer": "phone", "номер": "phone",
    # name variants (1C "Наименование" is the short client name on Контрагенты)
    "name": "name", "ism": "name", "имя": "name", "nom": "name",
    "fish": "name", "fio": "name", "фио": "name",
    "klient": "name", "клиент": "name", "mijoz": "name",
    "ф.и.о": "name", "ф.и.о.": "name", "ф и о": "name",
    "наименование": "name", "название": "name", "наим.": "name",
    # location
    "location": "location", "manzil": "location", "адрес": "location",
    "address": "location", "город": "location",
    "юридический адрес": "location", "юр.адрес": "location", "юр адрес": "location",
    "почтовый адрес": "location", "фактический адрес": "location",
    # source
    "source": "source", "manba": "source", "источник": "source",
    # 1c id (Контрагент in 1C is the client row itself; keep it mapping to client_id_1c
    # because we use the 1C NAME (string) as the link key — NOT the numeric Код.
    # The "Код" column is a numeric internal 1C id (e.g. 1701) — don't alias it here,
    # otherwise it overwrites the human-readable 1C name.
    "client_id_1c": "client_id_1c", "1c": "client_id_1c",
    "1c nomi": "client_id_1c", "1с nomi": "client_id_1c",
    "1c ismi": "client_id_1c", "1c name": "client_id_1c",
    "client 1c": "client_id_1c", "1c клиент": "client_id_1c",
    "1с клиент": "client_id_1c", "контрагент": "client_id_1c",
    "kontragent": "client_id_1c",
    # company (1C "Полное наименование" = legal entity form)
    "company": "company_name", "company_name": "company_name",
    "kompaniya": "company_name", "компания": "company_name",
    "firma": "company_name", "фирма": "company_name",
    "полное наименование": "company_name", "полн. наименование": "company_name",
    "юр.лицо": "company_name", "юрлицо": "company_name",
    "организация": "company_name",
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


def _find_header_row(table_rows, max_scan: int = 15) -> int:
    """1C exports often put the sheet title + blank rows + meta on rows 1-5 and
    the real header row below. Pick the earliest row with ≥ 2 alias hits;
    fall back to the single best-scoring row; if nothing scores return -1 so
    the caller knows no header was found (better than silently using row 0)."""
    best_idx, best_score = 0, -1
    for i, row in enumerate(table_rows[:max_scan]):
        s = _score_header_row(row)
        if s > best_score:
            best_score = s
            best_idx = i
        if s >= 2:
            return i
    if best_score <= 0:
        return -1
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
    if hdr_idx < 0:
        # No alias-matching header row found — return raw preview so operator
        # can see what's in the file.
        preview = rows[0] if rows else tuple()
        header_raw = [("" if c is None else str(c)) for c in preview]
        return header_raw, []
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
        for r in range(min(sh.nrows, 15))
    ]
    hdr_idx = _find_header_row(all_rows)
    if hdr_idx < 0:
        # No alias-matching header row — surface raw row 0 to operator.
        preview = [str(sh.cell_value(0, c) or "") for c in range(sh.ncols)] if sh.nrows else []
        return preview, []
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

        # client_id_1c must be a human-readable 1C NAME, never a numeric code.
        # If the uploaded value is purely numeric (e.g. "1701" from a "Код"
        # column that slipped through), discard it. Fall back to `name` when
        # that's a proper string.
        if cid_1c and cid_1c.isdigit():
            cid_1c = ""
        if not cid_1c and client_name and not client_name.isdigit():
            cid_1c = client_name

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
            flag_needs_review = False
            if client_name:
                updates.append("name = ?"); params.append(client_name)
            if location:
                updates.append("location = ?"); params.append(location)
            if source:
                updates.append("source_sheet = ?"); params.append(source)
            if cid_1c:
                # 1C ambiguity tiebreaker: if an existing name already has
                # recent activity (real_orders / payments in last 180d) and
                # the incoming Контрагент name differs, don't silently
                # overwrite — flag needs_review so an operator can pick.
                existing_cid = conn.execute(
                    "SELECT client_id_1c FROM allowed_clients WHERE id = ?",
                    (existing[0],),
                ).fetchone()
                prev_cid = (existing_cid[0] if existing_cid else None) or ""
                if prev_cid and prev_cid != cid_1c:
                    try:
                        prev_recent = conn.execute(
                            "SELECT MAX(doc_date) FROM real_orders "
                            "WHERE client_name_1c = ? "
                            "AND doc_date >= date('now','-180 days')",
                            (prev_cid,),
                        ).fetchone()[0]
                        new_recent = conn.execute(
                            "SELECT MAX(doc_date) FROM real_orders "
                            "WHERE client_name_1c = ? "
                            "AND doc_date >= date('now','-180 days')",
                            (cid_1c,),
                        ).fetchone()[0]
                        # Rule: prefer the name with the more recent activity.
                        # If both have activity within 180d, flag ambiguity.
                        if prev_recent and new_recent:
                            flag_needs_review = True
                            # Keep whichever is more recent (string ISO date)
                            if prev_recent >= new_recent:
                                cid_1c = prev_cid  # revert to existing
                        elif prev_recent and not new_recent:
                            cid_1c = prev_cid  # existing wins on activity
                        # else: no prev activity — the new cid_1c overwrites normally
                    except Exception:
                        pass  # activity check is best-effort
                updates.append("client_id_1c = ?"); params.append(cid_1c)
            if company:
                updates.append("company_name = ?"); params.append(company)
            if flag_needs_review:
                updates.append("needs_review = 1")
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
