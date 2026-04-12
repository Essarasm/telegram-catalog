"""
Import allowed clients from the CSV file into the allowed_clients table.
Normalizes phone numbers for matching against Telegram contacts.
Supports client_id_1c and company_name columns for 1C integration.
"""
import sqlite3
import os
import re

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


if __name__ == "__main__":
    import_clients()
