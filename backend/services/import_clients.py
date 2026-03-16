"""
Import allowed clients from the Excel file into the allowed_clients table.
Normalizes phone numbers for matching against Telegram contacts.
"""
import sqlite3
import os
import re

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/catalog.db")
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

            conn.execute(
                """INSERT OR IGNORE INTO allowed_clients
                   (phone_normalized, name, location, source_sheet)
                   VALUES (?, ?, ?, ?)""",
                (phone, name, location, source),
            )
            rows_inserted += 1

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]
    conn.close()
    print(f"[import_clients] Inserted {rows_inserted} new phones. Total: {total}")


if __name__ == "__main__":
    import_clients()
