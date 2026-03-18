"""Update product display names from Rassvet_Master.xlsx.

Reads the 'Katalog' sheet column A (ID) and column E (Ilovadagi nomi Latin)
and updates the name_display field in the products table.

This runs AFTER import_products to override auto-transliterated names
with manually corrected Uzbek display names.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from openpyxl import load_workbook
from backend.database import get_db, init_db

MASTER_CANDIDATES = [
    os.path.join(os.path.dirname(__file__), '..', '..', 'Rassvet_Master.xlsx'),
    '/data/Rassvet_Master.xlsx',
]


def find_master():
    for p in MASTER_CANDIDATES:
        if os.path.exists(p):
            return p
    return None


def update_display_names():
    path = find_master()
    if not path:
        print("update_display_names: Rassvet_Master.xlsx not found — skipping.")
        return

    init_db()
    conn = get_db()

    existing = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    if existing == 0:
        print("update_display_names: No products in DB — skipping.")
        conn.close()
        return

    print(f"update_display_names: Loading {path}...")
    wb = load_workbook(path, read_only=True, data_only=True)
    if 'Katalog' not in wb.sheetnames:
        print("update_display_names: 'Katalog' sheet not found — skipping.")
        wb.close()
        conn.close()
        return

    ws = wb['Katalog']
    updated = 0
    skipped = 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        pid = row[0]   # Column A = ID
        name = row[4]  # Column E = Ilovadagi nomi (Latin)
        if pid is None or name is None:
            skipped += 1
            continue
        try:
            pid = int(pid)
        except (ValueError, TypeError):
            skipped += 1
            continue

        name = str(name).strip()
        if not name:
            skipped += 1
            continue

        result = conn.execute(
            "UPDATE products SET name_display = ? WHERE id = ?",
            (name, pid)
        )
        if result.rowcount > 0:
            updated += 1

    conn.commit()
    conn.close()
    wb.close()
    print(f"update_display_names: Updated {updated} product names ({skipped} skipped).")


if __name__ == '__main__':
    update_display_names()
