"""Update product display names from Rassvet_Master.xlsx.

Reads the 'Katalog' sheet column A (ID) and column E (Ilovadagi nomi Latin)
and updates the name_display field in the products table.

IMPORTANT: Excel IDs are 4881-7320 but DB IDs are 1-2440 (AUTOINCREMENT).
The offset is 4880: DB_ID = EXCEL_ID - 4880.

This runs AFTER import_products to override auto-transliterated names
with manually corrected Uzbek display names.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from openpyxl import load_workbook
from backend.database import get_db, init_db

ID_OFFSET = 4880  # Excel ID 4881 = DB ID 1

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

    print(f"update_display_names: Loading {path} (DB has {existing} products)...")
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
        excel_id = row[0]   # Column A = Excel ID (4881-7320)
        name = row[4]       # Column E = Ilovadagi nomi (Latin)
        if excel_id is None or name is None:
            skipped += 1
            continue
        try:
            excel_id = int(excel_id)
        except (ValueError, TypeError):
            skipped += 1
            continue

        db_id = excel_id - ID_OFFSET  # Convert: 4881 -> 1, 4882 -> 2, etc.
        if db_id < 1:
            skipped += 1
            continue

        name = str(name).strip()
        if not name:
            skipped += 1
            continue

        result = conn.execute(
            "UPDATE products SET name_display = ? WHERE id = ?",
            (name, db_id)
        )
        if result.rowcount > 0:
            updated += 1
        else:
            skipped += 1

    conn.commit()
    conn.close()
    wb.close()
    print(f"update_display_names: Updated {updated} product names ({skipped} skipped).")


if __name__ == '__main__':
    update_display_names()
