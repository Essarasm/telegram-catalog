"""Update product display names, weights, and categories from Rassvet_Master.xlsx.

Reads the 'Katalog' sheet and syncs name_display, weight, and category_id
in the products table from the master spreadsheet.

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

    # Build category name → id map from DB
    cat_map = {}
    for row in conn.execute("SELECT id, name FROM categories").fetchall():
        cat_map[row[0]] = row[1]
    # Reverse: name → id
    cat_name_to_id = {v: k for k, v in cat_map.items()}

    print(f"update_display_names: Loading {path} (DB has {existing} products)...")
    wb = load_workbook(path, read_only=True, data_only=True)
    if 'Katalog' not in wb.sheetnames:
        print("update_display_names: 'Katalog' sheet not found — skipping.")
        wb.close()
        conn.close()
        return

    ws = wb['Katalog']
    updated_names = 0
    updated_weights = 0
    updated_cats = 0
    skipped = 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        excel_id = row[0]       # Column A = Excel ID (4881-7320)
        category = row[1]       # Column B = Category name
        name = row[4]           # Column E = Ilovadagi nomi (Latin)
        weight = row[10]        # Column K = Weight
        if excel_id is None or name is None:
            skipped += 1
            continue
        try:
            excel_id = int(excel_id)
        except (ValueError, TypeError):
            skipped += 1
            continue

        db_id = excel_id - ID_OFFSET
        if db_id < 1:
            skipped += 1
            continue

        name = str(name).strip()
        if not name:
            skipped += 1
            continue

        # Update name_display
        result = conn.execute(
            "UPDATE products SET name_display = ? WHERE id = ?",
            (name, db_id)
        )
        if result.rowcount > 0:
            updated_names += 1

        # Update weight if Excel has a value
        if weight is not None:
            try:
                w = float(weight)
                conn.execute(
                    "UPDATE products SET weight = ? WHERE id = ?",
                    (w, db_id)
                )
                updated_weights += 1
            except (ValueError, TypeError):
                pass

        # Update category if it differs
        if category and str(category).strip() in cat_name_to_id:
            new_cat_id = cat_name_to_id[str(category).strip()]
            conn.execute(
                "UPDATE products SET category_id = ? WHERE id = ?",
                (new_cat_id, db_id)
            )
            updated_cats += 1

    conn.commit()
    conn.close()
    wb.close()
    print(f"update_display_names: Updated {updated_names} names, "
          f"{updated_weights} weights, {updated_cats} categories "
          f"({skipped} skipped).")


if __name__ == '__main__':
    update_display_names()
