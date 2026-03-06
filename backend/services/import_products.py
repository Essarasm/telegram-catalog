"""Import products from the XLS spreadsheet into SQLite."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
from backend.database import get_db, init_db

CATEGORY_NAMES = [
    "ВОДОЭМУЛЬСИИ",
    "КАБЕЛЬ И ПРОВОДА",
    "Электро - Товары",
    "КЛЕИ",
    "КОЛЛЕРА",
    "КРАСКИ И ЭМАЛИ",
    "ЛАКИ И ОЛИФЫ",
    "ПРОЧИЕ ТОВАРЫ",
    "РАСТВОРИТЕЛИ",
    "САМОРЕЗЫ И ГВОЗДИ",
    "СУХИЕ СМЕСИ И ШТУКАТУРКИ",
    "ХОЗ ТОВАРЫ",
    "ЭЛЕКТРО - ТОВАРЫ",
]


def import_from_xls(xls_path: str):
    init_db()
    conn = get_db()

    df = pd.read_excel(xls_path, header=0)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)

    # Insert categories
    conn.execute("DELETE FROM products")
    conn.execute("DELETE FROM categories")
    cat_map = {}
    for i, name in enumerate(CATEGORY_NAMES):
        conn.execute(
            "INSERT INTO categories (name, sort_order) VALUES (?, ?)",
            (name, i + 1),
        )
        cat_id = conn.execute(
            "SELECT id FROM categories WHERE name = ?", (name,)
        ).fetchone()[0]
        cat_map[name] = cat_id

    # Assign products to categories by scanning rows in order
    current_category_id = None
    imported = 0
    skipped = 0

    for _, row in df.iterrows():
        name_val = row.get("Наименование")
        type_val = row.get("Тип номенклатуры")

        # Check if this is a category header row
        if pd.isna(type_val) and pd.notna(name_val):
            name_str = str(name_val).strip()
            for cat_name in CATEGORY_NAMES:
                if cat_name in name_str or name_str in cat_name:
                    current_category_id = cat_map[cat_name]
                    break
            continue

        # Skip non-product rows
        if type_val != "Товар" or current_category_id is None:
            skipped += 1
            continue

        product_name = str(name_val).strip() if pd.notna(name_val) else ""
        if not product_name or product_name == "ъ":
            skipped += 1
            continue

        code = str(row.get("Код", "")).strip()
        unit = str(row.get("Единица измерения", "шт")).strip()
        if unit == "nan" or not unit:
            unit = "шт"

        # Dual-currency pricing
        price_val = pd.to_numeric(row.get("ЦенаВал"), errors="coerce")
        price_local = pd.to_numeric(row.get("Цена"), errors="coerce")

        if pd.notna(price_val) and price_val > 0:
            price = float(price_val)
            currency = "USD"
        elif pd.notna(price_local) and price_local > 0:
            price = float(price_local)
            currency = "UZS"
        else:
            price = 0
            currency = "USD"

        weight_val = pd.to_numeric(row.get("Вес"), errors="coerce")
        weight = float(weight_val) if pd.notna(weight_val) else None

        conn.execute(
            """INSERT INTO products (code, name, category_id, unit, price, currency, weight, is_active)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
            (code, product_name, current_category_id, unit, price, currency, weight),
        )
        imported += 1

    conn.commit()

    # Print summary
    total_cats = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    total_products = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    usd_count = conn.execute("SELECT COUNT(*) FROM products WHERE currency='USD'").fetchone()[0]
    uzs_count = conn.execute("SELECT COUNT(*) FROM products WHERE currency='UZS'").fetchone()[0]

    print(f"Import complete:")
    print(f"  Categories: {total_cats}")
    print(f"  Products: {total_products} (USD: {usd_count}, UZS: {uzs_count})")
    print(f"  Skipped rows: {skipped}")

    # Show per-category counts
    rows = conn.execute(
        "SELECT c.name, COUNT(p.id) as cnt FROM categories c LEFT JOIN products p ON c.id = p.category_id GROUP BY c.id ORDER BY c.sort_order"
    ).fetchall()
    for r in rows:
        print(f"    {r['name']}: {r['cnt']}")

    conn.close()


if __name__ == "__main__":
    xls_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "products.xls")
    import_from_xls(xls_path)
