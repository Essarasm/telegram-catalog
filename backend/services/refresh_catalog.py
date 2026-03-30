"""Refresh product catalog from an Excel file without full redeploy.

Reads the same 'Catalog Clean' sheet format as import_products.py but:
- Adds NEW products (not in DB) with proper category/producer linking
- Updates existing products (price, weight, unit changes)
- Marks products NOT in the new Excel as inactive (is_active=0)
- Reactivates previously deactivated products if they reappear

This is a non-destructive operation — it never deletes products (preserving
cart references and order history), only toggles is_active.
"""
import io
import re
import logging
from typing import Dict, List, Optional

from openpyxl import load_workbook
from backend.database import get_db, init_db, build_search_text

logger = logging.getLogger(__name__)


# ── Cyrillic → Latin transliteration (duplicated from import_products for independence)
CYRILLIC_MAP = {
    'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E',
    'Ё': 'Yo', 'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K',
    'Л': 'L', 'М': 'M', 'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R',
    'С': 'S', 'Т': 'T', 'У': 'U', 'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts',
    'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Shch', 'Ъ': '', 'Ы': 'Y', 'Ь': '',
    'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya',
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e',
    'ё': 'yo', 'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k',
    'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r',
    'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts',
    'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '',
    'э': 'e', 'ю': 'yu', 'я': 'ya',
}


def transliterate(text):
    if not text:
        return text
    return ''.join(CYRILLIC_MAP.get(ch, ch) for ch in text)


def cyrillic_title_case(text):
    words = text.split()
    result = []
    for word in words:
        if not any('\u0400' <= c <= '\u04ff' for c in word):
            result.append(word)
        elif len(word) <= 3 and word.isupper():
            result.append(word)
        else:
            result.append(word[0].upper() + word[1:].lower() if len(word) > 1 else word.upper())
    return ' '.join(result)


def normalize_name(name: str) -> str:
    """Normalize a product name for matching."""
    if not name:
        return ""
    n = name.strip().lower()
    n = re.sub(r'\s+', ' ', n)
    return n


def refresh_catalog_from_excel(file_bytes: bytes) -> dict:
    """Refresh catalog from uploaded Excel. Returns summary of changes."""
    init_db()
    conn = get_db()

    # Load Excel
    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)

    # Try 'Catalog Clean' first, fall back to first sheet
    if 'Catalog Clean' in wb.sheetnames:
        ws = wb['Catalog Clean']
    else:
        ws = wb.active
        if ws is None:
            wb.close()
            return {"ok": False, "error": "No sheets found in Excel file"}

    # Count existing products
    db_products_before = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1"
    ).fetchone()[0]

    # Build lookup of existing products by normalized Cyrillic name
    existing_products = conn.execute(
        "SELECT id, name, name_display, category_id, producer_id, price_usd, price_uzs, weight, unit, is_active FROM products"
    ).fetchall()
    db_by_name = {}
    for p in existing_products:
        norm = normalize_name(p["name"])
        if norm not in db_by_name:
            db_by_name[norm] = p

    # Existing categories and producers
    cat_rows = conn.execute("SELECT id, name FROM categories").fetchall()
    cat_map = {r["name"]: r["id"] for r in cat_rows}

    prod_rows = conn.execute("SELECT id, name FROM producers").fetchall()
    prod_map = {r["name"]: r["id"] for r in prod_rows}

    # Process Excel rows
    rows = list(ws.iter_rows(min_row=2, values_only=False))

    excel_names_seen = set()  # Track which DB products are in this Excel
    new_products = 0
    updated_products = 0
    reactivated_products = 0
    new_product_names = []

    for row in rows:
        category = row[0].value    # A
        producer = row[1].value    # B
        name = row[2].value        # C — curated Latin display name
        weight_val = row[3].value if len(row) > 3 else None
        unit = row[4].value if len(row) > 4 else 'sht'
        price_uzs = row[5].value if len(row) > 5 else None
        price_usd = row[6].value if len(row) > 6 else None
        original_cyrillic_col = row[11].value if len(row) > 11 else None

        if not category or not producer or not name:
            continue

        category = str(category).strip()
        producer = str(producer).strip()
        name = str(name).strip()
        unit = str(unit).strip() if unit else 'sht'

        # Transliterate producer
        producer_latin = transliterate(cyrillic_title_case(producer))

        # Original Cyrillic name
        original_cyrillic = str(original_cyrillic_col).strip() if original_cyrillic_col else name

        # Parse prices
        p_usd = 0
        p_uzs = 0
        try:
            if price_usd is not None and price_usd != '' and price_usd != 0:
                p_usd = float(price_usd)
        except (ValueError, TypeError):
            pass
        try:
            if price_uzs is not None and price_uzs != '' and price_uzs != 0:
                p_uzs = float(price_uzs)
        except (ValueError, TypeError):
            pass

        # Parse weight
        weight = None
        try:
            if weight_val is not None and weight_val != '':
                weight = float(weight_val)
        except (ValueError, TypeError):
            pass

        # Display name
        display_name = name
        if any('\u0400' <= c <= '\u04ff' for c in display_name):
            display_name = transliterate(cyrillic_title_case(display_name))

        # Ensure category exists
        if category not in cat_map:
            conn.execute(
                "INSERT OR IGNORE INTO categories (name, sort_order) VALUES (?, ?)",
                (category, len(cat_map) + 1)
            )
            cat_id = conn.execute("SELECT id FROM categories WHERE name = ?", (category,)).fetchone()[0]
            cat_map[category] = cat_id

        # Ensure producer exists
        if producer_latin not in prod_map:
            conn.execute(
                "INSERT OR IGNORE INTO producers (name) VALUES (?)", (producer_latin,)
            )
            prod_id = conn.execute("SELECT id FROM producers WHERE name = ?", (producer_latin,)).fetchone()[0]
            prod_map[producer_latin] = prod_id

        # Check if product exists
        norm_cyrillic = normalize_name(original_cyrillic)
        excel_names_seen.add(norm_cyrillic)

        existing = db_by_name.get(norm_cyrillic)

        if existing:
            # Product exists — check for updates
            changes = []
            if p_usd > 0 and abs((existing["price_usd"] or 0) - p_usd) > 0.001:
                changes.append(("price_usd", p_usd))
            if p_uzs > 0 and abs((existing["price_uzs"] or 0) - p_uzs) > 0.5:
                changes.append(("price_uzs", p_uzs))
            if weight and abs((existing["weight"] or 0) - weight) > 0.001:
                changes.append(("weight", weight))
            if existing["category_id"] != cat_map[category]:
                changes.append(("category_id", cat_map[category]))
            if existing["producer_id"] != prod_map[producer_latin]:
                changes.append(("producer_id", prod_map[producer_latin]))

            # Reactivate if was deactivated
            if not existing["is_active"]:
                changes.append(("is_active", 1))
                reactivated_products += 1

            if changes:
                set_clauses = ", ".join(f"{col} = ?" for col, _ in changes)
                values = [v for _, v in changes] + [existing["id"]]
                conn.execute(f"UPDATE products SET {set_clauses} WHERE id = ?", values)
                updated_products += 1
        else:
            # New product — insert
            search_text = build_search_text(original_cyrillic, display_name, producer_latin)
            conn.execute(
                """INSERT INTO products
                   (name, name_display, category_id, producer_id, unit,
                    price_usd, price_uzs, weight, is_active, search_text)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)""",
                (original_cyrillic, display_name, cat_map[category], prod_map[producer_latin],
                 unit, p_usd, p_uzs, weight, search_text)
            )
            new_products += 1
            new_product_names.append(display_name[:50])

    # Deactivate products NOT in the new Excel
    deactivated_products = 0
    deactivated_names = []
    for p in existing_products:
        norm = normalize_name(p["name"])
        if norm not in excel_names_seen and p["is_active"]:
            conn.execute("UPDATE products SET is_active = 0 WHERE id = ?", (p["id"],))
            deactivated_products += 1
            deactivated_names.append((p["name_display"] or p["name"])[:50])

    # Update denormalized counts
    conn.execute("""
        UPDATE categories SET product_count = (
            SELECT COUNT(*) FROM products WHERE products.category_id = categories.id AND is_active = 1
        )
    """)
    conn.execute("""
        UPDATE producers SET product_count = (
            SELECT COUNT(*) FROM products WHERE products.producer_id = producers.id AND is_active = 1
        )
    """)

    conn.commit()
    conn.close()
    wb.close()

    return {
        "ok": True,
        "excel_products": len(excel_names_seen),
        "db_products_before": db_products_before,
        "new_products": new_products,
        "updated_products": updated_products,
        "reactivated_products": reactivated_products,
        "deactivated_products": deactivated_products,
        "new_product_names": new_product_names[:20],
        "deactivated_names": deactivated_names[:20],
    }
