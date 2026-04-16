"""Update product prices from an Excel file.

Enhanced matching logic:
1. Exact match on original Cyrillic name (p.name)
2. Normalized match (stripped whitespace, lowercase)
3. Auto-adds NEW products from Excel (not in DB) with name standardization
4. Marks products NOT in Excel as out-of-stock (visible with badge)
5. Restores in-stock status for matched products

1C is the single source of truth — all prices from 1C are accepted as-is.
Справочник is the complete product list: if a product is not in it, it's out of stock.

Supports both .xlsx and .xls (cp1251 encoding from 1C).
"""
import io
import re
import logging
from typing import Dict, List, Tuple
from datetime import datetime

import pandas as pd
from backend.database import get_db, build_search_text

logger = logging.getLogger(__name__)

# Excel column indices (0-based) — matches both Номенклатура .xlsx and Справочник .xls
COL_NAME = 1        # Наименование
COL_TYPE = 2        # Тип номенклатуры (== "Товар" for products)
COL_UNIT = 5        # Единица измерения
COL_UZS = 6         # Цена (UZS)
COL_USD = 15        # ЦенаВал (wholesale USD)
COL_WEIGHT = 18     # Вес

# Category name for auto-added products (admin assigns real category later)
NEW_ARRIVALS_CATEGORY = "Yangi mahsulotlar"
# Default producer for products where we can't determine the producer
DEFAULT_PRODUCER = "Boshqa"


def normalize_name(name: str) -> str:
    """Normalize a product name for fuzzy matching."""
    if not name:
        return ""
    n = name.strip().lower()
    n = re.sub(r'\s+', ' ', n)
    n = re.sub(r'^[\s\-\u2013\u2014/\\:,.«»"]+', '', n)
    n = re.sub(r'[\s\-\u2013\u2014/\\:,.«»"]+$', '', n)
    return n


def read_excel_with_encoding(file_bytes: bytes) -> list:
    """Read Excel file, handling .xls cp1251 encoding from 1C.

    Returns list of rows, where each row is a list of cell values.
    """
    # Try xlrd with cp1251 override first (for .xls from 1C)
    try:
        import xlrd
        wb = xlrd.open_workbook(file_contents=file_bytes, encoding_override='cp1251')
        sh = wb.sheet_by_index(0)
        rows = []
        for r in range(sh.nrows):
            row = []
            for c in range(sh.ncols):
                row.append(sh.cell_value(r, c))
            rows.append(row)
        logger.info(f"Read {len(rows)} rows via xlrd (cp1251 override)")
        return rows
    except Exception as e:
        logger.info(f"xlrd cp1251 failed ({e}), trying pandas")

    # Fallback: pandas (works for .xlsx and well-formed .xls)
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), header=None)
        rows = [df.columns.tolist()] + df.values.tolist()
        logger.info(f"Read {len(rows)} rows via pandas")
        return rows
    except Exception as e:
        logger.info(f"pandas failed ({e}), trying xlrd default")

    # Last resort: xlrd without encoding override
    import xlrd
    wb = xlrd.open_workbook(file_contents=file_bytes)
    sh = wb.sheet_by_index(0)
    rows = []
    for r in range(sh.nrows):
        row = [sh.cell_value(r, c) for c in range(sh.ncols)]
        rows.append(row)
    logger.info(f"Read {len(rows)} rows via xlrd (default encoding)")
    return rows


def parse_price_excel(file_bytes: bytes) -> Dict[str, dict]:
    """Parse price Excel and return name→{usd, uzs, weight, unit} mapping.

    Handles both .xlsx and .xls (cp1251) formats from 1C.
    """
    rows = read_excel_with_encoding(file_bytes)
    if not rows:
        return {}

    prices = {}
    for row in rows:
        # Ensure row has enough columns
        if len(row) <= max(COL_NAME, COL_TYPE, COL_USD, COL_WEIGHT):
            continue

        # Filter: only rows where COL_TYPE == "Товар"
        type_val = str(row[COL_TYPE]).strip()
        if type_val != 'Товар':
            continue

        name = str(row[COL_NAME]).strip()
        if not name or len(name) < 3:
            continue

        # Parse USD price
        try:
            usd_raw = str(row[COL_USD]).strip()
            usd = float(usd_raw) if usd_raw else 0
        except (ValueError, TypeError):
            usd = 0

        if usd <= 0:
            continue

        # Parse UZS price
        try:
            uzs_raw = str(row[COL_UZS]).strip()
            uzs = float(uzs_raw) if uzs_raw else 0
        except (ValueError, TypeError):
            uzs = 0

        # Parse weight — from Excel column first, fallback to product name
        try:
            weight_raw = str(row[COL_WEIGHT]).strip()
            weight = float(weight_raw) if weight_raw else None
            if weight is not None and weight <= 0:
                weight = None
        except (ValueError, TypeError):
            weight = None

        if weight is None or weight == 0:
            from backend.services.parse_weight import parse_weight_from_name
            parsed_w = parse_weight_from_name(name)
            if parsed_w is not None:
                weight = parsed_w

        # Parse unit
        unit = 'sht'
        if len(row) > COL_UNIT:
            unit_raw = str(row[COL_UNIT]).strip()
            if unit_raw and unit_raw.lower() not in ('', 'none', 'nan'):
                unit = unit_raw

        prices[name] = {
            'usd': usd,
            'uzs': uzs if uzs > 0 else 0,
            'weight': weight,
            'unit': unit,
        }

    logger.info(f"Parsed {len(prices)} products with USD prices from Excel")
    return prices


def _ensure_category(conn, category_name):
    """Get or create a category by name. Returns category_id."""
    row = conn.execute(
        "SELECT id FROM categories WHERE name = ?", (category_name,)
    ).fetchone()
    if row:
        return row["id"]
    conn.execute(
        "INSERT INTO categories (name, sort_order) VALUES (?, 999)",
        (category_name,)
    )
    return conn.execute(
        "SELECT id FROM categories WHERE name = ?", (category_name,)
    ).fetchone()["id"]


def _ensure_producer(conn, producer_name):
    """Get or create a producer by name. Returns producer_id."""
    row = conn.execute(
        "SELECT id FROM producers WHERE name = ?", (producer_name,)
    ).fetchone()
    if row:
        return row["id"]
    conn.execute(
        "INSERT INTO producers (name) VALUES (?)", (producer_name,)
    )
    return conn.execute(
        "SELECT id FROM producers WHERE name = ?", (producer_name,)
    ).fetchone()["id"]


def _auto_add_product(conn, cyrillic_name, price_data, category_id, producer_id):
    """Add a new product to the database with auto-generated display name.

    Uses the same name standardization pipeline as the initial import.
    """
    from backend.services.import_products import generate_display_name

    # Generate clean display name from Cyrillic
    display_name = generate_display_name(cyrillic_name, "")

    # Build search text for cross-language search
    search_text = build_search_text(
        cyrillic_name, display_name, DEFAULT_PRODUCER
    )

    now = datetime.utcnow().isoformat()

    conn.execute(
        """INSERT INTO products
           (name, name_display, category_id, producer_id, unit,
            price_usd, price_uzs, weight, is_active,
            stock_quantity, stock_status, stock_updated_at, search_text)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, 'in_stock', ?, ?)""",
        (
            cyrillic_name,
            display_name,
            category_id,
            producer_id,
            price_data.get('unit', 'sht'),
            price_data['usd'],
            price_data['uzs'],
            price_data.get('weight'),
            now,
            search_text,
        )
    )

    return display_name


def apply_price_updates(file_bytes: bytes) -> dict:
    """Apply price updates from Excel to the database.

    1C is the single source of truth — all prices are accepted as-is.
    Справочник = complete inventory: items NOT in file are marked out-of-stock.
    Items IN the file but NOT in DB are auto-added as new products.

    Returns detailed summary.
    """
    excel_prices = parse_price_excel(file_bytes)
    if not excel_prices:
        return {"ok": False, "error": "No products found in Excel"}

    conn = get_db()
    db_products = conn.execute(
        "SELECT id, name, name_display, price_usd, price_uzs, weight, stock_status FROM products WHERE is_active = 1"
    ).fetchall()

    # Build normalized lookup for DB products
    db_by_exact = {}      # exact name → product
    db_by_normalized = {}  # normalized name → product
    for p in db_products:
        db_name = p["name"].strip()
        db_by_exact[db_name] = p
        norm = normalize_name(db_name)
        if norm not in db_by_normalized:
            db_by_normalized[norm] = p

    updated = []
    matched_db_ids = set()
    matched_excel_names = set()
    match_methods = {"exact": 0, "normalized": 0}

    for excel_name, ep in excel_prices.items():
        product = None
        method = None

        # 1. Exact match
        if excel_name in db_by_exact:
            product = db_by_exact[excel_name]
            method = "exact"
        else:
            # 2. Normalized match
            norm_excel = normalize_name(excel_name)
            if norm_excel in db_by_normalized:
                product = db_by_normalized[norm_excel]
                method = "normalized"

        if product:
            matched_db_ids.add(product["id"])
            matched_excel_names.add(excel_name)
            match_methods[method] = match_methods.get(method, 0) + 1

            old_usd = product["price_usd"] or 0
            old_uzs = product["price_uzs"] or 0
            new_usd = ep['usd']
            new_uzs = ep['uzs']

            needs_update = False
            if abs(old_usd - new_usd) > 0.001:
                needs_update = True
            if new_uzs > 0 and abs(old_uzs - new_uzs) > 0.5:
                needs_update = True

            # Also update weight if provided and different
            old_weight = product["weight"] or 0
            new_weight = ep.get('weight')
            weight_changed = False
            if new_weight and abs(old_weight - new_weight) > 0.001:
                weight_changed = True

            if needs_update or weight_changed:
                update_sql = "UPDATE products SET price_usd = ?, price_uzs = ?"
                params = [new_usd, new_uzs if new_uzs > 0 else old_uzs]

                if weight_changed:
                    update_sql += ", weight = ?"
                    params.append(new_weight)

                update_sql += " WHERE id = ?"
                params.append(product["id"])
                conn.execute(update_sql, params)

                change_record = {
                    "id": product["id"],
                    "name": (product["name_display"] or product["name"])[:50],
                    "old_usd": old_usd,
                    "new_usd": new_usd,
                }
                if weight_changed:
                    change_record["old_weight"] = old_weight
                    change_record["new_weight"] = new_weight
                updated.append(change_record)

            # /prices should ONLY touch price fields. Stock is owned by
            # /stock (update_stock.py). Previously this block forced
            # stock_status = 'in_stock' on every product found in the
            # prices file, which silently erased whatever inventory the
            # most recent /stock upload had set. Removed.

    conn.commit()

    # ── Auto-add new products (in Excel but not in DB) ──────────────
    new_products = []
    unmatched_excel = []
    for name in excel_prices:
        if name not in matched_excel_names:
            unmatched_excel.append(name)

    if unmatched_excel:
        # Ensure "Yangi mahsulotlar" category and default producer exist
        new_cat_id = _ensure_category(conn, NEW_ARRIVALS_CATEGORY)
        new_prod_id = _ensure_producer(conn, DEFAULT_PRODUCER)

        for cyrillic_name in unmatched_excel:
            try:
                display_name = _auto_add_product(
                    conn, cyrillic_name, excel_prices[cyrillic_name],
                    new_cat_id, new_prod_id
                )
                new_products.append({
                    "cyrillic": cyrillic_name[:60],
                    "display": display_name[:40],
                })
            except Exception as e:
                logger.error(f"Failed to auto-add product '{cyrillic_name}': {e}")

        # Update category/producer counts
        conn.execute("""
            UPDATE categories SET product_count = (
                SELECT COUNT(*) FROM products
                WHERE products.category_id = categories.id AND is_active = 1
            )
        """)
        conn.execute("""
            UPDATE producers SET product_count = (
                SELECT COUNT(*) FROM products
                WHERE products.producer_id = producers.id AND is_active = 1
            )
        """)
        conn.commit()
        logger.info(f"Auto-added {len(new_products)} new products to '{NEW_ARRIVALS_CATEGORY}'")

    # NOTE: previous versions of this importer marked every DB product
    # absent from the prices file as out_of_stock with qty=0. That was
    # wrong — stock is owned by /stock, not /prices. Removed.
    # Tracking unmatched_db for the return summary only, no side effects.
    unmatched_db = [p["id"] for p in db_products if p["id"] not in matched_db_ids]

    conn.close()

    return {
        "ok": True,
        "excel_products": len(excel_prices),
        "db_products": len(db_products),
        "matched": len(matched_db_ids),
        "updated": len(updated),
        "changes": updated,
        "match_methods": match_methods,
        # New products auto-added
        "new_products": new_products[:30],
        "new_products_total": len(new_products),
        # Products in the DB that were not in this prices file. No longer
        # marked out_of_stock — /stock owns that status.
        "out_of_stock_count": 0,
        "restored_in_stock": 0,
        "unmatched_excel": [],
        "unmatched_excel_total": 0,
        "unmatched_db_count": len(unmatched_db),
    }
