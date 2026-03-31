"""Update product prices from an Excel file.

Enhanced matching logic:
1. Exact match on original Cyrillic name (p.name)
2. Normalized match (stripped whitespace, lowercase)
3. Reports unmatched products from both sides (Excel not in DB, DB not in Excel)

1C is the single source of truth — all prices from 1C are accepted as-is.

Supports both .xlsx and .xls (cp1251 encoding from 1C).
"""
import io
import re
import logging
from typing import Dict, List, Tuple

import pandas as pd
from backend.database import get_db

logger = logging.getLogger(__name__)

# Excel column indices (0-based) — matches both Номенклатура .xlsx and Справочник .xls
COL_NAME = 1        # Наименование
COL_TYPE = 2        # Тип номенклатуры (== "Товар" for products)
COL_UNIT = 5        # Единица измерения
COL_UZS = 6         # Цена (UZS)
COL_USD = 15        # ЦенаВал (wholesale USD)
COL_WEIGHT = 18     # Вес


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
    """Parse price Excel and return name→{usd, uzs, weight} mapping.

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
        if not name:
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

        # Parse weight
        try:
            weight_raw = str(row[COL_WEIGHT]).strip()
            weight = float(weight_raw) if weight_raw else None
            if weight is not None and weight <= 0:
                weight = None
        except (ValueError, TypeError):
            weight = None

        prices[name] = {
            'usd': usd,
            'uzs': uzs if uzs > 0 else 0,
            'weight': weight,
        }

    logger.info(f"Parsed {len(prices)} products with USD prices from Excel")
    return prices


def apply_price_updates(file_bytes: bytes) -> dict:
    """Apply price updates from Excel to the database.

    1C is the single source of truth — all prices are accepted as-is.
    Returns detailed summary.
    """
    excel_prices = parse_price_excel(file_bytes)
    if not excel_prices:
        return {"ok": False, "error": "No products found in Excel"}

    conn = get_db()
    db_products = conn.execute(
        "SELECT id, name, name_display, price_usd, price_uzs, weight FROM products WHERE is_active = 1"
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

    conn.commit()

    # Find unmatched items
    unmatched_excel = []
    for name in excel_prices:
        if name not in matched_excel_names:
            unmatched_excel.append(name[:60])

    unmatched_db_count = len(db_products) - len(matched_db_ids)

    conn.close()

    return {
        "ok": True,
        "excel_products": len(excel_prices),
        "db_products": len(db_products),
        "matched": len(matched_db_ids),
        "updated": len(updated),
        "changes": updated,
        "match_methods": match_methods,
        "unmatched_excel": unmatched_excel[:30],
        "unmatched_excel_total": len(unmatched_excel),
        "unmatched_db_count": unmatched_db_count,
    }
