"""Update product stock/inventory levels from an Excel file.

Expected Excel format from 1C:
- Column B (index 1): Product name (Наименование) — matches products.name (Cyrillic)
- Column C (index 2): Тип номенклатуры — filter for "Товар" only
- One or more quantity columns (auto-detected, or configurable)

Stock status thresholds:
- stock_quantity > 10: "in_stock"
- 0 < stock_quantity <= 10: "low_stock"
- stock_quantity == 0 or NULL: "out_of_stock"

The exact column for quantity will be auto-detected or can be configured
when the actual 1C export format is known.
"""
import io
import re
import logging
from typing import Dict, Optional

import pandas as pd
from backend.database import get_db

logger = logging.getLogger(__name__)

# Default column indices (0-based) — same as price Excel
COL_NAME = 1         # Наименование
COL_TYPE = 2         # Тип номенклатуры (== "Товар")

# Stock quantity column — will be configured once 1C export format is known
# For now, try common positions: column D (3), or search for "Остаток"/"Количество" header
COL_STOCK_CANDIDATES = [3, 4, 7, 8, 9, 10]

# Stock status thresholds
THRESHOLD_LOW = 10   # <= this = "low_stock"


def normalize_name(name: str) -> str:
    """Normalize a product name for matching."""
    if not name:
        return ""
    n = name.strip().lower()
    n = re.sub(r'\s+', ' ', n)
    n = re.sub(r'^[\s\-\u2013\u2014/\\:,.«»"]+', '', n)
    n = re.sub(r'[\s\-\u2013\u2014/\\:,.«»"]+$', '', n)
    return n


def detect_stock_column(df) -> Optional[int]:
    """Try to auto-detect which column has stock quantities.

    Looks for header rows containing stock-related keywords in Russian/English.
    Handles variations like 'Кол-во', 'Кол - во', 'Кол.во', 'кол', etc.
    Falls back to candidate column indices.
    """
    # Check first 10 rows for header keywords (some 1C exports have title rows)
    stock_keywords = [
        'остаток', 'количество', 'кол-во', 'кол -во', 'кол - во',
        'кол.во', 'кол.', 'stock', 'qty', 'запас', 'наличие',
        'остат', 'колич', 'кол‑во',  # em-dash variant
    ]
    for row_idx in range(min(10, len(df))):
        for col_idx in range(len(df.columns)):
            cell = str(df.iloc[row_idx, col_idx]).strip().lower()
            # Normalize dashes and spaces for matching
            cell_normalized = re.sub(r'[\s\-\u2013\u2014\u2010\u2011]+', '', cell)
            if any(kw.replace('-', '').replace(' ', '') in cell_normalized for kw in stock_keywords):
                logger.info(f"Found stock column at row {row_idx}, col {col_idx}: '{df.iloc[row_idx, col_idx]}'")
                return col_idx
            # Also check the raw cell with simple 'in'
            if any(kw in cell for kw in stock_keywords):
                logger.info(f"Found stock column at row {row_idx}, col {col_idx}: '{df.iloc[row_idx, col_idx]}'")
                return col_idx

    # Fallback: try candidate columns, pick the one with most numeric values
    best_col = None
    best_count = 0
    for col in COL_STOCK_CANDIDATES:
        if col < len(df.columns):
            numeric_count = pd.to_numeric(df[col], errors='coerce').notna().sum()
            if numeric_count > best_count:
                best_count = numeric_count
                best_col = col

    if best_col is not None:
        logger.info(f"Fallback: using column {best_col} (most numeric values: {best_count})")
    return best_col


def compute_stock_status(quantity: Optional[float]) -> str:
    """Compute stock status from quantity."""
    if quantity is None or quantity <= 0:
        return "out_of_stock"
    elif quantity <= THRESHOLD_LOW:
        return "low_stock"
    else:
        return "in_stock"


def detect_name_column(df) -> int:
    """Detect which column contains product names.

    Looks for header keywords like 'Наименование', 'Номенклатура', 'Товар', 'Название'.
    Falls back to column 1 (B) or the first text-heavy column.
    """
    name_keywords = ['наименование', 'номенклатура', 'товар', 'название', 'продукт', 'name']
    for row_idx in range(min(10, len(df))):
        for col_idx in range(len(df.columns)):
            cell = str(df.iloc[row_idx, col_idx]).strip().lower()
            if any(kw in cell for kw in name_keywords):
                logger.info(f"Found name column at row {row_idx}, col {col_idx}: '{df.iloc[row_idx, col_idx]}'")
                return col_idx
    return COL_NAME  # Default: column B (index 1)


def parse_stock_excel(file_bytes: bytes, stock_col: Optional[int] = None) -> Dict[str, dict]:
    """Parse stock Excel and return name→{quantity, status} mapping."""
    df = pd.read_excel(io.BytesIO(file_bytes), header=None)

    # Auto-detect name column
    name_col = detect_name_column(df)

    # Detect stock column
    if stock_col is None:
        stock_col = detect_stock_column(df)

    if stock_col is None:
        logger.warning("Could not detect stock quantity column")
        return {}

    # Filter for products only (if type column exists and has "Товар" values)
    has_type_col = COL_TYPE < len(df.columns)
    type_has_tovar = has_type_col and (df[COL_TYPE] == 'Товар').any()
    if type_has_tovar:
        products = df[(df[COL_TYPE] == 'Товар') & df[name_col].notna()]
    else:
        # No type column or no "Товар" values — use all rows with names
        products = df[df[name_col].notna()]
        # Skip header-like rows (first few rows that aren't product data)
        products = products[products[name_col].apply(
            lambda x: isinstance(x, str) and len(str(x).strip()) > 3
        )]

    logger.info(f"Using name column {name_col}, stock column {stock_col}")

    stocks = {}
    for _, row in products.iterrows():
        name = str(row[name_col]).strip()
        qty = pd.to_numeric(row[stock_col] if stock_col < len(row) else None, errors='coerce')

        if name:
            quantity = float(qty) if pd.notna(qty) else 0
            stocks[name] = {
                'quantity': quantity,
                'status': compute_stock_status(quantity),
            }

    return stocks


def apply_stock_updates(file_bytes: bytes, stock_col: Optional[int] = None) -> dict:
    """Apply stock updates from Excel to the database. Returns summary."""
    excel_stocks = parse_stock_excel(file_bytes, stock_col)
    if not excel_stocks:
        return {"ok": False, "error": "No stock data found in Excel. Could not detect quantity column."}

    conn = get_db()
    db_products = conn.execute(
        "SELECT id, name, name_display, stock_quantity, stock_status FROM products WHERE is_active = 1"
    ).fetchall()

    # Build lookup
    db_by_exact = {}
    db_by_normalized = {}
    for p in db_products:
        db_name = p["name"].strip()
        db_by_exact[db_name] = p
        norm = normalize_name(db_name)
        if norm not in db_by_normalized:
            db_by_normalized[norm] = p

    updated = []
    matched_count = 0
    status_counts = {"in_stock": 0, "low_stock": 0, "out_of_stock": 0}

    for excel_name, stock_data in excel_stocks.items():
        product = None

        # Exact match first, then normalized
        if excel_name in db_by_exact:
            product = db_by_exact[excel_name]
        else:
            norm = normalize_name(excel_name)
            if norm in db_by_normalized:
                product = db_by_normalized[norm]

        if product:
            matched_count += 1
            new_qty = stock_data['quantity']
            new_status = stock_data['status']
            old_qty = product["stock_quantity"]
            old_status = product["stock_status"]

            status_counts[new_status] = status_counts.get(new_status, 0) + 1

            # Check if update needed
            qty_changed = old_qty is None or abs((old_qty or 0) - new_qty) > 0.001
            status_changed = old_status != new_status

            if qty_changed or status_changed:
                conn.execute(
                    """UPDATE products
                       SET stock_quantity = ?, stock_status = ?, stock_updated_at = datetime('now')
                       WHERE id = ?""",
                    (new_qty, new_status, product["id"]),
                )

                # Track notable changes (status transitions)
                if status_changed and old_status is not None:
                    updated.append({
                        "id": product["id"],
                        "name": (product["name_display"] or product["name"])[:50],
                        "old_status": old_status or "unknown",
                        "new_status": new_status,
                        "quantity": new_qty,
                    })

    conn.commit()
    conn.close()

    unmatched_count = len(excel_stocks) - matched_count

    return {
        "ok": True,
        "excel_products": len(excel_stocks),
        "db_products": len(db_products),
        "matched": matched_count,
        "updated": len(updated),
        "status_changes": updated[:30],
        "status_counts": status_counts,
        "unmatched_count": unmatched_count,
    }
