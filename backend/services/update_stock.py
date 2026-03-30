"""Update product stock/inventory levels from an Excel file.

Supports the 1C "Прайс лист" export format:
- .xls (cp1251 encoding) or .xlsx
- Layout: Col 1=№, Col 2=Наименование, Col 3=Ед.изм., Col 4=кол-во, Col 5=кол-во упак.
- Header row auto-detected, category rows skipped
- Product names match products.name (Cyrillic) in the database

Stock status thresholds:
- stock_quantity > 10: "in_stock"
- 0 < stock_quantity <= 10: "low_stock"
- stock_quantity == 0 or NULL: "out_of_stock"
"""
import io
import re
import logging
from typing import Dict, Optional

import pandas as pd
from backend.database import get_db

logger = logging.getLogger(__name__)

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


def compute_stock_status(quantity: Optional[float]) -> str:
    """Compute stock status from quantity."""
    if quantity is None or quantity <= 0:
        return "out_of_stock"
    elif quantity <= THRESHOLD_LOW:
        return "low_stock"
    else:
        return "in_stock"


def read_excel_with_encoding(file_bytes: bytes) -> pd.DataFrame:
    """Read Excel file, handling .xls cp1251 encoding issues.

    Old .xls files from 1C often lack a CODEPAGE record, causing pandas/xlrd
    to default to iso-8859-1 which garbles Cyrillic text. We try multiple
    approaches to get proper decoding.
    """
    # First try: read with xlrd and cp1251 override (for .xls files)
    try:
        import xlrd
        wb = xlrd.open_workbook(file_contents=file_bytes, encoding_override='cp1251')
        ws = wb.sheet_by_index(0)
        data = []
        for i in range(ws.nrows):
            row = []
            for j in range(ws.ncols):
                row.append(ws.cell_value(i, j))
            data.append(row)
        wb.release_resources()
        df = pd.DataFrame(data)
        # Verify Cyrillic came through (check for common Russian chars)
        sample = ' '.join(str(v) for v in df.iloc[:10].values.flatten() if v)
        if any('\u0400' <= c <= '\u04ff' for c in sample):
            logger.info("Read .xls with cp1251 encoding override — Cyrillic OK")
            return df
    except Exception as e:
        logger.info(f"xlrd cp1251 read failed: {e}")

    # Second try: standard pandas read (works for .xlsx and some .xls)
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), header=None)
        sample = ' '.join(str(v) for v in df.iloc[:10].values.flatten() if v)
        if any('\u0400' <= c <= '\u04ff' for c in sample):
            logger.info("Read with pandas — Cyrillic OK")
            return df
        logger.info("Pandas read succeeded but no Cyrillic detected, trying xlrd fallback")
    except Exception as e:
        logger.info(f"Pandas read failed: {e}")

    # Third try: xlrd without encoding override
    try:
        import xlrd
        wb = xlrd.open_workbook(file_contents=file_bytes)
        ws = wb.sheet_by_index(0)
        data = []
        for i in range(ws.nrows):
            row = [ws.cell_value(i, j) for j in range(ws.ncols)]
            data.append(row)
        wb.release_resources()
        df = pd.DataFrame(data)
        logger.info("Read .xls with xlrd default encoding")
        return df
    except Exception as e:
        logger.info(f"xlrd default read failed: {e}")

    # Last resort: pandas with no special handling
    return pd.read_excel(io.BytesIO(file_bytes), header=None)


def detect_columns(df) -> dict:
    """Auto-detect column layout from header rows.

    Returns dict with 'name_col', 'stock_col', 'pkg_col', 'header_row'.
    """
    name_keywords = ['наименование', 'номенклатура', 'название', 'товар', 'name']
    stock_keywords = [
        'кол-во', 'колво', 'количество', 'остаток', 'qty', 'stock',
        'запас', 'наличие', 'кол.', 'кол ',
    ]

    result = {'name_col': None, 'stock_col': None, 'pkg_col': None, 'header_row': None}

    for row_idx in range(min(15, len(df))):
        for col_idx in range(len(df.columns)):
            cell_raw = str(df.iloc[row_idx, col_idx]).strip()
            cell = cell_raw.lower()
            # Normalize: remove newlines, collapse spaces
            cell_clean = re.sub(r'\s+', ' ', cell).strip()
            cell_nodash = re.sub(r'[\s\-\u2013\u2014\u2010\u2011]+', '', cell)

            # Detect name column
            if result['name_col'] is None:
                if any(kw in cell_clean for kw in name_keywords):
                    result['name_col'] = col_idx
                    result['header_row'] = row_idx
                    logger.info(f"Name column: col {col_idx} at row {row_idx} [{cell_raw}]")

            # Detect stock column (first кол-во match = quantity, second = packages)
            for kw in stock_keywords:
                kw_nodash = kw.replace('-', '').replace(' ', '')
                if kw_nodash in cell_nodash or kw in cell_clean:
                    if result['stock_col'] is None:
                        result['stock_col'] = col_idx
                        result['header_row'] = row_idx
                        logger.info(f"Stock column: col {col_idx} at row {row_idx} [{cell_raw}]")
                    elif result['pkg_col'] is None and col_idx != result['stock_col']:
                        # Second кол-во column = packages
                        if 'упак' in cell_clean:
                            result['pkg_col'] = col_idx
                            logger.info(f"Package column: col {col_idx} at row {row_idx} [{cell_raw}]")
                        elif result['stock_col'] is not None:
                            result['pkg_col'] = col_idx
                            logger.info(f"Package column (assumed): col {col_idx} [{cell_raw}]")
                    break

    # Defaults if not detected
    if result['name_col'] is None:
        result['name_col'] = 2  # Col C (common in 1C exports)
    if result['header_row'] is None:
        result['header_row'] = 0

    return result


def parse_stock_excel(file_bytes: bytes) -> Dict[str, dict]:
    """Parse stock Excel and return name→{quantity, status} mapping."""
    df = read_excel_with_encoding(file_bytes)

    if df.empty:
        logger.warning("Empty DataFrame after reading Excel")
        return {}

    # Detect layout
    cols = detect_columns(df)
    name_col = cols['name_col']
    stock_col = cols['stock_col']
    header_row = cols['header_row']

    if stock_col is None:
        logger.warning("Could not detect stock quantity column in any header row")
        return {}

    logger.info(f"Layout: name={name_col}, stock={stock_col}, header_row={header_row}")

    # Extract product rows (skip header and category rows)
    stocks = {}
    for i in range(header_row + 1, len(df)):
        name_val = df.iloc[i, name_col] if name_col < len(df.columns) else None
        stock_val = df.iloc[i, stock_col] if stock_col < len(df.columns) else None

        # Skip empty/non-string names and category headers
        if not isinstance(name_val, str) or len(name_val.strip()) < 4:
            continue

        name = name_val.strip()
        qty = pd.to_numeric(stock_val, errors='coerce')
        quantity = float(qty) if pd.notna(qty) else 0

        stocks[name] = {
            'quantity': quantity,
            'status': compute_stock_status(quantity),
        }

    logger.info(f"Parsed {len(stocks)} products from stock file")
    return stocks


def apply_stock_updates(file_bytes: bytes) -> dict:
    """Apply stock updates from Excel to the database. Returns summary."""
    excel_stocks = parse_stock_excel(file_bytes)
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

            qty_changed = old_qty is None or abs((old_qty or 0) - new_qty) > 0.001
            status_changed = old_status != new_status

            if qty_changed or status_changed:
                conn.execute(
                    """UPDATE products
                       SET stock_quantity = ?, stock_status = ?, stock_updated_at = datetime('now')
                       WHERE id = ?""",
                    (new_qty, new_status, product["id"]),
                )

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
