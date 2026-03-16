"""Update product prices from an Excel file."""
import io
import re
import logging
from typing import Dict, List, Tuple

import pandas as pd
from backend.database import get_db

logger = logging.getLogger(__name__)

# Excel column indices (0-based)
COL_NAME = 1        # Наименование
COL_TYPE = 2        # Тип номенклатуры (== "Товар" for products)
COL_UNIT = 5        # Единица измерения
COL_UZS = 6         # Цена (UZS)
COL_USD = 15        # ЦенаВал (wholesale USD)
COL_WEIGHT = 18     # Вес


def parse_price_excel(file_bytes: bytes) -> Dict[str, dict]:
    """Parse price Excel and return name→{usd, uzs, weight} mapping."""
    df = pd.read_excel(io.BytesIO(file_bytes), header=None)
    products = df[(df[COL_TYPE] == 'Товар') & df[COL_NAME].notna()]

    prices = {}
    for _, row in products.iterrows():
        name = str(row[COL_NAME]).strip()
        usd = pd.to_numeric(row[COL_USD], errors='coerce')
        uzs = pd.to_numeric(row[COL_UZS], errors='coerce')
        weight = pd.to_numeric(row[COL_WEIGHT], errors='coerce')

        if name and pd.notna(usd) and usd > 0:
            prices[name] = {
                'usd': float(usd),
                'uzs': float(uzs) if pd.notna(uzs) and uzs > 0 else 0,
                'weight': float(weight) if pd.notna(weight) and weight > 0 else None,
            }
    return prices


def apply_price_updates(file_bytes: bytes) -> dict:
    """Apply price updates from Excel to the database. Returns summary."""
    excel_prices = parse_price_excel(file_bytes)
    if not excel_prices:
        return {"ok": False, "error": "No products found in Excel"}

    conn = get_db()
    db_products = conn.execute("SELECT id, name, price_usd, price_uzs FROM products").fetchall()

    updated = []
    for p in db_products:
        db_name = p["name"].strip()
        if db_name in excel_prices:
            ep = excel_prices[db_name]
            old_usd = p["price_usd"] or 0
            old_uzs = p["price_uzs"] or 0
            new_usd = ep['usd']
            new_uzs = ep['uzs']

            needs_update = False
            if abs(old_usd - new_usd) > 0.001:
                needs_update = True
            if new_uzs > 0 and abs(old_uzs - new_uzs) > 0.5:
                needs_update = True

            if needs_update:
                conn.execute(
                    "UPDATE products SET price_usd = ?, price_uzs = ? WHERE id = ?",
                    (new_usd, new_uzs if new_uzs > 0 else old_uzs, p["id"]),
                )
                updated.append({
                    "id": p["id"],
                    "name": db_name[:50],
                    "old_usd": old_usd,
                    "new_usd": new_usd,
                })

    conn.commit()
    conn.close()

    matched = sum(1 for p in db_products if p["name"].strip() in excel_prices)

    return {
        "ok": True,
        "excel_products": len(excel_prices),
        "db_products": len(db_products),
        "matched": matched,
        "updated": len(updated),
        "changes": updated,
    }
