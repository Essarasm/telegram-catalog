"""Update product prices from an Excel file.

Enhanced matching logic:
1. Exact match on original Cyrillic name (p.name)
2. Normalized match (stripped whitespace, lowercase)
3. Reports unmatched products from both sides (Excel not in DB, DB not in Excel)

Safety guards:
- Skip incoming prices below MIN_PRICE_USD (likely 1C placeholders like $0.09)
- Reject price drops exceeding MAX_DROP_PCT (likely data errors)
"""
import io
import re
import logging
from typing import Dict, List, Tuple
from difflib import SequenceMatcher

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

# Safety thresholds
MIN_PRICE_USD = 0.50     # Skip prices below this (likely 1C placeholders like $0.09)
MAX_DROP_PCT = 80        # Reject price drops larger than this % (likely data errors)


def normalize_name(name: str) -> str:
    """Normalize a product name for fuzzy matching."""
    if not name:
        return ""
    # Lowercase, strip, collapse whitespace, remove extra punctuation
    n = name.strip().lower()
    n = re.sub(r'\s+', ' ', n)
    # Remove leading/trailing punctuation
    n = re.sub(r'^[\s\-\u2013\u2014/\\:,.«»"]+', '', n)
    n = re.sub(r'[\s\-\u2013\u2014/\\:,.«»"]+$', '', n)
    return n


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
    """Apply price updates from Excel to the database. Returns detailed summary."""
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
    skipped_low_price = 0      # Incoming price too low (placeholder)
    skipped_big_drop = 0       # Price drop exceeds safety threshold
    placeholder_fixes = 0      # Products gaining real price from placeholder

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

            # ── Safety guard 1: skip low incoming prices (1C placeholders) ──
            if new_usd < MIN_PRICE_USD:
                if old_usd >= MIN_PRICE_USD:
                    # Would overwrite real price with placeholder — skip
                    skipped_low_price += 1
                    continue
                # Both old and new are low — not a real product, skip entirely
                continue

            # ── Safety guard 2: reject suspiciously large price drops ──
            if old_usd > MIN_PRICE_USD and new_usd < old_usd:
                drop_pct = (old_usd - new_usd) / old_usd * 100
                if drop_pct > MAX_DROP_PCT:
                    skipped_big_drop += 1
                    continue

            # Track placeholder → real price fixes
            if old_usd < MIN_PRICE_USD and new_usd >= MIN_PRICE_USD:
                placeholder_fixes += 1

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
        "unmatched_excel": unmatched_excel[:30],  # Top 30 unmatched from Excel
        "unmatched_excel_total": len(unmatched_excel),
        "unmatched_db_count": unmatched_db_count,
        "skipped_low_price": skipped_low_price,
        "skipped_big_drop": skipped_big_drop,
        "placeholder_fixes": placeholder_fixes,
    }
