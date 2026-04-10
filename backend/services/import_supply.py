"""1C "Поступление товаров" parser — warehouse receipts + client returns.

Reuses the V-marker pattern from import_real_orders.py (col[0]=="V" for
doc headers, blank for line items). 17 columns.

Doc types classified by Контрагент:
  - "В О З В Р А Т" (letters separated by spaces) → 'return'
  - "ИСПРАВЛЕНИЕ" / "ИСПРАВЛЕНИЕ СКЛАД 2" → 'adjustment'
  - Everything else → 'supply'

Returns have Price=0, Sum=0 — only quantity is tracked.
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

from backend.database import get_db

# ── Reuse helpers from import_real_orders ─────────────────────────────
from backend.services.import_real_orders import (
    _load_workbook,
    _norm,
    _parse_number,
    _parse_doc_date,
    _parse_doc_time,
    _Sheet,
)

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────

SYSTEM_ADJUSTMENT_NAMES = frozenset([
    "исправление",
    "исправлениесклад2",
])


def _normalize_counterparty(raw: str) -> str:
    """Strip spaces and lowercase for classification."""
    return re.sub(r"\s+", "", raw).lower()


def _classify_doc_type(counterparty_name: str) -> str:
    """Classify document type from Контрагент value."""
    norm = _normalize_counterparty(counterparty_name)
    if norm == "возврат":
        return "return"
    if norm in SYSTEM_ADJUSTMENT_NAMES:
        return "adjustment"
    return "supply"


def _detect_currency(val) -> str:
    """Detect currency from Валюта column value."""
    s = str(val or "").strip().upper()
    if "USD" in s:
        return "USD"
    return "UZS"


# ── Product matching (same cache pattern as import_real_orders) ────────

_PRODUCT_CACHE: Dict[str, Optional[int]] = {}


def _try_match_product(product_name_raw: str, conn) -> Optional[int]:
    """Match a 1C product name to products.id. Cached per import run.

    Strategy:
    1. Exact match on products.name (original 1C Cyrillic name)
    2. Normalized match: LOWER(TRIM(name))
    """
    key = product_name_raw.strip().lower()
    if key in _PRODUCT_CACHE:
        return _PRODUCT_CACHE[key]

    # Exact match
    row = conn.execute(
        "SELECT id FROM products WHERE LOWER(TRIM(name)) = ? LIMIT 1",
        (key,),
    ).fetchone()
    if row:
        _PRODUCT_CACHE[key] = row[0]
        return row[0]

    # Try name_display
    row = conn.execute(
        "SELECT id FROM products WHERE LOWER(TRIM(name_display)) = ? LIMIT 1",
        (key,),
    ).fetchone()
    if row:
        _PRODUCT_CACHE[key] = row[0]
        return row[0]

    _PRODUCT_CACHE[key] = None
    return None


# ── Parser ─────────────────────────────────────────────────────────────


def parse_supply_xls(
    file_bytes: bytes,
    filename_hint: str = "",
) -> dict:
    """Parse a 1C "Поступление товаров" XLS file.

    Returns {ok, documents: [...], stats: {...}} or {ok: False, error: ...}.
    Each document has doc-level fields + items list.
    """
    sh, err = _load_workbook(file_bytes, filename_hint)
    if err or sh is None:
        return {"ok": False, "error": err or "Could not open workbook"}

    documents: List[dict] = []
    current: Optional[dict] = None

    for r in range(sh.nrows):
        marker = str(sh.cell(r, 0) or "").strip().upper()

        if marker == "V":
            # ── Doc-level header row ─────────────────────────────────
            if current is not None:
                documents.append(current)

            counterparty = str(sh.cell(r, 5) or "").strip()
            currency_raw = str(sh.cell(r, 16) or "").strip()
            currency = _detect_currency(currency_raw)
            exchange_rate = _parse_number(sh.cell(r, 15)) or (1.0 if currency == "UZS" else 0)

            current = {
                "doc_number": str(sh.cell(r, 1) or "").strip(),
                "doc_date": _parse_doc_date(sh.cell(r, 2)),
                "doc_time": _parse_doc_time(sh.cell(r, 3)),
                "author": str(sh.cell(r, 4) or "").strip() or None,
                "counterparty_name": counterparty,
                "doc_type": _classify_doc_type(counterparty),
                "contract": str(sh.cell(r, 6) or "").strip() or None,
                "counterparty_account": str(sh.cell(r, 7) or "").strip() or None,
                "warehouse": str(sh.cell(r, 8) or "").strip() or None,
                "vat_rate": str(sh.cell(r, 9) or "").strip() or None,
                "receipt_type": str(sh.cell(r, 10) or "").strip() or None,
                "supplier_advance": _parse_number(sh.cell(r, 11)),
                "supplier_advance_offset": _parse_number(sh.cell(r, 12)),
                "invoice_ref": str(sh.cell(r, 13) or "").strip() or None,
                "responsible_person": str(sh.cell(r, 14) or "").strip() or None,
                "exchange_rate": exchange_rate,
                "currency": currency,
                "items": [],
            }

        elif current is not None:
            # ── Line-item row ────────────────────────────────────────
            product_name = str(sh.cell(r, 2) or "").strip()
            if not product_name:
                continue  # skip blank rows

            line_no = _parse_number(sh.cell(r, 1))
            quantity = _parse_number(sh.cell(r, 3))
            price = _parse_number(sh.cell(r, 4))
            sum_local = _parse_number(sh.cell(r, 5))
            vat = _parse_number(sh.cell(r, 6))
            total_local = _parse_number(sh.cell(r, 7))
            base_price = _parse_number(sh.cell(r, 8))
            markup_pct = _parse_number(sh.cell(r, 9))
            markup_sum = _parse_number(sh.cell(r, 10))
            excise_pct = _parse_number(sh.cell(r, 11))
            excise_sum = _parse_number(sh.cell(r, 12))
            sum_currency = _parse_number(sh.cell(r, 13))
            price_currency = _parse_number(sh.cell(r, 14))
            unit = str(sh.cell(r, 15) or "").strip() or None

            # Self-heal: derive missing totals
            if not sum_local and price and quantity:
                sum_local = price * quantity
            if not total_local:
                total_local = (sum_local or 0) + (vat or 0)
            if not sum_currency and price_currency and quantity:
                sum_currency = price_currency * quantity

            current["items"].append({
                "line_no": int(line_no) if line_no else len(current["items"]) + 1,
                "product_name_raw": product_name,
                "quantity": quantity,
                "price": price,
                "sum_local": sum_local,
                "vat": vat,
                "total_local": total_local,
                "base_price": base_price,
                "markup_pct": markup_pct,
                "markup_sum": markup_sum,
                "excise_pct": excise_pct,
                "excise_sum": excise_sum,
                "sum_currency": sum_currency,
                "price_currency": price_currency,
                "unit": unit,
            })

    # Don't forget the last document
    if current is not None:
        documents.append(current)

    # Rollup doc-level totals
    for d in documents:
        d["total_sum"] = sum(i.get("total_local") or 0 for i in d["items"])
        d["total_sum_currency"] = sum(i.get("sum_currency") or 0 for i in d["items"])
        d["item_count"] = len(d["items"])

    # Stats
    supply_count = sum(1 for d in documents if d["doc_type"] == "supply")
    return_count = sum(1 for d in documents if d["doc_type"] == "return")
    adjustment_count = sum(1 for d in documents if d["doc_type"] == "adjustment")
    total_items = sum(len(d["items"]) for d in documents)
    counterparties = set(d["counterparty_name"] for d in documents)
    warehouses: Dict[str, int] = {}
    currency_counts: Dict[str, int] = {}
    for d in documents:
        wh = d.get("warehouse") or "Неизвестно"
        warehouses[wh] = warehouses.get(wh, 0) + 1
        cur = d.get("currency") or "UZS"
        currency_counts[cur] = currency_counts.get(cur, 0) + 1

    return {
        "ok": True,
        "documents": documents,
        "stats": {
            "total_docs": len(documents),
            "supply_count": supply_count,
            "return_count": return_count,
            "adjustment_count": adjustment_count,
            "total_items": total_items,
            "unique_counterparties": len(counterparties),
            "warehouses": warehouses,
            "currency_counts": currency_counts,
        },
    }


def apply_supply_import(
    file_bytes: bytes,
    filename_hint: str = "",
) -> dict:
    """Parse + upsert into supply_orders / supply_order_items.

    Idempotent: UNIQUE(doc_number, doc_date) — existing docs are updated
    (doc row refreshed, items DELETE+INSERT'd).

    Returns summary dict with inserted/updated/matched/unmatched counts.
    """
    parsed = parse_supply_xls(file_bytes, filename_hint)
    if not parsed.get("ok"):
        return parsed

    documents = parsed["documents"]
    stats = parsed["stats"]

    conn = get_db()
    _PRODUCT_CACHE.clear()

    inserted_docs = 0
    updated_docs = 0
    total_items = 0
    matched_products = 0
    unmatched_products: List[str] = []

    try:
        for d in documents:
            doc_number = d["doc_number"]
            doc_date = d["doc_date"]
            if not doc_number or not doc_date:
                continue

            # Check if document already exists
            existing = conn.execute(
                "SELECT id FROM supply_orders WHERE doc_number = ? AND doc_date = ?",
                (doc_number, doc_date),
            ).fetchone()

            if existing:
                supply_order_id = existing[0]
                conn.execute(
                    """UPDATE supply_orders SET
                        doc_time=?, author=?, counterparty_name=?, doc_type=?,
                        contract=?, counterparty_account=?, warehouse=?,
                        vat_rate=?, receipt_type=?,
                        supplier_advance=?, supplier_advance_offset=?,
                        invoice_ref=?, responsible_person=?,
                        exchange_rate=?, currency=?,
                        total_sum=?, total_sum_currency=?, item_count=?,
                        source_file=?, imported_at=datetime('now')
                       WHERE id=?""",
                    (
                        d["doc_time"], d["author"], d["counterparty_name"],
                        d["doc_type"], d["contract"], d["counterparty_account"],
                        d["warehouse"], d["vat_rate"], d["receipt_type"],
                        d["supplier_advance"], d["supplier_advance_offset"],
                        d["invoice_ref"], d["responsible_person"],
                        d["exchange_rate"], d["currency"],
                        d["total_sum"], d["total_sum_currency"], d["item_count"],
                        filename_hint, supply_order_id,
                    ),
                )
                conn.execute(
                    "DELETE FROM supply_order_items WHERE supply_order_id = ?",
                    (supply_order_id,),
                )
                updated_docs += 1
            else:
                cur = conn.execute(
                    """INSERT INTO supply_orders
                       (doc_number, doc_date, doc_time, author, counterparty_name,
                        doc_type, contract, counterparty_account, warehouse,
                        vat_rate, receipt_type, supplier_advance,
                        supplier_advance_offset, invoice_ref, responsible_person,
                        exchange_rate, currency, total_sum, total_sum_currency,
                        item_count, source_file)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        doc_number, doc_date, d["doc_time"], d["author"],
                        d["counterparty_name"], d["doc_type"],
                        d["contract"], d["counterparty_account"], d["warehouse"],
                        d["vat_rate"], d["receipt_type"],
                        d["supplier_advance"], d["supplier_advance_offset"],
                        d["invoice_ref"], d["responsible_person"],
                        d["exchange_rate"], d["currency"],
                        d["total_sum"], d["total_sum_currency"], d["item_count"],
                        filename_hint,
                    ),
                )
                supply_order_id = cur.lastrowid
                inserted_docs += 1

            # Insert items
            for it in d["items"]:
                product_id = _try_match_product(it["product_name_raw"], conn)
                if product_id is not None:
                    matched_products += 1
                else:
                    unmatched_products.append(it["product_name_raw"])

                conn.execute(
                    """INSERT OR REPLACE INTO supply_order_items
                       (supply_order_id, line_no, product_name_raw,
                        matched_product_id, quantity, price, sum_local,
                        vat, total_local, base_price, markup_pct, markup_sum,
                        excise_pct, excise_sum, sum_currency, price_currency, unit)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        supply_order_id, it["line_no"], it["product_name_raw"],
                        product_id, it["quantity"], it["price"], it["sum_local"],
                        it["vat"], it["total_local"], it["base_price"],
                        it["markup_pct"], it["markup_sum"],
                        it["excise_pct"], it["excise_sum"],
                        it["sum_currency"], it["price_currency"], it["unit"],
                    ),
                )
                total_items += 1

        conn.commit()

        # Unique unmatched
        unique_unmatched = sorted(set(unmatched_products))

        return {
            "ok": True,
            "inserted_docs": inserted_docs,
            "updated_docs": updated_docs,
            "total_items": total_items,
            "matched_products": matched_products,
            "unmatched_products_count": len(unique_unmatched),
            "unmatched_products": unique_unmatched[:50],  # Cap at 50 for display
            "stats": stats,
        }
    except Exception as e:
        conn.rollback()
        logger.error(f"apply_supply_import error: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}
    finally:
        _PRODUCT_CACHE.clear()
        conn.close()
