"""Parser + importer for 1C "Реализация товаров" xls exports.

These are the "Фактические заказы" monthly files. Each file has hundreds of
shipment documents; each document has a header row ('V' in col 0) followed
by multiple item rows. We sum the item totals to get per-document UZS + USD
amounts, then upsert into `derived_shipments`.

Pair with `client_payments` to compute clean running balances that don't
inherit the pre-2020 historical noise present in `client_balances`.
"""
from __future__ import annotations
import io
import re
import unicodedata
from typing import List, Optional

import xlrd

from backend.database import get_db
from backend.services.pseudo_clients import is_pseudo_client


def _normalize(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFC", str(s)).strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    return s


def _parse_ddmmyy(s: str) -> Optional[str]:
    """'19.02.26' -> '2026-02-19'. Returns None on parse error."""
    try:
        d, m, y = s.split(".")
        yr = int(y)
        yr = 2000 + yr if yr < 100 else yr
        return f"{yr:04d}-{int(m):02d}-{int(d):02d}"
    except Exception:
        return None


def _match_client_id(conn, client_name_1c: str):
    """Best-effort match to allowed_clients by normalized client_id_1c."""
    if not client_name_1c:
        return None
    norm = _normalize(client_name_1c)
    # Fast path: exact match
    row = conn.execute(
        "SELECT id FROM allowed_clients WHERE LOWER(client_id_1c) = LOWER(?) LIMIT 1",
        (client_name_1c,),
    ).fetchone()
    if row:
        return row["id"]
    # Cyrillic-aware via client_scores (already has matched names)
    row = conn.execute(
        "SELECT client_id FROM client_scores WHERE client_name = ? LIMIT 1",
        (client_name_1c,),
    ).fetchone()
    if row:
        return row["client_id"]
    # Last resort: try substring on allowed_clients 1c names
    cands = conn.execute(
        "SELECT id, client_id_1c FROM allowed_clients "
        "WHERE LENGTH(client_id_1c) > 0 LIMIT 5000"
    ).fetchall()
    for c in cands:
        if _normalize(c["client_id_1c"]) == norm:
            return c["id"]
    return None


def parse_shipments_xls(file_bytes: bytes) -> dict:
    """Parse one monthly 'Реализация товаров' xls.

    Returns {"period_hint": str|None, "docs": [{doc_number, doc_date, client_name,
             uzs, usd, items, currency}...]}.
    """
    wb = xlrd.open_workbook(
        file_contents=file_bytes,
        ignore_workbook_corruption=True,
        encoding_override="cp1251",
    )
    sh = wb.sheet_by_index(0)
    docs: list = []
    cur: dict | None = None
    for r in range(2, sh.nrows):
        col0 = sh.cell_value(r, 0)
        if col0 == "V":
            if cur is not None:
                docs.append(cur)
            raw_date = sh.cell_value(r, 2)
            client_name = sh.cell_value(r, 5)
            doc_number = sh.cell_value(r, 1)
            currency = sh.cell_value(r, 25) if sh.ncols > 25 else ""
            if isinstance(doc_number, float):
                doc_number = str(int(doc_number))
            else:
                doc_number = str(doc_number or "").strip()
            cur = {
                "doc_number": doc_number,
                "doc_date": _parse_ddmmyy(str(raw_date)),
                "client_name": (client_name or "").strip() if isinstance(client_name, str) else "",
                "currency": str(currency or "").strip(),
                "uzs": 0.0,
                "usd": 0.0,
                "items": 0,
            }
        elif cur is not None:
            line_num = sh.cell_value(r, 1)
            if line_num in ("", None):
                continue
            try:
                _ = float(line_num)
            except Exception:
                continue
            try:
                uzs = float(sh.cell_value(r, 5) or 0)
            except Exception:
                uzs = 0.0
            try:
                usd = float(sh.cell_value(r, 14) or 0) if sh.ncols > 14 else 0.0
            except Exception:
                usd = 0.0
            cur["uzs"] += uzs
            cur["usd"] += usd
            cur["items"] += 1
    if cur is not None:
        docs.append(cur)
    # Filter out docs without a valid date or client name
    docs = [d for d in docs if d["doc_date"] and d["client_name"]]
    return {"docs": docs, "total_in_file": len(docs)}


def apply_shipments_import(file_bytes: bytes) -> dict:
    """Parse + upsert one monthly file into derived_shipments.

    Returns summary: {"ok": bool, "inserted": N, "updated": N, "matched": N,
                       "skipped_zero": N, "unmatched_count": N,
                       "unmatched_sample": [names]}
    """
    result = parse_shipments_xls(file_bytes)
    docs = result["docs"]
    if not docs:
        return {"ok": False, "error": "no docs found"}

    conn = get_db()
    inserted = updated = 0
    matched = skipped_zero = 0
    unmatched_names: set = set()
    pseudo_skipped = 0

    for d in docs:
        # Skip rows where both amounts are zero AND items are zero-valued
        # (sometimes 1C emits return-stub rows with all zeros)
        if d["uzs"] == 0 and d["usd"] == 0:
            skipped_zero += 1
            continue
        # Skip pseudo-accounts (Наличка, etc.)
        if is_pseudo_client(d["client_name"]):
            pseudo_skipped += 1
            continue
        client_id = _match_client_id(conn, d["client_name"])
        if client_id:
            matched += 1
        else:
            unmatched_names.add(d["client_name"])

        # Upsert by (doc_number, doc_date)
        existing = conn.execute(
            "SELECT id FROM derived_shipments WHERE doc_number = ? AND doc_date = ? LIMIT 1",
            (d["doc_number"], d["doc_date"]),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE derived_shipments SET client_name_1c = ?, client_id = ?, "
                "uzs_amount = ?, usd_amount = ?, item_count = ?, currency_marker = ?, "
                "imported_at = datetime('now') WHERE id = ?",
                (d["client_name"], client_id, d["uzs"], d["usd"], d["items"],
                 d["currency"], existing["id"]),
            )
            updated += 1
        else:
            conn.execute(
                "INSERT INTO derived_shipments "
                "(doc_number, doc_date, client_name_1c, client_id, uzs_amount, "
                " usd_amount, item_count, currency_marker) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (d["doc_number"], d["doc_date"], d["client_name"], client_id,
                 d["uzs"], d["usd"], d["items"], d["currency"]),
            )
            inserted += 1
    conn.commit()
    conn.close()
    return {
        "ok": True,
        "total_in_file": result["total_in_file"],
        "inserted": inserted,
        "updated": updated,
        "skipped_zero": skipped_zero,
        "pseudo_skipped": pseudo_skipped,
        "matched_to_app": matched,
        "unmatched_count": len(unmatched_names),
        "unmatched_sample": sorted(unmatched_names)[:20],
    }


def compute_derived_balance(conn, client_id: int, as_of: Optional[str] = None) -> dict:
    """Compute running balance for a client from derived_shipments + client_payments.

    Anchored at 2025-01-01 = 0. No pre-2020 contamination.
    Returns {"uzs": float, "usd": float, "as_of": str}.
    """
    import datetime as _dt
    if as_of is None:
        as_of = _dt.date.today().isoformat()

    ship = conn.execute(
        "SELECT COALESCE(SUM(uzs_amount), 0) AS u, COALESCE(SUM(usd_amount), 0) AS d "
        "FROM derived_shipments WHERE client_id = ? AND doc_date <= ?",
        (client_id, as_of),
    ).fetchone()
    pay_uzs = conn.execute(
        "SELECT COALESCE(SUM(amount_local), 0) AS u FROM client_payments "
        "WHERE client_id = ? AND currency = 'UZS' AND doc_date <= ?",
        (client_id, as_of),
    ).fetchone()
    pay_usd = conn.execute(
        "SELECT COALESCE(SUM(amount_currency), 0) AS d FROM client_payments "
        "WHERE client_id = ? AND currency = 'USD' AND doc_date <= ?",
        (client_id, as_of),
    ).fetchone()
    return {
        "uzs": float(ship["u"] or 0) - float(pay_uzs["u"] or 0),
        "usd": float(ship["d"] or 0) - float(pay_usd["d"] or 0),
        "as_of": as_of,
    }
