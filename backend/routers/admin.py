"""Admin dashboard API — internal analytics for Rassvet's decision-makers.

Phase 2: Supplier auto-detection, clean revenue, client segmentation,
         interactive stock lists, product requests, YoY comparison.

All financial endpoints exclude auto-detected suppliers/accounting entries
unless ?include_suppliers=true is passed.
"""
import logging
from fastapi import APIRouter, Body, Query, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from backend.database import get_db

logger = logging.getLogger(__name__)
from backend.admin_auth import check_admin_key

router = APIRouter(prefix="/api/admin", tags=["admin"])


def _check_admin(admin_key: str):
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ── Pseudo-account exclusion ─────────────────────────────────────
#
# Real-client filtering uses `pseudo_clients.SYSTEM_NON_CLIENT_NAMES` —
# a curated list maintained from human-validated reviews (Группа2/3
# проверка, finances_client_merge_map.md). This replaces the legacy
# `_ENTITY_FILTER_CTE` heuristic that bucketed by <5% collection rate;
# the heuristic missed structural accounts that cycle credits (Наличка,
# СТРОЙКА) and wrongly silenced real clients with bad payment behavior.
# See `obsidian-vault/audits/2026-05-06_admin_filter_sweep.md`.

# Filter out cumulative records (pre-2025) and end-of-month partials (day != 01)
_PERIOD_FILTER = "cb.period_start >= '2025-01-01' AND strftime('%d', cb.period_start) = '01'"



@router.get("/debug-query")
def debug_query(
    q: str = Query(...),
    admin_key: str = Query(...),
):
    """Run a read-only SQL query for debugging. SELECT only."""
    _check_admin(admin_key)
    q_stripped = q.strip().upper()
    if not q_stripped.startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT queries allowed")
    conn = get_db()
    try:
        rows = conn.execute(q).fetchall()
        result = [dict(r) for r in rows]
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))
    conn.close()
    return {"ok": True, "rows": result, "count": len(result)}


@router.post("/set-test-client")
def set_test_client(
    telegram_id: int = Query(...),
    client_id: int = Query(...),
    admin_key: str = Query(...),
):
    """Set a user's client_id for testing. Same as /testclient but via API."""
    _check_admin(admin_key)
    conn = get_db()
    conn.execute(
        "UPDATE users SET client_id = ? WHERE telegram_id = ?",
        (client_id, telegram_id),
    )
    conn.commit()
    row = conn.execute(
        "SELECT u.client_id, ac.name, ac.client_id_1c FROM users u "
        "LEFT JOIN allowed_clients ac ON u.client_id = ac.id "
        "WHERE u.telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.close()
    return {"ok": True, "client_id": row["client_id"], "name": row["name"], "client_id_1c": row["client_id_1c"]}


@router.post("/cleanup-zero-balances")
def cleanup_zero_balances(admin_key: str = Query(...)):
    """Delete all-zero balance records from client_balances.

    These are records where all 6 financial columns are 0 — they carry
    no information and can mask real balances when they become the
    'latest period' for a client.
    """
    _check_admin(admin_key)
    conn = get_db()
    count = conn.execute(
        """SELECT COUNT(*) FROM client_balances
           WHERE opening_debit = 0 AND opening_credit = 0
             AND period_debit = 0 AND period_credit = 0
             AND closing_debit = 0 AND closing_credit = 0"""
    ).fetchone()[0]

    conn.execute(
        """DELETE FROM client_balances
           WHERE opening_debit = 0 AND opening_credit = 0
             AND period_debit = 0 AND period_credit = 0
             AND closing_debit = 0 AND closing_credit = 0"""
    )
    conn.commit()
    conn.close()
    return {"ok": True, "deleted": count}


# ── Backfill order_items.product_name to Cyrillic ────────────────

@router.post("/backfill-order-item-names")
def backfill_order_item_names(admin_key: str = Query(...)):
    """Session A policy: rewrite `order_items.product_name` for old wish-list
    orders so that the stored line-item name matches the Cyrillic 1C name
    (products.name), not the cleaned Latin display name.

    From commit 325b4cc onward, newly placed orders already save the Cyrillic
    name. This endpoint heals orders placed before that commit: for every
    `order_items` row with a linked `product_id`, we overwrite `product_name`
    with `products.name`. Rows with NULL product_id (free-text items, if any)
    are left untouched.

    Idempotent — running it twice is a no-op because the second pass would
    match `products.name` exactly.
    """
    _check_admin(admin_key)
    conn = get_db()
    # Count rows that would actually change so the response is informative.
    to_update = conn.execute(
        """SELECT COUNT(*)
           FROM order_items oi
           JOIN products p ON p.id = oi.product_id
           WHERE oi.product_id IS NOT NULL
             AND (oi.product_name IS NULL OR oi.product_name <> p.name)"""
    ).fetchone()[0]

    conn.execute(
        """UPDATE order_items
           SET product_name = (
               SELECT p.name FROM products p WHERE p.id = order_items.product_id
           )
           WHERE product_id IS NOT NULL
             AND product_name <> (
               SELECT p.name FROM products p WHERE p.id = order_items.product_id
             )"""
    )
    conn.commit()
    conn.close()
    return {"ok": True, "rows_updated": to_update}


# ── Fix weights from product names ───────────────────────────────

@router.post("/fix-weights")
def fix_weights_from_names(admin_key: str = Query(...)):
    """One-time fix: parse weight from product name (original cyrillic)
    when the DB weight is NULL or a round integer that contradicts a
    decimal weight found in the name.

    E.g. name="Грунтовка акриловая 0.75 кг", weight=1 → weight=0.75
    """
    _check_admin(admin_key)
    from backend.services.parse_weight import parse_weight_from_name

    conn = get_db()
    rows = conn.execute("SELECT id, name, weight FROM products").fetchall()

    updated = []
    for row in rows:
        pid, name, db_weight = row["id"], row["name"], row["weight"]
        parsed = parse_weight_from_name(name or "")
        if parsed is None:
            continue

        # Update if: no weight, or DB weight differs from what the name says
        should_update = False
        if db_weight is None or db_weight == 0:
            should_update = True
        elif round(db_weight, 4) != round(parsed, 4):
            # DB weight doesn't match name — could be wrong Excel data
            # or a bad parse from a previous run
            should_update = True

        if should_update:
            conn.execute("UPDATE products SET weight = ? WHERE id = ?", (parsed, pid))
            updated.append({"id": pid, "name": name, "old": db_weight, "new": parsed})

    conn.commit()
    conn.close()
    return {"ok": True, "updated_count": len(updated), "updated": updated[:50]}


# ── Image rotation fix ───────────────────────────────────────────

@router.post("/fix-image-rotation")
def fix_image_rotation(admin_key: str = Query(...)):
    """Re-process all product images: apply EXIF orientation transpose.

    This fixes photos taken on phones that appear rotated because the
    original process_image didn't apply EXIF orientation metadata.
    Images are re-saved in place with correct orientation.
    """
    _check_admin(admin_key)
    import os
    from PIL import Image, ImageOps

    images_dir = os.getenv("IMAGES_DIR", "./images")
    QUALITY = 85

    fixed = []
    skipped = 0
    errors = []

    for fname in os.listdir(images_dir):
        if not fname.lower().endswith(('.jpg', '.jpeg', '.png')):
            continue
        fpath = os.path.join(images_dir, fname)
        try:
            img = Image.open(fpath)
            exif = img.getexif()
            orientation = exif.get(0x0112)  # EXIF Orientation tag
            if orientation and orientation != 1:
                # Has non-default orientation — needs fixing
                img = ImageOps.exif_transpose(img)
                if img.mode in ('RGBA', 'P'):
                    img = img.convert('RGB')
                img.save(fpath, 'JPEG', quality=QUALITY, optimize=True)
                fixed.append({"file": fname, "orientation": orientation})
            else:
                skipped += 1
            img.close()
        except Exception as e:
            errors.append({"file": fname, "error": str(e)})

    return {
        "ok": True,
        "fixed_count": len(fixed),
        "skipped": skipped,
        "errors": errors[:20],
        "fixed": fixed[:50],
    }


@router.post("/rotate-image")
def rotate_image_endpoint(
    product_id: int = Query(...),
    degrees: int = Query(default=270, description="Rotation degrees counter-clockwise. 270 = 90° clockwise fix"),
    admin_key: str = Query(...),
):
    """Manually rotate a product image by given degrees counter-clockwise.

    Common use: degrees=270 to fix a photo rotated 90° clockwise.
    """
    _check_admin(admin_key)
    import os
    from backend.services.image_manager import rotate_image

    images_dir = os.getenv("IMAGES_DIR", "./images")
    fpath = os.path.join(images_dir, f"{product_id}.jpg")

    if not os.path.exists(fpath):
        raise HTTPException(status_code=404, detail=f"No image for product {product_id}")

    rotate_image(fpath, degrees)
    return {"ok": True, "product_id": product_id, "rotated_degrees": degrees}


@router.post("/rotate-images-batch")
def rotate_images_batch(
    product_ids: str = Query(..., description="Comma-separated product IDs"),
    degrees: int = Query(default=270),
    admin_key: str = Query(...),
):
    """Rotate multiple product images at once."""
    _check_admin(admin_key)
    import os
    from backend.services.image_manager import rotate_image

    images_dir = os.getenv("IMAGES_DIR", "./images")
    ids = [int(x.strip()) for x in product_ids.split(",") if x.strip().isdigit()]

    results = []
    for pid in ids:
        fpath = os.path.join(images_dir, f"{pid}.jpg")
        if os.path.exists(fpath):
            rotate_image(fpath, degrees)
            results.append({"id": pid, "status": "rotated"})
        else:
            results.append({"id": pid, "status": "no_image"})

    return {"ok": True, "results": results}


# ── Revenue Trend ────────────────────────────────────────────────


@router.get("/entities")
def entity_classification(admin_key: str = Query(...)):
    """List entities split by `pseudo_clients.SYSTEM_NON_CLIENT_NAMES`.

    Real clients vs pseudo-accounts (cash registers, structural ledger
    accounts, supplier-bonus accumulators, return markers, defunct cards).
    Used for the review screen where admin can spot newly-introduced
    pseudo-account names that haven't been added to the curated list yet.
    """
    from backend.services.pseudo_clients import is_pseudo_client

    _check_admin(admin_key)
    conn = get_db()

    rows = conn.execute("""
        SELECT cb.client_name_1c as name,
               SUM(cb.period_debit) as total_debit,
               SUM(cb.period_credit) as total_credit,
               ROUND(SUM(cb.period_credit) * 100.0 / NULLIF(SUM(cb.period_debit), 0), 1) as pay_pct,
               COUNT(DISTINCT cb.period_start) as months_active,
               COUNT(DISTINCT cb.currency) as currencies
          FROM client_balances cb
         GROUP BY cb.client_name_1c
         ORDER BY total_debit DESC
    """).fetchall()
    conn.close()

    pseudo, real = [], []
    for r in rows:
        rec = dict(r)
        rec["is_pseudo"] = is_pseudo_client(r["name"])
        (pseudo if rec["is_pseudo"] else real).append(rec)

    return {
        "ok": True,
        "total_entities": len(rows),
        "clients_count": len(real),
        "pseudo_count": len(pseudo),
        "pseudo_accounts": pseudo,
        "top_clients": real[:30],
    }


# ── Revenue Trend (clean) ───────────────────────────────────────


@router.get("/revenue")
def revenue_trend(
    admin_key: str = Query(...),
    include_suppliers: bool = Query(False),
):
    """Monthly revenue trend: SUM(period_debit) by month, by currency.
    Excludes suppliers by default.
    """
    from backend.services.pseudo_clients import (
        sql_exclusion_clause, sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()

    excl_clause = "" if include_suppliers else f" AND {sql_exclusion_clause('cb.client_name_1c')}"
    excl_params = () if include_suppliers else sql_exclusion_params()

    rows = conn.execute(f"""
        SELECT cb.period_start, cb.currency,
               SUM(cb.period_debit) as total_shipments,
               SUM(cb.period_credit) as total_collections,
               COUNT(DISTINCT cb.client_name_1c) as active_clients
          FROM client_balances cb
         WHERE (cb.period_debit > 0 OR cb.period_credit > 0)
               AND {_PERIOD_FILTER}
               {excl_clause}
         GROUP BY cb.period_start, cb.currency
         ORDER BY cb.period_start ASC
    """, excl_params).fetchall()

    conn.close()

    # Detect current calendar month to flag as partial
    from datetime import date
    today = date.today()
    current_month_start = today.replace(day=1).isoformat()

    result = {"UZS": [], "USD": []}
    for r in rows:
        cur = r["currency"]
        if cur not in result:
            result[cur] = []
        is_partial = r["period_start"] == current_month_start
        result[cur].append({
            "period": r["period_start"],
            "shipments": round(r["total_shipments"], 2),
            "collections": round(r["total_collections"], 2),
            "active_clients": r["active_clients"],
            "partial": is_partial,
        })

    # Add YoY comparison for each period
    for cur in result:
        periods = result[cur]
        for p in periods:
            try:
                year = int(p["period"][:4])
                month_day = p["period"][4:]
                prev_period = f"{year - 1}{month_day}"
                prev = next((x for x in periods if x["period"] == prev_period), None)
                if prev and prev["shipments"] > 0:
                    p["yoy_growth"] = round((p["shipments"] - prev["shipments"]) / prev["shipments"] * 100, 1)
                    p["yoy_prev_shipments"] = prev["shipments"]
                else:
                    p["yoy_growth"] = None
                    p["yoy_prev_shipments"] = None
            except Exception:
                p["yoy_growth"] = None
                p["yoy_prev_shipments"] = None

    # Compute last_full_month (the most recent non-partial month)
    last_full = {}
    for cur in result:
        full_months = [p for p in result[cur] if not p.get("partial")]
        last_full[cur] = full_months[-1]["period"] if full_months else None

    return {"ok": True, "data": result, "last_full_month": last_full}


# ── Collection Rate (clean) ──────────────────────────────────────


@router.get("/collections")
def collection_rate(
    admin_key: str = Query(...),
    include_suppliers: bool = Query(False),
):
    """Collection rate by month — excludes suppliers by default."""
    from backend.services.pseudo_clients import (
        sql_exclusion_clause, sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()

    excl_clause = "" if include_suppliers else f" AND {sql_exclusion_clause('cb.client_name_1c')}"
    excl_params = () if include_suppliers else sql_exclusion_params()

    rows = conn.execute(f"""
        SELECT cb.period_start, cb.currency,
               SUM(cb.period_debit) as total_debit,
               SUM(cb.period_credit) as total_credit
          FROM client_balances cb
         WHERE {_PERIOD_FILTER} {excl_clause}
         GROUP BY cb.period_start, cb.currency
         ORDER BY cb.period_start ASC
    """, excl_params).fetchall()

    conn.close()

    result = {"UZS": [], "USD": []}
    for r in rows:
        cur = r["currency"]
        if cur not in result:
            result[cur] = []
        debit = r["total_debit"] or 0
        credit = r["total_credit"] or 0
        rate = round(credit / debit * 100, 1) if debit > 0 else 0
        result[cur].append({
            "period": r["period_start"],
            "total_shipped": round(debit, 2),
            "total_collected": round(credit, 2),
            "collection_rate": rate,
        })

    return {"ok": True, "data": result}


# ── Top Clients (clean) ─────────────────────────────────────────


@router.get("/top-clients")
def top_clients(
    admin_key: str = Query(...),
    currency: str = Query("UZS"),
    limit: int = Query(50, ge=1, le=200),
    include_suppliers: bool = Query(False),
):
    """Top clients ranked by total shipments — excludes pseudo-accounts by default."""
    from backend.services.pseudo_clients import (
        sql_exclusion_clause, sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()

    excl_clause = "" if include_suppliers else f" AND {sql_exclusion_clause('cb.client_name_1c')}"
    excl_params = () if include_suppliers else sql_exclusion_params()

    rows = conn.execute(f"""
        SELECT cb.client_name_1c,
               SUM(cb.period_debit) as total_shipped,
               SUM(cb.period_credit) as total_paid,
               COUNT(DISTINCT cb.period_start) as months_active
          FROM client_balances cb
         WHERE cb.currency = ? {excl_clause}
         GROUP BY cb.client_name_1c
         ORDER BY total_shipped DESC
         LIMIT ?
    """, (currency, *excl_params, limit)).fetchall()

    clients = []
    for r in rows:
        latest = conn.execute("""
            SELECT closing_debit, closing_credit, period_start
            FROM client_balances
            WHERE client_name_1c = ? AND currency = ?
            ORDER BY period_start DESC LIMIT 1
        """, (r["client_name_1c"], currency)).fetchone()

        balance = 0
        latest_period = ""
        if latest:
            balance = (latest["closing_debit"] or 0) - (latest["closing_credit"] or 0)
            latest_period = latest["period_start"]

        # Determine segment based on last 3 months vs previous 3 months
        recent = conn.execute("""
            SELECT SUM(period_debit) as recent_ship
            FROM client_balances
            WHERE client_name_1c = ? AND currency = ?
            ORDER BY period_start DESC LIMIT 3
        """, (r["client_name_1c"], currency)).fetchone()

        older = conn.execute("""
            SELECT SUM(period_debit) as older_ship
            FROM client_balances
            WHERE client_name_1c = ? AND currency = ?
              AND period_start < (
                  SELECT period_start FROM client_balances
                  WHERE client_name_1c = ? AND currency = ?
                  ORDER BY period_start DESC LIMIT 1 OFFSET 2
              )
            ORDER BY period_start DESC LIMIT 3
        """, (r["client_name_1c"], currency, r["client_name_1c"], currency)).fetchone()

        recent_val = (recent["recent_ship"] or 0) if recent else 0
        older_val = (older["older_ship"] or 0) if older else 0

        if r["months_active"] <= 2:
            segment = "new"
        elif recent_val == 0:
            segment = "dormant"
        elif older_val > 0 and recent_val > older_val * 1.2:
            segment = "growing"
        elif older_val > 0 and recent_val < older_val * 0.5:
            segment = "declining"
        else:
            segment = "stable"

        pay_pct = round(r["total_paid"] * 100 / r["total_shipped"], 1) if r["total_shipped"] else 0

        clients.append({
            "name": r["client_name_1c"],
            "total_shipped": round(r["total_shipped"], 2),
            "total_paid": round(r["total_paid"], 2),
            "balance": round(balance, 2),
            "months_active": r["months_active"],
            "latest_period": latest_period,
            "segment": segment,
            "pay_pct": pay_pct,
        })

    conn.close()
    return {"ok": True, "currency": currency, "clients": clients}


# ── Receivables (clean) ─────────────────────────────────────────


_AGING_BUCKETS_UZS = ("0_30", "31_60", "61_90", "91_120", "120_plus")


@router.get("/receivables")
def receivables(
    admin_key: str = Query(...),
    currency: str = Query("UZS"),
):
    """Receivables + aging from 1C `client_debts` (debtor report).

    Source: latest `/debtors` snapshot in `client_debts`. Real day-aged
    buckets are taken straight from 1C for UZS. USD has no aging in the
    1C report — only a total.

    Pseudo-account exclusion (cash registers, structural ledger accounts,
    return markers, etc.) is applied via `pseudo_clients`.
    """
    from backend.services.pseudo_clients import (
        sql_exclusion_clause,
        sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()

    report_date = conn.execute(
        "SELECT MAX(report_date) FROM client_debts"
    ).fetchone()[0]

    if not report_date:
        conn.close()
        return {
            "ok": True,
            "currency": currency,
            "as_of": None,
            "total_receivable": 0,
            "total_clients_with_debt": 0,
            "aging": {},
            "aging_client_count": {},
            "aging_top_clients": {},
            "usd_total": 0,
            "usd_client_count": 0,
            "usd_aging_available": False,
            "methodology": "No /debtors data imported yet.",
        }

    excl_clause = sql_exclusion_clause("client_name_1c")
    excl_params = sql_exclusion_params()

    if currency == "USD":
        # USD has no aging in /debtors — return totals + top clients only.
        rows = conn.execute(
            f"""SELECT client_name_1c, debt_usd
                  FROM client_debts
                 WHERE report_date = ? AND debt_usd > 0 AND {excl_clause}
                 ORDER BY debt_usd DESC""",
            (report_date, *excl_params),
        ).fetchall()
        total = sum(r["debt_usd"] for r in rows)
        top = [
            {"name": r["client_name_1c"], "balance": round(r["debt_usd"], 2)}
            for r in rows[:10]
        ]
        conn.close()
        return {
            "ok": True,
            "currency": "USD",
            "as_of": report_date,
            "total_receivable": round(total, 2),
            "total_clients_with_debt": len(rows),
            "aging": {},
            "aging_client_count": {},
            "aging_top_clients": {"all": top},
            "usd_total": round(total, 2),
            "usd_client_count": len(rows),
            "usd_aging_available": False,
            "methodology": (
                "USD totals from latest 1C debtor report. 1C does not provide "
                "per-bucket aging for USD — only total outstanding per client."
            ),
        }

    # UZS path — real aging buckets from 1C
    rows = conn.execute(
        f"""SELECT client_name_1c, debt_uzs, debt_usd, last_transaction_date,
                   aging_0_30, aging_31_60, aging_61_90, aging_91_120, aging_120_plus
              FROM client_debts
             WHERE report_date = ? AND debt_uzs > 0 AND {excl_clause}
             ORDER BY debt_uzs DESC""",
        (report_date, *excl_params),
    ).fetchall()

    aging = {b: 0.0 for b in _AGING_BUCKETS_UZS}
    client_count = {b: 0 for b in _AGING_BUCKETS_UZS}
    bucket_clients: dict[str, list[dict]] = {b: [] for b in _AGING_BUCKETS_UZS}
    bucket_col = {
        "0_30": "aging_0_30",
        "31_60": "aging_31_60",
        "61_90": "aging_61_90",
        "91_120": "aging_91_120",
        "120_plus": "aging_120_plus",
    }
    total_receivable = 0.0
    for r in rows:
        total_receivable += r["debt_uzs"]
        for b, col in bucket_col.items():
            amt = r[col] or 0
            if amt > 0:
                aging[b] += amt
                client_count[b] += 1
                bucket_clients[b].append({
                    "name": r["client_name_1c"],
                    "balance": round(amt, 2),
                    "total_debt": round(r["debt_uzs"], 2),
                    "last_tx": r["last_transaction_date"],
                })

    # USD side-panel summary (computed even on UZS calls so frontend can show it)
    usd_rows = conn.execute(
        f"""SELECT client_name_1c, debt_usd
              FROM client_debts
             WHERE report_date = ? AND debt_usd > 0 AND {excl_clause}
             ORDER BY debt_usd DESC""",
        (report_date, *excl_params),
    ).fetchall()
    usd_total = sum(r["debt_usd"] for r in usd_rows)

    conn.close()

    # Top 10 per bucket (by amount in that bucket)
    for b in bucket_clients:
        bucket_clients[b].sort(key=lambda x: x["balance"], reverse=True)
        bucket_clients[b] = bucket_clients[b][:10]

    return {
        "ok": True,
        "currency": "UZS",
        "as_of": report_date,
        "total_receivable": round(total_receivable, 2),
        "total_clients_with_debt": len(rows),
        "aging": {k: round(v, 2) for k, v in aging.items()},
        "aging_client_count": client_count,
        "aging_top_clients": bucket_clients,
        "usd_total": round(usd_total, 2),
        "usd_client_count": len(usd_rows),
        "usd_aging_available": False,
        "methodology": (
            "Aging from 1C 'Дебиторская задолженность' report (per-day FIFO "
            "bucketed by 1C). UZS only — 1C does not provide USD aging. "
            "Pseudo-accounts (cash registers, structural ledger entries, "
            "return markers) excluded via pseudo_clients.SYSTEM_NON_CLIENT_NAMES."
        ),
    }


@router.get("/debtors-list")
def debtors_list(admin_key: str = Query(...)):
    """Per-client debtors list — mirrors the manager's printed report.

    Latest `client_debts` snapshot, real clients only (pseudo-accounts
    excluded via `pseudo_clients.SYSTEM_NON_CLIENT_NAMES`). Sorted by
    combined USD-equivalent debt DESC. Includes `last_transaction_date`
    and computed `days_since_last_tx` so the manager can spot stuck debt.

    Footer totals (count, sum_uzs, sum_usd) are returned alongside the
    rows for paper-list reconciliation.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from backend.services.pseudo_clients import (
        sql_exclusion_clause, sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()

    report_date = conn.execute(
        "SELECT MAX(report_date) FROM client_debts"
    ).fetchone()[0]

    if not report_date:
        conn.close()
        return {
            "ok": True,
            "as_of": None,
            "fxrate_used": 0,
            "count": 0,
            "total_uzs": 0,
            "total_usd": 0,
            "items": [],
        }

    fxrate = _latest_fxrate(conn)
    excl_clause = sql_exclusion_clause("client_name_1c")

    rows = conn.execute(
        f"""SELECT client_name_1c, client_id, debt_uzs, debt_usd,
                   last_transaction_date, last_transaction_no,
                   aging_0_30, aging_31_60, aging_61_90,
                   aging_91_120, aging_120_plus
              FROM client_debts
             WHERE report_date = ?
               AND (debt_uzs > 0 OR debt_usd > 0)
               AND {excl_clause}""",
        (report_date, *sql_exclusion_params()),
    ).fetchall()
    conn.close()

    today_tk = datetime.now(ZoneInfo("Asia/Tashkent")).date()

    def _days_since(date_str):
        if not date_str:
            return None
        try:
            return (today_tk - datetime.strptime(date_str, "%Y-%m-%d").date()).days
        except (ValueError, TypeError):
            return None

    items = []
    total_uzs = 0.0
    total_usd = 0.0
    for r in rows:
        debt_uzs = float(r["debt_uzs"] or 0)
        debt_usd = float(r["debt_usd"] or 0)
        usd_eq = debt_usd + (debt_uzs / fxrate if fxrate > 0 else 0)
        total_uzs += debt_uzs
        total_usd += debt_usd
        items.append({
            "client_name": r["client_name_1c"],
            "client_id": r["client_id"],
            "debt_uzs": round(debt_uzs, 2),
            "debt_usd": round(debt_usd, 2),
            "debt_usd_eq": round(usd_eq, 2),
            "last_transaction_date": r["last_transaction_date"],
            "last_transaction_no": r["last_transaction_no"],
            "days_since_last_tx": _days_since(r["last_transaction_date"]),
            "aging_uzs": {
                "0_30": round(r["aging_0_30"] or 0, 2),
                "31_60": round(r["aging_31_60"] or 0, 2),
                "61_90": round(r["aging_61_90"] or 0, 2),
                "91_120": round(r["aging_91_120"] or 0, 2),
                "120_plus": round(r["aging_120_plus"] or 0, 2),
            },
        })

    # Sort by combined USD-eq debt DESC
    items.sort(key=lambda x: x["debt_usd_eq"], reverse=True)
    for idx, it in enumerate(items, start=1):
        it["rank"] = idx

    return {
        "ok": True,
        "as_of": report_date,
        "fxrate_used": fxrate,
        "count": len(items),
        "total_uzs": round(total_uzs, 2),
        "total_usd": round(total_usd, 2),
        "items": items,
    }


@router.get("/receivables-trend")
def receivables_trend(
    admin_key: str = Query(...),
    currency: str = Query("UZS"),
):
    """Month-end total receivables per period — excludes suppliers.

    Sums (closing_debit - closing_credit) across real-client rows for each
    period_start. Negative closings (client overpayments / credits) are netted
    against positive ones to give the true trade-receivable figure.
    """
    from backend.services.pseudo_clients import (
        sql_exclusion_clause, sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()
    excl_clause = sql_exclusion_clause('cb.client_name_1c')
    rows = conn.execute(f"""
        SELECT cb.period_start,
               SUM(cb.closing_debit - cb.closing_credit) AS net_receivable,
               SUM(CASE WHEN (cb.closing_debit - cb.closing_credit) > 0
                        THEN (cb.closing_debit - cb.closing_credit) ELSE 0 END) AS positive_only,
               SUM(cb.period_debit) AS shipments,
               SUM(cb.period_credit) AS collections,
               COUNT(DISTINCT cb.client_name_1c) AS clients_with_row
          FROM client_balances cb
         WHERE cb.currency = ?
           AND cb.period_start >= '2025-01-01'
           AND {excl_clause}
         GROUP BY cb.period_start
         ORDER BY cb.period_start ASC
    """, (currency, *sql_exclusion_params())).fetchall()
    conn.close()

    from datetime import date
    today = date.today().replace(day=1).isoformat()
    return {
        "ok": True,
        "currency": currency,
        "periods": [
            {
                "period": r["period_start"],
                "month": r["period_start"][:7],
                "net_receivable": round(r["net_receivable"] or 0, 2),
                "positive_only": round(r["positive_only"] or 0, 2),
                "shipments": round(r["shipments"] or 0, 2),
                "collections": round(r["collections"] or 0, 2),
                "clients": r["clients_with_row"],
                "partial": r["period_start"] == today,
            }
            for r in rows
        ],
    }


# ── Client History (drill-down) ──────────────────────────────────


@router.get("/client/{client_name}/history")
def client_history(
    client_name: str,
    admin_key: str = Query(...),
):
    """Per-client balance history — 15-month chart data."""
    _check_admin(admin_key)
    conn = get_db()

    rows = conn.execute("""
        SELECT currency, period_start, period_end,
               opening_debit, opening_credit,
               period_debit, period_credit,
               closing_debit, closing_credit
        FROM client_balances
        WHERE client_name_1c = ?
        ORDER BY currency, period_start ASC
    """, (client_name,)).fetchall()

    conn.close()

    if not rows:
        return {"ok": True, "client_name": client_name, "history": {}}

    history = {}
    for r in rows:
        cur = r["currency"]
        if cur not in history:
            history[cur] = []
        history[cur].append({
            "period": r["period_start"],
            "period_end": r["period_end"],
            "period_debit": round(r["period_debit"] or 0, 2),
            "period_credit": round(r["period_credit"] or 0, 2),
            "closing_debit": round(r["closing_debit"] or 0, 2),
            "closing_credit": round(r["closing_credit"] or 0, 2),
            "balance": round((r["closing_debit"] or 0) - (r["closing_credit"] or 0), 2),
        })

    return {"ok": True, "client_name": client_name, "history": history}


# ── Stock Status (enhanced) ──────────────────────────────────────


@router.get("/demand-signals")
def demand_signals(
    admin_key: str = Query(...),
    days: int = Query(30, ge=1, le=365),
    threshold: int = Query(5, ge=1, le=100),
):
    """Top out-of-stock products ordered by clients.

    Returns products that have been ordered while marked out-of-stock,
    ranked by total demand (order count). Items crossing the threshold
    are flagged as noteworthy signals.
    """
    _check_admin(admin_key)
    conn = get_db()

    # Top out-of-stock products by demand
    top_demand = conn.execute("""
        SELECT ds.product_id,
               p.name_display,
               p.name as name_cyrillic,
               pr.name as producer_name,
               c.name as category_name,
               p.stock_status as current_stock_status,
               COUNT(DISTINCT ds.order_id) as order_count,
               SUM(ds.quantity) as total_quantity,
               COUNT(DISTINCT ds.telegram_id) as unique_clients,
               MIN(ds.created_at) as first_signal,
               MAX(ds.created_at) as last_signal
        FROM demand_signals ds
        JOIN products p ON p.id = ds.product_id
        JOIN producers pr ON pr.id = p.producer_id
        JOIN categories c ON c.id = p.category_id
        WHERE ds.created_at >= datetime('now', ?)
        GROUP BY ds.product_id
        ORDER BY order_count DESC
        LIMIT 50
    """, (f"-{days} days",)).fetchall()

    # Summary stats
    total_signals = conn.execute(
        "SELECT COUNT(*) FROM demand_signals WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    unique_products = conn.execute(
        "SELECT COUNT(DISTINCT product_id) FROM demand_signals WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    conn.close()

    items = []
    noteworthy_count = 0
    for r in top_demand:
        noteworthy = r["order_count"] >= threshold
        if noteworthy:
            noteworthy_count += 1
        items.append({
            "product_id": r["product_id"],
            "name": r["name_display"] or r["name_cyrillic"],
            "name_cyrillic": r["name_cyrillic"],
            "producer": r["producer_name"],
            "category": r["category_name"],
            "current_stock": r["current_stock_status"] or "unknown",
            "order_count": r["order_count"],
            "total_quantity": r["total_quantity"],
            "unique_clients": r["unique_clients"],
            "first_signal": r["first_signal"],
            "last_signal": r["last_signal"],
            "noteworthy": noteworthy,
        })

    return {
        "ok": True,
        "days": days,
        "threshold": threshold,
        "total_signals": total_signals,
        "unique_products": unique_products,
        "noteworthy_count": noteworthy_count,
        "items": items,
    }


# ── Stock Status ─────────────────────────────────────────────────



@router.get("/stock-status")
def stock_status(admin_key: str = Query(...)):
    """Enhanced stock overview with full item lists for each category.

    Returns stock_updated_at per item and a `stale_items` list:
    items not present in the most recent stock upload (likely 0 in 1C
    but excluded from "Прайс лист" export, leaving stale values in DB).
    """
    _check_admin(admin_key)
    conn = get_db()

    # Most recent stock upload timestamp — items NOT updated at this exact
    # time are considered stale (missing from latest upload).
    latest_upload = conn.execute(
        "SELECT MAX(stock_updated_at) FROM products WHERE stock_updated_at IS NOT NULL"
    ).fetchone()[0]

    total_products = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1"
    ).fetchone()[0]

    in_stock = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND stock_quantity > 10"
    ).fetchone()[0]

    low_stock = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND stock_quantity > 0 AND stock_quantity <= 10"
    ).fetchone()[0]

    out_of_stock = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND stock_quantity = 0"
    ).fetchone()[0]

    no_data = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND stock_quantity IS NULL"
    ).fetchone()[0]

    with_photos = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND image_path IS NOT NULL AND image_path != ''"
    ).fetchone()[0]

    # Stale items: have stock data but were NOT in the latest upload.
    # Most likely 0 in 1C (excluded from "Прайс лист" export which only
    # lists in-stock items), so DB still shows old positive value.
    stale_count = 0
    if latest_upload:
        # Anything updated >5 minutes before latest upload is from a
        # previous upload session = not in current 1C export
        stale_count = conn.execute(
            """SELECT COUNT(*) FROM products
               WHERE is_active = 1
                 AND stock_quantity > 0
                 AND stock_updated_at IS NOT NULL
                 AND datetime(stock_updated_at) < datetime(?, '-5 minutes')""",
            (latest_upload,)
        ).fetchone()[0]

    # Full list of low stock items (for uncle's review — Cyrillic names)
    low_stock_items = conn.execute("""
        SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
               pr.name as producer, c.name as category,
               p.stock_quantity, p.price_uzs, p.price_usd, p.stock_updated_at
        FROM products p
        JOIN producers pr ON pr.id = p.producer_id
        JOIN categories c ON c.id = p.category_id
        WHERE p.is_active = 1 AND p.stock_quantity > 0 AND p.stock_quantity <= 10
        ORDER BY p.stock_quantity ASC, p.name
    """).fetchall()

    # Full list of out-of-stock items
    out_of_stock_items = conn.execute("""
        SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
               pr.name as producer, c.name as category,
               p.stock_quantity, p.price_uzs, p.price_usd, p.stock_updated_at
        FROM products p
        JOIN producers pr ON pr.id = p.producer_id
        JOIN categories c ON c.id = p.category_id
        WHERE p.is_active = 1 AND p.stock_quantity = 0
        ORDER BY p.name
    """).fetchall()

    # No data items
    no_data_items = conn.execute("""
        SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
               pr.name as producer, c.name as category,
               p.price_uzs, p.price_usd, p.stock_updated_at
        FROM products p
        JOIN producers pr ON pr.id = p.producer_id
        JOIN categories c ON c.id = p.category_id
        WHERE p.is_active = 1 AND p.stock_quantity IS NULL
        ORDER BY pr.name, p.name
    """).fetchall()

    # Stale items: stock > 0 but not updated in the latest upload.
    # These probably have 0 in 1C but were excluded from the export.
    stale_items = []
    if latest_upload:
        stale_items = conn.execute(
            """SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
                      pr.name as producer, c.name as category,
                      p.stock_quantity, p.price_uzs, p.price_usd, p.stock_updated_at
               FROM products p
               JOIN producers pr ON pr.id = p.producer_id
               JOIN categories c ON c.id = p.category_id
               WHERE p.is_active = 1
                 AND p.stock_quantity > 0
                 AND p.stock_updated_at IS NOT NULL
                 AND datetime(p.stock_updated_at) < datetime(?, '-5 minutes')
               ORDER BY p.stock_quantity ASC, p.name""",
            (latest_upload,)
        ).fetchall()

    # Top ordered products (from app orders).
    # Group by product_id (canonical) so display always shows Cyrillic
    # `products.name`, regardless of how `oi.product_name` was stored at
    # order time (frontend could ship "<producer> — <cyrillic>" or anything).
    # NULL product_id means an orphan order_item (no product link); fall
    # back to the stored `product_name` for those.
    top_ordered_app = conn.execute("""
        SELECT COALESCE(p.name, oi.product_name) as product_name,
               COALESCE(pr.name, oi.producer_name) as producer_name,
               SUM(oi.quantity) as total_qty,
               COUNT(DISTINCT oi.order_id) as order_count,
               oi.currency, oi.price
        FROM order_items oi
        JOIN orders o ON o.id = oi.order_id
        LEFT JOIN products p ON p.id = oi.product_id
        LEFT JOIN producers pr ON pr.id = p.producer_id
        GROUP BY COALESCE(p.id, oi.product_name)
        ORDER BY order_count DESC, total_qty DESC
        LIMIT 30
    """).fetchall()

    # Most clicked products from search (demand signal)
    top_clicked = conn.execute("""
        SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
               pr.name as producer, p.stock_quantity,
               COUNT(*) as click_count
        FROM search_clicks sc
        JOIN products p ON p.id = sc.product_id
        JOIN producers pr ON pr.id = p.producer_id
        WHERE sc.created_at >= datetime('now', '-30 days')
        GROUP BY sc.product_id
        ORDER BY click_count DESC
        LIMIT 20
    """).fetchall()

    # Category breakdown
    categories = conn.execute("""
        SELECT c.name as category,
               COUNT(*) as product_count,
               SUM(CASE WHEN p.stock_quantity > 0 THEN 1 ELSE 0 END) as in_stock_count,
               SUM(CASE WHEN p.stock_quantity = 0 THEN 1 ELSE 0 END) as out_of_stock_count,
               SUM(CASE WHEN p.stock_quantity IS NULL THEN 1 ELSE 0 END) as no_data_count,
               SUM(CASE WHEN p.image_path IS NOT NULL AND p.image_path != '' THEN 1 ELSE 0 END) as with_photo
        FROM products p
        JOIN categories c ON c.id = p.category_id
        WHERE p.is_active = 1
        GROUP BY c.id
        ORDER BY product_count DESC
    """).fetchall()

    # Product requests ("Can't find?" submissions — unmet demand)
    product_requests = conn.execute("""
        SELECT request_text, COUNT(*) as request_count,
               MAX(created_at) as last_requested
        FROM product_requests
        GROUP BY LOWER(TRIM(request_text))
        ORDER BY request_count DESC
        LIMIT 20
    """).fetchall()

    # Pseudo-accounts found in 1C balances (curated list — cash registers,
    # supplier-bonus accumulators, structural ledger accounts).
    from backend.services.pseudo_clients import (
        sql_exclusion_params,
    )
    placeholders = ",".join("?" * len(sql_exclusion_params()))
    suppliers_1c = conn.execute(f"""
        SELECT client_name_1c as name,
               SUM(period_debit) as total_debit,
               SUM(period_credit) as total_credit
          FROM client_balances
         WHERE client_name_1c IN ({placeholders})
         GROUP BY client_name_1c
         ORDER BY total_debit DESC
    """, sql_exclusion_params()).fetchall()

    # App producers for comparison
    app_producers = conn.execute("""
        SELECT pr.name, pr.product_count,
               SUM(CASE WHEN p.stock_quantity > 0 THEN 1 ELSE 0 END) as in_stock,
               SUM(CASE WHEN p.image_path IS NOT NULL AND p.image_path != '' THEN 1 ELSE 0 END) as with_photo
        FROM producers pr
        LEFT JOIN products p ON p.producer_id = pr.id AND p.is_active = 1
        GROUP BY pr.id
        ORDER BY pr.product_count DESC
        LIMIT 30
    """).fetchall()

    # Smart alerts: only ACTIVE products (sold in 3mo or supplied 2+ in 6mo)
    try:
        from backend.services.stock_alerts import get_stock_alerts
        active_alerts = get_stock_alerts(conn)
    except Exception as e:
        logger.warning(f"stock_alerts failed: {e}")
        active_alerts = {"active_count": 0, "out_of_stock": [], "running_low": [], "healthy_count": 0}

    conn.close()

    return {
        "ok": True,
        "latest_upload": latest_upload,
        "stock_summary": {
            "total": total_products,
            "in_stock": in_stock,
            "low_stock": low_stock,
            "out_of_stock": out_of_stock,
            "no_data": no_data,
            "with_photos": with_photos,
            "stale": stale_count,
        },
        "active_alerts": active_alerts,
        "low_stock_items": [dict(r) for r in low_stock_items],
        "out_of_stock_items": [dict(r) for r in out_of_stock_items],
        "no_data_items": [dict(r) for r in no_data_items],
        "stale_items": [dict(r) for r in stale_items],
        "top_ordered_app": [dict(r) for r in top_ordered_app],
        "top_clicked": [dict(r) for r in top_clicked],
        "categories": [dict(r) for r in categories],
        "product_requests": [dict(r) for r in product_requests],
        "suppliers_1c": [dict(r) for r in suppliers_1c],
        "app_producers": [dict(r) for r in app_producers],
    }


# ── Search Insights ──────────────────────────────────────────────


@router.get("/search-insights")
def search_insights(
    admin_key: str = Query(...),
    days: int = Query(30, ge=1, le=365),
):
    """Search analytics summary."""
    _check_admin(admin_key)
    conn = get_db()

    total_searches = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",)
    ).fetchone()[0]

    unique_users = conn.execute(
        "SELECT COUNT(DISTINCT telegram_id) FROM search_logs WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",)
    ).fetchone()[0]

    zero_result_count = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE results_count = 0 AND created_at >= datetime('now', ?)",
        (f"-{days} days",)
    ).fetchone()[0]

    top_queries = conn.execute("""
        SELECT query, COUNT(*) as count,
               ROUND(AVG(results_count), 1) as avg_results
        FROM search_logs
        WHERE created_at >= datetime('now', ?)
        GROUP BY query ORDER BY count DESC LIMIT 20
    """, (f"-{days} days",)).fetchall()

    zero_results = conn.execute("""
        SELECT query, COUNT(*) as count,
               COUNT(DISTINCT telegram_id) as unique_users
        FROM search_logs
        WHERE results_count = 0 AND created_at >= datetime('now', ?)
        GROUP BY query ORDER BY count DESC LIMIT 20
    """, (f"-{days} days",)).fetchall()

    daily_volume = conn.execute("""
        SELECT DATE(created_at) as day,
               COUNT(*) as searches,
               COUNT(DISTINCT telegram_id) as users,
               SUM(CASE WHEN results_count = 0 THEN 1 ELSE 0 END) as zero_results
        FROM search_logs
        WHERE created_at >= datetime('now', ?)
        GROUP BY DATE(created_at) ORDER BY day ASC
    """, (f"-{days} days",)).fetchall()

    conn.close()

    return {
        "ok": True,
        "days": days,
        "overview": {
            "total_searches": total_searches,
            "unique_users": unique_users,
            "zero_result_count": zero_result_count,
            "zero_result_pct": round(zero_result_count / total_searches * 100, 1) if total_searches else 0,
        },
        "top_queries": [dict(r) for r in top_queries],
        "zero_results": [dict(r) for r in zero_results],
        "daily_volume": [dict(r) for r in daily_volume],
    }


# ── Platform Health ──────────────────────────────────────────────


@router.get("/platform-health")
def platform_health(admin_key: str = Query(...)):
    """Platform health metrics."""
    _check_admin(admin_key)
    conn = get_db()

    total_registered = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    total_approved = conn.execute("SELECT COUNT(*) FROM users WHERE is_approved = 1").fetchone()[0]
    users_with_orders = conn.execute("SELECT COUNT(DISTINCT telegram_id) FROM orders").fetchone()[0]

    repeat_users = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT telegram_id FROM orders GROUP BY telegram_id HAVING COUNT(*) >= 2
        )
    """).fetchone()[0]

    total_whitelist = conn.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]

    order_trend = conn.execute("""
        SELECT strftime('%Y-%m', created_at) as month,
               COUNT(*) as order_count,
               COUNT(DISTINCT telegram_id) as unique_buyers,
               SUM(total_usd) as total_usd,
               SUM(total_uzs) as total_uzs,
               SUM(item_count) as total_items
        FROM orders GROUP BY month ORDER BY month ASC
    """).fetchall()

    total_products = conn.execute("SELECT COUNT(*) FROM products WHERE is_active = 1").fetchone()[0]
    with_photos = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND image_path IS NOT NULL AND image_path != ''"
    ).fetchone()[0]
    with_stock = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND stock_quantity IS NOT NULL"
    ).fetchone()[0]
    with_price_usd = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND price_usd > 0"
    ).fetchone()[0]
    with_price_uzs = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1 AND price_uzs > 0"
    ).fetchone()[0]

    clients_with_1c_id = conn.execute(
        "SELECT COUNT(*) FROM allowed_clients WHERE client_id_1c IS NOT NULL AND client_id_1c != ''"
    ).fetchone()[0]
    clients_with_telegram = conn.execute(
        "SELECT COUNT(*) FROM allowed_clients WHERE matched_telegram_id IS NOT NULL"
    ).fetchone()[0]

    balance_clients = conn.execute("SELECT COUNT(DISTINCT client_name_1c) FROM client_balances").fetchone()[0]
    balance_periods = conn.execute("SELECT COUNT(DISTINCT period_start) FROM client_balances").fetchone()[0]

    recent_orders = conn.execute("SELECT COUNT(*) FROM orders WHERE created_at >= datetime('now', '-7 days')").fetchone()[0]
    recent_searches = conn.execute("SELECT COUNT(*) FROM search_logs WHERE created_at >= datetime('now', '-7 days')").fetchone()[0]
    recent_registrations = conn.execute("SELECT COUNT(*) FROM users WHERE registered_at >= datetime('now', '-7 days')").fetchone()[0]

    conn.close()

    return {
        "ok": True,
        "registration_funnel": {
            "whitelist": total_whitelist,
            "registered": total_registered,
            "approved": total_approved,
            "ordered": users_with_orders,
            "repeat": repeat_users,
        },
        "order_trend": [dict(r) for r in order_trend],
        "data_quality": {
            "total_products": total_products,
            "with_photos": with_photos,
            "photo_pct": round(with_photos / total_products * 100, 1) if total_products else 0,
            "with_stock_data": with_stock,
            "stock_pct": round(with_stock / total_products * 100, 1) if total_products else 0,
            "with_price_usd": with_price_usd,
            "with_price_uzs": with_price_uzs,
        },
        "client_data": {
            "total_whitelist": total_whitelist,
            "with_1c_id": clients_with_1c_id,
            "with_telegram": clients_with_telegram,
            "balance_clients": balance_clients,
            "balance_periods": balance_periods,
        },
        "recent_activity": {
            "orders_7d": recent_orders,
            "searches_7d": recent_searches,
            "registrations_7d": recent_registrations,
        },
    }


# ── Inventory Intelligence v2 — weekly tugagan + top sellers ─────


def _week_bounds_tashkent(weeks_back: int = 0):
    """Return (monday_utc_str, monday_tk_date_str, sunday_tk_date_str) for the
    work week N weeks before this Monday. weeks_back=0 → this week, 1 → last week.
    UTC string is `YYYY-MM-DD HH:MM:SS` for `stockout_at` comparisons; Tashkent
    date strings are `YYYY-MM-DD` for `real_orders.doc_date` comparisons.
    """
    from datetime import datetime, timedelta, timezone
    from zoneinfo import ZoneInfo
    tk = ZoneInfo("Asia/Tashkent")
    now_tk = datetime.now(tk)
    monday_tk = (now_tk - timedelta(days=now_tk.weekday() + 7 * weeks_back)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    sunday_tk = monday_tk + timedelta(days=6)
    monday_utc_str = monday_tk.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return monday_utc_str, monday_tk.strftime("%Y-%m-%d"), sunday_tk.strftime("%Y-%m-%d")


def _days_out_tashkent(stockout_at_utc: str, now_tk=None) -> int:
    """Whole calendar days in Tashkent between stockout_at and now."""
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    if not stockout_at_utc:
        return 0
    try:
        dt_utc = datetime.strptime(stockout_at_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return 0
    tk = ZoneInfo("Asia/Tashkent")
    if now_tk is None:
        now_tk = datetime.now(tk)
    dt_tk = dt_utc.astimezone(tk)
    return max(0, (now_tk.date() - dt_tk.date()).days)


@router.get("/inventory-week-out")
def inventory_week_out(admin_key: str = Query(...)):
    """Items that ran out this work week (Mon–Sat Tashkent) and are still 0.

    Cumulative within the week, resets Monday. Restocked items naturally drop
    because we filter `stock_quantity < 1`. Each row carries `days_out` for
    spotting items sitting unfilled long enough to be a personnel-monitoring
    signal (3+ days = warning territory).
    """
    _check_admin(admin_key)
    conn = get_db()
    monday_utc, monday_tk_date, _ = _week_bounds_tashkent(0)

    rows = conn.execute(
        """SELECT p.id,
                  p.name as name_cyrillic,
                  p.name_display,
                  p.unit,
                  p.stockout_at,
                  pr.name as producer
           FROM products p
           JOIN producers pr ON pr.id = p.producer_id
           WHERE p.is_active = 1
             AND p.stock_quantity < 1
             AND p.stockout_at IS NOT NULL
             AND p.stockout_at >= ?
           ORDER BY p.stockout_at ASC""",
        (monday_utc,),
    ).fetchall()

    if not rows:
        conn.close()
        return {
            "ok": True,
            "week_start": monday_tk_date,
            "count": 0,
            "items": [],
        }

    ids = [r["id"] for r in rows]
    placeholders = ",".join("?" for _ in ids)

    last_sold = {
        r["product_id"]: r["last_date"]
        for r in conn.execute(
            f"""SELECT roi.product_id, MAX(ro.doc_date) as last_date
                FROM real_order_items roi
                JOIN real_orders ro ON ro.id = roi.real_order_id
                WHERE roi.product_id IN ({placeholders})
                GROUP BY roi.product_id""",
            ids,
        ).fetchall()
    }
    last_supplied = {
        r["pid"]: r["last_date"]
        for r in conn.execute(
            f"""SELECT soi.matched_product_id as pid, MAX(so.doc_date) as last_date
                FROM supply_order_items soi
                JOIN supply_orders so ON so.id = soi.supply_order_id
                WHERE soi.matched_product_id IN ({placeholders})
                GROUP BY soi.matched_product_id""",
            ids,
        ).fetchall()
    }

    conn.close()

    items = []
    for r in rows:
        pid = r["id"]
        days_out = _days_out_tashkent(r["stockout_at"])
        items.append({
            "product_id": pid,
            "name": r["name_cyrillic"],
            "name_display": r["name_display"],
            "producer": r["producer"],
            "unit": r["unit"] or "шт",
            "stockout_at_utc": r["stockout_at"],
            "days_out": days_out,
            "last_sold": last_sold.get(pid),
            "last_supplied": last_supplied.get(pid),
        })

    return {
        "ok": True,
        "week_start": monday_tk_date,
        "count": len(items),
        "items": items,
    }


def _latest_fxrate(conn, fallback: float = 12000.0) -> float:
    row = conn.execute(
        """SELECT rate FROM daily_fx_rates
           WHERE currency_pair = 'USD_UZS'
           ORDER BY rate_date DESC LIMIT 1"""
    ).fetchone()
    return float(row["rate"]) if row and row["rate"] else fallback


def _aggregate_sales_by_product(conn, start_tk_date: str, end_tk_date: str, fxrate: float):
    """Return {product_id: {units, revenue_uzs_native, revenue_usd_native, revenue_usd_eq}}.

    revenue_uzs_native: sum of `total_local` from orders where currency='UZS'
    revenue_usd_native: sum of `total_local` from orders where currency='USD'
    revenue_usd_eq: USD-equivalent ranking value =
        revenue_usd_native + (revenue_uzs_native / fxrate)
    """
    rows = conn.execute(
        """SELECT roi.product_id,
                  p.name as name_cyrillic,
                  p.name_display,
                  p.unit,
                  SUM(roi.quantity) as units,
                  SUM(CASE WHEN ro.currency = 'UZS' THEN roi.total_local ELSE 0 END) as uzs_rev,
                  SUM(CASE WHEN ro.currency = 'USD' THEN roi.total_local ELSE 0 END) as usd_rev
           FROM real_order_items roi
           JOIN real_orders ro ON ro.id = roi.real_order_id
           LEFT JOIN products p ON p.id = roi.product_id
           WHERE ro.doc_date >= ?
             AND ro.doc_date <= ?
             AND roi.product_id IS NOT NULL
             AND roi.quantity > 0
           GROUP BY roi.product_id""",
        (start_tk_date, end_tk_date),
    ).fetchall()

    out = {}
    for r in rows:
        pid = r["product_id"]
        uzs = float(r["uzs_rev"] or 0)
        usd = float(r["usd_rev"] or 0)
        usd_eq = usd + (uzs / fxrate if fxrate > 0 else 0)
        out[pid] = {
            "product_id": pid,
            "name": (r["name_cyrillic"] or "—"),
            "name_display": r["name_display"],
            "unit": r["unit"] or "шт",
            "units": float(r["units"] or 0),
            "revenue_uzs_native": uzs,
            "revenue_usd_native": usd,
            "revenue_usd_eq": usd_eq,
        }
    return out


@router.get("/top-sellers-wow")
def top_sellers_wow(
    admin_key: str = Query(...),
    limit: int = Query(20, ge=1, le=100),
):
    """Top N products by USD-equivalent revenue this work week (Mon–Sat),
    with last-week comparison and rank change.

    USD-equivalent ranking — UZS revenue is converted via the latest
    `daily_fx_rates` rate for ranking only; native UZS and USD revenues are
    preserved per row so the dual-currency rule is not violated at the data
    layer. Display layer chooses how to surface.
    """
    _check_admin(admin_key)
    conn = get_db()

    fxrate = _latest_fxrate(conn)
    _, this_mon, this_sat = _week_bounds_tashkent(0)
    _, last_mon, last_sat = _week_bounds_tashkent(1)

    this_week = _aggregate_sales_by_product(conn, this_mon, this_sat, fxrate)
    last_week = _aggregate_sales_by_product(conn, last_mon, last_sat, fxrate)

    conn.close()

    # Rank both weeks by usd_eq desc
    this_ranking = sorted(
        this_week.values(), key=lambda r: r["revenue_usd_eq"], reverse=True
    )
    last_ranking = sorted(
        last_week.values(), key=lambda r: r["revenue_usd_eq"], reverse=True
    )
    last_rank_by_pid = {r["product_id"]: idx + 1 for idx, r in enumerate(last_ranking)}

    items = []
    for idx, r in enumerate(this_ranking[:limit]):
        pid = r["product_id"]
        last = last_week.get(pid)
        last_units = last["units"] if last else 0
        last_usd_eq = last["revenue_usd_eq"] if last else 0
        units_delta_pct = (
            round((r["units"] - last_units) / last_units * 100, 1)
            if last_units > 0 else None
        )
        usd_delta_pct = (
            round((r["revenue_usd_eq"] - last_usd_eq) / last_usd_eq * 100, 1)
            if last_usd_eq > 0 else None
        )
        last_rank = last_rank_by_pid.get(pid)
        rank_change = (last_rank - (idx + 1)) if last_rank is not None else None
        items.append({
            "rank": idx + 1,
            "product_id": pid,
            "name": r["name"],
            "unit": r["unit"],
            "this_week": {
                "units": r["units"],
                "revenue_uzs_native": r["revenue_uzs_native"],
                "revenue_usd_native": r["revenue_usd_native"],
                "revenue_usd_eq": round(r["revenue_usd_eq"], 2),
            },
            "last_week": {
                "units": last_units,
                "revenue_usd_eq": round(last_usd_eq, 2),
                "rank": last_rank,
            },
            "delta": {
                "units_pct": units_delta_pct,
                "usd_eq_pct": usd_delta_pct,
                "rank": rank_change,  # positive = moved up, None = new entry
            },
        })

    return {
        "ok": True,
        "week_start": this_mon,
        "last_week_start": last_mon,
        "fxrate_used": fxrate,
        "limit": limit,
        "items": items,
    }


@router.get("/top-sellers-period")
def top_sellers_period(
    admin_key: str = Query(...),
    period: str = Query("last_week"),
    limit: int = Query(20, ge=1, le=100),
):
    """Top N products by USD-equivalent revenue for a closed period.

    period:
      - 'last_week' → Monday to Sunday of the previous fully-completed
        week (Tashkent). Stable, doesn't shift during the day.
      - 'yesterday' → single Tashkent calendar day immediately before today.

    Pure ranking — no week-over-week comparison. UZS revenue is converted
    via latest `daily_fx_rates.rate` for the sort key only; native UZS /
    USD revenues are preserved per row (dual-currency rule).
    """
    if period not in ("last_week", "yesterday"):
        raise HTTPException(
            status_code=400,
            detail="period must be 'last_week' or 'yesterday'",
        )

    _check_admin(admin_key)
    conn = get_db()

    fxrate = _latest_fxrate(conn)

    if period == "last_week":
        _, start_tk, end_tk = _week_bounds_tashkent(1)
        period_label = f"{start_tk} — {end_tk}"
    else:
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        yesterday_tk = (
            datetime.now(ZoneInfo("Asia/Tashkent")) - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        start_tk = end_tk = yesterday_tk
        period_label = yesterday_tk

    agg = _aggregate_sales_by_product(conn, start_tk, end_tk, fxrate)
    conn.close()

    ranked = sorted(
        agg.values(), key=lambda r: r["revenue_usd_eq"], reverse=True
    )[:limit]
    items = [
        {
            "rank": idx + 1,
            "product_id": r["product_id"],
            "name": r["name"],
            "name_display": r["name_display"],
            "unit": r["unit"],
            "units": r["units"],
            "revenue_uzs_native": r["revenue_uzs_native"],
            "revenue_usd_native": r["revenue_usd_native"],
            "revenue_usd_eq": round(r["revenue_usd_eq"], 2),
        }
        for idx, r in enumerate(ranked)
    ]
    return {
        "ok": True,
        "period": period,
        "period_label": period_label,
        "start_date": start_tk,
        "end_date": end_tk,
        "fxrate_used": fxrate,
        "count": len(items),
        "items": items,
    }


@router.post("/upload-images")
async def upload_images(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Upload a ZIP of {product_id}.{png,jpg,jpeg,webp} files. Converts every
    image to WebP q=80 on intake (storage + bandwidth durable fix), removes any
    sibling-extension stragglers for the same product_id, then runs sync_images.
    """
    if not check_admin_key(admin_key):
        from fastapi.responses import JSONResponse
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    import zipfile
    import tempfile
    import os
    from pathlib import Path
    from backend.services.convert_to_webp import encode_webp_from_bytes

    file_bytes = await file.read()
    if not file_bytes:
        from fastapi.responses import JSONResponse
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)

    images_dir = Path(os.getenv("IMAGES_DIR", "./images"))
    images_dir.mkdir(parents=True, exist_ok=True)

    added = replaced = skipped = errors = 0
    error_files: list[dict] = []
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    try:
        with zipfile.ZipFile(tmp_path, "r") as zf:
            for name in zf.namelist():
                base = Path(name).name
                if not base or base.startswith(".") or base.startswith("__"):
                    continue
                ext = Path(base).suffix.lower()
                if ext not in ('.png', '.jpg', '.jpeg', '.webp'):
                    continue
                stem = Path(base).stem
                try:
                    int(stem)
                except ValueError:
                    skipped += 1
                    continue
                dest = images_dir / f"{stem}.webp"
                existed = any(
                    (images_dir / f"{stem}{e}").exists()
                    for e in ('.webp', '.png', '.jpg', '.jpeg')
                )
                try:
                    src_bytes = zf.read(name)
                    if ext == '.webp':
                        # Already WebP — passthrough, don't re-encode.
                        with open(dest, 'wb') as dst:
                            dst.write(src_bytes)
                    else:
                        encode_webp_from_bytes(src_bytes, dest)
                except Exception as e:
                    errors += 1
                    error_files.append({"file": base, "error": repr(e)[:200]})
                    continue
                # Remove sibling-extension stragglers (old PNG/JPG for this product).
                for stale_ext in ('.png', '.jpg', '.jpeg'):
                    stale = images_dir / f"{stem}{stale_ext}"
                    if stale.exists():
                        stale.unlink(missing_ok=True)
                if existed:
                    replaced += 1
                else:
                    added += 1
    finally:
        os.unlink(tmp_path)

    from backend.services.sync_images import sync
    sync()

    total = sum(1 for f in images_dir.iterdir()
                if f.suffix.lower() in ('.png', '.jpg', '.jpeg', '.webp'))

    return {"ok": True, "added": added, "replaced": replaced,
            "skipped": skipped, "errors": errors, "error_files": error_files[:10],
            "total": total}


@router.get("/unmatched-names")
def get_unmatched_names(admin_key: str = Query(...)):
    """List unresolved unmatched import names."""
    _check_admin(admin_key)
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, occurrences, source, created_at "
        "FROM unmatched_import_names WHERE resolved = 0 "
        "ORDER BY name"
    ).fetchall()
    conn.close()
    return {"ok": True, "count": len(rows), "names": [dict(r) for r in rows]}


# ── New-products review queue ────────────────────────────────────
#
# Surfaces items in "Yangi mahsulotlar" so admins can promote them
# (or anything still flagged auto_classified=1) to their correct
# category/producer. Without this, items dumped into the new-arrivals
# bucket by /prices auto-add or /realorders SKU ingest accumulate
# silently — no aging, no expiry. See product_classifier.py.

@router.get("/new-products-pending")
def new_products_pending(
    admin_key: str = Query(...),
    include_classified: int = Query(0),
):
    """Products that need review.

    Default: items currently sitting in "Yangi mahsulotlar" (no brand-prefix
    match → fell back to the new-arrivals bucket).

    include_classified=1: also include products auto-classified to a brand
    family but never confirmed by an admin (auto_classified=1). These were
    placed by prefix match — usually correct but worth a glance.
    """
    _check_admin(admin_key)
    conn = get_db()

    new_cat = conn.execute(
        "SELECT id FROM categories WHERE name = ?", ("Yangi mahsulotlar",)
    ).fetchone()
    new_cat_id = new_cat["id"] if new_cat else None

    where_parts = ["p.is_active = 1"]
    if include_classified:
        # Items in Yangi mahsulotlar OR auto-classified anywhere
        if new_cat_id is not None:
            where_parts.append(
                f"(p.category_id = {new_cat_id} OR p.auto_classified = 1)"
            )
        else:
            where_parts.append("p.auto_classified = 1")
    else:
        if new_cat_id is None:
            conn.close()
            return {"ok": True, "count": 0, "products": [], "categories": [], "producers": []}
        where_parts.append(f"p.category_id = {new_cat_id}")

    where_sql = " AND ".join(where_parts)

    rows = conn.execute(f"""
        SELECT p.id, p.name, p.name_display, p.created_at, p.auto_classified,
               p.price_usd, p.price_uzs, p.stock_status,
               p.category_id, p.producer_id,
               c.name AS category_name, pr.name AS producer_name
        FROM products p
        LEFT JOIN categories c ON c.id = p.category_id
        LEFT JOIN producers pr ON pr.id = p.producer_id
        WHERE {where_sql}
        ORDER BY (p.created_at IS NULL), p.created_at DESC, p.id DESC
        LIMIT 500
    """).fetchall()

    # Side cache: full lists so the UI can render reassign dropdowns
    categories = conn.execute(
        "SELECT id, name FROM categories ORDER BY name"
    ).fetchall()
    producers = conn.execute(
        "SELECT id, name FROM producers ORDER BY name"
    ).fetchall()

    conn.close()
    return {
        "ok": True,
        "count": len(rows),
        "new_arrivals_category_id": new_cat_id,
        "products": [dict(r) for r in rows],
        "categories": [dict(c) for c in categories],
        "producers": [dict(p) for p in producers],
    }


@router.post("/reassign-product")
def reassign_product(
    product_id: int = Form(...),
    category_id: int = Form(...),
    producer_id: int = Form(...),
    admin_key: str = Form(...),
):
    """Move a product to a different category/producer and clear the
    auto_classified flag so it leaves the review queue.

    Both IDs must reference existing rows; the endpoint refuses partials
    rather than silently leaving the product mis-assigned.
    """
    _check_admin(admin_key)
    conn = get_db()

    prod = conn.execute(
        "SELECT id, category_id, producer_id FROM products WHERE id = ?",
        (product_id,),
    ).fetchone()
    if not prod:
        conn.close()
        raise HTTPException(status_code=404, detail="Product not found")

    cat_ok = conn.execute(
        "SELECT 1 FROM categories WHERE id = ?", (category_id,)
    ).fetchone()
    prod_ok = conn.execute(
        "SELECT 1 FROM producers WHERE id = ?", (producer_id,)
    ).fetchone()
    if not cat_ok:
        conn.close()
        raise HTTPException(status_code=400, detail="Invalid category_id")
    if not prod_ok:
        conn.close()
        raise HTTPException(status_code=400, detail="Invalid producer_id")

    conn.execute(
        "UPDATE products SET category_id = ?, producer_id = ?, auto_classified = 0 "
        "WHERE id = ?",
        (category_id, producer_id, product_id),
    )
    # Refresh denorm counts on the affected categories/producers
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
    conn.close()
    return {"ok": True, "product_id": product_id,
            "category_id": category_id, "producer_id": producer_id}


# ── Agent application queue (Block C) ─────────────────────────────────────

@router.get("/pending-agents")
def list_pending_agents(admin_key: str = Query(...)):
    """List all current pending agent applications. Admin-key gated."""
    _check_admin(admin_key)
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, telegram_id, first_name, last_name, phone_normalized, "
            "       vehicle, requested_at "
            "FROM pending_agents WHERE status = 'pending' "
            "ORDER BY requested_at"
        ).fetchall()
        return {
            "ok": True,
            "items": [
                {
                    "application_id": r["id"],
                    "telegram_id": r["telegram_id"],
                    "first_name": r["first_name"],
                    "last_name": r["last_name"],
                    "phone": r["phone_normalized"],
                    "vehicle": r["vehicle"] or "",
                    "requested_at": r["requested_at"],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


@router.post("/approve-agent")
def approve_agent(payload: dict = Body(...), admin_key: str = Query(...)):
    """Approve an agent application. Admin-key gated. Body:
        {application_id: int, approver_telegram_id: int}
    """
    _check_admin(admin_key)
    application_id = payload.get("application_id")
    approver_telegram_id = payload.get("approver_telegram_id")
    if not isinstance(application_id, int) or not isinstance(approver_telegram_id, int):
        return JSONResponse(
            {"ok": False, "error": "application_id + approver_telegram_id required"},
            status_code=400,
        )
    from backend.services.agent_signup import approve_application
    conn = get_db()
    try:
        return approve_application(conn, application_id, approver_telegram_id)
    finally:
        conn.close()


@router.post("/clear-agent-application")
def clear_agent_application(payload: dict = Body(...), admin_key: str = Query(...)):
    """Reset agent-application state for a telegram_id. Two modes:

      full_reset=false (default): delete pending_agents rows + clear
        users.agent_role + is_agent. Phone, client_id, etc. preserved.
      full_reset=true: also DELETE the users row entirely so the next
        /api/users/check returns registered=false, mimicking a brand-new
        user. Use for end-to-end signup re-tests.

    Idempotent. Body: {telegram_id: int, full_reset?: bool}.
    """
    _check_admin(admin_key)
    telegram_id = payload.get("telegram_id")
    full_reset = bool(payload.get("full_reset"))
    if not isinstance(telegram_id, int):
        return JSONResponse(
            {"ok": False, "error": "telegram_id required"},
            status_code=400,
        )
    conn = get_db()
    try:
        cur1 = conn.execute(
            "DELETE FROM pending_agents WHERE telegram_id = ?",
            (telegram_id,),
        )
        if full_reset:
            cur2 = conn.execute(
                "DELETE FROM users WHERE telegram_id = ?",
                (telegram_id,),
            )
            users_action = "deleted"
        else:
            cur2 = conn.execute(
                "UPDATE users SET agent_role = NULL, is_agent = 0 "
                "WHERE telegram_id = ?",
                (telegram_id,),
            )
            users_action = "agent_role_cleared"
        conn.commit()
        return {
            "ok": True,
            "telegram_id": telegram_id,
            "full_reset": full_reset,
            "pending_deleted": cur1.rowcount,
            "users_action": users_action,
            "users_affected": cur2.rowcount,
        }
    finally:
        conn.close()


@router.post("/reject-agent")
def reject_agent(payload: dict = Body(...), admin_key: str = Query(...)):
    """Reject an agent application. Admin-key gated."""
    _check_admin(admin_key)
    application_id = payload.get("application_id")
    rejector_telegram_id = payload.get("rejector_telegram_id")
    reason = payload.get("reason") or None
    if not isinstance(application_id, int) or not isinstance(rejector_telegram_id, int):
        return JSONResponse(
            {"ok": False, "error": "application_id + rejector_telegram_id required"},
            status_code=400,
        )
    from backend.services.agent_signup import reject_application
    conn = get_db()
    try:
        return reject_application(conn, application_id, rejector_telegram_id, reason)
    finally:
        conn.close()
