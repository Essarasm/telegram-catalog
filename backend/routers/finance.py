"""Financial data API — client balances and debts from 1C."""
from fastapi import APIRouter, Query, UploadFile, File, Form
from fastapi.responses import JSONResponse
from typing import List
from backend.database import get_db, get_sibling_client_ids
from backend.services.import_balances import (
    apply_balance_import,
    get_client_balance,
    get_client_balance_history,
    bulk_import_balances,
)
from backend.admin_auth import check_admin_key
from backend.services.import_debts import (
    apply_debtors_import,
    get_client_debt,
)
from backend.services.import_real_orders import (
    apply_real_orders_import,
    list_unmatched_real_clients,
    list_unmatched_real_products,
    relink_real_orders,
    get_real_order_sample_for_client,
    backfill_real_order_totals,
    ingest_unmatched_skus,
)
from backend.services.import_client_master import apply_client_master_import
from backend.services.import_cash import apply_cash_import
from backend.services.credit_scoring import (
    get_client_score,
    run_nightly_scoring,
    get_scoring_summary,
    debug_client_scores,
    apply_score_adjustment,
    detect_anomalies,
    search_client_scores,
)

router = APIRouter(prefix="/api/finance", tags=["finance"])


@router.post("/import-balances")
async def import_balances(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Import client balances from 1C оборотно-сальдовая XLS file.

    Used by /balances bot command. Parses the turnover sheet and upserts
    balance snapshots per client per period.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)

    result = apply_balance_import(file_bytes)
    return result


@router.post("/bulk-import")
async def bulk_import(
    files: List[UploadFile] = File(...),
    admin_key: str = Form(""),
):
    """Import multiple balance XLS files at once.

    Accepts up to 30 files (15 months × 2 currencies).
    Used for one-time historical data import.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    if not files:
        return JSONResponse({"ok": False, "error": "No files"}, status_code=400)

    file_list = []
    for f in files:
        data = await f.read()
        if data:
            file_list.append((f.filename or "unknown.xls", data))

    if not file_list:
        return JSONResponse({"ok": False, "error": "All files empty"}, status_code=400)

    result = bulk_import_balances(file_list)
    return result


@router.post("/import-clients")
async def import_clients_upload(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Upload the allowed-clients list (XLS/XLSX). Powers the /clients bot command."""
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)
    from backend.services.import_clients import apply_clients_upload
    return apply_clients_upload(file_bytes, filename_hint=file.filename or "")


@router.post("/import-debts")
async def import_debts(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Import client debts from 1C 'Дебиторская задолженность на дату' XLS.

    Used by /debtors bot command. Replaces all records in client_debts
    with the new snapshot.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)

    result = apply_debtors_import(file_bytes)
    return result


@router.post("/import-real-orders")
async def import_real_orders(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Import real orders from 1C 'Реализация товаров' export.

    Used by the /realorders bot command. Idempotent on doc_number_1c —
    re-uploading the same period replaces existing documents instead of
    duplicating them.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)

    result = apply_real_orders_import(file_bytes, filename_hint=file.filename or "")
    return result


@router.post("/import-cash")
async def import_cash(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Import Касса (cash receipts journal) from 1C.

    Used by the /cash bot command. Idempotent on doc_number_1c — morning
    and evening files have disjoint numbers so both sets persist.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)

    result = apply_cash_import(file_bytes, filename_hint=file.filename or "")
    return result


@router.post("/import-supply")
async def import_supply(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Import Поступление товаров (supply receipts + returns) from 1C.

    Used by the /supply bot command. Idempotent on (doc_number, doc_date).
    Classifies docs as supply / return / adjustment from Контрагент value.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)

    from backend.services.import_supply import apply_supply_import
    result = apply_supply_import(file_bytes, filename_hint=file.filename or "")
    return result


@router.get("/unmatched-real-clients")
def unmatched_real_clients(
    admin_key: str = Query(""),
    limit: int = Query(200, ge=1, le=1000),
):
    """List real_orders rows where client_id IS NULL, grouped by client_name_1c.

    Used by the /unmatchedclients bot command to report which 1C client names
    are not linking to any allowed_clients row, ranked by doc count so ops can
    prioritize the biggest offenders (they tend to be heavy repeat buyers with
    unofficial-name brackets like "/ЯНГИ ЗАПЧ. БОЗОР/").

    System-only 1C markers (ИСПРАВЛЕНИЕ, ИСПРАВЛЕНИЕ СКЛАД 2) are filtered out
    — they are correction documents, not real clients.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return list_unmatched_real_clients(limit=limit)


@router.get("/unmatched-real-products")
def unmatched_real_products(
    admin_key: str = Query(""),
    limit: int = Query(100, ge=1, le=500),
):
    """List real_order_items rows where product_id IS NULL, grouped by product_name_1c.

    Used by the /unmatchedproducts bot command to report which 1C product names
    are not linking to any catalog products row, ranked by line-item count so
    Session A / catalog team can prioritize the SKUs that hurt the most. Unlike
    /unmatchedclients there is no system skip list — every unmatched product is
    a genuine catalog gap.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return list_unmatched_real_products(limit=limit)


@router.post("/relink-real-orders")
def relink_real_orders_endpoint(
    admin_key: str = Form(""),
):
    """Re-run client matching for every real_orders row where client_id IS NULL.

    Uses a Python-side cyrillic-aware normalization (the fresh-import matcher
    uses SQLite LOWER() which is ASCII-only and misses cyrillic name matches).
    Safe to run repeatedly — already-matched rows are never touched.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return relink_real_orders()


@router.get("/real-order-sample")
def real_order_sample(
    admin_key: str = Query(""),
    client: str = Query("", min_length=1),
):
    """Diagnostic: dump the most recent real_order for any client whose name
    matches `client` (substring, cyrillic-aware), with raw DB price columns.

    Used by the /realordersample bot command to determine whether a "no price
    in Cabinet" complaint is a parser bug (zeros in DB) or a render bug
    (data present, UI hiding it).
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return get_real_order_sample_for_client(client)


@router.post("/backfill-real-order-totals")
def backfill_real_order_totals_endpoint(
    admin_key: str = Form(""),
):
    """One-shot backfill: derive missing total_local / sum_local / total_currency
    on existing real_order_items rows, and missing total_sum / total_sum_currency
    on existing real_orders rows. Mirrors import-time post-processing so already-
    ingested 1C exports heal without requiring re-upload. Idempotent.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return backfill_real_order_totals()


@router.post("/ingest-unmatched-skus")
def api_ingest_unmatched_skus(
    admin_key: str = Form(""),
):
    """Add all unmatched product names from real_order_items to the products table.

    For each distinct product_name_1c WHERE product_id IS NULL:
    - Classifies into category/producer by brand family patterns
    - Generates a Latin display name via the import_products pipeline
    - INSERTs into products
    - UPDATEs real_order_items.product_id to link them

    Idempotent: skips products that already exist in the products table.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return ingest_unmatched_skus()


@router.post("/import-client-master-v2")
async def import_client_master_v2(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
    uploaded_by_user_id: int = Form(0),
    uploaded_by_name: str = Form(""),
):
    """v2 importer — reads full-mirror xlsx produced by /exportmaster.

    Only editable ✏️ columns are consumed; 🔒 columns are ignored. Handles
    conflict detection, phone edits with audit trail + collision check,
    status transitions (active/inactive/merged), and chunked commits.
    """
    if not check_admin_key(admin_key):
        return {"ok": False, "error": "bad admin key"}
    file_bytes = await file.read()
    from backend.services.import_client_master_v2 import apply_client_master_v2
    return apply_client_master_v2(
        file_bytes,
        uploaded_by_user_id=uploaded_by_user_id or None,
        uploaded_by_name=uploaded_by_name or None,
    )


@router.post("/import-client-master")
async def import_client_master(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Import the curated Client Master spreadsheet into allowed_clients.

    Used by the /clientmaster bot command. Reads `Contacts` (cyrillic 1C names
    + multi-phone) and `Usto` (contractor sub-clients) sheets, expands each row
    to one allowed_clients row per phone, and falls back to a phoneless row
    keyed by cyrillic name when no phone is present (so the cyrillic-aware
    relink_real_orders() pass can still find the entry).

    Idempotent — re-running with the same xlsx updates existing rows in place
    (non-empty source values do not overwrite existing populated DB fields with
    blanks). Designed to be run repeatedly as ops re-export the spreadsheet.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)
    return apply_client_master_import(file_bytes, filename_hint=file.filename or "")


@router.get("/balance")
def client_balance(telegram_id: int = Query(...)):
    """Get current balance for a client (used by Personal Cabinet).

    Priority: client_debts (дебиторка snapshot) > client_balances (оборотка).
    """
    conn = get_db()

    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user or not user["client_id"]:
        conn.close()
        return {"ok": True, "has_balance": False, "message": "No client record linked"}

    # Resolve all sibling IDs (multi-phone clients share the same client_id_1c)
    client_ids = get_sibling_client_ids(conn, user["client_id"])
    conn.close()

    # Try debtors snapshot first (most accurate)
    debt_data = get_client_debt(client_ids)
    if debt_data is not None:
        # Convert to balance-compatible format for the frontend
        return {
            "ok": True,
            "has_balance": True,
            "source": "debts",
            "balance": {
                "client_name_1c": debt_data["client_name_1c"],
                "debt_uzs": debt_data["debt_uzs"],
                "debt_usd": debt_data["debt_usd"],
                "report_date": debt_data["report_date"],
                "last_transaction_date": debt_data["last_transaction_date"],
                "aging": debt_data["aging"],
                "imported_at": debt_data["imported_at"],
                # Backward-compatible fields
                "balance": debt_data["debt_uzs"],
                "balances_by_currency": {
                    "UZS": {
                        "currency": "UZS",
                        "balance": debt_data["debt_uzs"],
                    },
                    "USD": {
                        "currency": "USD",
                        "balance": debt_data["debt_usd"],
                    },
                },
            },
        }

    # Fall back to оборотка data
    balance_data = get_client_balance(client_ids)

    if not balance_data:
        return {"ok": True, "has_balance": False, "message": "No financial data available yet"}

    return {
        "ok": True,
        "has_balance": True,
        "source": "balances",
        "balance": balance_data,
    }


@router.get("/balance-history")
def client_balance_history(
    telegram_id: int = Query(...),
    limit: int = Query(24, ge=1, le=36),
):
    """Get balance history for a client, separated by currency.

    Returns {UZS: [...], USD: [...]} with monthly snapshots
    sorted chronologically (oldest first, for charting).
    """
    conn = get_db()

    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user or not user["client_id"]:
        conn.close()
        return {"ok": True, "history": {}}

    # Resolve all sibling IDs for multi-phone clients
    client_ids = get_sibling_client_ids(conn, user["client_id"])
    conn.close()

    history = get_client_balance_history(client_ids, limit)

    return {"ok": True, "history": history}


# ── Session G: Credit Score endpoints ────────────────────────────

@router.get("/credit-score")
def client_credit_score(telegram_id: int = Query(...)):
    """Get credit score for a client (used by Personal Cabinet).

    Returns score, tier, volume bucket, credit limit, and 3 hint bullets
    in Uzbek for the soft-launch UI.
    """
    conn = get_db()

    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user or not user["client_id"]:
        conn.close()
        return {"ok": True, "has_score": False, "message": "No client record linked"}

    # Resolve all sibling IDs
    client_ids = get_sibling_client_ids(conn, user["client_id"])
    conn.close()

    # Try to find a score for any sibling
    score_data = None
    for cid in client_ids:
        score_data = get_client_score(cid)
        if score_data:
            break

    if not score_data:
        return {"ok": True, "has_score": False, "message": "No scoring data yet"}

    # Uzbek hint bullets (spec §6.1)
    hints = [
        "To'lovlarni o'z vaqtida amalga oshirish balingizni oshiradi",
        "Muntazam xaridlar balingizga ijobiy ta'sir qiladi",
        "Uzoq muddatli hamkorlik yuqori baho beradi",
    ]

    return {
        "ok": True,
        "has_score": True,
        "score": {
            "value": score_data["score"],
            "tier": score_data["tier"],
            "credit_limit_uzs": score_data["credit_limit_uzs"],
            "volume_bucket": score_data["volume_bucket"],
            "monthly_volume_usd": score_data["monthly_volume_usd"],
            "recalc_date": score_data["recalc_date"],
            "hints": hints,
        },
    }


@router.post("/run-scoring")
def trigger_scoring(admin_key: str = Form("")):
    """Manually trigger scoring recalculation (admin only)."""
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return run_nightly_scoring()


@router.get("/scoring-summary")
def scoring_summary():
    """Get summary statistics from the latest scoring run."""
    return get_scoring_summary()


@router.get("/scoring-debug")
def scoring_debug(limit: int = 10):
    """Debug: show sample client_scores rows (top by volume)."""
    return debug_client_scores(limit)


@router.get("/payments-debug")
def payments_debug(client_id: int = 0):
    """Debug: inspect client_payments match rates and sample data."""
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM client_payments").fetchone()["c"]
        matched = conn.execute("SELECT COUNT(*) as c FROM client_payments WHERE client_id IS NOT NULL").fetchone()["c"]
        null_id = total - matched

        # Sample unmatched names
        unmatched_names = conn.execute(
            """SELECT client_name_1c, COUNT(*) as cnt
               FROM client_payments WHERE client_id IS NULL
               GROUP BY client_name_1c ORDER BY cnt DESC LIMIT 15"""
        ).fetchall()

        # If specific client_id given, show their payments
        client_payments = []
        if client_id:
            rows = conn.execute(
                """SELECT id, doc_date, client_name_1c, client_id, currency,
                          amount_local, amount_currency, fx_rate
                   FROM client_payments
                   WHERE client_id = ?
                   ORDER BY doc_date DESC LIMIT 10""",
                (client_id,),
            ).fetchall()
            client_payments = [dict(r) for r in rows]

        # Also check: how many payments exist for a known name substring
        # (e.g., for client 694 = Зиедилло, what names appear?)
        ziedillo_payments = conn.execute(
            """SELECT id, doc_date, client_name_1c, client_id, currency,
                      amount_local, amount_currency
               FROM client_payments
               WHERE client_name_1c LIKE '%Зиедилло%' OR client_name_1c LIKE '%Ziedillo%'
               ORDER BY doc_date DESC LIMIT 5"""
        ).fetchall()

        # Check allowed_clients name for client 694
        ac_694 = conn.execute(
            "SELECT id, name, client_id_1c FROM allowed_clients WHERE id = 694"
        ).fetchone()

        conn.close()
        return {
            "total_payments": total,
            "matched_payments": matched,
            "unmatched_payments": null_id,
            "match_pct": round(matched / total * 100, 1) if total else 0,
            "top_unmatched_names": [
                {"name": r["client_name_1c"], "count": r["cnt"]}
                for r in unmatched_names
            ],
            "client_694_payments": client_payments if client_id == 694 else [],
            "ziedillo_name_search": [dict(r) for r in ziedillo_payments],
            "allowed_client_694": dict(ac_694) if ac_694 else None,
        }
    finally:
        conn.close()
