"""Financial data API — client balances and debts from 1C."""
from fastapi import APIRouter, Query, UploadFile, File, Form
from fastapi.responses import JSONResponse
from typing import List
from backend.database import get_db
from backend.services.import_balances import (
    apply_balance_import,
    get_client_balance,
    get_client_balance_history,
    bulk_import_balances,
)
from backend.services.import_debts import (
    apply_debtors_import,
    get_client_debt,
)
from backend.services.import_real_orders import (
    apply_real_orders_import,
    list_unmatched_real_clients,
    relink_real_orders,
)
from backend.services.import_client_master import apply_client_master_import

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
    if admin_key != "rassvet2026":
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
    if admin_key != "rassvet2026":
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


@router.post("/import-debts")
async def import_debts(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Import client debts from 1C 'Дебиторская задолженность на дату' XLS.

    Used by /debtors bot command. Replaces all records in client_debts
    with the new snapshot.
    """
    if admin_key != "rassvet2026":
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
    if admin_key != "rassvet2026":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)

    result = apply_real_orders_import(file_bytes, filename_hint=file.filename or "")
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
    if admin_key != "rassvet2026":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return list_unmatched_real_clients(limit=limit)


@router.post("/relink-real-orders")
def relink_real_orders_endpoint(
    admin_key: str = Form(""),
):
    """Re-run client matching for every real_orders row where client_id IS NULL.

    Uses a Python-side cyrillic-aware normalization (the fresh-import matcher
    uses SQLite LOWER() which is ASCII-only and misses cyrillic name matches).
    Safe to run repeatedly — already-matched rows are never touched.
    """
    if admin_key != "rassvet2026":
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return relink_real_orders()


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
    if admin_key != "rassvet2026":
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

    client_id = user["client_id"]
    conn.close()

    # Try debtors snapshot first (most accurate)
    debt_data = get_client_debt(client_id)
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
    balance_data = get_client_balance(client_id)

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

    history = get_client_balance_history(user["client_id"], limit)
    conn.close()

    return {"ok": True, "history": history}
