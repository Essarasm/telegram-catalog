"""Financial data API — client balances from 1C оборотно-сальдовая."""
from fastapi import APIRouter, Query, UploadFile, File, Form
from fastapi.responses import JSONResponse
from backend.database import get_db
from backend.services.import_balances import apply_balance_import, get_client_balance, get_client_balance_history

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


@router.get("/balance")
def client_balance(telegram_id: int = Query(...)):
    """Get current balance for a client (used by Personal Cabinet).

    Looks up the user's client_id from the users table, then fetches
    their latest balance from client_balances.
    """
    conn = get_db()

    # Get client_id for this telegram user
    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user or not user["client_id"]:
        conn.close()
        return {"ok": True, "has_balance": False, "message": "No client record linked"}

    client_id = user["client_id"]

    # Get latest balance
    balance_data = get_client_balance(client_id)
    conn.close()

    if not balance_data:
        return {"ok": True, "has_balance": False, "message": "No financial data available yet"}

    return {
        "ok": True,
        "has_balance": True,
        "balance": balance_data,
    }


@router.get("/balance-history")
def client_balance_history(
    telegram_id: int = Query(...),
    limit: int = Query(12, ge=1, le=24),
):
    """Get balance history for a client (last N periods)."""
    conn = get_db()

    user = conn.execute(
        "SELECT client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user or not user["client_id"]:
        conn.close()
        return {"ok": True, "history": []}

    history = get_client_balance_history(user["client_id"], limit)
    conn.close()

    return {"ok": True, "history": history}
