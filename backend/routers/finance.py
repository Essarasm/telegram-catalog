"""Financial data API — client balances and debts from 1C."""
from fastapi import APIRouter, Query, UploadFile, File, Form
from fastapi.responses import JSONResponse
from typing import List, Optional
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


@router.get("/fx-rate/today")
def fx_rate_today():
    """Return today's FX rate events (0, 1, or 2 rows, newest first) plus a
    yesterday fallback. Consumed by the agent panel FX banner:
      • is_stale=true  → no rate set yet today; banner shows yesterday + warning
      • 1 event         → one rate set today; banner shows it as current
      • 2 events        → newer rate prominent, older rate shown dimmer
    """
    from backend.services.daily_uploads import tashkent_today_str

    today = tashkent_today_str()
    conn = get_db()
    try:
        today_rows = conn.execute(
            """SELECT rate, set_at, set_by_name
               FROM daily_fx_rate_events
               WHERE rate_date = ? AND currency_pair = 'USD_UZS'
               ORDER BY set_at DESC""",
            (today,),
        ).fetchall()
        today_events = [
            {"rate": r["rate"], "set_at": r["set_at"], "set_by_name": r["set_by_name"]}
            for r in today_rows
        ]

        yesterday = None
        if not today_events:
            y = conn.execute(
                """SELECT rate_date, rate FROM daily_fx_rates
                   WHERE currency_pair = 'USD_UZS' AND rate_date < ?
                   ORDER BY rate_date DESC LIMIT 1""",
                (today,),
            ).fetchone()
            if y:
                yesterday = {"rate_date": y["rate_date"], "rate": y["rate"]}
    finally:
        conn.close()

    return {
        "ok": True,
        "today_date": today,
        "today_events": today_events,
        "yesterday": yesterday,
        "is_stale": len(today_events) == 0,
    }


@router.get("/fx-rates-monthly")
def fx_rates_monthly():
    """Return one representative USD_UZS rate per month (the last rate we have
    dated on or before the 1st of the following month — i.e. end-of-month rate)."""
    conn = get_db()
    rows = conn.execute(
        """SELECT rate_date, rate FROM daily_fx_rates
           WHERE currency_pair = 'USD_UZS'
           ORDER BY rate_date ASC"""
    ).fetchall()
    conn.close()
    monthly = {}
    for r in rows:
        m = r["rate_date"][:7]
        monthly[m] = {"rate_date": r["rate_date"], "rate": r["rate"]}
    return {"ok": True, "monthly": monthly, "count": len(rows)}


@router.post("/resend-feedback")
def resend_feedback(feedback_id: int = Form(...), admin_key: str = Form(...)):
    """One-shot: resend an order_feedback row to the Taklif va Xatolar group."""
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=403)
    import os
    import httpx as _httpx
    from backend.routers.feedback import ERRORS_GROUP_CHAT_ID
    bot_token = os.getenv("BOT_TOKEN", "")
    if not bot_token:
        return {"ok": False, "error": "BOT_TOKEN missing"}
    conn = get_db()
    row = conn.execute(
        """SELECT f.id, f.order_id, f.user_id, f.feedback_text, f.created_at,
                  u.first_name, u.last_name, u.phone, ac.client_id_1c
           FROM order_feedback f
           LEFT JOIN users u ON u.telegram_id = f.user_id
           LEFT JOIN allowed_clients ac ON ac.id = u.client_id
           WHERE f.id = ?""", (feedback_id,),
    ).fetchone()
    conn.close()
    if not row:
        return {"ok": False, "error": "feedback_id not found"}

    def esc(s):
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    name = " ".join(filter(None, [row["first_name"], row["last_name"]])) or ""
    lines = ["⚠️ <b>Buyurtma bo'yicha shikoyat</b> (qayta yuborildi)\n"]
    if row["client_id_1c"]:
        lines.append(f"🧾 1C: {esc(row['client_id_1c'])}")
    if name:
        lines.append(f"👤 Telegram: {esc(name)}")
    if row["phone"]:
        lines.append(f"📞 {esc(row['phone'])}")
    lines.append(f"🆔 Telegram ID: <code>{row['user_id']}</code>")
    lines.append(f"🕐 {row['created_at']}")
    lines.append("")
    lines.append(f"💬 {esc(row['feedback_text'])}")
    text = "\n".join(lines)

    try:
        r = _httpx.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": ERRORS_GROUP_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        return {"ok": True, "telegram_response": r.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/debug-errors-group")
def debug_errors_group(admin_key: str = Query(...)):
    """Diagnose why Taklif va Xatolar group isn't receiving messages.

    Pings the group via sendMessage and returns Telegram's response. Also
    returns the configured chat_id so we can see which value prod is using.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=403)
    import os
    import httpx as _httpx
    # Match feedback.py's default so we diagnose the same chat_id the prod flow uses
    from backend.routers.feedback import ERRORS_GROUP_CHAT_ID as chat_id
    bot_token = os.getenv("BOT_TOKEN", "")
    if not bot_token:
        return {"ok": False, "error": "BOT_TOKEN not set", "chat_id": chat_id}
    try:
        # Try getChat first — will tell us if the ID is valid
        r_info = _httpx.get(
            f"https://api.telegram.org/bot{bot_token}/getChat",
            params={"chat_id": chat_id}, timeout=10,
        )
        info = r_info.json()
        # Also try sending a test ping
        r_send = _httpx.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id,
                   "text": "🔧 Diagnostic ping from admin — ignore.",
                   "parse_mode": "HTML"},
            timeout=10,
        )
        send_result = r_send.json()
        return {"ok": True, "chat_id_tried": chat_id,
                "getChat_response": info,
                "sendMessage_response": send_result,
                "env_override_present": os.getenv("ERRORS_GROUP_CHAT_ID") is not None}
    except Exception as e:
        return {"ok": False, "error": str(e), "chat_id": chat_id}


@router.post("/migrate-payments-unique-key")
def migrate_payments_unique_key(admin_key: str = Form(...)):
    """One-time migration: change client_payments.doc_number_1c UNIQUE to
    composite UNIQUE(doc_number_1c, doc_date).

    Reason: 1C doc numbers cycle per year, so \"doc 191\" in Jan 2025 is a
    different document from \"doc 191\" in Jan 2026. The old constraint
    caused new-month imports to silently overwrite old-month data.

    Safe to re-run: detects the new schema and no-ops if already migrated.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=403)
    conn = get_db()
    try:
        # Check current schema
        schema_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='client_payments'"
        ).fetchone()
        if schema_row and "UNIQUE(doc_number_1c, doc_date)" in (schema_row["sql"] or ""):
            conn.close()
            return {"ok": True, "already_migrated": True}

        before = conn.execute("SELECT COUNT(*) AS n FROM client_payments").fetchone()["n"]

        conn.executescript("""
            CREATE TABLE client_payments_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_number_1c TEXT NOT NULL,
                doc_date TEXT NOT NULL,
                doc_time TEXT,
                author TEXT,
                received_from TEXT,
                basis TEXT,
                attachment TEXT,
                corr_account TEXT,
                client_name_1c TEXT,
                client_id INTEGER,
                subconto2 TEXT,
                subconto3 TEXT,
                currency TEXT DEFAULT 'UZS',
                amount_local REAL DEFAULT 0,
                amount_currency REAL DEFAULT 0,
                fx_rate REAL DEFAULT 0,
                cashflow_category TEXT,
                imported_at TEXT DEFAULT (datetime('now')),
                UNIQUE(doc_number_1c, doc_date)
            );

            INSERT INTO client_payments_v2
              (id, doc_number_1c, doc_date, doc_time, author, received_from,
               basis, attachment, corr_account, client_name_1c, client_id,
               subconto2, subconto3, currency, amount_local, amount_currency,
               fx_rate, cashflow_category, imported_at)
            SELECT id, doc_number_1c, doc_date, doc_time, author, received_from,
                   basis, attachment, corr_account, client_name_1c, client_id,
                   subconto2, subconto3, currency, amount_local, amount_currency,
                   fx_rate, cashflow_category, imported_at
            FROM client_payments;

            DROP TABLE client_payments;
            ALTER TABLE client_payments_v2 RENAME TO client_payments;

            CREATE INDEX IF NOT EXISTS idx_client_payments_doc_date ON client_payments(doc_date);
            CREATE INDEX IF NOT EXISTS idx_client_payments_client_name ON client_payments(client_name_1c);
            CREATE INDEX IF NOT EXISTS idx_client_payments_client_id ON client_payments(client_id);
            CREATE INDEX IF NOT EXISTS idx_client_payments_currency ON client_payments(currency);
        """)

        after = conn.execute("SELECT COUNT(*) AS n FROM client_payments").fetchone()["n"]
        conn.commit()
        conn.close()
        return {"ok": True, "already_migrated": False,
                "rows_before": before, "rows_after": after}
    except Exception as e:
        try: conn.close()
        except: pass
        return {"ok": False, "error": str(e)}


@router.post("/import-shipments")
async def import_shipments(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
):
    """Import one monthly 'Реализация товаров' xls into derived_shipments."""
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse({"ok": False, "error": "Empty file"}, status_code=400)
    from backend.services.import_shipments import apply_shipments_import
    return apply_shipments_import(file_bytes)


@router.post("/purge-derived-shipments")
def purge_derived_shipments(admin_key: str = Form(...)):
    """Admin: wipe derived_shipments for a clean rebuild."""
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=403)
    conn = get_db()
    n = conn.execute("SELECT COUNT(*) AS n FROM derived_shipments").fetchone()["n"]
    conn.execute("DELETE FROM derived_shipments")
    conn.commit()
    conn.close()
    return {"ok": True, "rows_deleted": n}


@router.get("/derived-balance")
def derived_balance(client_id: int = Query(...), as_of: Optional[str] = Query(None)):
    """Return derived balance (shipments − payments) for a client.

    No pre-2020 contamination — anchored at the oldest doc_date we have,
    typically 2025-01-01.
    """
    from backend.services.import_shipments import compute_derived_balance
    conn = get_db()
    bal = compute_derived_balance(conn, client_id, as_of)
    # Also pull recent events for drill-down
    events = []
    ship_rows = conn.execute(
        "SELECT doc_date, doc_number, uzs_amount, usd_amount, 'shipment' AS kind "
        "FROM derived_shipments WHERE client_id = ? ORDER BY doc_date DESC LIMIT 30",
        (client_id,),
    ).fetchall()
    pay_rows = conn.execute(
        "SELECT doc_date, doc_number_1c AS doc_number, amount_local, amount_currency, "
        "       currency, 'payment' AS kind "
        "FROM client_payments WHERE client_id = ? ORDER BY doc_date DESC LIMIT 30",
        (client_id,),
    ).fetchall()
    for r in ship_rows:
        events.append({"date": r["doc_date"], "doc": r["doc_number"],
                         "kind": "shipment",
                         "uzs": float(r["uzs_amount"] or 0),
                         "usd": float(r["usd_amount"] or 0)})
    for r in pay_rows:
        events.append({"date": r["doc_date"], "doc": r["doc_number"],
                         "kind": "payment",
                         "uzs": float(r["amount_local"] or 0) if r["currency"] == "UZS" else 0,
                         "usd": float(r["amount_currency"] or 0) if r["currency"] == "USD" else 0})
    events.sort(key=lambda e: e["date"], reverse=True)
    conn.close()
    return {"ok": True, "client_id": client_id, "balance": bal,
            "recent_events": events[:40]}


@router.post("/purge-balances-month")
async def purge_balances_month(
    period: str = Form(...),  # e.g. "2026-03"
    currency: str = Form("ALL"),  # UZS / USD / ALL
    admin_key: str = Form(...),
):
    """Admin: delete all client_balances rows for a given YYYY-MM period.

    Use when a 1C export needs to be re-imported fresh (e.g., to fix the
    phantom-shipment issue in March 2026). After purging, re-upload the
    Оборотно-сальдо file via /api/finance/import-balances or the bot's
    /balances command to re-import cleanly.
    """
    if not check_admin_key(admin_key):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=403)
    if not period or len(period) != 7 or period[4] != "-":
        return {"ok": False, "error": "period must be YYYY-MM"}

    conn = get_db()
    if currency == "ALL":
        cur_clause = ""
        params = (f"{period}%",)
    else:
        cur_clause = " AND currency = ?"
        params = (f"{period}%", currency)

    # Count first
    count_row = conn.execute(
        f"SELECT COUNT(*) AS n FROM client_balances "
        f"WHERE period_start LIKE ?{cur_clause}", params,
    ).fetchone()
    n_before = count_row["n"]

    # Delete
    conn.execute(
        f"DELETE FROM client_balances "
        f"WHERE period_start LIKE ?{cur_clause}", params,
    )
    conn.commit()
    conn.close()
    return {"ok": True, "period": period, "currency": currency,
            "rows_deleted": n_before,
            "next_step": "Re-upload the Оборотно-сальдо xls file(s) via /balances in the bot or /api/finance/import-balances."}


@router.get("/client-lookup")
def client_lookup(name_fragment: str = Query(...)):
    """Find clients whose name contains the fragment. Returns id + payment trail."""
    conn = get_db()
    frag = f"%{name_fragment}%"
    clients = conn.execute(
        "SELECT DISTINCT client_id, client_name FROM client_scores "
        "WHERE client_name LIKE ? LIMIT 20", (frag,),
    ).fetchall()
    out = []
    for c in clients:
        cid = c["client_id"]
        payments = conn.execute(
            "SELECT doc_date, amount_local, currency, corr_account FROM client_payments "
            "WHERE client_id = ? ORDER BY doc_date DESC LIMIT 30",
            (cid,),
        ).fetchall()
        balances = conn.execute(
            "SELECT currency, period_start, period_debit, period_credit, closing_debit, closing_credit "
            "FROM client_balances WHERE client_id = ? "
            "ORDER BY currency, period_start DESC LIMIT 40",
            (cid,),
        ).fetchall()
        out.append({
            "client_id": cid,
            "name": c["client_name"],
            "payment_count": len(payments),
            "last_payments": [{"date": p["doc_date"], "amount": p["amount_local"],
                               "currency": p["currency"], "account": p["corr_account"]}
                              for p in payments],
            "balances": [{"currency": b["currency"], "period": b["period_start"],
                          "debit": b["period_debit"], "credit": b["period_credit"],
                          "balance": (b["closing_debit"] or 0) - (b["closing_credit"] or 0)}
                         for b in balances],
        })
    conn.close()
    return {"ok": True, "matches": out}


@router.get("/seasonal-payment-gaps")
def seasonal_payment_gaps(thresholds_usd: str = Query("125,621,1721,4120")):
    """Median gap in days between consecutive payments, by Proposal B bucket × month.

    Uses client_payments.doc_date history. Each client's payments are sorted by
    date; we compute day-gaps between consecutive payments and tag each gap by
    (current bucket, month-of-current-payment). Median per (bucket, month) gives
    us a seasonality reference — what a "normal" wait-time looks like for a
    client of each size in each calendar month.

    Also returns p25 and p75 so we can show a band.
    """
    try:
        edges = [float(x.strip()) for x in thresholds_usd.split(",")]
        if len(edges) != 4:
            return {"ok": False, "error": "need exactly 4 thresholds"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

    conn = get_db()
    latest = conn.execute("SELECT MAX(recalc_date) AS d FROM client_scores").fetchone()
    if not latest or not latest["d"]:
        conn.close()
        return {"ok": False, "error": "no scoring data"}
    d = latest["d"]
    score_rows = conn.execute(
        "SELECT client_id, monthly_volume_usd FROM client_scores WHERE recalc_date = ?",
        (d,),
    ).fetchall()
    conn_rows = conn.execute(
        "SELECT client_id, doc_date FROM client_payments "
        "WHERE client_id IS NOT NULL AND doc_date IS NOT NULL "
        "ORDER BY client_id, doc_date"
    ).fetchall()
    conn.close()

    def classify(v):
        if v < edges[0]: return "Micro"
        if v < edges[1]: return "Small"
        if v < edges[2]: return "Medium"
        if v < edges[3]: return "Large"
        return "Heavy"

    bucket_of = {
        r["client_id"]: classify(float(r["monthly_volume_usd"] or 0))
        for r in score_rows
    }

    # Collect gaps: (bucket, month) -> [gap_days, ...]
    import datetime as _dt
    gaps: dict = {}
    prev_id = None
    prev_date = None
    for r in conn_rows:
        cid = r["client_id"]
        try:
            cur_date = _dt.date.fromisoformat(r["doc_date"][:10])
        except Exception:
            continue
        if cid == prev_id and prev_date is not None:
            gap = (cur_date - prev_date).days
            if gap >= 0:
                b = bucket_of.get(cid)
                if b:
                    month = cur_date.strftime("%Y-%m")
                    gaps.setdefault((b, month), []).append(gap)
        prev_id = cid
        prev_date = cur_date

    # Also aggregate per-bucket across all months (overall)
    per_bucket_all: dict = {}
    for (b, _), lst in gaps.items():
        per_bucket_all.setdefault(b, []).extend(lst)

    # Per-bucket × calendar month (month-of-year 1..12) — pooled across years
    per_bucket_monthofyear: dict = {}
    for (b, ym), lst in gaps.items():
        moy = int(ym.split("-")[1])
        per_bucket_monthofyear.setdefault((b, moy), []).extend(lst)

    def stats(lst):
        if not lst:
            return None
        s = sorted(lst)
        n = len(s)
        def pct(p):
            k = int(p * (n - 1))
            return s[k]
        return {
            "n": n,
            "median": pct(0.5),
            "p25": pct(0.25),
            "p75": pct(0.75),
            "mean": round(sum(s) / n, 1),
        }

    # Flat matrix: [{bucket, month_of_year, median, p25, p75, n}, ...]
    matrix = []
    for b in ("Micro","Small","Medium","Large","Heavy"):
        for moy in range(1, 13):
            st = stats(per_bucket_monthofyear.get((b, moy), []))
            matrix.append({"bucket": b, "month_of_year": moy,
                            **(st or {"n": 0, "median": None, "p25": None, "p75": None, "mean": None})})
    overall = {b: stats(per_bucket_all.get(b, [])) for b in ("Micro","Small","Medium","Large","Heavy")}
    return {"ok": True, "recalc_date": d, "thresholds_usd": edges,
            "matrix": matrix, "overall_by_bucket": overall}


@router.get("/bucket-examples")
def bucket_examples(
    thresholds_usd: str = Query("125,621,1721,4120"),
    pct: float = Query(0.10),
    min_per_bucket: int = Query(3),
    include_pseudo: bool = Query(False),
    include_all: bool = Query(False),
):
    """Return top and bottom clients per Proposal B bucket for board-ready examples.

    Selects the top `pct` (by score, descending) and bottom `pct` (by score,
    ascending) of each bucket. Falls back to `min_per_bucket` if 10% rounds to
    fewer than that.
    """
    try:
        edges = [float(x.strip()) for x in thresholds_usd.split(",")]
        if len(edges) != 4:
            return {"ok": False, "error": "need exactly 4 thresholds"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

    from backend.services.pseudo_clients import is_pseudo_client

    conn = get_db()
    latest = conn.execute("SELECT MAX(recalc_date) AS d FROM client_scores").fetchone()
    if not latest or not latest["d"]:
        conn.close()
        return {"ok": False, "error": "no scoring data"}
    d = latest["d"]
    all_rows = conn.execute(
        "SELECT client_id, client_name, score, tier, monthly_volume_usd, credit_limit_uzs, "
        "       on_time_rate, debt_ratio, tenure_months "
        "FROM client_scores WHERE recalc_date = ? ORDER BY score DESC", (d,)
    ).fetchall()
    if include_pseudo:
        rows = all_rows
        excluded_pseudo = 0
    else:
        rows = [r for r in all_rows if not is_pseudo_client(r["client_name"])]
        excluded_pseudo = len(all_rows) - len(rows)

    # Client-id / name set (used by several downstream queries)
    name_set = {r["client_name"] for r in rows}
    id_set = {r["client_id"] for r in rows if r["client_id"]}

    # DERIVED balances: cumulative shipments (from derived_shipments) minus
    # cumulative payments (from client_payments). Starts clean at zero — no
    # pre-2020 historical noise. Anchored at the earliest doc_date we have.
    debt_uzs_by_id = {}
    debt_usd_by_id = {}
    if id_set:
        placeholders = ",".join("?" for _ in id_set)
        ship_rows = conn.execute(
            f"SELECT client_id, "
            f"       COALESCE(SUM(uzs_amount), 0) AS uzs, "
            f"       COALESCE(SUM(usd_amount), 0) AS usd "
            f"FROM derived_shipments WHERE client_id IN ({placeholders}) "
            f"GROUP BY client_id",
            tuple(id_set),
        ).fetchall()
        for r in ship_rows:
            debt_uzs_by_id[r["client_id"]] = float(r["uzs"] or 0)
            debt_usd_by_id[r["client_id"]] = float(r["usd"] or 0)
        # Subtract payments
        pay_uzs = conn.execute(
            f"SELECT client_id, COALESCE(SUM(amount_local), 0) AS u "
            f"FROM client_payments WHERE client_id IN ({placeholders}) "
            f"AND currency = 'UZS' GROUP BY client_id",
            tuple(id_set),
        ).fetchall()
        for r in pay_uzs:
            debt_uzs_by_id[r["client_id"]] = (
                debt_uzs_by_id.get(r["client_id"], 0) - float(r["u"] or 0)
            )
        pay_usd = conn.execute(
            f"SELECT client_id, COALESCE(SUM(amount_currency), 0) AS d "
            f"FROM client_payments WHERE client_id IN ({placeholders}) "
            f"AND currency = 'USD' GROUP BY client_id",
            tuple(id_set),
        ).fetchall()
        for r in pay_usd:
            debt_usd_by_id[r["client_id"]] = (
                debt_usd_by_id.get(r["client_id"], 0) - float(r["d"] or 0)
            )

    # Latest FX rate for UZS→USD conversion of debt display
    fx_row = conn.execute(
        "SELECT rate FROM daily_fx_rates WHERE currency_pair = 'USD_UZS' "
        "ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    fx_rate = float(fx_row["rate"]) if fx_row else 12_200.0

    # days_unpaid: days between today and MAX(doc_date) in client_payments.
    # Anchor is MAX(latest_payment_date, current_date) so the number is never
    # negative — we use whatever is fresher.
    import datetime as _dt
    today_dt = _dt.date.today()
    global_max_row = conn.execute(
        "SELECT MAX(doc_date) AS m FROM client_payments"
    ).fetchone()
    if global_max_row and global_max_row["m"]:
        try:
            latest_payment_dt = _dt.date.fromisoformat(global_max_row["m"][:10])
            if latest_payment_dt > today_dt:
                today_dt = latest_payment_dt
        except Exception:
            pass

    days_unpaid_by_id = {}
    days_unpaid_by_name = {}
    # Longest historical payment gap per client (computed below)
    longest_gap_by_id = {}  # client_id → (gap_days, from_date, to_date)
    longest_gap_by_name = {}

    # Days since last payment
    if id_set:
        placeholders = ",".join("?" for _ in id_set)
        last_by_id = conn.execute(
            f"SELECT client_id, MAX(doc_date) AS last_date FROM client_payments "
            f"WHERE client_id IN ({placeholders}) GROUP BY client_id",
            tuple(id_set),
        ).fetchall()
        for lr in last_by_id:
            if lr["last_date"]:
                try:
                    last_dt = _dt.date.fromisoformat(lr["last_date"][:10])
                    days_unpaid_by_id[lr["client_id"]] = max(0, (today_dt - last_dt).days)
                except Exception:
                    pass
    if name_set:
        placeholders = ",".join("?" for _ in name_set)
        last_by_name = conn.execute(
            f"SELECT client_name_1c, MAX(doc_date) AS last_date FROM client_payments "
            f"WHERE client_name_1c IN ({placeholders}) GROUP BY client_name_1c",
            tuple(name_set),
        ).fetchall()
        for lr in last_by_name:
            if lr["last_date"]:
                try:
                    last_dt = _dt.date.fromisoformat(lr["last_date"][:10])
                    days_unpaid_by_name[lr["client_name_1c"]] = max(0, (today_dt - last_dt).days)
                except Exception:
                    pass

    # Longest historical gap BETWEEN PAYMENTS (no-activity streak)
    if id_set:
        placeholders = ",".join("?" for _ in id_set)
        all_pays = conn.execute(
            f"SELECT client_id, doc_date FROM client_payments "
            f"WHERE client_id IN ({placeholders}) AND doc_date IS NOT NULL "
            f"ORDER BY client_id, doc_date",
            tuple(id_set),
        ).fetchall()
        prev_id = None
        prev_date = None
        for p in all_pays:
            try:
                cur_date = _dt.date.fromisoformat(p["doc_date"][:10])
            except Exception:
                continue
            cid = p["client_id"]
            if cid == prev_id and prev_date is not None:
                gap = (cur_date - prev_date).days
                if gap > 0:
                    cur = longest_gap_by_id.get(cid)
                    if cur is None or gap > cur[0]:
                        longest_gap_by_id[cid] = (gap, prev_date.isoformat(), cur_date.isoformat())
            prev_id = cid
            prev_date = cur_date

    # Longest DEBT-OVERHANG (daily precision). Uses the derived-event
    # timeline — individual shipments (from derived_shipments) + individual
    # payments (from client_payments) — to walk day-by-day and track when
    # balance transitions from zero to positive and back.
    #
    # Overhang starts when balance crosses above threshold and ends when
    # ANY payment brings it back down OR a payment comes in (even partial).
    # This is the cleanest definition: "days between a shipment creating
    # new debt and the next payment against that debt".
    OVERHANG_USD_THRESHOLD = 50.0
    longest_overhang_by_id: dict = {}
    if id_set:
        placeholders = ",".join("?" for _ in id_set)
        ship_rows = conn.execute(
            f"SELECT client_id, doc_date, uzs_amount, usd_amount FROM derived_shipments "
            f"WHERE client_id IN ({placeholders}) AND doc_date IS NOT NULL "
            f"ORDER BY client_id, doc_date",
            tuple(id_set),
        ).fetchall()
        pay_rows = conn.execute(
            f"SELECT client_id, doc_date, "
            f"       (CASE WHEN currency='UZS' THEN amount_local ELSE 0 END) AS uzs, "
            f"       (CASE WHEN currency='USD' THEN amount_currency ELSE 0 END) AS usd "
            f"FROM client_payments "
            f"WHERE client_id IN ({placeholders}) AND doc_date IS NOT NULL "
            f"ORDER BY client_id, doc_date",
            tuple(id_set),
        ).fetchall()
        # Bucket events per client
        events_by_cid: dict = {}
        for r in ship_rows:
            try:
                d = _dt.date.fromisoformat(r["doc_date"][:10])
            except Exception:
                continue
            events_by_cid.setdefault(r["client_id"], []).append(
                ("ship", d, float(r["uzs_amount"] or 0), float(r["usd_amount"] or 0))
            )
        for r in pay_rows:
            try:
                d = _dt.date.fromisoformat(r["doc_date"][:10])
            except Exception:
                continue
            events_by_cid.setdefault(r["client_id"], []).append(
                ("pay", d, float(r["uzs"] or 0), float(r["usd"] or 0))
            )

        for cid, events in events_by_cid.items():
            # Sort by date; on same date, shipment before payment (conservative)
            events.sort(key=lambda e: (e[1], 0 if e[0] == "ship" else 1))
            bal_uzs = 0.0
            bal_usd = 0.0
            best = None
            run_start = None
            for kind, d, uzs, usd in events:
                if kind == "ship":
                    bal_uzs += uzs
                    bal_usd += usd
                    balance_usd_eq = bal_usd + bal_uzs / fx_rate
                    if balance_usd_eq > OVERHANG_USD_THRESHOLD and run_start is None:
                        run_start = d
                else:  # pay
                    if run_start is not None:
                        days = (d - run_start).days
                        if best is None or days > best[0]:
                            best = (days, run_start.isoformat(), d.isoformat(), False)
                        run_start = None
                    bal_uzs -= uzs
                    bal_usd -= usd
            # Ongoing streak
            current_balance_usd = bal_usd + bal_uzs / fx_rate
            if run_start is not None and current_balance_usd > OVERHANG_USD_THRESHOLD:
                days = (today_dt - run_start).days
                if best is None or days > best[0]:
                    best = (days, run_start.isoformat(), today_dt.isoformat(), True)
            if best:
                longest_overhang_by_id[cid] = best

    conn.close()

    def classify(v):
        if v < edges[0]: return "Micro"
        if v < edges[1]: return "Small"
        if v < edges[2]: return "Medium"
        if v < edges[3]: return "Large"
        return "Heavy"

    # Group
    buckets = {n: [] for n in ("Micro","Small","Medium","Large","Heavy")}
    for r in rows:
        nm = r["client_name"]
        cid = r["client_id"]
        days = days_unpaid_by_id.get(cid)
        if days is None:
            days = days_unpaid_by_name.get(nm)
        d_uzs = round(debt_uzs_by_id.get(cid, 0), 0)
        d_usd = round(debt_usd_by_id.get(cid, 0), 2)
        d_total_usd = round(d_usd + d_uzs / fx_rate, 2)
        lg = longest_gap_by_id.get(cid) or longest_gap_by_name.get(nm)
        lo = longest_overhang_by_id.get(cid)
        buckets[classify(float(r["monthly_volume_usd"] or 0))].append({
            "client_id": cid,
            "name": nm,
            "score": r["score"],
            "tier": r["tier"],
            "monthly_volume_usd": round(float(r["monthly_volume_usd"] or 0), 0),
            "credit_limit_uzs": round(float(r["credit_limit_uzs"] or 0), 0),
            "credit_limit_usd": round(float(r["credit_limit_uzs"] or 0) / fx_rate, 2),
            "on_time_rate": round(float(r["on_time_rate"] or 0), 3),
            "debt_ratio": round(float(r["debt_ratio"] or 0), 3),
            "tenure_months": round(float(r["tenure_months"] or 0), 1),
            "current_debt_uzs": d_uzs,
            "current_debt_usd_native": d_usd,
            "current_debt_total_usd": d_total_usd,
            "days_unpaid": days,
            # Longest no-activity gap (days between any two consecutive payments)
            "longest_gap_days": lg[0] if lg else None,
            "longest_gap_from": lg[1] if lg else None,
            "longest_gap_to": lg[2] if lg else None,
            # Longest debt-overhang (days carrying non-zero balance continuously)
            "longest_debt_overhang_days": lo[0] if lo else None,
            "longest_debt_overhang_from": lo[1] if lo else None,
            "longest_debt_overhang_to": lo[2] if lo else None,
            "longest_debt_overhang_ongoing": lo[3] if lo else False,
        })

    out = {}
    for name, clients in buckets.items():
        n = len(clients)
        k = max(min_per_bucket, int(round(n * pct)))
        k = min(k, n)
        clients_by_score = sorted(clients, key=lambda x: (-x["score"], -x["monthly_volume_usd"]))
        top = clients_by_score[:k]
        bottom = sorted(clients, key=lambda x: (x["score"], x["monthly_volume_usd"]))[:k]
        entry = {
            "total_in_bucket": n,
            "sample_size": k,
            "top_by_score": top,
            "bottom_by_score": bottom,
        }
        if include_all:
            entry["all_by_score"] = clients_by_score
        out[name] = entry
    return {"ok": True, "recalc_date": d, "thresholds_usd": edges, "pct": pct,
            "fx_rate_used": fx_rate, "buckets": out,
            "excluded_pseudo_count": excluded_pseudo}


@router.get("/bucket-aggregate")
def bucket_aggregate(
    thresholds_usd: str = Query("125,621,1721,4120"),
    include_pseudo: bool = Query(False),
):
    """Aggregate clients by arbitrary volume thresholds.

    Excludes 1C pseudo-accounts (Наличка, Организации (переч.), etc.) by default.
    Pass `include_pseudo=true` to see the raw-data view.
    """
    from backend.services.pseudo_clients import is_pseudo_client
    try:
        edges = [float(x.strip()) for x in thresholds_usd.split(",")]
        if len(edges) != 4:
            return {"ok": False, "error": "need exactly 4 thresholds"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

    conn = get_db()
    latest = conn.execute("SELECT MAX(recalc_date) AS d FROM client_scores").fetchone()
    if not latest or not latest["d"]:
        conn.close()
        return {"ok": False, "error": "no scoring data"}
    d = latest["d"]

    all_rows = conn.execute(
        "SELECT client_id, client_name, monthly_volume_usd, score, credit_limit_uzs "
        "FROM client_scores WHERE recalc_date = ?", (d,)
    ).fetchall()
    if include_pseudo:
        rows = all_rows
        excluded_pseudo = 0
    else:
        rows = [r for r in all_rows if not is_pseudo_client(r["client_name"])]
        excluded_pseudo = len(all_rows) - len(rows)

    # Latest balances in BOTH currencies, by client_name
    latest_uzs = conn.execute(
        "SELECT MAX(period_start) AS p FROM client_balances WHERE currency = 'UZS'"
    ).fetchone()
    latest_usd_b = conn.execute(
        "SELECT MAX(period_start) AS p FROM client_balances WHERE currency = 'USD'"
    ).fetchone()
    debt_uzs = {}
    debt_usd = {}
    if latest_uzs and latest_uzs["p"]:
        for br in conn.execute(
            "SELECT client_name_1c, closing_debit, closing_credit "
            "FROM client_balances WHERE period_start = ? AND currency = 'UZS'",
            (latest_uzs["p"],),
        ).fetchall():
            debt_uzs[br["client_name_1c"]] = (
                float(br["closing_debit"] or 0) - float(br["closing_credit"] or 0)
            )
    if latest_usd_b and latest_usd_b["p"]:
        for br in conn.execute(
            "SELECT client_name_1c, closing_debit, closing_credit "
            "FROM client_balances WHERE period_start = ? AND currency = 'USD'",
            (latest_usd_b["p"],),
        ).fetchall():
            debt_usd[br["client_name_1c"]] = (
                float(br["closing_debit"] or 0) - float(br["closing_credit"] or 0)
            )

    fx_row = conn.execute(
        "SELECT rate FROM daily_fx_rates WHERE currency_pair = 'USD_UZS' "
        "ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    fx_rate = float(fx_row["rate"]) if fx_row else 12_200.0
    conn.close()

    def classify(v):
        if v < edges[0]: return "Micro"
        if v < edges[1]: return "Small"
        if v < edges[2]: return "Medium"
        if v < edges[3]: return "Large"
        return "Heavy"

    agg = {name: {"clients": 0, "score_sum": 0, "vol_sum": 0.0,
                   "limit_sum_uzs": 0.0, "vol_min": float("inf"), "vol_max": 0,
                   "debt_uzs_sum": 0.0, "debt_usd_sum": 0.0}
           for name in ("Micro","Small","Medium","Large","Heavy")}
    for r in rows:
        v = float(r["monthly_volume_usd"] or 0)
        b = classify(v)
        agg[b]["clients"] += 1
        agg[b]["score_sum"] += (r["score"] or 0)
        agg[b]["vol_sum"] += v
        agg[b]["limit_sum_uzs"] += (r["credit_limit_uzs"] or 0)
        agg[b]["vol_min"] = min(agg[b]["vol_min"], v)
        agg[b]["vol_max"] = max(agg[b]["vol_max"], v)
        agg[b]["debt_uzs_sum"] += debt_uzs.get(r["client_name"], 0)
        agg[b]["debt_usd_sum"] += debt_usd.get(r["client_name"], 0)

    out = []
    for name in ("Micro","Small","Medium","Large","Heavy"):
        a = agg[name]
        if a["clients"] == 0:
            out.append({"bucket": name, "clients": 0, "avg_score": None,
                         "total_vol_usd": 0, "total_limit_uzs_current": 0,
                         "total_limit_usd_current": 0,
                         "current_debt_uzs": 0, "current_debt_usd_native": 0,
                         "current_debt_total_usd": 0,
                         "vol_min": None, "vol_max": None})
            continue
        debt_total_usd = a["debt_usd_sum"] + a["debt_uzs_sum"] / fx_rate
        out.append({
            "bucket": name,
            "clients": a["clients"],
            "avg_score": round(a["score_sum"]/a["clients"], 1),
            "total_vol_usd": round(a["vol_sum"], 0),
            "avg_vol_usd": round(a["vol_sum"]/a["clients"], 0),
            "vol_min": round(a["vol_min"], 0),
            "vol_max": round(a["vol_max"], 0),
            "total_limit_uzs_current": round(a["limit_sum_uzs"], 0),
            "total_limit_usd_current": round(a["limit_sum_uzs"] / fx_rate, 0),
            "current_debt_uzs": round(a["debt_uzs_sum"], 0),
            "current_debt_usd_native": round(a["debt_usd_sum"], 2),
            "current_debt_total_usd": round(debt_total_usd, 0),
        })
    return {"ok": True, "recalc_date": d, "thresholds_usd": edges,
            "fx_rate_used": fx_rate, "buckets": out,
            "excluded_pseudo_count": excluded_pseudo}


@router.get("/credit-limits-summary")
def credit_limits_summary(include_pseudo: bool = Query(False)):
    """Per-bucket aggregation: client count, average score, sum of allowable debt.

    Excludes 1C pseudo-accounts by default (Наличка, Организации (переч.), ИСПРАВЛЕНИЕ, etc.).
    """
    from backend.services.pseudo_clients import (
        sql_exclusion_clause, sql_exclusion_params,
    )
    conn = get_db()
    latest = conn.execute("SELECT MAX(recalc_date) AS d FROM client_scores").fetchone()
    if not latest or not latest["d"]:
        conn.close()
        return {"ok": False, "error": "no scoring data"}
    d = latest["d"]

    excl_clause = "" if include_pseudo else f" AND {sql_exclusion_clause('client_name')}"
    excl_params = () if include_pseudo else sql_exclusion_params()

    rows = conn.execute(f"""
        SELECT volume_bucket,
               COUNT(*) AS n,
               AVG(score) AS avg_score,
               MIN(score) AS min_score,
               MAX(score) AS max_score,
               SUM(credit_limit_uzs) AS total_limit_uzs,
               AVG(monthly_volume_usd) AS avg_volume_usd,
               SUM(monthly_volume_usd) AS total_volume_usd
        FROM client_scores
        WHERE recalc_date = ?{excl_clause}
        GROUP BY volume_bucket
        ORDER BY CASE volume_bucket
          WHEN 'Micro' THEN 1 WHEN 'Small' THEN 2
          WHEN 'Medium' THEN 3 WHEN 'Large' THEN 4 WHEN 'Heavy' THEN 5 ELSE 9 END
    """, (d, *excl_params)).fetchall()
    totals = conn.execute(f"""
        SELECT COUNT(*) AS n,
               AVG(score) AS avg_score,
               SUM(credit_limit_uzs) AS total_limit_uzs,
               SUM(monthly_volume_usd) AS total_volume_usd
        FROM client_scores WHERE recalc_date = ?{excl_clause}
    """, (d, *excl_params)).fetchone()

    # Aggregate actual per-currency outstanding debt across all scored clients
    latest_uzs_p = conn.execute(
        "SELECT MAX(period_start) AS p FROM client_balances WHERE currency = 'UZS'"
    ).fetchone()
    latest_usd_p = conn.execute(
        "SELECT MAX(period_start) AS p FROM client_balances WHERE currency = 'USD'"
    ).fetchone()
    total_debt_uzs = 0.0
    total_debt_usd_native = 0.0
    scored_names = [row[0] for row in conn.execute(
        f"SELECT client_name FROM client_scores WHERE recalc_date = ?{excl_clause}",
        (d, *excl_params),
    ).fetchall()]
    if scored_names and latest_uzs_p and latest_uzs_p["p"]:
        placeholders = ",".join("?" for _ in scored_names)
        r = conn.execute(
            f"SELECT SUM(closing_debit - closing_credit) AS s "
            f"FROM client_balances WHERE period_start = ? AND currency = 'UZS' "
            f"AND client_name_1c IN ({placeholders})",
            (latest_uzs_p["p"], *scored_names),
        ).fetchone()
        total_debt_uzs = float(r["s"] or 0)
    if scored_names and latest_usd_p and latest_usd_p["p"]:
        placeholders = ",".join("?" for _ in scored_names)
        r = conn.execute(
            f"SELECT SUM(closing_debit - closing_credit) AS s "
            f"FROM client_balances WHERE period_start = ? AND currency = 'USD' "
            f"AND client_name_1c IN ({placeholders})",
            (latest_usd_p["p"], *scored_names),
        ).fetchone()
        total_debt_usd_native = float(r["s"] or 0)

    fx_row = conn.execute(
        "SELECT rate FROM daily_fx_rates WHERE currency_pair = 'USD_UZS' "
        "ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    fx_rate = float(fx_row["rate"]) if fx_row else 12_200.0
    conn.close()
    return {
        "ok": True,
        "recalc_date": d,
        "fx_rate_used": fx_rate,
        "buckets": [
            {
                "bucket": r["volume_bucket"],
                "clients": r["n"],
                "avg_score": round(r["avg_score"], 1),
                "min_score": r["min_score"],
                "max_score": r["max_score"],
                "total_allowable_debt_uzs": round(r["total_limit_uzs"] or 0, 0),
                "total_allowable_debt_usd": round((r["total_limit_uzs"] or 0) / fx_rate, 0),
                "avg_monthly_volume_usd": round(r["avg_volume_usd"] or 0, 0),
                "total_monthly_volume_usd": round(r["total_volume_usd"] or 0, 0),
            }
            for r in rows
        ],
        "total": {
            "clients": totals["n"],
            "avg_score": round(totals["avg_score"], 1),
            "total_allowable_debt_uzs": round(totals["total_limit_uzs"] or 0, 0),
            "total_allowable_debt_usd": round((totals["total_limit_uzs"] or 0) / fx_rate, 0),
            "total_monthly_volume_usd": round(totals["total_volume_usd"] or 0, 0),
            "total_current_debt_uzs": round(total_debt_uzs, 0),
            "total_current_debt_usd_native": round(total_debt_usd_native, 0),
            "total_current_debt_in_usd": round(total_debt_usd_native + total_debt_uzs / fx_rate, 0),
        },
    }


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
