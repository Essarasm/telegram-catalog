"""Admin dashboard API — internal analytics for Rassvet's decision-makers.

Phase 2: Supplier auto-detection, clean revenue, client segmentation,
         interactive stock lists, product requests, YoY comparison.

All financial endpoints exclude auto-detected suppliers/accounting entries
unless ?include_suppliers=true is passed.

Note: this file used to host every `/api/admin/*` endpoint. To stay under
the 2,000-line god-module canary, endpoints have been split into sibling
routers — each is registered separately in `backend/main.py`:

- `admin_data_ops.py`  data corrections, image rotation, product review queue
- `admin_revenue.py`   revenue / collections / top-clients / top-sellers
- `admin_debtors.py`   receivables / debtors list / callbacks / client history
- `admin_supply.py`    (already separate) supply dashboard endpoints

The slim residual here owns diagnostics, entity classification, stock /
inventory dashboards, search insights, platform health, agent ops, and
HMAC failure log.
"""
import logging
from fastapi import APIRouter, Body, Query, HTTPException
from fastapi.responses import JSONResponse
from backend.database import get_db

logger = logging.getLogger(__name__)
from backend.admin_auth import check_admin_key

router = APIRouter(prefix="/api/admin", tags=["admin"])


def _check_admin(admin_key: str):
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


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


# ── Entity classification ────────────────────────────────────────


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

    Returns a `stale_items` list: active products NOT present in the most
    recent stock upload — i.e. 1C dropped them from the export entirely.
    These are deactivation candidates (the snapshot-reconciliation pass in
    update_stock.py auto-zeroes any *positive-stock* product missing from an
    upload, so anything left here is a dormant/dropped product worth review).

    NOTE: presence-in-upload is tracked by `stock_last_seen_at` (stamped for
    every product in the file, changed or not), NOT `stock_updated_at` (which
    only moves when qty/status changes — a present-but-unchanged product keeps
    an old value there). Keying staleness on stock_updated_at over-counts
    massively; always use stock_last_seen_at here. See Error Log #93.
    """
    _check_admin(admin_key)
    conn = get_db()

    # Most recent stock upload timestamp. stock_last_seen_at is stamped for
    # every product present in the upload, so its MAX == the latest upload
    # time, and "not seen at that time" == genuinely absent from the upload.
    latest_upload = conn.execute(
        "SELECT MAX(stock_last_seen_at) FROM products WHERE stock_last_seen_at IS NOT NULL"
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

    # "Not in last upload": active products absent from the most recent stock
    # upload (1C dropped them from the export). Deactivation candidates.
    stale_count = 0
    if latest_upload:
        # Not seen at the latest upload (>5min before it, or never seen).
        stale_count = conn.execute(
            """SELECT COUNT(*) FROM products
               WHERE is_active = 1
                 AND (stock_last_seen_at IS NULL
                      OR datetime(stock_last_seen_at) < datetime(?, '-5 minutes'))""",
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

    # "Not in last upload" items: active products absent from the latest
    # upload, longest-absent first (most actionable for deactivation review).
    stale_items = []
    if latest_upload:
        stale_items = conn.execute(
            """SELECT p.id, p.name as name_1c, COALESCE(p.name_display, p.name) as display_name,
                      pr.name as producer, c.name as category,
                      p.stock_quantity, p.price_uzs, p.price_usd,
                      p.stock_updated_at, p.stock_last_seen_at
               FROM products p
               JOIN producers pr ON pr.id = p.producer_id
               JOIN categories c ON c.id = p.category_id
               WHERE p.is_active = 1
                 AND (p.stock_last_seen_at IS NULL
                      OR datetime(p.stock_last_seen_at) < datetime(?, '-5 minutes'))
               ORDER BY p.stock_last_seen_at ASC, p.name""",
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


# ── Inventory Intelligence v2 — weekly tugagan ─────────────────


def _week_bounds_tashkent(weeks_back: int = 0):
    """Return (monday_utc_str, monday_tk_date_str, sunday_tk_date_str) for the
    work week N weeks before this Monday. weeks_back=0 → this week, 1 → last week.
    UTC string is `YYYY-MM-DD HH:MM:SS` for `stockout_at` comparisons; Tashkent
    date strings are `YYYY-MM-DD` for `real_orders.doc_date` comparisons.

    Duplicated in admin_revenue.py — `/top-sellers-*` keep their own copy
    so neither router has to import the other.
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
            # Also wipe the JSON backup; otherwise /api/users/check's
            # fallback path re-inserts the user from there on next call.
            from backend.services.backup_users import remove_user_from_backup
            backup_removed = remove_user_from_backup(telegram_id)
        else:
            cur2 = conn.execute(
                "UPDATE users SET agent_role = NULL, is_agent = 0 "
                "WHERE telegram_id = ?",
                (telegram_id,),
            )
            users_action = "agent_role_cleared"
            backup_removed = False
        conn.commit()
        return {
            "ok": True,
            "telegram_id": telegram_id,
            "full_reset": full_reset,
            "pending_deleted": cur1.rowcount,
            "users_action": users_action,
            "users_affected": cur2.rowcount,
            "backup_removed": backup_removed,
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


@router.get("/hmac-failures")
def hmac_failures(
    admin_key: str = Query(...),
    days: int = Query(14, ge=1, le=90),
    limit: int = Query(100, ge=1, le=500),
):
    """Recent initData HMAC validation failures from the user-auth gate.
    Trigger to escalate to Phase B (cover read endpoints): non-zero rows here.
    """
    _check_admin(admin_key)
    conn = get_db()
    try:
        summary = conn.execute(
            f"""SELECT reason, COUNT(*) as count
                FROM hmac_audit_log
                WHERE created_at >= datetime('now', '-{int(days)} days')
                GROUP BY reason
                ORDER BY count DESC"""
        ).fetchall()
        recent = conn.execute(
            f"""SELECT id, claimed_telegram_id, parsed_telegram_id, path, reason, created_at
                FROM hmac_audit_log
                WHERE created_at >= datetime('now', '-{int(days)} days')
                ORDER BY id DESC
                LIMIT ?""",
            (limit,),
        ).fetchall()
        return {
            "days": days,
            "summary": [dict(r) for r in summary],
            "recent": [dict(r) for r in recent],
        }
    finally:
        conn.close()
