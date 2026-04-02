"""Admin dashboard API — internal analytics for Rassvet's decision-makers.

Endpoints:
  /api/admin/revenue         — monthly revenue trend (period_debit by currency)
  /api/admin/collections     — collection rate trend (period_credit / period_debit)
  /api/admin/top-clients     — ranked by total period_debit
  /api/admin/receivables     — total outstanding + aging estimate
  /api/admin/client/{id}/history — per-client 15-month chart data
  /api/admin/stock-status    — stock overview + demand signals
  /api/admin/search-insights — top searches, zero-results, trending
  /api/admin/platform-health — registration funnel, order volume, data quality
"""
from fastapi import APIRouter, Query, HTTPException
from backend.database import get_db

router = APIRouter(prefix="/api/admin", tags=["admin"])

ADMIN_KEY = "rassvet2026"


def _check_admin(admin_key: str):
    if admin_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ── Revenue Trend ────────────────────────────────────────────────

@router.get("/revenue")
def revenue_trend(admin_key: str = Query(...)):
    """Monthly revenue trend: SUM(period_debit) by month, by currency.
    Also returns period_credit (collections) for each month.
    """
    _check_admin(admin_key)
    conn = get_db()

    rows = conn.execute("""
        SELECT period_start, currency,
               SUM(period_debit) as total_shipments,
               SUM(period_credit) as total_collections,
               COUNT(DISTINCT client_name_1c) as active_clients
        FROM client_balances
        WHERE period_debit > 0 OR period_credit > 0
        GROUP BY period_start, currency
        ORDER BY period_start ASC
    """).fetchall()

    conn.close()

    # Organize by currency
    result = {"UZS": [], "USD": []}
    for r in rows:
        cur = r["currency"]
        if cur not in result:
            result[cur] = []
        result[cur].append({
            "period": r["period_start"],
            "shipments": round(r["total_shipments"], 2),
            "collections": round(r["total_collections"], 2),
            "active_clients": r["active_clients"],
        })

    return {"ok": True, "data": result}


# ── Collection Rate ──────────────────────────────────────────────

@router.get("/collections")
def collection_rate(admin_key: str = Query(...)):
    """Collection rate by month: period_credit / period_debit.
    Shows how much of shipped goods were paid for each month.
    """
    _check_admin(admin_key)
    conn = get_db()

    rows = conn.execute("""
        SELECT period_start, currency,
               SUM(period_debit) as total_debit,
               SUM(period_credit) as total_credit
        FROM client_balances
        GROUP BY period_start, currency
        ORDER BY period_start ASC
    """).fetchall()

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


# ── Top Clients ──────────────────────────────────────────────────

@router.get("/top-clients")
def top_clients(
    admin_key: str = Query(...),
    currency: str = Query("UZS"),
    limit: int = Query(20, ge=1, le=100),
):
    """Top clients ranked by total period_debit (shipments = revenue proxy).
    Also shows total collections and current balance.
    """
    _check_admin(admin_key)
    conn = get_db()

    rows = conn.execute("""
        SELECT
            client_name_1c,
            SUM(period_debit) as total_shipped,
            SUM(period_credit) as total_paid,
            COUNT(DISTINCT period_start) as months_active
        FROM client_balances
        WHERE currency = ?
        GROUP BY client_name_1c
        ORDER BY total_shipped DESC
        LIMIT ?
    """, (currency, limit)).fetchall()

    # Get current balance (latest period) for each top client
    clients = []
    for r in rows:
        # Get latest closing balance
        latest = conn.execute("""
            SELECT closing_debit, closing_credit, period_start
            FROM client_balances
            WHERE client_name_1c = ? AND currency = ?
            ORDER BY period_start DESC
            LIMIT 1
        """, (r["client_name_1c"], currency)).fetchone()

        balance = 0
        latest_period = ""
        if latest:
            balance = (latest["closing_debit"] or 0) - (latest["closing_credit"] or 0)
            latest_period = latest["period_start"]

        clients.append({
            "name": r["client_name_1c"],
            "total_shipped": round(r["total_shipped"], 2),
            "total_paid": round(r["total_paid"], 2),
            "balance": round(balance, 2),
            "months_active": r["months_active"],
            "latest_period": latest_period,
        })

    conn.close()
    return {"ok": True, "currency": currency, "clients": clients}


# ── Receivables ──────────────────────────────────────────────────

@router.get("/receivables")
def receivables(
    admin_key: str = Query(...),
    currency: str = Query("UZS"),
):
    """Total receivables (outstanding debt) + aging estimate.

    Aging: checks how many months a client's closing balance has been
    roughly the same (i.e., debt hasn't been paid down).
    Buckets: 0-30d (current month), 30-60d, 60-90d, 90+ days.
    """
    _check_admin(admin_key)
    conn = get_db()

    # Get latest period
    latest_period = conn.execute(
        "SELECT MAX(period_start) FROM client_balances WHERE currency = ?",
        (currency,)
    ).fetchone()[0]

    if not latest_period:
        conn.close()
        return {"ok": True, "total_receivable": 0, "aging": {}, "currency": currency}

    # Get all clients with positive balance in latest period
    rows = conn.execute("""
        SELECT client_name_1c,
               closing_debit - closing_credit as balance
        FROM client_balances
        WHERE period_start = ? AND currency = ?
          AND (closing_debit - closing_credit) > 0
        ORDER BY balance DESC
    """, (latest_period, currency)).fetchall()

    total_receivable = 0
    aging = {"current": 0, "30_60": 0, "60_90": 0, "90_plus": 0}
    client_count = {"current": 0, "30_60": 0, "60_90": 0, "90_plus": 0}

    # Get all periods in order for aging estimation
    periods = conn.execute(
        "SELECT DISTINCT period_start FROM client_balances WHERE currency = ? ORDER BY period_start DESC",
        (currency,)
    ).fetchall()
    period_list = [p["period_start"] for p in periods]

    for r in rows:
        balance = r["balance"]
        total_receivable += balance
        client_name = r["client_name_1c"]

        # Aging: check how many months this client has had period_credit = 0
        # (i.e., hasn't been paying)
        months_unpaid = 0
        history = conn.execute("""
            SELECT period_start, period_credit
            FROM client_balances
            WHERE client_name_1c = ? AND currency = ?
            ORDER BY period_start DESC
            LIMIT 6
        """, (client_name, currency)).fetchall()

        for h in history:
            if (h["period_credit"] or 0) == 0:
                months_unpaid += 1
            else:
                break

        if months_unpaid <= 1:
            aging["current"] += balance
            client_count["current"] += 1
        elif months_unpaid <= 2:
            aging["30_60"] += balance
            client_count["30_60"] += 1
        elif months_unpaid <= 3:
            aging["60_90"] += balance
            client_count["60_90"] += 1
        else:
            aging["90_plus"] += balance
            client_count["90_plus"] += 1

    conn.close()

    return {
        "ok": True,
        "currency": currency,
        "latest_period": latest_period,
        "total_receivable": round(total_receivable, 2),
        "total_clients_with_debt": len(rows),
        "aging": {k: round(v, 2) for k, v in aging.items()},
        "aging_client_count": client_count,
    }


# ── Client History (drill-down) ──────────────────────────────────

@router.get("/client/{client_name}/history")
def client_history(
    client_name: str,
    admin_key: str = Query(...),
):
    """Per-client balance history — 15-month chart data.
    Uses the same pattern as get_client_balance_history but by name.
    """
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


# ── Stock Status ─────────────────────────────────────────────────

@router.get("/stock-status")
def stock_status(admin_key: str = Query(...)):
    """Stock overview + demand signals from search logs.

    Returns:
    - Stock distribution (in_stock, low_stock, out_of_stock, no_data)
    - Top products by app orders (order_items)
    - Demand-supply mismatch: products searched for but low/no stock
    """
    _check_admin(admin_key)
    conn = get_db()

    # Stock distribution
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

    # Top ordered products (from app orders)
    top_ordered = conn.execute("""
        SELECT oi.product_name, oi.producer_name,
               SUM(oi.quantity) as total_qty,
               COUNT(DISTINCT oi.order_id) as order_count
        FROM order_items oi
        JOIN orders o ON o.id = oi.order_id
        GROUP BY oi.product_name
        ORDER BY total_qty DESC
        LIMIT 20
    """).fetchall()

    # Most clicked products from search (demand signal)
    top_clicked = conn.execute("""
        SELECT p.name_display as name, pr.name as producer,
               p.stock_quantity,
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
               SUM(CASE WHEN p.image_path IS NOT NULL AND p.image_path != '' THEN 1 ELSE 0 END) as with_photo
        FROM products p
        JOIN categories c ON c.id = p.category_id
        WHERE p.is_active = 1
        GROUP BY c.id
        ORDER BY product_count DESC
    """).fetchall()

    conn.close()

    return {
        "ok": True,
        "stock_summary": {
            "total": total_products,
            "in_stock": in_stock,
            "low_stock": low_stock,
            "out_of_stock": out_of_stock,
            "no_data": no_data,
        },
        "top_ordered": [dict(r) for r in top_ordered],
        "top_clicked": [dict(r) for r in top_clicked],
        "categories": [dict(r) for r in categories],
    }


# ── Search Insights ──────────────────────────────────────────────

@router.get("/search-insights")
def search_insights(
    admin_key: str = Query(...),
    days: int = Query(30, ge=1, le=365),
):
    """Search analytics summary: top queries, zero-results, trending.
    Consumes existing search_logs data.
    """
    _check_admin(admin_key)
    conn = get_db()

    # Overview stats
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

    # Top queries
    top_queries = conn.execute("""
        SELECT query, COUNT(*) as count,
               ROUND(AVG(results_count), 1) as avg_results
        FROM search_logs
        WHERE created_at >= datetime('now', ?)
        GROUP BY query
        ORDER BY count DESC
        LIMIT 20
    """, (f"-{days} days",)).fetchall()

    # Zero-result queries (unmet demand)
    zero_results = conn.execute("""
        SELECT query, COUNT(*) as count,
               COUNT(DISTINCT telegram_id) as unique_users
        FROM search_logs
        WHERE results_count = 0 AND created_at >= datetime('now', ?)
        GROUP BY query
        ORDER BY count DESC
        LIMIT 20
    """, (f"-{days} days",)).fetchall()

    # Daily search volume trend
    daily_volume = conn.execute("""
        SELECT DATE(created_at) as day,
               COUNT(*) as searches,
               COUNT(DISTINCT telegram_id) as users,
               SUM(CASE WHEN results_count = 0 THEN 1 ELSE 0 END) as zero_results
        FROM search_logs
        WHERE created_at >= datetime('now', ?)
        GROUP BY DATE(created_at)
        ORDER BY day ASC
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
    """Platform health metrics: registration funnel, order volume,
    data quality scores, app adoption.
    """
    _check_admin(admin_key)
    conn = get_db()

    # Registration funnel
    total_registered = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    total_approved = conn.execute(
        "SELECT COUNT(*) FROM users WHERE is_approved = 1"
    ).fetchone()[0]

    # Users who have placed at least one order
    users_with_orders = conn.execute(
        "SELECT COUNT(DISTINCT telegram_id) FROM orders"
    ).fetchone()[0]

    # Users with 2+ orders (repeat customers)
    repeat_users = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT telegram_id, COUNT(*) as cnt
            FROM orders
            GROUP BY telegram_id
            HAVING cnt >= 2
        )
    """).fetchone()[0]

    # Total allowed clients (whitelist)
    total_whitelist = conn.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]

    # Order volume trend (monthly)
    order_trend = conn.execute("""
        SELECT strftime('%Y-%m', created_at) as month,
               COUNT(*) as order_count,
               COUNT(DISTINCT telegram_id) as unique_buyers,
               SUM(total_usd) as total_usd,
               SUM(total_uzs) as total_uzs,
               SUM(item_count) as total_items
        FROM orders
        GROUP BY month
        ORDER BY month ASC
    """).fetchall()

    # Data quality scores
    total_products = conn.execute(
        "SELECT COUNT(*) FROM products WHERE is_active = 1"
    ).fetchone()[0]

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

    # Client data quality
    clients_with_1c_id = conn.execute(
        "SELECT COUNT(*) FROM allowed_clients WHERE client_id_1c IS NOT NULL AND client_id_1c != ''"
    ).fetchone()[0]

    clients_with_telegram = conn.execute(
        "SELECT COUNT(*) FROM allowed_clients WHERE matched_telegram_id IS NOT NULL"
    ).fetchone()[0]

    # Financial data coverage
    balance_clients = conn.execute(
        "SELECT COUNT(DISTINCT client_name_1c) FROM client_balances"
    ).fetchone()[0]

    balance_periods = conn.execute(
        "SELECT COUNT(DISTINCT period_start) FROM client_balances"
    ).fetchone()[0]

    # Recent activity (last 7 days)
    recent_orders = conn.execute(
        "SELECT COUNT(*) FROM orders WHERE created_at >= datetime('now', '-7 days')"
    ).fetchone()[0]

    recent_searches = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE created_at >= datetime('now', '-7 days')"
    ).fetchone()[0]

    recent_registrations = conn.execute(
        "SELECT COUNT(*) FROM users WHERE registered_at >= datetime('now', '-7 days')"
    ).fetchone()[0]

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
