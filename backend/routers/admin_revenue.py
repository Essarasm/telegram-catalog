"""Admin revenue / top-clients / top-sellers / weekly-recap analytics.

Source-of-truth for the dashboard's clean-revenue tab:
- monthly revenue + collections trend (`/revenue`, `/collections`)
- USD-equivalent weekly recap with YoY (`/weekly-recap`)
- per-week top clients (`/top-clients-weekly`)
- cumulative top clients ranking (`/top-clients`)
- top sellers week-over-week + closed-period (`/top-sellers-wow`,
  `/top-sellers-period`)

Extracted from `admin.py` to keep that file under the 2,000-line god-module
canary. Endpoints kept on their original `/api/admin/...` URLs.
"""
from fastapi import APIRouter, HTTPException, Query

from backend.admin_auth import check_admin_key
from backend.database import get_db


router = APIRouter(prefix="/api/admin", tags=["admin"])


def _check_admin(admin_key: str):
    if not check_admin_key(admin_key):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ── Pseudo-account exclusion ─────────────────────────────────────
#
# Filter out cumulative records (pre-2025) and end-of-month partials
# (day != 01). Mirrors the constant in admin.py — duplicated rather
# than imported because the value is one short line and importing
# would re-couple the two routers.
_PERIOD_FILTER = "cb.period_start >= '2025-01-01' AND strftime('%d', cb.period_start) = '01'"

# Fallback FX rate when no daily_fx_rates row covers the period being
# looked at. Duplicated with admin_debtors.py — keep in sync if tuned.
FX_FALLBACK = 12000.0


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


# ── Weekly Recap (USD-equivalent, YoY) ──────────────────────────
#
# Source: real_orders (revenue / shipments) + client_payments (collections).
# Replaces client_balances for owner-facing weekly view — daily 1C uploads
# from 2026-04-13+ broke the day=01 monthly assumption, and weekly cadence
# matches how the owner consumes the data.
#
# UZS+USD legs converted to USD-equivalent using each week's AVG(daily_fx_rates).
# Falls back to FX_FALLBACK (12,000) when no rates exist for that week
# (covers all YoY 2025 weeks and the first ~9 weeks of 2026 — UZS/USD has
# been ±2% of 12,000 the whole period FX data exists, so this is a safe
# anchor). Each week is tagged with fx_source = "actual" or "fallback" so
# the chart can footnote it.
#
# YoY shift = exactly 364 days (52 weeks) to preserve Mon-Sun alignment.


def _closed_week_bounds_tashkent(weeks_back: int):
    """Return (monday_date_str, sunday_date_str) for a closed week N weeks
    before this Monday. weeks_back=1 → last closed week (most recent Sun-end).

    The "current" in-progress week (today's week) is weeks_back=0; charts
    skip it so only fully-closed weeks are shown.
    """
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    tk = ZoneInfo("Asia/Tashkent")
    now_tk = datetime.now(tk)
    monday_tk = (now_tk - timedelta(days=now_tk.weekday() + 7 * weeks_back)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    sunday_tk = monday_tk + timedelta(days=6)
    return monday_tk.strftime("%Y-%m-%d"), sunday_tk.strftime("%Y-%m-%d")


def _yoy_week_bounds(week_start_iso: str, week_end_iso: str):
    """Shift the given week back 364 days (= exactly 52 weeks).
    Keeps Mon-Sun alignment intact.
    """
    from datetime import date, timedelta
    ws = date.fromisoformat(week_start_iso) - timedelta(days=364)
    we = date.fromisoformat(week_end_iso) - timedelta(days=364)
    return ws.isoformat(), we.isoformat()


def _week_avg_fxrate(conn, start_iso: str, end_iso: str):
    """Return (rate, source) where source is 'actual' if any FX rows fall
    within [start_iso, end_iso] (inclusive), else ('fallback', FX_FALLBACK)."""
    row = conn.execute(
        """SELECT AVG(rate) as avg_rate, COUNT(*) as n
           FROM daily_fx_rates
           WHERE currency_pair = 'USD_UZS'
             AND rate_date BETWEEN ? AND ?""",
        (start_iso, end_iso),
    ).fetchone()
    if row and row["n"] and row["avg_rate"]:
        return float(row["avg_rate"]), "actual"
    return FX_FALLBACK, "fallback"


def _aggregate_week(conn, start_iso: str, end_iso: str, excl_clause_ro: str,
                    excl_clause_cp: str, excl_params: tuple):
    """Aggregate revenue (real_orders) + collections (client_payments) over a
    Mon-Sun week. Returns dict with native UZS/USD legs, order count, and
    distinct active client count.
    """
    rev = conn.execute(
        f"""SELECT COALESCE(SUM(total_sum), 0) as uzs,
                   COALESCE(SUM(total_sum_currency), 0) as usd,
                   COUNT(*) as orders,
                   COUNT(DISTINCT COALESCE(client_id, client_name_1c)) as clients
              FROM real_orders ro
             WHERE doc_date BETWEEN ? AND ?
               {excl_clause_ro}""",
        (start_iso, end_iso, *excl_params),
    ).fetchone()

    coll = conn.execute(
        f"""SELECT COALESCE(SUM(CASE WHEN currency = 'UZS' THEN amount_local ELSE 0 END), 0) as uzs,
                   COALESCE(SUM(CASE WHEN currency = 'USD' THEN amount_currency ELSE 0 END), 0) as usd,
                   COUNT(*) as pays
              FROM client_payments cp
             WHERE doc_date BETWEEN ? AND ?
               {excl_clause_cp}""",
        (start_iso, end_iso, *excl_params),
    ).fetchone()

    return {
        "revenue_uzs": float(rev["uzs"] or 0),
        "revenue_usd": float(rev["usd"] or 0),
        "collections_uzs": float(coll["uzs"] or 0),
        "collections_usd": float(coll["usd"] or 0),
        "order_count": int(rev["orders"] or 0),
        "active_clients": int(rev["clients"] or 0),
    }


def _format_week_label(start_iso: str, end_iso: str) -> str:
    """Human-readable label like 'May 4–10' (same month) or 'Apr 27–May 3'."""
    from datetime import date
    s = date.fromisoformat(start_iso)
    e = date.fromisoformat(end_iso)
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    if s.month == e.month:
        return f"{months[s.month - 1]} {s.day}–{e.day}"
    return f"{months[s.month - 1]} {s.day}–{months[e.month - 1]} {e.day}"


@router.get("/weekly-recap")
def weekly_recap(
    admin_key: str = Query(...),
    weeks: int = Query(13, ge=1, le=52),
    include_suppliers: bool = Query(False),
):
    """Weekly revenue + collections in USD-equivalent, with YoY comparison.

    Returns the most recent `weeks` closed Mon-Sun weeks (Tashkent), each with
    native UZS/USD legs, week-avg FX, USD-equivalent totals, and the same
    week 364 days earlier for YoY delta.

    Revenue source: real_orders (Realizatsiya). Collections: client_payments
    (Kassa). Pseudo-clients excluded by default.
    """
    from backend.services.pseudo_clients import (
        sql_exclusion_clause, sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()

    excl_clause_ro = "" if include_suppliers else f" AND {sql_exclusion_clause('ro.client_name_1c')}"
    excl_clause_cp = "" if include_suppliers else f" AND {sql_exclusion_clause('cp.client_name_1c')}"
    excl_params = () if include_suppliers else sql_exclusion_params()

    # Build list of (week_start, week_end) pairs — weeks_back=1 is the most
    # recent closed week, weeks_back=weeks is the oldest. We render oldest →
    # newest so the chart reads left-to-right chronologically.
    weeks_list = []
    for wb in range(weeks, 0, -1):
        ws, we = _closed_week_bounds_tashkent(wb)
        weeks_list.append((ws, we))

    fx_fallback_count = 0
    out = []
    for ws, we in weeks_list:
        cur = _aggregate_week(conn, ws, we, excl_clause_ro, excl_clause_cp, excl_params)
        fx_rate, fx_source = _week_avg_fxrate(conn, ws, we)
        if fx_source == "fallback":
            fx_fallback_count += 1

        revenue_usd_eq = cur["revenue_uzs"] / fx_rate + cur["revenue_usd"]
        collections_usd_eq = cur["collections_uzs"] / fx_rate + cur["collections_usd"]
        collection_rate = (collections_usd_eq / revenue_usd_eq * 100) if revenue_usd_eq > 0 else 0.0

        # YoY
        ws_y, we_y = _yoy_week_bounds(ws, we)
        prior = _aggregate_week(conn, ws_y, we_y, excl_clause_ro, excl_clause_cp, excl_params)
        fx_rate_y, fx_source_y = _week_avg_fxrate(conn, ws_y, we_y)
        yoy_revenue_usd_eq = prior["revenue_uzs"] / fx_rate_y + prior["revenue_usd"]
        yoy_collections_usd_eq = prior["collections_uzs"] / fx_rate_y + prior["collections_usd"]
        yoy_available = (prior["order_count"] + prior["revenue_uzs"] + prior["revenue_usd"]) > 0

        yoy_rev_delta = ((revenue_usd_eq - yoy_revenue_usd_eq) / yoy_revenue_usd_eq * 100) if yoy_revenue_usd_eq > 0 else None
        yoy_coll_delta = ((collections_usd_eq - yoy_collections_usd_eq) / yoy_collections_usd_eq * 100) if yoy_collections_usd_eq > 0 else None

        out.append({
            "week_start": ws,
            "week_end": we,
            "label": _format_week_label(ws, we),
            "revenue_uzs_native": round(cur["revenue_uzs"], 2),
            "revenue_usd_native": round(cur["revenue_usd"], 2),
            "revenue_usd_eq": round(revenue_usd_eq, 2),
            "collections_uzs_native": round(cur["collections_uzs"], 2),
            "collections_usd_native": round(cur["collections_usd"], 2),
            "collections_usd_eq": round(collections_usd_eq, 2),
            "collection_rate_pct": round(collection_rate, 1),
            "order_count": cur["order_count"],
            "active_clients": cur["active_clients"],
            "fx_rate": round(fx_rate, 2),
            "fx_source": fx_source,
            "yoy": {
                "week_start": ws_y,
                "week_end": we_y,
                "revenue_usd_eq": round(yoy_revenue_usd_eq, 2),
                "collections_usd_eq": round(yoy_collections_usd_eq, 2),
                "revenue_delta_pct": round(yoy_rev_delta, 1) if yoy_rev_delta is not None else None,
                "collections_delta_pct": round(yoy_coll_delta, 1) if yoy_coll_delta is not None else None,
                "fx_source": fx_source_y,
                "available": yoy_available,
            },
        })

    conn.close()

    return {
        "ok": True,
        "weeks_back": weeks,
        "weeks": out,
        "fx_fallback_rate": FX_FALLBACK,
        "fx_fallback_count": fx_fallback_count,
        "currency_basis": "USD-equivalent (week-avg FX)",
    }


# ── Top Clients (per-week, USD-eq + native UZS/USD legs) ─────────


@router.get("/top-clients-weekly")
def top_clients_weekly(
    admin_key: str = Query(...),
    weeks_back: int = Query(1, ge=1, le=13),
    limit: int = Query(50, ge=1, le=200),
    include_suppliers: bool = Query(False),
):
    """Top clients for a single closed Mon-Sun week (Tashkent).

    Source: real_orders (shipped) + client_payments (paid), pseudo-filter on.
    Each client returns native UZS + native USD legs for both shipped and paid,
    plus the USD-eq sum (week-avg FX). Ranking by shipped_usd_eq descending.

    Also returns three top-of-tab KPIs in USD-eq:
      - total_receivable_usd_eq (current outstanding from client_debts)
      - top_client_shipped_usd_eq (rank #1 in this week)
      - net_balance_usd_eq_week (Σ shipped − Σ paid across the top-N this week)
    """
    from backend.services.pseudo_clients import (
        sql_exclusion_clause,
        sql_exclusion_params,
    )

    _check_admin(admin_key)
    conn = get_db()

    ws, we = _closed_week_bounds_tashkent(weeks_back)
    fx_rate, fx_source = _week_avg_fxrate(conn, ws, we)

    excl_clause_ro = "" if include_suppliers else f" AND {sql_exclusion_clause('ro.client_name_1c')}"
    excl_clause_cp = "" if include_suppliers else f" AND {sql_exclusion_clause('cp.client_name_1c')}"
    excl_clause_cd = "" if include_suppliers else f" AND {sql_exclusion_clause('cd.client_name_1c')}"
    excl_params = () if include_suppliers else sql_exclusion_params()

    shipped_rows = conn.execute(
        f"""SELECT ro.client_name_1c AS name,
                   COALESCE(SUM(ro.total_sum), 0) AS uzs,
                   COALESCE(SUM(ro.total_sum_currency), 0) AS usd
              FROM real_orders ro
             WHERE ro.doc_date BETWEEN ? AND ?
               {excl_clause_ro}
             GROUP BY ro.client_name_1c""",
        (ws, we, *excl_params),
    ).fetchall()

    paid_rows = conn.execute(
        f"""SELECT cp.client_name_1c AS name,
                   COALESCE(SUM(CASE WHEN cp.currency='UZS' THEN cp.amount_local ELSE 0 END), 0) AS uzs,
                   COALESCE(SUM(CASE WHEN cp.currency='USD' THEN cp.amount_currency ELSE 0 END), 0) AS usd
              FROM client_payments cp
             WHERE cp.doc_date BETWEEN ? AND ?
               {excl_clause_cp}
             GROUP BY cp.client_name_1c""",
        (ws, we, *excl_params),
    ).fetchall()

    by_name: dict = {}
    for r in shipped_rows:
        by_name[r["name"]] = {
            "shipped_uzs": float(r["uzs"] or 0),
            "shipped_usd": float(r["usd"] or 0),
            "paid_uzs": 0.0,
            "paid_usd": 0.0,
        }
    for r in paid_rows:
        d = by_name.setdefault(r["name"], {
            "shipped_uzs": 0.0, "shipped_usd": 0.0,
            "paid_uzs": 0.0, "paid_usd": 0.0,
        })
        d["paid_uzs"] = float(r["uzs"] or 0)
        d["paid_usd"] = float(r["usd"] or 0)

    clients_all = []
    for name, d in by_name.items():
        shipped_usd_eq = d["shipped_usd"] + (d["shipped_uzs"] / fx_rate if fx_rate else 0)
        paid_usd_eq = d["paid_usd"] + (d["paid_uzs"] / fx_rate if fx_rate else 0)
        pay_pct = round(paid_usd_eq / shipped_usd_eq * 100, 1) if shipped_usd_eq > 0 else None
        clients_all.append({
            "client_name": name,
            "shipped_uzs": round(d["shipped_uzs"], 2),
            "shipped_usd": round(d["shipped_usd"], 2),
            "shipped_usd_eq": round(shipped_usd_eq, 2),
            "paid_uzs": round(d["paid_uzs"], 2),
            "paid_usd": round(d["paid_usd"], 2),
            "paid_usd_eq": round(paid_usd_eq, 2),
            "pay_pct": pay_pct,
        })

    clients_all.sort(key=lambda c: -c["shipped_usd_eq"])
    top = clients_all[:limit]

    top_client_shipped_usd_eq = top[0]["shipped_usd_eq"] if top else 0.0
    net_balance_usd_eq_week = sum(c["shipped_usd_eq"] - c["paid_usd_eq"] for c in top)

    # Total receivable: latest client_debts snapshot, USD-eq via current week's FX.
    report_date = conn.execute(
        "SELECT MAX(report_date) FROM client_debts"
    ).fetchone()[0]
    total_receivable_usd_eq = 0.0
    if report_date:
        row = conn.execute(
            f"""SELECT COALESCE(SUM(debt_uzs), 0) AS uzs,
                       COALESCE(SUM(debt_usd), 0) AS usd
                  FROM client_debts cd
                 WHERE cd.report_date = ?
                   {excl_clause_cd}""",
            (report_date, *excl_params),
        ).fetchone()
        total_receivable_usd_eq = float(row["usd"] or 0) + (
            float(row["uzs"] or 0) / fx_rate if fx_rate else 0
        )

    conn.close()

    return {
        "ok": True,
        "week_start": ws,
        "week_end": we,
        "week_label": _format_week_label(ws, we),
        "fx_rate": fx_rate,
        "fx_source": fx_source,
        "weeks_back": weeks_back,
        "count": len(top),
        "kpis": {
            "total_receivable_usd_eq": round(total_receivable_usd_eq, 2),
            "top_client_shipped_usd_eq": round(top_client_shipped_usd_eq, 2),
            "net_balance_usd_eq_week": round(net_balance_usd_eq_week, 2),
        },
        "clients": top,
    }


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


# ── Inventory Intelligence v2 — top sellers WoW + period ─────────


def _week_bounds_tashkent(weeks_back: int = 0):
    """Return (monday_utc_str, monday_tk_date_str, sunday_tk_date_str) for the
    work week N weeks before this Monday. weeks_back=0 → this week, 1 → last week.
    UTC string is `YYYY-MM-DD HH:MM:SS` for `stockout_at` comparisons; Tashkent
    date strings are `YYYY-MM-DD` for `real_orders.doc_date` comparisons.

    Duplicated with admin.py — `/inventory-week-out` keeps its own copy
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
