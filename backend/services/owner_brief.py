"""Daily owner morning brief — yesterday's collections, shipments, anomalies.

Fires at 09:00 Tashkent. Recipients configured via OWNER_DAILY_BRIEF_TARGETS
env var (comma-separated chat IDs — negative for groups, positive for users).

Two clean halves:
    gather_brief(conn, *, for_date=None, debt_threshold_uzs=...) -> dict
        Pure SQL over existing tables. No Telegram concerns. Testable.

    render_brief(data, *, today=None) -> str
        Pure formatting. Takes the dict from gather_brief, produces the
        Telegram HTML message. No DB. Testable.

Quiet on a fully empty day (0 collections + 0 shipments + 0 anomalies) so
the father's morning chat isn't polluted by no-news pings.

All per-client aggregations filter out pseudo-accounts (`Наличка`,
`СТРОЙКА`, `ИСПРАВЛЕНИЕ`, etc.) via backend.services.pseudo_clients.
Without this filter, system-bucket accounts appear in top-clients,
debtors, and silent-regulars (Error Log #36 — LEGACY_HEURISTIC_CLIENT_FILTER).

Stock anomaly is scoped to "products that went out yesterday" via
products.stockout_at, NOT the bulk pool of products currently at 0
(which includes default-0 products that were never actually tracked).

Memory: see Notion Command Center → Feature backlog A2 for the spec rationale.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional

from backend.services.pseudo_clients import (
    sql_exclusion_clause as _pseudo_exclusion_clause,
    sql_exclusion_params as _pseudo_exclusion_params,
)


TASHKENT = timezone(timedelta(hours=5))

# Default thresholds — tunable. Override per-call from the caller.
DEFAULT_DEBT_THRESHOLD_UZS = 50_000_000.0  # 50M UZS = ~$4K — flag chunky debts
DEFAULT_AGING_BUCKETS_OVERDUE = ("aging_91_120", "aging_120_plus")  # "30+ days late"
DEFAULT_TOP_CLIENTS_N = 3
DEFAULT_TOP_DEBTORS_N = 3


def _tashkent_date_str(d: Optional[date] = None) -> str:
    """Return ISO date string for today (Tashkent) unless overridden."""
    if d is None:
        d = datetime.now(TASHKENT).date()
    return d.isoformat()


def _fmt_uzs(n: float) -> str:
    """Format a UZS amount with `'` thousand separators + suffix.
    Big numbers compact: 1,234,500,000 → '1.23B'; 50_000_000 → '50M'.
    Smaller numbers shown with separators.
    """
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f}B UZS"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M UZS"
    if n >= 1_000:
        # Use ` for visual separation in Telegram (more compact than `,`)
        return f"{int(n):,} UZS".replace(",", "'")
    return f"{int(n)} UZS"


def _fmt_usd(n: float) -> str:
    """Format USD amount — usually small integers in this app."""
    if n >= 1_000:
        return f"${int(n):,}".replace(",", "'")
    if n == int(n):
        return f"${int(n)}"
    return f"${n:.2f}"


def gather_brief(
    conn,
    *,
    for_date: Optional[date] = None,
    debt_threshold_uzs: float = DEFAULT_DEBT_THRESHOLD_UZS,
) -> dict:
    """Collect yesterday's reconciliation data.

    `for_date` defaults to yesterday-Tashkent. Useful to set explicitly for
    tests or for back-filling a missed brief on demand.

    Returns a dict with the shape expected by `render_brief()`. Always
    fully populated, even on a quiet day — render_brief decides what to
    show.
    """
    if for_date is None:
        for_date = datetime.now(TASHKENT).date() - timedelta(days=1)
    for_str = for_date.isoformat()

    # ── 1. Cash (kassa) — UZS leg via corr_account 40.10, USD via 40.11 ─
    # Mirrors backend/services/import_cash.py:_detect_currency. Strip non-digits
    # so '40.10.0' / '4010' all classify as UZS, '40.11' / '4011' as USD.
    cash_uzs_row = conn.execute(
        """SELECT COALESCE(SUM(amount_local), 0) AS total, COUNT(*) AS n
           FROM client_payments
           WHERE doc_date = ?
             AND REPLACE(REPLACE(corr_account, '.', ''), ' ', '') LIKE '4010%'""",
        (for_str,),
    ).fetchone()
    cash_usd_row = conn.execute(
        """SELECT COALESCE(SUM(amount_currency), 0) AS total, COUNT(*) AS n
           FROM client_payments
           WHERE doc_date = ?
             AND REPLACE(REPLACE(corr_account, '.', ''), ' ', '') LIKE '4011%'""",
        (for_str,),
    ).fetchone()

    # ── 2. Shipments (realizatsiya) — via realorders_revenue helper. The
    # `real_orders.currency` column is a 1C export quirk (always 'USD'), so
    # filtering by it silently zeros the UZS leg. See
    # backend/services/realorders_revenue.py for the full mechanism.
    from backend.services.realorders_revenue import realorders_revenue
    _ship = realorders_revenue(date_min=for_str, date_max=for_str, conn=conn)
    ship_uzs_row = {
        "total": _ship["uzs"],
        "n": _ship["uzs_only_docs"] + _ship["dual_docs"],
    }
    ship_usd_row = {
        "total": _ship["usd"],
        "n": _ship["usd_only_docs"] + _ship["dual_docs"],
    }

    # ── Pseudo-client filter (Error Log #36 — LEGACY_HEURISTIC_CLIENT_FILTER) ─
    # Every per-client aggregation below excludes pseudo-accounts via the
    # canonical filter from backend.services.pseudo_clients. Without this,
    # the morning brief was full of false signals (Наличка №3 "overdue 30+
    # days with 587M debt", СТРОЙКА as a "silent regular", etc. on 2026-05-11).
    pseudo_clause_cp = _pseudo_exclusion_clause("client_name_1c")
    pseudo_clause_cd = _pseudo_exclusion_clause("cd.client_name_1c")
    pseudo_clause_ro = _pseudo_exclusion_clause("client_name_1c")
    pseudo_params = list(_pseudo_exclusion_params())

    # ── 3. Top N clients by yesterday's cash receipts (UZS, real clients) ─
    top_clients = conn.execute(
        f"""SELECT client_name_1c AS name, SUM(amount_local) AS total_uzs,
                   COUNT(*) AS n
            FROM client_payments
            WHERE doc_date = ?
              AND client_name_1c IS NOT NULL
              AND TRIM(client_name_1c) != ''
              AND REPLACE(REPLACE(corr_account, '.', ''), ' ', '') LIKE '4010%'
              AND {pseudo_clause_cp}
            GROUP BY client_name_1c
            ORDER BY total_uzs DESC
            LIMIT ?""",
        [for_str] + pseudo_params + [DEFAULT_TOP_CLIENTS_N],
    ).fetchall()

    # ── 4. Anomaly A: top debtors overdue 30+ days above threshold ──────
    # Latest report_date per client; sum the 91+ aging buckets.
    overdue_rows = conn.execute(
        f"""SELECT cd.client_name_1c AS name,
                  cd.debt_uzs AS total_debt,
                  (cd.{DEFAULT_AGING_BUCKETS_OVERDUE[0]} + cd.{DEFAULT_AGING_BUCKETS_OVERDUE[1]}) AS overdue_91p
           FROM client_debts cd
           INNER JOIN (
               SELECT client_name_1c, MAX(report_date) AS latest
               FROM client_debts
               GROUP BY client_name_1c
           ) latest_cd
             ON latest_cd.client_name_1c = cd.client_name_1c
            AND latest_cd.latest = cd.report_date
           WHERE cd.debt_uzs > ?
             AND (cd.{DEFAULT_AGING_BUCKETS_OVERDUE[0]} + cd.{DEFAULT_AGING_BUCKETS_OVERDUE[1]}) > 0
             AND {pseudo_clause_cd}
           ORDER BY overdue_91p DESC
           LIMIT ?""",
        [debt_threshold_uzs] + pseudo_params + [DEFAULT_TOP_DEBTORS_N],
    ).fetchall()

    # ── 5. Anomaly B: NEW stockouts yesterday ───────────────────────────
    # Uses `stockout_at` (TEXT, set when stock first hit 0) NOT the bulk
    # "stock_quantity=0" pool, which includes default-0 products that were
    # never tracked. We want "fell to 0 yesterday" — actionable signal.
    # `stockout_at` may include time; date() normalizes to YYYY-MM-DD.
    out_of_stock_row = conn.execute(
        """SELECT COUNT(*) AS n
           FROM products
           WHERE is_active = 1
             AND stock_quantity = 0
             AND stockout_at IS NOT NULL
             AND date(stockout_at) = ?""",
        (for_str,),
    ).fetchone()
    out_of_stock_count = int(out_of_stock_row["n"]) if out_of_stock_row else 0

    # ── 6. Anomaly C: silent regulars (had ≥3 in last 7 days, 0 yesterday) ─
    # Uses real_orders as the activity signal (shipments — proxy for orders).
    week_ago_str = (for_date - timedelta(days=6)).isoformat()  # last 7 days incl. yesterday
    silent_rows = conn.execute(
        f"""SELECT recent.name, recent.recent_count
            FROM (
              SELECT client_name_1c AS name, COUNT(*) AS recent_count
              FROM real_orders
              WHERE doc_date BETWEEN ? AND ?
                AND client_name_1c IS NOT NULL
                AND TRIM(client_name_1c) != ''
                AND {pseudo_clause_ro}
              GROUP BY client_name_1c
              HAVING COUNT(*) >= 3
            ) recent
            LEFT JOIN (
              SELECT DISTINCT client_name_1c AS name
              FROM real_orders
              WHERE doc_date = ?
            ) yesterday_active
              ON yesterday_active.name = recent.name
            WHERE yesterday_active.name IS NULL
            ORDER BY recent.recent_count DESC
            LIMIT 3""",
        [week_ago_str, for_str] + pseudo_params + [for_str],
    ).fetchall()

    return {
        "for_date": for_str,
        "cash_uzs_total": float(cash_uzs_row["total"]) if cash_uzs_row else 0.0,
        "cash_uzs_count": int(cash_uzs_row["n"]) if cash_uzs_row else 0,
        "cash_usd_total": float(cash_usd_row["total"]) if cash_usd_row else 0.0,
        "cash_usd_count": int(cash_usd_row["n"]) if cash_usd_row else 0,
        "ship_uzs_total": float(ship_uzs_row["total"]) if ship_uzs_row else 0.0,
        "ship_uzs_count": int(ship_uzs_row["n"]) if ship_uzs_row else 0,
        "ship_usd_total": float(ship_usd_row["total"]) if ship_usd_row else 0.0,
        "ship_usd_count": int(ship_usd_row["n"]) if ship_usd_row else 0,
        "top_clients": [
            {"name": r["name"], "total_uzs": float(r["total_uzs"]), "n": int(r["n"])}
            for r in top_clients
        ],
        "overdue_debtors": [
            {"name": r["name"], "total_debt": float(r["total_debt"]),
             "overdue_91p": float(r["overdue_91p"])}
            for r in overdue_rows
        ],
        "out_of_stock_count": out_of_stock_count,
        "silent_regulars": [
            {"name": r["name"], "recent_count": int(r["recent_count"])}
            for r in silent_rows
        ],
    }


def is_quiet_day(data: dict) -> bool:
    """A day with zero collections, zero shipments, zero anomalies is
    quiet — no morning ping. Saves chat noise on holidays / Sundays."""
    return (
        data["cash_uzs_count"] == 0
        and data["cash_usd_count"] == 0
        and data["ship_uzs_count"] == 0
        and data["ship_usd_count"] == 0
        and not data["overdue_debtors"]
        and data["out_of_stock_count"] == 0
        and not data["silent_regulars"]
    )


def render_brief(data: dict, *, today: Optional[date] = None) -> str:
    """Format the brief as Telegram HTML.

    `today` defaults to today-Tashkent (so the header shows TODAY's date
    + 'yesterday: <for_date>'). Override for tests.
    """
    if today is None:
        today = datetime.now(TASHKENT).date()
    today_str = today.isoformat()
    for_str = data["for_date"]

    lines: list[str] = []
    lines.append(f"☀️ <b>Kunlik hisobot</b> — {today_str}")
    lines.append(f"<i>(kecha: {for_str})</i>")
    lines.append("")

    # ── Kassa ──
    lines.append("💵 <b>Kassa (tushum):</b>")
    if data["cash_uzs_count"] > 0:
        lines.append(
            f"   UZS: {_fmt_uzs(data['cash_uzs_total'])} "
            f"({data['cash_uzs_count']} to'lov)"
        )
    if data["cash_usd_count"] > 0:
        lines.append(
            f"   USD: {_fmt_usd(data['cash_usd_total'])} "
            f"({data['cash_usd_count']} to'lov)"
        )
    if data["cash_uzs_count"] == 0 and data["cash_usd_count"] == 0:
        lines.append("   <i>0 to'lov</i>")
    lines.append("")

    # ── Realizatsiya ──
    lines.append("🚚 <b>Realizatsiya (yuborildi):</b>")
    if data["ship_uzs_count"] > 0:
        lines.append(
            f"   UZS: {_fmt_uzs(data['ship_uzs_total'])} "
            f"({data['ship_uzs_count']} hujjat)"
        )
    if data["ship_usd_count"] > 0:
        lines.append(
            f"   USD: {_fmt_usd(data['ship_usd_total'])} "
            f"({data['ship_usd_count']} hujjat)"
        )
    if data["ship_uzs_count"] == 0 and data["ship_usd_count"] == 0:
        lines.append("   <i>0 hujjat</i>")
    lines.append("")

    # ── Top clients ──
    if data["top_clients"]:
        lines.append("🏆 <b>Eng yaxshi mijozlar (kecha):</b>")
        for i, c in enumerate(data["top_clients"], start=1):
            lines.append(f"   {i}. {c['name']} — {_fmt_uzs(c['total_uzs'])}")
        lines.append("")

    # ── Anomalies ──
    anomaly_lines: list[str] = []
    for d in data["overdue_debtors"]:
        anomaly_lines.append(
            f"   • <b>{d['name']}</b> 30+ kun to'lamadi — "
            f"qarz {_fmt_uzs(d['total_debt'])} "
            f"(eski: {_fmt_uzs(d['overdue_91p'])})"
        )
    if data["out_of_stock_count"] > 0:
        anomaly_lines.append(
            f"   • Kecha <b>{data['out_of_stock_count']} mahsulot</b> "
            f"0'ga tushdi (<code>/zakazlar</code>)"
        )
    for s in data["silent_regulars"]:
        anomaly_lines.append(
            f"   • Kecha 0 hujjat: <b>{s['name']}</b> "
            f"(oxirgi 7 kunda {s['recent_count']} ta)"
        )

    if anomaly_lines:
        lines.append(f"⚠️ <b>Diqqat ({len(anomaly_lines)} ta):</b>")
        lines.extend(anomaly_lines)

    return "\n".join(lines)
