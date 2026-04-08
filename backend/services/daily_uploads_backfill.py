"""Historical backfill for the daily_uploads table.

The daily_uploads tracking started landing on 2026-04-08 with Session F's
renewal. Prior to that, ops were uploading via the existing commands
(/balances, /stock, /prices, /debtors, /realorders) but no checklist rows
existed. This service reconstructs those rows by *inferring* activity from
the downstream data tables:

    balances_uzs   ← client_balances     (imported_at day, currency='UZS')
    balances_usd   ← client_balances     (imported_at day, currency='USD')
    stock          ← products            (any activity — very weak signal,
                                          in practice we can only set 'done'
                                          for dates we have no signal for;
                                          skipped: no per-day footprint)
    prices         ← products            (same — weak signal, skipped)
    debtors        ← client_debts        (report_date / imported_at)
    realorders     ← real_orders         (imported_at day)
    cash           ← client_payments     (imported_at day; counts distinct
                                          imported_at hour-buckets so morning
                                          + evening files each contribute 1)
    fxrate         ← daily_fx_rates      (rate_date)

Since `products` is overwritten in place with no history column, /stock and
/prices cannot be reliably backfilled. We document this and leave those
historical rows as 'pending' — ops can /skipupload them retrospectively if
they want the dashboard clean.

All rows written by this service carry:
    uploaded_by_name = 'historical_backfill'
    notes            = 'Inferred from <table> on <inferred_date>'

Idempotent: never overwrites a row that already has a real uploader
(uploaded_by_name IS NOT NULL AND uploaded_by_name != 'historical_backfill').
Re-running is safe — only missing rows or existing backfill rows are touched.

Range: 2026-04-01 through yesterday (Asia/Tashkent). Sundays and registered
holidays are skipped entirely.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from backend.database import get_db
from backend.services.daily_uploads import (
    TASHKENT,
    tashkent_today,
    is_holiday,
    is_required_weekday,
    tashkent_now_str,
)

logger = logging.getLogger(__name__)

BACKFILL_START = date(2026, 4, 1)
BACKFILL_MARKER = "historical_backfill"


def _iter_dates(start: date, end_inclusive: date):
    d = start
    while d <= end_inclusive:
        yield d
        d = d + timedelta(days=1)


def _day_bounds(day_str: str) -> Tuple[str, str]:
    """Return ('YYYY-MM-DD 00:00:00', 'YYYY-MM-DD 23:59:59') for SQL BETWEEN."""
    return f"{day_str} 00:00:00", f"{day_str} 23:59:59"


def _infer_activity(conn) -> Dict[str, Dict[str, dict]]:
    """Aggregate per-day activity for each upload type.

    Returns {upload_type: {day_str: {actual, row_count, first_source}}}.
    """
    out: Dict[str, Dict[str, dict]] = {
        "balances_uzs": {},
        "balances_usd": {},
        "debtors": {},
        "realorders": {},
        "cash": {},
        "fxrate": {},
    }

    # balances_uzs / balances_usd — from client_balances.imported_at day + currency
    for row in conn.execute(
        """SELECT date(imported_at) AS day, currency, COUNT(*) AS cnt
           FROM client_balances
           WHERE imported_at IS NOT NULL
           GROUP BY day, currency"""
    ).fetchall():
        day = row["day"]
        cur = (row["currency"] or "UZS").upper()
        bucket = "balances_usd" if cur == "USD" else "balances_uzs"
        if day:
            out[bucket][day] = {"actual": 1, "row_count": row["cnt"], "source": "client_balances"}

    # debtors — one import per day (any row for that day)
    for row in conn.execute(
        """SELECT date(imported_at) AS day, COUNT(*) AS cnt
           FROM client_debts
           WHERE imported_at IS NOT NULL
           GROUP BY day"""
    ).fetchall():
        if row["day"]:
            out["debtors"][row["day"]] = {
                "actual": 1,
                "row_count": row["cnt"],
                "source": "client_debts",
            }

    # realorders — day of imported_at
    for row in conn.execute(
        """SELECT date(imported_at) AS day, COUNT(*) AS cnt
           FROM real_orders
           WHERE imported_at IS NOT NULL
           GROUP BY day"""
    ).fetchall():
        if row["day"]:
            out["realorders"][row["day"]] = {
                "actual": 1,
                "row_count": row["cnt"],
                "source": "real_orders",
            }

    # cash — expected 2/day. Use distinct hour buckets on imported_at as a
    # coarse proxy for "how many separate uploads happened today".
    for row in conn.execute(
        """SELECT date(imported_at) AS day,
                  COUNT(DISTINCT substr(imported_at, 12, 2)) AS hour_buckets,
                  COUNT(*) AS cnt
           FROM client_payments
           WHERE imported_at IS NOT NULL
           GROUP BY day"""
    ).fetchall():
        if row["day"]:
            actual = max(1, min(2, int(row["hour_buckets"] or 1)))
            out["cash"][row["day"]] = {
                "actual": actual,
                "row_count": row["cnt"],
                "source": "client_payments",
            }

    # fxrate — one per rate_date
    for row in conn.execute(
        """SELECT rate_date AS day, COUNT(*) AS cnt
           FROM daily_fx_rates
           GROUP BY rate_date"""
    ).fetchall():
        if row["day"]:
            out["fxrate"][row["day"]] = {
                "actual": 1,
                "row_count": row["cnt"],
                "source": "daily_fx_rates",
            }

    return out


def _existing_row(conn, day: str, upload_type: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM daily_uploads WHERE upload_date=? AND upload_type=?",
        (day, upload_type),
    ).fetchone()
    return dict(row) if row else None


def _is_safe_to_overwrite(row: Optional[dict]) -> bool:
    """We only touch rows that don't exist yet, or were written by a
    previous backfill run. Real operator uploads are never clobbered."""
    if row is None:
        return True
    name = row.get("uploaded_by_name") or ""
    return name == "" or name == BACKFILL_MARKER


def run_backfill(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Reconstruct missing daily_uploads rows from downstream data.

    Args:
        start_date: YYYY-MM-DD (default 2026-04-01)
        end_date:   YYYY-MM-DD (default yesterday in Tashkent)
        dry_run:    If True, count what would change without writing.

    Returns a summary dict with per-type counts and a sample of affected days.
    """
    start = date.fromisoformat(start_date) if start_date else BACKFILL_START
    end = (
        date.fromisoformat(end_date) if end_date
        else (tashkent_today() - timedelta(days=1))
    )
    if end < start:
        return {"ok": False, "error": f"end_date {end} before start_date {start}"}

    conn = get_db()
    try:
        activity = _infer_activity(conn)

        sched_rows = {
            r["upload_type"]: dict(r)
            for r in conn.execute(
                "SELECT * FROM daily_upload_schedule WHERE is_active=1"
            ).fetchall()
        }

        now = tashkent_now_str()
        inserted_by_type: Dict[str, int] = {k: 0 for k in sched_rows}
        updated_by_type: Dict[str, int] = {k: 0 for k in sched_rows}
        skipped_days = 0
        total_days = 0
        sample_days: List[str] = []

        for d in _iter_dates(start, end):
            total_days += 1
            day_str = d.isoformat()

            # Skip Sundays and holidays for all types.
            if d.isoweekday() == 7 or is_holiday(day_str, conn):
                skipped_days += 1
                continue

            day_touched = False

            for upload_type, sched in sched_rows.items():
                if not is_required_weekday(d, sched["required_weekdays"]):
                    continue

                existing = _existing_row(conn, day_str, upload_type)
                if not _is_safe_to_overwrite(existing):
                    continue

                info = activity.get(upload_type, {}).get(day_str)
                expected = int(sched.get("expected_count_per_day", 1) or 1)

                if info:
                    actual = int(info["actual"])
                    row_count = int(info["row_count"])
                    status = "done" if actual >= expected else "pending"
                    notes = f"Inferred from {info['source']}"
                else:
                    # No downstream signal for stock/prices/etc. — leave as
                    # pending so ops can /skipupload it if desired. Skip the
                    # write entirely when there's nothing to say.
                    continue

                if dry_run:
                    if existing:
                        updated_by_type[upload_type] += 1
                    else:
                        inserted_by_type[upload_type] += 1
                    day_touched = True
                    continue

                if existing:
                    conn.execute(
                        """UPDATE daily_uploads
                           SET status=?, actual_count=?, row_count=?,
                               uploaded_at=COALESCE(uploaded_at, ?),
                               uploaded_by_name=?,
                               notes=?,
                               updated_at=?
                           WHERE upload_date=? AND upload_type=?""",
                        (
                            status, actual, row_count,
                            now, BACKFILL_MARKER, notes, now,
                            day_str, upload_type,
                        ),
                    )
                    updated_by_type[upload_type] += 1
                else:
                    conn.execute(
                        """INSERT INTO daily_uploads
                           (upload_date, upload_type, status, actual_count,
                            uploaded_at, uploaded_by_name, row_count, notes, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            day_str, upload_type, status, actual,
                            now, BACKFILL_MARKER, row_count, notes, now,
                        ),
                    )
                    inserted_by_type[upload_type] += 1
                day_touched = True

            if day_touched and len(sample_days) < 10:
                sample_days.append(day_str)

        if not dry_run:
            conn.commit()

        total_inserted = sum(inserted_by_type.values())
        total_updated = sum(updated_by_type.values())

        return {
            "ok": True,
            "dry_run": dry_run,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "total_days": total_days,
            "skipped_days_sun_holiday": skipped_days,
            "total_inserted": total_inserted,
            "total_updated": total_updated,
            "inserted_by_type": inserted_by_type,
            "updated_by_type": updated_by_type,
            "sample_days": sample_days,
        }
    finally:
        conn.close()
