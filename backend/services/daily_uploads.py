"""Daily Upload Checklist tracking service.

Session F renewal (2026-04-08). Every successful data upload in the bot
(/balances, /stock, /prices, /debtors, /realorders, /cash, /fxrate) calls
``record_upload`` to bump the counter for today's row in ``daily_uploads``.
Rows are created lazily if missing.

``render_today_checklist`` produces the formatted text shown by /today and by
the scheduled 10:00 / 17:00 reminders.

All date math uses Asia/Tashkent — we NEVER look at UTC for user-facing times.
"""

from __future__ import annotations

import json
from datetime import datetime, date, timedelta
from typing import Optional, List, Tuple
from zoneinfo import ZoneInfo

from backend.database import get_db

TASHKENT = ZoneInfo("Asia/Tashkent")

# Canonical upload_type values. Must match the seed rows in database.py.
VALID_UPLOAD_TYPES = {
    "balances_uzs", "balances_usd", "stock", "prices",
    "debtors", "realorders", "cash", "fxrate", "supply",
}


# ── Time helpers ─────────────────────────────────────────────────────────


def tashkent_now() -> datetime:
    """Current timestamp in Asia/Tashkent."""
    return datetime.now(TASHKENT)


def tashkent_today() -> date:
    """Today's date in Asia/Tashkent."""
    return tashkent_now().date()


def tashkent_today_str() -> str:
    """Today's date (Asia/Tashkent) as ISO YYYY-MM-DD."""
    return tashkent_today().isoformat()


def tashkent_now_str() -> str:
    """Current Tashkent time as ISO string (no tz suffix, matches SQLite datetime())."""
    return tashkent_now().strftime("%Y-%m-%d %H:%M:%S")


# ── Holiday + weekday rules ─────────────────────────────────────────────


def is_holiday(target_date: date | str, conn=None) -> Optional[str]:
    """Return holiday name if the given date is a registered holiday, else None."""
    if isinstance(target_date, date):
        target_date = target_date.isoformat()
    own = conn is None
    if own:
        conn = get_db()
    try:
        row = conn.execute(
            "SELECT name FROM holidays WHERE holiday_date = ?", (target_date,)
        ).fetchone()
        return row["name"] if row else None
    finally:
        if own:
            conn.close()


def is_required_weekday(target_date: date, required_weekdays: str) -> bool:
    """required_weekdays is a CSV of ISO weekday numbers (Mon=1 … Sun=7)."""
    iso_weekday = target_date.isoweekday()
    allowed = {int(x) for x in required_weekdays.split(",") if x.strip()}
    return iso_weekday in allowed


# ── Core tracking API ──────────────────────────────────────────────────


def ensure_row(upload_type: str, upload_date: str, conn) -> dict:
    """Fetch (or create) today's row for upload_type. Returns the row as a dict."""
    row = conn.execute(
        "SELECT * FROM daily_uploads WHERE upload_date = ? AND upload_type = ?",
        (upload_date, upload_type),
    ).fetchone()
    if row:
        return dict(row)
    conn.execute(
        """INSERT INTO daily_uploads (upload_date, upload_type, status, actual_count, updated_at)
           VALUES (?, ?, 'pending', 0, ?)""",
        (upload_date, upload_type, tashkent_now_str()),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM daily_uploads WHERE upload_date = ? AND upload_type = ?",
        (upload_date, upload_type),
    ).fetchone()
    return dict(row)


def record_upload(
    upload_type: str,
    user_id: Optional[int] = None,
    user_name: Optional[str] = None,
    file_name: Optional[str] = None,
    row_count: int = 0,
    upload_date: Optional[str] = None,
    notes: Optional[str] = None,
) -> dict:
    """Record a successful upload. Idempotent per file_name if you want — we always
    increment actual_count on each call because the same file uploaded twice is a
    rare operator mistake and double-counting is better than silently swallowing.

    Returns a small dict with the resulting state (status, actual_count, expected).
    """
    if upload_type not in VALID_UPLOAD_TYPES:
        raise ValueError(f"Unknown upload_type: {upload_type}")

    upload_date = upload_date or tashkent_today_str()
    now = tashkent_now_str()

    conn = get_db()
    try:
        row = ensure_row(upload_type, upload_date, conn)
        sched = conn.execute(
            "SELECT expected_count_per_day FROM daily_upload_schedule WHERE upload_type = ?",
            (upload_type,),
        ).fetchone()
        expected = sched["expected_count_per_day"] if sched else 1

        # Build new file list
        existing_files: List[str] = []
        if row.get("file_names"):
            try:
                existing_files = json.loads(row["file_names"])
                if not isinstance(existing_files, list):
                    existing_files = []
            except Exception:
                existing_files = []
        if file_name:
            existing_files.append(file_name)

        new_count = (row["actual_count"] or 0) + 1
        new_row_count = (row["row_count"] or 0) + int(row_count or 0)
        new_status = "done" if new_count >= expected else "pending"

        conn.execute(
            """UPDATE daily_uploads
               SET actual_count = ?,
                   status = ?,
                   uploaded_at = ?,
                   uploaded_by_user_id = ?,
                   uploaded_by_name = ?,
                   row_count = ?,
                   file_names = ?,
                   notes = COALESCE(?, notes),
                   updated_at = ?
               WHERE upload_date = ? AND upload_type = ?""",
            (
                new_count, new_status, now,
                user_id, user_name,
                new_row_count,
                json.dumps(existing_files, ensure_ascii=False),
                notes,
                now,
                upload_date, upload_type,
            ),
        )
        conn.commit()

        return {
            "upload_type": upload_type,
            "upload_date": upload_date,
            "status": new_status,
            "actual_count": new_count,
            "expected_count": expected,
        }
    finally:
        conn.close()


# ── /today renderer ────────────────────────────────────────────────────

_RU_WEEKDAYS = [
    "Понедельник", "Вторник", "Среда", "Четверг",
    "Пятница", "Суббота", "Воскресенье",
]
_RU_MONTHS = [
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]


def _format_ru_date(d: date) -> str:
    return f"{_RU_WEEKDAYS[d.weekday()]}, {d.day} {_RU_MONTHS[d.month]} {d.year}"


def _status_icon(status: str, actual: int, expected: int) -> str:
    if status == "done":
        return "✅"
    if status == "skipped":
        return "⏭"
    if status == "failed":
        return "❌"
    # pending — but show partial progress for касса (1/2)
    if actual > 0 and actual < expected:
        return "🟡"
    return "⏳"


def get_checklist(target_date: Optional[date] = None) -> dict:
    """Return a structured checklist for target_date (defaults to today, Tashkent)."""
    target_date = target_date or tashkent_today()
    target_str = target_date.isoformat()

    conn = get_db()
    try:
        sched_rows = conn.execute(
            """SELECT * FROM daily_upload_schedule
               WHERE is_active = 1 ORDER BY sort_order"""
        ).fetchall()

        upload_rows = {
            r["upload_type"]: dict(r)
            for r in conn.execute(
                "SELECT * FROM daily_uploads WHERE upload_date = ?", (target_str,)
            ).fetchall()
        }

        holiday_name = is_holiday(target_str, conn)

        items = []
        missing = 0
        done = 0
        total_required = 0
        for s in sched_rows:
            sched = dict(s)
            required_here = is_required_weekday(target_date, sched["required_weekdays"])
            upload = upload_rows.get(sched["upload_type"])

            if holiday_name and not upload:
                # Holiday, no row → treated as skipped (not required).
                item = {
                    **sched,
                    "status": "skipped",
                    "actual_count": 0,
                    "row_count": 0,
                    "uploaded_at": None,
                    "uploaded_by_name": None,
                    "skip_reason": f"Holiday: {holiday_name}",
                    "required_today": False,
                }
            elif not required_here and not upload:
                item = {
                    **sched,
                    "status": "skipped",
                    "actual_count": 0,
                    "row_count": 0,
                    "uploaded_at": None,
                    "uploaded_by_name": None,
                    "skip_reason": "Not required today",
                    "required_today": False,
                }
            elif upload:
                item = {**sched, **upload, "required_today": required_here}
            else:
                item = {
                    **sched,
                    "status": "pending",
                    "actual_count": 0,
                    "row_count": 0,
                    "uploaded_at": None,
                    "uploaded_by_name": None,
                    "skip_reason": None,
                    "required_today": required_here,
                }

            if item.get("required_today"):
                total_required += 1
                if item["status"] == "done":
                    done += 1
                elif item["status"] in ("pending", "failed"):
                    missing += 1

            items.append(item)

        return {
            "date": target_date,
            "date_ru": _format_ru_date(target_date),
            "is_holiday": bool(holiday_name),
            "holiday_name": holiday_name,
            "is_sunday": target_date.isoweekday() == 7,
            "items": items,
            "done": done,
            "missing": missing,
            "total_required": total_required,
            "all_done": total_required > 0 and missing == 0,
        }
    finally:
        conn.close()


def render_checklist_text(checklist: dict, header: Optional[str] = None) -> str:
    """Format a checklist as a Telegram message (Markdown-safe plain text)."""
    lines: List[str] = []
    if header:
        lines.append(header)
    lines.append(f"📋 {checklist['date_ru']}")

    if checklist["is_holiday"]:
        lines.append(f"🎉 Выходной день: {checklist['holiday_name']}")
        lines.append("")
    elif checklist["is_sunday"]:
        lines.append("🛌 Воскресенье — выходной день")
        lines.append("")

    lines.append("")
    for it in checklist["items"]:
        icon = _status_icon(it["status"], it.get("actual_count", 0), it.get("expected_count_per_day", 1))
        name = it["display_name_ru"]
        expected = it.get("expected_count_per_day", 1)
        actual = it.get("actual_count", 0)

        if it["status"] == "done":
            who = it.get("uploaded_by_name") or "—"
            when = it.get("uploaded_at") or ""
            # Trim seconds for readability
            if when and len(when) >= 16:
                when = when[11:16]
            line = f"{icon} {name} — {when} · {who}"
        elif it["status"] == "skipped":
            reason = it.get("skip_reason") or ""
            line = f"{icon} {name}"
            if reason:
                line += f" ({reason})"
        elif expected > 1 and actual > 0:
            # partial progress (касса 1/2)
            who = it.get("uploaded_by_name") or "—"
            line = f"{icon} {name} — {actual}/{expected} · {who}"
        else:
            line = f"{icon} {name}"
        lines.append(line)

    lines.append("")
    if checklist["total_required"] > 0:
        lines.append(
            f"Итого: {checklist['done']}/{checklist['total_required']} загружено"
        )
    return "\n".join(lines)


# ── Skip / holiday management ──────────────────────────────────────────


def skip_upload(upload_type: str, upload_date: str, reason: str) -> int:
    """Mark a single upload row as skipped. Returns rowcount."""
    if upload_type not in VALID_UPLOAD_TYPES:
        raise ValueError(f"Unknown upload_type: {upload_type}")
    conn = get_db()
    try:
        ensure_row(upload_type, upload_date, conn)
        cur = conn.execute(
            """UPDATE daily_uploads
               SET status = 'skipped', skip_reason = ?, updated_at = ?
               WHERE upload_date = ? AND upload_type = ?""",
            (reason, tashkent_now_str(), upload_date, upload_type),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def skip_all_uploads(upload_date: str, reason: str) -> int:
    """Mark every upload_type for a given date as skipped. Returns number of rows touched."""
    conn = get_db()
    try:
        types = [r["upload_type"] for r in conn.execute(
            "SELECT upload_type FROM daily_upload_schedule WHERE is_active = 1"
        ).fetchall()]
        count = 0
        for t in types:
            ensure_row(t, upload_date, conn)
            conn.execute(
                """UPDATE daily_uploads
                   SET status = 'skipped', skip_reason = ?, updated_at = ?
                   WHERE upload_date = ? AND upload_type = ?""",
                (reason, tashkent_now_str(), upload_date, t),
            )
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def add_holiday(holiday_date: str, name: str, user_id: Optional[int] = None) -> dict:
    """Insert a holiday and retroactively skip any pending/failed rows on that date.
    Rows that are already 'done' are left alone (history preserved)."""
    conn = get_db()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO holidays (holiday_date, name, added_by_user_id)
               VALUES (?, ?, ?)""",
            (holiday_date, name, user_id),
        )
        # Retroactive skip ONLY of pending/failed.
        cur = conn.execute(
            """UPDATE daily_uploads
               SET status = 'skipped',
                   skip_reason = ?,
                   updated_at = ?
               WHERE upload_date = ?
                 AND status IN ('pending', 'failed')""",
            (f"Holiday: {name}", tashkent_now_str(), holiday_date),
        )
        touched = cur.rowcount
        conn.commit()
        return {"holiday_date": holiday_date, "name": name, "rows_updated": touched}
    finally:
        conn.close()


def remove_holiday(holiday_date: str) -> dict:
    """Remove a holiday and revert holiday-skipped rows back to pending."""
    conn = get_db()
    try:
        hol = conn.execute(
            "SELECT name FROM holidays WHERE holiday_date = ?", (holiday_date,)
        ).fetchone()
        if not hol:
            return {"removed": False, "rows_updated": 0}
        conn.execute("DELETE FROM holidays WHERE holiday_date = ?", (holiday_date,))
        cur = conn.execute(
            """UPDATE daily_uploads
               SET status = 'pending', skip_reason = NULL, updated_at = ?
               WHERE upload_date = ?
                 AND status = 'skipped'
                 AND skip_reason LIKE 'Holiday:%'""",
            (tashkent_now_str(), holiday_date),
        )
        touched = cur.rowcount
        conn.commit()
        return {"removed": True, "rows_updated": touched, "name": hol["name"]}
    finally:
        conn.close()


def list_holidays(days_ahead: int = 365) -> List[dict]:
    """Return holidays from today through N days ahead."""
    today = tashkent_today_str()
    end = (tashkent_today() + timedelta(days=days_ahead)).isoformat()
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT holiday_date, name FROM holidays
               WHERE holiday_date >= ? AND holiday_date <= ?
               ORDER BY holiday_date""",
            (today, end),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── FX rate helper ──────────────────────────────────────────────────────


def set_fx_rate(
    rate: float,
    user_id: Optional[int] = None,
    user_name: Optional[str] = None,
    rate_date: Optional[str] = None,
    currency_pair: str = "USD_UZS",
) -> dict:
    """Upsert today's FX rate and bump the /fxrate tracker."""
    rate_date = rate_date or tashkent_today_str()
    conn = get_db()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO daily_fx_rates
               (rate_date, currency_pair, rate, source, uploaded_by_user_id, uploaded_by_name, created_at)
               VALUES (?, ?, ?, 'manual', ?, ?, ?)""",
            (rate_date, currency_pair, rate, user_id, user_name, tashkent_now_str()),
        )
        conn.commit()
    finally:
        conn.close()

    record_upload(
        "fxrate",
        user_id=user_id,
        user_name=user_name,
        file_name=f"{rate:.2f} {currency_pair}",
        row_count=1,
        upload_date=rate_date,
    )
    return {"rate_date": rate_date, "rate": rate, "currency_pair": currency_pair}


def get_latest_fx_rate(currency_pair: str = "USD_UZS") -> Optional[dict]:
    conn = get_db()
    try:
        row = conn.execute(
            """SELECT * FROM daily_fx_rates WHERE currency_pair = ?
               ORDER BY rate_date DESC LIMIT 1""",
            (currency_pair,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ── /missing — backward-looking gaps report ───────────────────────────

_RU_WEEKDAYS_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

BACKFILL_START = date(2026, 4, 1)


def _parse_user_date(raw: str) -> Optional[date]:
    """Parse YYYY-MM-DD or DD.MM.YY (1C-style) into a date object."""
    raw = raw.strip()
    # Try ISO first
    try:
        return date.fromisoformat(raw)
    except ValueError:
        pass
    # Try DD.MM.YY
    try:
        parts = raw.split(".")
        if len(parts) == 3:
            d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
            if y < 100:
                y += 2000
            return date(y, m, d)
    except (ValueError, IndexError):
        pass
    return None


def get_missing_gaps(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> dict:
    """Return a structured report of missing/partial uploads in a date range.

    Defaults: start_date = 1st of current month (Tashkent), end_date = yesterday.
    Only looks backward (today and future are excluded).
    Read-only: does NOT insert virtual rows into daily_uploads.
    """
    today = tashkent_today()
    yesterday = today - timedelta(days=1)

    if start_date is None:
        start_date = today.replace(day=1)
    if end_date is None:
        end_date = yesterday

    # Clamp end to yesterday (never report today or future)
    if end_date >= today:
        end_date = yesterday

    if start_date > end_date:
        return {"ok": False, "error": "empty_range"}

    before_backfill = start_date < BACKFILL_START

    conn = get_db()
    try:
        # Load schedule
        sched_rows = conn.execute(
            """SELECT * FROM daily_upload_schedule
               WHERE is_active = 1 ORDER BY sort_order"""
        ).fetchall()
        sched_list = [dict(s) for s in sched_rows]

        # Load all upload rows in range
        upload_rows = {}
        for r in conn.execute(
            """SELECT * FROM daily_uploads
               WHERE upload_date >= ? AND upload_date <= ?""",
            (start_date.isoformat(), end_date.isoformat()),
        ).fetchall():
            key = (r["upload_date"], r["upload_type"])
            upload_rows[key] = dict(r)

        # Load holidays in range
        holidays_in_range = {}
        for r in conn.execute(
            """SELECT holiday_date, name FROM holidays
               WHERE holiday_date >= ? AND holiday_date <= ?""",
            (start_date.isoformat(), end_date.isoformat()),
        ).fetchall():
            holidays_in_range[r["holiday_date"]] = r["name"]

        # Build per-day gap data
        days_data = []
        total_gaps = 0
        total_working_days = 0
        d = start_date
        while d <= end_date:
            d_str = d.isoformat()
            is_sunday = d.isoweekday() == 7
            holiday_name = holidays_in_range.get(d_str)

            if is_sunday or holiday_name:
                days_data.append({
                    "date": d,
                    "date_str": d_str,
                    "is_off": True,
                    "off_reason": holiday_name or "Воскресенье",
                    "gaps": [],
                })
                d += timedelta(days=1)
                continue

            total_working_days += 1
            day_gaps = []
            day_done = []

            for sched in sched_list:
                utype = sched["upload_type"]
                expected = int(sched.get("expected_count_per_day", 1) or 1)
                required_wdays = sched.get("required_weekdays", "1,2,3,4,5,6")

                if not is_required_weekday(d, required_wdays):
                    continue

                upload = upload_rows.get((d_str, utype))

                if upload:
                    status = upload["status"]
                    actual = int(upload.get("actual_count", 0) or 0)

                    if status == "done" and actual >= expected:
                        day_done.append({
                            "upload_type": utype,
                            "display_name_ru": sched["display_name_ru"],
                            "command": sched["command"],
                            "kind": "done",
                            "actual": actual,
                            "expected": expected,
                        })
                        continue
                    if status == "skipped":
                        day_done.append({
                            "upload_type": utype,
                            "display_name_ru": sched["display_name_ru"],
                            "command": sched["command"],
                            "kind": "skipped",
                            "actual": actual,
                            "expected": expected,
                        })
                        continue

                    # Partial or pending/failed
                    if status == "done" and actual < expected:
                        # Partial (e.g. касса 1 of 2)
                        day_gaps.append({
                            "upload_type": utype,
                            "display_name_ru": sched["display_name_ru"],
                            "command": sched["command"],
                            "kind": "partial",
                            "actual": actual,
                            "expected": expected,
                        })
                        total_gaps += 1
                    else:
                        # pending or failed — fully missing
                        actual_count = int(upload.get("actual_count", 0) or 0)
                        if actual_count > 0 and actual_count < expected:
                            day_gaps.append({
                                "upload_type": utype,
                                "display_name_ru": sched["display_name_ru"],
                                "command": sched["command"],
                                "kind": "partial",
                                "actual": actual_count,
                                "expected": expected,
                            })
                        else:
                            day_gaps.append({
                                "upload_type": utype,
                                "display_name_ru": sched["display_name_ru"],
                                "command": sched["command"],
                                "kind": "missing",
                                "actual": 0,
                                "expected": expected,
                            })
                        total_gaps += 1
                else:
                    # No row at all — virtual missing
                    day_gaps.append({
                        "upload_type": utype,
                        "display_name_ru": sched["display_name_ru"],
                        "command": sched["command"],
                        "kind": "missing",
                        "actual": 0,
                        "expected": expected,
                    })
                    total_gaps += 1

            if day_gaps:
                days_data.append({
                    "date": d,
                    "date_str": d_str,
                    "is_off": False,
                    "off_reason": None,
                    "gaps": day_gaps,
                    "done": day_done,
                })

            d += timedelta(days=1)

        return {
            "ok": True,
            "start_date": start_date,
            "end_date": end_date,
            "before_backfill": before_backfill,
            "days": days_data,
            "total_gaps": total_gaps,
            "total_working_days": total_working_days,
        }
    finally:
        conn.close()


def render_missing_text(report: dict) -> str:
    """Format the missing-gaps report as a Telegram-friendly plain text message."""
    if not report.get("ok"):
        return "Укажите диапазон в прошлом. Формат: /missing или /missing 2026-04-01 2026-04-08"

    sd = report["start_date"]
    ed = report["end_date"]
    header = (
        f"📋 Пропущенные загрузки: "
        f"{sd.strftime('%d.%m')} — {ed.strftime('%d.%m.%Y')}"
    )

    lines: List[str] = [header, ""]

    if report.get("before_backfill"):
        lines.append(
            "⚠️ Данные до 01.04.2026 могут быть неполными"
            " (до запуска системы отслеживания)"
        )
        lines.append("")

    if report["total_gaps"] == 0 and not report["days"]:
        return (
            f"✅ Все данные загружены за период "
            f"{sd.strftime('%d.%m')} — {ed.strftime('%d.%m.%Y')}"
        )

    for day in report["days"]:
        d: date = day["date"]
        dd_mm = d.strftime("%d.%m")
        wd_short = _RU_WEEKDAYS_SHORT[d.weekday()]

        if day["is_off"]:
            lines.append(f"{dd_mm} ({wd_short}): Выходной ✓")
            continue

        # Show completed items first, then gaps
        done_parts = []
        for item in day.get("done", []):
            name_lc = item["display_name_ru"].lower()
            if item["kind"] == "skipped":
                done_parts.append(f"⏭ {name_lc}")
            else:
                done_parts.append(f"✅ {name_lc}")

        gap_parts = []
        for g in day["gaps"]:
            name_lc = g["display_name_ru"].lower()
            cmd = g["command"]
            if g["kind"] == "partial":
                gap_parts.append(
                    f"⚠️ {name_lc} ({cmd}) — {g['actual']} из {g['expected']}"
                )
            else:
                gap_parts.append(f"⏳ {name_lc} ({cmd})")

        all_parts = done_parts + gap_parts
        lines.append(f"{dd_mm} ({wd_short}): {' '.join(all_parts)}")

    lines.append("")
    lines.append(
        f"Итого: {report['total_gaps']} пропуска"
        f" за {report['total_working_days']} рабочих дней"
    )

    return "\n".join(lines)
