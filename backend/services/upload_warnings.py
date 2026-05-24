"""Caption-warning helpers for daily-flow XLS imports.

Two complementary signals catch the "wrong file uploaded" failure mode
that bit /realorders on 2026-05-14 (Alisher re-uploaded the previous
day's `real orders 13.05.26.xls` on 5/14; importer was a silent no-op):

  * stale-date — file's `date_max` is more than `grace_days` before
    today (Tashkent). Catches a stale file uploaded a day late.
  * no-new-rows — `inserted == 0 and updated > 0`. Catches an exact
    duplicate re-upload that replaced existing rows with identical data.

Warn-only — never blocks the import. Legitimate backfills exist
(Error Log #200's full-month cash recovery is the canonical case).
"""
from datetime import date as _date, datetime as _dt
from typing import Optional
from zoneinfo import ZoneInfo


def stale_upload_warnings(
    *,
    date_min,
    date_max,
    inserted: int,
    updated: int,
    grace_days: int = 1,
    today: Optional[_date] = None,
) -> list:
    """Return Uzbek ⚠️ caption lines for stale-date + no-new-rows.

    Empty list when neither signal triggers. Caller prepends to the
    success caption body.

    `date_min` / `date_max` may be ISO-string or None (when the importer
    couldn't determine a range, e.g. malformed file — warnings skipped).
    """
    if today is None:
        today = _dt.now(ZoneInfo("Asia/Tashkent")).date()

    warnings = []

    if date_max:
        try:
            d_max = _date.fromisoformat(str(date_max))
            days_stale = (today - d_max).days
            if days_stale > grace_days:
                if date_min and str(date_min) != str(date_max):
                    span = f"{date_min}…{date_max}"
                else:
                    span = str(date_max)
                warnings.append(
                    f"⚠️ <b>Eski ma'lumot:</b> fayldagi sanalar {span}, "
                    f"bugun {today.isoformat()}. To'g'ri fayl yuklandi?"
                )
        except ValueError:
            pass

    if inserted == 0 and updated > 0:
        warnings.append(
            f"⚠️ <b>Yangi qator yo'q:</b> {updated} ta yangilandi, "
            f"yangi qatorlar 0. Bu fayl avval yuklangan bo'lishi mumkin."
        )

    return warnings
