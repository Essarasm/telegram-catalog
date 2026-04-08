"""Scheduled daily-upload reminders for the Rassvet bot.

Runs two background tasks (pure asyncio, no APScheduler) that each sleep
until the next trigger time in Asia/Tashkent:

* **10:00 AM — morning nudge**: sends the full /today checklist to
  ADMIN_GROUP_CHAT_ID with a bilingual header.
* **17:00 PM — EOD escalation**: if anything is still missing, sends a
  list of pending items. If everything is done, sends positive reinforcement.

Both tasks skip Sundays and registered holidays. All timezone math goes
through ``ZoneInfo("Asia/Tashkent")`` — we NEVER use UTC for user-facing
times.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

TASHKENT = ZoneInfo("Asia/Tashkent")


def _next_trigger(hour: int, minute: int = 0) -> datetime:
    """Return the next Tashkent datetime for hour:minute in the future."""
    now = datetime.now(TASHKENT)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return target


async def _sleep_until(target: datetime) -> None:
    delay = (target - datetime.now(TASHKENT)).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)


def _should_send(today_date) -> tuple[bool, str | None]:
    """Return (should_send, skip_reason)."""
    from backend.services.daily_uploads import is_holiday
    if today_date.isoweekday() == 7:
        return False, "Sunday"
    hol = is_holiday(today_date.isoformat())
    if hol:
        return False, f"Holiday: {hol}"
    return True, None


async def _send_morning_nudge(bot, chat_id: int) -> None:
    from backend.services.daily_uploads import (
        get_checklist, render_checklist_text, tashkent_today,
    )

    today = tashkent_today()
    ok, reason = _should_send(today)
    if not ok:
        logger.info(f"Morning nudge skipped: {reason}")
        return

    try:
        ck = get_checklist(today)
        header = "🌅 <b>Ertalab yangilanish / Утренняя сводка</b>\n"
        text = render_checklist_text(ck)
        from bot.main import html_escape
        await bot.send_message(
            chat_id,
            f"{header}<pre>{html_escape(text)}</pre>",
            parse_mode="HTML",
        )
        logger.info(f"Morning nudge sent to {chat_id}")
    except Exception as e:
        logger.error(f"Morning nudge failed: {e}")


async def _send_eod_check(bot, chat_id: int) -> None:
    from backend.services.daily_uploads import get_checklist, tashkent_today
    from bot.main import html_escape

    today = tashkent_today()
    ok, reason = _should_send(today)
    if not ok:
        logger.info(f"EOD check skipped: {reason}")
        return

    try:
        ck = get_checklist(today)
        if ck["all_done"]:
            await bot.send_message(
                chat_id,
                "✅ <b>Barchasi yuklandi! / Все загружено за сегодня</b>\n"
                f"📋 {html_escape(ck['date_ru'])}\n"
                f"Yakuniy: {ck['done']}/{ck['total_required']}",
                parse_mode="HTML",
            )
            logger.info(f"EOD positive ping sent to {chat_id}")
            return

        missing_items = [
            it for it in ck["items"]
            if it.get("required_today") and it["status"] in ("pending", "failed")
        ]
        if not missing_items:
            return

        lines = [
            "⚠️ <b>Tugash nuqtasi / Конец дня — есть пропуски!</b>\n",
            f"📋 {html_escape(ck['date_ru'])}\n",
            "Bugun hali yuklanmagan:",
        ]
        for it in missing_items:
            name = html_escape(it.get("display_name_ru") or it.get("upload_type"))
            cmd = html_escape(it.get("command") or "")
            exp = it.get("expected_count_per_day", 1)
            act = it.get("actual_count", 0)
            if exp > 1:
                lines.append(f"  • {name} — {act}/{exp}  ({cmd})")
            else:
                lines.append(f"  • {name}  ({cmd})")
        lines.append(f"\nYakuniy: {ck['done']}/{ck['total_required']}")
        await bot.send_message(chat_id, "\n".join(lines), parse_mode="HTML")
        logger.info(f"EOD escalation sent to {chat_id} — {len(missing_items)} missing")
    except Exception as e:
        logger.error(f"EOD check failed: {e}")


async def run_daily_reminder(bot, chat_id: int, hour: int, minute: int, sender) -> None:
    """Forever loop that sleeps until the next trigger and calls ``sender``."""
    while True:
        try:
            target = _next_trigger(hour, minute)
            logger.info(f"Next {sender.__name__} at {target.isoformat()}")
            await _sleep_until(target)
            await sender(bot, chat_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Reminder loop error for {sender.__name__}: {e}")
            # Back off 5 minutes on unexpected errors so we don't tight-loop.
            await asyncio.sleep(300)


def start_reminder_tasks(bot, chat_id: int) -> list[asyncio.Task]:
    """Launch the two reminder background tasks. Returns the task handles."""
    tasks = [
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 10, 0, _send_morning_nudge),
            name="daily-upload-morning-nudge",
        ),
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 17, 0, _send_eod_check),
            name="daily-upload-eod-check",
        ),
    ]
    logger.info(f"Started {len(tasks)} daily-upload reminder tasks (chat {chat_id})")
    return tasks
