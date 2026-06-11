"""Scheduled daily-upload reminders for the Rassvet bot.

Runs two background tasks (pure asyncio, no APScheduler) that each sleep
until the next trigger time in Asia/Tashkent:

* **09:00 — morning nudge**: sends the full /today checklist to
  DAILY_GROUP_CHAT_ID with a bilingual header.
* **17:00 — EOD escalation**: if anything is still missing, sends a
  list of pending items. If everything is done, sends positive reinforcement.

Both tasks skip Sundays and registered holidays. All timezone math goes
through ``ZoneInfo("Asia/Tashkent")`` — we NEVER use UTC for user-facing
times.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from backend.admin_auth import get_admin_key

logger = logging.getLogger(__name__)

TASHKENT = ZoneInfo("Asia/Tashkent")


def _already_fired_today(reminder_name: str, fire_date_iso: str) -> bool:
    """Has this reminder slot already fired today (per reminder_fire_log)?
    Fail-open on DB error — better to maybe-duplicate than to miss a fire.
    """
    try:
        from backend.database import get_db
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT 1 FROM reminder_fire_log "
                "WHERE reminder_name = ? AND fire_date = ? LIMIT 1",
                (reminder_name, fire_date_iso),
            ).fetchone()
            return row is not None
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"_already_fired_today({reminder_name}, {fire_date_iso}) failed: {e}")
        return False


def _record_fire(reminder_name: str, fire_date_iso: str) -> None:
    """Stamp a successful fire so the next restart's catch-up skips this slot."""
    try:
        from backend.database import get_db
        conn = get_db()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO reminder_fire_log "
                "(reminder_name, fire_date, fired_at_utc) VALUES (?, ?, ?)",
                (reminder_name, fire_date_iso,
                 datetime.now(timezone.utc).isoformat(timespec="seconds")),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"_record_fire({reminder_name}, {fire_date_iso}) failed: {e}")


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


def _should_send(today_date, skip_sunday: bool = True) -> tuple[bool, str | None]:
    """Return (should_send, skip_reason).

    `skip_sunday=True` (default) preserves the original behavior for the
    morning nudge / EOD check / weekly-unlinked etc. The stock alert opts
    out (`skip_sunday=False`) so Saturday-evening /stock uploads surface
    on Sunday morning before Monday's reset wipes the week.
    """
    from backend.services.daily_uploads import is_holiday
    if skip_sunday and today_date.isoweekday() == 7:
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
        from bot.shared import html_escape
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
    from bot.shared import html_escape

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
        # Also nudge on items that are checklist-done but behind on the
        # reminder-count target (e.g. cash: expected=1, reminder=2).
        nudge_items = [
            it for it in ck["items"]
            if it.get("required_today")
            and it["status"] == "done"
            and (it.get("reminder_count_per_day") or 0)
                 > it.get("actual_count", 0)
        ]
        if not missing_items and not nudge_items:
            return

        lines = [
            "⚠️ <b>Tugash nuqtasi / Конец дня — есть пропуски!</b>\n",
            f"📋 {html_escape(ck['date_ru'])}\n",
        ]
        if missing_items:
            lines.append("Bugun hali yuklanmagan:")
            for it in missing_items:
                name = html_escape(it.get("display_name_ru") or it.get("upload_type"))
                cmd = html_escape(it.get("command") or "")
                exp = it.get("expected_count_per_day", 1)
                act = it.get("actual_count", 0)
                if exp > 1:
                    lines.append(f"  • {name} — {act}/{exp}  ({cmd})")
                else:
                    lines.append(f"  • {name}  ({cmd})")
        if nudge_items:
            lines.append("")
            lines.append("Eslatma / Напоминание (takroriy yuklash):")
            for it in nudge_items:
                name = html_escape(it.get("display_name_ru") or it.get("upload_type"))
                cmd = html_escape(it.get("command") or "")
                rem = it.get("reminder_count_per_day") or 0
                act = it.get("actual_count", 0)
                lines.append(f"  • {name} — {act}/{rem}  ({cmd})")
        lines.append(f"\nYakuniy: {ck['done']}/{ck['total_required']}")
        await bot.send_message(chat_id, "\n".join(lines), parse_mode="HTML")
        logger.info(
            f"EOD sent to {chat_id} — {len(missing_items)} missing, "
            f"{len(nudge_items)} nudge"
        )
    except Exception as e:
        logger.error(f"EOD check failed: {e}")


async def run_daily_reminder(bot, chat_id: int, hour: int, minute: int, sender,
                              catchup_grace_hours: float = 4.0) -> None:
    """Forever loop that fires ``sender`` at the next trigger; on startup,
    fires immediately if today's slot was missed by < ``catchup_grace_hours``
    AND today's slot is not already in reminder_fire_log (Error Log #32 +
    #NN, patterns CRON_RESTART_PAST_FIRE_DROPS_DAY + CRON_RESTART_REFIRES_DAY).
    """
    first_iter = True
    while True:
        try:
            if first_iter:
                first_iter = False
                now = datetime.now(TASHKENT)
                today_target = now.replace(hour=hour, minute=minute,
                                           second=0, microsecond=0)
                missed_s = (now - today_target).total_seconds()
                if 0 < missed_s < catchup_grace_hours * 3600:
                    today_iso = now.date().isoformat()
                    if _already_fired_today(sender.__name__, today_iso):
                        logger.info(
                            f"{sender.__name__}: catch-up skipped — already "
                            f"fired on {today_iso} (CRON_RESTART_REFIRES_DAY)"
                        )
                    else:
                        logger.info(
                            f"{sender.__name__}: catch-up fire — missed "
                            f"{hour:02d}:{minute:02d} by {int(missed_s/60)}m "
                            f"(< {catchup_grace_hours}h grace)"
                        )
                        await sender(bot, chat_id)
                        _record_fire(sender.__name__, today_iso)
            target = _next_trigger(hour, minute)
            logger.info(f"Next {sender.__name__} at {target.isoformat()}")
            await _sleep_until(target)
            fire_date_iso = datetime.now(TASHKENT).date().isoformat()
            await sender(bot, chat_id)
            _record_fire(sender.__name__, fire_date_iso)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Reminder loop error for {sender.__name__}: {e}")
            # Back off 5 minutes on unexpected errors so we don't tight-loop.
            await asyncio.sleep(300)


async def _run_daily_client_sync(bot, chat_id: int) -> None:
    """Re-import the latest Client Master XLSX to keep allowed_clients up to date.

    Runs at 06:00 Tashkent time daily. Uses the same import-client-master API
    endpoint that the /clientmaster bot command uses. Sends a summary to the
    Sales group (ORDER_GROUP_CHAT_ID).
    """
    import os
    import httpx

    latest_file = "/data/client_master_latest.xlsx"
    if not os.path.exists(latest_file):
        logger.info("Daily client sync skipped — no client_master_latest.xlsx found")
        return

    _BASE_URL = os.getenv("WEBAPP_URL", "https://telegram-catalog-production.up.railway.app")
    from bot.shared import ORDER_GROUP_CHAT_ID

    try:
        with open(latest_file, "rb") as f:
            file_bytes = f.read()

        api_url = f"{_BASE_URL}/api/finance/import-client-master"
        async with httpx.AsyncClient(timeout=300) as client:
            resp = await client.post(
                api_url,
                files={"file": ("client_master_daily_sync.xlsx", file_bytes,
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                data={"admin_key": get_admin_key()},
            )
            result = resp.json()

        if not result.get("ok"):
            logger.error(f"Daily client sync failed: {result.get('error')}")
            return

        totals = result.get("totals", {})
        approved = result.get("users_retroactively_approved", 0)
        db_total = result.get("db_total_allowed_clients", 0)

        lines = [
            "\U0001f504 <b>Kunlik kontragentlar sinxronizatsiyasi</b>",
            "",
            f"\u2795 Yangi: {totals.get('inserted', 0)}",
            f"\u267b\ufe0f Yangilangan: {totals.get('updated', 0)}",
            f"\U0001f4be Jami: {db_total} mijoz",
        ]
        if approved:
            lines.append(f"\U0001f464 Tasdiqlangan: {approved} foydalanuvchi")

        await bot.send_message(ORDER_GROUP_CHAT_ID, "\n".join(lines), parse_mode="HTML")
        logger.info(f"Daily client sync complete: +{totals.get('inserted', 0)}, ~{totals.get('updated', 0)}")

    except Exception as e:
        logger.error(f"Daily client sync error: {e}")


async def _send_weekly_unlinked(bot, chat_id: int) -> None:
    """Tuesday 17:00 — remind about unregistered/unlinked users."""
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone(timedelta(hours=5)))
    if now.weekday() != 1:  # 1 = Tuesday
        return
    try:
        import sqlite3
        DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        count = conn.execute(
            "SELECT COUNT(*) FROM users "
            "WHERE client_id IS NULL AND (dismiss_status IS NULL OR dismiss_status = '') "
            "AND phone IS NOT NULL AND phone != ''"
        ).fetchone()[0]
        conn.close()
        if count == 0:
            return
        await bot.send_message(
            chat_id,
            f"📋 <b>Haftalik eslatma:</b> {count} ta foydalanuvchi hali "
            f"1C mijozga bog'lanmagan.\n\n"
            f"Ko'rish: /unlinked",
            parse_mode="HTML",
        )
        logger.info(f"Weekly unlinked reminder sent: {count} users")
    except Exception as e:
        logger.error(f"Weekly unlinked reminder failed: {e}")


async def _send_weekly_scorecard(bot, chat_id: int) -> None:
    """Monday 09:00 — weekly ops scorecard (the learning-loop ritual).

    Four KPIs measuring even-distribution of human resource + revenue and
    resource effectiveness. See backend/services/ops_scorecard.py and the
    operational-resource-balancing skill.
    """
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone(timedelta(hours=5)))
    if now.weekday() != 0:  # 0 = Monday
        return
    try:
        from backend.services.ops_scorecard import format_scorecard
        text = format_scorecard()
        await bot.send_message(chat_id, text, parse_mode="HTML")
        logger.info("Weekly ops scorecard sent")
    except Exception as e:
        logger.error(f"Weekly ops scorecard failed: {e}")


async def _send_stock_alert(bot, chat_id: int) -> None:
    """08:00 daily — inventory alert. Three modes by Tashkent weekday:

    * **Monday**: full Mon→Sun recap of the *prior* week (currently-zero items
      whose stockout fell in last week, plus refill count + last-week TOP-10).
      Closes the week before the new one resets.
    * **Tue–Sun**: delta — TODAY's new tugagan + TODAY's refills + a short
      "earlier this week pending: Mon (3), Tue (5)…" note pointing at
      /stockalert hafta. Sunday is allowed (work-week ends Sat evening upload).

    Holidays still skip. Silent only when truly nothing to report.
    """
    today = datetime.now(TASHKENT)
    ok, reason = _should_send(today, skip_sunday=False)
    if not ok:
        logger.info(f"Stock alert skipped: {reason}")
        return

    try:
        from backend.services.stock_alerts import (
            get_stock_alerts,
            get_refilled_today,
            get_last_week_recap,
            format_daily_delta_message,
            format_weekly_recap_message,
        )

        if today.isoweekday() == 1:  # Monday — recap mode
            recap = get_last_week_recap()
            if recap.get("active_count", 0) == 0:
                logger.info("Stock alert skipped — no active products detected")
                return
            messages = format_weekly_recap_message(recap)
            if not messages:
                logger.info(
                    f"Monday recap skipped — last week was empty "
                    f"(out={len(recap.get('out_of_stock', []))}, "
                    f"refilled={recap.get('refilled_count', 0)}, "
                    f"top={len(recap.get('top_sellers', []))})"
                )
                return
            for text in messages:
                await bot.send_message(chat_id, text, parse_mode="HTML")
            logger.info(
                f"Monday recap sent ({len(messages)} msg): "
                f"out={len(recap.get('out_of_stock', []))}, "
                f"refilled={recap.get('refilled_count', 0)}, "
                f"top={len(recap.get('top_sellers', []))}"
            )
            return

        # Tue–Sun: delta mode
        alerts = get_stock_alerts()
        if alerts["active_count"] == 0:
            logger.info("Stock alert skipped — no active products detected")
            return
        refilled = get_refilled_today()
        messages = format_daily_delta_message(alerts, refilled)
        if not messages:
            logger.info(
                f"Stock alert skipped — nothing new today "
                f"(weekly_out={len(alerts['weekly_out_of_stock'])}, "
                f"refilled_today={len(refilled)}, "
                f"cumulative_oos={len(alerts['out_of_stock'])})"
            )
            return
        for text in messages:
            await bot.send_message(chat_id, text, parse_mode="HTML")
        logger.info(
            f"Stock alert sent ({len(messages)} msg, delta): "
            f"weekly_out={len(alerts['weekly_out_of_stock'])}, "
            f"refilled_today={len(refilled)}, "
            f"top_sellers={len(alerts.get('weekly_top_sellers', []))}, "
            f"cumulative_oos={len(alerts['out_of_stock'])}"
        )
    except Exception as e:
        logger.error(f"Stock alert failed: {e}")


async def _recompute_units_score(bot, chat_id: int) -> None:
    """Daily 04:30 — refresh products/categories units_score so the rolling
    30d/60d windows roll over even on days with no /realorders import.
    Silent on success; noisy in logs only on failure."""
    try:
        import subprocess
        import sys
        script = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "tools", "update_units_score.py",
        )
        subprocess.Popen(
            [sys.executable, script],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        logger.info("units_score daily recompute triggered")
    except Exception as e:
        logger.error(f"units_score daily recompute failed: {e}")


async def _run_consistency_audit(bot, chat_id: int) -> None:
    """Daily 04:00 — run the data-consistency audit and post to Admin group
    if any issues are found. Silent if everything looks clean."""
    try:
        from backend.services.consistency_audit import (
            run_audit, format_audit_message,
            load_prior_snapshot, save_snapshot,
        )
        # 09:00 cron auto-heals healable_orphans (defense-in-depth on top of
        # per-mutator heal from Session F refactor phase 8). Other audit
        # checks remain alert-only.
        findings = run_audit(fix=True)
        # Diff against yesterday's snapshot for spike detection — closes
        # the signal-fatigue gap that let the 2026-05-15→18 incident
        # accumulate for 3 days as unchanged-looking noise (Error Log #56).
        prior = load_prior_snapshot()
        msg = format_audit_message(findings, prior_findings=prior)
        # Persist today's counts for tomorrow's comparison whether or not
        # we posted (a clean run still updates the baseline).
        save_snapshot(findings)
        if not msg:
            logger.info("consistency_audit: clean, nothing to report")
            return
        await bot.send_message(chat_id, msg, parse_mode="HTML")
        logger.info(f"consistency_audit: posted report ({len(findings)} checks flagged)")
    except Exception as e:
        logger.error(f"consistency_audit failed: {e}")


async def _send_offsite_db_backup(bot, chat_id: int) -> None:
    """Daily 03:00 — post the latest DB backup to Admin group as a document.
    Offsite copy beyond the Railway volume — if Railway disk corrupts/loses
    data, Admin group archive has a recent snapshot.
    """
    try:
        from aiogram.types import BufferedInputFile
        backup_dir = os.getenv("DB_BACKUP_DIR", "/data/db_backups")
        if not os.path.isdir(backup_dir):
            logger.info("offsite_db_backup: backup dir missing")
            return
        files = sorted([f for f in os.listdir(backup_dir)
                        if f.startswith("catalog_") and f.endswith(".sql.gz")])
        if not files:
            logger.info("offsite_db_backup: no backups yet")
            return
        latest = files[-1]
        path = os.path.join(backup_dir, latest)
        size_mb = os.path.getsize(path) / 1_000_000
        # Telegram bot API file limit is 50 MB. If gzipped dump somehow grows
        # past that, skip with a warning rather than crashing.
        if size_mb > 49:
            logger.warning(f"offsite_db_backup: {latest} too large ({size_mb:.1f} MB) — skipping")
            return
        with open(path, "rb") as f:
            data = f.read()
        caption = (
            f"💾 <b>Offsite DB backup</b>\n"
            f"Fayl: <code>{latest}</code>\n"
            f"Hajmi: {size_mb:.1f} MB\n\n"
            f"<i>Railway disk yo'qolsa shu fayldan tiklanadi. "
            f"Saqlab qo'ying.</i>"
        )
        await bot.send_document(
            chat_id,
            BufferedInputFile(data, filename=latest),
            caption=caption,
            parse_mode="HTML",
        )
        logger.info(f"offsite_db_backup: posted {latest} ({size_mb:.1f} MB)")
    except Exception as e:
        logger.error(f"offsite_db_backup failed: {e}")


async def _send_master_auto_export(bot, chat_id: int) -> None:
    """Monday 08:00 — auto-export Client Master xlsx to the Admin group."""
    today = datetime.now(TASHKENT)
    if today.weekday() != 0:  # 0 = Monday
        return
    try:
        from aiogram.types import BufferedInputFile
        from backend.services.export_client_master import build_xlsx_bytes, write_xlsx_to_archive
        data = build_xlsx_bytes()
        try:
            write_xlsx_to_archive()
        except Exception as e:
            logger.warning(f"master auto-export archive failed: {e}")
        ts = today.strftime("%Y-%m-%d")
        filename = f"Client_Master_{ts}.xlsx"
        caption = (
            "📋 <b>Dushanba: Client Master sinxronlash</b>\n\n"
            f"Sana: {ts}\n"
            "Bu hafta bo'yi ✏️ sariq ustunlarni tahrirlang. "
            "Juma kuni men sizga eslatma yuboraman — "
            "<code>/clientmaster</code> caption bilan qayta yuboring."
        )
        await bot.send_document(
            chat_id,
            BufferedInputFile(data, filename=filename),
            caption=caption,
            parse_mode="HTML",
        )
        logger.info(f"Master auto-export sent: {len(data)} bytes to {chat_id}")
    except Exception as e:
        logger.error(f"Master auto-export failed: {e}")


async def _send_master_sync_prompt(bot, chat_id: int) -> None:
    """Friday 17:00 — prompt admins to send back edited Client Master xlsx."""
    today = datetime.now(TASHKENT)
    if today.weekday() != 4:  # 4 = Friday
        return
    try:
        await bot.send_message(
            chat_id,
            "📋 <b>Haftalik Client Master sinxronlash vaqti keldi!</b>\n\n"
            "Agar siz <code>Client_Master_...xlsx</code> faylini tahrir qilgan bo'lsangiz, "
            "iltimos, shu xabarga javob sifatida <code>/clientmaster</code> caption bilan qayta yuboring.\n\n"
            "Agar hech narsa o'zgartirmagan bo'lsangiz, bu xabarni e'tiborga olmang — "
            "Dushanba kuni yangi fayl yuboriladi.",
            parse_mode="HTML",
        )
        logger.info(f"Master sync prompt sent to {chat_id}")
    except Exception as e:
        logger.error(f"Master sync prompt failed: {e}")


PAT_STAMP_FILE = os.getenv("PAT_STAMP_FILE", "/data/.pat_rotated_at")
PAT_WARN_AT_DAYS = 75   # warn when token is 75+ days old (GitHub default expiry 90d)
PAT_REWARN_EVERY_DAYS = 7


async def _send_pat_rotation_reminder(bot, chat_id: int) -> None:
    """Daily 09:00 — if the stamped PAT rotation date is ≥75 days old,
    nudge Admin group to regenerate. Silent otherwise. Skips entirely if
    the stamp file doesn't exist (no data yet = no reminder)."""
    try:
        from datetime import date as _date
        if not os.path.exists(PAT_STAMP_FILE):
            return
        with open(PAT_STAMP_FILE) as f:
            stamp_text = f.read().strip()
        try:
            stamped_at = _date.fromisoformat(stamp_text[:10])
        except ValueError:
            return
        age_days = (_date.today() - stamped_at).days
        if age_days < PAT_WARN_AT_DAYS:
            return
        # Suppress re-nudge unless it's been >= PAT_REWARN_EVERY_DAYS since
        # the last nudge. We record the last nudge date by re-touching the
        # stamp file's second line.
        last_nudge_path = PAT_STAMP_FILE + ".last_nudge"
        today = _date.today()
        if os.path.exists(last_nudge_path):
            try:
                last = _date.fromisoformat(open(last_nudge_path).read().strip()[:10])
                if (today - last).days < PAT_REWARN_EVERY_DAYS:
                    return
            except ValueError:
                pass
        await bot.send_message(
            chat_id,
            f"🔑 <b>GitHub PAT rotation reminder</b>\n\n"
            f"Token <code>{stamped_at.isoformat()}</code> kuni o'rnatilgan — "
            f"<b>{age_days} kun bo'ldi</b>.\n"
            f"GitHub tokenlari 90 kunda tugaydi; hozir yangilasangiz yaxshi.\n\n"
            f"1. https://github.com/settings/tokens → eski tokenni revoke qiling\n"
            f"2. Yangi token yarating (scope: <code>repo</code>)\n"
            f"3. <code>/patrotated NEW_TOKEN</code> buyrug'ini shu guruhda yuboring\n"
            f"   (yoki terminalda <code>echo NEW &gt; .credentials</code>)",
            parse_mode="HTML",
        )
        with open(last_nudge_path, "w") as f:
            f.write(today.isoformat())
        logger.info(f"pat_rotation_reminder: sent (age {age_days}d)")
    except Exception as e:
        logger.error(f"pat_rotation_reminder failed: {e}")


async def _send_sunday_infra_nudge(bot, chat_id: int) -> None:
    """Sunday 09:00 Tashkent — infra-maintenance day reminder → Platform-Ops.

    Posted to PLATFORM_OPS_GROUP_CHAT_ID per the "every Sunday is infrastructure
    day" rhythm (Notion Command Center, 2026-05-17). Includes a quarterly
    add-on line on the first Sunday of Mar/Jun/Sep/Dec for the deep
    foundation-auditor run.

    No-ops Mon–Sat (function fires daily via run_daily_reminder; the date
    check is the only gate). Silent on send failure — operator can re-check
    audit.sh manually.
    """
    today = datetime.now(TASHKENT).date()
    if today.weekday() != 6:  # 6 = Sunday
        return

    # First Sunday of Mar/Jun/Sep/Dec → also nudge the deep audit
    is_first_sunday = today.day <= 7
    is_quarter_month = today.month in {3, 6, 9, 12}
    is_quarterly = is_first_sunday and is_quarter_month

    # One-time top item, self-expiring after 2026-06-07 (approved 2026-06-03).
    # Per-session git worktrees — eliminate the parallel-session WIP-bundling
    # class (Rule Violations #6/#14; an unauth endpoint reached prod this way).
    # Plain plan in Session F handoff 2026-06-02 / this convo; do it on a quiet
    # Sunday with no other session live. Drops off automatically next week.
    onetime = ""
    if today.isoformat() <= "2026-06-07":
        onetime = (
            "⭐ <b>FIRST (one-time setup): per-session git worktrees</b>\n"
            "Give each Claude session its own private copy so parallel sessions "
            "can't bundle each other's unfinished work (this is how an "
            "unauthenticated page reached prod on 2026-06-03). Plan approved — "
            "start a session and say: <i>\"set up worktrees per the approved plan.\"</i>\n\n"
            "──────────\n\n"
        )

    base = (
        onetime +
        "🛠 <b>Sunday infrastructure maintenance</b>\n\n"
        "• <code>bash tools/audit.sh</code> — 30-sec foundation snapshot\n"
        "• Address any 🟡 / ❌ in the summary\n"
        "• Skim <code>obsidian-vault/🐛 Error Log.md</code> for new patterns"
    )
    if is_quarterly:
        base += (
            "\n\n📊 <b>Quarterly deep audit also due today</b>\n"
            f"• Q{(today.month - 1) // 3 + 1} {today.year} — run <code>/audit</code> "
            "for the 5-dimension foundation-auditor analysis"
        )

    try:
        await bot.send_message(chat_id, base, parse_mode="HTML")
        logger.info(
            f"Sunday infra nudge sent to {chat_id} "
            f"(quarterly={is_quarterly})"
        )
    except Exception as e:
        logger.error(f"Sunday infra nudge failed: {e}")


async def _send_p6_followup_reminder(bot, chat_id: int) -> None:
    """One-shot 09:00 Tashkent reminder on 2026-05-24: follow up on P6
    schema migration deferred from 2026-05-18 Session Ops.

    Fires once on that date; no-op on every other day. The wiring in the
    daily loop can be removed in a cleanup pass after the date passes
    (it'll keep silently no-op'ing indefinitely if left in place).
    """
    from datetime import date as _date
    today = datetime.now(TASHKENT).date()
    if today != _date(2026, 5, 24):
        return

    text = (
        "📋 <b>P6 follow-up — schema migration (deferred from 2026-05-18)</b>\n\n"
        "Today's the day we set aside last Tuesday. Migration scope:\n"
        "• <code>allowed_clients.status</code>: retire <code>'merged_into:&lt;id&gt;'</code> string format\n"
        "• Add <code>is_merged BOOLEAN</code> + <code>merged_into_canonical_id INTEGER</code>\n"
        "• Backfill existing tombstones\n"
        "• Replace 41 callsites' <code>NOT LIKE 'merged%'</code> → <code>is_merged = 0</code>\n"
        "• Update <code>tools/merge_duplicate_1c_clients.py</code> to write new shape\n"
        "• Update Guard 6 + <code>.claude/contracts/soft_delete_vocabulary.md</code>\n"
        "• Update <code>tests/test_tombstone_routing.py</code>\n\n"
        "<b>Why this matters</b>: eliminates the TOMBSTONE_STATUS_FILTER_MISMATCH "
        "failure class entirely (Error Log #56 — Жамшед Сифат Гагарин incident, "
        "198 clients / 667 finance rows / 3-day exposure window).\n\n"
        "<b>Estimated effort</b>: half-day session. Recommend booking a dedicated "
        "Session Ops block rather than squeezing into another session's tail."
    )
    try:
        await bot.send_message(chat_id, text, parse_mode="HTML")
        logger.info(f"P6 follow-up reminder sent to {chat_id}")
    except Exception as e:
        logger.error(f"P6 follow-up reminder failed: {e}")


async def _send_master_sync_nudge(bot, chat_id: int) -> None:
    """Sunday 12:00 — gentle nudge if no /clientmaster upload this week."""
    today = datetime.now(TASHKENT)
    if today.weekday() != 6:  # 6 = Sunday
        return
    try:
        import sqlite3
        DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """SELECT COUNT(*) AS n FROM master_upload_log
               WHERE uploaded_at >= datetime('now', '-6 days')"""
        ).fetchone()
        conn.close()
        if row and row["n"]:
            return  # already uploaded this week
        await bot.send_message(
            chat_id,
            "⏰ <b>Yakshanba nudge</b>\n\n"
            "Bu hafta Client Masterni yuklamadingiz. "
            "Tahrirlaringiz bor bo'lsa, <code>/clientmaster</code> caption bilan yuboring. "
            "Yoki keyingi dushanbani kuting.",
            parse_mode="HTML",
        )
        logger.info(f"Master sync nudge sent to {chat_id}")
    except Exception as e:
        logger.error(f"Master sync nudge failed: {e}")


async def _send_owner_morning_brief(bot, _ignored_chat_id: int) -> None:
    """09:00 Tashkent — owner daily reconciliation brief.

    Targets are configured via OWNER_DAILY_BRIEF_TARGETS env var (list of
    chat IDs — groups or users). The chat_id argument from run_daily_reminder
    is ignored: this reminder fans out to its own configured targets, not
    to the generic admin chat. Quiet on a fully-empty day.

    Notion Command Center feature backlog A2 (2026-05-11).
    """
    from backend.services.group_config import OWNER_DAILY_BRIEF_TARGETS
    if not OWNER_DAILY_BRIEF_TARGETS:
        # Feature inert by design — nothing configured, nothing sent.
        logger.debug("owner_morning_brief: no targets configured, skipping")
        return

    today = datetime.now(TASHKENT)
    ok, reason = _should_send(today)
    if not ok:
        logger.info(f"Owner morning brief skipped: {reason}")
        return

    try:
        from backend.database import get_db
        from backend.services.owner_brief import (
            gather_brief, render_brief, is_quiet_day,
        )
        conn = get_db()
        try:
            data = gather_brief(conn)
        finally:
            conn.close()

        if is_quiet_day(data):
            logger.info(
                f"owner_morning_brief: quiet day for {data['for_date']} "
                f"— no message sent"
            )
            return

        text = render_brief(data)
        sent = 0
        for target in OWNER_DAILY_BRIEF_TARGETS:
            try:
                await bot.send_message(target, text, parse_mode="HTML")
                sent += 1
            except Exception as e:
                logger.warning(
                    f"owner_morning_brief: failed to send to {target}: {e}"
                )
        logger.info(
            f"owner_morning_brief: sent to {sent}/{len(OWNER_DAILY_BRIEF_TARGETS)} "
            f"targets for {data['for_date']} "
            f"(uzs_cash={data['cash_uzs_total']:.0f}, "
            f"anomalies={len(data['overdue_debtors']) + (1 if data['out_of_stock_count'] else 0) + len(data['silent_regulars'])})"
        )
    except Exception as e:
        logger.error(f"owner_morning_brief failed: {e}", exc_info=True)


async def _send_cashbook_today_list(bot, chat_id: int) -> None:
    """19:00 Tashkent — post today's per-client payment list to the
    cashier group. One row per client (combined UZS + USD), sorted by
    first-payment time. Text-only (no inline keyboard — cashiers use
    /bugunpul on demand to edit/cancel). Quiet on a 0-row day."""
    if not chat_id:
        return
    today = datetime.now(TASHKENT)
    ok, reason = _should_send(today)
    if not ok:
        logger.info(f"Cashbook today-list skipped: {reason}")
        return
    try:
        from backend.database import get_db
        from bot.handlers.cashier import (
            _today_intake_rows,
            _aggregate_today_by_client,
            _render_today_by_client,
        )
        conn = get_db()
        try:
            date, rows = _today_intake_rows(conn)
        finally:
            conn.close()
        if not rows:
            logger.info("Cashbook today-list: 0 rows — staying quiet")
            return
        clients = _aggregate_today_by_client(rows)
        # Chunk to stay under Telegram's 4096-char cap on busy days
        # (Error Log #83 — /bugunpul hit it first; same growth applies here).
        from bot.shared import chunk_message
        for chunk in chunk_message(_render_today_by_client(date, clients)):
            await bot.send_message(chat_id, chunk, parse_mode="HTML")
        logger.info(
            f"Cashbook today-list sent: {len(clients)} clients, "
            f"{sum(c['count'] for c in clients)} payments"
        )
    except Exception as e:
        logger.error(f"Cashbook today-list failed: {e}")


async def _run_payment_notif_sweeper(bot, admin_chat_id: int) -> None:
    """Daily 18:00 Tashkent — sweep pending payment notifications that are
    >24h old. Runs after 17:00 EOD uploads check, so any legitimately queued
    notification has had a full window for /debtors to land first.
    Posts a short summary to Admin group only if rows were actually swept."""
    from backend.services.payment_notifications import sweep_stale
    try:
        result = sweep_stale()
        swept = int(result.get("swept", 0))
        if swept <= 0:
            return
        await bot.send_message(
            admin_chat_id,
            f"🧹 <b>Payment notifications sweep</b>\n\n"
            f"{swept} ta pending bildirishnoma 24 soatdan ko'p kutdi — "
            f"<code>missed_notifications</code>'ga o'tkazildi.\n\n"
            f"Ko'rish: <code>/missed</code>",
            parse_mode="HTML",
        )
        logger.info(f"payment_notif_sweeper: swept {swept} stale rows")
    except Exception as e:
        logger.error(f"payment_notif_sweeper failed: {e}")


async def _run_group_health_check(bot, admin_chat_id: int) -> None:
    """Daily 09:05 — check every forwarding group is still reachable.

    When a regular group is upgraded to a supergroup, its chat_id changes and
    the bot silently fails to post there (Telegram returns 400 "group chat
    was upgraded to a supergroup chat"). We learned this the hard way on
    2026-04-21 when "Taklif va Xatolar" feedback was silently dropping.

    For each configured group, we call getChat + dryrun a send test. If
    Telegram reports the group was migrated, we alert Admin with the new
    chat_id so it can be updated. If the bot was removed or the group
    deleted, we also alert Admin.
    """
    from bot.shared import (
        ADMIN_GROUP_CHAT_ID, PLATFORM_OPS_GROUP_CHAT_ID, DAILY_GROUP_CHAT_ID,
        INVENTORY_GROUP_CHAT_ID, ORDER_GROUP_CHAT_ID, AGENTS_GROUP_CHAT_ID,
        ERRORS_GROUP_CHAT_ID,
    )
    groups = [
        ("Admin",            ADMIN_GROUP_CHAT_ID),
        ("Platform-Ops",     PLATFORM_OPS_GROUP_CHAT_ID),
        ("Daily",            DAILY_GROUP_CHAT_ID),
        ("Inventory",        INVENTORY_GROUP_CHAT_ID),
        ("Orders/Sales",     ORDER_GROUP_CHAT_ID),
        ("Agents",           AGENTS_GROUP_CHAT_ID),
        ("Taklif va Xatolar", ERRORS_GROUP_CHAT_ID),
    ]
    issues: list[str] = []
    for label, cid in groups:
        try:
            await bot.get_chat(cid)
        except Exception as e:
            msg = str(e)
            # Check for supergroup migration
            if "migrate" in msg.lower() or "supergroup" in msg.lower():
                # Try to extract the new chat_id Telegram returns
                import re
                m = re.search(r"migrate_to_chat_id[\"': ]+(-?\d+)", msg)
                new_id = m.group(1) if m else "unknown"
                issues.append(
                    f"• <b>{label}</b> ({cid}) was upgraded to supergroup.\n"
                    f"  New chat_id: <code>{new_id}</code>\n"
                    f"  Update the env var or default and redeploy."
                )
            elif "not found" in msg.lower() or "bot was kicked" in msg.lower() or "chat not found" in msg.lower():
                issues.append(f"• <b>{label}</b> ({cid}): BOT REMOVED or group deleted. {msg[:150]}")
            else:
                issues.append(f"• <b>{label}</b> ({cid}): {msg[:200]}")
    if not issues:
        logger.info(f"group_health_check: all {len(groups)} groups reachable")
        return
    text = (
        "🚨 <b>Group health check — FAILURES</b>\n\n"
        "One or more forwarding groups are unreachable. Messages sent to "
        "these groups will be silently dropped until fixed.\n\n"
        + "\n\n".join(issues)
    )
    try:
        await bot.send_message(admin_chat_id, text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"group_health_check: failed to alert Admin: {e}")


def _fmt_uzs(amount: float) -> str:
    """1234567 → '1 234 567'."""
    return f"{int(round(amount or 0)):,}".replace(",", " ")


def _fmt_usd(amount: float) -> str:
    """123.5 → '123.50'."""
    return f"{float(amount or 0):,.2f}"


async def _run_payment_reconciler(bot, target_chat_id: int) -> None:
    """Daily 08:00 Tashkent — refresh payment_reconciliation and post the
    morning mismatch report to the dedicated reconciliation group.

    Bookkeeper (Alisher) uploads the 1C /cash file at end of day; we
    rerun the reconciler at 08:00 the next morning (idempotent within a
    day) so the snapshot reflects last night's upload, then post a
    yesterday-only breakdown: matched count + bot_only rows (cashier saw,
    1C didn't) + kassa_only rows (1C has, cashier didn't).

    Always posts — even on clean days, so the reconciler's heartbeat is
    visible and failures are noticed.
    """
    from backend.database import get_db
    from backend.services.payment_reconciler import (
        reconcile_payments,
        get_yesterday_client_totals,
        find_unrecorded_discounts,
    )
    from bot.shared import html_escape

    conn = get_db()
    try:
        summary = reconcile_payments(conn, lookback_days=30)
        detail = get_yesterday_client_totals(conn, summary["reconcile_date"])
        unrecorded = find_unrecorded_discounts(conn, summary["reconcile_date"])
    except Exception as e:
        logger.exception("payment_reconciler failed: %s", e)
        try:
            await bot.send_message(
                target_chat_id,
                f"⚠️ Payment reconciler failed: <code>{e}</code>",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return
    finally:
        conn.close()

    logger.info("payment_reconciler done: %s", summary)

    report_date = detail["report_date"]
    fx_rate = detail["fx_rate"]
    fx_source = detail["fx_source"]
    matched_clients = detail["matched_clients"]
    mismatched = detail["mismatched"]
    orphans = detail["orphan_onec_rows"]
    discounts = detail.get("discounts", [])
    # Unrecorded discounts are an issue (Alisher forgot to book it); 1C-only
    # discounts that ARE booked are just info and don't break "clean".
    clean = not mismatched and not orphans and not unrecorded

    fx_note = (
        f"FX (kecha): <b>{_fmt_uzs(fx_rate)}</b> so'm/$"
        + ("" if fx_source == "actual" else "  <i>(default — /fxrate yo'q)</i>")
    )

    lines = [
        f"🔁 <b>Kassa ↔ 1C sverka</b> — {report_date}",
        fx_note,
    ]
    if clean:
        lines.append(
            f"✅ Mos keldi: <b>{matched_clients}</b> ta mijoz. "
            f"Kechagi to'lovlar bo'yicha farq yo'q."
        )
    else:
        bits = [f"✅ Mos: <b>{matched_clients}</b> mijoz"]
        if mismatched:
            bits.append(f"⚠️ Farq: <b>{len(mismatched)}</b> mijoz")
        if orphans:
            bits.append(f"❓ 1C-da ulanmagan: <b>{len(orphans)}</b>")
        if unrecorded:
            bits.append(f"💸 Kiritilmagan chegirma: <b>{len(unrecorded)}</b>")
        lines.append(" · ".join(bits))

    def _fmt_amt(amount: float, currency: str) -> str:
        if currency == "USD":
            return f"${_fmt_usd(amount)}"
        return f"{_fmt_uzs(amount)} so'm"

    def _fmt_onec_amt(row: dict) -> str:
        if row["currency"] == "USD":
            return f"${_fmt_usd(row['amount_currency'])}"
        return f"{_fmt_uzs(row['amount_local'])} so'm"

    for i, m in enumerate(mismatched, 1):
        lines.append("")
        client = html_escape(m["client"] or "unknown")
        delta = m["delta_usd"]
        sign = "+" if delta > 0 else ""
        lines.append(
            f"<b>{i}) {client}</b>  "
            f"(farq: {sign}${_fmt_usd(delta)})"
        )

        cashier_total = f"${_fmt_usd(m['cashier_usd_eq'])}"
        if m["cashier_rows"]:
            leg_parts = [
                f"{r['time']} {_fmt_amt(r['amount'], r['currency'])}"
                for r in m["cashier_rows"]
            ]
            legs = "; ".join(leg_parts)
            lines.append(f"   Kassa: {cashier_total}  →  {legs}")
        else:
            lines.append("   Kassa: <i>(yozuv yo'q)</i>")

        onec_total = f"${_fmt_usd(m['onec_usd_eq'])}"
        if m["onec_rows"]:
            leg_parts = []
            for r in m["onec_rows"]:
                doc = html_escape(r["doc_no"] or "")
                leg_parts.append(f"{_fmt_onec_amt(r)} (#{doc})")
            legs = "; ".join(leg_parts)
            lines.append(f"   1C:    {onec_total}  →  {legs}")
        else:
            lines.append("   1C:    <i>(yozuv yo'q)</i>")

    if orphans:
        lines.append("")
        lines.append("❓ <b>1C-da bor, mijoz aniqlanmagan</b>:")
        for r in orphans:
            client = html_escape(r["client_name_1c"] or "?")
            doc = html_escape(r["doc_no"] or "")
            lines.append(f"• {client} — {_fmt_onec_amt(r)} (1C #{doc})")

    # Part B — real-order discounts Alisher hasn't booked into 1C касса.
    if unrecorded:
        lines.append("")
        lines.append(
            "💸 <b>Chegirma 1C ga kiritilmagan</b> "
            "(real orderda bor, kassada yo'q):"
        )
        for u in unrecorded[:15]:
            client = html_escape(u["client"] or "?")
            if u["amount"] is not None:
                amt = (
                    f"${_fmt_usd(u['amount'])}" if u["currency"] == "USD"
                    else f"{_fmt_uzs(u['amount'])} so'm"
                )
            else:
                amt = html_escape((u["comment"] or "").strip()[:40])
            age = u["age_days"]
            age_txt = "bugun" if age <= 0 else f"{age} kun"
            lines.append(f"• {client} — {amt}  <i>({age_txt})</i>")
        if len(unrecorded) > 15:
            lines.append(f"  …va yana {len(unrecorded) - 15} ta")

    # Part A — 1C-only discounts that ARE booked (info; kept out of the match).
    if discounts:
        def _fmt_disc(d: dict) -> str:
            if d["currency"] == "USD":
                return f"${_fmt_usd(d['amount_currency'])}"
            return f"{_fmt_uzs(d['amount_local'])} so'm"
        lines.append("")
        lines.append(f"💸 <i>1C chegirmalar (kecha): {len(discounts)} ta</i>")
        for d in discounts[:10]:
            client = html_escape(d["client"] or "?")
            lines.append(f"• {client} — {_fmt_disc(d)}")
        if len(discounts) > 10:
            lines.append(f"  …va yana {len(discounts) - 10} ta")

    text = "\n".join(lines)
    try:
        await bot.send_message(target_chat_id, text, parse_mode="HTML")
    except Exception as e:
        logger.error("payment_reconciler: failed to post morning report: %s", e)


def start_reminder_tasks(bot, chat_id: int) -> list[asyncio.Task]:
    """Launch the reminder and sync background tasks. Returns the task handles.

    `chat_id` here is the Admin group (kept for admin-only reminders).
    Daily-upload nudges (morning + EOD) go to the new Daily group.
    Stock alert goes to Inventory group.
    """
    from bot.shared import (
        ORDER_GROUP_CHAT_ID, INVENTORY_GROUP_CHAT_ID, DAILY_GROUP_CHAT_ID,
        CASHIER_GROUP_CHAT_ID, PLATFORM_OPS_GROUP_CHAT_ID,
        RECONCILIATION_GROUP_CHAT_ID,
    )
    ops = PLATFORM_OPS_GROUP_CHAT_ID
    reconciliation = RECONCILIATION_GROUP_CHAT_ID
    tasks = [
        # Daily-upload reminders → Daily group (were Admin group).
        asyncio.create_task(
            run_daily_reminder(bot, DAILY_GROUP_CHAT_ID, 9, 0, _send_morning_nudge),
            name="daily-upload-reminder",
        ),
        asyncio.create_task(
            run_daily_reminder(bot, DAILY_GROUP_CHAT_ID, 17, 0, _send_eod_check),
            name="daily-upload-eod-check",
        ),
        # Client-sync runs → Order/Sales group (unchanged).
        asyncio.create_task(
            run_daily_reminder(bot, ORDER_GROUP_CHAT_ID, 6, 0, _run_daily_client_sync),
            name="daily-client-sync",
        ),
        # Weekly unlinked users → Admin group (admin concern, not daily ops).
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 17, 0, _send_weekly_unlinked),
            name="weekly-unlinked-reminder",
        ),
        # Weekly ops scorecard → Platform-Ops group (Monday 09:00, the learning ritual).
        asyncio.create_task(
            run_daily_reminder(bot, ops, 9, 0, _send_weekly_scorecard),
            name="weekly-ops-scorecard",
        ),
        # Daily stock alert → Inventory group.
        asyncio.create_task(
            run_daily_reminder(bot, INVENTORY_GROUP_CHAT_ID, 8, 0, _send_stock_alert),
            name="daily-stock-alert",
        ),
        # Client Master weekly sync cycle → Platform-Ops group.
        # Whole cycle (Mon xlsx / Fri prompt / Sun nudge) moves together so
        # the "reply with /clientmaster" UX stays in one chat.
        asyncio.create_task(
            run_daily_reminder(bot, ops, 8, 0, _send_master_auto_export),
            name="master-mon-auto-export",
        ),
        asyncio.create_task(
            run_daily_reminder(bot, ops, 17, 0, _send_master_sync_prompt),
            name="master-fri-sync-prompt",
        ),
        asyncio.create_task(
            run_daily_reminder(bot, ops, 12, 0, _send_master_sync_nudge),
            name="master-sun-sync-nudge",
        ),
        # Daily 03:00 — offsite DB backup → Platform-Ops group (durability
        # beyond Railway volume).
        asyncio.create_task(
            run_daily_reminder(bot, ops, 3, 0, _send_offsite_db_backup),
            name="daily-offsite-db-backup",
        ),
        # Daily 09:00 — data-consistency audit → Platform-Ops group (silent
        # when clean).
        asyncio.create_task(
            run_daily_reminder(bot, ops, 9, 0, _run_consistency_audit),
            name="daily-consistency-audit",
        ),
        # Daily 09:00 — check if the GitHub PAT is 75+ days old and nudge
        # Admin group to rotate (silent if stamp missing or fresh).
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 9, 0, _send_pat_rotation_reminder),
            name="daily-pat-rotation-reminder",
        ),
        # Daily 09:05 — verify every forwarding group is still reachable.
        # Catches supergroup migrations and bot-removal silently-drops.
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 9, 5, _run_group_health_check),
            name="daily-group-health-check",
        ),
        # One-shot 09:00 on 2026-05-24 — P6 follow-up reminder → Platform-Ops.
        # Function gates by date; no-ops on every other day. Wiring kept in
        # the daily loop for simplicity (idempotent no-op after target date).
        # Set 2026-05-18 (Session Ops, Error Log #56 prevention measures).
        asyncio.create_task(
            run_daily_reminder(bot, ops, 9, 0, _send_p6_followup_reminder),
            name="oneshot-p6-followup-2026-05-24",
        ),
        # Daily 18:00 — sweep stale pending payment notifications → Platform-Ops
        # group. Runs one hour after EOD check so /debtors has had its full window.
        asyncio.create_task(
            run_daily_reminder(bot, ops, 18, 0, _run_payment_notif_sweeper),
            name="daily-payment-notif-sweeper",
        ),
        # Daily 08:00 — cashier↔1C reconciler → dedicated Reconciliation
        # group. Refreshes payment_reconciliation (also feeds the Kabinet's
        # per-row "Tekshirish kerak" flag) and posts yesterday's matched /
        # bot_only / kassa_only breakdown for bookkeeper review. Runs after
        # Alisher's EOD /cash upload so the morning report reflects last
        # night's 1C input. Always posts — clean days included — so silent
        # failures are visible.
        asyncio.create_task(
            run_daily_reminder(bot, reconciliation, 8, 0, _run_payment_reconciler),
            name="daily-payment-reconciler",
        ),
        # Daily 19:00 — Per-client payment list into the cashier group.
        # One row per client (combined UZS + USD), sorted by first-payment
        # time. Text-only; quiet on a 0-row day.
        asyncio.create_task(
            run_daily_reminder(
                bot,
                CASHIER_GROUP_CHAT_ID,
                19, 0,
                _send_cashbook_today_list,
            ),
            name="daily-cashbook-today-list",
        ),
        # Daily 04:30 — refresh catalog units_score (rolling 30/60d windows).
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 4, 30, _recompute_units_score),
            name="daily-units-score-recompute",
        ),
        # Sunday 09:00 — infra-maintenance day reminder → Platform-Ops group.
        # Fires daily at 09:00 but no-ops Mon–Sat (internal date gate). On
        # first Sunday of Mar/Jun/Sep/Dec, the message also includes the
        # quarterly deep-audit nudge. Established 2026-05-17 per the
        # "every Sunday is infrastructure day" rhythm.
        asyncio.create_task(
            run_daily_reminder(bot, ops, 9, 0, _send_sunday_infra_nudge),
            name="sunday-infra-nudge",
        ),
        # Daily 09:00 — owner daily reconciliation brief to configured
        # targets (OWNER_DAILY_BRIEF_TARGETS env). Fans out to its own
        # target list, not the generic admin chat — the chat_id arg here
        # is just to satisfy the run_daily_reminder signature.
        # Notion Command Center feature backlog A2 (2026-05-11).
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 9, 0, _send_owner_morning_brief),
            name="daily-owner-morning-brief",
        ),
    ]
    logger.info(
        f"Started {len(tasks)} background tasks "
        f"(admin={chat_id}, ops={ops}, daily={DAILY_GROUP_CHAT_ID}, "
        f"sales={ORDER_GROUP_CHAT_ID}, inventory={INVENTORY_GROUP_CHAT_ID}, "
        f"reconciliation={reconciliation})"
    )
    return tasks
