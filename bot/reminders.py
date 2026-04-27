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
import os
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

from backend.admin_auth import get_admin_key

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
    ORDER_GROUP_CHAT_ID = int(os.getenv("ORDER_GROUP_CHAT_ID", "-1003740010463"))

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


async def _send_stock_alert(bot, chat_id: int) -> None:
    """09:00 daily — work-week-cumulative inventory alert: items that ran out
    on or after this week's Monday 00:00 Tashkent. Resets each Monday.
    Cumulative TUGAGAN list stays available on demand via /stockalert tugagan.
    Silent when nothing has run out this week yet (Monday-fresh)."""
    today = datetime.now(TASHKENT)
    ok, reason = _should_send(today)
    if not ok:
        logger.info(f"Stock alert skipped: {reason}")
        return

    try:
        from backend.services.stock_alerts import get_stock_alerts, format_daily_inventory_message
        alerts = get_stock_alerts()
        if alerts["active_count"] == 0:
            logger.info("Stock alert skipped — no active products detected")
            return
        messages = format_daily_inventory_message(alerts)
        if not messages:
            logger.info(
                f"Stock alert skipped — nothing has run out and no sales yet this week "
                f"(cumulative tugagan: {len(alerts['out_of_stock'])})"
            )
            return
        for text in messages:
            await bot.send_message(chat_id, text, parse_mode="HTML")
        logger.info(
            f"Stock alert sent ({len(messages)} msg): "
            f"weekly_out={len(alerts['weekly_out_of_stock'])}, "
            f"top_sellers={len(alerts.get('weekly_top_sellers', []))}, "
            f"cumulative_oos={len(alerts['out_of_stock'])}"
        )
    except Exception as e:
        logger.error(f"Stock alert failed: {e}")


async def _run_consistency_audit(bot, chat_id: int) -> None:
    """Daily 04:00 — run the data-consistency audit and post to Admin group
    if any issues are found. Silent if everything looks clean."""
    try:
        from backend.services.consistency_audit import run_audit, format_audit_message
        findings = run_audit()
        msg = format_audit_message(findings)
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
        from datetime import datetime as _dt, date as _date
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
    groups = [
        ("Admin",            int(os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051"))),
        ("Daily",            int(os.getenv("DAILY_GROUP_CHAT_ID", "-5243912135"))),
        ("Inventory",        int(os.getenv("INVENTORY_GROUP_CHAT_ID", "-5133871411"))),
        ("Orders/Sales",     int(os.getenv("ORDER_GROUP_CHAT_ID", "-1003740010463"))),
        ("Agents",           int(os.getenv("AGENTS_GROUP_CHAT_ID", "-1003922400481"))),
        ("Taklif va Xatolar", int(os.getenv("ERRORS_GROUP_CHAT_ID", "-1003896597497"))),
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


def start_reminder_tasks(bot, chat_id: int) -> list[asyncio.Task]:
    """Launch the reminder and sync background tasks. Returns the task handles.

    `chat_id` here is the Admin group (kept for admin-only reminders).
    Daily-upload nudges (morning + EOD) go to the new Daily group.
    Stock alert goes to Inventory group.
    """
    ORDER_GROUP_CHAT_ID = int(os.getenv("ORDER_GROUP_CHAT_ID", "-1003740010463"))
    INVENTORY_GROUP_CHAT_ID = int(os.getenv("INVENTORY_GROUP_CHAT_ID", "-5133871411"))
    DAILY_GROUP_CHAT_ID = int(os.getenv("DAILY_GROUP_CHAT_ID", "-5243912135"))
    tasks = [
        # Daily-upload reminders → Daily group (were Admin group).
        asyncio.create_task(
            run_daily_reminder(bot, DAILY_GROUP_CHAT_ID, 17, 0, _send_morning_nudge),
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
        # Daily stock alert → Inventory group.
        asyncio.create_task(
            run_daily_reminder(bot, INVENTORY_GROUP_CHAT_ID, 9, 0, _send_stock_alert),
            name="daily-stock-alert",
        ),
        # Client Master weekly sync cycle (sender checks weekday internally).
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 8, 0, _send_master_auto_export),
            name="master-mon-auto-export",
        ),
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 17, 0, _send_master_sync_prompt),
            name="master-fri-sync-prompt",
        ),
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 12, 0, _send_master_sync_nudge),
            name="master-sun-sync-nudge",
        ),
        # Daily 03:00 — offsite DB backup to Admin group (durability beyond
        # Railway volume).
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 3, 0, _send_offsite_db_backup),
            name="daily-offsite-db-backup",
        ),
        # Daily 09:00 — data-consistency audit (silent when clean).
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 9, 0, _run_consistency_audit),
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
        # Daily 18:00 — sweep stale pending payment notifications (Session N).
        # Runs one hour after EOD check so /debtors has had its full window.
        asyncio.create_task(
            run_daily_reminder(bot, chat_id, 18, 0, _run_payment_notif_sweeper),
            name="daily-payment-notif-sweeper",
        ),
    ]
    logger.info(
        f"Started {len(tasks)} background tasks "
        f"(admin={chat_id}, daily={DAILY_GROUP_CHAT_ID}, "
        f"sales={ORDER_GROUP_CHAT_ID}, inventory={INVENTORY_GROUP_CHAT_ID})"
    )
    return tasks
