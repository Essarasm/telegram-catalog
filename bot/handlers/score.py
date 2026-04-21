"""Session G: Credit Scoring bot commands.

Commands: /clientscore, /runscore, /payments, /scorestats, /adjustscore, /scoreanomalies
"""
import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.shared import get_db, html_escape, is_admin, log_admin_action

logger = logging.getLogger("bot")
router = Router(name="score")


@router.message(Command("clientscore"))
async def cmd_clientscore(message: Message):
    """Look up credit score for a client. Usage: /clientscore <name_substring>"""
    if not is_admin(message):
        return

    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.answer(
            "Использование: /clientscore <имя или #ID>\n"
            "Примеры:\n"
            "  /clientscore Бахром\n"
            "  /clientscore #142"
        )
        return

    query = args[1].strip()
    try:
        from backend.services.credit_scoring import search_client_scores
        results = search_client_scores(query, limit=5)
    except Exception as e:
        logger.error(f"/clientscore error: {e}")
        await message.answer(f"Ошибка: {e}")
        return

    if not results:
        await message.answer(
            f"Клиент «{html_escape(query)}» не найден в системе скоринга.\n"
            "Запустите /runscore для пересчёта баллов.",
            parse_mode="HTML",
        )
        return

    for r in results:
        limit_str = "Ручной контроль" if r["volume_bucket"] == "Heavy" else f"{r['credit_limit_uzs']:,.0f} сўм"

        text = (
            f"📊 <b>Кредитный балл: {html_escape(r['client_name'])}</b>\n"
            f"\n"
            f"Балл: <b>{r['score']}</b> / 100 — <b>{html_escape(r['tier'])}</b>\n"
            f"Бакет: {html_escape(r['volume_bucket'])} (${r['monthly_volume_usd']:,.0f}/мес)\n"
            f"Лимит: {limit_str}\n"
            f"\n"
            f"── Факторы ──\n"
            f"Дисциплина:     {r['discipline_score']:5.1f} / 40  "
            f"({('мало данных' if r.get('on_time_rate', 0) < 0 else str(round(r['on_time_rate']*100)) + '% вовремя')})\n"
            f"Долг:           {r['debt_score']:5.1f} / 25  (коэфф. {r['debt_ratio']:.2f})\n"
            f"Регулярность:   {r['consistency_score']:5.1f} / 20  (CV = {r['consistency_cv']:.2f})\n"
            f"Стаж:           {r['tenure_score']:5.1f} / 15  ({r['tenure_months']:.0f} мес.)\n"
            f"\n"
            f"Последний пересчёт: {r['recalc_date']} {r['recalc_time']}"
        )
        await message.answer(text, parse_mode="HTML")


@router.message(Command("runscore"))
async def cmd_runscore(message: Message):
    """Manually trigger credit score recalculation for all clients."""
    if not is_admin(message):
        return
    log_admin_action(message, "runscore")

    status_msg = await message.answer("⏳ Пересчёт кредитных баллов...")

    try:
        from backend.services.credit_scoring import run_nightly_scoring
        result = run_nightly_scoring()
    except Exception as e:
        logger.error(f"/runscore error: {e}")
        await status_msg.edit_text(f"❌ Ошибка: {e}")
        return

    if not result.get("ok"):
        await status_msg.edit_text(f"❌ Ошибка: {result.get('error', 'unknown')}")
        return

    tiers = result.get("tiers", {})
    buckets = result.get("buckets", {})

    tier_lines = "\n".join(f"  {k}: {v}" for k, v in sorted(tiers.items()))
    bucket_lines = "\n".join(f"  {k}: {v}" for k, v in sorted(buckets.items()))

    pay_fix = result.get("payments_relinked", 0)
    debt_fix = result.get("debts_relinked", 0)
    relink_line = ""
    if pay_fix or debt_fix:
        relink_line = f"\n🔗 Привязано: платежей {pay_fix}, долгов {debt_fix}\n"

    text = (
        f"✅ <b>Скоринг завершён</b>\n"
        f"\n"
        f"Клиентов оценено: <b>{result['scored']}</b>\n"
        f"Курс USD/UZS: {result['fx_rate']:,.0f}\n"
        f"Дата: {result['date']}\n"
        f"{relink_line}"
        f"\n"
        f"<b>По уровням:</b>\n{tier_lines}\n"
        f"\n"
        f"<b>По бакетам:</b>\n{bucket_lines}"
    )
    await status_msg.edit_text(text, parse_mode="HTML")


@router.message(Command("payments"))
async def cmd_payments(message: Message):
    """View recent payments for a client. Usage: /payments <name_substring> [count]"""
    if not is_admin(message):
        return

    args = (message.text or "").split(maxsplit=2)
    if len(args) < 2 or not args[1].strip():
        await message.answer(
            "Использование: /payments <имя клиента> [кол-во]\n"
            "Пример: /payments Бахром 10"
        )
        return

    query = args[1].strip()
    limit = 10
    if len(args) > 2:
        try:
            limit = int(args[2].strip())
            limit = max(1, min(50, limit))
        except ValueError:
            pass

    conn = get_db()
    try:
        pattern = f"%{query}%"
        rows = conn.execute(
            """SELECT doc_number_1c, doc_date, client_name_1c,
                      currency, amount_local, amount_currency, corr_account
               FROM client_payments
               WHERE client_name_1c LIKE ?
               ORDER BY doc_date DESC
               LIMIT ?""",
            (pattern, limit),
        ).fetchall()

        if not rows:
            await message.answer(f"Платежи для «{html_escape(query)}» не найдены.")
            conn.close()
            return

        total = conn.execute(
            "SELECT COUNT(*) as c FROM client_payments WHERE client_name_1c LIKE ?",
            (pattern,),
        ).fetchone()["c"]

        lines = [f"💰 <b>Платежи: {html_escape(rows[0]['client_name_1c'] or query)}</b>"]
        lines.append(f"Всего: {total} | Показано: {len(rows)}\n")

        for r in rows:
            if r["currency"] == "USD":
                amt = f"${r['amount_currency']:,.2f}"
            else:
                amt = f"{r['amount_local']:,.0f} UZS"
            lines.append(f"  {r['doc_date']}  {amt}  №{r['doc_number_1c']}")

        await message.answer("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"/payments error: {e}")
        await message.answer(f"Ошибка: {e}")
    finally:
        conn.close()


@router.message(Command("scorestats"))
async def cmd_scorestats(message: Message):
    """Show summary statistics from the latest scoring run."""
    if not is_admin(message):
        return

    try:
        from backend.services.credit_scoring import get_scoring_summary
        summary = get_scoring_summary()
    except Exception as e:
        logger.error(f"/scorestats error: {e}")
        await message.answer(f"Ошибка: {e}")
        return

    if not summary.get("ok"):
        await message.answer("Данных скоринга ещё нет. Запустите /runscore.")
        return

    tiers = summary.get("tiers", {})
    buckets = summary.get("buckets", {})

    tier_lines = "\n".join(f"  {k}: {v}" for k, v in sorted(tiers.items()))
    bucket_lines = "\n".join(f"  {k}: {v}" for k, v in sorted(buckets.items()))

    text = (
        f"📊 <b>Статистика скоринга</b>\n"
        f"\n"
        f"Дата пересчёта: {summary['date']}\n"
        f"Всего клиентов: <b>{summary['total_clients']}</b>\n"
        f"Средний балл: <b>{summary['avg_score']}</b>\n"
        f"\n"
        f"<b>По уровням:</b>\n{tier_lines}\n"
        f"\n"
        f"<b>По бакетам:</b>\n{bucket_lines}"
    )
    await message.answer(text, parse_mode="HTML")


@router.message(Command("adjustscore"))
async def cmd_adjustscore(message: Message):
    """Manual score adjustment. Usage: /adjustscore <name> <delta> <reason>"""
    if not is_admin(message):
        return

    args = message.text.split(None, 3)
    if len(args) < 4:
        await message.answer(
            "Использование: /adjustscore <имя> <дельта> <причина>\n"
            "Примеры:\n"
            "  /adjustscore Бахром +15 Задержка сотрудника при вводе\n"
            "  /adjustscore #142 -20 Возврат товара\n\n"
            "Дельта: от -50 до +50. Действует 30 дней."
        )
        return

    query = args[1].strip()
    try:
        delta = int(args[2])
    except ValueError:
        await message.answer("Дельта должна быть числом от -50 до +50.")
        return

    reason = args[3].strip()

    try:
        from backend.services.credit_scoring import search_client_scores, apply_score_adjustment
        results = search_client_scores(query, limit=1)
    except Exception as e:
        await message.answer("Ошибка поиска: " + str(e))
        return

    if not results:
        await message.answer("Клиент не найден в системе скоринга.")
        return

    client = results[0]
    cid = client["client_id"]
    cname = client["client_name"]

    admin_name = ""
    if message.from_user:
        admin_name = message.from_user.full_name or message.from_user.username or ""
    admin_id = message.from_user.id if message.from_user else 0

    result = apply_score_adjustment(
        client_id=cid,
        client_name=cname,
        delta=delta,
        reason=reason,
        admin_user_id=admin_id,
        admin_name=admin_name,
    )

    if not result.get("ok"):
        await message.answer("Ошибка: " + result.get("error", "unknown"))
        return

    sign = "+" if delta > 0 else ""
    new_score = max(0, min(100, client["score"] + delta))
    text = (
        "✅ <b>Корректировка балла</b>\n\n"
        "Клиент: " + html_escape(cname) + "\n"
        "Текущий балл: " + str(client["score"]) + "\n"
        "Дельта: <b>" + sign + str(delta) + "</b>\n"
        "Новый балл (при пересчёте): ~" + str(new_score) + "\n"
        "Причина: " + html_escape(reason) + "\n"
        "Истекает: " + result["expires_at"] + "\n"
        "Админ: " + html_escape(admin_name)
    )
    await message.answer(text, parse_mode="HTML")


@router.message(Command("scoreanomalies"))
async def cmd_scoreanomalies(message: Message):
    """Weekly anomaly report: clients with score drops + stale payment data."""
    if not is_admin(message):
        return

    try:
        from backend.services.credit_scoring import detect_anomalies
        anomalies = detect_anomalies()
    except Exception as e:
        await message.answer("Ошибка: " + str(e))
        return

    if not anomalies:
        await message.answer(
            "✅ <b>Аномалии не обнаружены</b>\n\n"
            "Все клиенты с падением балла имеют свежие данные о платежах.",
            parse_mode="HTML",
        )
        return

    lines = ["⚠️ <b>Аномалии скоринга</b> (" + str(len(anomalies)) + ")\n"]
    lines.append("Клиенты с падением балла и устаревшими данными Кассы:\n")

    for a in anomalies[:15]:
        lines.append(
            "• <b>" + html_escape(a["client_name"]) + "</b> "
            "[" + a["volume_bucket"] + "]\n"
            "  Балл: " + str(a["previous_score"]) + " → " + str(a["current_score"]) + " "
            "(−" + str(a["drop"]) + ")\n"
            "  Посл. оплата: " + str(a["last_payment"]) + "\n"
            "  Посл. отгрузка: " + str(a["last_order"])
        )

    if len(anomalies) > 15:
        lines.append("\n... и ещё " + str(len(anomalies) - 15))

    lines.append("\nДействие: проверьте, не забыли ли сотрудники внести платежи в Кассу.")

    await message.answer("\n".join(lines), parse_mode="HTML")


# ── Session L: Points Simulation ─────────────────────────────────

@router.message(Command("simpoints"))
async def cmd_simpoints(message: Message):
    """Simulate per-producer tiered points for Jan 2025 – Mar 2026."""
    if not is_admin(message):
        return

    status_msg = await message.answer("⏳ 15 oy uchun ball simulyatsiyasi...")
    try:
        from backend.services.points_simulation import simulate_all_months
        result = simulate_all_months()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ {result.get('error', 'Xatolik')}")
            return

        months = result.get("months", [])
        if not months:
            await status_msg.edit_text("Ma'lumot topilmadi.")
            return

        lines = [
            "📊 <b>Ball simulyatsiyasi: Flat vs Tiered</b>\n",
            "<b>Oy      | Mijoz | Flat      | Tiered    | Farq</b>",
        ]

        total_flat = 0
        total_tiered = 0
        for m in months:
            total_flat += m["flat_total"]
            total_tiered += m["tiered_total"]
            diff_sign = "+" if m["difference"] >= 0 else ""
            lines.append(
                f"{m['month']} | {m['clients']:>4}  | {m['flat_total']:>9,} | {m['tiered_total']:>9,} | {diff_sign}{m['diff_pct']}%"
            )

        diff_total = total_tiered - total_flat
        diff_pct = round((total_tiered / total_flat - 1) * 100, 1) if total_flat > 0 else 0
        lines.append(f"\n<b>JAMI:</b>")
        lines.append(f"  Flat:   {total_flat:>12,}")
        lines.append(f"  Tiered: {total_tiered:>12,}")
        lines.append(f"  Farq:   {'+' if diff_total >= 0 else ''}{diff_total:,} ({'+' if diff_pct >= 0 else ''}{diff_pct}%)")

        # Top 10 for latest month
        top10 = result.get("latest_top_10", [])
        if top10:
            lines.append(f"\n🏆 <b>Top 10 — {result['latest_month']} (tiered):</b>")
            for i, c in enumerate(top10[:10]):
                flat = c["flat_effective"]
                tiered = c["tiered_effective"]
                diff = tiered - flat
                lines.append(
                    f"  {i+1}. {html_escape(c['client_name'][:22])} — "
                    f"{tiered:,} ({c['discipline_grade']}) "
                    f"[flat: {flat:,}, {'+' if diff >= 0 else ''}{diff:,}]"
                )

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"/simpoints error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


# ── Session L: Loyalty Points Commands ───────────────────────────

@router.message(Command("calcpoints"))
async def cmd_calcpoints(message: Message):
    """Calculate loyalty points for a month. Usage: /calcpoints [YYYY-MM]"""
    if not is_admin(message):
        return

    args = (message.text or "").split(maxsplit=1)
    month_str = args[1].strip() if len(args) > 1 else None

    status_msg = await message.answer("⏳ Ball hisoblanyapti...")
    try:
        from backend.services.loyalty_points import calculate_monthly_points
        result = calculate_monthly_points(month_str)

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ {result.get('error', 'Xatolik')}")
            return

        grades = result.get("grades", {})
        grade_lines = "\n".join(f"  {g}: {c}" for g, c in sorted(grades.items()) if c)

        text = (
            f"✅ <b>Ball hisoblandi</b>\n\n"
            f"Oy: <b>{result['month']}</b>\n"
            f"Mijozlar: <b>{result['scored']}</b>\n"
            f"Jami ball: <b>{result['total_points']:,}</b>\n\n"
            f"<b>Intizom darajalari:</b>\n{grade_lines}"
        )
        await status_msg.edit_text(text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"/calcpoints error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


@router.message(Command("clientpoints"))
async def cmd_clientpoints(message: Message):
    """Look up loyalty points for a client. Usage: /clientpoints <name>"""
    if not is_admin(message):
        return

    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.answer("Foydalanish: /clientpoints <mijoz nomi>")
        return

    query = args[1].strip()
    conn = get_db()
    try:
        pattern = f"%{query}%"
        rows = conn.execute(
            """SELECT client_id, client_name, month, purchase_points,
                      discipline_grade, multiplier, clean_sheet_bonus,
                      effective_points, volume_bucket, bucket_rank, bucket_total
               FROM client_points_monthly
               WHERE client_name LIKE ?
               ORDER BY month DESC
               LIMIT 12""",
            (pattern,),
        ).fetchall()

        if not rows:
            await message.answer(f"'{html_escape(query)}' uchun ball topilmadi.\n/calcpoints bilan hisoblang.")
            return

        total = conn.execute(
            "SELECT SUM(effective_points) as t FROM client_points_monthly WHERE client_id = ?",
            (rows[0]["client_id"],),
        ).fetchone()

        client_name = rows[0]["client_name"]
        lines = [f"⭐ <b>{html_escape(client_name)}</b>\n"]
        lines.append(f"Jami ball: <b>{int(total['t'] or 0):,}</b>\n")

        for r in rows[:6]:
            rank_str = f" #{r['bucket_rank']}/{r['bucket_total']}" if r["bucket_rank"] else ""
            lines.append(
                f"  {r['month']} — <b>{r['effective_points']}</b> ball "
                f"({r['discipline_grade']} x{r['multiplier']:.1f}"
                f"{' +50' if r['clean_sheet_bonus'] else ''})"
                f" [{r['volume_bucket']}{rank_str}]"
            )

        await message.answer("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"/clientpoints error: {e}")
        await message.answer(f"Xatolik: {str(e)[:200]}")
    finally:
        conn.close()


@router.message(Command("leaderboard"))
async def cmd_leaderboard(message: Message):
    """Show points leaderboard. Usage: /leaderboard [bucket] [YYYY-MM]"""
    if not is_admin(message):
        return

    args = (message.text or "").split()
    bucket = None
    month = None
    for a in args[1:]:
        if a in ("Micro", "Small", "Medium", "Large", "Heavy"):
            bucket = a
        elif len(a) == 7 and a[4] == "-":
            month = a

    try:
        from backend.services.loyalty_points import get_leaderboard
        result = get_leaderboard(month, bucket)

        if not result.get("month"):
            await message.answer("Ball hali hisoblanmagan. /calcpoints buyrug'ini ishga tushiring.")
            return

        leaders = result["leaders"]
        if not leaders:
            await message.answer("Bu oy uchun natijalar yo'q.")
            return

        bucket_label = f" ({bucket})" if bucket else ""
        lines = [f"🏆 <b>Reyting — {result['month']}{bucket_label}</b>\n"]

        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        for i, r in enumerate(leaders[:10]):
            medal = medals[i] if i < 5 else f"{i+1}."
            grade = r["discipline_grade"]
            bonus = " +50" if r["clean_sheet_bonus"] else ""
            lines.append(
                f"{medal} <b>{html_escape(r['client_name'][:25])}</b> — "
                f"{r['effective_points']:,} ball "
                f"({grade}{bonus}) [{r['volume_bucket']}]"
            )

        await message.answer("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"/leaderboard error: {e}")
        await message.answer(f"Xatolik: {str(e)[:200]}")
