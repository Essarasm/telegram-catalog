"""Admin bot commands — extracted from bot/main.py for isolation."""
from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot.shared import get_db, html_escape, is_admin, logger

router = Router()


@router.message(Command("lastorders"))
async def cmd_lastorders(message: types.Message):
    """Show recent app-placed orders. Usage: /lastorders [N] (default 10)"""
    if not is_admin(message):
        return
    parts = (message.text or "").split()
    limit = 10
    if len(parts) > 1 and parts[1].isdigit():
        limit = min(int(parts[1]), 30)

    conn = get_db()
    rows = conn.execute(
        """SELECT o.id, o.created_at, o.total_uzs, o.total_usd,
                  o.item_count, o.status, o.parent_order_id,
                  ac.client_id_1c, o.placed_by_telegram_id,
                  u.first_name AS placer_name, u.is_agent
           FROM orders o
           LEFT JOIN allowed_clients ac ON ac.id = o.client_id
           LEFT JOIN users u ON u.telegram_id = o.placed_by_telegram_id
           ORDER BY o.id DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()

    if not rows:
        await message.reply("Hali buyurtmalar yo'q.")
        return

    lines = [f"📋 <b>Oxirgi {len(rows)} buyurtma</b>", ""]
    for r in rows:
        oid = r["id"]
        time_part = (r["created_at"] or "")[:16].replace("T", " ")
        client = r["client_id_1c"] or "—"
        totals = []
        if r["total_usd"] and r["total_usd"] > 0:
            totals.append(f"${r['total_usd']:,.2f}")
        if r["total_uzs"] and r["total_uzs"] > 0:
            totals.append(f"{int(r['total_uzs']):,} so'm".replace(",", " "))
        total_str = " + ".join(totals) or "—"
        items = r["item_count"] or 0
        parent = f" (asl: #{r['parent_order_id']})" if r["parent_order_id"] else ""
        prefix = "📦" if r["parent_order_id"] else "📋"
        status_icon = {"submitted": "🟡", "confirmed": "✅"}.get(r["status"] or "", "⚪")

        lines.append(f"{prefix} <b>#{oid}</b>{parent} {status_icon} {time_part}")
        lines.append(f"   {html_escape(client)} · {total_str} · {items} ta")
        if r["is_agent"] and r["placer_name"]:
            lines.append(f"   💼 Agent: {html_escape(r['placer_name'])}")
        lines.append("")

    await message.reply("\n".join(lines), parse_mode="HTML")



@router.message(Command("unlinked"))
async def cmd_unlinked(message: types.Message):
    """Show registered users who haven't been linked to a 1C client.

    Lists users with client_id=NULL and dismiss_status IS NULL (not
    tagged as demo/employee). Each row has inline buttons to link or
    dismiss.
    """
    if not is_admin(message):
        return

    conn = get_db()
    rows = conn.execute(
        """SELECT telegram_id, first_name, last_name, phone, registered_at,
                  username
           FROM users
           WHERE client_id IS NULL
             AND (dismiss_status IS NULL OR dismiss_status = '')
             AND phone IS NOT NULL AND phone != ''
           ORDER BY registered_at DESC
           LIMIT 20""",
    ).fetchall()
    conn.close()

    if not rows:
        await message.reply("✅ Barcha foydalanuvchilar bog'langan yoki belgilangan.")
        return

    lines = [f"👥 <b>Bog'lanmagan foydalanuvchilar ({len(rows)})</b>", ""]
    kb_rows: list[list[InlineKeyboardButton]] = []

    for i, r in enumerate(rows, 1):
        name = " ".join(filter(None, [r["first_name"], r["last_name"]])) or "—"
        phone = r["phone"] or "—"
        uname = f"@{r['username']}" if r["username"] else ""
        reg_date = (r["registered_at"] or "")[:10]
        lines.append(f"{i}. <b>{html_escape(name)}</b> · {phone} {uname}")
        if reg_date:
            lines.append(f"   📅 {reg_date}")

        tg_id = r["telegram_id"]
        kb_rows.append([
            InlineKeyboardButton(
                text=f"🔗 {name[:20]} → bog'lash",
                callback_data=f"ul:link:{tg_id}",
            ),
            InlineKeyboardButton(
                text="❌ Demo/Xodim",
                callback_data=f"ul:dismiss:{tg_id}",
            ),
        ])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
    await message.reply("\n".join(lines), parse_mode="HTML", reply_markup=kb)



@router.callback_query(F.data.startswith("ul:"))
async def on_unlinked_callback(cb: types.CallbackQuery):
    """Handle /unlinked inline buttons — link or dismiss."""
    if not is_admin(cb.message):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return

    data = (cb.data or "").split(":")
    if len(data) < 3:
        await cb.answer()
        return

    action = data[1]
    target_tg = data[2]

    if action == "dismiss":
        conn = get_db()
        conn.execute(
            "UPDATE users SET dismiss_status = 'demo_or_employee' WHERE telegram_id = ?",
            (int(target_tg),),
        )
        conn.commit()
        # Get name for confirmation
        row = conn.execute(
            "SELECT first_name FROM users WHERE telegram_id = ?", (int(target_tg),)
        ).fetchone()
        conn.close()
        name = row["first_name"] if row else target_tg
        await cb.answer(f"❌ {name} — demo/xodim deb belgilandi", show_alert=False)
        try:
            await cb.message.reply(
                f"❌ <b>{html_escape(str(name))}</b> (ID: <code>{target_tg}</code>) "
                f"— demo/xodim deb belgilandi. Keyingi /unlinked da ko'rinmaydi.",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    if action == "link":
        # Trigger the testclient search for this user's name
        conn = get_db()
        row = conn.execute(
            "SELECT first_name, phone FROM users WHERE telegram_id = ?", (int(target_tg),)
        ).fetchone()
        conn.close()
        if not row:
            await cb.answer("Foydalanuvchi topilmadi", show_alert=True)
            return
        name = row["first_name"] or ""
        await cb.answer(f"🔗 {name} uchun qidirish...", show_alert=False)
        # Send the testclient search prompt with the user's name
        try:
            await cb.message.reply(
                f"🔗 <b>{html_escape(name)}</b> (ID: <code>{target_tg}</code>) "
                f"uchun mijoz topish:\n\n"
                f"<code>/testclient link {target_tg} CLIENT_ID</code>\n\n"
                f"Yoki qidiring: <code>/testclient {html_escape(name)}</code>",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    await cb.answer("Noma'lum amal", show_alert=False)



@router.message(Command("makeagent"))
async def cmd_makeagent(message: types.Message):
    """Toggle users.is_agent for a given Telegram ID.

    Usage:
        /makeagent 652836922            — set is_agent=1
        /makeagent 652836922 off        — set is_agent=0
        /makeagent list                 — show all current agents
    """
    if not is_admin(message):
        return
    parts = (message.text or "").split()
    conn = get_db()
    try:
        if len(parts) == 2 and parts[1].lower() == "list":
            rows = conn.execute(
                "SELECT telegram_id, first_name, last_name, client_id "
                "FROM users WHERE is_agent = 1"
            ).fetchall()
            if not rows:
                await message.reply("Agentlar ro'yxati bo'sh.")
                return
            lines = [f"👔 <b>Agentlar ({len(rows)}):</b>", ""]
            for r in rows:
                name = " ".join(filter(None, [r["first_name"], r["last_name"]])) or "—"
                lines.append(f"  <code>{r['telegram_id']}</code> — {html_escape(name)}")
            await message.reply("\n".join(lines), parse_mode="HTML")
            return

        if len(parts) < 2 or not parts[1].isdigit():
            await message.reply(
                "Foydalanish:\n"
                "<code>/makeagent 652836922</code> — qo'shish\n"
                "<code>/makeagent 652836922 off</code> — o'chirish\n"
                "<code>/makeagent list</code> — ro'yxat",
                parse_mode="HTML",
            )
            return

        target_id = int(parts[1])
        turn_off = len(parts) >= 3 and parts[2].lower() in ("off", "0", "false")
        new_value = 0 if turn_off else 1

        conn.execute(
            "INSERT OR IGNORE INTO users (telegram_id, is_approved) VALUES (?, 1)",
            (target_id,),
        )
        conn.execute(
            "UPDATE users SET is_agent = ? WHERE telegram_id = ?",
            (new_value, target_id),
        )
        conn.commit()
        label = "✅ Agent qilindi" if new_value else "❌ Agent emas"
        await message.reply(f"{label}: <code>{target_id}</code>", parse_mode="HTML")
    finally:
        conn.close()



@router.message(Command("demand"))
async def cmd_demand(message: types.Message):
    """Show top out-of-stock products that clients are still ordering (demand signals)."""
    if not is_admin(message):
        return

    # Parse optional days argument: /demand 60
    parts = message.text.split()
    days = 30
    if len(parts) > 1 and parts[1].isdigit():
        days = min(int(parts[1]), 365)

    THRESHOLD = 5  # orders to be considered noteworthy

    conn = get_db()

    # Check if demand_signals table exists
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='demand_signals'"
    ).fetchall()]
    if not tables:
        conn.close()
        await message.reply(
            "ℹ️ Demand signals tizimi hali ishga tushmagan.\n"
            "Keyingi /prices yuklashdan so'ng ma'lumotlar yig'ila boshlaydi.",
        )
        return

    # Summary
    total_signals = conn.execute(
        "SELECT COUNT(*) FROM demand_signals WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    if total_signals == 0:
        conn.close()
        await message.reply(
            f"📊 Oxirgi {days} kun ichida tugagan mahsulotga buyurtma yo'q.\n\n"
            "Bu yaxshi — barcha buyurtmalar mavjud mahsulotlarga.",
        )
        return

    unique_products = conn.execute(
        "SELECT COUNT(DISTINCT product_id) FROM demand_signals WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    # Top demand signals
    top = conn.execute("""
        SELECT ds.product_id,
               COALESCE(p.name_display, p.name) as name,
               pr.name as producer,
               p.stock_status as current_stock,
               COUNT(DISTINCT ds.order_id) as order_count,
               SUM(ds.quantity) as total_qty,
               COUNT(DISTINCT ds.telegram_id) as unique_clients
        FROM demand_signals ds
        JOIN products p ON p.id = ds.product_id
        JOIN producers pr ON pr.id = p.producer_id
        WHERE ds.created_at >= datetime('now', ?)
        GROUP BY ds.product_id
        ORDER BY order_count DESC
        LIMIT 20
    """, (f"-{days} days",)).fetchall()

    conn.close()

    noteworthy = [r for r in top if r["order_count"] >= THRESHOLD]

    lines = [
        f"📊 <b>Talab signallari ({days} kun)</b>\n",
        f"🔔 Jami signallar: <b>{total_signals}</b>",
        f"📦 Mahsulotlar soni: <b>{unique_products}</b>",
    ]

    if noteworthy:
        lines.append(f"🔥 Muhim ({THRESHOLD}+ buyurtma): <b>{len(noteworthy)}</b>\n")
        lines.append(f"<b>⚠️ Diqqat — ko'p so'ralgan tugagan mahsulotlar:</b>")
        for i, r in enumerate(noteworthy, 1):
            stock_icon = "🔴" if r["current_stock"] == "out_of_stock" else "🟢"
            lines.append(
                f"  {i}. {stock_icon} <b>{html_escape(r['name'])}</b>"
                f"\n     {html_escape(r['producer'])} | "
                f"{r['order_count']} buyurtma, {r['total_qty']} dona, "
                f"{r['unique_clients']} mijoz"
            )
    else:
        lines.append(f"\nℹ️ Hali {THRESHOLD}+ buyurtmali mahsulot yo'q.")

    # Show rest of top items (below threshold)
    below = [r for r in top if r["order_count"] < THRESHOLD]
    if below:
        lines.append(f"\n<b>Boshqa signallar:</b>")
        for r in below[:10]:
            stock_icon = "🔴" if r["current_stock"] == "out_of_stock" else "🟢"
            lines.append(
                f"  {stock_icon} {html_escape(r['name'])} — "
                f"{r['order_count']} buyurtma ({r['unique_clients']} mijoz)"
            )
        if len(below) > 10:
            lines.append(f"  ... va yana {len(below) - 10} ta")

    lines.append(f"\n💡 /demand {days * 2} — ko'proq kunlik ma'lumot")

    await message.reply("\n".join(lines), parse_mode="HTML")



@router.message(Command("help"))
async def cmd_help(message: types.Message):
    """Show available admin commands."""
    await message.reply(
        "📋 <b>Admin buyruqlar:</b>\n\n"
        "<b>/add</b> <code>telefon ism joylashuv</code>\n"
        "Yangi mijozni qo'shish\n\n"
        "<b>/approve</b> <code>telegram_id</code>\n"
        "Foydalanuvchini tasdiqlash\n\n"
        "<b>/link</b> <code>telegram_id 1C_nomi_yoki_telefon</code>\n"
        "Foydalanuvchini mavjud 1C mijozga bog'lash\n\n"
        "<b>/list</b>\n"
        "Tasdiqlanmaganlar ro'yxati\n\n"
        "<b>/prices</b> (reply to Excel file)\n"
        "Narxlarni yangilash\n\n"
        "<b>/stock</b> (reply to Excel file)\n"
        "Inventarizatsiya (qoldiq) yangilash\n\n"
        "<b>/catalog</b> (reply to Excel file)\n"
        "Katalogni yangilash (yangi/o'chirilgan mahsulotlar)\n\n"
        "<b>/debtors</b> (reply to XLS file)\n"
        "Дебиторка yuklash (1C дебиторская задолженность)\n\n"
        "<b>/balances</b> (reply to XLS file)\n"
        "Оборотка yuklash (1C оборотно-сальдовая)\n\n"
        "<b>/demand</b> <code>[kunlar]</code>\n"
        "Tugagan mahsulotlarga talab signallari (default: 30 kun)\n\n"
        "<b>/realorders</b> (reply to XLS/XLSX file)\n"
        "Реализация yuklash (1C \"Реализация товаров\" — haqiqiy buyurtmalar)\n\n"
        "<b>/unmatchedclients</b>\n"
        "Haqiqiy buyurtmalardagi bog'lanmagan mijozlar ro'yxati (ko'p hujjatdan kam tomonga)\n\n"
        "<b>/unmatchedproducts</b>\n"
        "Haqiqiy buyurtmalardagi bog'lanmagan mahsulotlar ro'yxati (ko'p qatordan kam tomonga)\n\n"
        "<b>/relinkrealorders</b>\n"
        "Bog'lanmagan haqiqiy buyurtmalarni qayta bog'lash (allowed_clients yangilagandan keyin)\n\n"
        "<b>/ingestskus</b>\n"
        "Bog'lanmagan mahsulotlarni products jadvaliga qo'shish + real_order_items qayta bog'lash\n\n"
        "<b>/clientmaster</b> (reply to XLSX file)\n"
        "Client Master jadvalini allowed_clients ga import qilish (1C cyrillic nomlari + telefonlar)\n\n"
        "<b>/realordersample</b> <code>&lt;mijoz parchasi&gt;</code>\n"
        "Diagnostika: bitta haqiqiy buyurtmaning xom narx ustunlari (DB dump)\n\n"
        "<b>/backfillrealordertotals</b>\n"
        "Mavjud haqiqiy buyurtmalarda yo'qolgan jami narxlarni qayta hisoblash (1 marta ishlatiladi)\n\n"
        "<b>/backfillordernames</b>\n"
        "Eski wish-list buyurtmalaridagi nomlarni 1C Kirillcha variantiga o'tkazish (1 marta ishlatiladi)\n\n"
        "<b>/testclient</b> <code>[имя или #ID]</code>\n"
        "Test: link your account to a client's balance data\n\n"
        "<b>/duplicateclients</b> <code>[qidiruv]</code>\n"
        "Ko'p telefonli mijozlar auditi (bir 1C nom — bir nechta telefon)\n\n"
        "<b>/chatid</b>\n"
        "Chat va User ID ko'rish\n\n"
        "<b>/reports</b>\n"
        "Oxirgi xatolik xabarlari va mahsulot so'rovlari\n\n"
        "<b>/wrongphotos</b>\n"
        "Noto'g'ri rasm xabarlari (mahsulot bo'yicha)\n\n"
        "<b>/searches</b> <code>[kunlar]</code>\n"
        "Qidiruv statistikasi (default: 7 kun)\n\n"
        "<b>/datacoverage</b> <code>[valyuta]</code>\n"
        "Yuklangan ma'lumotlar qamrovi (oylik tekshiruv)\n\n"
        "<b>/realordersstats</b>\n"
        "Real orders sifat tahlili (match rates, agents, wish-list gap)\n\n"
        "<b>/wipewishlists</b> <code>[CONFIRM]</code>\n"
        "Demo wish-list ma'lumotlarini tozalash (launch oldidan)",
        parse_mode="HTML",
    )



@router.message(Command("reports"))
async def cmd_reports(message: types.Message):
    """Show recent issue reports and product requests."""
    if not is_admin(message):
        return

    conn = get_db()

    # Recent issue reports
    reports = conn.execute(
        """SELECT r.id, p.name_display, p.name, r.report_type, r.note, r.status, r.created_at
           FROM reports r
           JOIN products p ON p.id = r.product_id
           ORDER BY r.created_at DESC
           LIMIT 15""",
    ).fetchall()

    # Recent product requests
    requests = conn.execute(
        """SELECT id, request_text, status, created_at
           FROM product_requests
           ORDER BY created_at DESC
           LIMIT 10""",
    ).fetchall()
    conn.close()

    type_labels = {
        "wrong_photo": "📷 Rasm",
        "wrong_price": "💰 Narx",
        "wrong_name": "📝 Nom",
        "wrong_category": "📂 Kategoriya",
        "other": "❓ Boshqa",
    }

    status_icons = {
        "new": "🔴",
        "reviewed": "🟡",
        "fixed": "✅",
        "dismissed": "⚪",
    }

    lines = []

    if reports:
        lines.append(f"🚩 <b>Xatolik xabarlari ({len(reports)}):</b>\n")
        for r in reports:
            name = r["name_display"] or r["name"]
            tl = type_labels.get(r["report_type"], r["report_type"])
            si = status_icons.get(r["status"], "❓")
            line = f"#{r['id']} {si} {tl} — {name}"
            if r["note"]:
                line += f"\n   💬 {r['note'][:60]}"
            lines.append(line)
    else:
        lines.append("🚩 Xatolik xabarlari yo'q.")

    lines.append("")

    if requests:
        lines.append(f"🔍 <b>Mahsulot so'rovlari ({len(requests)}):</b>\n")
        for pr in requests:
            si = status_icons.get(pr["status"], "❓")
            lines.append(f"#{pr['id']} {si} {pr['request_text'][:80]}")
    else:
        lines.append("🔍 Mahsulot so'rovlari yo'q.")

    lines.append("\n🔴 new  🟡 reviewed  ✅ fixed  ⚪ dismissed")

    await message.reply("\n".join(lines), parse_mode="HTML")



@router.message(Command("wrongphotos"))
async def cmd_wrongphotos(message: types.Message):
    """Show wrong_photo reports grouped by product, sorted by priority (report count)."""
    if not is_admin(message):
        return

    conn = get_db()
    rows = conn.execute(
        """SELECT r.product_id, p.name_display, p.name, p.image_path,
                  COUNT(*) as cnt,
                  GROUP_CONCAT(r.id) as rids
           FROM reports r
           JOIN products p ON p.id = r.product_id
           WHERE r.report_type = 'wrong_photo' AND r.status = 'new'
           GROUP BY r.product_id
           ORDER BY cnt DESC""",
    ).fetchall()
    conn.close()

    if not rows:
        await message.reply("✅ Noto'g'ri rasm xabarlari yo'q (hammasi hal qilingan).")
        return

    total_reports = sum(r["cnt"] for r in rows)
    lines = [f"📷 <b>Noto'g'ri rasm xabarlari:</b> {total_reports} ta xabar, {len(rows)} ta mahsulot\n"]

    for r in rows:
        name = r["name_display"] or r["name"]
        has_photo = "🖼" if r["image_path"] else "❌"
        rids = r["rids"]
        lines.append(f"  {has_photo} <b>#{r['product_id']}</b> {name} — {r['cnt']}x (#{rids})")

    lines.append(f"\n💡 Rasmni o'chirish: PATCH /api/reports/ID/status {{\"status\": \"fixed\"}}")

    await message.reply("\n".join(lines), parse_mode="HTML")



@router.message(Command("searches"))
async def cmd_searches(message: types.Message):
    """Show search analytics: top queries, zero-result queries, and funnel stats."""
    if not is_admin(message):
        return

    # Parse optional days argument: /searches 30
    parts = message.text.split()
    days = 7
    if len(parts) > 1 and parts[1].isdigit():
        days = min(int(parts[1]), 365)

    conn = get_db()

    # Summary stats
    total = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    if total == 0:
        conn.close()
        await message.reply(
            f"🔍 Oxirgi {days} kun ichida qidiruv yo'q.\n\n"
            "Ma'lumotlar yig'ilishi uchun biroz vaqt kerak.",
        )
        return

    unique_users = conn.execute(
        "SELECT COUNT(DISTINCT telegram_id) FROM search_logs WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    zero_count = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE results_count = 0 AND created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    # Top queries
    top = conn.execute(
        """SELECT query, COUNT(*) as cnt, ROUND(AVG(results_count),0) as avg_res
           FROM search_logs WHERE created_at >= datetime('now', ?)
           GROUP BY query ORDER BY cnt DESC LIMIT 10""",
        (f"-{days} days",),
    ).fetchall()

    # Zero-result queries (unmet demand)
    zeros = conn.execute(
        """SELECT query, COUNT(*) as cnt, COUNT(DISTINCT telegram_id) as users
           FROM search_logs
           WHERE results_count = 0 AND created_at >= datetime('now', ?)
           GROUP BY query ORDER BY cnt DESC LIMIT 10""",
        (f"-{days} days",),
    ).fetchall()

    # Funnel: clicks and cart adds
    click_count = conn.execute(
        """SELECT COUNT(DISTINCT sl.id) FROM search_logs sl
           JOIN search_clicks sc ON sc.search_log_id = sl.id AND sc.action = 'click'
           WHERE sl.created_at >= datetime('now', ?)""",
        (f"-{days} days",),
    ).fetchone()[0]

    cart_count = conn.execute(
        """SELECT COUNT(DISTINCT sl.id) FROM search_logs sl
           JOIN search_clicks sc ON sc.search_log_id = sl.id AND sc.action = 'cart'
           WHERE sl.created_at >= datetime('now', ?)""",
        (f"-{days} days",),
    ).fetchone()[0]

    conn.close()

    # Build message
    lines = [
        f"🔍 <b>Qidiruv statistikasi ({days} kun)</b>\n",
        f"📊 Jami qidiruvlar: <b>{total}</b>",
        f"👥 Unikal foydalanuvchilar: <b>{unique_users}</b>",
        f"❌ Natijasiz: <b>{zero_count}</b> ({round(zero_count/total*100)}%)",
        f"👆 Bosish bor: <b>{click_count}</b>",
        f"🛒 Savatga qo'shish: <b>{cart_count}</b>",
    ]

    if top:
        lines.append(f"\n📈 <b>Top qidiruvlar:</b>")
        for i, r in enumerate(top, 1):
            avg = int(r["avg_res"])
            lines.append(f"  {i}. <code>{r['query']}</code> — {r['cnt']}x ({avg} natija)")

    if zeros:
        lines.append(f"\n🚨 <b>Topilmagan (talab bor!):</b>")
        for i, r in enumerate(zeros, 1):
            lines.append(f"  {i}. <code>{r['query']}</code> — {r['cnt']}x ({r['users']} kishi)")

    lines.append(f"\n💡 /searches {days*2} — ko'proq kunlik ma'lumot")

    await message.reply("\n".join(lines), parse_mode="HTML")



@router.message(Command("datacoverage"))
async def cmd_datacoverage(message: types.Message):
    """Show which monthly periods have been uploaded to client_balances.

    Highlights missing months, coverage gaps, and per-month stats.
    Usage: /datacoverage [currency]   (default: both UZS and USD)
    """
    if not is_admin(message):
        return

    parts = message.text.split()
    currency_filter = parts[1].upper() if len(parts) > 1 else None

    conn = get_db()
    try:
        # Get all distinct periods with stats, per currency
        rows = conn.execute(
            """SELECT currency,
                      period_start,
                      COUNT(DISTINCT client_name_1c) as clients,
                      SUM(period_debit) as shipments,
                      SUM(period_credit) as collections
               FROM client_balances
               WHERE period_start >= '2025-01-01'
                 AND strftime('%d', period_start) = '01'
               GROUP BY currency, period_start
               ORDER BY currency, period_start"""
        ).fetchall()

        if not rows:
            await message.reply("❌ Ma'lumotlar bazasida hech qanday davr topilmadi.")
            conn.close()
            return

        # Group by currency
        from collections import defaultdict
        from datetime import date, timedelta
        by_currency = defaultdict(list)
        for r in rows:
            by_currency[r["currency"]].append({
                "period": r["period_start"],
                "clients": r["clients"],
                "shipments": r["shipments"] or 0,
                "collections": r["collections"] or 0,
            })

        lines = ["📊 <b>Ma'lumotlar qamrovi (Data Coverage)</b>\n"]

        today = date.today()
        current_month = today.replace(day=1)

        for curr in sorted(by_currency.keys()):
            if currency_filter and curr != currency_filter:
                continue

            periods = by_currency[curr]
            covered_months = {p["period"] for p in periods}

            # Find range
            first = min(covered_months)
            last = max(covered_months)

            lines.append(f"\n{'💴' if curr == 'UZS' else '💵'} <b>{curr}</b>")
            lines.append(f"📅 Diapazon: {first[:7]} — {last[:7]}")
            lines.append(f"👥 Mijozlar: {max(p['clients'] for p in periods)}")

            # Generate expected months between first and last
            from datetime import datetime
            first_dt = datetime.strptime(first, "%Y-%m-%d").date()
            last_dt = datetime.strptime(last, "%Y-%m-%d").date()

            expected = []
            d = first_dt
            while d <= last_dt:
                expected.append(d.isoformat())
                # Next month
                if d.month == 12:
                    d = d.replace(year=d.year + 1, month=1)
                else:
                    d = d.replace(month=d.month + 1)

            missing = [m for m in expected if m not in covered_months]

            # Month-by-month breakdown
            lines.append("")
            month_names = {
                1: "Yan", 2: "Fev", 3: "Mar", 4: "Apr", 5: "May", 6: "Iyn",
                7: "Iyl", 8: "Avg", 9: "Sen", 10: "Okt", 11: "Noy", 12: "Dek"
            }

            for p in periods:
                dt = datetime.strptime(p["period"], "%Y-%m-%d").date()
                m_name = month_names.get(dt.month, "?")
                is_partial = (dt.year == current_month.year and dt.month == current_month.month)
                partial_tag = " ⚠️" if is_partial else ""

                if curr == "UZS":
                    ship_fmt = f"{round(p['shipments'] / 1e9, 1)}B"
                    coll_fmt = f"{round(p['collections'] / 1e9, 1)}B"
                else:
                    ship_fmt = f"${round(p['shipments'] / 1e3, 1)}K"
                    coll_fmt = f"${round(p['collections'] / 1e3, 1)}K"

                lines.append(
                    f"  {'✅' if not is_partial else '🔶'} {m_name} {dt.year} — "
                    f"{p['clients']} mijoz | ↑{ship_fmt} ↓{coll_fmt}{partial_tag}"
                )

            # Missing months
            if missing:
                lines.append(f"\n  ❌ <b>Yuklanmagan oylar ({len(missing)}):</b>")
                for m in missing:
                    dt = datetime.strptime(m, "%Y-%m-%d").date()
                    m_name = month_names.get(dt.month, "?")
                    lines.append(f"    • {m_name} {dt.year} ({m[:7]})")
            else:
                lines.append(f"\n  ✅ Barcha oylar yuklangan!")

            # Check if current month is covered
            current_iso = current_month.isoformat()
            if current_iso not in covered_months and current_iso >= first:
                lines.append(f"  ℹ️ Joriy oy ({month_names.get(current_month.month)} {current_month.year}) hali yuklanmagan")

        # Summary
        total_periods = len(rows)
        total_clients = conn.execute(
            "SELECT COUNT(DISTINCT client_name_1c) FROM client_balances WHERE period_start >= '2025-01-01'"
        ).fetchone()[0]
        lines.append(f"\n📈 <b>Jami:</b> {total_periods} davr, {total_clients} unikal mijoz")

        await message.reply("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Data coverage error: {e}")
        await message.reply(f"❌ Xatolik: {str(e)[:200]}")
    finally:
        conn.close()



@router.message(Command("holiday"))
async def cmd_holiday(message: types.Message):
    """Manage holidays.

    Usage:
        /holiday add YYYY-MM-DD <name>
        /holiday remove YYYY-MM-DD
        /holiday list
    """
    if not is_admin(message):
        return

    parts = (message.text or "").split(maxsplit=3)
    if len(parts) < 2:
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "<code>/holiday add YYYY-MM-DD nomi</code>\n"
            "<code>/holiday remove YYYY-MM-DD</code>\n"
            "<code>/holiday list</code>",
            parse_mode="HTML",
        )
        return

    action = parts[1].lower()
    from backend.services.daily_uploads import add_holiday, remove_holiday, list_holidays

    try:
        if action == "list":
            holidays = list_holidays(days_ahead=365)
            if not holidays:
                await message.reply("📅 Kelgusi 365 kun ichida bayramlar yo'q.")
                return
            lines = ["📅 <b>Bayramlar (365 kun):</b>\n"]
            for h in holidays:
                lines.append(f"• {h['holiday_date']} — {html_escape(h['name'])}")
            await message.reply("\n".join(lines), parse_mode="HTML")

        elif action == "add":
            if len(parts) < 4:
                await message.reply("❌ Foydalanish: /holiday add YYYY-MM-DD nomi")
                return
            date_arg = parts[2]
            name = parts[3]
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_arg):
                await message.reply("❌ Sana formati: YYYY-MM-DD")
                return
            user_id = message.from_user.id if message.from_user else None
            result = add_holiday(date_arg, name, user_id=user_id)
            await message.reply(
                f"✅ Bayram qo'shildi: <b>{date_arg}</b> — {html_escape(name)}\n"
                f"Retroaktiv skip qilindi: {result['rows_updated']} qator",
                parse_mode="HTML",
            )

        elif action == "remove":
            if len(parts) < 3:
                await message.reply("❌ Foydalanish: /holiday remove YYYY-MM-DD")
                return
            date_arg = parts[2]
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_arg):
                await message.reply("❌ Sana formati: YYYY-MM-DD")
                return
            result = remove_holiday(date_arg)
            if not result.get("removed"):
                await message.reply(f"❌ {date_arg} uchun bayram topilmadi.")
            else:
                await message.reply(
                    f"✅ Bayram o'chirildi: <b>{date_arg}</b>\n"
                    f"Skip bekor qilindi: {result['rows_updated']} qator",
                    parse_mode="HTML",
                )

        else:
            await message.reply(f"❌ Noma'lum amal: {action}\nKerak: add, remove, list")

    except Exception as e:
        logger.error(f"/holiday error: {e}")
        await message.reply(f"❌ Xatolik: {str(e)[:200]}")



@router.message(Command("backfilldailyuploads"))
async def cmd_backfill_daily_uploads(message: types.Message):
    """One-shot historical backfill of daily_uploads from 2026-04-01 onward."""
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Daily uploads backfill ishga tushdi...")
    try:
        from backend.services.daily_uploads_backfill import run_backfill
        result = run_backfill()
        if not result.get("ok"):
            await status_msg.edit_text(f"❌ {result.get('error', 'backfill failed')}")
            return
        lines = [
            "✅ <b>Backfill tugadi!</b>\n",
            f"📅 Davr: {result.get('start_date')} — {result.get('end_date')}",
            f"📊 Jami kunlar: {result.get('total_days')}",
            f"⏭ Yakshanba/bayram: {result.get('skipped_days_sun_holiday')}",
            f"🆕 Qo'shilgan: {result.get('total_inserted')}",
            f"♻️ Yangilangan: {result.get('total_updated')}",
        ]
        per_type_ins = result.get("inserted_by_type") or {}
        per_type_upd = result.get("updated_by_type") or {}
        all_types = sorted(set(per_type_ins) | set(per_type_upd))
        if all_types:
            lines.append("\n<b>Upload turi bo'yicha (yangi / yangilangan):</b>")
            for t in all_types:
                ins = per_type_ins.get(t, 0)
                upd = per_type_upd.get(t, 0)
                if ins or upd:
                    lines.append(f"  • {t}: {ins} / {upd}")
        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"backfilldailyuploads error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")



@router.message(Command("backfillordernames"))
async def cmd_backfillordernames(message: types.Message):
    """Session A policy: old wish-list orders (pre-commit 325b4cc) stored
    the cleaned Latin display name in order_items.product_name. The new rule
    is that order history should show the raw 1C Cyrillic name so sales can
    reconcile against 1C. This backfills all rows with a linked product_id
    to use products.name instead. Idempotent.
    """
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Backfill ishlayapti...")

    try:
        import httpx

        api_url = f"{_BASE_URL}/api/admin/backfill-order-item-names"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(api_url, params={"admin_key": "rassvet2026"})
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ Xatolik: {result.get('error', 'Unknown')}")
            return

        rows = result.get("rows_updated", 0)
        await status_msg.edit_text(
            "✅ <b>Order item nomlari yangilandi</b>\n\n"
            f"Yangilangan qatorlar: <b>{rows}</b>\n\n"
            "Endi eski wish-list buyurtmalari ham 1C Kirillcha nom bilan ko‘rinadi.",
            parse_mode="HTML",
        )

    except Exception as e:
        logger.error(f"Backfillordernames error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")



@router.message(Command("backfillrealordertotals"))
async def cmd_backfillrealordertotals(message: types.Message):
    """One-shot backfill: derive missing total_local/sum_local/total_currency
    on existing real_order_items rows, and missing total_sum/total_sum_currency
    on existing real_orders rows. Mirrors import-time post-processing so docs
    already in the DB heal without requiring re-upload of all months.
    Idempotent — safe to run multiple times.
    """
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Backfill ishlayapti...")

    try:
        import httpx

        api_url = f"{_BASE_URL}/api/finance/backfill-real-order-totals"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(api_url, data={"admin_key": "rassvet2026"})
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ Xatolik: {result.get('error', 'Unknown')}")
            return

        phases = result.get("phases", {}) or {}
        lines = [
            "✅ <b>Backfill tugadi</b>",
            "",
            f"<b>Jami yangilangan qatorlar:</b> {result.get('rows_touched_total', 0)}",
            "",
            "<b>Bosqichlar:</b>",
            f"  • item.sum_local ← price×qty: {phases.get('item_sum_local_from_price_qty', 0)}",
            f"  • item.total_local ← sum+vat: {phases.get('item_total_local_from_sum', 0)}",
            f"  • item.total_local ← price×qty: {phases.get('item_total_local_from_price_qty', 0)}",
            f"  • item.sum_currency ← price_cur×qty: {phases.get('item_sum_currency_from_price_qty', 0)}",
            f"  • item.total_currency ← sum_cur: {phases.get('item_total_currency_from_sum', 0)}",
            f"  • item.total_currency ← price_cur×qty: {phases.get('item_total_currency_from_price_qty', 0)}",
            f"  • order.total_sum ← Σitems: {phases.get('order_total_sum_from_items', 0)}",
            f"  • order.total_sum_cur ← Σitems: {phases.get('order_total_sum_currency_from_items', 0)}",
            "",
            "<b>DB holati:</b>",
            f"  • orders: {result.get('db_orders_with_total', 0)} / {result.get('db_total_orders', 0)} (jami narx > 0)",
            f"  • items:  {result.get('db_items_with_total', 0)} / {result.get('db_total_items', 0)} (jami narx > 0)",
        ]
        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Backfillrealordertotals error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


# ── /stockalert — smart stock alerts for active products ─────────

@router.message(Command("stockalert"))
async def cmd_stockalert(message: types.Message):
    """Show out-of-stock and running-low alerts for active products."""
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Faol mahsulotlarni tekshirmoqda...")
    try:
        from backend.services.stock_alerts import get_stock_alerts, format_stock_alert_message
        alerts = get_stock_alerts()
        text = format_stock_alert_message(alerts)
        await status_msg.edit_text(text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"/stockalert error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


# ── /rebuildsearch — rebuild search_text index for all products ───

@router.message(Command("rebuildsearch"))
async def cmd_rebuildsearch(message: types.Message):
    """Rebuild search_text index for all products."""
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Search index qayta qurilmoqda...")
    try:
        from backend.database import rebuild_all_search_text
        count = rebuild_all_search_text()
        await status_msg.edit_text(
            f"✅ Search index yangilandi!\n"
            f"📊 {count} ta mahsulot qayta indekslandi.\n\n"
            f"Yangi: Latin→Cyrillic reverse transliteration + phonetic aliases.",
        )
    except Exception as e:
        logger.error(f"/rebuildsearch error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


# ── /addmissing — add unmatched stock names as new catalog products ──

@router.message(Command("addmissing"))
async def cmd_addmissing(message: types.Message):
    """Add all unmatched stock import names as new products in the catalog."""
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Topilmagan nomlarni katalogga qo'shmoqda...")
    try:
        from backend.services.add_missing_products import add_missing_from_unmatched
        result = add_missing_from_unmatched()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ {result.get('message', 'Xatolik')}")
            return

        added = result.get("added", 0)
        if added == 0:
            await status_msg.edit_text("✅ Topilmagan nomlar yo'q — barchasi allaqachon katalogda!")
            return

        products = result.get("products", [])
        lines = [
            f"✅ <b>{added} ta yangi mahsulot qo'shildi</b>\n",
        ]

        cat_counts = {}
        for p in products:
            cat_counts[p["category"]] = cat_counts.get(p["category"], 0) + 1

        lines.append("Kategoriyalar bo'yicha:")
        for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {cat}: {cnt}")

        lines.append(f"\nNamunalar:")
        for p in products[:10]:
            lines.append(f"  • {html_escape(p['name_1c'])}")
            lines.append(f"    → {html_escape(p['name_latin'])}")
        if len(products) > 10:
            lines.append(f"  ... va yana {len(products) - 10} ta")

        lines.append(f"\n💡 Narxlar keyingi /prices yuklashda avtomatik to'ldiriladi.")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"/addmissing error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


# ── /seedaliases — one-time alias table seeding from production data ──

@router.message(Command("seedaliases"))
async def cmd_seedaliases(message: types.Message):
    """Seed product_aliases from DB products + supply history. Run once."""
    if not is_admin(message):
        return

    import unicodedata

    status_msg = await message.reply("⏳ Alias jadvalni to'ldirmoqda...")
    conn = get_db()
    try:
        existing = conn.execute("SELECT COUNT(*) FROM product_aliases").fetchone()[0]

        # Source 1: every product's 1C name
        products = conn.execute(
            "SELECT id, name, name_display FROM products WHERE is_active = 1 AND name IS NOT NULL"
        ).fetchall()
        s1 = 0
        for p in products:
            alias = unicodedata.normalize("NFC", p["name"].strip().lower())
            if not alias:
                continue
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO product_aliases (alias_name, alias_name_lower, product_id, source) "
                    "VALUES (?, ?, ?, 'db_product')",
                    (p["name"].strip(), alias, p["id"]),
                )
                s1 += 1
            except Exception:
                pass
            # Also add display name if different
            if p["name_display"]:
                disp = unicodedata.normalize("NFC", p["name_display"].strip().lower())
                if disp and disp != alias:
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO product_aliases (alias_name, alias_name_lower, product_id, source) "
                            "VALUES (?, ?, ?, 'db_display')",
                            (p["name_display"].strip(), disp, p["id"]),
                        )
                        s1 += 1
                    except Exception:
                        pass

        # Source 2: supply history
        s2 = 0
        try:
            supply_rows = conn.execute(
                """SELECT DISTINCT soi.product_name_1c, p.id as product_id
                   FROM supply_order_items soi
                   JOIN products p ON p.id = soi.product_id
                   WHERE soi.product_name_1c IS NOT NULL AND soi.product_id IS NOT NULL"""
            ).fetchall()
            for r in supply_rows:
                alias = unicodedata.normalize("NFC", r["product_name_1c"].strip().lower())
                if not alias:
                    continue
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO product_aliases (alias_name, alias_name_lower, product_id, source) "
                        "VALUES (?, ?, ?, 'supply_history')",
                        (r["product_name_1c"].strip(), alias, r["product_id"]),
                    )
                    s2 += 1
                except Exception:
                    pass
        except Exception:
            pass

        conn.commit()
        final = conn.execute("SELECT COUNT(*) FROM product_aliases").fetchone()[0]
        products_covered = conn.execute("SELECT COUNT(DISTINCT product_id) FROM product_aliases").fetchone()[0]
        total_products = conn.execute("SELECT COUNT(*) FROM products WHERE is_active = 1").fetchone()[0]

        by_source = conn.execute(
            "SELECT source, COUNT(*) as c FROM product_aliases GROUP BY source ORDER BY c DESC"
        ).fetchall()

        lines = [
            f"✅ <b>Alias jadvali to'ldirildi</b>\n",
            f"Avval: {existing} → Hozir: <b>{final}</b> ta alias",
            f"Qoplangan: {products_covered} / {total_products} ta mahsulot\n",
        ]
        for r in by_source:
            lines.append(f"  {r['source']}: {r['c']}")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"/seedaliases error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")
    finally:
        conn.close()


# ── /aliases — product alias table management ────────────────────

@router.message(Command("aliases"))
async def cmd_aliases(message: types.Message):
    """Show alias table stats and unmatched import names.

    Usage:
        /aliases           — show stats + top unmatched names
        /aliases link NAME PRODUCT_ID — manually link an unmatched name
    """
    if not is_admin(message):
        return

    args = (message.text or "").split(maxsplit=3)
    conn = get_db()

    try:
        if len(args) >= 2 and args[1].lower() == "resolve":
            # /aliases resolve — auto-match unmatched names using fuzzy + keyword
            import re as _re
            from difflib import get_close_matches as _gcm

            unmatched_rows = conn.execute(
                "SELECT id, name FROM unmatched_import_names WHERE resolved = 0"
            ).fetchall()
            if not unmatched_rows:
                await message.reply("✅ Topilmagan nomlar yo'q!")
                conn.close()
                return

            products = conn.execute(
                "SELECT id, name FROM products WHERE is_active = 1 AND name IS NOT NULL"
            ).fetchall()

            def _norm(s):
                n = (s or "").strip().lower()
                n = _re.sub(r'\s+', ' ', n)
                n = _re.sub(r'\.(\s|$)', r'\1', n)
                return n

            norm_index = {}
            for p in products:
                norm_index[_norm(p["name"])] = p

            norm_keys = list(norm_index.keys())
            resolved = 0
            suggested = []

            for row in unmatched_rows:
                nm = _norm(row["name"])
                close = _gcm(nm, norm_keys, n=1, cutoff=0.82)
                if close:
                    match = norm_index[close[0]]
                    conn.execute(
                        "INSERT OR IGNORE INTO product_aliases (alias_name, alias_name_lower, product_id, source) "
                        "VALUES (?, ?, ?, 'auto_resolve')",
                        (row["name"].strip(), nm, match["id"]),
                    )
                    conn.execute(
                        "UPDATE unmatched_import_names SET resolved = 1, resolved_product_id = ?, resolved_at = datetime('now') WHERE id = ?",
                        (match["id"], row["id"]),
                    )
                    resolved += 1
                else:
                    close2 = _gcm(nm, norm_keys, n=1, cutoff=0.65)
                    if close2:
                        match = norm_index[close2[0]]
                        suggested.append((row["name"][:40], match["id"], match["name"][:40]))

            conn.commit()

            lines = [f"🔍 <b>Auto-resolve natijasi</b>\n"]
            lines.append(f"✅ Auto-resolved: <b>{resolved}</b>")
            lines.append(f"Qolgan: <b>{len(unmatched_rows) - resolved}</b>\n")

            if suggested:
                lines.append(f"💡 <b>Taklif ({len(suggested[:10])}):</b>")
                for uname, pid, pname in suggested[:10]:
                    lines.append(f"  {html_escape(uname)}")
                    lines.append(f"  <code>/aliases link {html_escape(uname)} {pid}</code>\n")

            await message.reply("\n".join(lines), parse_mode="HTML")
            conn.close()
            return

        if len(args) >= 4 and args[1].lower() == "link":
            # /aliases link NAME PRODUCT_ID
            link_name = args[2]
            try:
                link_pid = int(args[3])
            except ValueError:
                await message.reply("❌ Product ID raqam bo'lishi kerak")
                conn.close()
                return

            product = conn.execute(
                "SELECT id, name FROM products WHERE id = ?", (link_pid,)
            ).fetchone()
            if not product:
                await message.reply(f"❌ Product #{link_pid} topilmadi")
                conn.close()
                return

            conn.execute(
                "INSERT OR REPLACE INTO product_aliases (alias_name, alias_name_lower, product_id, source) "
                "VALUES (?, ?, ?, 'manual')",
                (link_name.strip(), link_name.strip().lower(), link_pid),
            )
            conn.execute(
                "UPDATE unmatched_import_names SET resolved = 1, resolved_product_id = ?, resolved_at = datetime('now') "
                "WHERE name_lower = ?",
                (link_pid, link_name.strip().lower()),
            )
            conn.commit()
            await message.reply(
                f"✅ Alias qo'shildi:\n"
                f"  <b>{html_escape(link_name)}</b> → #{link_pid} ({html_escape(product['name'][:40])})",
                parse_mode="HTML",
            )
            conn.close()
            return

        # Default: show stats
        total_aliases = conn.execute("SELECT COUNT(*) FROM product_aliases").fetchone()[0]
        by_source = conn.execute(
            "SELECT source, COUNT(*) as c FROM product_aliases GROUP BY source ORDER BY c DESC"
        ).fetchall()

        unmatched = conn.execute(
            "SELECT name, occurrences, source FROM unmatched_import_names "
            "WHERE resolved = 0 ORDER BY occurrences DESC LIMIT 15"
        ).fetchall()
        unmatched_total = conn.execute(
            "SELECT COUNT(*) FROM unmatched_import_names WHERE resolved = 0"
        ).fetchone()[0]

        lines = [f"🔗 <b>Product Aliases</b>\n"]
        lines.append(f"Jami: <b>{total_aliases}</b> ta alias\n")
        for r in by_source:
            lines.append(f"  {r['source']}: {r['c']}")

        if unmatched_total > 0:
            lines.append(f"\n❓ <b>Topilmagan nomlar</b> ({unmatched_total} ta):\n")
            for u in unmatched:
                lines.append(f"  [{u['occurrences']}x] {html_escape(u['name'][:45])}")
            lines.append(f"\nBog'lash: <code>/aliases link NOM PRODUCT_ID</code>")
        else:
            lines.append("\n✅ Barcha nomlar bog'langan!")

        await message.reply("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"/aliases error: {e}")
        await message.reply(f"❌ Xatolik: {str(e)[:200]}")
    finally:
        conn.close()

