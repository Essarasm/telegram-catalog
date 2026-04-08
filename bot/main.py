"""Telegram bot with Mini App integration and admin commands."""
import os
import re
import json
import asyncio
import logging
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonWebApp,
    WebAppInfo,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
_BASE_URL = os.getenv("WEBAPP_URL", "https://telegram-catalog-production.up.railway.app")
WEBAPP_URL = f"{_BASE_URL}?v=15"
ORDER_GROUP_CHAT_ID = int(os.getenv("ORDER_GROUP_CHAT_ID", "-1003740010463"))
ADMIN_GROUP_CHAT_ID = int(os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051"))

# Admin user IDs who can use /add, /approve, /list commands
# Add Alisher's ID and other manager IDs via env var or hardcode below
ADMIN_IDS = set()
_admin_env = os.getenv("ADMIN_IDS", "")
if _admin_env:
    ADMIN_IDS = {int(x.strip()) for x in _admin_env.split(",") if x.strip().isdigit()}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def html_escape(text: str) -> str:
    """Escape HTML special characters for Telegram parse_mode=HTML."""
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")


def get_db():
    """Get database connection (same as backend)."""
    import sqlite3
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    # Register Unicode-aware LOWER for Cyrillic search (SQLite built-in only handles ASCII)
    conn.create_function("LOWER", 1, lambda s: s.lower() if s else s)
    return conn


def normalize_phone(raw: str) -> str:
    """Strip to last 9 digits for matching."""
    digits = re.sub(r"\D", "", raw or "")
    return digits[-9:] if len(digits) >= 9 else digits


def is_admin(message: types.Message) -> bool:
    """Check if user is an admin. Allow from sales/admin groups or listed admin IDs."""
    if ADMIN_IDS and message.from_user.id in ADMIN_IDS:
        return True
    # Allow commands from the sales managers group or admin/ops group
    if message.chat.id in (ORDER_GROUP_CHAT_ID, ADMIN_GROUP_CHAT_ID):
        return True
    return False


# ───────────────────────────────────────────
# Public commands
# ───────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    """Send welcome message with Mini App button."""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📋 Katalogni ochish",
                    web_app=WebAppInfo(url=WEBAPP_URL),
                )
            ]
        ]
    )
    await message.answer(
        "Assalomu alaykum! 👋\n\n"
        "Qurilish materiallari katalogiga xush kelibsiz.\n\n"
        "Quyidagi tugmani bosib, mahsulotlar ro'yxatini ko'ring, "
        "savatga qo'shing va buyurtma yarating.",
        reply_markup=keyboard,
    )


@dp.message(Command("chatid"))
async def cmd_chatid(message: types.Message):
    """Report the chat ID."""
    await message.answer(
        f"Chat ID: <code>{message.chat.id}</code>\n"
        f"User ID: <code>{message.from_user.id}</code>",
        parse_mode="HTML",
    )


# ───────────────────────────────────────────
# Admin commands
# ───────────────────────────────────────────

@dp.message(Command("add"))
async def cmd_add(message: types.Message):
    """
    Add a new client to the whitelist.
    Usage: /add 901234567 Ism Familiya Joylashuv
    """
    if not is_admin(message):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "<code>/add telefon Ism Joylashuv</code>\n\n"
            "<b>Misollar:</b>\n"
            "<code>/add 901234567 Akbar Karimov Sergeli</code>\n"
            "<code>/add 998901234567</code>",
            parse_mode="HTML",
        )
        return

    args = parts[1].strip().split()
    phone_raw = args[0]
    phone_norm = normalize_phone(phone_raw)

    if len(phone_norm) < 9:
        await message.reply("❌ Telefon raqam noto'g'ri. Kamida 9 raqam bo'lishi kerak.")
        return

    client_name = " ".join(args[1:]) if len(args) > 1 else ""

    conn = get_db()

    # Check if already exists
    existing = conn.execute(
        "SELECT id, name FROM allowed_clients WHERE phone_normalized = ? LIMIT 1",
        (phone_norm,),
    ).fetchone()

    if existing:
        await message.reply(
            f"⚠️ Bu raqam allaqachon ro'yxatda.\n"
            f"ID: {existing['id']}, Ism: {existing['name'] or '—'}",
        )
        conn.close()
        return

    # Add to allowed_clients
    conn.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, source_sheet, status) VALUES (?, ?, ?, ?)",
        (phone_norm, client_name or None, "bot_added", "active"),
    )

    # Check if there's already a registered user with this phone → auto-approve
    all_users = conn.execute(
        "SELECT telegram_id, phone FROM users WHERE phone IS NOT NULL",
    ).fetchall()

    approved_user = None
    for u in all_users:
        if normalize_phone(u["phone"]) == phone_norm:
            client_id = conn.execute(
                "SELECT id FROM allowed_clients WHERE phone_normalized = ? LIMIT 1",
                (phone_norm,),
            ).fetchone()["id"]
            conn.execute(
                "UPDATE users SET is_approved = 1, client_id = ? WHERE telegram_id = ?",
                (client_id, u["telegram_id"]),
            )
            approved_user = u["telegram_id"]
            break

    conn.commit()
    conn.close()

    response = f"✅ Yangi mijoz qo'shildi!\n\n📱 Telefon: <code>{phone_norm}</code>"
    if client_name:
        response += f"\n📛 Ism: {client_name}"

    if approved_user:
        response += f"\n\n🎉 Foydalanuvchi (ID: {approved_user}) avtomatik tasdiqlandi! Ilovani qayta ochsa, narxlarni ko'radi."
    else:
        response += "\n\nℹ️ Foydalanuvchi hali ro'yxatdan o'tmagan. Ro'yxatdan o'tganda avtomatik tasdiqlanadi."

    await message.reply(response, parse_mode="HTML")


@dp.message(Command("approve"))
async def cmd_approve(message: types.Message):
    """
    Approve a user by Telegram ID.
    Usage: /approve 123456789
    """
    if not is_admin(message):
        return

    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "<code>/approve telegram_id</code>\n\n"
            "<b>Misol:</b> <code>/approve 123456789</code>\n\n"
            "Telegram ID ro'yxatdan o'tish xabarida ko'rsatilgan.",
            parse_mode="HTML",
        )
        return

    telegram_id = int(parts[1])
    conn = get_db()

    user = conn.execute(
        "SELECT telegram_id, phone, first_name, is_approved FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user:
        await message.reply(f"❌ Telegram ID {telegram_id} topilmadi.")
        conn.close()
        return

    if user["is_approved"]:
        await message.reply(
            f"ℹ️ Allaqachon tasdiqlangan: {user['first_name'] or ''} ({user['phone']})",
        )
        conn.close()
        return

    # Approve the user
    conn.execute("UPDATE users SET is_approved = 1 WHERE telegram_id = ?", (telegram_id,))

    # Also add their phone to allowed_clients if not there
    phone_norm = normalize_phone(user["phone"])
    existing_client = conn.execute(
        "SELECT id FROM allowed_clients WHERE phone_normalized = ? LIMIT 1",
        (phone_norm,),
    ).fetchone()

    if not existing_client:
        conn.execute(
            "INSERT INTO allowed_clients (phone_normalized, name, source_sheet, status, matched_telegram_id) VALUES (?, ?, ?, ?, ?)",
            (phone_norm, user["first_name"], "bot_approved", "active", telegram_id),
        )
        client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    else:
        client_id = existing_client["id"]
        conn.execute(
            "UPDATE allowed_clients SET matched_telegram_id = ? WHERE id = ?",
            (telegram_id, client_id),
        )

    conn.execute("UPDATE users SET client_id = ? WHERE telegram_id = ?", (client_id, telegram_id))
    conn.commit()

    # Persist to JSON backup (survives volume issues)
    try:
        from backend.services.backup_users import save_user_to_backup
        row = conn.execute(
            "SELECT telegram_id, phone, first_name, last_name, username, latitude, longitude, is_approved, client_id, registered_at FROM users WHERE telegram_id = ?",
            (telegram_id,),
        ).fetchone()
        if row:
            save_user_to_backup(dict(row))
    except Exception as e:
        logging.warning(f"backup after /approve failed: {e}")

    # Also add to approved_overrides.json in the repo (ultimate failsafe)
    try:
        overrides_path = os.path.join(os.path.dirname(__file__), '..', 'approved_overrides.json')
        overrides = {"always_approved_ids": []}
        if os.path.exists(overrides_path):
            with open(overrides_path, 'r') as f:
                overrides = json.load(f)
        ids = set(overrides.get('always_approved_ids', []))
        if telegram_id not in ids:
            ids.add(telegram_id)
            overrides['always_approved_ids'] = sorted(ids)
            with open(overrides_path, 'w') as f:
                json.dump(overrides, f, indent=2)
    except Exception as e:
        logging.warning(f"overrides update failed: {e}")

    conn.close()

    await message.reply(
        f"✅ Tasdiqlandi!\n\n"
        f"📛 {user['first_name'] or '—'}\n"
        f"📱 {user['phone']}\n"
        f"🆔 {telegram_id}\n\n"
        f"Ilovani qayta ochsa, narxlarni ko'radi.",
        parse_mode="HTML",
    )


@dp.message(Command("list"))
async def cmd_list(message: types.Message):
    """List recent unapproved users."""
    if not is_admin(message):
        return

    conn = get_db()
    rows = conn.execute(
        """SELECT telegram_id, phone, first_name, last_name, username, registered_at
           FROM users
           WHERE is_approved = 0 AND phone IS NOT NULL
           ORDER BY registered_at DESC
           LIMIT 20""",
    ).fetchall()
    conn.close()

    if not rows:
        await message.reply("✅ Tasdiqlanmagan foydalanuvchilar yo'q!")
        return

    lines = [f"⏳ <b>Tasdiqlanmagan ({len(rows)}):</b>\n"]
    for i, r in enumerate(rows, 1):
        name = " ".join(filter(None, [r["first_name"], r["last_name"]])) or "—"
        username = f" @{r['username']}" if r["username"] else ""
        lines.append(
            f"{i}. <b>{name}</b>{username}\n"
            f"   📱 {r['phone']} | 🆔 <code>{r['telegram_id']}</code>\n"
            f"   → /approve {r['telegram_id']}"
        )

    await message.reply("\n".join(lines), parse_mode="HTML")


@dp.message(Command("prices"))
async def cmd_prices(message: types.Message):
    """Update prices from an Excel file. Reply to a document with /prices."""
    if not is_admin(message):
        return

    # Check if replying to a document
    doc = None
    if message.reply_to_message and message.reply_to_message.document:
        doc = message.reply_to_message.document
    elif message.document:
        doc = message.document

    if not doc:
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "1. Excel faylni guruhga yuboring\n"
            "2. Faylga javob sifatida /prices yozing\n\n"
            "Yoki faylni /prices caption bilan yuboring.",
            parse_mode="HTML",
        )
        return

    if not doc.file_name or not doc.file_name.endswith(('.xlsx', '.xls')):
        await message.reply("❌ Faqat Excel (.xlsx) fayllar qabul qilinadi.")
        return

    status_msg = await message.reply("⏳ Narxlar yangilanmoqda...")

    try:
        import httpx

        # Download file from Telegram
        file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        async with httpx.AsyncClient() as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        # Send to our API
        api_url = f"{_BASE_URL}/api/products/update-prices"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                api_url,
                files={"file": (doc.file_name, file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                data={"admin_key": "rassvet2026"},
            )
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ Xatolik: {result.get('error', 'Unknown')}")
            return

        lines = [
            "✅ <b>Narxlar yangilandi!</b>\n",
            f"📊 Excel: {result['excel_products']} ta mahsulot",
            f"🗄 Baza: {result['db_products']} ta mahsulot",
            f"🔗 Mos kelgan: {result['matched']}",
            f"✏️ O'zgartirilgan: {result['updated']}",
        ]

        # Show match method breakdown if available
        methods = result.get('match_methods', {})
        if methods:
            exact = methods.get('exact', 0)
            normalized = methods.get('normalized', 0)
            if normalized > 0:
                lines.append(f"🔍 Moslik: aniq={exact}, normalizatsiya={normalized}")

        # New products auto-added
        new_total = result.get('new_products_total', 0)
        if new_total > 0:
            lines.append(f"\n🆕 <b>Yangi mahsulotlar qo'shildi: {new_total} ta</b>")
            lines.append(f"📁 Kategoriya: \"Yangi mahsulotlar\"")
            for np in result.get('new_products', [])[:10]:
                cyr = html_escape(np['cyrillic'])
                disp = html_escape(np['display'])
                lines.append(f"  • {cyr} → <i>{disp}</i>")
            if new_total > 10:
                lines.append(f"  ... va yana {new_total - 10} ta")

        # Stock status changes
        out_count = result.get('out_of_stock_count', 0)
        restored = result.get('restored_in_stock', 0)
        if out_count > 0 or restored > 0:
            lines.append(f"\n📦 <b>Ombor holati:</b>")
            if restored > 0:
                lines.append(f"  ✅ Qayta mavjud: {restored} ta")
            if out_count > 0:
                lines.append(f"  🔴 Tugagan (Excel'da yo'q): {out_count} ta")

        if result['changes']:
            lines.append("\n<b>Narx o'zgarishlar:</b>")
            for c in result['changes'][:15]:
                old = c['old_usd']
                new = c['new_usd']
                arrow = "📈" if new > old else "📉"
                lines.append(f"{arrow} {html_escape(c['name'])}: ${old:.2f} → ${new:.2f}")
            if len(result['changes']) > 15:
                lines.append(f"... va yana {len(result['changes']) - 15} ta")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Price update error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:200]}")


@dp.message(Command("stock"))
async def cmd_stock(message: types.Message):
    """Update stock/inventory levels from an Excel file. Reply to a document with /stock."""
    if not is_admin(message):
        return

    # Check if replying to a document
    doc = None
    if message.reply_to_message and message.reply_to_message.document:
        doc = message.reply_to_message.document
    elif message.document:
        doc = message.document

    if not doc:
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "1. 1C'dan inventarizatsiya Excel faylni yuboring\n"
            "2. Faylga javob sifatida /stock yozing\n\n"
            "Yoki faylni /stock caption bilan yuboring.\n\n"
            "<b>Holatlar:</b>\n"
            "🟢 Mavjud (>10)\n"
            "🟡 Kam qoldi (1-10)\n"
            "🔴 Tugagan (0)",
            parse_mode="HTML",
        )
        return

    if not doc.file_name or not doc.file_name.endswith(('.xlsx', '.xls')):
        await message.reply("❌ Faqat Excel (.xlsx) fayllar qabul qilinadi.")
        return

    status_msg = await message.reply("⏳ Inventarizatsiya yangilanmoqda...")

    try:
        import httpx

        # Download file from Telegram
        file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        async with httpx.AsyncClient() as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        # Send to our API
        api_url = f"{_BASE_URL}/api/products/update-stock"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                api_url,
                files={"file": (doc.file_name, file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                data={"admin_key": "rassvet2026"},
            )
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ Xatolik: {result.get('error', 'Unknown')}")
            return

        sc = result.get('status_counts', {})
        lines = [
            "✅ <b>Inventarizatsiya yangilandi!</b>\n",
            f"📊 Excel: {result['excel_products']} ta mahsulot",
            f"🗄 Baza: {result['db_products']} ta mahsulot",
            f"🔗 Mos kelgan: {result['matched']}",
            "",
            "<b>Holat:</b>",
            f"🟢 Mavjud: {sc.get('in_stock', 0)}",
            f"🟡 Kam qoldi: {sc.get('low_stock', 0)}",
            f"🔴 Tugagan: {sc.get('out_of_stock', 0)}",
        ]

        # Show notable status changes
        changes = result.get('status_changes', [])
        if changes:
            lines.append(f"\n<b>Holat o'zgarishlari ({len(changes)}):</b>")
            status_emoji = {"in_stock": "🟢", "low_stock": "🟡", "out_of_stock": "🔴", "unknown": "⚪"}
            for c in changes[:15]:
                old_e = status_emoji.get(c['old_status'], '⚪')
                new_e = status_emoji.get(c['new_status'], '⚪')
                lines.append(f"  {old_e}→{new_e} {c['name']}")
            if len(changes) > 15:
                lines.append(f"  ... va yana {len(changes) - 15} ta")

        unmatched = result.get('unmatched_count', 0)
        if unmatched > 0:
            lines.append(f"\n⚠️ {unmatched} ta Excel mahsulot bazada topilmadi")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Stock update error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:200]}")


@dp.message(Command("catalog"))
async def cmd_catalog(message: types.Message):
    """Refresh catalog from an Excel file without full redeploy.

    Reply to an Excel with /catalog to:
    - Add new products not in the database
    - Update existing product details (category, producer, weight, unit)
    - Mark products not in Excel as discontinued (is_active=0)
    """
    if not is_admin(message):
        return

    # Check if replying to a document
    doc = None
    if message.reply_to_message and message.reply_to_message.document:
        doc = message.reply_to_message.document
    elif message.document:
        doc = message.document

    if not doc:
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "1. Excel faylni yuboring (Rassvet_Master yoki 1C export)\n"
            "2. Faylga javob sifatida /catalog yozing\n\n"
            "<b>Rassvet_Master rejimi:</b>\n"
            "• Yangi mahsulotlarni qo'shadi\n"
            "• Mavjud mahsulotlarni yangilaydi\n"
            "• Excel'da yo'q mahsulotlarni o'chiradi\n\n"
            "<b>1C Номенклатура rejimi:</b>\n"
            "• Narx va og'irlikni yangilaydi\n"
            "• Yangi mahsulotlarni aniqlaydi (qo'l bilan kiritish kerak)",
            parse_mode="HTML",
        )
        return

    if not doc.file_name or not doc.file_name.endswith(('.xlsx', '.xls')):
        await message.reply("❌ Faqat Excel (.xlsx) fayllar qabul qilinadi.")
        return

    status_msg = await message.reply("⏳ Katalog yangilanmoqda... (bu biroz vaqt olishi mumkin)")

    try:
        import httpx

        # Download file from Telegram
        file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        async with httpx.AsyncClient() as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        # Send to our API
        api_url = f"{_BASE_URL}/api/products/refresh-catalog"
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                api_url,
                files={"file": (doc.file_name, file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                data={"admin_key": "rassvet2026"},
            )
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ Xatolik: {result.get('error', 'Unknown')}")
            return

        fmt = result.get('format', 'catalog_clean')

        if fmt == '1c_nomenklatura':
            # ── 1C Номенклатура response ──
            lines = [
                "✅ <b>1C sinxronizatsiya bajarildi!</b>\n",
                f"📊 1C mahsulotlar: {result.get('excel_products', 0)}",
                f"🗄 Bazadagi: {result.get('db_products_before', 0)}",
                f"🔗 Mos kelgan: {result.get('matched', 0)}",
                f"✏️ Yangilangan: {result.get('updated_products', 0)}",
            ]

            pc = result.get('price_changes', 0)
            wc = result.get('weight_changes', 0)
            if pc:
                lines.append(f"💰 Narx o'zgarishlari: {pc}")
            if wc:
                lines.append(f"⚖️ Og'irlik o'zgarishlari: {wc}")

            # Sample price changes
            samples = result.get('sample_price_changes', [])
            if samples:
                lines.append(f"\n<b>Narx o'zgarishlari (namuna):</b>")
                for c in samples[:10]:
                    arrow = "📈" if c['new'] > c['old'] else "📉"
                    lines.append(f"{arrow} {c['name']}: ${c['old']:.2f} → ${c['new']:.2f}")
                if len(samples) > 10:
                    lines.append(f"  ... va yana {len(samples) - 10} ta")

            # New products detected
            new_total = result.get('new_in_1c_total', 0)
            new_names = result.get('new_in_1c', [])
            if new_total:
                lines.append(f"\n🆕 <b>1C'da yangi ({new_total} ta — katalogda yo'q):</b>")
                for n in new_names[:10]:
                    lines.append(f"  • {n}")
                if new_total > 10:
                    lines.append(f"  ... va yana {new_total - 10} ta")
                lines.append("ℹ️ Yangi mahsulotlar qo'l bilan Rassvet_Master'ga kiritilishi kerak")
        else:
            # ── Catalog Clean response (original) ──
            lines = [
                "✅ <b>Katalog yangilandi!</b>\n",
                f"📊 Excel: {result.get('excel_products', 0)} ta mahsulot",
                f"🗄 Bazadagi: {result.get('db_products_before', 0)} ta",
                "",
            ]

            new_count = result.get('new_products', 0)
            updated_count = result.get('updated_products', 0)
            deactivated_count = result.get('deactivated_products', 0)
            reactivated_count = result.get('reactivated_products', 0)

            if new_count:
                lines.append(f"🆕 Yangi qo'shildi: {new_count}")
            if updated_count:
                lines.append(f"✏️ Yangilandi: {updated_count}")
            if reactivated_count:
                lines.append(f"♻️ Qayta faollashtirildi: {reactivated_count}")
            if deactivated_count:
                lines.append(f"🚫 O'chirildi: {deactivated_count}")

            if not any([new_count, updated_count, deactivated_count, reactivated_count]):
                lines.append("ℹ️ O'zgarish yo'q — katalog yangi.")

            # Show sample new products
            new_names = result.get('new_product_names', [])
            if new_names:
                lines.append(f"\n<b>Yangi mahsulotlar:</b>")
                for n in new_names[:10]:
                    lines.append(f"  • {n}")
                if len(new_names) > 10:
                    lines.append(f"  ... va yana {len(new_names) - 10} ta")

            # Show deactivated products
            deact_names = result.get('deactivated_names', [])
            if deact_names:
                lines.append(f"\n<b>O'chirilganlar:</b>")
                for n in deact_names[:10]:
                    lines.append(f"  • {n}")
                if len(deact_names) > 10:
                    lines.append(f"  ... va yana {len(deact_names) - 10} ta")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Catalog refresh error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:200]}")


async def _download_and_import(doc) -> dict:
    """Download a Telegram document and import it as a balance file.
    Returns the API result dict.
    """
    import httpx

    file = await bot.get_file(doc.file_id)
    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

    async with httpx.AsyncClient() as client:
        resp = await client.get(file_url)
        file_bytes = resp.content

    api_url = f"{_BASE_URL}/api/finance/import-balances"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            api_url,
            files={"file": (doc.file_name, file_bytes, "application/vnd.ms-excel")},
            data={"admin_key": "rassvet2026"},
        )
        return resp.json()


def _format_import_result(result: dict, file_label: str = "") -> str:
    """Format a single import result into readable lines."""
    sections = result.get('sections', [])
    lines = []

    if file_label:
        lines.append(f"📄 <b>{html_escape(file_label)}</b>")

    lines.append(f"📅 Davr: {result.get('period', '?')}")

    if len(sections) > 1:
        for sec in sections:
            cur = sec['currency']
            emoji = '💵' if cur == 'USD' else '💴'
            lines.append(f"  {emoji} {cur}: {sec['clients']} mijoz, {sec['inserted']} yangi, {sec['updated']} yangilangan")

    lines.extend([
        f"👥 Mijozlar: {result['total_clients_in_file']}",
        f"🆕 Yangi: {result['inserted']} · ✏️ Yangilangan: {result['updated']}",
        f"🔗 Bog'langan: {result['matched_to_app']}",
    ])

    skipped = result.get('skipped_zero', 0)
    if skipped > 0:
        lines.append(f"⏭️ Bo'sh qatorlar: {skipped}")

    unmatched = result.get('unmatched_count', 0)
    if unmatched > 0:
        lines.append(f"⚠️ Bog'lanmagan: {unmatched}")

    return "\n".join(lines)


@dp.message(Command("balances"))
async def cmd_balances(message: types.Message):
    """Import client balances from 1C оборотно-сальдовая.

    Supports:
    - Single file: send XLS with /balances caption, or reply /balances to a file
    - Multiple files: send 2+ XLS as album, then reply /balances to any of them
    """
    if not is_admin(message):
        return

    # Collect documents to process
    docs = []

    # Check if replying to an album (media group)
    if message.reply_to_message and message.reply_to_message.media_group_id:
        gid = message.reply_to_message.media_group_id
        if gid in _album_buffers and _album_buffers[gid]["messages"]:
            for m in _album_buffers[gid]["messages"]:
                if m.document and m.document.file_name and m.document.file_name.endswith(('.xls', '.xlsx')):
                    docs.append(m.document)
            _album_buffers[gid]["processed"] = True

    # Single file: reply to document or caption on document
    if not docs:
        doc = None
        if message.reply_to_message and message.reply_to_message.document:
            doc = message.reply_to_message.document
        elif message.document:
            doc = message.document

        if doc:
            docs.append(doc)

    if not docs:
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "1️⃣ <b>Bitta fayl:</b> XLS faylni /balances caption bilan yuboring\n"
            "2️⃣ <b>Ikki fayl:</b> Ikkala XLS ni album sifatida yuboring,"
            " so'ng istalgan biriga /balances deb javob yozing\n\n"
            "<b>Ma'lumotlar:</b>\n"
            "💳 Дебет = отгрузки (jo'natilgan tovarlar)\n"
            "💰 Кредит = оплаты (to'lovlar)\n"
            "📊 Сальдо = qarz (дебет − кредит)",
            parse_mode="HTML",
        )
        return

    # Validate all files
    for doc in docs:
        if not doc.file_name or not doc.file_name.endswith(('.xls', '.xlsx')):
            await message.reply(f"❌ Faqat Excel fayllar: {doc.file_name}")
            return

    file_count = len(docs)
    status_msg = await message.reply(
        f"⏳ {file_count} ta fayl yuklanmoqda..." if file_count > 1
        else "⏳ Moliyaviy ma'lumotlar yuklanmoqda..."
    )

    try:
        all_lines = [f"✅ <b>Moliyaviy ma'lumotlar yuklandi!</b>\n"]

        for i, doc in enumerate(docs):
            result = await _download_and_import(doc)
            if not result.get("ok"):
                all_lines.append(f"❌ {html_escape(doc.file_name)}: {result.get('error', 'Unknown')}")
                continue
            label = doc.file_name if file_count > 1 else ""
            all_lines.append(_format_import_result(result, label))
            if i < file_count - 1:
                all_lines.append("")  # separator

        # DB totals
        import httpx
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"{_BASE_URL}/api/admin/debug-query",
                params={
                    "admin_key": "rassvet2026",
                    "q": "SELECT COUNT(DISTINCT client_name_1c) as c, COUNT(DISTINCT period_start||currency) as p FROM client_balances",
                },
            )
            db = r.json().get("rows", [{}])[0]
            all_lines.append(f"\n📊 Bazada jami: {db.get('c', '?')} mijoz, {db.get('p', '?')} davr")

        await status_msg.edit_text("\n".join(all_lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Balance import error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:200]}")


@dp.message(Command("testclient"))
async def cmd_testclient(message: types.Message):
    """Link admin's account to a 1C client for testing the Cabinet balance view.

    Usage:
        /testclient              — show current link + top clients to choose from
        /testclient КЛИЕНТ       — search by name and link to first match
        /testclient #123         — link to allowed_clients.id directly
        /testclient clear        — remove the test link
    """
    if not is_admin(message):
        return

    telegram_id = message.from_user.id
    conn = get_db()
    parts = message.text.split(maxsplit=1)
    arg = parts[1].strip() if len(parts) > 1 else ""

    # Ensure this admin has a users record
    user = conn.execute("SELECT client_id FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()
    if not user:
        conn.execute(
            "INSERT INTO users (telegram_id, first_name, is_approved) VALUES (?, ?, 1)",
            (telegram_id, message.from_user.first_name or "Admin"),
        )
        conn.commit()
        user = conn.execute("SELECT client_id FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()

    # /testclient clear — remove link
    if arg.lower() == 'clear':
        conn.execute("UPDATE users SET client_id = NULL WHERE telegram_id = ?", (telegram_id,))
        conn.commit()
        conn.close()
        await message.reply("✅ Test link removed. Cabinet will show no balance data.")
        return

    # /testclient #ID — link by allowed_clients.id
    if arg.startswith('#') and arg[1:].isdigit():
        target_id = int(arg[1:])
        target = conn.execute(
            "SELECT id, name, client_id_1c FROM allowed_clients WHERE id = ?", (target_id,)
        ).fetchone()
        if not target:
            conn.close()
            await message.reply(f"❌ allowed_clients ID {target_id} not found.")
            return
        conn.execute("UPDATE users SET client_id = ? WHERE telegram_id = ?", (target_id, telegram_id))
        conn.commit()
        # Check if this client has balance data
        bal_count = conn.execute(
            "SELECT COUNT(*) FROM client_balances WHERE client_id = ?", (target_id,)
        ).fetchone()[0]
        conn.close()
        await message.reply(
            f"✅ Linked to: <b>{html_escape(target['name'] or '—')}</b>\n"
            f"1C: <code>{html_escape(target['client_id_1c'] or '—')}</code>\n"
            f"Balance records: {bal_count}\n\n"
            f"Open 🏛️ Cabinet to see their data.",
            parse_mode="HTML",
        )
        return

    # /testclient addclient NAME — auto-create allowed_clients record from client_balances
    if arg.lower() == 'addclient' and len(parts) > 1:
        rest = message.text.split(maxsplit=2)
        if len(rest) < 3 or not rest[2].strip():
            conn.close()
            await message.reply(
                "❌ <b>Foydalanish:</b>\n"
                "<code>/testclient addclient 1C nomi</code>",
                parse_mode="HTML",
            )
            return
        client_name_1c = rest[2].strip()
        # Verify this name exists in client_balances
        cb_exists = conn.execute(
            "SELECT COUNT(*) FROM client_balances WHERE client_name_1c = ?",
            (client_name_1c,),
        ).fetchone()[0]
        if not cb_exists:
            conn.close()
            await message.reply(f"❌ 1C'da <b>{html_escape(client_name_1c)}</b> topilmadi.", parse_mode="HTML")
            return
        # Check if already in allowed_clients
        existing = conn.execute(
            "SELECT id FROM allowed_clients WHERE client_id_1c = ? LIMIT 1",
            (client_name_1c,),
        ).fetchone()
        if existing:
            conn.close()
            await message.reply(
                f"ℹ️ Allaqachon ro'yxatda: #{existing['id']}\n"
                f"<code>/testclient #{existing['id']}</code>",
                parse_mode="HTML",
            )
            return
        # Create allowed_clients record with client_id_1c set
        conn.execute(
            "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, source_sheet, status) "
            "VALUES (?, ?, ?, ?, ?)",
            ("", client_name_1c, client_name_1c, "bot_from_1c", "active"),
        )
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        # Link client_balances records to this new allowed_clients.id
        conn.execute(
            "UPDATE client_balances SET client_id = ? WHERE client_name_1c = ? AND client_id IS NULL",
            (new_id, client_name_1c),
        )
        conn.commit()
        conn.close()
        await message.reply(
            f"✅ Mijoz qo'shildi!\n\n"
            f"🏢 1C nomi: <b>{html_escape(client_name_1c)}</b>\n"
            f"🆔 ID: #{new_id}\n\n"
            f"Bog'lash: <code>/testclient #{new_id}</code>",
            parse_mode="HTML",
        )
        return

    # /testclient link TELEGRAM_ID CLIENT_ID — link any user to a client
    if arg.lower() == 'link' and len(parts) > 1:
        rest = message.text.split(maxsplit=2)
        link_args = rest[2].strip().split() if len(rest) > 2 else []
        if len(link_args) == 2 and link_args[0].isdigit() and link_args[1].isdigit():
            tg_id = int(link_args[0])
            client_id = int(link_args[1])
            client = conn.execute(
                "SELECT id, name, client_id_1c FROM allowed_clients WHERE id = ?",
                (client_id,),
            ).fetchone()
            if not client:
                conn.close()
                await message.reply(f"❌ Mijoz #{client_id} topilmadi.")
                return
            conn.execute(
                "UPDATE users SET client_id = ?, is_approved = 1 WHERE telegram_id = ?",
                (client_id, tg_id),
            )
            conn.execute(
                "UPDATE allowed_clients SET matched_telegram_id = ? WHERE id = ?",
                (tg_id, client_id),
            )
            conn.commit()
            # Persist to backup
            try:
                from backend.services.backup_users import save_user_to_backup
                row = conn.execute(
                    "SELECT telegram_id, phone, first_name, last_name, username, "
                    "latitude, longitude, is_approved, client_id FROM users WHERE telegram_id = ?",
                    (tg_id,),
                ).fetchone()
                if row:
                    save_user_to_backup(dict(row))
            except Exception as e:
                logging.warning(f"backup after /testclient link failed: {e}")
            conn.close()
            name = html_escape(client["client_id_1c"] or client["name"] or f"#{client_id}")
            await message.reply(
                f"✅ Bog'landi!\n"
                f"👤 Telegram ID: <code>{tg_id}</code>\n"
                f"🏢 Mijoz: {name} (#{client_id})",
                parse_mode="HTML",
            )
            return
        else:
            conn.close()
            await message.reply(
                "❌ <b>Foydalanish:</b>\n"
                "<code>/testclient link TELEGRAM_ID CLIENT_ID</code>",
                parse_mode="HTML",
            )
            return

    # /testclient NAME — search by name (Cyrillic or Latin), always show list
    if arg:
        search = f"%{arg.lower()}%"
        # Case-insensitive search using LOWER() — needed for Cyrillic (LIKE is ASCII-only)
        # Search across allowed_clients (name, client_id_1c) AND client_balances (client_name_1c)
        matches = conn.execute(
            """SELECT ac.id, ac.name, ac.client_id_1c,
                      (SELECT COUNT(*) FROM client_balances WHERE client_id = ac.id) as bal_count
               FROM allowed_clients ac
               WHERE LOWER(ac.client_id_1c) LIKE ? OR LOWER(ac.name) LIKE ?
                  OR ac.id IN (
                      SELECT DISTINCT client_id FROM client_balances
                      WHERE LOWER(client_name_1c) LIKE ? AND client_id IS NOT NULL
                  )
               ORDER BY bal_count DESC
               LIMIT 15""",
            (search, search, search),
        ).fetchall()

        # Fallback: search client_balances for clients NOT in allowed_clients
        cb_only = []
        if len(matches) < 15:
            cb_only = conn.execute(
                """SELECT DISTINCT cb.client_name_1c,
                          COUNT(*) as bal_count,
                          MAX(cb.period_end) as latest_period
                   FROM client_balances cb
                   WHERE LOWER(cb.client_name_1c) LIKE ?
                     AND (cb.client_id IS NULL
                          OR cb.client_id NOT IN (SELECT id FROM allowed_clients))
                   GROUP BY cb.client_name_1c
                   LIMIT ?""",
                (search, 15 - len(matches)),
            ).fetchall()

        if not matches and not cb_only:
            conn.close()
            await message.reply(
                f"❌ '{html_escape(arg)}' bo'yicha hech narsa topilmadi.\n\n"
                f"💡 1C dagi nom bilan qidiring, masalan:\n"
                f"<code>/testclient Улугбек</code>",
                parse_mode="HTML",
            )
            return

        lines = []

        if matches:
            lines.append(f"🔍 <b>{len(matches)}</b> ta natija '{html_escape(arg)}' bo'yicha:\n")
            lines.append("<b>📋 Ro'yxatdagi mijozlar:</b>")
            for m in matches:
                name_1c = html_escape(m['client_id_1c'] or '—')
                name_app = html_escape(m['name'] or '—')
                # Show both names if they differ, otherwise just one
                if m['client_id_1c'] and m['name'] and m['client_id_1c'].strip() != m['name'].strip():
                    display = f"{name_1c}\n      📱 {name_app}"
                else:
                    display = name_1c if m['client_id_1c'] else name_app
                lines.append(
                    f"  <code>/testclient #{m['id']}</code> — {display}"
                    f"  [{m['bal_count']} oy]"
                )

        if cb_only:
            lines.append("")
            lines.append("<b>📒 Faqat 1C'da (ro'yxatda yo'q):</b>")
            for c in cb_only:
                name = html_escape(c['client_name_1c'])
                lines.append(f"  🟡 <b>{name}</b>")
                lines.append(f"     📊 {c['bal_count']} yozuv | oxirgi: {c['latest_period'] or '?'}")
                lines.append(f"     ➕ <code>/testclient addclient {c['client_name_1c']}</code>")

        if matches:
            lines.append(f"\n👆 Kerakli mijozni tanlang — <code>/testclient #ID</code>")
        elif cb_only:
            lines.append(f"\n💡 Avval <code>/testclient addclient ...</code> bilan ro'yxatga qo'shing")

        conn.close()
        await message.reply("\n".join(lines), parse_mode="HTML")
        return

    # /testclient (no args) — show current state + usage hints
    current_name = "—"
    current_1c = "—"
    current_bal = 0
    if user["client_id"]:
        linked = conn.execute(
            "SELECT name, client_id_1c FROM allowed_clients WHERE id = ?", (user["client_id"],)
        ).fetchone()
        if linked:
            current_name = html_escape(linked["name"] or "—")
            current_1c = html_escape(linked["client_id_1c"] or "—")
            current_bal = conn.execute(
                "SELECT COUNT(*) FROM client_balances WHERE client_id = ?", (user["client_id"],)
            ).fetchone()[0]
    conn.close()

    lines = [
        f"🔗 <b>Joriy bog'lanish:</b>\n"
        f"  Mijoz: {current_name}\n"
        f"  1C: <code>{current_1c}</code>\n"
        f"  Balans yozuvlari: {current_bal} oy\n",
    ]

    lines.append("<b>Foydalanish:</b>")
    lines.append("<code>/testclient Улугбек</code> — 1C nomi bo'yicha qidirish")
    lines.append("<code>/testclient #123</code> — ID bo'yicha bog'lash")
    lines.append("<code>/testclient clear</code> — bog'lanishni o'chirish")

    await message.reply("\n".join(lines), parse_mode="HTML")


@dp.message(Command("demand"))
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


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    """Show available admin commands."""
    await message.reply(
        "📋 <b>Admin buyruqlar:</b>\n\n"
        "<b>/add</b> <code>telefon ism joylashuv</code>\n"
        "Yangi mijozni qo'shish\n\n"
        "<b>/approve</b> <code>telegram_id</code>\n"
        "Foydalanuvchini tasdiqlash\n\n"
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
        "<b>/relinkrealorders</b>\n"
        "Bog'lanmagan haqiqiy buyurtmalarni qayta bog'lash (allowed_clients yangilagandan keyin)\n\n"
        "<b>/clientmaster</b> (reply to XLSX file)\n"
        "Client Master jadvalini allowed_clients ga import qilish (1C cyrillic nomlari + telefonlar)\n\n"
        "<b>/realordersample</b> <code>&lt;mijoz parchasi&gt;</code>\n"
        "Diagnostika: bitta haqiqiy buyurtmaning xom narx ustunlari (DB dump)\n\n"
        "<b>/backfillrealordertotals</b>\n"
        "Mavjud haqiqiy buyurtmalarda yo'qolgan jami narxlarni qayta hisoblash (1 marta ishlatiladi)\n\n"
        "<b>/testclient</b> <code>[имя или #ID]</code>\n"
        "Test: link your account to a client's balance data\n\n"
        "<b>/chatid</b>\n"
        "Chat va User ID ko'rish\n\n"
        "<b>/reports</b>\n"
        "Oxirgi xatolik xabarlari va mahsulot so'rovlari\n\n"
        "<b>/wrongphotos</b>\n"
        "Noto'g'ri rasm xabarlari (mahsulot bo'yicha)\n\n"
        "<b>/searches</b> <code>[kunlar]</code>\n"
        "Qidiruv statistikasi (default: 7 kun)\n\n"
        "<b>/datacoverage</b> <code>[valyuta]</code>\n"
        "Yuklangan ma'lumotlar qamrovi (oylik tekshiruv)",
        parse_mode="HTML",
    )


@dp.message(Command("reports"))
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


@dp.message(Command("wrongphotos"))
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


@dp.message(Command("searches"))
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


# ───────────────────────────────────────────
# Handle document uploads with command captions
# ───────────────────────────────────────────

@dp.message(F.document & F.caption.startswith("/prices"))
async def handle_prices_document(message: types.Message):
    """Handle Excel file sent with /prices as caption."""
    if not is_admin(message):
        return
    await cmd_prices(message)


@dp.message(F.document & F.caption.startswith("/stock"))
async def handle_stock_document(message: types.Message):
    """Handle Excel file sent with /stock as caption."""
    if not is_admin(message):
        return
    await cmd_stock(message)


@dp.message(F.document & F.caption.startswith("/catalog"))
async def handle_catalog_document(message: types.Message):
    """Handle Excel file sent with /catalog as caption."""
    if not is_admin(message):
        return
    await cmd_catalog(message)


@dp.message(F.document & F.caption.startswith("/balances"))
async def handle_balances_document(message: types.Message):
    """Handle XLS file sent with /balances as caption."""
    if not is_admin(message):
        return
    await cmd_balances(message)


# ── Multi-file balance import (media group / album) ──
_album_buffers: dict = {}  # group_id -> {files: [], timer: ...}


@dp.message(F.media_group_id & F.document)
async def handle_album_document(message: types.Message):
    """Collect album files for multi-file balance import.
    Send 2+ XLS files as an album, then reply /balances to any of them.
    Files are buffered briefly so they can be imported together.
    """
    if not is_admin(message):
        return
    gid = message.media_group_id
    if gid not in _album_buffers:
        _album_buffers[gid] = {"messages": [], "processed": False}
    _album_buffers[gid]["messages"].append(message)


# ───────────────────────────────────────────
# /debtors — import дебиторская задолженность
# ───────────────────────────────────────────

@dp.message(Command("debtors"))
async def cmd_debtors(message: types.Message):
    """Import client debts from 1C 'Дебиторская задолженность на дату'.
    Reply to XLS file with /debtors, or send file with /debtors caption.
    """
    if not is_admin(message):
        return

    doc = None
    if message.reply_to_message and message.reply_to_message.document:
        doc = message.reply_to_message.document
    elif message.document:
        doc = message.document

    if not doc:
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "1C'dan «Дебиторская задолженность на дату» XLS faylni\n"
            "/debtors caption bilan yuboring.\n\n"
            "Yoki faylga javob sifatida /debtors yozing.",
            parse_mode="HTML",
        )
        return

    if not doc.file_name or not doc.file_name.endswith(('.xls', '.xlsx')):
        await message.reply("❌ Faqat Excel (.xls/.xlsx) fayllar qabul qilinadi.")
        return

    status_msg = await message.reply("⏳ Дебиторка yuklanmoqda...")

    try:
        import httpx

        file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        async with httpx.AsyncClient() as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        api_url = f"{_BASE_URL}/api/finance/import-debts"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                api_url,
                files={"file": (doc.file_name, file_bytes, "application/vnd.ms-excel")},
                data={"admin_key": "rassvet2026"},
            )
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ Xatolik: {result.get('error', 'Unknown')}")
            return

        lines = [
            f"✅ <b>Дебиторка yuklandi!</b>\n",
            f"📅 Sana: {result.get('report_date', '?')}",
            f"👥 Qarzdorlar: {result['total_clients']}",
            f"🔗 Ilovaga bog'langan: {result['matched_to_app']}",
            f"\n💴 Jami UZS: {round(result['total_uzs']):,}".replace(',', ' '),
            f"💵 Jami USD: ${result['total_usd']:,.2f}",
        ]

        unmatched = result.get('unmatched_count', 0)
        if unmatched > 0:
            lines.append(f"\n⚠️ Bog'lanmagan ({unmatched}):")
            for name in result.get('unmatched_sample', [])[:10]:
                lines.append(f"  • {html_escape(name)}")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Debtors import error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:200]}")


@dp.message(F.document & F.caption.startswith("/debtors"))
async def handle_debtors_document(message: types.Message):
    """Handle XLS file sent with /debtors as caption."""
    if not is_admin(message):
        return
    await cmd_debtors(message)


# ───────────────────────────────────────────
# /datacoverage — check uploaded data coverage
# ───────────────────────────────────────────

@dp.message(Command("datacoverage"))
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


# ───────────────────────────────────────────
# /realorders — import 1C "Реализация товаров"
# ───────────────────────────────────────────

@dp.message(Command("realorders"))
async def cmd_realorders(message: types.Message):
    """Import real (shipped) orders from 1C 'Реализация товаров' export.

    Reply to an XLS/XLSX file with /realorders, or send a file with
    /realorders as the caption. Idempotent on doc_number_1c — re-uploading
    the same period replaces existing documents.
    """
    if not is_admin(message):
        return

    doc = None
    if message.reply_to_message and message.reply_to_message.document:
        doc = message.reply_to_message.document
    elif message.document:
        doc = message.document

    if not doc:
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "1C'dan «Реализация товаров» XLS/XLSX faylni\n"
            "/realorders caption bilan yuboring.\n\n"
            "Yoki faylga javob sifatida /realorders yozing.\n\n"
            "💡 Bir oy uchun bitta fayl. Aynan o'sha oyni\n"
            "qayta yuklasangiz — yangilanadi (dublikat bo'lmaydi).",
            parse_mode="HTML",
        )
        return

    if not doc.file_name or not doc.file_name.lower().endswith(('.xls', '.xlsx')):
        await message.reply("❌ Faqat Excel (.xls/.xlsx) fayllar qabul qilinadi.")
        return

    status_msg = await message.reply("⏳ Реализация yuklanmoqda...")

    try:
        import httpx

        file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        api_url = f"{_BASE_URL}/api/finance/import-real-orders"
        async with httpx.AsyncClient(timeout=300) as client:
            resp = await client.post(
                api_url,
                files={"file": (doc.file_name, file_bytes, "application/vnd.ms-excel")},
                data={"admin_key": "rassvet2026"},
            )
            result = resp.json()

        if not result.get("ok"):
            err_lines = [f"❌ Xatolik: {result.get('error', 'Unknown')}"]
            diag = result.get("diagnostics")
            if diag:
                err_lines.append("")
                err_lines.append("<b>Birinchi qatorlar (diagnostika):</b>")
                err_lines.append("<pre>")
                for i, row in enumerate(diag[:20]):
                    non_empty = [c for c in row if c]
                    if not non_empty:
                        continue
                    line = f"r{i:02d}: " + " | ".join(non_empty[:8])
                    err_lines.append(html_escape(line[:180]))
                err_lines.append("</pre>")
            msg = "\n".join(err_lines)
            if len(msg) > 3800:
                msg = msg[:3800] + "\n...(truncated)"
            await status_msg.edit_text(msg, parse_mode="HTML")
            return

        st = result.get("stats", {})
        date_min = st.get("date_min") or "?"
        date_max = st.get("date_max") or "?"
        date_range = date_min if date_min == date_max else f"{date_min} — {date_max}"

        lines = [
            f"✅ <b>Реализация yuklandi!</b>\n",
            f"📅 Davr: {date_range} ({st.get('date_count', 0)} kun)",
            f"📄 Hujjatlar: {st.get('doc_count', 0)} (yangi: {result.get('inserted_docs', 0)}, yangilangan: {result.get('updated_docs', 0)})",
            f"📦 Qatorlar: {result.get('inserted_items', 0)}",
            f"👥 Mijozlar: {st.get('client_count', 0)} (bog'langan: {result.get('matched_clients', 0)})",
            f"🛒 Mahsulotlar: {st.get('product_count', 0)} (mos: {result.get('matched_products', 0)})",
        ]

        total_local = st.get("total_local") or 0
        total_currency = st.get("total_currency") or 0
        if total_local:
            lines.append(f"\n💴 Jami (mahalliy): {round(total_local):,}".replace(",", " "))
        if total_currency:
            lines.append(f"💵 Jami (valyuta): {total_currency:,.2f}".replace(",", " "))

        unmatched_c = result.get("unmatched_clients_count", 0)
        unmatched_p = result.get("unmatched_products_count", 0)

        if unmatched_c:
            lines.append(f"\n⚠️ Mijozlar bog'lanmagan ({unmatched_c}):")
            for name in result.get("unmatched_clients_sample", [])[:8]:
                lines.append(f"  • {html_escape(name)}")

        if unmatched_p:
            lines.append(f"\n⚠️ Mahsulotlar mos kelmadi ({unmatched_p}):")
            for name in result.get("unmatched_products_sample", [])[:8]:
                lines.append(f"  • {html_escape(name[:60])}")

        lines.append(f"\n💾 DBda jami: {result.get('db_total_docs', 0)} hujjat / {result.get('db_total_items', 0)} qator")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Realorders import error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


@dp.message(F.document & F.caption.startswith("/realorders"))
async def handle_realorders_document(message: types.Message):
    """Handle XLS/XLSX file sent with /realorders as caption."""
    if not is_admin(message):
        return
    await cmd_realorders(message)


@dp.message(Command("unmatchedclients"))
async def cmd_unmatchedclients(message: types.Message):
    """Show real_orders rows where client_id is NULL, grouped by 1C name.

    Ranks by doc count so ops can prioritize the biggest offenders. System
    correction docs (ИСПРАВЛЕНИЕ / ИСПРАВЛЕНИЕ СКЛАД 2) are filtered out.
    Use this after /realorders to see who isn't linking and in what volume.
    """
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Bog'lanmagan mijozlar tekshirilmoqda...")

    try:
        import httpx

        api_url = f"{_BASE_URL}/api/finance/unmatched-real-clients"
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(
                api_url,
                params={"admin_key": "rassvet2026", "limit": 30},
            )
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(
                f"❌ Xatolik: {result.get('error', 'Unknown')}"
            )
            return

        total_docs = result.get("db_total_docs", 0)
        matched = result.get("db_matched_docs", 0)
        unmatched = result.get("db_unmatched_docs", 0)
        match_pct = (matched / total_docs * 100) if total_docs else 0
        skipped_sys = result.get("skipped_system_docs", 0)
        after_skip_docs = result.get("total_unmatched_docs_after_skip", 0)
        after_skip_local = result.get("total_unmatched_local_after_skip", 0)
        items = result.get("items", [])

        lines = [
            "📊 <b>Bog'lanmagan haqiqiy buyurtmalar</b>\n",
            f"💾 Jami hujjatlar: {total_docs}",
            f"✅ Bog'langan: {matched} ({match_pct:.1f}%)",
            f"❌ Bog'lanmagan: {unmatched}",
        ]
        if skipped_sys:
            lines.append(f"⚪ Tizim hujjatlari (o'tkazildi): {skipped_sys}")
        if after_skip_local:
            lines.append(
                f"💴 Haqiqiy bog'lanmagan summa: {after_skip_local:,}".replace(",", " ")
            )
        lines.append(f"\n👥 Noyob nomlar ({len(items)}) — eng ko'p hujjatdan:")

        if not items:
            lines.append("\n🎉 Hammasi bog'langan!")
        else:
            for i, it in enumerate(items[:20], 1):
                name = it["client_name_1c"] or "(empty)"
                if len(name) > 50:
                    name = name[:47] + "..."
                period = it["first_seen"] or "?"
                if it["last_seen"] and it["last_seen"] != it["first_seen"]:
                    period = f"{it['first_seen']}…{it['last_seen']}"
                local = it.get("total_local", 0)
                local_str = f"{local:,}".replace(",", " ") if local else "0"
                lines.append(
                    f"\n{i}. <b>{html_escape(name)}</b>\n"
                    f"   📄 {it['doc_count']} hujjat · 💴 {local_str} · {period}"
                )

        lines.append(
            "\n\n💡 <code>/relinkrealorders</code> — agar "
            "<code>allowed_clients</code>da nom yoki client_id_1c qo'shgan bo'lsangiz, "
            "bog'lanmagan hujjatlarni qayta bog'lash uchun ishga tushiring."
        )

        msg = "\n".join(lines)
        if len(msg) > 3800:
            msg = msg[:3800] + "\n...(truncated)"
        await status_msg.edit_text(msg, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Unmatchedclients error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


@dp.message(Command("relinkrealorders"))
async def cmd_relinkrealorders(message: types.Message):
    """Re-run client matching on real_orders rows with client_id IS NULL.

    Uses a Python-side cyrillic-aware name comparison (unlike SQLite LOWER
    which is ASCII-only). Safe to run multiple times — already-matched rows
    are never touched. Typically run AFTER updating allowed_clients with
    missing names or client_id_1c values.
    """
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Qayta bog'lash ishlayapti...")

    try:
        import httpx

        api_url = f"{_BASE_URL}/api/finance/relink-real-orders"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                api_url,
                data={"admin_key": "rassvet2026"},
            )
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(
                f"❌ Xatolik: {result.get('error', 'Unknown')}"
            )
            return

        total_docs = result.get("db_total_docs", 0)
        matched = result.get("db_matched_docs", 0)
        unmatched_after = result.get("db_unmatched_docs", 0)
        match_pct = (matched / total_docs * 100) if total_docs else 0

        # "Real client" view — excludes 1C placeholder / walk-in cash / aggregate
        # buckets (Наличка, Организации (переч.), ИСПРАВЛЕНИЕ, etc.) from the
        # denominator. This is the number ops cares about — unmatched placeholder
        # docs are not a data quality problem.
        sys_docs = result.get("db_system_docs", 0)
        real_total = result.get("db_real_client_docs", 0)
        real_matched = result.get("db_real_client_matched", 0)
        real_unmatched = result.get("db_real_client_unmatched", 0)
        real_pct = result.get("db_real_client_match_pct", 0)

        lines = [
            "✅ <b>Qayta bog'lash tugadi</b>\n",
            f"🔍 Skanerlangan: {result.get('scanned', 0)}",
            f"🔗 Bog'landi (jami): {result.get('relinked_total', 0)}",
            f"  • client_id_1c orqali: {result.get('relinked_by_client_id_1c', 0)}",
            f"  • Nom orqali: {result.get('relinked_by_name', 0)}",
            f"❌ Hali ham bog'lanmagan: {result.get('still_unmatched', 0)}",
            f"⚪ Tizim hujjatlari o'tkazildi: {result.get('skipped_system', 0)}",
            "",
            "<b>📊 Haqiqiy mijozlar (placeholder'larsiz):</b>",
            f"💾 Jami: {real_total} hujjat",
            f"✅ Bog'langan: {real_matched} ({real_pct:.1f}%)",
            f"❌ Bog'lanmagan: {real_unmatched}",
            "",
            "<b>📋 Barcha hujjatlar (xom hisob):</b>",
            f"💾 Jami: {total_docs} (shundan {sys_docs} — placeholder)",
            f"✅ Bog'langan: {matched} ({match_pct:.1f}%)",
            f"❌ Bog'lanmagan: {unmatched_after}",
        ]
        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Relinkrealorders error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


# ───────────────────────────────────────────
# /realordersample — diagnostic dump of one real order's price columns
# ───────────────────────────────────────────

@dp.message(Command("realordersample"))
async def cmd_realordersample(message: types.Message):
    """Diagnostic: dump the most recent real_order for any client whose name
    matches the argument substring, with raw DB price columns. Used to
    decide whether a "no price in Cabinet" complaint is a parser bug
    (zeros in DB) or a render bug (data present, UI hiding it).

    Usage:  /realordersample Улугбек
    """
    if not is_admin(message):
        return

    # Extract the substring after the command
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply(
            "ℹ️ Foydalanish: <code>/realordersample &lt;mijoz nomidan parcha&gt;</code>\n"
            "Masalan: <code>/realordersample Улугбек</code>",
            parse_mode="HTML",
        )
        return

    needle = parts[1].strip()
    status_msg = await message.reply("⏳ Diagnostika ishlayapti...")

    try:
        import httpx

        api_url = f"{_BASE_URL}/api/finance/real-order-sample"
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(
                api_url,
                params={"admin_key": "rassvet2026", "client": needle},
            )
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ Xatolik: {result.get('error', 'Unknown')}")
            return

        ac_rows = result.get("matched_allowed_clients") or []
        ro_rows = result.get("real_orders") or []
        items = result.get("items_dump") or []

        if not ro_rows:
            await status_msg.edit_text(
                f"🔍 <b>{needle}</b> — bunga mos real_orders topilmadi.\n"
                f"allowed_clients mosliklari: {len(ac_rows)}",
                parse_mode="HTML",
            )
            return

        sample = result.get("sample_doc") or ro_rows[0]
        lines = [
            f"🔍 <b>Diagnostika: {needle}</b>",
            "",
            f"<b>allowed_clients mosliklari:</b> {len(ac_rows)}",
        ]
        for ac in ac_rows[:3]:
            lines.append(f"  • id={ac['id']} {ac.get('name','')[:40]} ph={ac.get('phone_normalized','') or '—'}")
        if len(ac_rows) > 3:
            lines.append(f"  • +{len(ac_rows) - 3} ko'p…")

        lines += [
            "",
            f"<b>real_orders topildi:</b> {len(ro_rows)}",
            f"<b>Tahlil hujjati:</b>",
            f"  • doc № {sample['doc_number_1c']} · {sample['doc_date']}",
            f"  • client_name_1c: {sample.get('client_name_1c','')}",
            f"  • client_id: {sample.get('client_id') if sample.get('client_id') is not None else '— (bogʻlanmagan)'}",
            f"  • currency: {sample.get('currency') or '—'} · rate: {sample.get('exchange_rate') or '—'}",
            f"  • total_sum (UZS): {sample.get('total_sum') or 0}",
            f"  • total_sum_currency: {sample.get('total_sum_currency') or 0}",
            f"  • item_count: {sample.get('item_count') or 0}",
            "",
            f"<b>Qatorlar (item_count={result.get('items_count', 0)}):</b>",
            f"  • price &gt; 0: {result.get('items_with_price', 0)}",
            f"  • sum_local &gt; 0: {result.get('items_with_sum_local', 0)}",
            f"  • total_local &gt; 0: {result.get('items_with_total_local', 0)}",
            f"  • vat &gt; 0: {result.get('items_with_vat', 0)}",
            f"  • price_currency &gt; 0: {result.get('items_with_price_currency', 0)}",
            f"  • sum_currency &gt; 0: {result.get('items_with_sum_currency', 0)}",
            f"  • total_currency &gt; 0: {result.get('items_with_total_currency', 0)}",
        ]

        # Dump first ~6 items in a compact form. Two-line layout per item:
        # line 1 = UZS columns + vat, line 2 = currency columns. This gives us
        # the full picture of which price columns the parser captured vs missed.
        if items:
            lines += ["", "<b>Birinchi 6 qator (xom DB qiymatlari):</b>"]
            for it in items[:6]:
                name_short = (it.get("product_name_1c") or "")[:30]
                lines.append(f"  · <b>{name_short}</b> qty={it.get('quantity') or 0}")
                lines.append(
                    f"    UZS: price={it.get('price') or 0} "
                    f"sum={it.get('sum_local') or 0} "
                    f"total={it.get('total_local') or 0} "
                    f"vat={it.get('vat') or 0}"
                )
                lines.append(
                    f"    USD: price_cur={it.get('price_currency') or 0} "
                    f"sum_cur={it.get('sum_currency') or 0} "
                    f"total_cur={it.get('total_currency') or 0}"
                )

        text_out = "\n".join(lines)
        if len(text_out) > 4000:
            text_out = text_out[:3990] + "\n…"
        await status_msg.edit_text(text_out, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Realordersample error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


# ───────────────────────────────────────────
# /backfillrealordertotals — heal missing totals on existing real_orders
# ───────────────────────────────────────────

@dp.message(Command("backfillrealordertotals"))
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


# ───────────────────────────────────────────
# /clientmaster — import curated Client Master xlsx into allowed_clients
# ───────────────────────────────────────────

@dp.message(Command("clientmaster"))
async def cmd_clientmaster(message: types.Message):
    """Import the curated Client Master spreadsheet into allowed_clients.

    Reply to an XLSX file with /clientmaster, or send the file with
    /clientmaster as the caption. Reads `Contacts` (cyrillic 1C names +
    multi-phone) and `Usto` (contractor sub-clients). Idempotent — re-running
    updates existing rows in place. After this lands, run /relinkrealorders
    to sweep up real_orders that were unmatched due to missing cyrillic names.
    """
    if not is_admin(message):
        return

    doc = None
    if message.reply_to_message and message.reply_to_message.document:
        doc = message.reply_to_message.document
    elif message.document:
        doc = message.document

    if not doc:
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "Client Master XLSX faylni\n"
            "/clientmaster caption bilan yuboring.\n\n"
            "Yoki faylga javob sifatida /clientmaster yozing.\n\n"
            "💡 Idempotent — bir xil faylni qayta yuklasangiz,\n"
            "mavjud yozuvlar yangilanadi (dublikat bo'lmaydi).",
            parse_mode="HTML",
        )
        return

    if not doc.file_name or not doc.file_name.lower().endswith((".xlsx", ".xls")):
        await message.reply("❌ Faqat Excel (.xlsx) fayllar qabul qilinadi.")
        return

    status_msg = await message.reply("⏳ Client Master yuklanmoqda...")

    try:
        import httpx

        file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        api_url = f"{_BASE_URL}/api/finance/import-client-master"
        async with httpx.AsyncClient(timeout=300) as client:
            resp = await client.post(
                api_url,
                files={"file": (doc.file_name, file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                data={"admin_key": "rassvet2026"},
            )
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(
                f"❌ Xatolik: {result.get('error', 'Unknown')}"
            )
            return

        totals = result.get("totals", {})
        sheets = result.get("sheets", [])

        lines = [
            "✅ <b>Client Master yuklandi!</b>\n",
            f"➕ Yangi (telefon bilan): {totals.get('inserted', 0)}",
            f"➕ Yangi (telefonsiz, nom bo'yicha): {totals.get('phoneless_inserted', 0)}",
            f"♻️ Yangilangan: {totals.get('updated', 0)}",
            f"⚪ O'tkazib yuborilgan (bo'sh): {totals.get('skipped_empty', 0)}",
            "",
        ]

        for sh in sheets:
            sheet_name = sh.get("sheet", "?")
            if sh.get("skipped"):
                lines.append(f"📋 <b>{sheet_name}</b>: {sh['skipped']}")
                continue
            lines.append(
                f"📋 <b>{sheet_name}</b>: {sh.get('rows_seen', 0)} qator → "
                f"+{sh.get('inserted_with_phone', 0)} (tel.) "
                f"+{sh.get('phoneless_inserted', 0)} (nom) "
                f"~{sh.get('updated', 0)}"
            )

        approved = result.get("users_retroactively_approved", 0)
        if approved:
            lines.append(f"\n👤 Telefon bo'yicha tasdiqlandi: {approved} foydalanuvchi")

        lines.append(
            f"\n💾 DBda jami: {result.get('db_total_allowed_clients', 0)} mijoz "
            f"({result.get('db_distinct_client_names', 0)} noyob nom)"
        )
        lines.append(
            "\n💡 <code>/relinkrealorders</code> — endi haqiqiy buyurtmalarni qayta bog'lang."
        )

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Clientmaster import error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


@dp.message(F.document & F.caption.startswith("/clientmaster"))
async def handle_clientmaster_document(message: types.Message):
    """Handle XLSX file sent with /clientmaster as caption."""
    if not is_admin(message):
        return
    await cmd_clientmaster(message)


# ───────────────────────────────────────────
# Fallback — only for private chats
# ───────────────────────────────────────────

@dp.message()
async def fallback(message: types.Message):
    """Handle unrecognized messages in private chats."""
    if message.chat.type != "private":
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📋 Katalogni ochish",
                    web_app=WebAppInfo(url=WEBAPP_URL),
                )
            ]
        ]
    )
    await message.answer(
        "Katalogni ochish uchun quyidagi tugmani bosing:",
        reply_markup=keyboard,
    )


async def main():
    logger.info("Bot started in polling mode...")
    try:
        await bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(
                text="Katalog",
                web_app=WebAppInfo(url=WEBAPP_URL),
            )
        )
        logger.info(f"Menu button set to webapp: {WEBAPP_URL}")
    except Exception as e:
        logger.warning(f"Could not set menu button: {e}")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
