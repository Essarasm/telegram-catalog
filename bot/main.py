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
    """Check if user is an admin. Allow from sales group or listed admin IDs."""
    if ADMIN_IDS and message.from_user.id in ADMIN_IDS:
        return True
    # Allow commands from the sales managers group
    if message.chat.id == ORDER_GROUP_CHAT_ID:
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

        if result['changes']:
            lines.append("\n<b>O'zgarishlar:</b>")
            for c in result['changes'][:20]:
                old = c['old_usd']
                new = c['new_usd']
                arrow = "📈" if new > old else "📉"
                lines.append(f"{arrow} {c['name']}: ${old:.2f} → ${new:.2f}")
            if len(result['changes']) > 20:
                lines.append(f"... va yana {len(result['changes']) - 20} ta")

        # Show unmatched summary
        unmatched_total = result.get('unmatched_excel_total', 0)
        if unmatched_total > 0:
            lines.append(f"\n⚠️ <b>Mos kelmagan ({unmatched_total} ta Excel'dan):</b>")
            for name in result.get('unmatched_excel', [])[:10]:
                lines.append(f"  • {name}")
            if unmatched_total > 10:
                lines.append(f"  ... va yana {unmatched_total - 10} ta")

        unmatched_db = result.get('unmatched_db_count', 0)
        if unmatched_db > 0:
            lines.append(f"\nℹ️ Bazada {unmatched_db} ta mahsulot Excel'da topilmadi")

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


@dp.message(Command("balances"))
async def cmd_balances(message: types.Message):
    """Import client balances from 1C оборотно-сальдовая. Reply to XLS file with /balances."""
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
            "1. 1C'dan оборотно-сальдовая (счет 40.10) XLS faylni yuboring\n"
            "2. Faylga javob sifatida /balances yozing\n\n"
            "Yoki faylni /balances caption bilan yuboring.\n\n"
            "<b>Ma'lumotlar:</b>\n"
            "💳 Дебет = отгрузки (jo'natilgan tovarlar)\n"
            "💰 Кредит = оплаты (to'lovlar)\n"
            "📊 Сальдо = qarz (дебет − кредит)",
            parse_mode="HTML",
        )
        return

    if not doc.file_name or not doc.file_name.endswith(('.xls', '.xlsx')):
        await message.reply("❌ Faqat Excel (.xls/.xlsx) fayllar qabul qilinadi.")
        return

    status_msg = await message.reply("⏳ Moliyaviy ma'lumotlar yuklanmoqda...")

    try:
        import httpx

        # Download file from Telegram
        file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        async with httpx.AsyncClient() as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        # Send to our API
        api_url = f"{_BASE_URL}/api/finance/import-balances"
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

        sections = result.get('sections', [])
        lines = [
            f"✅ <b>Moliyaviy ma'lumotlar yuklandi!</b>\n",
            f"📅 Davr: {result.get('period', '?')}",
        ]

        # Show per-section breakdown if multiple currencies
        if len(sections) > 1:
            for sec in sections:
                cur = sec['currency']
                emoji = '💵' if cur == 'USD' else '💴'
                lines.append(f"\n{emoji} <b>{cur}:</b>")
                lines.append(f"  👥 Mijozlar: {sec['clients']}")
                lines.append(f"  🆕 Yangi: {sec['inserted']}")
                lines.append(f"  ✏️ Yangilangan: {sec['updated']}")
                lines.append(f"  🔗 Bog'langan: {sec['matched']}")
            lines.append(f"\n<b>Jami:</b>")

        lines.extend([
            f"👥 Mijozlar: {result['total_clients_in_file']}",
            f"🆕 Yangi: {result['inserted']}",
            f"✏️ Yangilangan: {result['updated']}",
            f"🔗 Ilovaga bog'langan: {result['matched_to_app']}",
        ])

        unmatched = result.get('unmatched_count', 0)
        if unmatched > 0:
            lines.append(f"\n⚠️ <b>Bog'lanmagan ({unmatched} ta):</b>")
            for name in result.get('unmatched_sample', [])[:10]:
                lines.append(f"  • {html_escape(name)}")
            if unmatched > 10:
                lines.append(f"  ... va yana {unmatched - 10} ta")

        lines.append(f"\n📊 Bazada jami: {result['db_total_clients']} mijoz, {result['db_total_periods']} davr")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

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
        "<b>/balances</b> (reply to XLS file)\n"
        "Mijoz qarzlari yangilash (1C оборотно-сальдовая)\n\n"
        "<b>/testclient</b> <code>[имя или #ID]</code>\n"
        "Test: link your account to a client's balance data\n\n"
        "<b>/chatid</b>\n"
        "Chat va User ID ko'rish\n\n"
        "<b>/reports</b>\n"
        "Oxirgi xatolik xabarlari va mahsulot so'rovlari\n\n"
        "<b>/searches</b> <code>[kunlar]</code>\n"
        "Qidiruv statistikasi (default: 7 kun)",
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
        """SELECT r.id, p.name_display, p.name, r.report_type, r.note, r.created_at
           FROM reports r
           JOIN products p ON p.id = r.product_id
           ORDER BY r.created_at DESC
           LIMIT 10""",
    ).fetchall()

    # Recent product requests
    requests = conn.execute(
        """SELECT id, request_text, created_at
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

    lines = []

    if reports:
        lines.append(f"🚩 <b>Xatolik xabarlari ({len(reports)}):</b>\n")
        for r in reports:
            name = r["name_display"] or r["name"]
            tl = type_labels.get(r["report_type"], r["report_type"])
            line = f"#{r['id']} {tl} — {name}"
            if r["note"]:
                line += f"\n   💬 {r['note'][:60]}"
            lines.append(line)
    else:
        lines.append("🚩 Xatolik xabarlari yo'q.")

    lines.append("")

    if requests:
        lines.append(f"🔍 <b>Mahsulot so'rovlari ({len(requests)}):</b>\n")
        for pr in requests:
            lines.append(f"#{pr['id']} {pr['request_text'][:80]}")
    else:
        lines.append("🔍 Mahsulot so'rovlari yo'q.")

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
