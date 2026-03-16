"""Telegram bot with Mini App integration and admin commands."""
import os
import re
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

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")


def get_db():
    """Get database connection (same as backend)."""
    import sqlite3
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
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

        if result['changes']:
            lines.append("\n<b>O'zgarishlar:</b>")
            for c in result['changes'][:20]:
                old = c['old_usd']
                new = c['new_usd']
                arrow = "📈" if new > old else "📉"
                lines.append(f"{arrow} {c['name']}: ${old:.2f} → ${new:.2f}")
            if len(result['changes']) > 20:
                lines.append(f"... va yana {len(result['changes']) - 20} ta")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Price update error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:200]}")


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
        "<b>/chatid</b>\n"
        "Chat va User ID ko'rish",
        parse_mode="HTML",
    )


# ───────────────────────────────────────────
# Handle document uploads with /prices caption
# ───────────────────────────────────────────

@dp.message(F.document & F.caption.startswith("/prices"))
async def handle_prices_document(message: types.Message):
    """Handle Excel file sent with /prices as caption."""
    if not is_admin(message):
        return
    # Reuse the prices command handler
    await cmd_prices(message)


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
