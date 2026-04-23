"""Telegram bot with Mini App integration and admin commands."""
import os
import re
import json
import asyncio
import logging
from html import escape as _h
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonWebApp,
    WebAppInfo,
)

from bot.shared import (
    BOT_TOKEN, WEBAPP_URL, DATABASE_PATH,
    ORDER_GROUP_CHAT_ID, ADMIN_GROUP_CHAT_ID, AGENTS_GROUP_CHAT_ID,
    ADMIN_IDS,
    get_db, normalize_phone, html_escape, is_admin,
)

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# ───────────────────────────────────────────
# Public commands
# ───────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    """Send welcome message with Mini App button, or location prompt if deep-linked."""
    args = message.text.split(maxsplit=1)
    deep_link = args[1] if len(args) > 1 else ""

    if deep_link == "share_location":
        await message.answer(
            "📍 Yetkazib berish manzilini saqlash uchun joylashuvingizni yuboring.\n\n"
            "📎 tugmasini bosing → Joylashuv → yuboring.",
        )
        return

    if deep_link == "support":
        from bot.handlers.support import start_support_prompt
        await start_support_prompt(message)
        return

    if deep_link == "panel":
        await message.answer(
            "🧭 <b>Agent paneli</b>\n\n"
            "• Bugungi valyuta kursi\n"
            "• Mijoz qidirish va kabinetga kirish\n"
            "• Oxirgi mijozlar ro'yxati",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="🧭 Panelni ochish",
                        web_app=WebAppInfo(url=WEBAPP_URL),
                    )
                ]]
            ),
        )
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
    """Add a new client to the whitelist.
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

    conn.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, source_sheet, status) VALUES (?, ?, ?, ?)",
        (phone_norm, client_name or None, "bot_added", "active"),
    )

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
    """Approve a user by Telegram ID. Usage: /approve 123456789"""
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

    conn.execute("UPDATE users SET is_approved = 1 WHERE telegram_id = ?", (telegram_id,))

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


@dp.message(Command("link"))
async def cmd_link(message: types.Message):
    """Link a user to an existing 1C client.
    Usage: /link telegram_id 1C_client_name
       or: /link telegram_id phone_number
    """
    if not is_admin(message):
        return

    parts = message.text.split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit():
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "<code>/link telegram_id 1C_nomi</code>\n"
            "<code>/link telegram_id telefon_raqam</code>\n\n"
            "<b>Misollar:</b>\n"
            "<code>/link 123456789 ООО Акбар</code>\n"
            "<code>/link 123456789 901234567</code>\n\n"
            "Foydalanuvchini mavjud 1C mijozga bog'laydi.",
            parse_mode="HTML",
        )
        return

    telegram_id = int(parts[1])
    lookup = parts[2].strip()
    conn = get_db()

    user = conn.execute(
        "SELECT telegram_id, phone, first_name, is_approved, client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user:
        await message.reply(f"❌ Telegram ID {telegram_id} topilmadi.")
        conn.close()
        return

    lookup_norm = normalize_phone(lookup)
    target_client = None

    if len(lookup_norm) >= 9:
        target_client = conn.execute(
            "SELECT id, client_id_1c, name, phone_normalized FROM allowed_clients "
            "WHERE phone_normalized = ? AND COALESCE(status, 'active') != 'merged' LIMIT 1",
            (lookup_norm,),
        ).fetchone()

    if not target_client:
        target_client = conn.execute(
            "SELECT id, client_id_1c, name, phone_normalized FROM allowed_clients "
            "WHERE client_id_1c = ? AND COALESCE(status, 'active') != 'merged' LIMIT 1",
            (lookup,),
        ).fetchone()

    if not target_client:
        target_client = conn.execute(
            "SELECT id, client_id_1c, name, phone_normalized FROM allowed_clients "
            "WHERE client_id_1c LIKE ? AND COALESCE(status, 'active') != 'merged' LIMIT 1",
            (f"%{lookup}%",),
        ).fetchone()

    if not target_client:
        await message.reply(
            f"❌ Mijoz topilmadi: <b>{lookup}</b>\n\n"
            "1C nomi yoki telefon raqamini tekshiring.",
            parse_mode="HTML",
        )
        conn.close()
        return

    client_id_1c = target_client["client_id_1c"]
    if not client_id_1c:
        await message.reply(
            f"❌ Mijoz (ID={target_client['id']}) da client_id_1c yo'q.\n"
            "Avval 1C nomini belgilash kerak.",
            parse_mode="HTML",
        )
        conn.close()
        return

    user_phone_norm = normalize_phone(user["phone"])
    existing_row = conn.execute(
        "SELECT id, client_id_1c FROM allowed_clients WHERE phone_normalized = ? LIMIT 1",
        (user_phone_norm,),
    ).fetchone()

    if existing_row:
        conn.execute(
            "UPDATE allowed_clients SET client_id_1c = ?, matched_telegram_id = ? WHERE id = ?",
            (client_id_1c, telegram_id, existing_row["id"]),
        )
        client_id = existing_row["id"]
    else:
        conn.execute(
            "INSERT INTO allowed_clients (phone_normalized, name, source_sheet, status, client_id_1c, matched_telegram_id) "
            "VALUES (?, ?, 'bot_linked', 'active', ?, ?)",
            (user_phone_norm, user["first_name"], client_id_1c, telegram_id),
        )
        client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        "UPDATE users SET is_approved = 1, client_id = ? WHERE telegram_id = ?",
        (client_id, telegram_id),
    )
    conn.commit()

    try:
        from backend.services.backup_users import save_user_to_backup
        row = conn.execute(
            "SELECT telegram_id, phone, first_name, last_name, username, latitude, longitude, is_approved, client_id, registered_at FROM users WHERE telegram_id = ?",
            (telegram_id,),
        ).fetchone()
        if row:
            save_user_to_backup(dict(row))
    except Exception as e:
        logging.warning(f"backup after /link failed: {e}")

    siblings = conn.execute(
        "SELECT phone_normalized, name FROM allowed_clients "
        "WHERE client_id_1c = ? AND COALESCE(status, 'active') != 'merged'",
        (client_id_1c,),
    ).fetchall()

    sibling_lines = [f"  📱 {s['phone_normalized']} — {s['name'] or '—'}" for s in siblings]

    conn.close()

    await message.reply(
        f"✅ Bog'landi!\n\n"
        f"📛 {user['first_name'] or '—'} ({user['phone']})\n"
        f"🏢 1C mijoz: <b>{client_id_1c}</b>\n\n"
        f"📋 Bog'langan telefonlar ({len(siblings)}):\n" +
        "\n".join(sibling_lines) +
        f"\n\n💡 Endi kabinet, balans va buyurtma tarixini ko'radi.",
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


# ───────────────────────────────────────────
# Fallback — private chats only (MUST BE LAST)
# ───────────────────────────────────────────

# The fallback lives on the dispatcher (root router), which in aiogram 3 is
# tried BEFORE any included sub-router. Anything the fallback matches is
# swallowed and never reaches `bot/handlers/*`. Exclude message shapes that
# sub-routers need to handle — currently location pins (location_router) and
# documents (uploads_router has document-caption handlers for private DMs too).
@dp.message(F.chat.type == "private", ~F.location, ~F.document)
async def fallback(message: types.Message):
    """Handle unrecognized messages in private chats."""

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

    from bot.handlers.testclient import router as testclient_router
    from bot.handlers.admin import router as admin_router
    from bot.handlers.uploads import router as uploads_router
    from bot.handlers.score import router as score_router
    from bot.handlers.location import router as location_router
    from bot.handlers.orders import router as orders_router
    from bot.handlers.registration import router as registration_router
    from bot.handlers.support import router as support_router

    dp.include_router(testclient_router)
    dp.include_router(admin_router)
    dp.include_router(uploads_router)
    dp.include_router(score_router)
    dp.include_router(orders_router)
    dp.include_router(location_router)
    dp.include_router(registration_router)
    dp.include_router(support_router)
    logger.info("Loaded handler modules: testclient, admin, uploads, score, orders, location, registration, support")

    # Error alerter: any uncaught exception inside a bot handler now posts
    # to Admin group with full traceback (same infrastructure as the
    # FastAPI side). 5-min suppression per error signature.
    try:
        from backend.services.error_alert import install_aiogram_handler
        install_aiogram_handler(dp)
        logger.info("Bot error alerter installed")
    except Exception as _e:
        logger.warning(f"Bot error alerter install failed: {_e}")

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

    try:
        from bot.reminders import start_reminder_tasks
        start_reminder_tasks(bot, ADMIN_GROUP_CHAT_ID)
    except Exception as e:
        logger.error(f"Failed to start daily-upload reminder tasks: {e}")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
