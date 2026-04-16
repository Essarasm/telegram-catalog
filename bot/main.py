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
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    MenuButtonWebApp,
    WebAppInfo,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
_BASE_URL = os.getenv("WEBAPP_URL", "https://telegram-catalog-production.up.railway.app")
WEBAPP_URL = f"{_BASE_URL}?v=15"
ORDER_GROUP_CHAT_ID = int(os.getenv("ORDER_GROUP_CHAT_ID", "-1003740010463"))
ADMIN_GROUP_CHAT_ID = int(os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051"))
AGENTS_GROUP_CHAT_ID = int(os.getenv("AGENTS_GROUP_CHAT_ID", "-1003922400481"))

# Admin user IDs who can use /add, /approve, /list commands
# Add Alisher's ID and other manager IDs via env var or hardcode below
ADMIN_IDS = set()
_admin_env = os.getenv("ADMIN_IDS", "")
if _admin_env:
    ADMIN_IDS = {int(x.strip()) for x in _admin_env.split(",") if x.strip().isdigit()}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from backend.services.credit_scoring import (
    run_nightly_scoring,
    search_client_scores,
    get_scoring_summary,
    apply_score_adjustment,
    detect_anomalies,
)


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


def _sender_display_name(message: types.Message) -> str:
    """Best-effort cached display name for daily_uploads.uploaded_by_name."""
    u = message.from_user
    if not u:
        return "unknown"
    for attr in ("full_name", "username", "first_name"):
        v = getattr(u, attr, None)
        if v:
            return str(v)
    return str(u.id)


def _track_daily_upload(
    upload_type: str,
    message: types.Message,
    file_name: str | None = None,
    row_count: int = 0,
    notes: str | None = None,
    upload_date: str | None = None,
) -> None:
    """Fire-and-forget: record a successful upload into daily_uploads.

    When ``upload_date`` is provided (e.g. a historical snapshot reimport),
    the row is registered under that date instead of today. Any exception is
    logged and swallowed — a tracking failure must never break the main
    upload flow.
    """
    try:
        from backend.services.daily_uploads import record_upload
        user = message.from_user
        record_upload(
            upload_type,
            user_id=user.id if user else None,
            user_name=_sender_display_name(message),
            file_name=file_name,
            row_count=int(row_count or 0),
            notes=notes,
            upload_date=upload_date,
        )
    except Exception as e:
        logger.error(f"daily_uploads tracking failed for {upload_type}: {e}")


_DDMMYYYY_RE = __import__("re").compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")


def _extract_snapshot_date(message: types.Message) -> str | None:
    """Parse a DD/MM/YYYY token from the command caption/text, if present.

    Looks first at the document caption (for file-with-caption uploads),
    then at the reply-to-message text, then at the message text. Returns
    ISO YYYY-MM-DD or None.
    """
    candidates: list[str] = []
    if message.text:
        candidates.append(message.text)
    if message.caption:
        candidates.append(message.caption)
    if message.reply_to_message and message.reply_to_message.caption:
        candidates.append(message.reply_to_message.caption)
    for text in candidates:
        m = _DDMMYYYY_RE.search(text)
        if m:
            try:
                from datetime import date as _date
                return _date(int(m.group(3)), int(m.group(2)),
                             int(m.group(1))).isoformat()
            except (ValueError, TypeError):
                continue
    return None


# ───────────────────────────────────────────
# Public commands
# ───────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    """Send welcome message with Mini App button, or location prompt if deep-linked."""
    # Check for deep link parameter
    args = message.text.split(maxsplit=1)
    deep_link = args[1] if len(args) > 1 else ""

    if deep_link == "share_location":
        # User came from Mini App to share location
        await message.answer(
            "📍 Yetkazib berish manzilini saqlash uchun joylashuvingizni yuboring.\n\n"
            "📎 tugmasini bosing → Joylashuv → yuboring.",
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


@dp.message(Command("link"))
async def cmd_link(message: types.Message):
    """
    Link a user to an existing 1C client.
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

    # Find the user
    user = conn.execute(
        "SELECT telegram_id, phone, first_name, is_approved, client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user:
        await message.reply(f"❌ Telegram ID {telegram_id} topilmadi.")
        conn.close()
        return

    # Try to find the target client: first by phone, then by client_id_1c
    lookup_norm = normalize_phone(lookup)
    target_client = None

    if len(lookup_norm) >= 9:
        # Lookup by phone number
        target_client = conn.execute(
            "SELECT id, client_id_1c, name, phone_normalized FROM allowed_clients "
            "WHERE phone_normalized = ? AND COALESCE(status, 'active') != 'merged' LIMIT 1",
            (lookup_norm,),
        ).fetchone()

    if not target_client:
        # Lookup by client_id_1c name
        target_client = conn.execute(
            "SELECT id, client_id_1c, name, phone_normalized FROM allowed_clients "
            "WHERE client_id_1c = ? AND COALESCE(status, 'active') != 'merged' LIMIT 1",
            (lookup,),
        ).fetchone()

    if not target_client:
        # Try partial match on client_id_1c
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

    # Check if user already has an allowed_clients row
    user_phone_norm = normalize_phone(user["phone"])
    existing_row = conn.execute(
        "SELECT id, client_id_1c FROM allowed_clients WHERE phone_normalized = ? LIMIT 1",
        (user_phone_norm,),
    ).fetchone()

    if existing_row:
        # Update existing row with the correct client_id_1c
        conn.execute(
            "UPDATE allowed_clients SET client_id_1c = ?, matched_telegram_id = ? WHERE id = ?",
            (client_id_1c, telegram_id, existing_row["id"]),
        )
        client_id = existing_row["id"]
    else:
        # Create new allowed_clients row linked to the same client_id_1c
        conn.execute(
            "INSERT INTO allowed_clients (phone_normalized, name, source_sheet, status, client_id_1c, matched_telegram_id) "
            "VALUES (?, ?, 'bot_linked', 'active', ?, ?)",
            (user_phone_norm, user["first_name"], client_id_1c, telegram_id),
        )
        client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Approve and link the user
    conn.execute(
        "UPDATE users SET is_approved = 1, client_id = ? WHERE telegram_id = ?",
        (client_id, telegram_id),
    )
    conn.commit()

    # Persist to backup
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

    # Count all sibling phones for this client
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

        _track_daily_upload(
            "prices",
            message,
            file_name=doc.file_name,
            row_count=int(result.get("excel_products") or 0),
            upload_date=_extract_snapshot_date(message),
        )

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


@dp.message(Command("syncimages"))
async def cmd_syncimages(message: types.Message):
    """Upload product images to the Railway volume.

    Usage:
        Reply to a ZIP file with /syncimages — extracts PNGs named {product_id}.png
        Reply to a single PNG with /syncimages — copies it directly
        /syncimages status — show current image count on the volume

    After upload, re-runs sync_images to update product.image_path in the DB.
    """
    if not is_admin(message):
        return

    import zipfile, tempfile, shutil

    images_dir = Path(os.getenv("IMAGES_DIR", "./images"))
    images_dir.mkdir(parents=True, exist_ok=True)

    parts = (message.text or "").split()
    if len(parts) >= 2 and parts[1].lower() == "status":
        count = sum(1 for f in images_dir.iterdir()
                    if f.suffix.lower() in ('.png', '.jpg', '.jpeg', '.webp'))
        total_mb = sum(f.stat().st_size for f in images_dir.iterdir()
                       if f.suffix.lower() in ('.png', '.jpg', '.jpeg', '.webp')) / 1024 / 1024
        await message.reply(
            f"📸 <b>Volume image status</b>\n\n"
            f"📂 Path: {images_dir}\n"
            f"🖼 Files: {count}\n"
            f"💾 Size: {total_mb:.1f} MB",
            parse_mode="HTML",
        )
        return

    doc = None
    if message.reply_to_message and message.reply_to_message.document:
        doc = message.reply_to_message.document
    elif message.document:
        doc = message.document

    if not doc:
        await message.reply(
            "📸 <b>/syncimages</b>\n\n"
            "Foydalanish:\n"
            "• ZIP fayl bilan javob: <code>/syncimages</code>\n"
            "• Bitta PNG bilan javob: <code>/syncimages</code>\n"
            "• Status: <code>/syncimages status</code>\n\n"
            "ZIP ichidagi {product_id}.png fayllar /data/images/ ga ko'chiriladi.",
            parse_mode="HTML",
        )
        return

    status_msg = await message.reply("⏳ Rasmlar yuklanmoqda...")

    try:
        import httpx
        file_info = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_info.file_path}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        fname = (doc.file_name or "").lower()
        added = 0
        replaced = 0
        skipped = 0

        if fname.endswith(".zip"):
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name
            try:
                with zipfile.ZipFile(tmp_path, "r") as zf:
                    for name in zf.namelist():
                        base = Path(name).name
                        if not base or base.startswith(".") or base.startswith("__"):
                            continue
                        if Path(base).suffix.lower() not in ('.png', '.jpg', '.jpeg', '.webp'):
                            continue
                        stem = Path(base).stem
                        try:
                            int(stem)
                        except ValueError:
                            skipped += 1
                            continue
                        dest = images_dir / base
                        existed = dest.exists()
                        with zf.open(name) as src, open(dest, "wb") as dst:
                            shutil.copyfileobj(src, dst)
                        if existed:
                            replaced += 1
                        else:
                            added += 1
            finally:
                os.unlink(tmp_path)
        elif fname.endswith(('.png', '.jpg', '.jpeg', '.webp')):
            stem = Path(fname).stem
            try:
                int(stem)
            except ValueError:
                await status_msg.edit_text(
                    f"❌ Fayl nomi {fname} product ID emas. "
                    "Fayl nomi {{product_id}}.png bo'lishi kerak."
                )
                return
            dest = images_dir / fname
            existed = dest.exists()
            with open(dest, "wb") as f:
                f.write(file_bytes)
            if existed:
                replaced += 1
            else:
                added += 1
        else:
            await status_msg.edit_text("❌ Faqat ZIP yoki PNG/JPG fayllar qabul qilinadi.")
            return

        from backend.services.sync_images import sync
        sync()

        total = sum(1 for f in images_dir.iterdir()
                    if f.suffix.lower() in ('.png', '.jpg', '.jpeg', '.webp'))
        await status_msg.edit_text(
            f"✅ <b>Rasmlar yuklandi!</b>\n\n"
            f"➕ Yangi: {added}\n"
            f"🔄 Almashtirdi: {replaced}\n"
            f"⏭ O'tkazib yuborildi: {skipped}\n"
            f"📂 Jami volumeda: {total}",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"syncimages error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


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

        _track_daily_upload(
            "stock",
            message,
            file_name=doc.file_name,
            row_count=int(result.get("excel_products") or 0),
            upload_date=_extract_snapshot_date(message),
        )

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

            # Daily checklist tracking — detect 40.10 (UZS) vs 40.11 (USD)
            # from the "sections" list returned by the parser and track each
            # currency as its own upload_type.
            sections = result.get("sections") or []
            tracked_any = False
            for sec in sections:
                cur = (sec.get("currency") or "").upper()
                if cur == "UZS":
                    _track_daily_upload(
                        "balances_uzs", message,
                        file_name=doc.file_name,
                        row_count=int(sec.get("clients") or 0),
                    )
                    tracked_any = True
                elif cur == "USD":
                    _track_daily_upload(
                        "balances_usd", message,
                        file_name=doc.file_name,
                        row_count=int(sec.get("clients") or 0),
                    )
                    tracked_any = True
            if not tracked_any:
                # Fallback: older/degenerate result shape without sections —
                # default to UZS so we at least record something.
                _track_daily_upload(
                    "balances_uzs", message,
                    file_name=doc.file_name,
                    row_count=int(result.get("total_clients_in_file") or 0),
                )

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


def _is_agent_or_admin(message: types.Message) -> bool:
    """Allow from admin group, agents group, or listed admin IDs."""
    if is_admin(message):
        return True
    if message.chat.id == AGENTS_GROUP_CHAT_ID:
        return True
    return False


@dp.message(Command("testclient"))
async def cmd_testclient(message: types.Message):
    """Link admin's account to a 1C client for testing the Cabinet balance view.

    Usage:
        /testclient              — show current link + top clients to choose from
        /testclient КЛИЕНТ       — search by name and link to first match
        /testclient #123         — link to allowed_clients.id directly
        /testclient clear        — remove the test link
    """
    if not _is_agent_or_admin(message):
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
               WHERE (LOWER(ac.client_id_1c) LIKE ? OR LOWER(ac.name) LIKE ?
                  OR ac.id IN (
                      SELECT DISTINCT client_id FROM client_balances
                      WHERE LOWER(client_name_1c) LIKE ? AND client_id IS NOT NULL
                  ))
                 AND COALESCE(ac.status, 'active') != 'merged'
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
            # Group results by client_id_1c so multi-phone clients show as one entry
            from collections import OrderedDict
            _grouped = OrderedDict()
            for m in matches:
                cid = (m['client_id_1c'] or '').strip()
                key = cid if cid else f"__no1c_{m['id']}"
                _grouped.setdefault(key, []).append(m)

            unique_clients = len(_grouped)
            lines.append(f"🔍 <b>{unique_clients}</b> ta mijoz '{html_escape(arg)}' bo'yicha:\n")
            lines.append("<b>📋 Ro'yxatdagi mijozlar:</b>")

            for key, group in _grouped.items():
                # Sum up balance months across all sibling records
                total_bal = sum(m['bal_count'] for m in group)
                first = group[0]
                name_1c = html_escape(first['client_id_1c'] or '—')

                if len(group) == 1:
                    # Single record — show as before
                    m = first
                    name_app = html_escape(m['name'] or '—')
                    if m['client_id_1c'] and m['name'] and m['client_id_1c'].strip() != m['name'].strip():
                        display = f"{name_1c}\n      📱 {name_app}"
                    else:
                        display = name_1c if m['client_id_1c'] else name_app
                    lines.append(
                        f"  <code>/testclient #{m['id']}</code> — {display}"
                        f"  [{total_bal} oy]"
                    )
                else:
                    # Multi-phone client — show grouped with phone sub-entries
                    lines.append(f"  📍 <b>{name_1c}</b>  [{total_bal} oy]  ({len(group)} tel.)")
                    for m in group:
                        name_app = html_escape(m['name'] or '—')
                        bal_info = f" ({m['bal_count']} oy)" if m['bal_count'] > 0 else ""
                        lines.append(
                            f"      <code>/testclient #{m['id']}</code> — {name_app}{bal_info}"
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

        # Snapshot date = the report_date the importer parsed out of the XLS
        # title ("Дебиторская задолженность на 2 Апреля 2026 г.").
        # This keeps the historical checklist accurate when past-dated files
        # are re-uploaded.
        snapshot_date = result.get("report_date") or _extract_snapshot_date(message)
        _track_daily_upload(
            "debtors",
            message,
            file_name=doc.file_name,
            row_count=int(result.get("total_clients") or 0),
            upload_date=snapshot_date,
        )

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
# /realordersstats — real_orders data quality diagnostic
# ───────────────────────────────────────────

@dp.message(Command("realordersstats"))
async def cmd_realordersstats(message: types.Message):
    """Data-quality diagnostic for real_orders.

    Reports coverage, match rates, sale_agent fill rate, monthly breakdown,
    and (most importantly) the distribution of wish-list → real-order time
    gaps — so we can pick a data-driven N-days conversion window for the
    upcoming dashboard Phase 3 views.
    """
    if not is_admin(message):
        return

    conn = get_db()
    try:
        # ── Section 1: Basic counts & coverage ────────────────────
        total_orders = conn.execute("SELECT COUNT(*) FROM real_orders").fetchone()[0]
        if total_orders == 0:
            await message.reply(
                "❌ <b>real_orders jadvali bo'sh.</b>\n\n"
                "Avval /realorders buyrug'i bilan 1C 'Реализация товаров' "
                "faylini yuklang.",
                parse_mode="HTML",
            )
            return

        total_items = conn.execute("SELECT COUNT(*) FROM real_order_items").fetchone()[0]

        date_range = conn.execute(
            "SELECT MIN(doc_date), MAX(doc_date) FROM real_orders"
        ).fetchone()
        first_date, last_date = date_range[0], date_range[1]

        per_currency = conn.execute(
            """SELECT currency,
                      COUNT(*) as orders,
                      SUM(total_sum) as total_local,
                      SUM(total_sum_currency) as total_curr
               FROM real_orders
               GROUP BY currency"""
        ).fetchall()

        # ── Section 2: Match rates ────────────────────────────────
        orders_with_client = conn.execute(
            "SELECT COUNT(*) FROM real_orders WHERE client_id IS NOT NULL"
        ).fetchone()[0]
        orders_with_agent = conn.execute(
            "SELECT COUNT(*) FROM real_orders WHERE sale_agent IS NOT NULL AND sale_agent != ''"
        ).fetchone()[0]
        items_with_product = conn.execute(
            "SELECT COUNT(*) FROM real_order_items WHERE product_id IS NOT NULL"
        ).fetchone()[0]

        def pct(n, d):
            return f"{100 * n / d:.1f}%" if d else "—"

        # ── Section 3: Sales agent breakdown ──────────────────────
        agents = conn.execute(
            """SELECT sale_agent,
                      COUNT(*) as orders,
                      SUM(CASE WHEN currency = 'UZS' THEN total_sum_currency ELSE 0 END) as uzs,
                      SUM(CASE WHEN currency = 'USD' THEN total_sum_currency ELSE 0 END) as usd
               FROM real_orders
               WHERE sale_agent IS NOT NULL AND sale_agent != ''
               GROUP BY sale_agent
               ORDER BY orders DESC
               LIMIT 10"""
        ).fetchall()

        # ── Section 4: Monthly coverage ───────────────────────────
        monthly = conn.execute(
            """SELECT substr(doc_date, 1, 7) as ym,
                      COUNT(*) as orders,
                      COUNT(DISTINCT client_id) as clients
               FROM real_orders
               GROUP BY ym
               ORDER BY ym"""
        ).fetchall()

        # ── Section 5: Wish-list → real-order gap distribution ────
        # For each wish-list order (orders table), find the nearest subsequent
        # real_order from the SAME client (via users.client_id bridge) and
        # compute the day gap. Unmatched wish-lists are counted separately.
        gap_rows = conn.execute(
            """WITH wish AS (
                   SELECT o.id as wish_id,
                          u.client_id,
                          date(o.created_at) as wish_date
                   FROM orders o
                   JOIN users u ON u.telegram_id = o.telegram_id
                   WHERE u.client_id IS NOT NULL
               )
               SELECT w.wish_id,
                      (SELECT MIN(julianday(ro.doc_date) - julianday(w.wish_date))
                       FROM real_orders ro
                       WHERE ro.client_id = w.client_id
                         AND date(ro.doc_date) >= w.wish_date
                         AND julianday(ro.doc_date) - julianday(w.wish_date) <= 90
                      ) as gap_days
               FROM wish w"""
        ).fetchall()

        buckets = {
            "0-1": 0, "2-3": 0, "4-7": 0,
            "8-14": 0, "15-30": 0, "31-60": 0,
            "61-90": 0, "none": 0,
        }
        for r in gap_rows:
            g = r["gap_days"]
            if g is None:
                buckets["none"] += 1
            elif g <= 1:
                buckets["0-1"] += 1
            elif g <= 3:
                buckets["2-3"] += 1
            elif g <= 7:
                buckets["4-7"] += 1
            elif g <= 14:
                buckets["8-14"] += 1
            elif g <= 30:
                buckets["15-30"] += 1
            elif g <= 60:
                buckets["31-60"] += 1
            else:
                buckets["61-90"] += 1

        total_wish = len(gap_rows)

        # Cumulative conversion at common cutoffs (for N-days discussion)
        def cum_by(days):
            hit = 0
            for r in gap_rows:
                g = r["gap_days"]
                if g is not None and g <= days:
                    hit += 1
            return hit

        cum7 = cum_by(7)
        cum14 = cum_by(14)
        cum30 = cum_by(30)

        # ── Build reply ───────────────────────────────────────────
        lines = ["📊 <b>Real Orders — Data Quality Diagnostic</b>\n"]

        lines.append(f"<b>Qamrov:</b>")
        lines.append(f"  📦 {total_orders:,} ta hujjat, {total_items:,} ta qator")
        lines.append(f"  📅 {_h(str(first_date))} → {_h(str(last_date))}")
        for p in per_currency:
            curr = _h(str(p["currency"] or "—"))
            n = p["orders"]
            sym = "💴" if curr == "UZS" else "💵"
            tot = p["total_curr"] or 0
            if curr == "UZS":
                fmt_tot = f"{round(tot / 1e9, 2)}B"
            else:
                fmt_tot = f"${round(tot / 1e3, 1)}K"
            lines.append(f"  {sym} {curr}: {n:,} hujjat, {fmt_tot}")

        lines.append(f"\n<b>Bog'lanish sifati:</b>")
        lines.append(
            f"  👥 Mijoz bog'langan: {orders_with_client:,}/{total_orders:,} ({pct(orders_with_client, total_orders)})"
        )
        lines.append(
            f"  📦 Mahsulot bog'langan: {items_with_product:,}/{total_items:,} ({pct(items_with_product, total_items)})"
        )
        lines.append(
            f"  🧑‍💼 Sales agent to'ldirilgan: {orders_with_agent:,}/{total_orders:,} ({pct(orders_with_agent, total_orders)})"
        )

        if agents:
            lines.append(f"\n<b>Top sales agentlar:</b>")
            for a in agents[:8]:
                name = _h((a["sale_agent"] or "—")[:25])
                tot_uzs = a["uzs"] or 0
                tot_usd = a["usd"] or 0
                extras = []
                if tot_uzs:
                    extras.append(f"{round(tot_uzs / 1e9, 1)}B UZS")
                if tot_usd:
                    extras.append(f"${round(tot_usd / 1e3, 1)}K")
                extras_str = " · ".join(extras) if extras else "—"
                lines.append(f"  • {name}: {a['orders']} hujjat · {extras_str}")

        if monthly:
            lines.append(f"\n<b>Oylik taqsimot:</b>")
            for m in monthly[-12:]:  # last 12 months
                ym = _h(str(m['ym'] or '—'))
                lines.append(f"  {ym} — {m['orders']:,} hujjat, {m['clients']} mijoz")

        if total_wish > 0:
            lines.append(f"\n<b>Wish-list → Real Order gap:</b>")
            lines.append(f"  Jami wish-list (mijozga bog'langan): {total_wish:,}")
            lines.append("")
            lines.append(f"  <b>Bucket taqsimoti:</b>")
            for label in ["0-1", "2-3", "4-7", "8-14", "15-30", "31-60", "61-90", "none"]:
                count = buckets[label]
                if total_wish > 0:
                    bar_len = int(20 * count / total_wish)
                    bar = "▓" * bar_len + "░" * (20 - bar_len)
                else:
                    bar = "░" * 20
                label_padded = f"{label:>6} kun" if label != "none" else f"{'none':>6}    "
                lines.append(f"  {label_padded} {bar} {count} ({pct(count, total_wish)})")

            lines.append("")
            lines.append(f"  <b>Kumulyativ konversiya (N-days window tahlili):</b>")
            lines.append(f"  ≤ 7 kun:  {cum7:,} ({pct(cum7, total_wish)})")
            lines.append(f"  ≤14 kun:  {cum14:,} ({pct(cum14, total_wish)})")
            lines.append(f"  ≤30 kun:  {cum30:,} ({pct(cum30, total_wish)})")
        else:
            lines.append(f"\n⚠️ Wish-list ↔ real-order bog'lanishi topilmadi — users.client_id orqali bridging ishlamayapti.")

        # Telegram has a 4096-char limit on messages. Split if needed.
        text = "\n".join(lines)
        if len(text) <= 4000:
            await message.reply(text, parse_mode="HTML")
        else:
            # Split on section boundaries
            chunks = []
            current = []
            current_len = 0
            for line in lines:
                if current_len + len(line) + 1 > 3800:
                    chunks.append("\n".join(current))
                    current = [line]
                    current_len = len(line)
                else:
                    current.append(line)
                    current_len += len(line) + 1
            if current:
                chunks.append("\n".join(current))
            for i, chunk in enumerate(chunks):
                await message.reply(chunk, parse_mode="HTML")

    except Exception as e:
        logger.error(f"realordersstats error: {e}", exc_info=True)
        await message.reply(f"❌ Xatolik: {str(e)[:300]}")
    finally:
        conn.close()


# ───────────────────────────────────────────
# /wipewishlists — destructive cleanup of demo wish-list data
# ───────────────────────────────────────────

@dp.message(Command("wipewishlists"))
async def cmd_wipewishlists(message: types.Message):
    """Wipe all wish-list data (orders, order_items, product_requests,
    demand_signals, cart_items) before the real app launch.

    Safety: without the CONFIRM keyword, only reports counts (dry-run).
    With /wipewishlists CONFIRM, performs a transactional delete and
    writes a logger entry.
    """
    if not is_admin(message):
        return

    # Parse the confirmation token
    parts = (message.text or "").strip().split(maxsplit=1)
    token = parts[1].strip().upper() if len(parts) > 1 else ""
    is_confirmed = (token == "CONFIRM")

    conn = get_db()
    try:
        # Collect pre-wipe counts
        counts = {
            "orders": conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
            "order_items": conn.execute("SELECT COUNT(*) FROM order_items").fetchone()[0],
            "product_requests": conn.execute("SELECT COUNT(*) FROM product_requests").fetchone()[0],
            "demand_signals": conn.execute("SELECT COUNT(*) FROM demand_signals").fetchone()[0],
            "cart_items": conn.execute("SELECT COUNT(*) FROM cart_items").fetchone()[0],
        }
        total = sum(counts.values())

        if total == 0:
            await message.reply(
                "✅ <b>Wish-list jadvallari allaqachon bo'sh.</b>\n\n"
                "Hech qanday o'chirish kerak emas.",
                parse_mode="HTML",
            )
            return

        if not is_confirmed:
            # Dry run: show what WOULD be deleted
            lines = [
                "⚠️ <b>Wish-list ma'lumotlarini tozalash (DRY-RUN)</b>\n",
                "Agar /wipewishlists CONFIRM buyrug'ini yuborsangiz, "
                "quyidagi yozuvlar <b>butunlay o'chiriladi</b>:\n",
                f"  🛒 orders: <b>{counts['orders']:,}</b>",
                f"  📦 order_items: <b>{counts['order_items']:,}</b>",
                f"  📝 product_requests: <b>{counts['product_requests']:,}</b>",
                f"  📡 demand_signals: <b>{counts['demand_signals']:,}</b>",
                f"  🧺 cart_items: <b>{counts['cart_items']:,}</b>",
                "",
                f"  <b>Jami:</b> {total:,} yozuv",
                "",
                "⚠️ Bu amal <b>qaytarib bo'lmaydi</b>.",
                "Real ma'lumotlarga ta'sir qilmaydi (real_orders, "
                "real_order_items, client_balances — tegilmaydi).",
                "",
                "Tasdiqlash uchun: <code>/wipewishlists CONFIRM</code>",
            ]
            await message.reply("\n".join(lines), parse_mode="HTML")
            return

        # CONFIRMED: perform the wipe in a single transaction
        logger.warning(
            f"wipewishlists CONFIRMED by user={message.from_user.id} "
            f"deleting: {counts}"
        )
        try:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM demand_signals")
            conn.execute("DELETE FROM order_items")
            conn.execute("DELETE FROM orders")
            conn.execute("DELETE FROM product_requests")
            conn.execute("DELETE FROM cart_items")
            # Reset autoincrement counters so future IDs start fresh
            conn.execute(
                "DELETE FROM sqlite_sequence WHERE name IN "
                "('orders','order_items','product_requests','demand_signals')"
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        # Verify post-wipe state
        post = {
            "orders": conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
            "order_items": conn.execute("SELECT COUNT(*) FROM order_items").fetchone()[0],
            "product_requests": conn.execute("SELECT COUNT(*) FROM product_requests").fetchone()[0],
            "demand_signals": conn.execute("SELECT COUNT(*) FROM demand_signals").fetchone()[0],
            "cart_items": conn.execute("SELECT COUNT(*) FROM cart_items").fetchone()[0],
        }
        post_total = sum(post.values())

        lines = [
            "✅ <b>Wish-list ma'lumotlari tozalandi</b>\n",
            "O'chirildi:",
            f"  🛒 orders: {counts['orders']:,}",
            f"  📦 order_items: {counts['order_items']:,}",
            f"  📝 product_requests: {counts['product_requests']:,}",
            f"  📡 demand_signals: {counts['demand_signals']:,}",
            f"  🧺 cart_items: {counts['cart_items']:,}",
            "",
            f"  <b>Jami:</b> {total:,} yozuv o'chirildi",
            "",
            f"Qolgan: {post_total} yozuv "
            + ("✅" if post_total == 0 else "⚠️ (kutilmagan)"),
            "",
            "ℹ️ real_orders va client_balances jadvallariga "
            "hech qanday ta'sir qilmadi.",
        ]
        await message.reply("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"wipewishlists error: {e}", exc_info=True)
        await message.reply(f"❌ Xatolik: {_h(str(e)[:300])}")
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

        _track_daily_upload(
            "realorders",
            message,
            file_name=doc.file_name,
            row_count=int(result.get("inserted_docs") or 0) + int(result.get("updated_docs") or 0),
        )

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


@dp.message(F.document & F.chat.id == ORDER_GROUP_CHAT_ID & F.reply_to_message)
async def handle_order_confirmation_reply(message: types.Message):
    """Sotuv bo'lim group: a manager replies to a "Yangi buyurtma #N" message
    (or its attached Excel) with the 1C-exported Excel of the finalized order.

    The bot finds the matching wishlist order via stored sales-group message
    ids, parses the Excel, stores it as a confirmed_orders row linked to the
    wishlist order, and notifies the client in DM.
    """
    doc = message.document
    fname = (doc.file_name or "").lower()
    if not (fname.endswith(".xls") or fname.endswith(".xlsx")):
        return

    replied_mid = message.reply_to_message.message_id
    conn = get_db()
    try:
        row = conn.execute(
            """SELECT id, telegram_id, client_name, client_name AS cname
               FROM orders
               WHERE sales_group_message_id = ? OR sales_group_doc_message_id = ?
               LIMIT 1""",
            (replied_mid, replied_mid),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        # Reply not tied to a known order — ignore silently so /realorders
        # and other document replies keep working.
        return

    wishlist_order_id = row["id"]
    client_tg_id = row["telegram_id"]

    status_msg = await message.reply("⏳ Tasdiqlangan buyurtma yuklanmoqda...")
    try:
        import httpx
        from backend.services.import_real_orders import parse_real_orders_xls
        from backend.database import get_db as _get_db

        file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        parsed = parse_real_orders_xls(file_bytes, filename_hint=doc.file_name or "")
        if not parsed.get("ok") or not parsed.get("documents"):
            await status_msg.edit_text(
                "❌ Fayl o'qib bo'lmadi. /realorders formatidagi xls kutilayotgan edi."
            )
            return
        docs = parsed["documents"]
        first = docs[0]

        items = first.get("items") or []
        total_uzs = sum(float(it.get("total_local") or 0) for it in items)
        total_usd = sum(float(it.get("total_currency") or 0) for it in items)

        import json as _json
        items_payload = [
            {
                "name": it.get("product_name_1c") or "",
                "qty": float(it.get("quantity") or 0),
                "price_uzs": float(it.get("price") or 0) if not it.get("price_currency") else 0,
                "price_usd": float(it.get("price_currency") or 0),
                "total_uzs": float(it.get("total_local") or 0),
                "total_usd": float(it.get("total_currency") or 0),
            }
            for it in items
        ]
        uploader = _sender_display_name(message)
        uploader_id = message.from_user.id if message.from_user else None

        conn2 = _get_db()
        try:
            conn2.execute(
                """INSERT INTO confirmed_orders
                   (wishlist_order_id, file_name, telegram_file_id,
                    confirmed_by_tg_id, confirmed_by_name,
                    total_uzs, total_usd, item_count, items_json,
                    doc_number_1c, doc_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    wishlist_order_id,
                    doc.file_name, doc.file_id,
                    uploader_id, uploader,
                    total_uzs, total_usd, len(items),
                    _json.dumps(items_payload, ensure_ascii=False),
                    first.get("doc_number_1c"), first.get("doc_date"),
                ),
            )
            conn2.execute(
                "UPDATE orders SET status = 'confirmed' WHERE id = ?",
                (wishlist_order_id,),
            )
            conn2.commit()
        finally:
            conn2.close()

        # Notify the client
        try:
            if client_tg_id:
                import httpx as _httpx
                _httpx.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": client_tg_id,
                        "text": (
                            "✅ <b>Buyurtmangiz tayyor!</b>\n\n"
                            "Boshqaruvchilar buyurtmangizni 1C ga kiritdilar. "
                            "Qanaqa farqlar borligini ilovadagi 'Kabinet' bo'limida ko'ring."
                        ),
                        "parse_mode": "HTML",
                    },
                    timeout=10,
                )
        except Exception as e:
            logger.error(f"Failed to notify client about confirmed order: {e}")

        await status_msg.edit_text(
            f"✅ Tasdiqlangan buyurtma saqlandi (#{wishlist_order_id}).\n"
            f"📦 {len(items)} ta tovar · UZS {total_uzs:,.0f} · USD {total_usd:,.2f}\n"
            f"🧾 Mijozga ilovaga xabar yuborildi."
        )
    except Exception as e:
        logger.error(f"Order confirmation reply failed: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:200]}")


# ─────────────────────────────────────────────────────────────────
# Session F renewal: Daily Upload Checklist — new commands
# /cash, /fxrate, /today, /skipupload, /holiday, /backfilldailyuploads
# ─────────────────────────────────────────────────────────────────


@dp.message(Command("cash"))
async def cmd_cash(message: types.Message):
    """Import Касса (cash receipts) from 1C.

    Two uploads per day are required (morning + evening). Re-uploading the
    same file is safe — idempotent on doc_number_1c.
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
            "1C'dan Касса (Приходный кассовый ордер) XLS faylni\n"
            "/cash caption bilan yuboring.\n\n"
            "Yoki faylga javob sifatida /cash yozing.\n\n"
            "💡 Har kuni 2 marta: ertalab va kechqurun.",
            parse_mode="HTML",
        )
        return

    if not doc.file_name or not doc.file_name.lower().endswith(('.xls', '.xlsx')):
        await message.reply("❌ Faqat Excel (.xls/.xlsx) fayllar qabul qilinadi.")
        return

    status_msg = await message.reply("⏳ Касса yuklanmoqda...")

    try:
        import httpx

        file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        api_url = f"{_BASE_URL}/api/finance/import-cash"
        async with httpx.AsyncClient(timeout=300) as client:
            resp = await client.post(
                api_url,
                files={"file": (doc.file_name, file_bytes, "application/vnd.ms-excel")},
                data={"admin_key": "rassvet2026"},
            )
            result = resp.json()

        if not result.get("ok"):
            err = result.get("error", "Unknown")
            diag = result.get("diagnostics")
            msg_lines = [f"❌ Xatolik: {err}"]
            if diag:
                msg_lines.append("<pre>")
                for i, row in enumerate(diag[:15]):
                    non_empty = [c for c in row if c]
                    if non_empty:
                        msg_lines.append(html_escape(f"r{i:02d}: " + " | ".join(non_empty[:8])[:180]))
                msg_lines.append("</pre>")
            await status_msg.edit_text("\n".join(msg_lines)[:3900], parse_mode="HTML")
            return

        _track_daily_upload(
            "cash",
            message,
            file_name=doc.file_name,
            row_count=int(result.get("inserted") or 0) + int(result.get("updated") or 0),
        )

        st = result.get("stats", {})
        inserted = result.get("inserted", 0)
        updated = result.get("updated", 0)
        date_min = st.get("date_min") or "?"
        date_max = st.get("date_max") or "?"
        date_range = date_min if date_min == date_max else f"{date_min} — {date_max}"

        lines = [
            "✅ <b>Касса yuklandi!</b>\n",
            f"📅 Davr: {date_range}",
            f"📄 Jami qatorlar: {st.get('row_count', 0)}",
            f"🆕 Yangi: {inserted} · 🔄 Yangilangan (dublikat): {updated}",
            f"👥 Mijozlar: {st.get('client_count', 0)} (bog'langan: {result.get('matched_clients', 0)})",
        ]
        if st.get("total_uzs"):
            lines.append(f"\n💴 UZS jami: {round(st['total_uzs']):,}".replace(",", " "))
        if st.get("total_usd"):
            lines.append(f"💵 USD jami: ${st['total_usd']:,.2f}")
        lines.append(f"\n💾 DBda jami: {result.get('db_total', 0)} qator")

        # Show today's checklist status for cash
        from backend.services.daily_uploads import get_checklist
        ck = get_checklist()
        cash_item = next((i for i in ck["items"] if i["upload_type"] == "cash"), None)
        if cash_item:
            actual = cash_item.get("actual_count", 0)
            # Reminder target (ritual) may exceed the checklist target.
            reminder = cash_item.get("reminder_count_per_day") or \
                       cash_item.get("expected_count_per_day", 1)
            if actual < reminder:
                lines.append(
                    f"\n📋 Bugungi касса: {actual}/{reminder} "
                    f"(kechqurun fayl ham kerak)"
                )
            else:
                lines.append(f"\n📋 Bugungi касса: ✅ {actual}/{reminder} tugatildi")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Cash import error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


@dp.message(F.document & F.caption.startswith("/cash"))
async def handle_cash_document(message: types.Message):
    """Handle XLS/XLSX file sent with /cash as caption."""
    if not is_admin(message):
        return
    await cmd_cash(message)


@dp.message(Command("fxrate"))
async def cmd_fxrate(message: types.Message):
    """Set USD/UZS rate.

    Usage:
        /fxrate 12650                  → bugungi kurs
        /fxrate 01/04/2026 11230       → o'tgan sana (DD/MM/YYYY)
    """
    if not is_admin(message):
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        from backend.services.daily_uploads import get_latest_fx_rate
        latest = get_latest_fx_rate()
        if latest:
            await message.reply(
                f"📈 <b>Oxirgi USD/UZS kursi</b>\n\n"
                f"Sana: {latest['rate_date']}\n"
                f"Kurs: <b>{latest['rate']:,.2f}</b> UZS/USD\n"
                f"Kiritgan: {html_escape(latest.get('uploaded_by_name') or '—')}\n\n"
                f"💡 Bugungi kurs: <code>/fxrate 12650</code>\n"
                f"💡 O'tgan sana: <code>/fxrate 01/04/2026 11230</code>",
                parse_mode="HTML",
            )
        else:
            await message.reply(
                "❌ Kurs hali kiritilmagan.\n\n"
                "💡 Foydalanish:\n"
                "<code>/fxrate 12650</code>\n"
                "<code>/fxrate 01/04/2026 11230</code>",
                parse_mode="HTML",
            )
        return

    # Optional DD/MM/YYYY date as first arg
    rate_date = None
    rate_token = parts[1]
    if "/" in parts[1] and len(parts) >= 3:
        try:
            dd, mm, yyyy = parts[1].split("/")
            from datetime import date as _date
            rate_date = _date(int(yyyy), int(mm), int(dd)).isoformat()
        except (ValueError, TypeError):
            await message.reply(
                "❌ Sana formati noto'g'ri. DD/MM/YYYY bo'lishi kerak.\n"
                "Masalan: <code>/fxrate 01/04/2026 11230</code>",
                parse_mode="HTML",
            )
            return
        rate_token = parts[2]

    try:
        rate = float(rate_token.replace(",", ".").replace(" ", ""))
    except ValueError:
        await message.reply(
            "❌ Kurs raqam bo'lishi kerak.\n"
            "Masalan: <code>/fxrate 12650</code> yoki "
            "<code>/fxrate 01/04/2026 11230</code>",
            parse_mode="HTML",
        )
        return

    if rate < 5000 or rate > 20000:
        await message.reply(
            f"❌ Kurs {rate:g} haqiqiy emas.\n"
            "USD/UZS kursi 5000 dan 20000 gacha bo'lishi kerak."
        )
        return

    try:
        from backend.services.daily_uploads import set_fx_rate, tashkent_today_str
        user_name = _sender_display_name(message)
        user_id = message.from_user.id if message.from_user else None
        result = set_fx_rate(
            rate,
            user_id=user_id,
            user_name=user_name,
            rate_date=rate_date,
        )
        shown_date = result.get("rate_date") or tashkent_today_str()

        await message.reply(
            f"✅ <b>FX rate saqlandi</b>\n\n"
            f"📅 Sana: {shown_date}\n"
            f"💱 Kurs: <b>{rate:,.2f}</b> UZS/USD\n"
            f"👤 Kiritgan: {html_escape(user_name)}",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"fxrate error: {e}")
        await message.reply(f"❌ Xatolik: {str(e)[:200]}")


@dp.message(Command("today"))
async def cmd_today(message: types.Message):
    """Show today's daily upload checklist (8 items)."""
    try:
        from backend.services.daily_uploads import get_checklist, render_checklist_text
        ck = get_checklist()
        text = render_checklist_text(ck)
        await message.reply(f"<pre>{html_escape(text)}</pre>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"/today error: {e}")
        await message.reply(f"❌ Xatolik: {str(e)[:200]}")


@dp.message(Command("missing"))
async def cmd_missing(message: types.Message):
    """Show backward-looking gaps: which files are missing from which days.

    Usage:
        /missing                         → 1st of current month through yesterday
        /missing 2026-04-01              → from that date through yesterday
        /missing 2026-04-01 2026-04-08   → specific range
        /missing 01.04.26 08.04.26       → DD.MM.YY format (1C style)
    """
    if not is_admin(message):
        return
    try:
        from backend.services.daily_uploads import (
            get_missing_gaps,
            render_missing_text,
            _parse_user_date,
        )

        args = (message.text or "").split()[1:]  # strip "/missing"
        start_date = None
        end_date = None

        if len(args) >= 1:
            start_date = _parse_user_date(args[0])
            if start_date is None:
                await message.reply(
                    "❌ Формат даты не распознан. Используйте YYYY-MM-DD "
                    "или DD.MM.YY\n\n"
                    "Примеры:\n"
                    "<code>/missing 2026-04-01</code>\n"
                    "<code>/missing 01.04.26 08.04.26</code>",
                    parse_mode="HTML",
                )
                return
        if len(args) >= 2:
            end_date = _parse_user_date(args[1])
            if end_date is None:
                await message.reply(
                    "❌ Вторая дата не распознана. Используйте YYYY-MM-DD "
                    "или DD.MM.YY",
                    parse_mode="HTML",
                )
                return

        report = get_missing_gaps(start_date=start_date, end_date=end_date)

        if not report.get("ok"):
            await message.reply(
                "Укажите диапазон в прошлом. Формат:\n"
                "<code>/missing</code>\n"
                "<code>/missing 2026-04-01</code>\n"
                "<code>/missing 2026-04-01 2026-04-08</code>",
                parse_mode="HTML",
            )
            return

        text = render_missing_text(report)
        await message.reply(f"<pre>{html_escape(text)}</pre>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"/missing error: {e}")
        await message.reply(f"❌ Xatolik: {str(e)[:200]}")


@dp.message(Command("skipupload"))
async def cmd_skipupload(message: types.Message):
    """Mark a specific upload (or all for a date) as skipped.

    Usage:
        /skipupload <type> <date> <reason>
        /skipupload all <date> <reason>

    <type>: balances_uzs | balances_usd | stock | prices | debtors | realorders | cash | fxrate | all
    <date>: YYYY-MM-DD or "today"
    """
    if not is_admin(message):
        return

    parts = (message.text or "").split(maxsplit=3)
    if len(parts) < 4:
        await message.reply(
            "❌ <b>Foydalanish:</b>\n"
            "<code>/skipupload &lt;type&gt; &lt;date&gt; &lt;reason&gt;</code>\n\n"
            "Turlar: balances_uzs, balances_usd, stock, prices, debtors, realorders, cash, fxrate, all\n"
            "Sana: YYYY-MM-DD yoki <b>today</b>\n\n"
            "Masalan:\n"
            "<code>/skipupload cash today 1C offline</code>\n"
            "<code>/skipupload all 2026-04-05 power outage</code>",
            parse_mode="HTML",
        )
        return

    upload_type = parts[1].lower()
    date_arg = parts[2]
    reason = parts[3]

    from backend.services.daily_uploads import (
        skip_upload, skip_all_uploads, tashkent_today_str, VALID_UPLOAD_TYPES,
    )

    if date_arg.lower() == "today":
        target_date = tashkent_today_str()
    else:
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_arg):
            await message.reply("❌ Sana formati: YYYY-MM-DD yoki 'today'")
            return
        target_date = date_arg

    try:
        if upload_type == "all":
            n = skip_all_uploads(target_date, reason)
            await message.reply(
                f"✅ {n} ta upload turi {target_date} uchun skip qilindi.\n"
                f"Sabab: {html_escape(reason)}",
                parse_mode="HTML",
            )
        elif upload_type in VALID_UPLOAD_TYPES:
            skip_upload(upload_type, target_date, reason)
            await message.reply(
                f"✅ <b>{upload_type}</b> {target_date} uchun skip qilindi.\n"
                f"Sabab: {html_escape(reason)}",
                parse_mode="HTML",
            )
        else:
            await message.reply(
                f"❌ Noma'lum tur: {upload_type}\n"
                f"Ruxsat etilganlar: {', '.join(sorted(VALID_UPLOAD_TYPES))} yoki 'all'"
            )
    except Exception as e:
        logger.error(f"/skipupload error: {e}")
        await message.reply(f"❌ Xatolik: {str(e)[:200]}")


@dp.message(Command("holiday"))
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


@dp.message(Command("backfilldailyuploads"))
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


@dp.message(Command("supply"))
async def cmd_supply(message: types.Message):
    """Import 1C Поступление товаров (supply receipts + returns) from XLS.

    Classifies each document as supply / return / adjustment based on
    the Контрагент field. Idempotent on (doc_number, doc_date).
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
            "📎 XLS faylni yuboring va /supply bilan javob bering.\n"
            "Yoki faylni /supply caption bilan yuboring."
        )
        return

    fname = doc.file_name or "supply.xls"
    if not fname.lower().endswith((".xls", ".xlsx")):
        await message.reply("❌ Faqat .xls yoki .xlsx fayllar qabul qilinadi")
        return

    status_msg = await message.reply(f"⏳ {fname} tahlil qilinmoqda...")
    try:
        import aiohttp
        tg_file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{bot.token}/{tg_file.file_path}"
        async with aiohttp.ClientSession() as sess:
            async with sess.get(file_url) as resp:
                file_bytes = await resp.read()

        api_url = f"{_BASE_URL}/api/finance/import-supply"
        form = aiohttp.FormData()
        form.add_field("file", file_bytes, filename=fname)
        form.add_field("admin_key", "rassvet2026")

        async with aiohttp.ClientSession() as sess:
            async with sess.post(api_url, data=form) as resp:
                result = await resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ Xatolik: {result.get('error', 'unknown')}")
            return

        s = result.get("stats", {})
        ins = result.get("inserted_docs", 0)
        upd = result.get("updated_docs", 0)
        total_items = result.get("total_items", 0)
        matched = result.get("matched_products", 0)
        unmatched_count = result.get("unmatched_products_count", 0)
        match_rate = (matched / total_items * 100) if total_items else 0

        supply_n = s.get("supply_count", 0)
        return_n = s.get("return_count", 0)
        adj_n = s.get("adjustment_count", 0)

        doc_parts = []
        if supply_n:
            doc_parts.append(f"{supply_n} поступлений")
        if return_n:
            doc_parts.append(f"{return_n} возврат{'ов' if return_n > 1 else ''}")
        if adj_n:
            doc_parts.append(f"{adj_n} исправлени{'й' if adj_n > 1 else 'е'}")
        doc_breakdown = " + ".join(doc_parts) if doc_parts else "0"

        warehouses = s.get("warehouses", {})
        wh_parts = [f"{k} ({v})" for k, v in sorted(warehouses.items(), key=lambda x: -x[1])]
        wh_text = ", ".join(wh_parts) if wh_parts else "—"

        cur_counts = s.get("currency_counts", {})
        cur_parts = [f"{k} ({v} док.)" for k, v in sorted(cur_counts.items())]
        cur_text = " / ".join(cur_parts) if cur_parts else "—"

        lines = [
            f"✅ <b>Загружено: {fname}</b>\n",
            f"📄 Документов: {ins + upd} ({doc_breakdown})",
        ]
        if upd:
            lines.append(f"   🆕 новых: {ins}, ♻️ обновлено: {upd}")
        lines.extend([
            f"📦 Товарных строк: {total_items}",
            f"🔗 Товары сопоставлены: {matched}/{total_items} ({match_rate:.1f}%)",
        ])
        if unmatched_count:
            lines.append(f"❓ Несопоставленные товары: {unmatched_count}")
        lines.extend([
            f"\n🏭 Поставщики: {s.get('unique_counterparties', 0)} уникальных",
            f"🏢 Склады: {wh_text}",
            f"💱 Валюта: {cur_text}",
        ])

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

        # Track in daily uploads
        _track_daily_upload(
            "supply", message, file_name=fname,
            row_count=ins + upd,
            notes=f"supply={supply_n} return={return_n} adj={adj_n} items={total_items}",
        )
    except Exception as e:
        logger.error(f"/supply error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


@dp.message(F.document & F.caption.startswith("/supply"))
async def handle_supply_document(message: types.Message):
    """Handle XLS uploads with /supply as caption."""
    await cmd_supply(message)


@dp.message(Command("bulksupply"))
async def cmd_bulksupply(message: types.Message):
    """One-shot bulk ingest of all historical supply files from the mounted folder.

    Reads all .xls files from Inventory/Поступление - возврат/2025/ and 2026/.
    """
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Omborga kirim/qaytarish fayllarni yuklash boshlandi...")
    try:
        import glob as globmod
        from backend.services.import_supply import apply_supply_import

        base_paths = [
            "/sessions/exciting-nifty-tesla/mnt/Catalogue:Telegram app/Inventory/Поступление - возврат/2025",
            "/sessions/exciting-nifty-tesla/mnt/Catalogue:Telegram app/Inventory/Поступление - возврат/2026",
        ]

        all_files = []
        for bp in base_paths:
            all_files.extend(sorted(globmod.glob(f"{bp}/*.xls")))
            all_files.extend(sorted(globmod.glob(f"{bp}/*.xlsx")))

        if not all_files:
            await status_msg.edit_text("❌ Fayllar topilmadi.")
            return

        total_inserted = 0
        total_updated = 0
        total_items = 0
        total_matched = 0
        total_unmatched = 0
        file_results = []
        all_unmatched_names: set = set()
        supply_total = 0
        return_total = 0
        adj_total = 0

        for fpath in all_files:
            fname = fpath.rsplit("/", 1)[-1]
            with open(fpath, "rb") as f:
                file_bytes = f.read()
            r = apply_supply_import(file_bytes, filename_hint=fname)
            if r.get("ok"):
                ins = r.get("inserted_docs", 0)
                upd = r.get("updated_docs", 0)
                items = r.get("total_items", 0)
                matched = r.get("matched_products", 0)
                unm = r.get("unmatched_products_count", 0)
                s = r.get("stats", {})
                total_inserted += ins
                total_updated += upd
                total_items += items
                total_matched += matched
                total_unmatched += unm
                supply_total += s.get("supply_count", 0)
                return_total += s.get("return_count", 0)
                adj_total += s.get("adjustment_count", 0)
                all_unmatched_names.update(r.get("unmatched_products", []))
                file_results.append(f"  ✅ {fname}: {ins}+{upd} док, {items} строк")
            else:
                file_results.append(f"  ❌ {fname}: {r.get('error', '?')}")

        match_rate = (total_matched / total_items * 100) if total_items else 0

        lines = [
            f"✅ <b>Bulk supply ingest tugadi!</b>\n",
            f"📁 Fayllar: {len(all_files)}",
            f"📄 Jami dokumentlar: {total_inserted + total_updated}"
            f" (поступлений: {supply_total}, возвратов: {return_total}"
            f", исправлений: {adj_total})",
            f"   🆕 yangi: {total_inserted}, ♻️ yangilangan: {total_updated}",
            f"📦 Jami tovar qatorlari: {total_items}",
            f"🔗 Sопоставлено: {total_matched}/{total_items} ({match_rate:.1f}%)",
            f"❓ Несопоставленные: {len(all_unmatched_names)} уникальных",
        ]

        if len(file_results) <= 20:
            lines.append("\n<b>Fayllar:</b>")
            lines.extend(file_results)
        else:
            lines.append(f"\n<b>Fayllar:</b> {len(all_files)} ta (ro'yxat qisqartirildi)")

        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3950] + "\n... (qisqartirildi)"
        await status_msg.edit_text(text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"/bulksupply error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


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


@dp.message(Command("unmatchedproducts"))
async def cmd_unmatchedproducts(message: types.Message):
    """Show real_order_items rows where product_id is NULL, grouped by 1C name.

    Ranks by line-item count so Session A / catalog team can prioritize the
    SKUs that hurt the most. Unlike /unmatchedclients there is no skip list —
    every unmatched product is a genuine catalog gap.
    """
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Bog'lanmagan mahsulotlar tekshirilmoqda...")

    try:
        import httpx

        api_url = f"{_BASE_URL}/api/finance/unmatched-real-products"
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(
                api_url,
                params={"admin_key": "rassvet2026", "limit": 100},
            )
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(
                f"❌ Xatolik: {result.get('error', 'Unknown')}"
            )
            return

        total_items = result.get("db_total_items", 0)
        matched = result.get("db_matched_items", 0)
        unmatched = result.get("db_unmatched_items", 0)
        match_pct = (matched / total_items * 100) if total_items else 0
        full_unique = result.get("total_unique_unmatched_names_full", 0)
        total_lines = result.get("total_unmatched_lines", 0)
        total_local = result.get("total_unmatched_local", 0)
        total_currency = result.get("total_unmatched_currency", 0)
        items = result.get("items", [])

        lines = [
            "📦 <b>Bog'lanmagan mahsulotlar</b>\n",
            f"💾 Jami qatorlar: {total_items:,}".replace(",", " "),
            f"✅ Bog'langan: {matched:,} ({match_pct:.1f}%)".replace(",", " "),
            f"❌ Bog'lanmagan: {unmatched:,}".replace(",", " "),
            f"🔢 Noyob nomlar (jami): {full_unique}",
        ]
        if total_local:
            lines.append(
                f"💴 Jami UZS summa: {total_local:,}".replace(",", " ")
            )
        if total_currency:
            lines.append(f"💵 Jami USD summa: {total_currency:,.2f}".replace(",", " "))

        lines.append(f"\n📋 Top-20 (eng ko'p qatordan):")

        if not items:
            lines.append("\n🎉 Hammasi bog'langan!")
        else:
            for i, it in enumerate(items[:20], 1):
                name = it["product_name_1c"] or "(empty)"
                if len(name) > 55:
                    name = name[:52] + "..."
                period = it["first_seen"] or "?"
                if it["last_seen"] and it["last_seen"] != it["first_seen"]:
                    period = f"{it['first_seen']}…{it['last_seen']}"
                qty = it.get("total_quantity", 0)
                local = it.get("total_local", 0)
                curr = it.get("total_currency", 0)
                parts = [f"📄 {it['line_count']} qator", f"📦 {qty:g}"]
                if local:
                    parts.append(f"💴 {local:,}".replace(",", " "))
                if curr:
                    parts.append(f"💵 {curr:,.0f}".replace(",", " "))
                parts.append(f"👥 {it['client_count']}")
                lines.append(
                    f"\n{i}. <b>{html_escape(name)}</b>\n"
                    f"   {' · '.join(parts)}\n"
                    f"   {period}"
                )

        lines.append(
            "\n\n💡 Bu ro'yxat katalog bo'shliqlarini ko'rsatadi — "
            "Session A / katalog jamoasi uchun."
        )

        msg = "\n".join(lines)
        if len(msg) > 3800:
            msg = msg[:3800] + "\n...(truncated)"
        await status_msg.edit_text(msg, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Unmatchedproducts error: {e}")
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


@dp.message(Command("ingestskus"))
async def cmd_ingest_skus(message: types.Message):
    """Add all unmatched product names from real_order_items to the products table.

    For each distinct product_name_1c WHERE product_id IS NULL:
    - Classifies into category/producer by brand family patterns
    - Generates a Latin display name via the import_products pipeline
    - INSERTs into products
    - UPDATEs real_order_items.product_id to link them
    """
    if not is_admin(message):
        return

    status_msg = await message.reply("⏳ Yangi SKU'lar qo'shilmoqda...")

    try:
        import httpx

        api_url = f"{_BASE_URL}/api/finance/ingest-unmatched-skus"
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

        added = result.get("products_added", 0)
        existed = result.get("products_already_existed", 0)
        relinked = result.get("items_relinked", 0)
        match_rate = result.get("new_match_rate", "N/A")
        remaining = result.get("remaining_unmatched", 0)

        lines = [
            "✅ <b>SKU ingestion tugadi</b>\n",
            f"➕ Yangi mahsulotlar qo'shildi: {added}",
            f"♻️ Mavjud mahsulotlar (qayta bog'landi): {existed}",
            f"🔗 Bog'langan qatorlar: {relinked}",
            "",
            f"<b>📊 Yangi match rate:</b> {match_rate}",
            f"❌ Hali ham bog'lanmagan: {remaining} qator",
        ]

        # Show details of top 15 added products
        details = result.get("details", [])
        if details:
            lines.append("\n<b>Qo'shilgan mahsulotlar:</b>")
            for d in details[:15]:
                action = "➕" if d["action"] == "added" else "♻️"
                lines.append(
                    f"{action} <code>{d['name_1c'][:45]}</code> "
                    f"→ {d.get('display_name', d.get('product_id', '?'))[:25]} "
                    f"({d['items_relinked']} qator)"
                )
            if len(details) > 15:
                lines.append(f"  ... va yana {len(details) - 15} ta")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Ingest SKUs error: {e}")
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
# /backfillordernames — rewrite wish-list order_items.product_name to Cyrillic
# ───────────────────────────────────────────

@dp.message(Command("backfillordernames"))
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
# /duplicateclients — audit multi-phone client groups
# ───────────────────────────────────────────

@dp.message(Command("duplicateclients"))
async def cmd_duplicateclients(message: types.Message):
    """Audit multi-phone client groups in allowed_clients.

    One real-world client (shop) can have up to 5 phone registrations
    (owner, relatives, workers). This command shows which client_id_1c
    names have multiple records, and how financial data is distributed
    across those records.

    Usage:
      /duplicateclients          — summary + top groups
      /duplicateclients SEARCH   — search for a specific client
    """
    if not is_admin(message):
        return

    arg = message.text.split(maxsplit=1)[1].strip() if len(message.text.split()) > 1 else ""

    status_msg = await message.reply("⏳ Ko'p telefonli mijozlarni tahlil qilmoqda...")

    try:
        conn = get_db()

        if arg:
            # Search mode — find a specific client's phone registrations
            search = f"%{arg.lower()}%"
            groups = conn.execute(
                """SELECT client_id_1c, COUNT(*) as cnt
                   FROM allowed_clients
                   WHERE client_id_1c IS NOT NULL AND client_id_1c != ''
                     AND COALESCE(status, 'active') != 'merged'
                     AND LOWER(client_id_1c) LIKE ?
                   GROUP BY client_id_1c
                   HAVING COUNT(*) > 1
                   ORDER BY cnt DESC
                   LIMIT 10""",
                (search,),
            ).fetchall()

            if not groups:
                conn.close()
                await status_msg.edit_text(
                    f"'{html_escape(arg)}' bo'yicha ko'p telefonli guruh topilmadi.",
                    parse_mode="HTML",
                )
                return

            lines = [f"🔍 <b>'{html_escape(arg)}' bo'yicha ko'p telefonli guruhlar:</b>\n"]
        else:
            # Summary mode
            groups = conn.execute(
                """SELECT client_id_1c, COUNT(*) as cnt
                   FROM allowed_clients
                   WHERE client_id_1c IS NOT NULL AND client_id_1c != ''
                     AND COALESCE(status, 'active') != 'merged'
                   GROUP BY client_id_1c
                   HAVING COUNT(*) > 1
                   ORDER BY cnt DESC, client_id_1c"""
            ).fetchall()

            if not groups:
                conn.close()
                await status_msg.edit_text("✅ Barcha mijozlar yagona telefon bilan — dublikat yo'q.")
                return

            total_multi = len(groups)
            total_phones = sum(g["cnt"] for g in groups)

            lines = [
                f"📊 <b>Ko'p telefonli mijozlar</b>\n",
                f"Jami: <b>{total_multi}</b> mijoz, <b>{total_phones}</b> telefon yozuvi",
                f"  📞×2: {sum(1 for g in groups if g['cnt'] == 2)} mijoz",
                f"  📞×3: {sum(1 for g in groups if g['cnt'] == 3)} mijoz",
                f"  📞×4+: {sum(1 for g in groups if g['cnt'] >= 4)} mijoz",
            ]

            # Check financial data distribution
            data_on_one = 0  # All financial data on 1 ID only
            data_spread = 0  # Data on multiple IDs
            no_data = 0      # No financial data at all

            for g in groups[:50]:  # Sample first 50 for speed
                recs = conn.execute(
                    """SELECT id FROM allowed_clients
                       WHERE client_id_1c = ? AND COALESCE(status, 'active') != 'merged'""",
                    (g["client_id_1c"],),
                ).fetchall()
                ids_with_data = 0
                for rec in recs:
                    has = conn.execute(
                        """SELECT (SELECT COUNT(*) FROM real_orders WHERE client_id = ?) +
                                  (SELECT COUNT(*) FROM client_balances WHERE client_id = ?) +
                                  (SELECT COUNT(*) FROM client_debts WHERE client_id = ?) as total""",
                        (rec["id"], rec["id"], rec["id"]),
                    ).fetchone()
                    if has["total"] > 0:
                        ids_with_data += 1

                if ids_with_data == 0:
                    no_data += 1
                elif ids_with_data == 1:
                    data_on_one += 1
                else:
                    data_spread += 1

            lines.append(f"\n<b>Moliyaviy ma'lumot taqsimoti</b> (ilk 50):")
            lines.append(f"  📊 1 ID da: {data_on_one}")
            lines.append(f"  📊 bir necha ID da: {data_spread}")
            lines.append(f"  ⚪ hech qayerda yo'q: {no_data}")
            lines.append(f"\n✅ Sibling resolution yoqilgan — barcha telefonlar bir xil moliyaviy ma'lumotni ko'radi.")
            lines.append(f"\n<b>Top guruhlar:</b>")
            groups = groups[:15]  # Show top 15

        # Show detailed group info
        for g in groups:
            cid_1c = g["client_id_1c"]
            recs = conn.execute(
                """SELECT ac.id, ac.phone_normalized, ac.name, ac.matched_telegram_id,
                          (SELECT COUNT(*) FROM real_orders WHERE client_id = ac.id) as orders,
                          (SELECT COUNT(*) FROM client_balances WHERE client_id = ac.id) as bal,
                          (SELECT COUNT(*) FROM users WHERE client_id = ac.id) as usr
                   FROM allowed_clients ac
                   WHERE ac.client_id_1c = ? AND COALESCE(ac.status, 'active') != 'merged'
                   ORDER BY ac.id""",
                (cid_1c,),
            ).fetchall()

            lines.append(f"\n📍 <b>{html_escape(cid_1c)}</b> — {len(recs)} tel.:")
            for rec in recs:
                tg = "✅" if rec["matched_telegram_id"] else "—"
                data_icons = []
                if rec["orders"] > 0:
                    data_icons.append(f"📦{rec['orders']}")
                if rec["bal"] > 0:
                    data_icons.append(f"💰{rec['bal']}")
                if rec["usr"] > 0:
                    data_icons.append(f"👤{rec['usr']}")
                data_str = " ".join(data_icons) if data_icons else "bo'sh"
                lines.append(
                    f"  #{rec['id']} tel:{rec['phone_normalized']} "
                    f"TG:{tg} {data_str}"
                )

        conn.close()

        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3990] + "\n..."

        await status_msg.edit_text(text, parse_mode="HTML")

    except Exception as e:
        logger.exception("duplicateclients error")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:500]}")


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

        # Persist file for daily auto-sync
        try:
            with open("/data/client_master_latest.xlsx", "wb") as _cm_f:
                _cm_f.write(file_bytes)
            logger.info("Client Master saved to /data/client_master_latest.xlsx for daily sync")
        except Exception as _cm_e:
            logger.warning(f"Could not persist client master file: {_cm_e}")

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
# Reply-to-link: link unmatched registrations
# to 1C clients via group message replies
# ───────────────────────────────────────────

@dp.message(F.reply_to_message)
async def handle_registration_reply(message: types.Message):
    """Handle replies to unmatched registration notifications in the Sales group.

    Sales team can reply with:
      - A 1C client name → links the user to that client and approves them
      - 'new' → marks as new client (still approves the user)
    """
    # Only process in the Sales group
    if message.chat.id != ORDER_GROUP_CHAT_ID:
        return
    if not message.text or not message.text.strip():
        return
    if not is_admin(message):
        return

    replied_msg_id = message.reply_to_message.message_id
    reply_text = message.text.strip()

    conn = get_db()
    try:
        # Check if this is a reply to a saved unmatched registration notification
        row = conn.execute(
            "SELECT id, telegram_id, phone, first_name FROM unmatched_registrations "
            "WHERE notification_message_id = ? AND status = 'pending'",
            (replied_msg_id,),
        ).fetchone()

        if not row:
            return  # Not a reply to a registration notification — ignore

        unreg_id = row["id"]
        tg_id = row["telegram_id"]
        phone = row["phone"]
        user_first_name = row["first_name"] or "—"

        if reply_text.lower() == "new":
            # Mark as new client, approve the user
            conn.execute(
                "UPDATE unmatched_registrations SET status = 'new_client', resolved_at = datetime('now') WHERE id = ?",
                (unreg_id,),
            )
            conn.execute("UPDATE users SET is_approved = 1 WHERE telegram_id = ?", (tg_id,))

            # Create allowed_clients row with phone
            phone_norm = normalize_phone(phone)
            if phone_norm:
                existing = conn.execute(
                    "SELECT id FROM allowed_clients WHERE phone_normalized = ? LIMIT 1",
                    (phone_norm,),
                ).fetchone()
                if not existing:
                    conn.execute(
                        "INSERT INTO allowed_clients (phone_normalized, name, source_sheet, status, matched_telegram_id) "
                        "VALUES (?, ?, 'bot_new_client', 'active', ?)",
                        (phone_norm, user_first_name, tg_id),
                    )
                    client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                else:
                    client_id = existing["id"]
                    conn.execute("UPDATE allowed_clients SET matched_telegram_id = ? WHERE id = ?", (tg_id, client_id))
                conn.execute("UPDATE users SET client_id = ? WHERE telegram_id = ?", (client_id, tg_id))

            conn.commit()

            # Persist to backup
            try:
                from backend.services.backup_users import save_user_to_backup
                u = conn.execute(
                    "SELECT telegram_id, phone, first_name, last_name, username, latitude, longitude, is_approved, client_id, registered_at FROM users WHERE telegram_id = ?",
                    (tg_id,),
                ).fetchone()
                if u:
                    save_user_to_backup(dict(u))
            except Exception:
                pass

            await message.reply(
                f"✅ <b>Yangi mijoz</b> sifatida belgilandi!\n\n"
                f"📛 {user_first_name}\n"
                f"🆔 {tg_id}\n\n"
                f"Foydalanuvchi tasdiqlandi. 1C da yangi kontragent yarating.",
                parse_mode="HTML",
            )

        else:
            # Try to match the reply text to an existing 1C client name
            # Search allowed_clients.name (case-insensitive)
            client_row = conn.execute(
                "SELECT id, name, phone_normalized FROM allowed_clients "
                "WHERE LOWER(TRIM(name)) = LOWER(TRIM(?)) AND name != '' LIMIT 1",
                (reply_text,),
            ).fetchone()

            if not client_row:
                # Try partial match (LIKE)
                client_row = conn.execute(
                    "SELECT id, name, phone_normalized FROM allowed_clients "
                    "WHERE name LIKE ? AND name != '' LIMIT 1",
                    (f"%{reply_text}%",),
                ).fetchone()

            if not client_row:
                await message.reply(
                    f"❌ <b>{_h(reply_text)}</b> — 1C bazasida topilmadi.\n\n"
                    f"Qayta urinib ko'ring yoki <b>new</b> yozing.",
                    parse_mode="HTML",
                )
                return

            client_id = client_row["id"]
            client_name = client_row["name"]

            # Link user to this client
            conn.execute(
                "UPDATE unmatched_registrations SET status = 'linked', linked_client_name = ?, resolved_at = datetime('now') WHERE id = ?",
                (client_name, unreg_id),
            )
            conn.execute("UPDATE users SET is_approved = 1, client_id = ? WHERE telegram_id = ?", (client_id, tg_id))
            conn.execute("UPDATE allowed_clients SET matched_telegram_id = ? WHERE id = ?", (tg_id, client_id))
            conn.commit()

            # Persist to backup
            try:
                from backend.services.backup_users import save_user_to_backup
                u = conn.execute(
                    "SELECT telegram_id, phone, first_name, last_name, username, latitude, longitude, is_approved, client_id, registered_at FROM users WHERE telegram_id = ?",
                    (tg_id,),
                ).fetchone()
                if u:
                    save_user_to_backup(dict(u))
            except Exception:
                pass

            await message.reply(
                f"✅ Bog'landi!\n\n"
                f"🏢 1C mijoz: <b>{_h(client_name)}</b>\n"
                f"📛 Telegram: {_h(user_first_name)}\n"
                f"🆔 {tg_id}\n\n"
                f"Foydalanuvchi tasdiqlandi.",
                parse_mode="HTML",
            )

    except Exception as e:
        logger.error(f"handle_registration_reply error: {e}")
        await message.reply(f"❌ Xatolik: {e}")
    finally:
        conn.close()


# ── Session G: Credit Scoring Commands ─────────────────────────────

@dp.message(Command("clientscore"))
async def cmd_clientscore(message: types.Message):
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
        # Format credit limit
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


@dp.message(Command("runscore"))
async def cmd_runscore(message: types.Message):
    """Manually trigger credit score recalculation for all clients."""
    if not is_admin(message):
        return

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

    # Relink info (payments/debts fixed before scoring)
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


@dp.message(Command("payments"))
async def cmd_payments(message: types.Message):
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

        # Count total payments
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


@dp.message(Command("scorestats"))
async def cmd_scorestats(message: types.Message):
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


@dp.message(Command("adjustscore"))
async def cmd_adjustscore(message: types.Message):
    """Manual score adjustment. Usage: /adjustscore <name> <delta> <reason>"""
    if not is_admin(message):
        return

    args = message.text.split(None, 3)  # /adjustscore <name> <delta> <reason>
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

    # Find the client
    try:
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


@dp.message(Command("scoreanomalies"))
async def cmd_scoreanomalies(message: types.Message):
    """Weekly anomaly report: clients with score drops + stale payment data."""
    if not is_admin(message):
        return

    try:
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

    for a in anomalies[:15]:  # limit output
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


# ───────────────────────────────────────────
# ───────────────────────────────────────────
# Location sharing handler (MUST be before fallback)
# ───────────────────────────────────────────

def _reverse_geocode(lat: float, lng: float) -> dict:
    """Reverse geocode lat/lng using Nominatim (OpenStreetMap).

    Returns dict with keys: address, region, district.
    """
    result = {"address": "", "region": "", "district": ""}
    try:
        import httpx
        resp = httpx.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lng, "format": "json", "accept-language": "uz,ru", "zoom": 16},
            headers={"User-Agent": "RassvetCatalogBot/1.0"},
            timeout=5,
        )
        data = resp.json()
        addr = data.get("address", {})

        # Region (viloyat)
        result["region"] = addr.get("state", "")

        # District (tuman/shahar) — try county first, then city/town
        result["district"] = addr.get("county", "") or addr.get("city", "") or addr.get("town", "") or addr.get("village", "")

        # Build readable address: city/district + street details
        parts = []
        city = addr.get("city", "") or addr.get("town", "") or addr.get("village", "")
        if city:
            parts.append(city)
        # Add street-level detail
        road = addr.get("road", "")
        neighbourhood = addr.get("neighbourhood", "") or addr.get("suburb", "")
        if road:
            street = road
            if addr.get("house_number"):
                street += " " + addr["house_number"]
            parts.append(street)
        elif neighbourhood:
            parts.append(neighbourhood)

        result["address"] = ", ".join(parts) if parts else data.get("display_name", "")[:100]
    except Exception as e:
        logger.warning(f"Reverse geocode failed: {e}")
    return result


@dp.message(F.location)
async def handle_location_before_fallback(message: types.Message):
    """Handle shared location from user — save coordinates + reverse geocode."""
    loc = message.location
    telegram_id = message.from_user.id

    conn = get_db()
    user = conn.execute(
        "SELECT telegram_id, first_name, is_approved FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user:
        conn.close()
        await message.reply(
            "❌ Siz hali ro'yxatdan o'tmagansiz.\n"
            "Avval ilovada ro'yxatdan o'ting.",
        )
        return

    geo = _reverse_geocode(loc.latitude, loc.longitude)

    conn.execute(
        "UPDATE users SET latitude = ?, longitude = ?, location_address = ?, location_region = ?, location_district = ?, location_updated = datetime('now') WHERE telegram_id = ?",
        (loc.latitude, loc.longitude, geo["address"], geo["region"], geo["district"], telegram_id),
    )
    conn.commit()
    conn.close()

    maps_url = f"https://maps.google.com/?q={loc.latitude},{loc.longitude}"
    # Build display: region + district + address detail
    display_parts = []
    if geo["region"]:
        display_parts.append(geo["region"])
    if geo["address"]:
        display_parts.append(geo["address"])
    address_display = ", ".join(display_parts) if display_parts else "manzil aniqlandi"

    # First remove any reply keyboard
    await message.answer(
        f"✅ Joylashuvingiz saqlandi!\n\n"
        f"📍 <b>{address_display}</b>\n"
        f"🗺 <a href=\"{maps_url}\">Xaritada ko'rish</a>\n\n"
        f"💡 Buyurtma berishda ushbu manzil ishlatiladi.\n"
        f"Yangilash uchun yangi joylashuv yuboring.",
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=ReplyKeyboardRemove(),
    )

    # Then send the catalog button separately
    back_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Katalogga qaytish", web_app=WebAppInfo(url=WEBAPP_URL))]
        ]
    )
    await message.answer(
        "Katalogni ochish uchun quyidagi tugmani bosing:",
        reply_markup=back_keyboard,
    )


# Fallback — only for private chats (MUST BE LAST — catches all unmatched messages)
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

    try:
        from bot.reminders import start_reminder_tasks
        start_reminder_tasks(bot, ADMIN_GROUP_CHAT_ID)
    except Exception as e:
        logger.error(f"Failed to start daily-upload reminder tasks: {e}")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
