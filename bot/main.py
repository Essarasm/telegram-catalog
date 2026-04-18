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
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    MenuButtonWebApp,
    WebAppInfo,
)

TESTCLIENT_PROMPT = "🔎 Qidirish uchun mijoz ismini yozing"

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


class _DictRow(dict):
    """Dict-like row that supports BOTH r["name"] and r[0] access.
    Also supports .get() (unlike sqlite3.Row)."""
    __slots__ = ('_values',)
    def __init__(self, cursor, row):
        cols = [col[0] for col in cursor.description]
        super().__init__(zip(cols, row))
        self._values = row
    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)


def get_db():
    """Get database connection (same as backend)."""
    import sqlite3
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = _DictRow
    # Register Unicode-aware LOWER for Cyrillic search (SQLite built-in only handles ASCII)
    conn.create_function("LOWER", 1, lambda s: s.lower() if s else s)
    return conn


def normalize_phone(raw: str) -> str:
    """Strip to last 9 digits for matching."""
    digits = re.sub(r"\D", "", raw or "")
    return digits[-9:] if len(digits) >= 9 else digits


def is_admin(message: types.Message) -> bool:
    """Check if the sender is an admin who may run commands here.

    Rule: Sotuv bo'lim (ORDER_GROUP_CHAT_ID) is kept clean — the only bot
    traffic there is the "new order #N" card + the manager's reply-with-
    Excel confirmation. Admin commands are silenced in Sotuv so they
    don't clutter the order feed. Use the dedicated Admin group (or DM
    the bot as an ADMIN_IDS user) for all other bot commands.
    """
    if message.chat.id == ORDER_GROUP_CHAT_ID:
        return False
    if ADMIN_IDS and message.from_user and message.from_user.id in ADMIN_IDS:
        return True
    if message.chat.id == ADMIN_GROUP_CHAT_ID:
        return True
    return False


def _is_sotuv_sender(message: types.Message) -> bool:
    """Looser gate for reply-driven flows that are inherently Sotuv-bound.
    Kept for historical Sotuv-anchored replies; no active caller today.
    """
    return message.chat.id == ORDER_GROUP_CHAT_ID


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


def _is_agent_or_admin_cb(cb: types.CallbackQuery) -> bool:
    """Same gate as _is_agent_or_admin but for callback queries."""
    chat_id = cb.message.chat.id if cb.message else None
    if ADMIN_IDS and cb.from_user and cb.from_user.id in ADMIN_IDS:
        return True
    if chat_id in (ORDER_GROUP_CHAT_ID, ADMIN_GROUP_CHAT_ID, AGENTS_GROUP_CHAT_ID):
        return True
    return False


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

@dp.message(F.document & (F.chat.id == ORDER_GROUP_CHAT_ID) & F.reply_to_message)
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


# ───────────────────────────────────────────
# /realordersample — diagnostic dump of one real order's price columns
# ───────────────────────────────────────────

# ───────────────────────────────────────────
# /backfillordernames — rewrite wish-list order_items.product_name to Cyrillic
# ───────────────────────────────────────────

# ───────────────────────────────────────────
# /backfillrealordertotals — heal missing totals on existing real_orders
# ───────────────────────────────────────────

# ───────────────────────────────────────────
# /duplicateclients — audit multi-phone client groups
# ───────────────────────────────────────────

# ───────────────────────────────────────────
# /clientmaster — import curated Client Master xlsx into allowed_clients
# ───────────────────────────────────────────

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
    # Registration notifications now post in the Admin group. Only accept
    # replies there (and silently ignore replies in Sotuv / Agents / DMs).
    if message.chat.id != ADMIN_GROUP_CHAT_ID:
        return
    if not message.text or not message.text.strip():
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
    """Handle shared location from user — save coordinates + reverse geocode.

    When an agent is /testclient-linked to a client, the GPS is saved on
    the CLIENT's profile (not the agent's own), so agents can tag client
    shop locations on the spot.
    """
    loc = message.location
    telegram_id = message.from_user.id

    conn = get_db()
    user = conn.execute(
        "SELECT telegram_id, first_name, is_approved, is_agent, client_id "
        "FROM users WHERE telegram_id = ?",
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

    # Determine who is setting the location and for whom
    is_agent_linked = user["is_agent"] and user["client_id"]
    client_1c_name = ""
    setter_name = user["first_name"] or str(telegram_id)
    setter_role = "agent" if is_agent_linked else "client"

    # Check if location already existed (for overwrite detection)
    target_tg = telegram_id
    if is_agent_linked:
        # For agents, check the target CLIENT's location
        target_user = conn.execute(
            "SELECT latitude, longitude, location_set_by_name, location_set_by_role, "
            "location_set_by_tg_id, location_updated "
            "FROM users WHERE client_id = ? AND latitude IS NOT NULL LIMIT 1",
            (user["client_id"],),
        ).fetchone()
    else:
        target_user = conn.execute(
            "SELECT latitude, longitude, location_set_by_name, location_set_by_role, "
            "location_set_by_tg_id, location_updated "
            "FROM users WHERE telegram_id = ? AND latitude IS NOT NULL",
            (telegram_id,),
        ).fetchone()

    had_location = bool(target_user and target_user["latitude"])
    prev_setter_name = target_user["location_set_by_name"] if target_user else None
    prev_setter_role = target_user["location_set_by_role"] if target_user else None
    prev_lat = target_user["latitude"] if target_user else None
    prev_lng = target_user["longitude"] if target_user else None

    # Build the SET clause for location tracking columns
    loc_tracking = (
        "latitude = ?, longitude = ?, location_address = ?, "
        "location_region = ?, location_district = ?, location_updated = datetime('now'), "
        "location_set_by_tg_id = ?, location_set_by_name = ?, location_set_by_role = ?"
    )
    loc_params = (loc.latitude, loc.longitude, geo["address"], geo["region"],
                  geo["district"], telegram_id, setter_name, setter_role)

    if is_agent_linked:
        ac = conn.execute(
            "SELECT client_id_1c FROM allowed_clients WHERE id = ?",
            (user["client_id"],),
        ).fetchone()
        client_1c_name = ac["client_id_1c"] if ac else ""
        # Save to the agent's own row
        conn.execute(f"UPDATE users SET {loc_tracking} WHERE telegram_id = ?",
                     loc_params + (telegram_id,))
        # Save to allowed_clients
        conn.execute(
            "UPDATE allowed_clients SET location = ? WHERE id = ?",
            (f"{loc.latitude},{loc.longitude}|{geo['address'] or ''}", user["client_id"]),
        )
        # Save to ALL sibling users
        from backend.database import get_sibling_client_ids
        siblings = get_sibling_client_ids(conn, user["client_id"])
        for sid in siblings:
            conn.execute(
                f"UPDATE users SET {loc_tracking} WHERE client_id = ? AND telegram_id != ?",
                loc_params + (sid, telegram_id),
            )
    else:
        conn.execute(f"UPDATE users SET {loc_tracking} WHERE telegram_id = ?",
                     loc_params + (telegram_id,))

    conn.commit()
    conn.close()

    # Notify Xatolar group ONLY on overwrites (location already existed)
    if had_location:
        try:
            import httpx as _httpx
            ERRORS_CHAT = os.getenv("ERRORS_GROUP_CHAT_ID", "-5085083917")
            client_label = client_1c_name or setter_name
            prev_maps = f"https://maps.google.com/?q={prev_lat},{prev_lng}" if prev_lat else "—"
            new_maps = f"https://maps.google.com/?q={loc.latitude},{loc.longitude}"
            lines = [
                "📍 <b>Joylashuv o'zgartirildi (overwrite)</b>",
                "",
                f"🧾 Mijoz: <b>{html_escape(client_label)}</b>",
                f"👤 O'zgartiruvchi: {html_escape(setter_name)} ({setter_role})",
                f"🆔 TG ID: <code>{telegram_id}</code>",
                "",
                f"📍 Avvalgi: {html_escape(prev_setter_name or '—')} ({prev_setter_role or '—'})",
                f"   <a href=\"{prev_maps}\">Eski joylashuv</a>",
                f"📍 Yangi: <a href=\"{new_maps}\">Yangi joylashuv</a>",
            ]
            _httpx.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": ERRORS_CHAT, "text": "\n".join(lines),
                      "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=10,
            )
        except Exception as e:
            logger.error(f"Failed to notify location overwrite: {e}")

    maps_url = f"https://maps.google.com/?q={loc.latitude},{loc.longitude}"
    display_parts = []
    if geo["region"]:
        display_parts.append(geo["region"])
    if geo["address"]:
        display_parts.append(geo["address"])
    address_display = ", ".join(display_parts) if display_parts else "manzil aniqlandi"

    if is_agent_linked and client_1c_name:
        confirm_text = (
            f"✅ <b>{client_1c_name}</b> joylashuvi saqlandi!\n\n"
            f"📍 <b>{address_display}</b>\n"
            f"🗺 <a href=\"{maps_url}\">Xaritada ko'rish</a>\n\n"
            f"💡 Buyurtma berishda ushbu manzil ishlatiladi."
        )
    else:
        confirm_text = (
            f"✅ Joylashuvingiz saqlandi!\n\n"
            f"📍 <b>{address_display}</b>\n"
            f"🗺 <a href=\"{maps_url}\">Xaritada ko'rish</a>\n\n"
            f"💡 Buyurtma berishda ushbu manzil ishlatiladi.\n"
            f"Yangilash uchun yangi joylashuv yuboring."
        )

    await message.answer(
        confirm_text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=ReplyKeyboardRemove(),
    )

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

    # Include modular handler routers (split from monolith for isolation)
    from bot.handlers.testclient import router as testclient_router
    from bot.handlers.admin import router as admin_router
    from bot.handlers.uploads import router as uploads_router
    dp.include_router(testclient_router)
    dp.include_router(admin_router)
    dp.include_router(uploads_router)
    logger.info("Loaded handler modules: testclient, admin, uploads")

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
