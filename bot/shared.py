"""Shared constants, helpers, and DB access for all bot handlers.

Every handler module imports from here instead of bot.main, so the
monolith can be split without circular imports.
"""
import os
import re
import sqlite3
import logging

from html import escape as _h
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("bot")

# ── Constants ────────────────────────────────────────────────────────

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
_BASE_URL = os.getenv("WEBAPP_URL", "https://telegram-catalog-production.up.railway.app")
WEBAPP_URL = f"{_BASE_URL}?v=16"
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")

ORDER_GROUP_CHAT_ID = int(os.getenv("ORDER_GROUP_CHAT_ID", "-1003740010463"))
ADMIN_GROUP_CHAT_ID = int(os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051"))
AGENTS_GROUP_CHAT_ID = int(os.getenv("AGENTS_GROUP_CHAT_ID", "-1003922400481"))
DAILY_GROUP_CHAT_ID = int(os.getenv("DAILY_GROUP_CHAT_ID", "-5243912135"))
INVENTORY_GROUP_CHAT_ID = int(os.getenv("INVENTORY_GROUP_CHAT_ID", "-5133871411"))
# Cashier-only group (Aunt + Uncle for now). 0 = unconfigured;
# the cashier FSM stays inert until this is set in the env.
CASHIER_GROUP_CHAT_ID = int(os.getenv("CASHIER_GROUP_CHAT_ID", "0"))


def chat_context(message) -> str:
    """Classify chat for /help filtering and context-aware routing.
    Returns one of: 'daily', 'admin', 'sales', 'inventory', 'agents',
    'dm_admin', 'dm_user', 'unknown'."""
    cid = message.chat.id if getattr(message, 'chat', None) else None
    if cid == DAILY_GROUP_CHAT_ID:
        return 'daily'
    if cid == ADMIN_GROUP_CHAT_ID:
        return 'admin'
    if cid == ORDER_GROUP_CHAT_ID:
        return 'sales'
    if cid == INVENTORY_GROUP_CHAT_ID:
        return 'inventory'
    if cid == AGENTS_GROUP_CHAT_ID:
        return 'agents'
    if CASHIER_GROUP_CHAT_ID and cid == CASHIER_GROUP_CHAT_ID:
        return 'cashier'
    if getattr(message, 'chat', None) and message.chat.type == 'private':
        uid = message.from_user.id if getattr(message, 'from_user', None) else None
        if uid and ADMIN_IDS and uid in ADMIN_IDS:
            return 'dm_admin'
        return 'dm_user'
    return 'unknown'

ADMIN_IDS: set[int] = set()
_admin_env = os.getenv("ADMIN_IDS", "")
if _admin_env:
    ADMIN_IDS = {int(x.strip()) for x in _admin_env.split(",") if x.strip().isdigit()}

# Cashier whitelist — Aunt Muqaddas (275116966) + Uncle (21117506) for the
# parallel-mode launch. Uncle reverts to bank-transfers-only after cutover;
# his ID stays here while we test.
CASHIER_IDS: set[int] = set()
_cashier_env = os.getenv("CASHIER_IDS", "")
if _cashier_env:
    CASHIER_IDS = {int(x.strip()) for x in _cashier_env.split(",") if x.strip().isdigit()}

TESTCLIENT_PROMPT = "🔎 Qidirish uchun mijoz ismini yozing"


# ── DB access ────────────────────────────────────────────────────────

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
    """Get database connection with dict row factory + Unicode LOWER."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = _DictRow
    conn.create_function("LOWER", 1, lambda s: s.lower() if s else s)
    return conn


# ── HTML escape ──────────────────────────────────────────────────────

def html_escape(text: str) -> str:
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── Phone normalization ──────────────────────────────────────────────

def normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    return digits[-9:] if len(digits) >= 9 else digits


# ── Permission checks ───────────────────────────────────────────────

def is_admin(message) -> bool:
    """Check if the sender may run admin commands in this chat.
    Sotuv bo'lim (Sales) is silenced — admin commands only in Admin group,
    Daily group (for the daily-upload commands), or DM of an admin user."""
    cid = message.chat.id if hasattr(message, 'chat') else None
    if cid == ORDER_GROUP_CHAT_ID:
        return False
    uid = message.from_user.id if getattr(message, 'from_user', None) else None
    if ADMIN_IDS and uid and uid in ADMIN_IDS:
        return True
    if cid in (ADMIN_GROUP_CHAT_ID, DAILY_GROUP_CHAT_ID, INVENTORY_GROUP_CHAT_ID):
        return True
    return False


def _is_sotuv_sender(message) -> bool:
    return hasattr(message, 'chat') and message.chat.id == ORDER_GROUP_CHAT_ID


def is_agent_or_admin(message) -> bool:
    if is_admin(message):
        return True
    if hasattr(message, 'chat') and message.chat.id == AGENTS_GROUP_CHAT_ID:
        return True
    return False


def is_agent_or_admin_cb(cb) -> bool:
    chat_id = cb.message.chat.id if cb.message else None
    if ADMIN_IDS and cb.from_user and cb.from_user.id in ADMIN_IDS:
        return True
    if chat_id in (ORDER_GROUP_CHAT_ID, ADMIN_GROUP_CHAT_ID, AGENTS_GROUP_CHAT_ID):
        return True
    return False


def is_admin_cb(cb) -> bool:
    """Callback-variant of is_admin — admin user-id whitelist OR admin-
    type group (admin / daily / inventory). Excludes ORDER_GROUP."""
    if ADMIN_IDS and cb.from_user and cb.from_user.id in ADMIN_IDS:
        return True
    chat_id = cb.message.chat.id if cb.message else None
    if chat_id == ORDER_GROUP_CHAT_ID:
        return False
    if chat_id in (ADMIN_GROUP_CHAT_ID, DAILY_GROUP_CHAT_ID, INVENTORY_GROUP_CHAT_ID):
        return True
    return False


def is_cashier(message) -> bool:
    """User is whitelisted as a cashier (CASHIER_IDS) or message comes
    from the dedicated cashier group."""
    uid = message.from_user.id if getattr(message, 'from_user', None) else None
    if CASHIER_IDS and uid and uid in CASHIER_IDS:
        return True
    cid = message.chat.id if hasattr(message, 'chat') else None
    if CASHIER_GROUP_CHAT_ID and cid == CASHIER_GROUP_CHAT_ID:
        return True
    return False


def is_cashier_or_admin(message) -> bool:
    return is_admin(message) or is_cashier(message)


def is_cashier_or_admin_cb(cb) -> bool:
    if ADMIN_IDS and cb.from_user and cb.from_user.id in ADMIN_IDS:
        return True
    if CASHIER_IDS and cb.from_user and cb.from_user.id in CASHIER_IDS:
        return True
    chat_id = cb.message.chat.id if cb.message else None
    if chat_id in (ADMIN_GROUP_CHAT_ID,) or (CASHIER_GROUP_CHAT_ID and chat_id == CASHIER_GROUP_CHAT_ID):
        return True
    return False


def get_user_role(message):
    """Return the most-privileged role that applies. Used for audit and
    role tagging on intake records. Returns None if no recognized role."""
    if is_admin(message):
        return "admin"
    if is_cashier(message):
        return "cashier"
    if is_agent_or_admin(message):
        return "agent"
    return None


# ── Display helpers ──────────────────────────────────────────────────

def sender_display_name(message) -> str:
    u = message.from_user
    if not u:
        return "unknown"
    parts = [u.first_name or "", u.last_name or ""]
    name = " ".join(p for p in parts if p).strip()
    return name or u.username or str(u.id)


# ── Daily uploads tracking ───────────────────────────────────────────

_DDMMYYYY_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")


def extract_snapshot_date(message) -> str | None:
    """Parse a DD/MM/YYYY token from the command caption/text."""
    candidates: list[str] = []
    if hasattr(message, 'text') and message.text:
        candidates.append(message.text)
    if hasattr(message, 'caption') and message.caption:
        candidates.append(message.caption)
    if hasattr(message, 'reply_to_message') and message.reply_to_message:
        if hasattr(message.reply_to_message, 'caption') and message.reply_to_message.caption:
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


def log_admin_action(message, command: str, args: str = "") -> None:
    """Fire-and-forget: record an admin action into admin_action_log for
    forensic audit. Covers destructive / irreversible commands so post-
    incident review can answer who did what, when. Never raises."""
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO admin_action_log (telegram_id, user_name, chat_id, command, args) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                message.from_user.id if message.from_user else None,
                sender_display_name(message),
                message.chat.id if hasattr(message, 'chat') and message.chat else None,
                command,
                (args or "")[:500] or None,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"log_admin_action failed for {command}: {e}")


def track_daily_upload(
    upload_type: str,
    message,
    file_name: str | None = None,
    row_count: int = 0,
    notes: str | None = None,
    upload_date: str | None = None,
) -> None:
    """Fire-and-forget: record a successful upload into daily_uploads."""
    try:
        from backend.services.daily_uploads import record_upload
        user = message.from_user
        record_upload(
            upload_type,
            user_id=user.id if user else None,
            user_name=sender_display_name(message),
            file_name=file_name,
            row_count=int(row_count or 0),
            notes=notes,
            upload_date=upload_date,
        )
    except Exception as e:
        logger.error(f"daily_uploads tracking failed for {upload_type}: {e}")
