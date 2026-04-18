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
WEBAPP_URL = f"{_BASE_URL}?v=15"
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")

ORDER_GROUP_CHAT_ID = int(os.getenv("ORDER_GROUP_CHAT_ID", "-1003740010463"))
ADMIN_GROUP_CHAT_ID = int(os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051"))
AGENTS_GROUP_CHAT_ID = int(os.getenv("AGENTS_GROUP_CHAT_ID", "-1003922400481"))

ADMIN_IDS: set[int] = set()
_admin_env = os.getenv("ADMIN_IDS", "")
if _admin_env:
    ADMIN_IDS = {int(x.strip()) for x in _admin_env.split(",") if x.strip().isdigit()}

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
    Sotuv bo'lim is silenced — admin commands only in Admin group or DM."""
    if hasattr(message, 'chat') and message.chat.id == ORDER_GROUP_CHAT_ID:
        return False
    if ADMIN_IDS and hasattr(message, 'from_user') and message.from_user and message.from_user.id in ADMIN_IDS:
        return True
    if hasattr(message, 'chat') and message.chat.id == ADMIN_GROUP_CHAT_ID:
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
