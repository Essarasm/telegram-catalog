"""Catalog-group /foto workflow.

Lives in CATALOG_GROUP_CHAT_ID. Three flows:

  1. /foto                — anyone in the group; posts (or re-lists) the
                            active batch of up to 10 product messages.
  2. F.document reply     — accept image file (HEIC/JPEG/PNG/WebP) → push
                            to Google Drive, mark item photographed, edit
                            the original message header to ✅.
  3. F.photo reply        — REJECT with "fayl shaklida yuboring" to keep
                            raw quality for offline trimming.
  4. cb_foto_skip:<id>    — skip button under each item. Marks item
                            skipped, edits header to ⏭.

Batch advance: when the last pending item in the active batch is resolved
(photographed OR skipped), the bot auto-posts the next 10. If no more
missing-photo products remain, posts the completion message.

All filters chat-id-gated to CATALOG_GROUP_CHAT_ID to avoid
HANDLER_ORDER_SWALLOW (Error Log #33).
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from bot.shared import BOT_TOKEN, CATALOG_GROUP_CHAT_ID, html_escape
from backend.services import photo_batch as pb
from backend.services import gdrive_uploader

logger = logging.getLogger(__name__)

TASHKENT_TZ = timezone(timedelta(hours=5))

# Accepted document MIME types. iOS Telegram sends HEIC; Android sends JPEG;
# desktop can send PNG or WebP. Anything else (PDF, ZIP, video) is rejected.
ACCEPTED_MIME_PREFIXES = ("image/",)
ACCEPTED_EXT_FALLBACK = {".heic", ".heif", ".jpg", ".jpeg", ".png", ".webp", ".tiff", ".bmp"}

# Cap upload size at 20 MB. Phone photos as File are typically 2-8 MB; this
# is generous headroom while protecting against unintended large uploads.
MAX_UPLOAD_BYTES = 20 * 1024 * 1024

router = Router(name="photo_batch")


# ── helpers ────────────────────────────────────────────────────────


def _in_catalog_group(message: Message) -> bool:
    if not CATALOG_GROUP_CHAT_ID:
        return False
    return message.chat and message.chat.id == CATALOG_GROUP_CHAT_ID


def _is_accepted_image_doc(mime: Optional[str], filename: Optional[str]) -> bool:
    if mime and any(mime.lower().startswith(p) for p in ACCEPTED_MIME_PREFIXES):
        return True
    if filename:
        lower = filename.lower()
        for ext in ACCEPTED_EXT_FALLBACK:
            if lower.endswith(ext):
                return True
    return False


def _safe_segment(text: str, max_len: int = 80) -> str:
    """Filename-safe segment: strip path separators, collapse whitespace,
    truncate. Preserves Cyrillic (Drive + macOS handle UTF-8 fine).
    """
    if not text:
        return ""
    # Strip path separators and characters Google Drive / macOS dislike.
    cleaned = re.sub(r"[/\\\n\r\t\x00]", "_", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip()
    return cleaned


def _build_filename(product_id: int, name_1c: str, ext: str) -> str:
    """`{id}_{1c name}_{YYYYMMDD-HHMMSS}.{ext}`. Cyrillic preserved.
    Timestamp avoids overwrites when multiple uploads land for one product.
    """
    ts = datetime.now(TASHKENT_TZ).strftime("%Y%m%d-%H%M%S")
    safe_name = _safe_segment(name_1c) or "noname"
    safe_ext = ext.lstrip(".").lower() if ext else "bin"
    return f"{product_id}_{safe_name}_{ts}.{safe_ext}"


def _ext_from(filename: Optional[str], mime: Optional[str]) -> str:
    """Pick the most informative extension we can. Prefers original
    filename suffix; falls back to mime-type mapping; last resort 'bin'.
    """
    if filename and "." in filename:
        return filename.rsplit(".", 1)[1]
    mime_map = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
        "image/heic": "heic",
        "image/heif": "heif",
        "image/tiff": "tiff",
        "image/bmp": "bmp",
    }
    return mime_map.get((mime or "").lower(), "bin")


def _item_message_text(item: dict) -> str:
    """Pending-state text for an item message."""
    name_1c = html_escape(item.get("product_name_1c") or "")
    producer = html_escape(item.get("producer_name") or "")
    category = html_escape(item.get("category_name") or "")
    order_count = item.get("order_count", 0)
    pid = item.get("product_id")
    header = f"<b>№{pid}</b> · {order_count}× / 60 kun"
    meta_parts = [p for p in (producer, category) if p]
    meta = " · ".join(meta_parts)
    return (
        f"{header}\n"
        f"<b>{name_1c}</b>\n"
        f"{meta}\n\n"
        f"📎 Foto qo'shish uchun shu xabarga <b>Fayl</b> shaklida foto yuboring "
        f"(rasm sifati saqlanishi uchun).\n"
        f"<i>📎 → Fayl → Galereya</i>"
    )


def _resolved_message_text(item: dict, marker: str) -> str:
    """Header text after photographed/skipped. Strips upload instructions."""
    name_1c = html_escape(item.get("product_name_1c") or "")
    pid = item.get("product_id")
    return f"{marker} <b>№{pid}</b> · <b>{name_1c}</b>"


def _skip_keyboard(item_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Skip", callback_data=f"foto_skip:{item_id}"),
    ]])


# ── /foto command ─────────────────────────────────────────────────


@router.message(Command("foto"), F.chat.id == CATALOG_GROUP_CHAT_ID)
async def cmd_foto(message: Message):
    """Start a new batch or re-list pending items in the active batch."""
    if not CATALOG_GROUP_CHAT_ID:
        return

    if not gdrive_uploader.is_configured():
        await message.reply(
            "⚠️ Google Drive sozlanmagan. Admin GDRIVE_SERVICE_ACCOUNT_JSON "
            "va GDRIVE_EMPLOYEE_UPLOADS_FOLDER_ID env-larini Railway'da o'rnatishi kerak.",
        )
        return

    batch_id, items, is_new = pb.start_or_resume_batch()

    if not items:
        await message.reply(
            "✅ Hammasi tugadi — rasm yo'q mahsulotlar qolmadi (oxirgi 60 kun)."
        )
        return

    if not is_new:
        # Existing batch — summarize instead of re-posting.
        lines = [f"📷 Hozir <b>{batch_id}-batch</b> davom etmoqda — {len(items)} ta xabar javob kutmoqda."]
        lines.append("")
        for it in items:
            name = html_escape(it.get("product_name_1c") or "")
            lines.append(f"• №{it['product_id']} — {name}")
        lines.append("")
        lines.append("Eski xabarlarga foto bilan javob bering yoki ⏭ Skip bosing.")
        await message.reply("\n".join(lines), parse_mode="HTML")
        return

    # New batch — post one message per item, save message_id mapping.
    await message.reply(
        f"📷 <b>{batch_id}-batch</b> boshlandi — {len(items)} ta mahsulot.\n"
        f"Har bir xabarga foto bilan javob bering (Fayl shaklida).",
        parse_mode="HTML",
    )
    for it in items:
        sent = await message.bot.send_message(
            chat_id=CATALOG_GROUP_CHAT_ID,
            text=_item_message_text(it),
            parse_mode="HTML",
            reply_markup=_skip_keyboard(it["id"]),
        )
        pb.register_message_id(it["id"], sent.message_id)
        # Avoid hitting Telegram's group rate limit (~20 msg/min). Small
        # stagger keeps us well under and preserves message ordering.
        await asyncio.sleep(0.4)


# ── photo reply rejection (file-not-photo discipline) ─────────────


@router.message(
    F.chat.id == CATALOG_GROUP_CHAT_ID,
    F.photo,
    F.reply_to_message,
)
async def reject_photo_reply(message: Message):
    """Telegram-compressed photos lose quality; reject and instruct.
    Only fires for replies to OUR tracked item messages — random photos
    in the group are ignored silently."""
    if not message.reply_to_message:
        return
    item = pb.find_item_by_message_id(message.reply_to_message.message_id)
    if not item:
        return  # not one of our items — ignore
    await message.reply(
        "⚠️ Foto sifati saqlanishi uchun rasmni <b>Fayl</b> shaklida yuboring:\n"
        "📎 → <b>Fayl</b> → Galereya → tanlang.\n\n"
        "Telegram \"Photo\" qilib yuborilgan rasmlar sifati pasaytiriladi.",
        parse_mode="HTML",
    )


# ── document reply (the actual happy path) ────────────────────────


@router.message(
    F.chat.id == CATALOG_GROUP_CHAT_ID,
    F.document,
    F.reply_to_message,
)
async def on_document_reply(message: Message):
    if not message.reply_to_message:
        return
    item = pb.find_item_by_message_id(message.reply_to_message.message_id)
    if not item:
        return  # reply to a non-tracked message — ignore

    if item["status"] != "pending":
        await message.reply(
            f"ℹ️ Bu mahsulot allaqachon {'foto bilan' if item['status'] == 'photographed' else 'skipped'} belgilangan.",
        )
        return

    doc = message.document
    mime = doc.mime_type or ""
    fname = doc.file_name or ""

    if not _is_accepted_image_doc(mime, fname):
        unknown_label = fname or mime or "noma'lum"
        await message.reply(
            f"⚠️ Faqat rasm fayllari qabul qilinadi (HEIC/JPEG/PNG/WebP). "
            f"Yuborilgan — <code>{html_escape(unknown_label)}</code>",
            parse_mode="HTML",
        )
        return

    if doc.file_size and doc.file_size > MAX_UPLOAD_BYTES:
        await message.reply(
            f"⚠️ Fayl juda katta ({doc.file_size // 1024 // 1024} MB). "
            f"Maksimal — {MAX_UPLOAD_BYTES // 1024 // 1024} MB.",
        )
        return

    # Download from Telegram, upload to Drive, mark item, edit message.
    progress = await message.reply("⏳ Yuklanmoqda...")

    try:
        tg_file = await message.bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{tg_file.file_path}"
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(file_url)
            resp.raise_for_status()
            file_bytes = resp.content
    except Exception as e:
        logger.exception("Failed to download Telegram file for item %s", item["id"])
        await progress.edit_text(f"❌ Telegram'dan yuklab olishda xato: {e}")
        return

    ext = _ext_from(fname, mime)
    drive_name = _build_filename(item["product_id"], item["product_name_1c"] or "", ext)

    try:
        result = gdrive_uploader.upload_bytes(file_bytes, drive_name, mime or "application/octet-stream")
    except Exception as e:
        logger.exception("Drive upload failed for item %s", item["id"])
        await progress.edit_text(f"❌ Google Drive'ga yuklashda xato: {e}")
        return

    sender_tg_id = message.from_user.id if message.from_user else 0
    pb.mark_photographed(
        item_id=item["id"],
        tg_id=sender_tg_id,
        telegram_file_id=doc.file_id,
        original_filename=fname or None,
        mime_type=mime or None,
        file_size_bytes=doc.file_size,
        drive_file_id=result.get("id"),
        drive_file_name=result.get("name"),
    )

    # Edit the original tracked message to mark photographed.
    try:
        await message.bot.edit_message_text(
            chat_id=CATALOG_GROUP_CHAT_ID,
            message_id=item["message_id"],
            text=_resolved_message_text(item, "✅"),
            parse_mode="HTML",
            reply_markup=None,
        )
    except Exception as e:
        # Non-fatal — the item is recorded, the group just sees stale state.
        logger.warning("Could not edit item message %s: %s", item["message_id"], e)

    await progress.edit_text(f"✅ Saqlandi: <code>{html_escape(drive_name)}</code>", parse_mode="HTML")

    await _maybe_advance_batch(message, item["batch_id"])


# ── skip button ───────────────────────────────────────────────────


@router.callback_query(F.data.startswith("foto_skip:"))
async def on_skip(cb: CallbackQuery):
    if not cb.message or cb.message.chat.id != CATALOG_GROUP_CHAT_ID:
        await cb.answer()
        return

    try:
        item_id = int(cb.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri callback", show_alert=False)
        return

    item = pb.find_item_by_id(item_id)
    if not item:
        await cb.answer("Mahsulot topilmadi", show_alert=False)
        return

    if item["status"] != "pending":
        await cb.answer(f"Allaqachon {item['status']}", show_alert=False)
        return

    sender_tg_id = cb.from_user.id if cb.from_user else 0
    pb.mark_skipped(item_id, sender_tg_id)

    try:
        await cb.message.edit_text(
            text=_resolved_message_text(item, "⏭"),
            parse_mode="HTML",
            reply_markup=None,
        )
    except Exception as e:
        logger.warning("Could not edit skipped item message %s: %s", cb.message.message_id, e)

    await cb.answer("Skipped")
    await _maybe_advance_batch(cb.message, item["batch_id"])


# ── batch advance ─────────────────────────────────────────────────


async def _maybe_advance_batch(anchor_message: Message, batch_id: int) -> None:
    """If the active batch is fully resolved, post the next 10 (or the
    completion message if no more products remain). Called after every
    photograph / skip.
    """
    if not pb.is_batch_complete(batch_id):
        return

    stats = pb.batch_stats(batch_id)
    bot = anchor_message.bot
    if not bot:
        return

    await bot.send_message(
        chat_id=CATALOG_GROUP_CHAT_ID,
        text=(
            f"🎉 <b>{batch_id}-batch tugadi</b> — "
            f"{stats['photographed']} foto, {stats['skipped']} skip.\n"
            f"Keyingi 10 ta yuborilmoqda..."
        ),
        parse_mode="HTML",
    )

    next_id, items, is_new = pb.start_or_resume_batch()
    if not items:
        await bot.send_message(
            chat_id=CATALOG_GROUP_CHAT_ID,
            text="✅ Hammasi tugadi — rasm yo'q mahsulotlar qolmadi (oxirgi 60 kun).",
        )
        return

    for it in items:
        sent = await bot.send_message(
            chat_id=CATALOG_GROUP_CHAT_ID,
            text=_item_message_text(it),
            parse_mode="HTML",
            reply_markup=_skip_keyboard(it["id"]),
        )
        pb.register_message_id(it["id"], sent.message_id)
        await asyncio.sleep(0.4)
