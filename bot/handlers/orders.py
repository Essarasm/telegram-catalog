"""Order-related handlers: wish-list wipe, order confirmation reply, skip upload.

Handles the Sotuv bo'lim reply-with-Excel flow for confirmed orders,
/wipewishlists destructive cleanup, and /skipupload daily-upload management.
"""
import re
import logging

from html import escape as _h

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message

from bot.shared import (
    get_db, html_escape, is_admin, sender_display_name,
    BOT_TOKEN, ORDER_GROUP_CHAT_ID,
)

logger = logging.getLogger("bot")
router = Router(name="orders")


# ── /wipewishlists ──────────────────────────────────────────────────

@router.message(Command("wipewishlists"))
async def cmd_wipewishlists(message: Message):
    """Wipe all wish-list data. Safety: dry-run without CONFIRM keyword."""
    if not is_admin(message):
        return

    parts = (message.text or "").strip().split(maxsplit=1)
    token = parts[1].strip().upper() if len(parts) > 1 else ""
    is_confirmed = (token == "CONFIRM")

    conn = get_db()
    try:
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
            conn.execute(
                "DELETE FROM sqlite_sequence WHERE name IN "
                "('orders','order_items','product_requests','demand_signals')"
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

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


# ── Order confirmation reply (Sotuv bo'lim) ─────────────────────────

@router.message(F.document & (F.chat.id == ORDER_GROUP_CHAT_ID) & F.reply_to_message)
async def handle_order_confirmation_reply(message: Message):
    """Manager replies to a 'Yangi buyurtma #N' message with 1C-exported Excel."""
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
        return

    wishlist_order_id = row["id"]
    client_tg_id = row["telegram_id"]

    status_msg = await message.reply("⏳ Tasdiqlangan buyurtma yuklanmoqda...")
    try:
        import httpx
        from backend.services.import_real_orders import parse_real_orders_xls
        from backend.database import get_db as _get_db

        file = await message.bot.get_file(doc.file_id)
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
        uploader = sender_display_name(message)
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


# ── /skipupload ─────────────────────────────────────────────────────

@router.message(Command("skipupload"))
async def cmd_skipupload(message: Message):
    """Mark a specific upload (or all for a date) as skipped."""
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
