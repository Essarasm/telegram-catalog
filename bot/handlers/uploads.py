"""Upload & data import bot commands — extracted from bot/main.py.

Handles: /prices, /stock, /clients, /supply, /balances, /debtors,
/realorders, /cash, /fxrate, /syncimages, /catalog, /today, /missing,
/bulksupply, /clientmaster, /unmatchedclients, /unmatchedproducts,
/relinkrealorders, /ingestskus, /realordersample, /duplicateclients,
and all caption-based document handlers.
"""
import os
import re
import json
from pathlib import Path

from aiogram import Router, F, types
from aiogram.filters import Command

from bot.shared import (
    get_db, html_escape, is_admin, logger, BOT_TOKEN, _BASE_URL,
    track_daily_upload, extract_snapshot_date, sender_display_name,
)

router = Router()


@router.message(Command("prices"))
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

        track_daily_upload(
            "prices",
            message,
            file_name=doc.file_name,
            row_count=int(result.get("excel_products") or 0),
            upload_date=extract_snapshot_date(message),
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


@router.message(Command("syncimages"))
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
    from pathlib import Path

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


@router.message(F.document & F.caption.startswith("/syncimages"))
async def handle_syncimages_document(message: types.Message):
    """Handle ZIP/image file sent with /syncimages as caption."""
    if not is_admin(message):
        return
    await cmd_syncimages(message)


@router.message(Command("stock"))
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

        track_daily_upload(
            "stock",
            message,
            file_name=doc.file_name,
            row_count=int(result.get("excel_products") or 0),
            upload_date=extract_snapshot_date(message),
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

        alias_hits = result.get('alias_hits', 0)
        auto_learned = result.get('auto_learned', 0)
        if alias_hits > 0 or auto_learned > 0:
            lines.append(f"\n🔗 Alias: {alias_hits} ta tezkor topildi, {auto_learned} ta yangi o'rganildi")

        unmatched = result.get('unmatched_count', 0)
        if unmatched > 0:
            lines.append(f"\n⚠️ {unmatched} ta Excel mahsulot bazada topilmadi")
            unmatched_names = result.get('unmatched_names', [])
            if unmatched_names:
                for un in unmatched_names[:5]:
                    lines.append(f"  ❓ {html_escape(un)}")
                if len(unmatched_names) > 5:
                    lines.append(f"  ... va yana {len(unmatched_names) - 5} ta")

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Stock update error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:200]}")


@router.message(Command("catalog"))
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


@router.message(Command("balances"))
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
                    track_daily_upload(
                        "balances_uzs", message,
                        file_name=doc.file_name,
                        row_count=int(sec.get("clients") or 0),
                    )
                    tracked_any = True
                elif cur == "USD":
                    track_daily_upload(
                        "balances_usd", message,
                        file_name=doc.file_name,
                        row_count=int(sec.get("clients") or 0),
                    )
                    tracked_any = True
            if not tracked_any:
                # Fallback: older/degenerate result shape without sections —
                # default to UZS so we at least record something.
                track_daily_upload(
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



@router.message(F.document & F.caption.startswith("/prices"))
async def handle_prices_document(message: types.Message):
    """Handle Excel file sent with /prices as caption."""
    if not is_admin(message):
        return
    await cmd_prices(message)


@router.message(F.document & F.caption.startswith("/stock"))
async def handle_stock_document(message: types.Message):
    """Handle Excel file sent with /stock as caption."""
    if not is_admin(message):
        return
    await cmd_stock(message)


@router.message(F.document & F.caption.startswith("/catalog"))
async def handle_catalog_document(message: types.Message):
    """Handle Excel file sent with /catalog as caption."""
    if not is_admin(message):
        return
    await cmd_catalog(message)


@router.message(F.document & F.caption.startswith("/balances"))
async def handle_balances_document(message: types.Message):
    """Handle XLS file sent with /balances as caption."""
    if not is_admin(message):
        return
    await cmd_balances(message)



@router.message(F.media_group_id & F.document)
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



@router.message(Command("clients"))
async def cmd_clients(message: types.Message):
    """Upload the allowed-clients list (XLS/XLSX).

    Usage: send an Excel file with columns phone, name, client_id_1c,
    company_name (all except phone optional), with /clients as caption,
    OR reply to the file with /clients.
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
            "Mijozlar ro'yxati Excel faylini /clients caption bilan yuboring.\n\n"
            "Ustunlar: <code>phone, name, client_id_1c, company_name</code>",
            parse_mode="HTML",
        )
        return

    if not doc.file_name or not doc.file_name.lower().endswith(('.xls', '.xlsx')):
        await message.reply("❌ Faqat Excel (.xls/.xlsx) fayllar qabul qilinadi.")
        return

    status_msg = await message.reply("⏳ Mijozlar ro'yxati yuklanmoqda...")

    try:
        import httpx
        file = await bot.get_file(doc.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(file_url)
            file_bytes = resp.content

        api_url = f"{_BASE_URL}/api/finance/import-clients"
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                api_url,
                files={"file": (doc.file_name, file_bytes,
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                data={"admin_key": "rassvet2026"},
            )
            result = resp.json()

        if not result.get("ok"):
            await status_msg.edit_text(f"❌ Xatolik: {result.get('error', 'Unknown')}")
            return

        inserted = int(result.get("inserted") or 0)
        updated = int(result.get("updated") or 0)
        skipped = int(result.get("skipped") or 0)

        if inserted or updated:
            track_daily_upload(
                "clients", message,
                file_name=doc.file_name, row_count=inserted + updated,
            )

        lines = [f"✅ <b>Mijozlar ro'yxati yangilandi!</b>", ""]
        lines.append(f"➕ Yangi: {inserted}")
        lines.append(f"🔄 Yangilangan: {updated}")
        lines.append(f"⏭ O'tkazib yuborildi: {skipped}")
        lines.append(f"📊 Jami ro'yxatda: {result.get('total_clients', 0)}")

        # If zero rows matched a phone column, surface the headers so we
        # can tell which column names your Excel actually uses.
        if not result.get("phone_column_detected") and not (inserted or updated):
            headers = result.get("headers_seen") or []
            header_preview = ", ".join([h for h in headers if str(h).strip()][:15])
            lines.append("")
            lines.append("⚠️ <b>Telefon ustuni topilmadi.</b>")
            lines.append(f"Fayldagi sarlavhalar: <code>{html_escape(header_preview)}</code>")
            lines.append(
                "Kerakli ustunlar: <code>phone, name, client_id_1c, company_name</code>. "
                "Sarlavhalarni ushbu xabarga javob qilib yuboring — kerakli moslashtirishlarni qo'shamiz."
            )

        await status_msg.edit_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"/clients error: {e}")
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:200]}")


@router.message(F.document & F.caption.startswith("/clients"))
async def handle_clients_document(message: types.Message):
    """Handle XLS/XLSX file sent with /clients as caption."""
    if not is_admin(message):
        return
    await cmd_clients(message)


@router.message(Command("debtors"))
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
        snapshot_date = result.get("report_date") or extract_snapshot_date(message)
        track_daily_upload(
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


@router.message(F.document & F.caption.startswith("/debtors"))
async def handle_debtors_document(message: types.Message):
    """Handle XLS file sent with /debtors as caption."""
    if not is_admin(message):
        return
    await cmd_debtors(message)



@router.message(Command("realordersstats"))
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



@router.message(Command("realorders"))
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

        track_daily_upload(
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


@router.message(F.document & F.caption.startswith("/realorders"))
async def handle_realorders_document(message: types.Message):
    """Handle XLS/XLSX file sent with /realorders as caption."""
    if not is_admin(message):
        return
    await cmd_realorders(message)



@router.message(Command("cash"))
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

        track_daily_upload(
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


@router.message(F.document & F.caption.startswith("/cash"))
async def handle_cash_document(message: types.Message):
    """Handle XLS/XLSX file sent with /cash as caption."""
    if not is_admin(message):
        return
    await cmd_cash(message)


@router.message(Command("fxrate"))
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
        user_name = sender_display_name(message)
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


@router.message(Command("today"))
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


@router.message(Command("missing"))
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



@router.message(Command("supply"))
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
        track_daily_upload(
            "supply", message, file_name=fname,
            row_count=ins + upd,
            notes=f"supply={supply_n} return={return_n} adj={adj_n} items={total_items}",
        )
    except Exception as e:
        logger.error(f"/supply error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Xatolik: {str(e)[:300]}")


@router.message(F.document & F.caption.startswith("/supply"))
async def handle_supply_document(message: types.Message):
    """Handle XLS uploads with /supply as caption."""
    await cmd_supply(message)


@router.message(Command("bulksupply"))
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


@router.message(Command("unmatchedclients"))
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


@router.message(Command("unmatchedproducts"))
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


@router.message(Command("relinkrealorders"))
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


@router.message(Command("ingestskus"))
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



@router.message(Command("realordersample"))
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



@router.message(Command("duplicateclients"))
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



@router.message(Command("clientmaster"))
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


@router.message(F.document & F.caption.startswith("/clientmaster"))
async def handle_clientmaster_document(message: types.Message):
    """Handle XLSX file sent with /clientmaster as caption."""
    if not is_admin(message):
        return
    await cmd_clientmaster(message)



