"""Location sharing handler — saves GPS coordinates + reverse geocode.

When an agent is /testclient-linked to a client, GPS is saved on the
CLIENT's profile, so agents can tag client shop locations on the spot.
"""
import os
import logging

from aiogram import Router, F
from aiogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    WebAppInfo,
)

from bot.shared import get_db, html_escape, BOT_TOKEN, WEBAPP_URL

logger = logging.getLogger("bot")
router = Router(name="location")


def _reverse_geocode(lat: float, lng: float) -> dict:
    """Reverse geocode lat/lng using Nominatim (OpenStreetMap)."""
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

        result["region"] = addr.get("state", "")
        result["district"] = addr.get("county", "") or addr.get("city", "") or addr.get("town", "") or addr.get("village", "")

        parts = []
        city = addr.get("city", "") or addr.get("town", "") or addr.get("village", "")
        if city:
            parts.append(city)
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


@router.message(F.location)
async def handle_location(message: Message):
    """Handle shared location from user — save coordinates + reverse geocode."""
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

    is_agent_linked = user["is_agent"] and user["client_id"]
    client_1c_name = ""
    setter_name = user["first_name"] or str(telegram_id)
    setter_role = "agent" if is_agent_linked else "client"

    target_tg = telegram_id
    if is_agent_linked:
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
        conn.execute(f"UPDATE users SET {loc_tracking} WHERE telegram_id = ?",
                     loc_params + (telegram_id,))
        conn.execute(
            "UPDATE allowed_clients SET location = ? WHERE id = ?",
            (f"{loc.latitude},{loc.longitude}|{geo['address'] or ''}", user["client_id"]),
        )
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
