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
    """Reverse geocode lat/lng using Nominatim (OpenStreetMap).

    Priority for district: prefer MORE specific settlements (village → town →
    city_district/suburb/county) before falling back to the broader `city`
    which in Uzbekistan often returns the regional capital (e.g. "Samarqand
    shaxri") even when the actual point is in a smaller town like Chelak.
    Fix 2026-04-21 — Chelak location was mis-labeled as "Samarqand shaxri".
    """
    result = {"address": "", "region": "", "district": ""}
    try:
        import httpx
        resp = httpx.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lng, "format": "json",
                     "accept-language": "uz,ru", "zoom": 14,
                     "addressdetails": 1},
            headers={"User-Agent": "RassvetCatalogBot/1.0"},
            timeout=5,
        )
        data = resp.json()
        addr = data.get("address", {})

        # Region is the broad admin unit (виloyat)
        result["region"] = (
            addr.get("state", "")
            or addr.get("region", "")
            or ""
        )

        # District — MOST SPECIFIC first. Village/town are the local settlement;
        # city_district/suburb disambiguate within a bigger city; county/city
        # are regional fallbacks.
        result["district"] = (
            addr.get("village", "")
            or addr.get("town", "")
            or addr.get("hamlet", "")
            or addr.get("city_district", "")
            or addr.get("suburb", "")
            or addr.get("municipality", "")
            or addr.get("county", "")
            or addr.get("city", "")
            or ""
        )

        # Human-readable address: use the settlement name we picked as district,
        # then add road/house/neighbourhood for precision.
        parts = []
        settlement = result["district"]
        if settlement:
            parts.append(settlement)
        road = addr.get("road", "")
        neighbourhood = addr.get("neighbourhood", "") or addr.get("suburb", "")
        if road:
            street = road
            if addr.get("house_number"):
                street += " " + addr["house_number"]
            parts.append(street)
        elif neighbourhood and neighbourhood != settlement:
            parts.append(neighbourhood)

        result["address"] = ", ".join(parts) if parts else data.get("display_name", "")[:100]
    except Exception as e:
        logger.warning(f"Reverse geocode failed: {e}")
    return result


def _audit_insert(conn, message: Message) -> int:
    """INSERT-FIRST into location_attempts before any processing.

    Implements the zero-data-loss rule: even if downstream logic crashes,
    we have the raw lat/lng, timestamp, and full message payload on record.
    Returns the new row id so the handler can update status later.
    """
    try:
        loc = message.location
        fu = message.from_user
        ff = getattr(message, "forward_from", None)
        ffc = getattr(message, "forward_from_chat", None)
        raw_json = None
        try:
            raw_json = message.model_dump_json(exclude_none=True)
        except Exception:
            pass
        cur = conn.execute(
            """INSERT INTO location_attempts
               (telegram_id, first_name, username, chat_id, chat_type,
                latitude, longitude, is_forward, forward_from_id, forward_from_chat_id,
                raw_message_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                fu.id, fu.first_name, fu.username,
                message.chat.id, message.chat.type,
                loc.latitude, loc.longitude,
                1 if (ff or ffc) else 0,
                ff.id if ff else None,
                ffc.id if ffc else None,
                raw_json,
            ),
        )
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        logger.error(f"location audit insert failed (unusual!): {e}")
        return 0


def _audit_finalize(conn, row_id: int, *, ok: bool, error: str | None = None,
                     geocode_dict: dict | None = None,
                     is_agent: int | None = None, linked_client_id: int | None = None,
                     linked_client_1c: str | None = None) -> None:
    """Update the audit row with processing outcome."""
    if not row_id:
        return
    try:
        import json as _json
        conn.execute(
            """UPDATE location_attempts SET processed_ok=?, error_reason=?,
                 reverse_geocode_json=?, is_agent=?, linked_client_id=?, linked_client_1c=?
               WHERE id=?""",
            (1 if ok else 0, error,
             _json.dumps(geocode_dict) if geocode_dict else None,
             is_agent, linked_client_id, linked_client_1c, row_id),
        )
        conn.commit()
    except Exception as e:
        logger.error(f"location audit finalize failed: {e}")


@router.message(F.location)
async def handle_location(message: Message):
    """Handle shared location from user — save coordinates + reverse geocode.

    Accepts both user-sent locations AND forwarded locations. First action
    is ALWAYS an audit INSERT so raw data is preserved even if processing
    fails anywhere downstream.
    """
    loc = message.location
    telegram_id = message.from_user.id

    # Step 0 — durable audit (insert-first, pre-processing). Even if the
    # logic below raises, the lat/lng/timestamp/user is persisted.
    audit_conn = get_db()
    audit_id = _audit_insert(audit_conn, message)
    audit_conn.close()

    logger.info(
        f"[location] audit_id={audit_id} from tg={telegram_id} "
        f"({message.from_user.first_name}) chat_type={message.chat.type} "
        f"lat={loc.latitude} lng={loc.longitude}"
    )

    conn = get_db()
    user = conn.execute(
        "SELECT telegram_id, first_name, is_approved, is_agent, client_id "
        "FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()

    if not user:
        # Audit: raw lat/lng was preserved — we know WHO sent it so admin
        # can reach out even though we can't auto-link the location.
        _audit_finalize(conn, audit_id, ok=False,
                         error="user_not_registered")
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

    # Previous client-level GPS (for the overwrite-notification, and to know
    # whether this is a first-time tag). Read from the canonical gps_* columns
    # on allowed_clients — never from users rows, because a users row's coords
    # belong to that telegram user, not to whichever client they last tagged.
    prev_client_gps = None
    if user["client_id"]:
        prev_client_gps = conn.execute(
            "SELECT gps_latitude, gps_longitude, gps_set_by_name, gps_set_by_role "
            "FROM allowed_clients WHERE id = ?",
            (user["client_id"],),
        ).fetchone()

    had_location = bool(prev_client_gps and prev_client_gps["gps_latitude"] is not None)
    prev_setter_name = prev_client_gps["gps_set_by_name"] if prev_client_gps else None
    prev_setter_role = prev_client_gps["gps_set_by_role"] if prev_client_gps else None
    prev_lat = prev_client_gps["gps_latitude"] if prev_client_gps else None
    prev_lng = prev_client_gps["gps_longitude"] if prev_client_gps else None

    if user["client_id"]:
        ac = conn.execute(
            "SELECT client_id_1c FROM allowed_clients WHERE id = ?",
            (user["client_id"],),
        ).fetchone()
        client_1c_name = ac["client_id_1c"] if ac else ""
        conn.execute(
            "UPDATE allowed_clients SET "
            "gps_latitude = ?, gps_longitude = ?, gps_address = ?, "
            "gps_region = ?, gps_district = ?, gps_set_at = datetime('now'), "
            "gps_set_by_tg_id = ?, gps_set_by_name = ?, gps_set_by_role = ? "
            "WHERE id = ?",
            (loc.latitude, loc.longitude, geo["address"], geo["region"],
             geo["district"], telegram_id, setter_name, setter_role,
             user["client_id"]),
        )

    # The user's own users row tracks where THEY are (their personal GPS) —
    # never the coords of a client they happen to be tagging. Only update it
    # when the user is sharing their own location (no agent → client write).
    if not is_agent_linked:
        conn.execute(
            "UPDATE users SET latitude = ?, longitude = ?, location_address = ?, "
            "location_region = ?, location_district = ?, "
            "location_updated = datetime('now'), "
            "location_set_by_tg_id = ?, location_set_by_name = ?, "
            "location_set_by_role = ? WHERE telegram_id = ?",
            (loc.latitude, loc.longitude, geo["address"], geo["region"],
             geo["district"], telegram_id, setter_name, setter_role, telegram_id),
        )

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

    # Finalize audit row — success path
    try:
        fin_conn = get_db()
        _audit_finalize(
            fin_conn, audit_id, ok=True, geocode_dict=geo,
            is_agent=int(bool(user["is_agent"])) if user else 0,
            linked_client_id=user["client_id"] if user else None,
            linked_client_1c=client_1c_name or None,
        )
        fin_conn.close()
    except Exception as e:
        logger.error(f"audit finalize (success path) failed: {e}")


