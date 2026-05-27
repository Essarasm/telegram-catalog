"""Location sharing handler — saves GPS coordinates + reverse geocode.

When an agent is /testclient-linked to a client, GPS is saved on the
CLIENT's profile, so agents can tag client shop locations on the spot.
"""
import logging

from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    WebAppInfo,
)

from bot.shared import get_db, html_escape, BOT_TOKEN, WEBAPP_URL, DRIVER_GROUP_CHAT_ID
from backend.services.location_display import backfill_text_from_gps

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


def _frozen_fix_prior(conn, telegram_id: int, before_audit_id: int,
                      client_id: int, lat: float, lng: float):
    """Detect a stale/frozen GPS fix being re-sent across consecutive shops.

    Returns the agent's immediately-prior `location_attempts` row when the
    incoming pin is bit-identical to it BUT was linked to a DIFFERENT client
    — the signature of a phone returning a cached fix while the agent tags
    several shops in a row (2026-05-01 Juma incident: one stuck fix landed on
    ШУХРАТ, Санжар and ХУРШИД). Returns None when there's no such match, so
    legitimate first pins and same-client self-corrections pass through.

    Scoped to the last 2h so it only fires within one field session; an exact
    six-decimal coincidence across days is astronomically unlikely anyway.
    """
    if not before_audit_id:
        return None
    row = conn.execute(
        "SELECT latitude, longitude, linked_client_id, linked_client_1c "
        "FROM location_attempts "
        "WHERE telegram_id = ? AND id < ? AND latitude IS NOT NULL "
        "AND linked_client_id IS NOT NULL "
        "AND received_at >= datetime('now', '-2 hours') "
        "ORDER BY id DESC LIMIT 1",
        (telegram_id, before_audit_id),
    ).fetchone()
    if not row or row["linked_client_id"] == client_id:
        return None
    if (abs((row["latitude"] or 0.0) - lat) < 1e-6
            and abs((row["longitude"] or 0.0) - lng) < 1e-6):
        return row
    return None


def _notify_manzillar(*, client_label: str, setter_name: str, setter_role: str,
                      setter_tg: int, lat: float, lng: float,
                      prev_lat=None, prev_lng=None, prev_setter_name=None,
                      prev_setter_role=None, is_renewal: bool = False) -> None:
    """Post an agent/driver client-location set to the MANZILLAR (driver) group,
    so mini-app/DM sets are as visible as in-group `/lokatsiya` pins (user
    request 2026-05-27). Best-effort — a failed send never breaks the save."""
    try:
        import httpx as _httpx
        from bot.shared import DRIVER_GROUP_CHAT_ID
        if not DRIVER_GROUP_CHAT_ID:
            return
        new_maps = f"https://maps.google.com/?q={lat},{lng}"
        header = ("♻️ <b>Mijoz lokatsiyasi yangilandi</b>" if is_renewal
                  else "✅ <b>Mijoz lokatsiyasi o'rnatildi</b>")
        lines = [
            header, "",
            f"🧾 Mijoz: <b>{html_escape(client_label)}</b>",
            f"👤 O'rnatdi: {html_escape(setter_name)} ({setter_role})",
            f"🆔 TG ID: <code>{setter_tg}</code> · 📲 Mini App",
        ]
        if is_renewal and prev_lat:
            prev_maps = f"https://maps.google.com/?q={prev_lat},{prev_lng}"
            lines.append(
                f"📍 Avvalgi: {html_escape(prev_setter_name or '—')} "
                f"({html_escape(prev_setter_role or '—')}) — "
                f"<a href=\"{prev_maps}\">eski joylashuv</a>"
            )
        lines.append(f"📍 <a href=\"{new_maps}\">Yangi joylashuv</a>")
        _httpx.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": DRIVER_GROUP_CHAT_ID, "text": "\n".join(lines),
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"Failed to notify MANZILLAR location set: {e}")


@router.message(F.location)
async def handle_location(message: Message):
    """Handle shared location from user — save coordinates + reverse geocode.

    Accepts both user-sent locations AND forwarded locations. First action
    is ALWAYS an audit INSERT so raw data is preserved even if processing
    fails anywhere downstream.
    """
    # Driver group has its own dedicated FSM-based handler in
    # bot/handlers/driver_location.py with an explicit client picker.
    # Skip here so we don't double-process pins sent inside that group.
    if DRIVER_GROUP_CHAT_ID and message.chat.id == DRIVER_GROUP_CHAT_ID:
        return

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
            "SELECT name, client_id_1c, gps_latitude, gps_longitude, gps_address, "
            "gps_region, gps_district, gps_set_at, gps_set_by_tg_id, "
            "gps_set_by_name, gps_set_by_role "
            "FROM allowed_clients WHERE id = ?",
            (user["client_id"],),
        ).fetchone()

    had_location = bool(prev_client_gps and prev_client_gps["gps_latitude"] is not None)
    prev_setter_name = prev_client_gps["gps_set_by_name"] if prev_client_gps else None
    prev_setter_role = prev_client_gps["gps_set_by_role"] if prev_client_gps else None
    prev_lat = prev_client_gps["gps_latitude"] if prev_client_gps else None
    prev_lng = prev_client_gps["gps_longitude"] if prev_client_gps else None

    # ── Frozen-GPS guard (agent-tagging-client path only) ───────────────
    # If this exact coordinate was the agent's previous submission for a
    # DIFFERENT client, the phone is likely returning a stale/cached fix.
    # Hold the write and ask for confirmation instead of silently planting a
    # stale pin on another client (2026-05-01 Juma incident, Error Log #68).
    # The agent can override for the rare two-shops-one-spot case.
    if is_agent_linked and user["client_id"]:
        frozen_prior = _frozen_fix_prior(
            conn, telegram_id, audit_id, user["client_id"],
            loc.latitude, loc.longitude,
        )
        if frozen_prior is not None:
            held_1c = (prev_client_gps["client_id_1c"] if prev_client_gps else "") or ""
            _audit_finalize(
                conn, audit_id, ok=False, error="stale_fix_held_for_confirm",
                geocode_dict=geo, is_agent=1,
                linked_client_id=user["client_id"], linked_client_1c=held_1c,
            )
            conn.close()
            prev_label = frozen_prior["linked_client_1c"] or "oldingi mijoz"
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Ha, shu yerda — saqla",
                                     callback_data=f"locok:{audit_id}"),
                InlineKeyboardButton(text="❌ Yo'q",
                                     callback_data=f"locno:{audit_id}"),
            ]])
            await message.answer(
                "⚠️ <b>Diqqat — GPS qotib qolgan bo'lishi mumkin</b>\n\n"
                f"Bu nuqta avvalgi mijoz (<b>{html_escape(prev_label)}</b>) bilan "
                "<b>aynan bir xil</b>. Telefon eski joylashuvni qaytarayotgan "
                "bo'lishi mumkin.\n\n"
                "📵 10–20 soniya kuting va lokatsiyani <b>qaytadan</b> yuboring.\n"
                "Agar ikkala do'kon <b>haqiqatan ham bir joyda</b> bo'lsa — "
                "«Ha, shu yerda» tugmasini bosing.",
                parse_mode="HTML", reply_markup=kb,
            )
            return

    if user["client_id"]:
        client_1c_name = prev_client_gps["client_id_1c"] if prev_client_gps else ""
        # Auto-snapshot the prior pin BEFORE overwrite so any accidental
        # overwrite (stale users.client_id, wrong agent, fat-fingered share)
        # is reversible via /api/locations/restore-pin. The location_attempts
        # audit row preserves the raw incoming lat/lng on the OTHER side;
        # this snapshot preserves what we're about to clobber on this side.
        if had_location:
            import json as _json
            snap_args = _json.dumps({
                "client_id": user["client_id"],
                "client_name": prev_client_gps["name"],
                "client_id_1c": prev_client_gps["client_id_1c"],
                "prior_gps_latitude": prev_client_gps["gps_latitude"],
                "prior_gps_longitude": prev_client_gps["gps_longitude"],
                "prior_gps_address": prev_client_gps["gps_address"],
                "prior_gps_region": prev_client_gps["gps_region"],
                "prior_gps_district": prev_client_gps["gps_district"],
                "prior_gps_set_at": prev_client_gps["gps_set_at"],
                "prior_gps_set_by_tg_id": prev_client_gps["gps_set_by_tg_id"],
                "prior_gps_set_by_name": prev_client_gps["gps_set_by_name"],
                "prior_gps_set_by_role": prev_client_gps["gps_set_by_role"],
                "overwritten_by_tg_id": telegram_id,
                "overwritten_by_name": setter_name,
                "overwritten_by_role": setter_role,
                "overwritten_with_lat": loc.latitude,
                "overwritten_with_lng": loc.longitude,
            }, ensure_ascii=False)
            try:
                conn.execute(
                    "INSERT INTO admin_action_log (telegram_id, user_name, command, args) "
                    "VALUES (?, ?, ?, ?)",
                    (telegram_id, setter_name, "auto_overwrite_snapshot", snap_args),
                )
            except Exception as e:
                logger.error(f"auto_overwrite_snapshot failed (non-fatal): {e}")

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
        backfill_text_from_gps(conn, user["client_id"], geo)

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

    if is_agent_linked and client_1c_name:
        # Agent/driver set a client's location via the mini-app/DM flow — surface
        # it in MANZILLAR (driver group) alongside in-group /lokatsiya pins, on
        # BOTH first-time set and renewal (user request 2026-05-27). Previously
        # this only fired on overwrite and went to PLATFORM_OPS (commit 6ef9224).
        _notify_manzillar(
            client_label=client_1c_name, setter_name=setter_name,
            setter_role=setter_role, setter_tg=telegram_id,
            lat=loc.latitude, lng=loc.longitude,
            prev_lat=prev_lat, prev_lng=prev_lng,
            prev_setter_name=prev_setter_name, prev_setter_role=prev_setter_role,
            is_renewal=had_location,
        )
    elif had_location:
        # Non-agent overwrite (a client re-pinning their own shop) — keep the
        # existing audit alert to PLATFORM_OPS (2026-05-16 admin/ops split).
        try:
            import httpx as _httpx
            from bot.shared import PLATFORM_OPS_GROUP_CHAT_ID
            target_chat = PLATFORM_OPS_GROUP_CHAT_ID
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
                json={"chat_id": target_chat, "text": "\n".join(lines),
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


# ── Frozen-GPS guard: confirm / reject a held pin ────────────────────────

@router.callback_query(F.data.startswith("locok:"))
async def cb_confirm_stale_location(cb: CallbackQuery):
    """Agent confirms a held (frozen-fix-suspected) pin is genuinely correct
    — write it through. The held `location_attempts` row carries the raw
    lat/lng + client linkage + reverse geocode, so we recover everything from
    it (no state to stash between the warning and this tap)."""
    try:
        audit_id = int(cb.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await cb.answer()
        return
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT telegram_id, first_name, latitude, longitude, linked_client_id, "
            "linked_client_1c, reverse_geocode_json FROM location_attempts WHERE id = ?",
            (audit_id,),
        ).fetchone()
        if not row or row["linked_client_id"] is None or row["latitude"] is None:
            await cb.answer("Eskirgan tugma", show_alert=True)
            return
        if cb.from_user.id != row["telegram_id"]:
            await cb.answer("Faqat yuboruvchi tasdiqlay oladi", show_alert=True)
            return

        import json as _json
        try:
            g = _json.loads(row["reverse_geocode_json"]) if row["reverse_geocode_json"] else {}
        except Exception:
            g = {}
        geo = {"address": g.get("address", ""), "region": g.get("region", ""),
               "district": g.get("district", "")}
        client_id = row["linked_client_id"]
        setter_name = row["first_name"] or str(row["telegram_id"])
        lat, lng = row["latitude"], row["longitude"]

        # Snapshot any prior pin BEFORE overwrite so the confirmed write stays
        # reversible via /api/locations/restore-pin (same shape as the main path).
        prior = conn.execute(
            "SELECT name, client_id_1c, gps_latitude, gps_longitude, gps_address, "
            "gps_region, gps_district, gps_set_at, gps_set_by_tg_id, gps_set_by_name, "
            "gps_set_by_role FROM allowed_clients WHERE id = ?",
            (client_id,),
        ).fetchone()
        if prior and prior["gps_latitude"] is not None:
            snap_args = _json.dumps({
                "client_id": client_id, "client_name": prior["name"],
                "client_id_1c": prior["client_id_1c"],
                "prior_gps_latitude": prior["gps_latitude"],
                "prior_gps_longitude": prior["gps_longitude"],
                "prior_gps_address": prior["gps_address"],
                "prior_gps_region": prior["gps_region"],
                "prior_gps_district": prior["gps_district"],
                "prior_gps_set_at": prior["gps_set_at"],
                "prior_gps_set_by_tg_id": prior["gps_set_by_tg_id"],
                "prior_gps_set_by_name": prior["gps_set_by_name"],
                "prior_gps_set_by_role": prior["gps_set_by_role"],
                "overwritten_by_tg_id": row["telegram_id"],
                "overwritten_by_name": setter_name, "overwritten_by_role": "agent",
                "overwritten_with_lat": lat, "overwritten_with_lng": lng,
                "snapshot_source": "frozen_fix_confirm",
            }, ensure_ascii=False)
            try:
                conn.execute(
                    "INSERT INTO admin_action_log (telegram_id, user_name, command, args) "
                    "VALUES (?, ?, ?, ?)",
                    (row["telegram_id"], setter_name, "auto_overwrite_snapshot", snap_args),
                )
            except Exception as e:
                logger.error(f"frozen_fix_confirm snapshot failed (non-fatal): {e}")

        conn.execute(
            "UPDATE allowed_clients SET gps_latitude = ?, gps_longitude = ?, "
            "gps_address = ?, gps_region = ?, gps_district = ?, "
            "gps_set_at = datetime('now'), gps_set_by_tg_id = ?, gps_set_by_name = ?, "
            "gps_set_by_role = 'agent' WHERE id = ?",
            (lat, lng, geo["address"], geo["region"], geo["district"],
             row["telegram_id"], setter_name, client_id),
        )
        backfill_text_from_gps(conn, client_id, geo)
        _audit_finalize(conn, audit_id, ok=True, geocode_dict=geo, is_agent=1,
                        linked_client_id=client_id, linked_client_1c=row["linked_client_1c"])
        conn.commit()
        label = row["linked_client_1c"] or ""
        notify = dict(
            client_label=label or setter_name, setter_name=setter_name,
            setter_role="agent", setter_tg=row["telegram_id"], lat=lat, lng=lng,
            prev_lat=(prior["gps_latitude"] if prior else None),
            prev_lng=(prior["gps_longitude"] if prior else None),
            prev_setter_name=(prior["gps_set_by_name"] if prior else None),
            prev_setter_role=(prior["gps_set_by_role"] if prior else None),
            is_renewal=bool(prior and prior["gps_latitude"] is not None),
        )
    finally:
        conn.close()
    await cb.answer("Saqlandi")
    try:
        await cb.message.edit_text(
            f"✅ <b>{html_escape(label)}</b> uchun lokatsiya tasdiqlandi va saqlandi.",
            parse_mode="HTML",
        )
    except Exception:
        pass
    # Confirmed agent set — surface it in MANZILLAR like any other set.
    _notify_manzillar(**notify)


@router.callback_query(F.data.startswith("locno:"))
async def cb_reject_stale_location(cb: CallbackQuery):
    """Agent rejects the held pin — they'll wait and resend a fresh one."""
    await cb.answer()
    try:
        await cb.message.edit_text(
            "❌ Bekor qilindi. Iltimos, 10–20 soniya kuting va lokatsiyani "
            "qaytadan yuboring.",
        )
    except Exception:
        pass


