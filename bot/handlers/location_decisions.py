"""Per-client location-decision queue — admin pick between prior and incoming pin.

When an agent/driver sends a pin >100m from the existing one, the write path
(driver_location.py / location.py) does NOT overwrite. It calls
`dispatch_location_decision()` here, which inserts a `pending_location_decisions`
row and posts a comparison message to AGENT_APPROVAL_GROUP_CHAT_ID with
[✓ Eski qoldirilsin] [↻ Yangisi olinsin] buttons. Admin taps; this module's
callback handlers commit the decision.

Origin: Session M 2026-05-28 (Bektimur's two blocked attempts; user request
to replace hard-block / silent-overwrite with admin pick on divergence).
"""
from __future__ import annotations

import json
import logging

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from bot.shared import (
    get_db,
    html_escape,
    BOT_TOKEN,
    AGENT_APPROVAL_GROUP_CHAT_ID,
    is_admin_cb,
    sender_display_name,
)
from backend.services.location_display import backfill_text_from_gps

logger = logging.getLogger("bot")
router = Router(name="location_decisions")


def _yandex_url(lat: float, lng: float) -> str:
    return f"https://yandex.uz/maps/?rtext=~{lat},{lng}&rtt=auto"


def _tashkent_str(utc_str: str | None) -> str:
    """Convert a UTC 'YYYY-MM-DD HH:MM:SS' string to Tashkent (UTC+5) display."""
    if not utc_str:
        return "—"
    try:
        from datetime import datetime, timedelta
        dt = datetime.fromisoformat(utc_str.replace(" ", "T"))
        return (dt + timedelta(hours=5)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return utc_str


def _comparison_text(*, client_name: str, client_id_1c: str | None,
                     client_id: int, distance_m: float,
                     prior_lat: float, prior_lng: float, prior_address: str | None,
                     prior_region: str | None, prior_district: str | None,
                     prior_set_at: str | None, prior_set_by_name: str | None,
                     prior_set_by_role: str | None,
                     incoming_lat: float, incoming_lng: float,
                     incoming_address: str | None, incoming_region: str | None,
                     incoming_district: str | None,
                     incoming_by_name: str | None, incoming_by_role: str | None,
                     source_path: str) -> str:
    """Render the comparison message body shown in the agent-approval group."""
    label = client_id_1c or client_name or f"ID {client_id}"
    prior_addr_bits = [p for p in (prior_address, prior_district, prior_region) if p]
    new_addr_bits = [p for p in (incoming_address, incoming_district, incoming_region) if p]
    source_label = {
        "driver_lokatsiya": "Manzillar /lokatsiya",
        "mini_app_dm": "Mini App",
    }.get(source_path, source_path)
    return (
        "🔀 <b>Lokatsiya taqqoslash kerak</b>\n"
        f"\n🧾 Mijoz: <b>{html_escape(label)}</b> (id={client_id})\n"
        f"📏 Masofa: <b>{distance_m:.0f} m</b>\n"
        f"📲 Manba: {html_escape(source_label)}\n"
        "\n📍 <b>Eski</b> (joriy)\n"
        f"   👤 {html_escape(prior_set_by_name or '—')} "
        f"({html_escape(prior_set_by_role or '—')}) — {_tashkent_str(prior_set_at)}\n"
        f"   🏷 {html_escape(', '.join(prior_addr_bits) or '—')}\n"
        f"   🗺 <a href=\"{_yandex_url(prior_lat, prior_lng)}\">xaritada</a>\n"
        "\n📍 <b>Yangi</b> (kelgan)\n"
        f"   👤 {html_escape(incoming_by_name or '—')} "
        f"({html_escape(incoming_by_role or '—')})\n"
        f"   🏷 {html_escape(', '.join(new_addr_bits) or '—')}\n"
        f"   🗺 <a href=\"{_yandex_url(incoming_lat, incoming_lng)}\">xaritada</a>"
    )


def _decision_keyboard(pld_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✓ Eski qoldirilsin",
                             callback_data=f"locdec:keep:{pld_id}"),
        InlineKeyboardButton(text="↻ Yangisi olinsin",
                             callback_data=f"locdec:use:{pld_id}"),
    ]])


def dispatch_location_decision(conn, *, client_id: int, client_name: str,
                                client_id_1c: str | None,
                                prior_row: dict,
                                incoming_lat: float, incoming_lng: float,
                                incoming_geo: dict,
                                incoming_by_tg_id: int, incoming_by_name: str,
                                incoming_by_role: str,
                                incoming_attempt_id: int,
                                distance_m: float,
                                source_path: str) -> int | None:
    """Insert a pending_location_decisions row + post comparison message to the
    agent-approval group. Returns the new pld_id, or None on failure.

    Best-effort: a failed Telegram send leaves the row in 'pending' status
    with dispatched_message_id=NULL, so admin can resurface via a future sweep.
    """
    try:
        cur = conn.execute(
            "INSERT INTO pending_location_decisions "
            "(client_id, client_name, client_id_1c, "
            " prior_lat, prior_lng, prior_address, prior_region, prior_district, "
            " prior_set_at, prior_set_by_tg_id, prior_set_by_name, prior_set_by_role, "
            " incoming_lat, incoming_lng, incoming_address, incoming_region, "
            " incoming_district, incoming_by_tg_id, incoming_by_name, incoming_by_role, "
            " incoming_attempt_id, distance_m, source_path) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (client_id, client_name, client_id_1c,
             prior_row["gps_latitude"], prior_row["gps_longitude"],
             prior_row["gps_address"], prior_row["gps_region"],
             prior_row["gps_district"], prior_row["gps_set_at"],
             prior_row["gps_set_by_tg_id"], prior_row["gps_set_by_name"],
             prior_row["gps_set_by_role"],
             incoming_lat, incoming_lng, incoming_geo.get("address"),
             incoming_geo.get("region"), incoming_geo.get("district"),
             incoming_by_tg_id, incoming_by_name, incoming_by_role,
             incoming_attempt_id, distance_m, source_path),
        )
        pld_id = cur.lastrowid
        conn.commit()
    except Exception as e:
        logger.error(f"pending_location_decisions INSERT failed: {e}")
        return None

    text = _comparison_text(
        client_name=client_name, client_id_1c=client_id_1c, client_id=client_id,
        distance_m=distance_m,
        prior_lat=prior_row["gps_latitude"], prior_lng=prior_row["gps_longitude"],
        prior_address=prior_row["gps_address"], prior_region=prior_row["gps_region"],
        prior_district=prior_row["gps_district"], prior_set_at=prior_row["gps_set_at"],
        prior_set_by_name=prior_row["gps_set_by_name"],
        prior_set_by_role=prior_row["gps_set_by_role"],
        incoming_lat=incoming_lat, incoming_lng=incoming_lng,
        incoming_address=incoming_geo.get("address"),
        incoming_region=incoming_geo.get("region"),
        incoming_district=incoming_geo.get("district"),
        incoming_by_name=incoming_by_name, incoming_by_role=incoming_by_role,
        source_path=source_path,
    )

    if not AGENT_APPROVAL_GROUP_CHAT_ID or not BOT_TOKEN:
        return pld_id

    try:
        import httpx
        kb = _decision_keyboard(pld_id)
        resp = httpx.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": AGENT_APPROVAL_GROUP_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "reply_markup": kb.model_dump(exclude_none=True),
            },
            timeout=10,
        )
        data = resp.json()
        if data.get("ok"):
            msg_id = data["result"]["message_id"]
            conn.execute(
                "UPDATE pending_location_decisions SET dispatched_chat_id=?, "
                "dispatched_message_id=? WHERE id=?",
                (AGENT_APPROVAL_GROUP_CHAT_ID, msg_id, pld_id),
            )
            conn.commit()
        else:
            logger.error(f"locdec dispatch sendMessage not ok: {data}")
    except Exception as e:
        logger.error(f"locdec dispatch sendMessage failed: {e}")

    return pld_id


# ── Callback handlers ─────────────────────────────────────────────────

def _load_pending(conn, pld_id: int):
    return conn.execute(
        "SELECT * FROM pending_location_decisions WHERE id=?",
        (pld_id,),
    ).fetchone()


def _decided_footer(verdict: str, admin_name: str) -> str:
    from datetime import datetime, timedelta
    ts = (datetime.utcnow() + timedelta(hours=5)).strftime("%Y-%m-%d %H:%M")
    icon = "✅" if verdict == "keep_old" else "↻"
    label = "Eski qoldirildi" if verdict == "keep_old" else "Yangisi olindi"
    return f"\n\n{icon} <b>{label}</b> — {html_escape(admin_name)} · {ts}"


@router.callback_query(F.data.startswith("locdec:keep:"))
async def cb_keep_old(cb: CallbackQuery):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=True)
        return
    try:
        pld_id = int(cb.data.split(":", 2)[2])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri tanlov", show_alert=True)
        return
    conn = get_db()
    try:
        row = _load_pending(conn, pld_id)
        if not row:
            await cb.answer("Topilmadi (eski tugma bo'lishi mumkin)", show_alert=True)
            return
        if row["status"] != "pending":
            await cb.answer(f"Allaqachon hal qilingan: {row['status']}", show_alert=True)
            return
        admin_name = sender_display_name(cb)
        conn.execute(
            "UPDATE pending_location_decisions SET status='keep_old', "
            "decided_at=datetime('now'), decided_by_tg_id=?, decided_by_name=? "
            "WHERE id=?",
            (cb.from_user.id, admin_name, pld_id),
        )
        conn.commit()
    finally:
        conn.close()
    await cb.answer("Eski qoldirildi")
    try:
        await cb.message.edit_text(
            (cb.message.html_text or cb.message.text or "")
            + _decided_footer("keep_old", admin_name),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.warning(f"locdec keep_old edit failed: {e}")


@router.callback_query(F.data.startswith("locdec:use:"))
async def cb_use_new(cb: CallbackQuery):
    if not is_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=True)
        return
    try:
        pld_id = int(cb.data.split(":", 2)[2])
    except (ValueError, IndexError):
        await cb.answer("Noto'g'ri tanlov", show_alert=True)
        return
    conn = get_db()
    try:
        row = _load_pending(conn, pld_id)
        if not row:
            await cb.answer("Topilmadi", show_alert=True)
            return
        if row["status"] != "pending":
            await cb.answer(f"Allaqachon hal qilingan: {row['status']}", show_alert=True)
            return

        admin_name = sender_display_name(cb)
        # Snapshot the prior (which may have changed since dispatch — re-read
        # live state so admin_action_log captures what's actually being replaced).
        current_prior = conn.execute(
            "SELECT name, client_id_1c, gps_latitude, gps_longitude, gps_address, "
            "gps_region, gps_district, gps_set_at, gps_set_by_tg_id, gps_set_by_name, "
            "gps_set_by_role FROM allowed_clients WHERE id=?",
            (row["client_id"],),
        ).fetchone()
        snap_args = json.dumps({
            "pld_id": pld_id,
            "client_id": row["client_id"],
            "client_name": row["client_name"],
            "client_id_1c": row["client_id_1c"],
            "prior_gps_latitude": current_prior["gps_latitude"] if current_prior else None,
            "prior_gps_longitude": current_prior["gps_longitude"] if current_prior else None,
            "prior_gps_address": current_prior["gps_address"] if current_prior else None,
            "prior_gps_region": current_prior["gps_region"] if current_prior else None,
            "prior_gps_district": current_prior["gps_district"] if current_prior else None,
            "prior_gps_set_at": current_prior["gps_set_at"] if current_prior else None,
            "prior_gps_set_by_tg_id": current_prior["gps_set_by_tg_id"] if current_prior else None,
            "prior_gps_set_by_name": current_prior["gps_set_by_name"] if current_prior else None,
            "prior_gps_set_by_role": current_prior["gps_set_by_role"] if current_prior else None,
            "overwritten_by_tg_id": row["incoming_by_tg_id"],
            "overwritten_by_name": row["incoming_by_name"],
            "overwritten_by_role": row["incoming_by_role"],
            "overwritten_with_lat": row["incoming_lat"],
            "overwritten_with_lng": row["incoming_lng"],
            "decided_by_tg_id": cb.from_user.id,
            "decided_by_name": admin_name,
            "snapshot_source": "manual_pin_replacement",
        }, ensure_ascii=False)
        try:
            conn.execute(
                "INSERT INTO admin_action_log (telegram_id, user_name, command, args) "
                "VALUES (?, ?, ?, ?)",
                (cb.from_user.id, admin_name, "manual_pin_replacement", snap_args),
            )
        except Exception as e:
            logger.error(f"manual_pin_replacement snapshot failed (non-fatal): {e}")

        # Write the incoming pin to allowed_clients. Attribution stays with the
        # ORIGINAL agent who submitted (gps_set_by_*) — admin is just the approver.
        geo = {
            "address": row["incoming_address"],
            "region": row["incoming_region"],
            "district": row["incoming_district"],
        }
        conn.execute(
            "UPDATE allowed_clients SET gps_latitude=?, gps_longitude=?, "
            "gps_address=?, gps_region=?, gps_district=?, gps_set_at=datetime('now'), "
            "gps_set_by_tg_id=?, gps_set_by_name=?, gps_set_by_role=? WHERE id=?",
            (row["incoming_lat"], row["incoming_lng"], geo["address"],
             geo["region"], geo["district"],
             row["incoming_by_tg_id"], row["incoming_by_name"],
             row["incoming_by_role"], row["client_id"]),
        )
        backfill_text_from_gps(conn, row["client_id"], geo)

        conn.execute(
            "UPDATE pending_location_decisions SET status='use_new', "
            "decided_at=datetime('now'), decided_by_tg_id=?, decided_by_name=? "
            "WHERE id=?",
            (cb.from_user.id, admin_name, pld_id),
        )
        conn.commit()
    finally:
        conn.close()

    await cb.answer("Yangisi olindi")
    try:
        await cb.message.edit_text(
            (cb.message.html_text or cb.message.text or "")
            + _decided_footer("use_new", admin_name),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.warning(f"locdec use_new edit failed: {e}")
