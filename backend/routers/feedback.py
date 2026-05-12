"""Post-order feedback endpoint."""
import os
import logging
import httpx
from fastapi import APIRouter, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional, List
from backend.database import get_db

router = APIRouter(prefix="/api/feedback", tags=["feedback"])
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
# Group "Taklif va Xatolar" was upgraded to supergroup on 2026-04 — chat ID
# changed from -5085083917 (regular) to -1003896597497 (supergroup). Using
# the old ID caused all feedback forwarding to silently fail (Telegram
# returned 400 "group chat was upgraded to a supergroup chat"). Фикс 2026-04-21.
from backend.services.group_config import ORDER_GROUP_CHAT_ID, ERRORS_GROUP_CHAT_ID
from backend.services.notify_group import build_dispatch_markup


class FeedbackRequest(BaseModel):
    order_id: Optional[int] = None
    telegram_id: int
    feedback_text: str


def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


@router.post("")
def submit_feedback(req: FeedbackRequest):
    """Save post-order feedback and append it to the Sotuv-group order message.

    Behavior change (2026-05-11): instead of posting a separate "Yangi
    fikr-mulohaza" notification to the admin group, the comment is edited
    onto the end of the existing Sotuv order message so the sales team
    sees client thoughts in-context with the order they belong to. The
    frozen-original message text lives in `orders.sales_group_message_text`;
    re-submissions overlay the latest comment on top of the original (no
    stacking). Dispatch keyboard is rebuilt from current order state so
    editMessageText doesn't strip the pick/assigned button.
    """
    text = (req.feedback_text or "").strip()
    if not text:
        return {"ok": False, "error": "Feedback text is empty"}
    if len(text) > 250:
        text = text[:250]

    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO order_feedback (order_id, user_id, feedback_text)
               VALUES (?, ?, ?)""",
            (req.order_id, req.telegram_id, text),
        )
        conn.commit()
    finally:
        conn.close()

    if not req.order_id:
        logger.info("feedback received without order_id — saved to DB, no Sotuv edit")
        return {"ok": True, "appended": False}

    if not BOT_TOKEN or not ORDER_GROUP_CHAT_ID:
        logger.warning("BOT_TOKEN or ORDER_GROUP_CHAT_ID missing; feedback saved but not appended")
        return {"ok": True, "appended": False}

    conn2 = get_db()
    try:
        order = conn2.execute(
            "SELECT sales_group_message_id, sales_group_message_text, "
            "delivery_status, assigned_agent_id "
            "FROM orders WHERE id = ?",
            (req.order_id,),
        ).fetchone()
        agent = None
        if order and order["assigned_agent_id"]:
            agent = conn2.execute(
                "SELECT first_name, vehicle, vehicle_capacity_tons "
                "FROM users WHERE telegram_id = ?",
                (order["assigned_agent_id"],),
            ).fetchone()
    finally:
        conn2.close()

    if not order or not order["sales_group_message_id"] or not order["sales_group_message_text"]:
        logger.warning(
            f"feedback for order {req.order_id}: missing sales_group_message_id/_text "
            f"(legacy order pre-migration?); saved to DB, no Sotuv edit"
        )
        return {"ok": True, "appended": False}

    new_text = (
        order["sales_group_message_text"]
        + f"\n\n\U0001f4ac <b>Mijoz izohi:</b>\n{_esc(text)}"
    )
    agent_dict = None
    if agent:
        agent_dict = {
            "first_name": agent["first_name"],
            "vehicle": agent["vehicle"],
            "vehicle_capacity_tons": agent["vehicle_capacity_tons"],
        }
    markup = build_dispatch_markup(req.order_id, order["delivery_status"], agent_dict)

    payload = {
        "chat_id": ORDER_GROUP_CHAT_ID,
        "message_id": order["sales_group_message_id"],
        "text": new_text,
        "parse_mode": "HTML",
    }
    if markup:
        payload["reply_markup"] = markup
    try:
        resp = httpx.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
            json=payload,
            timeout=10,
        )
        if resp.status_code != 200:
            logger.error(
                f"editMessageText for order {req.order_id} returned "
                f"{resp.status_code}: {resp.text[:200]}"
            )
            return {"ok": True, "appended": False}
    except Exception as e:
        logger.error(f"editMessageText for order {req.order_id} failed: {e}")
        return {"ok": True, "appended": False}

    return {"ok": True, "appended": True}


@router.post("/order-issue")
async def submit_order_issue(
    order_id: int = Form(...),
    telegram_id: int = Form(...),
    comment: str = Form(""),
    order_doc_number: str = Form(""),
    order_date: str = Form(""),
    files: Optional[List[UploadFile]] = File(None),
):
    """Client complaint about a real order: wrong items, missing items, etc.

    Persists the comment in order_feedback, then pushes the complaint to
    the "Xatolar katalog" group with any attached photos so the agents
    can call the client back.
    """
    text = (comment or "").strip()[:800]

    conn = get_db()
    row = None
    try:
        # order_feedback.order_id has an FK to the wishlist `orders` table.
        # Real-order complaints won't match; store with NULL order_id in that
        # case and prefix the comment so admins can still find the real order.
        try:
            conn.execute(
                """INSERT INTO order_feedback (order_id, user_id, feedback_text)
                   VALUES (?, ?, ?)""",
                (order_id, telegram_id, text or "(photo only)"),
            )
        except Exception:
            prefixed = f"[real_order #{order_id}] {text or '(photo only)'}"[:800]
            conn.execute(
                """INSERT INTO order_feedback (order_id, user_id, feedback_text)
                   VALUES (NULL, ?, ?)""",
                (telegram_id, prefixed),
            )
        row = conn.execute(
            """SELECT u.first_name, u.last_name, u.phone, ac.client_id_1c
               FROM users u
               LEFT JOIN allowed_clients ac ON ac.id = u.client_id
               WHERE u.telegram_id = ?""",
            (telegram_id,),
        ).fetchone()
        conn.commit()
    finally:
        conn.close()

    name_parts = []
    phone = ""
    client_1c = ""
    if row:
        full = " ".join(filter(None, [row["first_name"], row["last_name"]]))
        if full:
            name_parts.append(full)
        phone = row["phone"] or ""
        client_1c = row["client_id_1c"] or ""

    def esc(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # Compose the caption / message (all user-controlled values HTML-escaped)
    lines = ["⚠️ <b>Buyurtma bo'yicha shikoyat</b>\n"]
    if client_1c:
        lines.append(f"🧾 1C: {esc(client_1c)}")
    if name_parts:
        lines.append(f"👤 Telegram: {esc(name_parts[0])}")
    if phone:
        lines.append(f"📞 {esc(phone)}")
    lines.append(f"🆔 Telegram ID: <code>{telegram_id}</code>")
    lines.append(f"🚚 Buyurtma #{order_id}" +
                 (f" ({esc(order_doc_number)})" if order_doc_number else "") +
                 (f" · {esc(order_date)}" if order_date else ""))
    if text:
        lines.append("")
        lines.append(f"💬 {esc(text)}")
    caption = "\n".join(lines)
    # Telegram caption limit is 1024 chars
    if len(caption) > 1000:
        caption = caption[:1000] + "…"

    if not BOT_TOKEN or not ERRORS_GROUP_CHAT_ID:
        logger.warning("ERRORS_GROUP_CHAT_ID or BOT_TOKEN missing; skipping forward")
        return {"ok": True, "forwarded": False}

    # Drop any placeholder upload (Starlette sends a phantom entry when the
    # multipart field exists but no file was chosen).
    real_files = [f for f in (files or []) if f and (f.filename or "").strip()]

    try:
        if real_files:
            first = True
            async with httpx.AsyncClient(timeout=30) as client:
                for idx, f in enumerate(real_files):
                    data = await f.read()
                    content_type = f.content_type or "image/jpeg"
                    payload_caption = caption if first else f"(rasm {idx + 1})"
                    resp = await client.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                        data={
                            "chat_id": ERRORS_GROUP_CHAT_ID,
                            "caption": payload_caption,
                            "parse_mode": "HTML",
                        },
                        files={
                            "photo": (f.filename or f"photo_{idx}.jpg",
                                      data, content_type),
                        },
                    )
                    if resp.status_code != 200:
                        logger.error(f"sendPhoto failed: {resp.status_code} {resp.text[:200]}")
                    first = False
        else:
            httpx.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": ERRORS_GROUP_CHAT_ID,
                    "text": caption,
                    "parse_mode": "HTML",
                },
                timeout=10,
            )
    except Exception as e:
        logger.error(f"Failed to forward order-issue to errors group: {e}")
        return {"ok": True, "forwarded": False, "error": str(e)[:200]}

    return {"ok": True, "forwarded": True}
