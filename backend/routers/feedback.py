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
ADMIN_GROUP_CHAT_ID = os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051")
ERRORS_GROUP_CHAT_ID = os.getenv("ERRORS_GROUP_CHAT_ID", "-5085083917")


class FeedbackRequest(BaseModel):
    order_id: Optional[int] = None
    telegram_id: int
    feedback_text: str


@router.post("")
def submit_feedback(req: FeedbackRequest):
    """Save post-order feedback and notify admins."""
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

    # Look up client name for the notification
    client_name = ""
    if req.telegram_id:
        conn2 = get_db()
        row = conn2.execute(
            "SELECT first_name, last_name FROM users WHERE telegram_id = ?",
            (req.telegram_id,),
        ).fetchone()
        conn2.close()
        if row:
            client_name = " ".join(filter(None, [row["first_name"], row["last_name"]]))

    # Notify admin group
    if BOT_TOKEN and ADMIN_GROUP_CHAT_ID:
        preview = text[:100] + ("..." if len(text) > 100 else "")
        name_part = client_name or f"ID {req.telegram_id}"
        message = f"\U0001f4ac Yangi fikr-mulohaza: {name_part} \u2014 {preview}"
        try:
            httpx.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": ADMIN_GROUP_CHAT_ID, "text": message},
                timeout=10,
            )
        except Exception as e:
            logger.error(f"Failed to send feedback notification: {e}")

    return {"ok": True}


@router.post("/order-issue")
async def submit_order_issue(
    order_id: int = Form(...),
    telegram_id: int = Form(...),
    comment: str = Form(""),
    order_doc_number: str = Form(""),
    order_date: str = Form(""),
    files: List[UploadFile] = File(default_factory=list),
):
    """Client complaint about a real order: wrong items, missing items, etc.

    Persists the comment in order_feedback, then pushes the complaint to
    the "Xatolar katalog" group with any attached photos so the agents
    can call the client back.
    """
    text = (comment or "").strip()[:800]

    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO order_feedback (order_id, user_id, feedback_text)
               VALUES (?, ?, ?)""",
            (order_id, telegram_id, text or "(photo only)"),
        )
        # Resolve the client's 1C name + phone for the notification
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

    # Compose the caption / message
    header = "⚠️ <b>Buyurtma bo'yicha shikoyat</b>\n"
    lines = [header]
    if client_1c:
        lines.append(f"🧾 1C: {client_1c}")
    if name_parts:
        lines.append(f"👤 Telegram: {name_parts[0]}")
    if phone:
        lines.append(f"📞 {phone}")
    lines.append(f"🆔 Telegram ID: <code>{telegram_id}</code>")
    lines.append(f"🚚 Buyurtma #{order_id}" +
                 (f" ({order_doc_number})" if order_doc_number else "") +
                 (f" · {order_date}" if order_date else ""))
    if text:
        lines.append("")
        lines.append(f"💬 {text}")
    caption = "\n".join(lines)

    if not BOT_TOKEN or not ERRORS_GROUP_CHAT_ID:
        logger.warning("ERRORS_GROUP_CHAT_ID or BOT_TOKEN missing; skipping forward")
        return {"ok": True, "forwarded": False}

    try:
        # If there are photos, send first as sendPhoto with full caption;
        # additional photos follow with short captions.
        if files:
            first = True
            async with httpx.AsyncClient(timeout=30) as client:
                for idx, f in enumerate(files):
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
