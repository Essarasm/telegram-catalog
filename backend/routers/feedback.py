"""Post-order feedback endpoint."""
import os
import logging
import httpx
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from backend.database import get_db

router = APIRouter(prefix="/api/feedback", tags=["feedback"])
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_GROUP_CHAT_ID = os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051")


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
