"""User registration — stores phone number from Telegram contact sharing."""
from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
from backend.database import get_db

router = APIRouter(prefix="/api/users", tags=["users"])


class UserRegister(BaseModel):
    telegram_id: int
    phone: str
    first_name: Optional[str] = ""
    last_name: Optional[str] = ""
    username: Optional[str] = ""


@router.get("/check")
def check_user(telegram_id: int = Query(...)):
    """Check if user has registered (shared phone number)."""
    conn = get_db()
    row = conn.execute(
        "SELECT telegram_id, phone, first_name FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.close()
    if row:
        return {"registered": True, "phone": row["phone"], "first_name": row["first_name"]}
    return {"registered": False}


@router.post("/register")
def register_user(user: UserRegister):
    """Save user phone number from Telegram contact sharing."""
    conn = get_db()
    conn.execute(
        """INSERT INTO users (telegram_id, phone, first_name, last_name, username)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(telegram_id) DO UPDATE SET
               phone = excluded.phone,
               first_name = excluded.first_name,
               last_name = excluded.last_name,
               username = excluded.username""",
        (user.telegram_id, user.phone, user.first_name, user.last_name, user.username),
    )
    conn.commit()
    conn.close()
    return {"ok": True}
