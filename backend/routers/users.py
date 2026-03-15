"""User registration — stores phone number and location."""
from fastapi import APIRouter, Query
from fastapi.responses import Response
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
    latitude: Optional[float] = None
    longitude: Optional[float] = None


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
    """Save user info: phone, name, and optional location."""
    conn = get_db()
    conn.execute(
        """INSERT INTO users (telegram_id, phone, first_name, last_name, username, latitude, longitude)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(telegram_id) DO UPDATE SET
               phone = excluded.phone,
               first_name = excluded.first_name,
               last_name = excluded.last_name,
               username = excluded.username,
               latitude = COALESCE(excluded.latitude, users.latitude),
               longitude = COALESCE(excluded.longitude, users.longitude)""",
        (user.telegram_id, user.phone, user.first_name, user.last_name,
         user.username, user.latitude, user.longitude),
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@router.get("/export-map")
def export_clients_csv():
    """Export all clients as CSV for Google My Maps import."""
    conn = get_db()
    rows = conn.execute(
        """SELECT telegram_id, phone, first_name, last_name, username,
                  latitude, longitude, registered_at
           FROM users ORDER BY registered_at"""
    ).fetchall()
    conn.close()

    lines = ["Name,Phone,Latitude,Longitude,Username,Registered"]
    for r in rows:
        name = " ".join(filter(None, [r["first_name"], r["last_name"]])) or r["username"] or str(r["telegram_id"])
        lat = r["latitude"] or ""
        lng = r["longitude"] or ""
        phone = (r["phone"] or "").replace(",", "")
        lines.append(f'"{name}","{phone}",{lat},{lng},"{r["username"] or ""}","{r["registered_at"] or ""}"')

    csv_content = "\n".join(lines)
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=clients_map.csv"},
    )
