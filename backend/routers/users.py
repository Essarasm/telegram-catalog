"""User registration with client whitelist verification."""
from fastapi import APIRouter, Query
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Optional
from backend.database import get_db
from backend.services.notify_registration import send_registration_notification
import re
import threading

router = APIRouter(prefix="/api/users", tags=["users"])


def normalize_phone(raw: str) -> str:
    """Strip to last 9 digits for matching."""
    digits = re.sub(r"\D", "", raw or "")
    return digits[-9:] if len(digits) >= 9 else digits


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
    """Check if user is registered AND approved."""
    conn = get_db()
    row = conn.execute(
        "SELECT telegram_id, phone, first_name, latitude, longitude, is_approved, client_id FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.close()

    if not row or not row["phone"]:
        return {"registered": False, "approved": False}

    is_approved = bool(row["is_approved"])

    if is_approved:
        return {"registered": True, "approved": True, "phone": row["phone"], "first_name": row["first_name"]}
    else:
        return {"registered": True, "approved": False, "phone": row["phone"]}


@router.post("/register")
def register_user(user: UserRegister):
    """Save user info and check whitelist."""
    conn = get_db()
    phone_norm = normalize_phone(user.phone)

    # Check if phone is in allowed_clients
    client_row = conn.execute(
        "SELECT id, name, location FROM allowed_clients WHERE phone_normalized = ? LIMIT 1",
        (phone_norm,),
    ).fetchone()

    is_approved = 1 if client_row else 0
    client_id = client_row["id"] if client_row else None
    client_name = client_row["name"] if client_row else None

    conn.execute(
        """INSERT INTO users (telegram_id, phone, first_name, last_name, username, latitude, longitude, is_approved, client_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(telegram_id) DO UPDATE SET
               phone = excluded.phone,
               first_name = excluded.first_name,
               last_name = excluded.last_name,
               username = excluded.username,
               latitude = COALESCE(excluded.latitude, users.latitude),
               longitude = COALESCE(excluded.longitude, users.longitude),
               is_approved = excluded.is_approved,
               client_id = excluded.client_id""",
        (user.telegram_id, user.phone, user.first_name, user.last_name,
         user.username, user.latitude, user.longitude, is_approved, client_id),
    )

    # Link telegram_id back to allowed_clients for future reference
    if client_row:
        conn.execute(
            "UPDATE allowed_clients SET matched_telegram_id = ? WHERE id = ?",
            (user.telegram_id, client_row["id"]),
        )

    conn.commit()
    conn.close()

    # Notify manager about new registration (non-blocking)
    try:
        threading.Thread(
            target=send_registration_notification,
            kwargs={
                "telegram_id": user.telegram_id,
                "phone": user.phone,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "username": user.username,
                "latitude": user.latitude,
                "longitude": user.longitude,
                "is_approved": bool(is_approved),
                "client_name": client_name,
            },
            daemon=True,
        ).start()
    except Exception:
        pass  # Don't fail registration if notification fails

    return {
        "ok": True,
        "approved": bool(is_approved),
        "client_name": client_name,
    }


@router.post("/approve")
def approve_user(telegram_id: int = Query(...), admin_key: str = Query(...)):
    """Manually approve a user (for admin use)."""
    if admin_key != "rassvet2026":
        return {"error": "Invalid admin key"}
    conn = get_db()
    conn.execute("UPDATE users SET is_approved = 1 WHERE telegram_id = ?", (telegram_id,))
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
