"""User registration with client whitelist verification."""
from fastapi import APIRouter, Query
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Optional
from backend.database import get_db
from backend.services.notify_registration import send_registration_notification
from backend.services.backup_users import save_user_to_backup
from backend.admin_auth import check_admin_key
import json
import os
import re
import threading

# Load always-approved IDs from multiple sources (belt + suspenders):
# 1. ALWAYS_APPROVED_IDS env var on Railway (most reliable — survives everything)
# 2. approved_overrides.json in the repo (committed to git = permanent)
_ALWAYS_APPROVED = set()

# Source 1: Environment variable (comma-separated telegram IDs)
_env_ids = os.getenv("ALWAYS_APPROVED_IDS", "")
if _env_ids:
    _ALWAYS_APPROVED = {int(x.strip()) for x in _env_ids.split(",") if x.strip().isdigit()}

# Source 2: JSON file in repo
_OVERRIDES_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'approved_overrides.json')
try:
    with open(_OVERRIDES_PATH, 'r') as _f:
        _data = json.load(_f)
        _ALWAYS_APPROVED |= set(_data.get('always_approved_ids', []))
except Exception:
    pass

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


def _find_user_in_backup(telegram_id):
    """Last-resort: look up a user directly in the JSON backup file."""
    try:
        backup_path = os.getenv("USERS_BACKUP_PATH", "/data/users_backup.json")
        if not os.path.exists(backup_path):
            return None
        with open(backup_path, 'r') as f:
            users = json.load(f)
        for u in users:
            if u.get('telegram_id') == telegram_id and u.get('phone'):
                return u
    except Exception as e:
        print(f"[users] _find_user_in_backup error: {e}")
    return None


@router.get("/check")
def check_user(telegram_id: int = Query(...)):
    """Check if user is registered AND approved."""
    conn = get_db()
    row = conn.execute(
        "SELECT telegram_id, phone, first_name, latitude, longitude, is_approved, client_id, is_agent FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.close()

    if not row or not row["phone"]:
        # DB doesn't have this user — check JSON backup as fallback
        backup_user = _find_user_in_backup(telegram_id)
        if backup_user:
            # Re-insert from backup into DB so future checks are fast
            try:
                conn2 = get_db()
                conn2.execute(
                    """INSERT INTO users (telegram_id, phone, first_name, last_name,
                       username, latitude, longitude, is_approved, client_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(telegram_id) DO UPDATE SET
                           phone = excluded.phone,
                           first_name = COALESCE(excluded.first_name, users.first_name),
                           is_approved = MAX(excluded.is_approved, users.is_approved)""",
                    (
                        backup_user.get('telegram_id'),
                        backup_user.get('phone'),
                        backup_user.get('first_name', ''),
                        backup_user.get('last_name', ''),
                        backup_user.get('username', ''),
                        backup_user.get('latitude'),
                        backup_user.get('longitude'),
                        backup_user.get('is_approved', 0),
                        backup_user.get('client_id'),
                    ),
                )
                conn2.commit()
                conn2.close()
                print(f"[users] Recovered user {telegram_id} from JSON backup into DB")
            except Exception as e:
                print(f"[users] Failed to re-insert backup user {telegram_id}: {e}")

            is_approved = bool(backup_user.get('is_approved')) or (telegram_id in _ALWAYS_APPROVED)
            return {
                "registered": True,
                "approved": is_approved,
                "phone": backup_user.get('phone'),
                "first_name": backup_user.get('first_name', ''),
            }

        # Even if not in DB or backup, check hardcoded overrides
        override = telegram_id in _ALWAYS_APPROVED
        return {"registered": False, "approved": override}

    is_approved = bool(row["is_approved"]) or (telegram_id in _ALWAYS_APPROVED)

    # If override says approved but DB doesn't, fix the DB
    if telegram_id in _ALWAYS_APPROVED and not row["is_approved"]:
        try:
            conn2 = get_db()
            conn2.execute("UPDATE users SET is_approved = 1 WHERE telegram_id = ?", (telegram_id,))
            conn2.commit()
            conn2.close()
        except Exception:
            pass

    is_agent = bool(row["is_agent"]) if row["is_agent"] else False

    if is_approved:
        return {"registered": True, "approved": True, "phone": row["phone"],
                "first_name": row["first_name"], "is_agent": is_agent}
    else:
        return {"registered": True, "approved": False, "phone": row["phone"]}


@router.post("/register")
def register_user(user: UserRegister):
    """Save user info and check whitelist."""
    conn = get_db()
    phone_norm = normalize_phone(user.phone)

    # Check if phone is in allowed_clients
    client_row = conn.execute(
        "SELECT id, name, location, client_id_1c FROM allowed_clients WHERE phone_normalized = ? LIMIT 1",
        (phone_norm,),
    ).fetchone()

    is_approved = 1 if (client_row or user.telegram_id in _ALWAYS_APPROVED) else 0
    client_id = client_row["id"] if client_row else None
    client_name = client_row["name"] if client_row else None
    client_id_1c = client_row["client_id_1c"] if client_row else None

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

    # Persist to JSON backup so approval survives deploys
    save_user_to_backup({
        'telegram_id': user.telegram_id,
        'phone': user.phone,
        'first_name': user.first_name,
        'last_name': user.last_name or '',
        'username': user.username or '',
        'latitude': user.latitude,
        'longitude': user.longitude,
        'is_approved': is_approved,
        'client_id': client_id,
    })

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
                "client_id_1c": client_id_1c,
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
    if not check_admin_key(admin_key):
        return {"error": "Invalid admin key"}
    conn = get_db()
    conn.execute("UPDATE users SET is_approved = 1 WHERE telegram_id = ?", (telegram_id,))
    # Read full user row to persist to backup
    row = conn.execute(
        "SELECT telegram_id, phone, first_name, last_name, username, latitude, longitude, is_approved, client_id, registered_at FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    conn.commit()
    conn.close()

    if row:
        save_user_to_backup(dict(row))

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
