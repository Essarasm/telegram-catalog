"""Dashboard auth endpoints — Telegram WebApp login → session token.

Flow:
  1. Agent taps "Open dashboard" in the bot → Telegram WebApp opens
     `/admin` and injects signed `initData`.
  2. Frontend POSTs `init_data` to `/api/admin/auth/telegram`.
  3. We validate the HMAC, look up the user's panel role via
     `backend.services.roles.get_role`, and issue a session token.
  4. Frontend stores the token and uses it as `?admin_key=<token>` on
     subsequent calls. Coverage endpoints accept it; admin-only endpoints
     don't.

Browser-direct admins are unaffected — they still type the env-var
`ADMIN_API_KEY` into the auth form and get full access.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.database import get_db
from backend.services.dashboard_auth import (
    allowed_tabs_for,
    create_session,
    revoke_session,
    validate_telegram_init_data,
)
from backend.services.roles import get_role

router = APIRouter(prefix="/api/admin/auth", tags=["dashboard-auth"])


class TelegramAuthRequest(BaseModel):
    init_data: str


class LogoutRequest(BaseModel):
    session_token: str


@router.post("/telegram")
def telegram_auth(req: TelegramAuthRequest):
    user = validate_telegram_init_data(req.init_data)
    if not user:
        raise HTTPException(status_code=401, detail="invalid init_data")

    telegram_id = user.get("id")
    if not telegram_id:
        raise HTTPException(status_code=401, detail="no user id in init_data")

    name = (
        (user.get("first_name") or "")
        + (" " + user.get("last_name") if user.get("last_name") else "")
    ).strip() or user.get("username") or str(telegram_id)

    conn = get_db()
    role = get_role(conn, telegram_id)
    conn.close()

    if not role:
        raise HTTPException(status_code=403, detail="no panel role for this user")

    tabs = allowed_tabs_for(role)
    if not tabs:
        raise HTTPException(status_code=403, detail=f"role '{role}' has no dashboard tabs configured")

    session = create_session(role, telegram_id, name)
    return {
        "session_token": session["token"],
        "expires_at": session["expires_at"],
        "role": role,
        "telegram_id": telegram_id,
        "name": name,
        "allowed_tabs": tabs,
    }


@router.post("/logout")
def logout(req: LogoutRequest):
    revoke_session(req.session_token)
    return {"ok": True}
