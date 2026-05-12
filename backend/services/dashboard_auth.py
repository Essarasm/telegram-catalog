"""Dashboard auth — Telegram WebApp identity → panel role → session token.

Two pieces:

1. `validate_telegram_init_data` — verifies the HMAC-SHA256 signature on
   the `initData` string Telegram passes to a WebApp. Returns the parsed
   `user` dict on success, None on failure. This is the standard
   Telegram-WebApp validation algorithm
   (https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app).

2. In-memory session store. A successful Telegram auth issues a random
   token; subsequent requests pass that token as `?admin_key=` and the
   coverage endpoints accept it (alongside the env-var admin key). Tokens
   expire after `_SESSION_TTL_SECONDS`.

In-memory is intentional for Phase 1: keeps the surface small. Sessions
die on Railway redeploy, which is acceptable — agents re-launch from the
bot's /dashboard button. Move to a `dashboard_sessions` table when the
re-auth friction starts to bite.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import time
from typing import Optional
from urllib.parse import parse_qsl


_SESSION_TTL_SECONDS = 12 * 60 * 60  # 12h
_TOKEN_PREFIX = "dash_"
_SESSIONS: dict[str, dict] = {}


def validate_telegram_init_data(init_data: str, max_age_seconds: int = 86400) -> Optional[dict]:
    """Validate the HMAC on `initData` and return the parsed `user` dict
    on success, None otherwise. `BOT_TOKEN` env var must be set.
    """
    bot_token = os.getenv("BOT_TOKEN")
    if not init_data or not bot_token:
        return None

    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=False))
    except Exception:
        return None

    received_hash = parsed.pop("hash", None)
    if not received_hash:
        return None

    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    computed = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed, received_hash):
        return None

    # auth_date freshness
    try:
        auth_date = int(parsed.get("auth_date", "0"))
    except ValueError:
        return None
    if auth_date and (time.time() - auth_date) > max_age_seconds:
        return None

    user_raw = parsed.get("user")
    if not user_raw:
        return None
    try:
        return json.loads(user_raw)
    except Exception:
        return None


def create_session(role: str, telegram_id: int, name: Optional[str] = None) -> dict:
    """Issue a session token for a freshly-authenticated dashboard user.
    Returns `{token, expires_at}`.
    """
    token = _TOKEN_PREFIX + secrets.token_urlsafe(32)
    expires_at = time.time() + _SESSION_TTL_SECONDS
    _SESSIONS[token] = {
        "role": role,
        "telegram_id": telegram_id,
        "name": name,
        "expires_at": expires_at,
    }
    _gc()
    return {"token": token, "expires_at": expires_at}


def resolve_session(token: Optional[str]) -> Optional[dict]:
    """Return `{role, telegram_id, name, expires_at}` for a valid token,
    None if missing/expired/unknown.
    """
    if not token or not token.startswith(_TOKEN_PREFIX):
        return None
    rec = _SESSIONS.get(token)
    if not rec:
        return None
    if rec["expires_at"] < time.time():
        _SESSIONS.pop(token, None)
        return None
    return rec


def revoke_session(token: str) -> None:
    _SESSIONS.pop(token, None)


def _gc() -> None:
    now = time.time()
    expired = [t for t, r in _SESSIONS.items() if r["expires_at"] < now]
    for t in expired:
        _SESSIONS.pop(t, None)


# Tab visibility per role. Add cashier/worker entries when those roles
# are wired up; today only `admin` and `agent` are defined.
ROLE_ALLOWED_TABS: dict[str, list[str]] = {
    "admin": ["overview", "clients", "inventory", "supply", "health", "search", "coverage", "collections"],
    "agent": ["coverage"],
}


def allowed_tabs_for(role: str) -> list[str]:
    return list(ROLE_ALLOWED_TABS.get(role, []))
