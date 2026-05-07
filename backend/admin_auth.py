"""Shared admin-API-key check for internal endpoints.

Replaces the legacy hardcoded string `"rassvet2026"` with an env-var-
driven value. If `ADMIN_API_KEY` is not set, falls back to the legacy
string so prod doesn't break during rotation. To rotate:
  1. Set `ADMIN_API_KEY=<new_random_value>` on Railway
  2. Redeploy
  3. After confirming endpoints work, this fallback can be removed
     (and you can eventually move all bot calls to read the same env var)

`resolve_auth` is the role-aware variant: it accepts either the env-var
admin key (→ admin role) or a dashboard session token issued by the
Telegram-WebApp auth flow (→ whatever role the user has). Endpoints that
want to allow non-admin roles call `resolve_auth` instead of
`check_admin_key`.
"""
import os
from typing import Optional

from backend.services.dashboard_auth import resolve_session

_FALLBACK = "rassvet2026"
_CURRENT = os.getenv("ADMIN_API_KEY") or _FALLBACK


def check_admin_key(key: str) -> bool:
    """True if the provided admin_key is valid for full-admin access.

    Two valid forms:
      1. The env-var `ADMIN_API_KEY` (browser-direct admin path, internal
         bot-to-API calls, smoke tests).
      2. A dashboard session token whose role is `admin` (issued by the
         Telegram-WebApp auth endpoint when an admin opens /dashboard
         from the bot).

    Endpoints that want to allow non-admin roles (agent, cashier, …)
    should call `resolve_auth` and check the role themselves instead.
    """
    if not key:
        return False
    if key == _CURRENT:
        return True
    sess = resolve_session(key)
    return bool(sess and sess.get("role") == "admin")


def get_admin_key() -> str:
    """Return the current admin key for internal bot-to-API calls.
    Bot handlers should pass this as the `admin_key` form field instead
    of hardcoding a string."""
    return _CURRENT


def using_fallback() -> bool:
    """True if ADMIN_API_KEY env var is unset (legacy string in use)."""
    return os.getenv("ADMIN_API_KEY") is None


def resolve_auth(key: Optional[str]) -> Optional[dict]:
    """Resolve a key to `{role, telegram_id, name}` or None.

    - Env-var match → role='admin', telegram_id=None, name=None
    - Session-token match → role from session, telegram_id from session
    - Anything else → None
    """
    if not key:
        return None
    if check_admin_key(key):
        return {"role": "admin", "telegram_id": None, "name": None}
    sess = resolve_session(key)
    if sess:
        return {"role": sess["role"], "telegram_id": sess["telegram_id"], "name": sess.get("name")}
    return None
