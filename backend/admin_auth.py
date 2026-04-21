"""Shared admin-API-key check for internal endpoints.

Replaces the legacy hardcoded string `"rassvet2026"` with an env-var-
driven value. If `ADMIN_API_KEY` is not set, falls back to the legacy
string so prod doesn't break during rotation. To rotate:
  1. Set `ADMIN_API_KEY=<new_random_value>` on Railway
  2. Redeploy
  3. After confirming endpoints work, this fallback can be removed
     (and you can eventually move all bot calls to read the same env var)
"""
import os

_FALLBACK = "rassvet2026"
_CURRENT = os.getenv("ADMIN_API_KEY") or _FALLBACK


def check_admin_key(key: str) -> bool:
    """True if the provided admin_key matches the currently-configured one."""
    return bool(key) and key == _CURRENT


def get_admin_key() -> str:
    """Return the current admin key for internal bot-to-API calls.
    Bot handlers should pass this as the `admin_key` form field instead
    of hardcoding a string."""
    return _CURRENT


def using_fallback() -> bool:
    """True if ADMIN_API_KEY env var is unset (legacy string in use)."""
    return os.getenv("ADMIN_API_KEY") is None
