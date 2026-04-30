"""Role resolution for the agent panel.

Roles live in `users.agent_role` (admin / cashier / agent / worker / NULL).
This module is the single read-side helper — every backend role check
should go through `get_role()` so the resolution rules stay consistent.

Resolution order:
1. `users.agent_role` from the DB (authoritative)
2. Env fallbacks: ADMIN_IDS → admin, CASHIER_IDS → cashier (matches the
   pre-existing bot helpers in `bot/shared.py`); ensures a freshly-set env
   ID can use the panel before /makeagent has been called.
3. Legacy `users.is_agent = 1` → agent (back-compat for old rows that
   never got migrated).
4. None.
"""
from __future__ import annotations

import os
from typing import Iterable, Optional

VALID_ROLES = ("admin", "cashier", "agent", "worker")


def _env_id_set(var: str) -> set[int]:
    raw = os.getenv(var, "")
    if not raw:
        return set()
    return {int(x.strip()) for x in raw.split(",") if x.strip().isdigit()}


def get_role(conn, telegram_id: int) -> Optional[str]:
    """Return the user's panel role, or None if they have none."""
    if not telegram_id:
        return None
    row = conn.execute(
        "SELECT agent_role, is_agent FROM users WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    db_role = (row["agent_role"] if row else None) or None
    if db_role in VALID_ROLES:
        return db_role

    # Env fallback: ADMIN_IDS first (admin trumps everything).
    if telegram_id in _env_id_set("ADMIN_IDS"):
        return "admin"
    if telegram_id in _env_id_set("CASHIER_IDS"):
        return "cashier"

    # Legacy is_agent=1 with no explicit role → treat as plain agent.
    if row and row["is_agent"]:
        return "agent"

    return None


def role_in(conn, telegram_id: int, allowed: Iterable[str]) -> bool:
    """True if the user's resolved role is in the allowed set."""
    role = get_role(conn, telegram_id)
    return role is not None and role in set(allowed)
