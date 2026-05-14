"""User-side request auth — Telegram WebApp `initData` HMAC enforcement.

Scope (Phase A, 2026-05-13): write + comp-sensitive endpoints only —
cart/set, cart/clear, payments/legal-transfer, payments/agent-cash-handover,
agent/commission, cabinet/orders/{id}/reorder. Read-side cabinet endpoints
remain on bare-telegram_id auth pending Phase B.

Why a manual call (not FastAPI Depends): the protected endpoints take
their claimed telegram_id from inconsistent places — query, form, JSON
body, Pydantic model — so a uniform Depends signature is awkward. A
plain `assert_init_data(request, claimed_id)` invocation at the top of
each endpoint reads cleaner and keeps the failure path explicit.
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import HTTPException, Request

from backend.database import get_db
from backend.services.dashboard_auth import validate_telegram_init_data


_HEADER_NAME = "x-telegram-init-data"


def _log_failure(claimed_id: int, parsed_id: Optional[int], path: str, reason: str) -> None:
    """Append a row to hmac_audit_log. Never raises — auth logging must
    not become its own failure path.
    """
    try:
        conn = get_db()
        try:
            conn.execute(
                """INSERT INTO hmac_audit_log
                   (claimed_telegram_id, parsed_telegram_id, path, reason)
                   VALUES (?, ?, ?, ?)""",
                (claimed_id, parsed_id, path, reason),
            )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        pass


def assert_init_data(request: Request, claimed_telegram_id: int) -> dict:
    """Validate `X-Telegram-Init-Data` and assert it identifies the
    same user as `claimed_telegram_id`. Returns the parsed user dict on
    success; raises HTTPException(401) otherwise.

    Failure modes (all logged to hmac_audit_log):
      - missing_header: no X-Telegram-Init-Data
      - invalid_hmac: header present but HMAC verification fails or
        auth_date too old (>24h) or user payload malformed
      - id_mismatch: HMAC valid but user.id != claimed_telegram_id
    """
    path = request.url.path
    init_data = request.headers.get(_HEADER_NAME, "") or ""

    if not init_data:
        _log_failure(claimed_telegram_id, None, path, "missing_header")
        raise HTTPException(status_code=401, detail="missing_init_data")

    parsed_user = validate_telegram_init_data(init_data)
    if not parsed_user:
        _log_failure(claimed_telegram_id, None, path, "invalid_hmac")
        raise HTTPException(status_code=401, detail="invalid_init_data")

    try:
        parsed_id = int(parsed_user.get("id") or 0)
    except (TypeError, ValueError):
        parsed_id = 0

    if not parsed_id or parsed_id != claimed_telegram_id:
        _log_failure(claimed_telegram_id, parsed_id, path, "id_mismatch")
        raise HTTPException(status_code=401, detail="init_data_user_mismatch")

    return parsed_user
