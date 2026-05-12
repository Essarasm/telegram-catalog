"""Canonical home for Telegram group chat IDs.

Before this module existed, the same hardcoded chat-ID fallback literals
were duplicated across ~12 files (bot/shared.py, error_alert.py, notify_*.py,
feedback.py, reports.py, payments.py, agent_signup.py, bot/handlers/admin.py,
bot/handlers/cashier.py, bot/handlers/location.py, bot/reminders.py). Adding
or rotating a chat meant editing every fallback by hand, and at least one
silent drift was caught by the foundation audit (location.py used the wrong
fallback for ERRORS_GROUP_CHAT_ID — it shipped REPORT_GROUP_CHAT_ID's value
instead).

All env vars resolve at module-import time; restart the service to pick up
new env values. Values are always int (Telegram chat IDs); consumers that
historically used str can still pass int — the Telegram API accepts both.
0 means "unconfigured" — the dependent feature stays inert.
"""
import os
from dotenv import load_dotenv

# Idempotent — safe to call again even if backend.main / bot.main loaded it.
load_dotenv()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_int_list(name: str) -> list[int]:
    """Parse a comma-separated env var into a list of ints.
    Empty / unset → empty list (feature stays inert).
    Malformed entries are skipped silently (logged at debug elsewhere).
    """
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return out


# ── Canonical group chat IDs ─────────────────────────────────────────

# Admin group — error alerts, registration notifications, ops chatter.
ADMIN_GROUP_CHAT_ID: int = _env_int("ADMIN_GROUP_CHAT_ID", -5224656051)

# Daily group — morning/EOD upload nudges, daily checklist.
DAILY_GROUP_CHAT_ID: int = _env_int("DAILY_GROUP_CHAT_ID", -5243912135)

# Sales/orders group — order confirmations, exports.
ORDER_GROUP_CHAT_ID: int = _env_int("ORDER_GROUP_CHAT_ID", -1003740010463)

# Inventory group — stock alerts, product-interest signals.
INVENTORY_GROUP_CHAT_ID: int = _env_int("INVENTORY_GROUP_CHAT_ID", -5133871411)

# Agents group — agent-signup approvals, agent-side ops.
AGENTS_GROUP_CHAT_ID: int = _env_int("AGENTS_GROUP_CHAT_ID", -1003922400481)

# Driver group — client-location capture by drivers/agents in the field.
DRIVER_GROUP_CHAT_ID: int = _env_int("DRIVER_GROUP_CHAT_ID", -4998450084)

# Agent-approval group — Block C agent self-registration. Bot must be admin
# in this supergroup for inline-button callbacks to deliver
# (memory: feedback_telegram_bot_supergroup_admin.md).
AGENT_APPROVAL_GROUP_CHAT_ID: int = _env_int("AGENT_APPROVAL_GROUP_CHAT_ID", -1003967758004)

# Errors-and-feedback group ("Taklif va Xatolar"). Distinct from the report
# group below — that drift was a real bug pre-renovation.
ERRORS_GROUP_CHAT_ID: int = _env_int("ERRORS_GROUP_CHAT_ID", -1003896597497)

# Report-issue group — used by /report endpoints.
REPORT_GROUP_CHAT_ID: int = _env_int("REPORT_GROUP_CHAT_ID", -5085083917)

# Cashier group (Aunt + Uncle for cash-direct flow). 0 = unconfigured;
# the cashier FSM stays inert until set.
CASHIER_GROUP_CHAT_ID: int = _env_int("CASHIER_GROUP_CHAT_ID", 0)

# Bank-transfer group (Uchqun + Shuhrat). Sister to cashier; routes through
# bot/handlers/bank_transfer.py. 0 = unconfigured.
BANK_TRANSFER_GROUP_CHAT_ID: int = _env_int("BANK_TRANSFER_GROUP_CHAT_ID", 0)

# Legal-transfer notifications group. Falls back to CASHIER_GROUP_CHAT_ID
# at consumer site if not set, preserving the historical behavior in
# backend/routers/payments.py and bot/handlers/cashier.py.
LEGAL_TRANSFER_GROUP_CHAT_ID: int = _env_int("LEGAL_TRANSFER_GROUP_CHAT_ID", 0)

# Direct-message manager (single user) — used by registration notifications.
# 0 = unconfigured.
MANAGER_CHAT_ID: int = _env_int("MANAGER_CHAT_ID", 0)

# Owner daily-brief targets — comma-separated list of chat IDs (negative for
# groups, positive for individual users). Each receives the 09:00 Tashkent
# morning reconciliation DM. Empty → feature inert. Notion Command Center
# backlog A2 (2026-05-11). Test target: -1003933938202 (supergroup).
# Once verified, swap to or add the father's user ID, e.g. "-1003,652...".
OWNER_DAILY_BRIEF_TARGETS: list[int] = _env_int_list("OWNER_DAILY_BRIEF_TARGETS")

# 1C handlers — tagged in every Sotuv-group order message so they don't
# forget to enter the order into 1C and reply with the confirmed Excel.
# Pairs of (telegram_id, display_name). Defaults to Alisher + Ibrat.
# Override via env: ONEC_HANDLER_IDS="123,456" (display names stay as fallback).
ONEC_HANDLERS: list[tuple[int, str]] = [
    (1914160011, "Alisher"),
    (7650055227, "Ibrat"),
]
_onec_override = _env_int_list("ONEC_HANDLER_IDS")
if _onec_override:
    # Preserve display names where ID matches the default pair, else label
    # unknown IDs as "1C". Restart required to pick up env changes.
    _default_map = {tg_id: name for tg_id, name in ONEC_HANDLERS}
    ONEC_HANDLERS = [(tg_id, _default_map.get(tg_id, "1C")) for tg_id in _onec_override]


def legal_transfer_target() -> int:
    """Resolve the chat ID for legal-transfer notifications, falling back
    to the cashier group if LEGAL_TRANSFER_GROUP_CHAT_ID is unset.
    Centralizes the OR-fallback logic that was duplicated across
    bot/handlers/cashier.py and backend/routers/payments.py.
    """
    return LEGAL_TRANSFER_GROUP_CHAT_ID or CASHIER_GROUP_CHAT_ID
