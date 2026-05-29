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

# Admin group — registration notifications, support DMs, role/admin commands,
# group-health alerts, PAT rotation reminder. Human-attention surface.
ADMIN_GROUP_CHAT_ID: int = _env_int("ADMIN_GROUP_CHAT_ID", -5224656051)

# Platform-ops group — automated cron output: offsite DB backups, payment
# reconciler, payment-notif sweep, weekly Client Master cycle, data-consistency
# audit, error alerts. Split off from ADMIN_GROUP_CHAT_ID on 2026-05-16 to
# isolate machine chatter from human-facing admin traffic.
PLATFORM_OPS_GROUP_CHAT_ID: int = _env_int("PLATFORM_OPS_GROUP_CHAT_ID", -1003987299154)

# Reconciliation group — dedicated morning cashier↔1C mismatch report
# (08:00 Tashkent, after Alisher's EOD /cash upload). Bookkeeper + owner
# review yesterday's matched / bot_only / kassa_only rows here. Split off
# from PLATFORM_OPS on 2026-05-18 because mismatch review needs a human-
# attention surface, not the silent ops feed.
RECONCILIATION_GROUP_CHAT_ID: int = _env_int("RECONCILIATION_GROUP_CHAT_ID", -1003949360710)

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

# P2P card-to-card review group. Hosts only the P2P payment notifications
# (confirm/reject buttons). Falls back to CASHIER_GROUP_CHAT_ID at the
# consumer site (see p2p_target) so deployments that never set it keep the
# historical behavior of posting P2P into the cashier group. Split out from
# the cashier group on 2026-05-29 so the naqd-handover queue + 19:00 cashbook
# list stay where they were. 0 = use the cashier group.
P2P_GROUP_CHAT_ID: int = _env_int("P2P_GROUP_CHAT_ID", 0)

# Bank-transfer group (Uchqun + Shuhrat). Sister to cashier; routes through
# bot/handlers/bank_transfer.py. 0 = unconfigured.
BANK_TRANSFER_GROUP_CHAT_ID: int = _env_int("BANK_TRANSFER_GROUP_CHAT_ID", 0)

# Legal-transfer notifications group. Falls back to CASHIER_GROUP_CHAT_ID
# at consumer site if not set, preserving the historical behavior in
# backend/routers/payments.py and bot/handlers/cashier.py.
LEGAL_TRANSFER_GROUP_CHAT_ID: int = _env_int("LEGAL_TRANSFER_GROUP_CHAT_ID", 0)

# Catalog photo-fill group — employees pick up /foto batches here, reply
# with phone photos (as File, not as Photo, to preserve quality) under
# each item message; bot uploads raw files to Google Drive
# `product_photos_original/Employees uploads/` for offline trimming.
# 0 = unconfigured; the /foto flow stays inert.
CATALOG_GROUP_CHAT_ID: int = _env_int("CATALOG_GROUP_CHAT_ID", 0)

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


def p2p_target() -> int:
    """Resolve the chat ID for P2P card-to-card notifications, falling back
    to the cashier group if P2P_GROUP_CHAT_ID is unset. Same OR-fallback
    shape as legal_transfer_target so older deployments don't break.
    """
    return P2P_GROUP_CHAT_ID or CASHIER_GROUP_CHAT_ID
