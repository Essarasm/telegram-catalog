"""Client-facing "payment received" notifications (Session N).

Flow:
    /cash import  → queue_from_cash_payments()  → pending_payment_notifications
    /debtors import → fire_pending_for_today() (background thread)
                   → sent_payment_notifications + Telegram sendMessage,
                     or missed_notifications on failure
    18:00 Tashkent sweeper → sweep_stale() drops rows whose kassa_date is
                   >24h old and still pending (no debtors refresh landed).

Sign convention: client_debts.debt_uzs/debt_usd positive = client owes us.
Message says "Yangi qoldiq: N so'm" — N is the outstanding debt after
the payment was applied.
"""
from __future__ import annotations

import os
import logging
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional

import httpx

from backend.database import get_db, get_sibling_client_ids

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TASHKENT = ZoneInfo("Asia/Tashkent")

STALE_AFTER_HOURS = 24


def _tashkent_today_iso() -> str:
    return datetime.now(TASHKENT).date().isoformat()


def _fmt_uzs(n) -> str:
    return f"{round(float(n or 0)):,}".replace(",", " ") + " so'm"


def _fmt_usd(n) -> str:
    return f"{float(n or 0):,.2f} $"


def _display_date(iso_date: str) -> str:
    try:
        y, m, d = iso_date.split("-")
        return f"{d}.{m}.{y}"
    except Exception:
        return iso_date


def _resolve_telegram_ids(conn, client_id: int) -> List[int]:
    """Return every approved telegram_id bound to this client or its siblings.
    One shop can have up to 5 phone registrations sharing one client_id_1c —
    all of them should hear about the payment."""
    ids = get_sibling_client_ids(conn, client_id)
    if not ids:
        return []
    placeholders = ",".join("?" * len(ids))
    rows = conn.execute(
        f"""SELECT DISTINCT u.telegram_id
            FROM users u
            WHERE u.client_id IN ({placeholders})
              AND u.telegram_id IS NOT NULL
              AND COALESCE(u.is_approved, 0) = 1""",
        tuple(ids),
    ).fetchall()
    return [r["telegram_id"] for r in rows]


def _lookup_debt(conn, client_id: int) -> Dict[str, float]:
    """Return {'uzs': ..., 'usd': ...} for this client (or 0/0 if settled /
    not present). Uses sibling IDs to catch multi-phone clients."""
    ids = get_sibling_client_ids(conn, client_id) or [client_id]
    placeholders = ",".join("?" * len(ids))
    row = conn.execute(
        f"""SELECT COALESCE(SUM(debt_uzs), 0) AS uzs,
                   COALESCE(SUM(debt_usd), 0) AS usd
            FROM client_debts
            WHERE client_id IN ({placeholders})""",
        tuple(ids),
    ).fetchone()
    return {
        "uzs": float(row["uzs"] or 0) if row else 0.0,
        "usd": float(row["usd"] or 0) if row else 0.0,
    }


def queue_from_cash_payments(conn, payments: List[Dict]) -> Dict[str, int]:
    """Enqueue payment-received notifications.

    Call site: inside apply_cash_import AFTER per-row INSERTs and BEFORE
    commit (shares the same connection + transaction). For every payment:
    resolve client_id → telegram_id(s), then INSERT OR IGNORE into
    pending_payment_notifications. Unmatched or unbound clients are logged
    to missed_notifications so the admin can follow up via /missed.

    Zero-amount rows and rows without doc_no/doc_date are silently skipped.
    """
    queued = 0
    missed_unmatched = 0
    missed_no_bind = 0

    for p in payments:
        client_name = (p.get("client_name_1c") or "").strip()
        if not client_name:
            continue

        currency = (p.get("currency") or "UZS").upper()
        if currency == "USD":
            amount = float(p.get("amount_currency") or 0)
        else:
            amount = float(p.get("amount_local") or 0)
        if amount <= 0:
            continue

        doc_no = (p.get("doc_number_1c") or "").strip()
        doc_date = (p.get("doc_date") or "").strip()
        if not doc_no or not doc_date:
            continue

        row = conn.execute(
            """SELECT client_id FROM client_payments
               WHERE doc_number_1c = ? AND doc_date = ?""",
            (doc_no, doc_date),
        ).fetchone()
        client_id = row["client_id"] if row else None

        if not client_id:
            conn.execute(
                """INSERT INTO missed_notifications
                   (kassa_doc_no, kassa_date, client_name_1c,
                    currency, amount, reason, detail)
                   VALUES (?, ?, ?, ?, ?, 'unmatched_name', ?)""",
                (doc_no, doc_date, client_name, currency, amount,
                 "Client name not matched to allowed_clients."),
            )
            missed_unmatched += 1
            continue

        telegram_ids = _resolve_telegram_ids(conn, client_id)
        if not telegram_ids:
            conn.execute(
                """INSERT INTO missed_notifications
                   (kassa_doc_no, kassa_date, client_name_1c, client_id,
                    currency, amount, reason, detail)
                   VALUES (?, ?, ?, ?, ?, ?, 'no_telegram_bind', ?)""",
                (doc_no, doc_date, client_name, client_id,
                 currency, amount,
                 "No approved Telegram user bound to this client."),
            )
            missed_no_bind += 1
            continue

        for tg_id in telegram_ids:
            cur = conn.execute(
                """INSERT OR IGNORE INTO pending_payment_notifications
                   (telegram_id, client_id, client_name_1c,
                    kassa_doc_no, kassa_date, currency, amount)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (tg_id, client_id, client_name, doc_no, doc_date,
                 currency, amount),
            )
            if cur.rowcount > 0:
                queued += 1

    logger.info(
        f"payment_notifications.queue: queued={queued} "
        f"missed_unmatched={missed_unmatched} missed_no_bind={missed_no_bind}"
    )
    return {
        "queued": queued,
        "missed_unmatched": missed_unmatched,
        "missed_no_bind": missed_no_bind,
    }


def _compose_message(legs: List[Dict], debt_uzs: float, debt_usd: float) -> str:
    """Build the Uzbek Latin message body. `legs` = pending rows for ONE
    telegram_id, potentially spanning multiple currencies and dates."""
    uzs_legs = [l for l in legs if l["currency"] == "UZS"]
    usd_legs = [l for l in legs if l["currency"] == "USD"]
    uzs_total = sum(float(l["amount"]) for l in uzs_legs)
    usd_total = sum(float(l["amount"]) for l in usd_legs)

    # Use the most recent kassa_date across all legs as the display date.
    date_iso = max(l["kassa_date"] for l in legs)
    display_date = _display_date(date_iso)

    lines = ["To'lov qabul qilindi", "", f"Sana: {display_date}"]

    if uzs_total > 0 and usd_total > 0:
        lines.append(f"UZS: {_fmt_uzs(uzs_total)}")
        lines.append(f"USD: {_fmt_usd(usd_total)}")
        lines.append("")
        lines.append("Yangi qoldiq")
        lines.append(f"UZS: {_fmt_uzs(debt_uzs)}")
        lines.append(f"USD: {_fmt_usd(debt_usd)}")
    elif uzs_total > 0:
        lines.append(f"Summa: {_fmt_uzs(uzs_total)}")
        lines.append(f"Yangi qoldiq: {_fmt_uzs(debt_uzs)}")
    else:
        lines.append(f"Summa: {_fmt_usd(usd_total)}")
        lines.append(f"Yangi qoldiq: {_fmt_usd(debt_usd)}")

    return "\n".join(lines)


def _send_telegram(telegram_id: int, text: str) -> Dict:
    """POST to Telegram sendMessage. Returns {'ok': bool, 'message_id': int?,
    'error': str?}."""
    if not BOT_TOKEN:
        return {"ok": False, "error": "BOT_TOKEN not set"}
    try:
        resp = httpx.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": telegram_id, "text": text},
            timeout=10,
        )
        j = resp.json() if resp.content else {}
        if j.get("ok"):
            mid = (j.get("result") or {}).get("message_id")
            return {"ok": True, "message_id": mid}
        return {"ok": False, "error": j.get("description") or f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def fire_pending_for_today_sync() -> Dict:
    """Fire pending notifications whose kassa_date is <= today Tashkent.

    Runs in a background thread (see apply_debtors_import hook). For each
    (telegram_id) group: look up current debt, compose message, send, move
    to sent/missed. Never raises — all errors fall into missed_notifications.
    """
    today_iso = _tashkent_today_iso()
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT id, telegram_id, client_id, client_name_1c,
                      kassa_doc_no, kassa_date, currency, amount
               FROM pending_payment_notifications
               WHERE kassa_date <= ?
               ORDER BY telegram_id, kassa_date""",
            (today_iso,),
        ).fetchall()

        if not rows:
            logger.info("payment_notifications.fire: no pending rows")
            return {"fired": 0, "failed": 0, "telegram_ids": 0}

        groups: Dict[int, List[Dict]] = defaultdict(list)
        for r in rows:
            groups[r["telegram_id"]].append({
                "id": r["id"],
                "client_id": r["client_id"],
                "client_name_1c": r["client_name_1c"],
                "kassa_doc_no": r["kassa_doc_no"],
                "kassa_date": r["kassa_date"],
                "currency": r["currency"],
                "amount": r["amount"],
            })

        fired = 0
        failed = 0
        for tg_id, legs in groups.items():
            client_id = legs[0]["client_id"]
            debts = _lookup_debt(conn, client_id)
            text = _compose_message(legs, debts["uzs"], debts["usd"])
            result = _send_telegram(tg_id, text)

            leg_ids = [l["id"] for l in legs]
            placeholders = ",".join("?" * len(leg_ids))

            if result.get("ok"):
                for l in legs:
                    conn.execute(
                        """INSERT OR IGNORE INTO sent_payment_notifications
                           (telegram_id, client_id, client_name_1c,
                            kassa_doc_no, kassa_date, currency, amount,
                            debt_uzs_after, debt_usd_after, telegram_message_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (tg_id, l["client_id"], l["client_name_1c"],
                         l["kassa_doc_no"], l["kassa_date"],
                         l["currency"], l["amount"],
                         debts["uzs"], debts["usd"],
                         result.get("message_id")),
                    )
                conn.execute(
                    f"DELETE FROM pending_payment_notifications WHERE id IN ({placeholders})",
                    tuple(leg_ids),
                )
                fired += len(legs)
            else:
                err = result.get("error") or "unknown"
                for l in legs:
                    conn.execute(
                        """INSERT INTO missed_notifications
                           (kassa_doc_no, kassa_date, client_name_1c, client_id,
                            telegram_id, currency, amount, reason, detail)
                           VALUES (?, ?, ?, ?, ?, ?, ?, 'bot_send_failed', ?)""",
                        (l["kassa_doc_no"], l["kassa_date"],
                         l["client_name_1c"], l["client_id"],
                         tg_id, l["currency"], l["amount"], err[:500]),
                    )
                conn.execute(
                    f"DELETE FROM pending_payment_notifications WHERE id IN ({placeholders})",
                    tuple(leg_ids),
                )
                failed += len(legs)

        conn.commit()
        logger.info(
            f"payment_notifications.fire: fired={fired} failed={failed} "
            f"telegram_ids={len(groups)}"
        )
        return {"fired": fired, "failed": failed, "telegram_ids": len(groups)}
    except Exception as e:
        logger.error(f"payment_notifications.fire: unexpected error: {e}")
        return {"fired": 0, "failed": 0, "telegram_ids": 0, "error": str(e)}
    finally:
        conn.close()


def fire_pending_for_today_async() -> None:
    """Kick off fire_pending_for_today_sync in a daemon thread so the
    /debtors HTTP handler returns immediately. Failures inside the thread
    are swallowed to missed_notifications — caller never sees them."""
    t = threading.Thread(
        target=fire_pending_for_today_sync,
        name="payment-notif-fire",
        daemon=True,
    )
    t.start()


def sweep_stale() -> Dict:
    """18:00 Tashkent sweeper. Pending rows whose queued_at is >24h old and
    still present (no /debtors landed to fire them) → missed_notifications
    with reason='balance_missing_after_24h'. Clears pending table of orphans
    so it doesn't grow unbounded."""
    cutoff = (datetime.now(TASHKENT) - timedelta(hours=STALE_AFTER_HOURS)).isoformat(" ")
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT id, telegram_id, client_id, client_name_1c,
                      kassa_doc_no, kassa_date, currency, amount, queued_at
               FROM pending_payment_notifications
               WHERE queued_at < ?""",
            (cutoff,),
        ).fetchall()
        if not rows:
            return {"swept": 0}

        ids = [r["id"] for r in rows]
        for r in rows:
            conn.execute(
                """INSERT INTO missed_notifications
                   (kassa_doc_no, kassa_date, client_name_1c, client_id,
                    telegram_id, currency, amount, reason, detail)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'balance_missing_after_24h', ?)""",
                (r["kassa_doc_no"], r["kassa_date"], r["client_name_1c"],
                 r["client_id"], r["telegram_id"], r["currency"], r["amount"],
                 f"Pending since {r['queued_at']} — no /debtors import landed to provide updated balance."),
            )
        placeholders = ",".join("?" * len(ids))
        conn.execute(
            f"DELETE FROM pending_payment_notifications WHERE id IN ({placeholders})",
            tuple(ids),
        )
        conn.commit()
        logger.info(f"payment_notifications.sweep: {len(ids)} stale pending rows → missed")
        return {"swept": len(ids)}
    finally:
        conn.close()


def list_unresolved_missed(limit: int = 30) -> List[Dict]:
    """For the /missed admin command — latest unresolved rows."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT id, kassa_doc_no, kassa_date, client_name_1c, client_id,
                      telegram_id, currency, amount, reason, detail, created_at
               FROM missed_notifications
               WHERE resolved_at IS NULL
               ORDER BY created_at DESC
               LIMIT ?""",
            (int(limit),),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def count_unresolved_missed_by_reason() -> Dict[str, int]:
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT reason, COUNT(*) AS n
               FROM missed_notifications
               WHERE resolved_at IS NULL
               GROUP BY reason"""
        ).fetchall()
        return {r["reason"]: int(r["n"]) for r in rows}
    finally:
        conn.close()
