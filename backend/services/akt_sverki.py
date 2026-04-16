"""Акт сверки — unified dual-currency timeline with FIFO allocation.

Each event carries BOTH uzs_amount and usd_amount. Orders are one row per
real_orders.id (may have both currencies in their line items). Payments
are grouped per (date, client_id) to collapse the 1C "касса UZS + касса
USD" pair into one client-facing payment event.

FIFO runs independently per currency; each event also carries the running
balance of BOTH currencies after the event.
"""
from __future__ import annotations

from datetime import date, datetime

from backend.database import get_db


def _as_float(x) -> float:
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def _fetch_events(conn, client_ids: list[int]) -> list[dict]:
    placeholders = ",".join("?" * len(client_ids))

    # Orders: one row per real_orders.id, with both currency totals.
    order_rows = conn.execute(
        f"""SELECT id, doc_date, doc_time, doc_number_1c,
                   COALESCE(total_sum, 0) AS uzs_amount,
                   COALESCE(total_sum_currency, 0) AS usd_amount
            FROM real_orders
            WHERE client_id IN ({placeholders})
              AND (COALESCE(total_sum, 0) > 0 OR COALESCE(total_sum_currency, 0) > 0)""",
        tuple(client_ids),
    ).fetchall()

    events: list[dict] = []
    for r in order_rows:
        events.append({
            "type": "order",
            "id": r["id"],
            "doc_number": r["doc_number_1c"],
            "date": r["doc_date"],
            "uzs_amount": _as_float(r["uzs_amount"]),
            "usd_amount": _as_float(r["usd_amount"]),
        })

    # Payments: sum per (date, client_id) across all касса entries of either
    # currency — one logical "client paid money today" event.
    pay_rows = conn.execute(
        f"""SELECT doc_date,
                   SUM(CASE WHEN UPPER(COALESCE(currency,'UZS'))='UZS'
                            THEN COALESCE(amount_currency, amount_local, 0)
                            ELSE 0 END) AS uzs_amount,
                   SUM(CASE WHEN UPPER(currency)='USD'
                            THEN COALESCE(amount_currency, amount_local, 0)
                            ELSE 0 END) AS usd_amount,
                   GROUP_CONCAT(id) AS ids,
                   MAX(doc_number_1c) AS doc_number
            FROM client_payments
            WHERE client_id IN ({placeholders})
            GROUP BY doc_date""",
        tuple(client_ids),
    ).fetchall()
    for r in pay_rows:
        uzs = _as_float(r["uzs_amount"])
        usd = _as_float(r["usd_amount"])
        if uzs <= 0 and usd <= 0:
            continue
        events.append({
            "type": "payment",
            "id": "pay-" + str(r["ids"] or ""),
            "ids": [int(x) for x in (r["ids"] or "").split(",") if x.strip().isdigit()],
            "doc_number": r["doc_number"] or "",
            "date": r["doc_date"],
            "uzs_amount": uzs,
            "usd_amount": usd,
        })

    # Sort: by date asc, then type (orders before payments on same date), then id.
    def _key(e):
        return (e["date"] or "", 0 if e["type"] == "order" else 1,
                str(e.get("id", "")))
    events.sort(key=_key)
    return events


def _allocate_fifo(events: list[dict]) -> dict:
    open_uzs: list[dict] = []
    open_usd: list[dict] = []
    uzs_adv = 0.0
    usd_adv = 0.0

    for e in events:
        e["uzs_paid_by"] = []
        e["usd_paid_by"] = []
        e["uzs_covers"] = []
        e["usd_covers"] = []

        if e["type"] == "order":
            for ccy, amt_key, open_queue, adv_var in [
                ("uzs", "uzs_amount", open_uzs, "uzs_adv"),
                ("usd", "usd_amount", open_usd, "usd_adv"),
            ]:
                amt = e[amt_key]
                if amt <= 0:
                    e[f"{ccy}_remaining"] = 0
                    continue
                adv = uzs_adv if adv_var == "uzs_adv" else usd_adv
                if adv > 0:
                    used = min(adv, amt)
                    amt -= used
                    if adv_var == "uzs_adv":
                        uzs_adv -= used
                    else:
                        usd_adv -= used
                    e[f"{ccy}_paid_by"].append({
                        "kind": "advance", "amount": used, "date": e["date"],
                    })
                e[f"{ccy}_remaining"] = amt
                if amt > 0:
                    open_queue.append(e)
        elif e["type"] == "payment":
            for ccy, amt_key, open_queue, adv_var in [
                ("uzs", "uzs_amount", open_uzs, "uzs_adv"),
                ("usd", "usd_amount", open_usd, "usd_adv"),
            ]:
                to_alloc = e[amt_key]
                if to_alloc <= 0:
                    e[f"{ccy}_advance_created"] = 0
                    continue
                for order in list(open_queue):
                    if to_alloc <= 0:
                        break
                    rem_key = f"{ccy}_remaining"
                    if order[rem_key] <= 0:
                        continue
                    used = min(order[rem_key], to_alloc)
                    order[rem_key] -= used
                    to_alloc -= used
                    order[f"{ccy}_paid_by"].append({
                        "kind": "payment",
                        "payment_event_id": e["id"],
                        "amount": used,
                        "date": e["date"],
                    })
                    e[f"{ccy}_covers"].append({
                        "order_id": order["id"],
                        "order_doc": order.get("doc_number"),
                        "order_date": order["date"],
                        "amount": used,
                        "fully_closed": order[rem_key] <= 0.001,
                    })
                    if order[rem_key] <= 0.001:
                        open_queue.remove(order)
                e[f"{ccy}_advance_created"] = to_alloc
                if adv_var == "uzs_adv":
                    uzs_adv += to_alloc
                else:
                    usd_adv += to_alloc

        # Running balances after event
        e["uzs_balance"] = round(
            uzs_adv - sum(o.get("uzs_remaining", 0) or 0 for o in open_uzs), 2)
        e["usd_balance"] = round(
            usd_adv - sum(o.get("usd_remaining", 0) or 0 for o in open_usd), 2)

    uzs_debt = round(sum(o.get("uzs_remaining", 0) or 0 for o in open_uzs), 2)
    usd_debt = round(sum(o.get("usd_remaining", 0) or 0 for o in open_usd), 2)

    oldest = {
        "uzs": min((o["date"] for o in open_uzs), default=None),
        "usd": min((o["date"] for o in open_usd), default=None),
    }
    return {
        "events": events,
        "uzs_balance": round(uzs_adv - uzs_debt, 2),
        "usd_balance": round(usd_adv - usd_debt, 2),
        "uzs_debt": uzs_debt,
        "usd_debt": usd_debt,
        "uzs_advance": round(uzs_adv, 2) if uzs_adv > 0 else 0,
        "usd_advance": round(usd_adv, 2) if usd_adv > 0 else 0,
        "oldest_debt": oldest,
    }


def _state_for(debt: float, advance: float, oldest_date: str | None) -> dict:
    if debt <= 0.001 and advance > 0.001:
        return {"code": "advance", "days_overdue": 0, "advance": advance}
    if debt <= 0.001:
        return {"code": "clean", "days_overdue": 0}
    days = 0
    if oldest_date:
        try:
            d = datetime.strptime(oldest_date, "%Y-%m-%d").date()
            days = (date.today() - d).days
        except (ValueError, TypeError):
            days = 0
    if days < 15:
        code = "debt_0_14"
    elif days < 30:
        code = "debt_15_29"
    else:
        code = "debt_30_plus"
    return {"code": code, "days_overdue": days, "debt": debt, "since": oldest_date}


def build(client_ids: list[int], events_limit: int | None = None) -> dict:
    """Return a unified dual-currency акт сверки for the client."""
    if not client_ids:
        return {"ok": True, "linked": False, "events": [],
                "uzs_state": {"code": "clean", "days_overdue": 0},
                "usd_state": {"code": "clean", "days_overdue": 0}}

    conn = get_db()
    try:
        events = _fetch_events(conn, client_ids)
    finally:
        conn.close()
    allocated = _allocate_fifo(events)

    events_out = allocated["events"]
    if events_limit and len(events_out) > events_limit:
        events_out = events_out[-events_limit:]

    return {
        "ok": True,
        "linked": True,
        "events": events_out,
        "total_events": len(allocated["events"]),
        "uzs_state": _state_for(allocated["uzs_debt"], allocated["uzs_advance"],
                                allocated["oldest_debt"]["uzs"]),
        "usd_state": _state_for(allocated["usd_debt"], allocated["usd_advance"],
                                allocated["oldest_debt"]["usd"]),
        "uzs_balance": allocated["uzs_balance"],
        "usd_balance": allocated["usd_balance"],
        "uzs_debt": allocated["uzs_debt"],
        "usd_debt": allocated["usd_debt"],
        "uzs_advance": allocated["uzs_advance"],
        "usd_advance": allocated["usd_advance"],
    }
