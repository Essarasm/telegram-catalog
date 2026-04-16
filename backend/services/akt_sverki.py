"""Акт сверки — FIFO-based allocation of payments to orders per client+currency.

Returns a unified timeline (orders + payments, chronological), a per-event
running balance, FIFO links (which payments covered which orders, and
vice versa), and the current balance state (clean / advance / debt).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable

from backend.database import get_db


def _as_float(x) -> float:
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def _fetch_events(conn, client_ids: list[int], currency: str) -> list[dict]:
    """Pull orders + payments for the given client ids in the given currency.

    Orders: use total_sum_currency if currency=USD, total_sum if UZS.
    Payments: amount_currency if present else amount_local.
    """
    placeholders = ",".join("?" * len(client_ids))
    events: list[dict] = []

    if currency == "USD":
        order_rows = conn.execute(
            f"""SELECT id, doc_date, doc_time, doc_number_1c,
                       COALESCE(total_sum_currency, 0) AS amount
                FROM real_orders
                WHERE client_id IN ({placeholders})
                  AND currency = 'USD'
                  AND COALESCE(total_sum_currency, 0) > 0""",
            tuple(client_ids),
        ).fetchall()
    else:
        order_rows = conn.execute(
            f"""SELECT id, doc_date, doc_time, doc_number_1c,
                       COALESCE(total_sum, 0) AS amount
                FROM real_orders
                WHERE client_id IN ({placeholders})
                  AND (currency IS NULL OR currency = 'UZS')
                  AND COALESCE(total_sum, 0) > 0""",
            tuple(client_ids),
        ).fetchall()

    for r in order_rows:
        events.append({
            "type": "order",
            "id": r["id"],
            "doc_number": r["doc_number_1c"],
            "date": r["doc_date"],
            "time": (r["doc_time"] or "")[:5],
            "amount": _as_float(r["amount"]),
            "currency": currency,
        })

    pay_rows = conn.execute(
        f"""SELECT id, doc_date, doc_time, doc_number_1c,
                   amount_local, amount_currency, currency
            FROM client_payments
            WHERE client_id IN ({placeholders})
              AND UPPER(COALESCE(currency, 'UZS')) = ?""",
        tuple(client_ids) + (currency,),
    ).fetchall()
    for r in pay_rows:
        amt = _as_float(r["amount_currency"]) or _as_float(r["amount_local"])
        if amt <= 0:
            continue
        events.append({
            "type": "payment",
            "id": r["id"],
            "doc_number": r["doc_number_1c"],
            "date": r["doc_date"],
            "time": (r["doc_time"] or "")[:5],
            "amount": amt,
            "currency": currency,
        })

    events.sort(key=lambda e: (e["date"] or "", e["time"] or "", e["id"]))
    return events


def _allocate_fifo(events: list[dict]) -> dict:
    """Walk the events; maintain an advance pool and an open-orders queue.

    Each event gets:
      - running_balance after the event (advance - sum(remaining))
      - for orders: remaining, paid_by (list of {payment_id, amount, date})
      - for payments: covers (list of {order_id, amount, date}),
        advance_created
    Returns: {events, current_balance, oldest_debt_date, total_debt,
              advance, open_orders, closed_orders}
    """
    open_orders: list[dict] = []  # refs into events (order rows)
    advance = 0.0

    for e in events:
        if e["type"] == "order":
            e.setdefault("paid_by", [])
            e["remaining"] = e["amount"]
            # First consume any advance sitting on the account
            if advance > 0:
                used = min(advance, e["remaining"])
                advance -= used
                e["remaining"] -= used
                e["paid_by"].append({
                    "kind": "advance",
                    "amount": used,
                    "date": e["date"],
                })
            if e["remaining"] > 0:
                open_orders.append(e)
        elif e["type"] == "payment":
            e.setdefault("covers", [])
            to_allocate = e["amount"]
            for order in list(open_orders):
                if to_allocate <= 0:
                    break
                used = min(order["remaining"], to_allocate)
                order["remaining"] -= used
                order["paid_by"].append({
                    "kind": "payment",
                    "payment_id": e["id"],
                    "amount": used,
                    "date": e["date"],
                })
                e["covers"].append({
                    "order_id": order["id"],
                    "order_doc": order.get("doc_number"),
                    "order_date": order["date"],
                    "amount": used,
                    "fully_closed": order["remaining"] <= 0.001,
                })
                to_allocate -= used
                if order["remaining"] <= 0.001:
                    open_orders.remove(order)
            e["advance_created"] = to_allocate
            advance += to_allocate
        # running balance after the event
        open_sum = sum(o["remaining"] for o in open_orders)
        e["running_balance"] = round(advance - open_sum, 2)

    total_debt = round(sum(o["remaining"] for o in open_orders), 2)
    oldest_debt = None
    if open_orders:
        oldest_debt = min(o["date"] for o in open_orders)

    return {
        "events": events,
        "current_balance": round(advance - total_debt, 2),
        "advance": round(advance, 2) if advance > 0 else 0.0,
        "total_debt": total_debt,
        "oldest_debt_date": oldest_debt,
        "open_orders_count": len(open_orders),
    }


def _state_from_totals(total_debt: float, advance: float,
                       oldest_debt_date: str | None) -> dict:
    """Classify the current state for the hero card.

    Buckets by age of the oldest open debt:
      - clean (no debt, no advance)
      - advance (positive balance)
      - debt_0_14
      - debt_15_29
      - debt_30_plus
    """
    if total_debt <= 0.001 and advance > 0.001:
        return {"code": "advance", "days_overdue": 0, "advance": advance}
    if total_debt <= 0.001:
        return {"code": "clean", "days_overdue": 0}

    days = 0
    if oldest_debt_date:
        try:
            d = datetime.strptime(oldest_debt_date, "%Y-%m-%d").date()
            days = (date.today() - d).days
        except (ValueError, TypeError):
            days = 0

    if days < 15:
        code = "debt_0_14"
    elif days < 30:
        code = "debt_15_29"
    else:
        code = "debt_30_plus"
    return {
        "code": code, "days_overdue": days,
        "debt": total_debt, "since": oldest_debt_date,
    }


def build(client_ids: list[int], currency: str = "UZS",
          events_limit: int | None = None) -> dict:
    """Public: build the full акт-сверки payload for a client (one currency)."""
    if not client_ids:
        return {"ok": True, "linked": False, "currency": currency,
                "events": [], "state": {"code": "clean", "days_overdue": 0}}

    conn = get_db()
    try:
        events = _fetch_events(conn, client_ids, currency)
    finally:
        conn.close()

    allocated = _allocate_fifo(events)
    state = _state_from_totals(
        allocated["total_debt"], allocated["advance"],
        allocated["oldest_debt_date"],
    )

    events_out = allocated["events"]
    if events_limit and len(events_out) > events_limit:
        events_out = events_out[-events_limit:]

    return {
        "ok": True,
        "linked": True,
        "currency": currency,
        "state": state,
        "current_balance": allocated["current_balance"],
        "total_debt": allocated["total_debt"],
        "advance": allocated["advance"],
        "oldest_debt_date": allocated["oldest_debt_date"],
        "events": events_out,
        "total_events": len(allocated["events"]),
    }
