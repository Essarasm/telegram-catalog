"""Stock alert service — identifies active products that are out of stock or running low.

"Active" = had positive stock within the last 60 days (was in uncle's /stock
upload with qty > 0). This uses the stock file as source of truth — uncle only
uploads products he actively stocks. Fallback criteria for products without
stock_last_positive_at: sold in last 3 months or supplied recently.
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from backend.database import get_db

logger = logging.getLogger(__name__)

ACTIVE_STOCK_DAYS = 60
SOLD_MONTHS = 3
SUPPLY_MONTHS = 6

# 09:00 daily inventory alert: cumulative across the work week (Mon–Sat,
# Sundays skipped by the cron). Window resets at Monday 00:00 Tashkent. An
# item drops off the list when restocked (qty > 0) — naturally enforced by
# the existing `qty < 1` filter on `out_of_stock`.
TASHKENT_TZ = ZoneInfo("Asia/Tashkent")
UZ_DAYS_FULL = [
    "Dushanba", "Seshanba", "Chorshanba", "Payshanba",
    "Juma", "Shanba", "Yakshanba",
]


def _current_week_start_utc_str() -> str:
    """Monday 00:00 Tashkent → UTC string in SQLite's `datetime('now')` format."""
    now_tk = datetime.now(TASHKENT_TZ)
    monday_tk = (now_tk - timedelta(days=now_tk.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return monday_tk.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _current_week_start_tk_date_str() -> str:
    """Monday of this week in Tashkent — `YYYY-MM-DD` for `real_orders.doc_date` comparisons."""
    now_tk = datetime.now(TASHKENT_TZ)
    monday_tk = (now_tk - timedelta(days=now_tk.weekday()))
    return monday_tk.strftime("%Y-%m-%d")


def _stockout_to_tk_day(stockout_at_str: Optional[str]) -> Tuple[int, str]:
    """Parse a UTC `YYYY-MM-DD HH:MM:SS` stamp → (weekday 0–6, dd/mm) in Tashkent."""
    if not stockout_at_str:
        return -1, ""
    try:
        dt_utc = datetime.strptime(stockout_at_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        dt_tk = dt_utc.astimezone(TASHKENT_TZ)
        return dt_tk.weekday(), dt_tk.strftime("%d/%m")
    except (ValueError, TypeError):
        return -1, ""


def get_active_product_ids(conn) -> set:
    """Find products that are genuinely active (not dead catalog weight).

    Primary criterion: had positive stock within last 30 days
    (stock_last_positive_at). This means uncle included it in a recent
    /stock upload with qty > 0 — his own curation of active inventory.

    Fallback criteria (for products without stock history):
    1. Had a sale (real_order_item) in the last 3 months
    2. Had a supply delivery in the last 6 months
    """
    active_ids = set()

    # Primary: had positive stock within last 30 days
    try:
        stocked = conn.execute(
            f"""SELECT id FROM products
                WHERE is_active = 1
                  AND stock_last_positive_at IS NOT NULL
                  AND datetime(stock_last_positive_at) >= datetime('now', '-{ACTIVE_STOCK_DAYS} days')"""
        ).fetchall()
        for r in stocked:
            active_ids.add(r["id"])
    except Exception as e:
        logger.warning(f"Could not query stock_last_positive_at: {e}")

    # Also include products currently in stock (even without the timestamp)
    try:
        in_stock = conn.execute(
            "SELECT id FROM products WHERE is_active = 1 AND stock_quantity > 0"
        ).fetchall()
        for r in in_stock:
            active_ids.add(r["id"])
    except Exception:
        pass

    # Fallback: sold recently
    try:
        sold = conn.execute(
            f"""SELECT DISTINCT roi.product_id
                FROM real_order_items roi
                JOIN real_orders ro ON ro.id = roi.real_order_id
                WHERE ro.doc_date >= date('now', '-{SOLD_MONTHS} months')
                  AND roi.product_id IS NOT NULL"""
        ).fetchall()
        for r in sold:
            active_ids.add(r["product_id"])
    except Exception as e:
        logger.warning(f"Could not query real_order_items: {e}")

    # Fallback: supplied recently
    try:
        supplied = conn.execute(
            f"""SELECT DISTINCT soi.matched_product_id as pid
                FROM supply_order_items soi
                JOIN supply_orders so ON so.id = soi.supply_order_id
                WHERE so.doc_date >= date('now', '-{SUPPLY_MONTHS} months')
                  AND soi.matched_product_id IS NOT NULL"""
        ).fetchall()
        for r in supplied:
            active_ids.add(r["pid"])
    except Exception as e:
        logger.warning(f"Could not query supply_order_items: {e}")

    return active_ids


def _get_weekly_top_sellers(conn, week_start_tk_date: str, limit: int = 5) -> list:
    """Top N products by units sold since this week's Monday (Tashkent).

    Joins `real_order_items` × `real_orders` filtered by `doc_date >=` Monday;
    groups by product_id; sorts by total units descending. Items with NULL
    `product_id` (unmatched 1C names) are ignored — we can't display them.
    """
    try:
        rows = conn.execute(
            """SELECT roi.product_id,
                      COALESCE(p.name_display, p.name) AS display_name,
                      p.unit AS unit,
                      SUM(roi.quantity) AS units_sold
               FROM real_order_items roi
               JOIN real_orders ro ON ro.id = roi.real_order_id
               LEFT JOIN products p ON p.id = roi.product_id
               WHERE ro.doc_date >= ?
                 AND roi.product_id IS NOT NULL
                 AND roi.quantity > 0
               GROUP BY roi.product_id
               ORDER BY units_sold DESC
               LIMIT ?""",
            (week_start_tk_date, limit),
        ).fetchall()
    except Exception as e:
        logger.warning(f"weekly top sellers query failed: {e}")
        return []

    return [
        {
            "product_id": r["product_id"],
            "name": (r["display_name"] or "—")[:50],
            "unit": r["unit"] or "шт",
            "units_sold": float(r["units_sold"] or 0),
        }
        for r in rows
    ]


def get_stock_alerts(conn=None, week_start_utc: Optional[str] = None,
                     week_start_tk_date: Optional[str] = None) -> dict:
    """Generate stock alerts for active products.

    Args:
        conn: optional DB connection (for tests).
        week_start_utc: optional override for the weekly stockout cutoff (UTC string).
            Defaults to Monday 00:00 Tashkent expressed as UTC.
        week_start_tk_date: optional override for the top-sellers cutoff
            (`YYYY-MM-DD` Tashkent date). Defaults to this Monday in Tashkent.

    Returns:
        {
            "active_count": int,
            "out_of_stock": [{name, producer, last_sold, last_supplied, stockout_at}, ...],
            "weekly_out_of_stock": subset of out_of_stock stamped on/after this
                week's Monday 00:00 Tashkent. Cumulative across Mon–Sat; resets
                Monday. Restocked items drop because out_of_stock filters qty<1.
            "weekly_top_sellers": top 5 products by units sold this week (Mon–Sat).
            "running_low": [{name, producer, qty, last_sold, last_supplied}, ...],
            "healthy_count": int,
        }
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db()

    try:
        active_ids = get_active_product_ids(conn)
        if not active_ids:
            return {
                "active_count": 0,
                "out_of_stock": [],
                "weekly_out_of_stock": [],
                "weekly_top_sellers": [],
                "running_low": [],
                "healthy_count": 0,
            }

        placeholders = ",".join("?" for _ in active_ids)
        id_list = list(active_ids)

        products = conn.execute(
            f"""SELECT p.id, p.name, p.name_display, pr.name as producer_name,
                       p.stock_quantity, p.stock_status, p.unit, p.stockout_at
                FROM products p
                JOIN producers pr ON pr.id = p.producer_id
                WHERE p.is_active = 1 AND p.id IN ({placeholders})""",
            id_list,
        ).fetchall()

        # Get last sold date per product
        last_sold = {}
        try:
            sold_rows = conn.execute(
                f"""SELECT roi.product_id, MAX(ro.doc_date) as last_date
                    FROM real_order_items roi
                    JOIN real_orders ro ON ro.id = roi.real_order_id
                    WHERE roi.product_id IN ({placeholders})
                    GROUP BY roi.product_id""",
                id_list,
            ).fetchall()
            for r in sold_rows:
                last_sold[r["product_id"]] = r["last_date"]
        except Exception:
            pass

        # Get last supplied date per product
        last_supplied = {}
        try:
            supply_rows = conn.execute(
                f"""SELECT soi.matched_product_id as pid, MAX(so.doc_date) as last_date
                    FROM supply_order_items soi
                    JOIN supply_orders so ON so.id = soi.supply_order_id
                    WHERE soi.matched_product_id IN ({placeholders})
                    GROUP BY soi.matched_product_id""",
                id_list,
            ).fetchall()
            for r in supply_rows:
                last_supplied[r["pid"]] = r["last_date"]
        except Exception:
            pass

        out_of_stock = []
        running_low = []
        healthy = 0

        for p in products:
            pid = p["id"]
            raw_qty = p["stock_quantity"]
            qty = float(raw_qty) if raw_qty is not None else 0
            name = p["name"] or p["name_display"]
            info = {
                "id": pid,
                "name": name[:50],
                "producer": p["producer_name"] or "",
                "qty": qty,
                "unit": p["unit"] or "шт",
                "last_sold": last_sold.get(pid, "—"),
                "last_supplied": last_supplied.get(pid, "—"),
                "stockout_at": p["stockout_at"],
            }

            if qty < 1:
                out_of_stock.append(info)
            elif qty <= 3:
                running_low.append(info)
            else:
                healthy += 1

        # Sort: most recently sold first (urgent = items customers actually buy)
        out_of_stock.sort(key=lambda x: x["last_sold"] or "", reverse=True)
        running_low.sort(key=lambda x: x["qty"])

        # Weekly cumulative: items stamped on/after this week's Monday 00:00
        # Tashkent. SQLite datetime('now') writes UTC, so the cutoff is UTC too.
        cutoff = week_start_utc if week_start_utc is not None else _current_week_start_utc_str()
        weekly_out_of_stock = [
            item for item in out_of_stock
            if item["stockout_at"] and item["stockout_at"] >= cutoff
        ]
        weekly_out_of_stock.sort(key=lambda x: x["stockout_at"] or "", reverse=True)

        # Top sellers this week — uses Tashkent date for `real_orders.doc_date`.
        date_cutoff = week_start_tk_date if week_start_tk_date is not None else _current_week_start_tk_date_str()
        weekly_top_sellers = _get_weekly_top_sellers(conn, date_cutoff, limit=5)

        return {
            "active_count": len(products),
            "out_of_stock": out_of_stock,
            "weekly_out_of_stock": weekly_out_of_stock,
            "weekly_top_sellers": weekly_top_sellers,
            "running_low": running_low,
            "healthy_count": healthy,
        }
    finally:
        if own_conn:
            conn.close()


def _chunk_lines(header: str, items_lines: list[str], max_chars: int = 3800) -> list[str]:
    """Break a long item list into Telegram-safe chunks, each with a header."""
    chunks = []
    current = [header]
    current_len = len(header)
    for ln in items_lines:
        if current_len + len(ln) + 1 > max_chars and len(current) > 1:
            chunks.append("\n".join(current))
            current = [f"{header} (davom)"]
            current_len = len(current[0])
        current.append(ln)
        current_len += len(ln) + 1
    if len(current) > 1:
        chunks.append("\n".join(current))
    return chunks


def format_daily_inventory_message(alerts: dict) -> list[str]:
    """09:00 cron message — items that ran out so far this work week (Mon–Sat)
    plus the top 5 best-selling products this week.

    The TUGAGAN list is cumulative within the week and resets Monday morning;
    restocked items drop automatically (qty<1 filter). Items are grouped under
    `<Day> — dd/mm` subheaders, Monday → Saturday. The top-5 section reflects
    `real_orders` shipped since this Monday. Returns [] only when both halves
    are empty (typical Monday morning before any /stock or /realorders runs).
    """
    if alerts.get("active_count", 0) == 0:
        return []
    weekly_out = alerts.get("weekly_out_of_stock", [])
    top_sellers = alerts.get("weekly_top_sellers", [])
    if not weekly_out and not top_sellers:
        return []

    summary_parts = [
        f"📦 <b>Kunlik inventarizatsiya xabari</b>\n",
        f"Faol mahsulotlar: <b>{alerts['active_count']}</b>",
        f"🟢 Yetarli: {alerts['healthy_count']}",
        f"🟡 Kam qoldi: {len(alerts['running_low'])}",
        f"🔴 Tugagan: {len(alerts['out_of_stock'])} (bu haftada: <b>{len(weekly_out)}</b>)",
        "",
        f"🔍 To'liq ro'yxat: <code>/stockalert tugagan</code>",
    ]
    messages = ["\n".join(summary_parts)]

    # ── Top 5 sellers this week ──────────────────────────────────────
    if top_sellers:
        top_lines = ["🔥 <b>BU HAFTA TOP-5 SOTILGAN:</b>"]
        for idx, item in enumerate(top_sellers, start=1):
            q = item["units_sold"]
            qty_str = str(int(q)) if q == int(q) else f"{q:.1f}"
            top_lines.append(f"  {idx}. {item['name']} — <b>{qty_str}</b> {item['unit']}")
        messages.append("\n".join(top_lines))

    # ── BU HAFTA TUGAGAN, grouped by day ─────────────────────────────
    if weekly_out:
        groups = defaultdict(list)  # type: ignore[var-annotated]
        for item in weekly_out:
            weekday, date_str = _stockout_to_tk_day(item["stockout_at"])
            if weekday < 0:
                continue
            groups[(weekday, date_str)].append(item)

        sorted_keys = sorted(groups.keys(), key=lambda k: k[0])

        header = f"🔴 <b>BU HAFTA TUGAGAN</b> ({len(weekly_out)} ta):"
        item_lines = []
        for weekday, date_str in sorted_keys:
            day_name = UZ_DAYS_FULL[weekday]
            day_items = groups[(weekday, date_str)]
            item_lines.append("")
            item_lines.append(f"<b>{day_name} — {date_str}</b> ({len(day_items)} ta):")
            for item in day_items:
                sold = (
                    f" (sotilgan: {item['last_sold']})"
                    if item.get("last_sold") and item["last_sold"] != "—"
                    else ""
                )
                item_lines.append(f"  • {item['name']}{sold}")
        messages.extend(_chunk_lines(header, item_lines))

    return messages


def format_stock_alert_message(alerts: dict,
                                include_out: bool = True,
                                include_low: bool = True,
                                full: bool = False) -> list[str]:
    """Format alerts into one or more Telegram-ready HTML messages.

    Returns a list of message strings. The first message is always the
    summary; subsequent messages contain the item lists (chunked to fit
    inside Telegram's ~4096-char limit). Callers send each message
    separately in order.

    Args:
      include_out: include TUGAGAN (out-of-stock) section
      include_low: include KAM QOLDI (running-low) section
      full: show full lists (vs. top-25/top-30 preview)
    """
    if alerts["active_count"] == 0:
        return ["📦 Faol mahsulotlar topilmadi. /stock faylni yuklang."]

    # ── 1) Summary message ──────────────────────────────────────────
    summary_parts = [
        f"📦 <b>Kunlik inventarizatsiya xabari</b>\n",
        f"Faol mahsulotlar: <b>{alerts['active_count']}</b>",
        f"🟢 Yetarli: {alerts['healthy_count']}",
        f"🟡 Kam qoldi: {len(alerts['running_low'])}",
        f"🔴 Tugagan: {len(alerts['out_of_stock'])}",
    ]
    if not full:
        hints = []
        if alerts["out_of_stock"]:
            hints.append("<code>/stockalert tugagan</code>")
        if alerts["running_low"]:
            hints.append("<code>/stockalert kam</code>")
        if hints:
            summary_parts.append("")
            summary_parts.append("🔍 To'liq ro'yxat: " + " yoki ".join(hints))
            summary_parts.append("Yoki <code>/stockalert full</code> — ikkalasi ham.")
    messages = ["\n".join(summary_parts)]

    # ── 2) TUGAGAN section ────────────────────────────────────────
    if include_out and alerts["out_of_stock"]:
        out_items = alerts["out_of_stock"] if full else alerts["out_of_stock"][:25]
        header = f"🔴 <b>TUGAGAN — buyurtma kerak ({len(out_items)}/{len(alerts['out_of_stock'])}):</b>"
        item_lines = []
        for item in out_items:
            sold = f" (sotilgan: {item['last_sold']})" if item["last_sold"] != "—" else ""
            item_lines.append(f"  • {item['name']}{sold}")
        if not full and len(alerts["out_of_stock"]) > 25:
            item_lines.append(f"  ... va yana {len(alerts['out_of_stock']) - 25} ta "
                              f"(to'liq: <code>/stockalert tugagan</code>)")
        messages.extend(_chunk_lines(header, item_lines))

    # ── 3) KAM QOLDI section ──────────────────────────────────────
    if include_low and alerts["running_low"]:
        low_items = alerts["running_low"] if full else alerts["running_low"][:30]
        header = f"🟡 <b>KAM QOLDI ({len(low_items)}/{len(alerts['running_low'])}):</b>"
        item_lines = []
        for item in low_items:
            q = item['qty']
            qty_str = str(int(q)) if q == int(q) else f"{q:.1f}"
            item_lines.append(f"  • {item['name']} — <b>{qty_str}</b> {item['unit']}")
        if not full and len(alerts["running_low"]) > 30:
            item_lines.append(f"  ... va yana {len(alerts['running_low']) - 30} ta "
                              f"(to'liq: <code>/stockalert kam</code>)")
        messages.extend(_chunk_lines(header, item_lines))

    return messages
