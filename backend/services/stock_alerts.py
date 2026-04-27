"""Stock alert service — identifies active products that are out of stock or running low.

"Active" = had positive stock within the last 60 days (was in uncle's /stock
upload with qty > 0). This uses the stock file as source of truth — uncle only
uploads products he actively stocks. Fallback criteria for products without
stock_last_positive_at: sold in last 3 months or supplied recently.
"""
import logging
from datetime import datetime, timedelta
from backend.database import get_db

logger = logging.getLogger(__name__)

ACTIVE_STOCK_DAYS = 60
SOLD_MONTHS = 3
SUPPLY_MONTHS = 6
NEWLY_OUT_HOURS = 24  # delta window for the 09:00 daily inventory alert


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


def get_stock_alerts(conn=None) -> dict:
    """Generate stock alerts for active products.

    Returns:
        {
            "active_count": int,
            "out_of_stock": [{name, producer, last_sold, last_supplied, stockout_at}, ...],
            "newly_out_of_stock": subset of out_of_stock with stockout_at within NEWLY_OUT_HOURS,
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
                "newly_out_of_stock": [],
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

        # Delta: items that flipped to 0 within the last NEWLY_OUT_HOURS.
        # SQLite datetime('now') writes UTC, so we compare against UTC here too.
        cutoff = (datetime.utcnow() - timedelta(hours=NEWLY_OUT_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
        newly_out_of_stock = [
            item for item in out_of_stock
            if item["stockout_at"] and item["stockout_at"] >= cutoff
        ]
        # Within the delta, surface fresh stockouts first.
        newly_out_of_stock.sort(key=lambda x: x["stockout_at"] or "", reverse=True)

        return {
            "active_count": len(products),
            "out_of_stock": out_of_stock,
            "newly_out_of_stock": newly_out_of_stock,
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
    """09:00 cron message — focuses on items that flipped to 0 in the last 24h.

    Cumulative count is shown for context; the full TUGAGAN list stays
    available on demand via /stockalert tugagan. Returns [] when there is
    nothing new — caller should treat that as "no message to send".
    """
    if alerts.get("active_count", 0) == 0:
        return []
    newly_out = alerts.get("newly_out_of_stock", [])
    if not newly_out:
        return []

    summary_parts = [
        f"📦 <b>Kunlik inventarizatsiya xabari</b>\n",
        f"Faol mahsulotlar: <b>{alerts['active_count']}</b>",
        f"🟢 Yetarli: {alerts['healthy_count']}",
        f"🟡 Kam qoldi: {len(alerts['running_low'])}",
        f"🔴 Tugagan: {len(alerts['out_of_stock'])} (bugun: <b>{len(newly_out)}</b>)",
        "",
        f"🔍 To'liq ro'yxat: <code>/stockalert tugagan</code>",
    ]
    messages = ["\n".join(summary_parts)]

    header = f"🆕 <b>BUGUN TUGAGAN ({len(newly_out)} ta):</b>"
    item_lines = []
    for item in newly_out:
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
