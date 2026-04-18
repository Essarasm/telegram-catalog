"""Stock alert service — identifies active products that are out of stock or running low.

"Active" = had positive stock within the last 30 days (was in uncle's /stock
upload with qty > 0). This uses the stock file as source of truth — uncle only
uploads products he actively stocks. Fallback criteria for products without
stock_last_positive_at: sold in last 3 months or supplied recently.
"""
import logging
from backend.database import get_db

logger = logging.getLogger(__name__)

ACTIVE_STOCK_DAYS = 30
SOLD_MONTHS = 3
SUPPLY_MONTHS = 6


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
            "out_of_stock": [{name, producer, last_sold, last_supplied}, ...],
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
            return {"active_count": 0, "out_of_stock": [], "running_low": [], "healthy_count": 0}

        placeholders = ",".join("?" for _ in active_ids)
        id_list = list(active_ids)

        products = conn.execute(
            f"""SELECT p.id, p.name, p.name_display, pr.name as producer_name,
                       p.stock_quantity, p.stock_status, p.unit
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

        return {
            "active_count": len(products),
            "out_of_stock": out_of_stock,
            "running_low": running_low,
            "healthy_count": healthy,
        }
    finally:
        if own_conn:
            conn.close()


def format_stock_alert_message(alerts: dict) -> str:
    """Format alerts into a Telegram-ready HTML message."""
    if alerts["active_count"] == 0:
        return "📦 Faol mahsulotlar topilmadi. /stock faylni yuklang."

    lines = [
        f"📦 <b>Kunlik inventarizatsiya xabari</b>\n",
        f"Faol mahsulotlar: <b>{alerts['active_count']}</b>",
        f"🟢 Yetarli: {alerts['healthy_count']}",
        f"🟡 Kam qoldi: {len(alerts['running_low'])}",
        f"🔴 Tugagan: {len(alerts['out_of_stock'])}",
    ]

    if alerts["out_of_stock"]:
        lines.append(f"\n🔴 <b>TUGAGAN — buyurtma kerak:</b>")
        for item in alerts["out_of_stock"][:20]:
            sold = f" (oxirgi sotilgan: {item['last_sold']})" if item["last_sold"] != "—" else ""
            lines.append(f"  • {item['name']}{sold}")
        if len(alerts["out_of_stock"]) > 20:
            lines.append(f"  ... va yana {len(alerts['out_of_stock']) - 20} ta")

    if alerts["running_low"]:
        lines.append(f"\n🟡 <b>KAM QOLDI — diqqat:</b>")
        for item in alerts["running_low"][:20]:
            q = item['qty']
            qty_str = str(int(q)) if q == int(q) else f"{q:.1f}"
            lines.append(f"  • {item['name']} — <b>{qty_str}</b> {item['unit']} qoldi")
        if len(alerts["running_low"]) > 20:
            lines.append(f"  ... va yana {len(alerts['running_low']) - 20} ta")

    return "\n".join(lines)
