"""60-day sales pattern analysis — runs on Railway prod.
Output: JSON to stdout.

Window: today (Tashkent) - 59d through today.
Excludes pseudo-clients (suppliers/internal accounts) and unapproved real_orders.
USD-eq = uzs/fx + usd; FX = day-of, fallback to most-recent-prior, else 12000.
"""
import sqlite3
import json
import os
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo

FX_FALLBACK = 12000.0
DB = os.environ.get("DATABASE_PATH", "/data/catalog.db")

# Mirror of backend.services.pseudo_clients.SYSTEM_NON_CLIENT_NAMES (2026-05-28)
PSEUDO = [
    "ИСПРАВЛЕНИЕ", "ИСПРАВЛЕНИЕ СКЛАД 2", "Наличка №1", "Наличка №2",
    "Наличка №3", "Наличка СКЛАД", "Наличка - Магазин", "Организации (переч.)",
    "СТРОЙКА", "В О З В Р А Т ПОСТАВЩИКУ",
    "ПАРВИЗ SILKCOAT ФИРМЕННЫЙ МАГАЗИН", "ЖАМШЕД УРГУТ", "ДИЛДОРА МАХМУДОВА СУПЕР",
    "САМАВТО", "1", "В О З В Р А Т", "<...>", "DEKS", "DEKS - БОНУС",
    "СОМОФИКС БОНУС", "DELUX Самандар ака", "EAST COLOR /BUILD TECHNO TRADE/",
    "GAMMA COLOR SERVICE", "GOOGLE", "LAMA STANDART", "PAINTERA", "R O Y A L",
    "SILKCOAT PAINT", "SIMPLEX BIZNES", "ZIP КОЛЛЕР", "АКФИКС", "ДЕКОАРТ",
    "КАРБИД", "ЛИНОЛЕУМ САНФА", "ЛОПАТКИ /РАЗНЫЕ/", "ПРОЧИЕ", "Растворитель",
    "СЕНТИФОН", "СОУДАЛ /ПОЛИСАН/", "УЗКАБЕЛЬ", "ШЛИФ ШКУРКА", "ЭЛЕКТРОД",
    "ЭМАЛЬ НЦ-132П", "Ташкент Трубный з-д", "САМОРЕЗ  OFM", "ШЛАНГ ПОЛИВНОЙ",
    "KRIPTEKS - METAL", "ЭКОС /КораСарой/", "MASHXAD", "PUFA MIX", "WEBER",
    "ДЕКОПЛАСТ", "НАЦИОНАЛ КЕРАМИК", "НОРА ойти", "НЮМИКС", "ПалИЖ КОЛЛЕР",
    "СОБСАН", "СОМО FIX", "ЦЕМЕНТ", "ЭЛЕРОН ЭЛИТ СЕРВИС", "FUBER",
    "ГВОЗДИ /KRIPTEKS-METAL/", "КораСарой/ЭКОС/", "Саморез TAGERT",
    "СП ООО \"RANGLI B O' Y O Q\"", "RANGLI BO'YOQ", "ORIGINAL COLORMIX",
    "УГОЛОК", "COLOREX", "ФИРДАВС 3 D НАЛИВН ПОЛ УСТО",
    "БЕКЗОД ПАНДЖОБ /Маг Авто Запчасть/", "40.12",
]

# Proposal-B monthly USD-eq thresholds
def bucket_label(monthly_usd):
    if monthly_usd >= 4120: return "Heavy"
    if monthly_usd >= 1721: return "Large"
    if monthly_usd >= 621: return "Medium"
    if monthly_usd >= 125: return "Small"
    return "Micro"

tk = ZoneInfo("Asia/Tashkent")
today = datetime.now(tk).date()
start = today - timedelta(days=59)

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row


def day_fx(d_iso):
    r = conn.execute(
        "SELECT rate FROM daily_fx_rates WHERE currency_pair='USD_UZS' "
        "AND rate_date<=? ORDER BY rate_date DESC LIMIT 1",
        (d_iso,),
    ).fetchone()
    if r and r["rate"]:
        return float(r["rate"]), "actual"
    return FX_FALLBACK, "fallback"


ph = ",".join("?" for _ in PSEUDO)
DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# ---- Per-day rollup ----
daily = []
d = start
while d <= today:
    iso = d.isoformat()
    rate, src = day_fx(iso)
    row = conn.execute(
        f"""SELECT COALESCE(SUM(total_sum),0) uzs,
                   COALESCE(SUM(total_sum_currency),0) usd,
                   COUNT(*) n_orders,
                   COUNT(DISTINCT COALESCE(client_id, client_name_1c)) n_clients
              FROM real_orders
             WHERE doc_date=? AND COALESCE(is_approved,1)=1
               AND client_name_1c NOT IN ({ph})""",
        (iso, *PSEUDO),
    ).fetchone()
    uzs = float(row["uzs"] or 0)
    usd = float(row["usd"] or 0)
    usd_eq = uzs / rate + usd
    daily.append({
        "date": iso, "dow": DOW[d.weekday()],
        "uzs": round(uzs, 2), "usd": round(usd, 2),
        "fx": rate, "fx_src": src,
        "usd_eq": round(usd_eq, 2),
        "n_orders": int(row["n_orders"] or 0),
        "n_clients": int(row["n_clients"] or 0),
    })
    d += timedelta(days=1)

# ---- Quartile split (exclude Sundays + zero days) ----
working = [x for x in daily if x["dow"] != "Sun" and x["usd_eq"] > 0]
working.sort(key=lambda x: x["usd_eq"])
n = len(working)
q = max(1, n // 4)
low_days = sorted(working[:q], key=lambda x: x["date"])
high_days = sorted(working[-q:], key=lambda x: x["date"])
mid_days = working[q:-q]

low_dates = [x["date"] for x in low_days]
high_dates = [x["date"] for x in high_days]

# ---- Client window-spend → bucket assignment ----
months_in_window = 60.0 / 30.0  # ~2.0
all_fx = [day_fx(x["date"])[0] for x in daily]
avg_fx_window = sum(all_fx) / len(all_fx)

cli_rows = conn.execute(
    f"""SELECT COALESCE(client_id, 'NAME:'||client_name_1c) ckey,
              client_name_1c,
              SUM(total_sum) uzs, SUM(total_sum_currency) usd
         FROM real_orders
        WHERE doc_date BETWEEN ? AND ?
          AND COALESCE(is_approved,1)=1
          AND client_name_1c NOT IN ({ph})
        GROUP BY ckey, client_name_1c""",
    (start.isoformat(), today.isoformat(), *PSEUDO),
).fetchall()

client_bucket = {}
for r in cli_rows:
    uzs = float(r["uzs"] or 0); usd = float(r["usd"] or 0)
    usd_eq = uzs / avg_fx_window + usd
    monthly = usd_eq / months_in_window
    client_bucket[r["ckey"]] = {
        "name": r["client_name_1c"],
        "window_usd_eq": round(usd_eq, 2),
        "monthly_usd_eq": round(monthly, 2),
        "bucket": bucket_label(monthly),
    }


def drilldown(dates, label):
    if not dates:
        return {"label": label, "dates": [], "empty": True}
    ph_d = ",".join("?" for _ in dates)
    fx_rates = [day_fx(d)[0] for d in dates]
    avg_fx = sum(fx_rates) / len(fx_rates)

    # Top clients
    rows = conn.execute(
        f"""SELECT COALESCE(client_id, 'NAME:'||client_name_1c) ckey,
                  client_name_1c,
                  SUM(total_sum) uzs, SUM(total_sum_currency) usd,
                  COUNT(*) n_orders,
                  COUNT(DISTINCT doc_date) n_active_days
             FROM real_orders
            WHERE doc_date IN ({ph_d})
              AND COALESCE(is_approved,1)=1
              AND client_name_1c NOT IN ({ph})
            GROUP BY ckey, client_name_1c""",
        (*dates, *PSEUDO),
    ).fetchall()
    clients = []
    for r in rows:
        uzs = float(r["uzs"] or 0); usd = float(r["usd"] or 0)
        usd_eq = uzs / avg_fx + usd
        b = client_bucket.get(r["ckey"], {})
        clients.append({
            "name": r["client_name_1c"],
            "usd_eq": round(usd_eq, 2),
            "n_orders": r["n_orders"],
            "n_active_days": r["n_active_days"],
            "bucket": b.get("bucket", "?"),
            "monthly_window_usd_eq": b.get("monthly_usd_eq"),
        })
    clients.sort(key=lambda x: -x["usd_eq"])
    total_usd_eq = sum(c["usd_eq"] for c in clients)

    # Bucket distribution
    bd = {"Heavy": 0, "Large": 0, "Medium": 0, "Small": 0, "Micro": 0, "?": 0}
    bc = {"Heavy": 0, "Large": 0, "Medium": 0, "Small": 0, "Micro": 0, "?": 0}
    for c in clients:
        bd[c["bucket"]] += c["usd_eq"]; bc[c["bucket"]] += 1

    # Top products
    pr = conn.execute(
        f"""SELECT ri.product_name_1c, p.name_display, p.stock_status,
                  COALESCE(p.stock_quantity, 0) stock_qty,
                  SUM(ri.quantity) qty,
                  SUM(ri.total_local) uzs,
                  SUM(ri.total_currency) usd,
                  COUNT(DISTINCT ro.id) n_orders
             FROM real_order_items ri
             JOIN real_orders ro ON ro.id = ri.real_order_id
             LEFT JOIN products p ON p.name = ri.product_name_1c
            WHERE ro.doc_date IN ({ph_d})
              AND COALESCE(ro.is_approved,1)=1
              AND ro.client_name_1c NOT IN ({ph})
            GROUP BY ri.product_name_1c""",
        (*dates, *PSEUDO),
    ).fetchall()
    products = []
    for r in pr:
        uzs = float(r["uzs"] or 0); usd = float(r["usd"] or 0)
        usd_eq = uzs / avg_fx + usd
        products.append({
            "name_1c": r["product_name_1c"],
            "display": r["name_display"],
            "stock_status": r["stock_status"],
            "stock_qty": r["stock_qty"],
            "qty": float(r["qty"] or 0),
            "usd_eq": round(usd_eq, 2),
            "n_orders": r["n_orders"],
        })
    products.sort(key=lambda x: -x["usd_eq"])

    return {
        "label": label,
        "n_days": len(dates),
        "dates": dates,
        "avg_fx": round(avg_fx, 2),
        "total_usd_eq": round(total_usd_eq, 2),
        "avg_daily_usd_eq": round(total_usd_eq / len(dates), 2),
        "n_unique_clients": len(clients),
        "top_clients": clients[:15],
        "top_products": products[:30],
        "bucket_revenue": {k: round(v, 2) for k, v in bd.items()},
        "bucket_n_clients": bc,
    }


high = drilldown(high_dates, "high_quartile")
low = drilldown(low_dates, "low_quartile")

# ---- DOW summary ----
dow_summary = {}
for x in daily:
    k = x["dow"]
    s = dow_summary.setdefault(k, {"n_days": 0, "n_working_days": 0,
                                    "total_usd_eq": 0.0, "min_usd_eq": float("inf"),
                                    "max_usd_eq": 0.0})
    s["n_days"] += 1
    if x["usd_eq"] > 0:
        s["n_working_days"] += 1
        s["total_usd_eq"] += x["usd_eq"]
        s["min_usd_eq"] = min(s["min_usd_eq"], x["usd_eq"])
        s["max_usd_eq"] = max(s["max_usd_eq"], x["usd_eq"])
for k, s in dow_summary.items():
    if s["n_working_days"]:
        s["avg_usd_eq"] = round(s["total_usd_eq"] / s["n_working_days"], 2)
        s["min_usd_eq"] = round(s["min_usd_eq"], 2)
    else:
        s["avg_usd_eq"] = 0; s["min_usd_eq"] = 0

# ---- Inventory state ----
inv = conn.execute(
    "SELECT COALESCE(stock_status,'unknown') status, COUNT(*) n FROM products "
    "WHERE COALESCE(is_active,1)=1 GROUP BY stock_status"
).fetchall()
inventory_overall = [dict(r) for r in inv]

# Stock status of top-50 movers from BOTH quartiles
def stock_for(names):
    if not names:
        return []
    php = ",".join("?" for _ in names)
    return [dict(r) for r in conn.execute(
        f"SELECT name, name_display, stock_quantity, stock_status, is_active, "
        f"stockout_at, restocked_at FROM products WHERE name IN ({php})",
        names,
    )]

high_top50_names = [p["name_1c"] for p in high.get("top_products", [])[:50]]
low_top50_names = [p["name_1c"] for p in low.get("top_products", [])[:50]]
high_top50_stock = stock_for(high_top50_names)
low_top50_stock = stock_for(low_top50_names)

# Stock-status counter for each
def status_counter(rows):
    c = {}
    for r in rows:
        k = r.get("stock_status") or "unknown"
        c[k] = c.get(k, 0) + 1
    return c

result = {
    "generated_at_tashkent": datetime.now(tk).strftime("%Y-%m-%d %H:%M"),
    "window": {"start": start.isoformat(), "end": today.isoformat(), "n_days": 60},
    "daily": daily,
    "dow_summary": dow_summary,
    "high_quartile": high,
    "low_quartile": low,
    "inventory_overall": inventory_overall,
    "high_top50_inventory": high_top50_stock,
    "high_top50_status_counts": status_counter(high_top50_stock),
    "low_top50_inventory": low_top50_stock,
    "low_top50_status_counts": status_counter(low_top50_stock),
    "n_working_days_total": len(working),
    "n_quartile_size": q,
    "avg_fx_window": round(avg_fx_window, 2),
}

print(json.dumps(result, ensure_ascii=False, default=str))
