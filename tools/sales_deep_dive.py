"""Deeper sales-pattern analysis — runs on Railway prod.

Four blocks:
  1. Spike attribution — per high-quartile day, top 5 contributing clients
     with the specific products they bought that day.
  2. Inventory utilization gap (180d) — currently-OOS SKUs with material
     180d sales history; at-risk low_stock anchors.
  3. Co-purchase affinity (180d) — for each 180d top-15 anchor, top 5
     co-occurring products by lift.
  4. Cross-sell opportunities — clients who bought anchor X in last 60d
     but did NOT buy paired Y in last 60d.

Output: single JSON to stdout.
"""
import sqlite3
import json
import os
from collections import defaultdict
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo

FX_FALLBACK = 12000.0
DB = os.environ.get("DATABASE_PATH", "/data/catalog.db")

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

tk = ZoneInfo("Asia/Tashkent")
today = datetime.now(tk).date()
W60_START = today - timedelta(days=59)
W180_START = today - timedelta(days=179)

# High-quartile dates from the previous run (lock them in for stability)
HIGH_DATES = ['2026-04-10', '2026-04-15', '2026-04-17', '2026-04-21', '2026-04-23',
              '2026-04-27', '2026-05-06', '2026-05-07', '2026-05-12', '2026-05-16',
              '2026-05-19', '2026-05-20']

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
ph = ",".join("?" for _ in PSEUDO)

# FX cache
_fx_cache = {}
def day_fx(d_iso):
    if d_iso in _fx_cache:
        return _fx_cache[d_iso]
    r = conn.execute(
        "SELECT rate FROM daily_fx_rates WHERE currency_pair='USD_UZS' "
        "AND rate_date<=? ORDER BY rate_date DESC LIMIT 1",
        (d_iso,),
    ).fetchone()
    rate = float(r["rate"]) if r and r["rate"] else FX_FALLBACK
    _fx_cache[d_iso] = rate
    return rate

DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
def dow_name(d_iso):
    return DOW_NAMES[date.fromisoformat(d_iso).weekday()]


# =============================================================================
# BLOCK 1 — Spike attribution per high day
# =============================================================================

# First compute DOW medians from 60d window so we can compare each spike day
dow_totals = defaultdict(list)
d = W60_START
while d <= today:
    iso = d.isoformat()
    if d.weekday() != 6:  # exclude Sundays
        rate = day_fx(iso)
        row = conn.execute(
            f"""SELECT COALESCE(SUM(total_sum),0) uzs,
                       COALESCE(SUM(total_sum_currency),0) usd
                  FROM real_orders
                 WHERE doc_date=? AND COALESCE(is_approved,1)=1
                   AND client_name_1c NOT IN ({ph})""",
            (iso, *PSEUDO),
        ).fetchone()
        usd_eq = (float(row["uzs"] or 0)/rate) + float(row["usd"] or 0)
        if usd_eq > 0:
            dow_totals[d.weekday()].append(usd_eq)
    d += timedelta(days=1)

dow_median = {}
for k, vals in dow_totals.items():
    vals_sorted = sorted(vals)
    n = len(vals_sorted)
    if n == 0:
        dow_median[k] = 0
    elif n % 2:
        dow_median[k] = vals_sorted[n // 2]
    else:
        dow_median[k] = (vals_sorted[n // 2 - 1] + vals_sorted[n // 2]) / 2

spike_days = []
for d_iso in HIGH_DATES:
    d_obj = date.fromisoformat(d_iso)
    rate = day_fx(d_iso)
    # Day total
    tot = conn.execute(
        f"""SELECT COALESCE(SUM(total_sum),0) uzs,
                   COALESCE(SUM(total_sum_currency),0) usd,
                   COUNT(*) n_orders,
                   COUNT(DISTINCT COALESCE(client_id, client_name_1c)) n_clients
              FROM real_orders
             WHERE doc_date=? AND COALESCE(is_approved,1)=1
               AND client_name_1c NOT IN ({ph})""",
        (d_iso, *PSEUDO),
    ).fetchone()
    day_total = float(tot["uzs"] or 0)/rate + float(tot["usd"] or 0)

    # Top 5 clients by USD-eq
    cli_rows = conn.execute(
        f"""SELECT client_name_1c,
                  COALESCE(client_id, 'NAME:'||client_name_1c) ckey,
                  SUM(total_sum) uzs, SUM(total_sum_currency) usd,
                  COUNT(*) n_orders
             FROM real_orders
            WHERE doc_date=? AND COALESCE(is_approved,1)=1
              AND client_name_1c NOT IN ({ph})
            GROUP BY ckey, client_name_1c
            ORDER BY (SUM(total_sum)/? + SUM(total_sum_currency)) DESC
            LIMIT 5""",
        (d_iso, *PSEUDO, rate),
    ).fetchall()

    contributors = []
    for c in cli_rows:
        c_usd_eq = float(c["uzs"] or 0)/rate + float(c["usd"] or 0)
        # What did this client buy that day? top 3 products
        prods = conn.execute(
            f"""SELECT ri.product_name_1c,
                      SUM(ri.quantity) qty,
                      SUM(ri.total_local) uzs,
                      SUM(ri.total_currency) usd
                 FROM real_order_items ri
                 JOIN real_orders ro ON ro.id = ri.real_order_id
                WHERE ro.doc_date=?
                  AND ro.client_name_1c=?
                  AND COALESCE(ro.is_approved,1)=1
                GROUP BY ri.product_name_1c
                ORDER BY (SUM(ri.total_local)/? + SUM(ri.total_currency)) DESC
                LIMIT 3""",
            (d_iso, c["client_name_1c"], rate),
        ).fetchall()
        contributors.append({
            "client": c["client_name_1c"],
            "usd_eq": round(c_usd_eq, 2),
            "share_of_day_pct": round(c_usd_eq / day_total * 100, 1) if day_total > 0 else 0,
            "n_orders": c["n_orders"],
            "top_products": [
                {"name": p["product_name_1c"],
                 "qty": float(p["qty"] or 0),
                 "usd_eq": round(float(p["uzs"] or 0)/rate + float(p["usd"] or 0), 2)}
                for p in prods
            ],
        })
    median_for_dow = dow_median.get(d_obj.weekday(), 0)
    vs_median_pct = ((day_total - median_for_dow) / median_for_dow * 100) if median_for_dow > 0 else None
    spike_days.append({
        "date": d_iso,
        "dow": dow_name(d_iso),
        "day_total_usd_eq": round(day_total, 2),
        "n_orders": int(tot["n_orders"] or 0),
        "n_clients": int(tot["n_clients"] or 0),
        "dow_median_usd_eq": round(median_for_dow, 2),
        "vs_dow_median_pct": round(vs_median_pct, 1) if vs_median_pct is not None else None,
        "top_5_contributors": contributors,
        "top_5_combined_share_pct": round(sum(c["share_of_day_pct"] for c in contributors), 1),
    })


# =============================================================================
# BLOCK 2 — Inventory utilization gap (180d)
# =============================================================================

# Avg FX over 180d for product-level revenue (small approximation, simpler)
fx_samples_180 = []
d = W180_START
while d <= today:
    fx_samples_180.append(day_fx(d.isoformat()))
    d += timedelta(days=30)
avg_fx_180 = sum(fx_samples_180) / len(fx_samples_180)

# All product sales in 180d
prod_rows = conn.execute(
    f"""SELECT ri.product_name_1c name_1c,
              p.name_display,
              p.stock_status,
              COALESCE(p.stock_quantity, 0) stock_qty,
              SUM(ri.quantity) qty_180d,
              SUM(ri.total_local) uzs_180d,
              SUM(ri.total_currency) usd_180d,
              COUNT(DISTINCT ro.id) n_orders_180d,
              COUNT(DISTINCT ro.doc_date) n_days_with_sales_180d,
              MAX(ro.doc_date) last_sold,
              MIN(ro.doc_date) first_sold
         FROM real_order_items ri
         JOIN real_orders ro ON ro.id = ri.real_order_id
         LEFT JOIN products p ON p.name = ri.product_name_1c
        WHERE ro.doc_date BETWEEN ? AND ?
          AND COALESCE(ro.is_approved,1)=1
          AND ro.client_name_1c NOT IN ({ph})
        GROUP BY ri.product_name_1c""",
    (W180_START.isoformat(), today.isoformat(), *PSEUDO),
).fetchall()

all_products_180 = []
for r in prod_rows:
    usd_eq = float(r["uzs_180d"] or 0)/avg_fx_180 + float(r["usd_180d"] or 0)
    all_products_180.append({
        "name_1c": r["name_1c"],
        "display": r["name_display"],
        "stock_status": r["stock_status"],
        "stock_qty": float(r["stock_qty"] or 0),
        "qty_180d": float(r["qty_180d"] or 0),
        "usd_eq_180d": round(usd_eq, 2),
        "n_orders_180d": r["n_orders_180d"],
        "n_days_with_sales": r["n_days_with_sales_180d"],
        "last_sold": r["last_sold"],
        "first_sold": r["first_sold"],
    })

all_products_180.sort(key=lambda x: -x["usd_eq_180d"])

# OOS but historically meaningful (top 30 by 180d revenue among currently-OOS)
oos_with_history = [p for p in all_products_180 if p["stock_status"] == "out_of_stock"][:30]

# At-risk low_stock anchors (currently low_stock, in top-50 movers)
top50_names = set(p["name_1c"] for p in all_products_180[:50])
at_risk = [p for p in all_products_180 if p["stock_status"] == "low_stock" and p["name_1c"] in top50_names]

# Restocked-recently products (positive stock now, history in 180d)
just_restocked = [p for p in all_products_180
                  if p["stock_status"] == "in_stock" and p["stock_qty"] > 0
                  and p["n_days_with_sales"] >= 5
                  and (date.fromisoformat(p["last_sold"]) >= (today - timedelta(days=14)) if p["last_sold"] else False)]
just_restocked.sort(key=lambda x: -x["usd_eq_180d"])

# Lifecycle/dormant: had sales but last_sold > 30 days ago AND currently OOS
dormant_oos = []
for p in all_products_180:
    if p["stock_status"] == "out_of_stock" and p["last_sold"]:
        days_since = (today - date.fromisoformat(p["last_sold"])).days
        if days_since > 30 and p["usd_eq_180d"] > 500:
            dormant_oos.append({**p, "days_since_last_sold": days_since})
dormant_oos.sort(key=lambda x: -x["usd_eq_180d"])


# =============================================================================
# BLOCK 3 — Co-purchase affinity (180d)
# =============================================================================

# 180d top-15 anchors
anchors_180 = all_products_180[:15]
anchor_names = [a["name_1c"] for a in anchors_180]

# Build affinity for each anchor
affinity_results = []

# Total order count in 180d (denominator for lift)
total_orders_180 = conn.execute(
    f"""SELECT COUNT(*) n FROM real_orders
       WHERE doc_date BETWEEN ? AND ?
         AND COALESCE(is_approved,1)=1
         AND client_name_1c NOT IN ({ph})""",
    (W180_START.isoformat(), today.isoformat(), *PSEUDO),
).fetchone()["n"]

# For each product, count orders containing it (in 180d)
prod_order_count = {}
for r in conn.execute(
    f"""SELECT ri.product_name_1c, COUNT(DISTINCT ro.id) n
         FROM real_order_items ri
         JOIN real_orders ro ON ro.id = ri.real_order_id
        WHERE ro.doc_date BETWEEN ? AND ?
          AND COALESCE(ro.is_approved,1)=1
          AND ro.client_name_1c NOT IN ({ph})
        GROUP BY ri.product_name_1c""",
    (W180_START.isoformat(), today.isoformat(), *PSEUDO),
):
    prod_order_count[r["product_name_1c"]] = r["n"]

for anchor in anchors_180:
    a_name = anchor["name_1c"]
    a_order_count = prod_order_count.get(a_name, 0)
    if a_order_count == 0:
        continue
    # Orders containing anchor → other products on those orders
    co_rows = conn.execute(
        f"""SELECT ri2.product_name_1c co_product,
                  p2.name_display co_display,
                  p2.stock_status co_stock_status,
                  COUNT(DISTINCT ro.id) n_co_orders,
                  SUM(ri2.quantity) co_qty,
                  SUM(ri2.total_local) co_uzs,
                  SUM(ri2.total_currency) co_usd
             FROM real_orders ro
             JOIN real_order_items ri1 ON ri1.real_order_id = ro.id
             JOIN real_order_items ri2 ON ri2.real_order_id = ro.id
             LEFT JOIN products p2 ON p2.name = ri2.product_name_1c
            WHERE ro.doc_date BETWEEN ? AND ?
              AND COALESCE(ro.is_approved,1)=1
              AND ro.client_name_1c NOT IN ({ph})
              AND ri1.product_name_1c = ?
              AND ri2.product_name_1c != ?
            GROUP BY ri2.product_name_1c
            HAVING COUNT(DISTINCT ro.id) >= 5
            ORDER BY n_co_orders DESC
            LIMIT 20""",
        (W180_START.isoformat(), today.isoformat(), *PSEUDO, a_name, a_name),
    ).fetchall()

    pairs = []
    p_a = a_order_count / total_orders_180 if total_orders_180 else 0
    for c in co_rows:
        co_name = c["co_product"]
        co_order_count = prod_order_count.get(co_name, 0)
        p_b = co_order_count / total_orders_180 if total_orders_180 else 0
        # confidence = P(B|A) = n_co / n_a
        confidence = c["n_co_orders"] / a_order_count if a_order_count else 0
        # lift = P(B|A) / P(B)
        lift = (confidence / p_b) if p_b > 0 else 0
        co_usd_eq = float(c["co_uzs"] or 0)/avg_fx_180 + float(c["co_usd"] or 0)
        pairs.append({
            "co_product": co_name,
            "co_display": c["co_display"],
            "co_stock_status": c["co_stock_status"],
            "n_co_orders": c["n_co_orders"],
            "confidence_pct": round(confidence * 100, 1),
            "lift": round(lift, 2),
            "co_qty_180d": float(c["co_qty"] or 0),
            "co_usd_eq_180d": round(co_usd_eq, 2),
        })
    # Sort by lift × confidence (both matter — lift = is association real, confidence = is it strong)
    pairs.sort(key=lambda x: -(x["lift"] * x["confidence_pct"]))
    affinity_results.append({
        "anchor_name_1c": a_name,
        "anchor_display": anchor["display"],
        "anchor_usd_eq_180d": anchor["usd_eq_180d"],
        "anchor_n_orders_180d": a_order_count,
        "top_5_pairs": pairs[:5],
        # Also surface: which co-pairs are NOT themselves in top-15 anchors
        "secondary_pairs_not_in_top15": [
            p for p in pairs[:10] if p["co_product"] not in anchor_names
        ][:5],
    })


# =============================================================================
# BLOCK 4 — Cross-sell opportunities
# =============================================================================

# For top-5 (anchor, secondary_pair) combinations from Block 3:
# Find clients who bought anchor in last 60d but NOT the pair in last 60d.
cross_sell_targets = []
seen_pairs = set()

for af in affinity_results:
    anchor_name = af["anchor_name_1c"]
    for pair in af.get("secondary_pairs_not_in_top15", [])[:3]:
        pair_name = pair["co_product"]
        key = (anchor_name, pair_name)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)

        # Clients who bought anchor in last 60d
        anchor_buyers = conn.execute(
            f"""SELECT DISTINCT ro.client_name_1c, ro.client_id,
                      MAX(ro.doc_date) last_anchor_buy
                 FROM real_orders ro
                 JOIN real_order_items ri ON ri.real_order_id = ro.id
                WHERE ro.doc_date BETWEEN ? AND ?
                  AND COALESCE(ro.is_approved,1)=1
                  AND ro.client_name_1c NOT IN ({ph})
                  AND ri.product_name_1c = ?
                GROUP BY ro.client_name_1c, ro.client_id""",
            (W60_START.isoformat(), today.isoformat(), *PSEUDO, anchor_name),
        ).fetchall()

        for buyer in anchor_buyers:
            # Did they ALSO buy the pair in last 60d?
            also_bought = conn.execute(
                f"""SELECT COUNT(*) n
                     FROM real_orders ro
                     JOIN real_order_items ri ON ri.real_order_id = ro.id
                    WHERE ro.doc_date BETWEEN ? AND ?
                      AND COALESCE(ro.is_approved,1)=1
                      AND ro.client_name_1c = ?
                      AND ri.product_name_1c = ?""",
                (W60_START.isoformat(), today.isoformat(), buyer["client_name_1c"], pair_name),
            ).fetchone()["n"]
            if also_bought > 0:
                continue
            # Buyer's total 60d window USD-eq (proxy for size)
            spend = conn.execute(
                f"""SELECT SUM(total_sum) uzs, SUM(total_sum_currency) usd
                     FROM real_orders
                    WHERE doc_date BETWEEN ? AND ?
                      AND COALESCE(is_approved,1)=1
                      AND client_name_1c = ?""",
                (W60_START.isoformat(), today.isoformat(), buyer["client_name_1c"]),
            ).fetchone()
            buyer_60d_usd = float(spend["uzs"] or 0)/avg_fx_180 + float(spend["usd"] or 0)
            cross_sell_targets.append({
                "client": buyer["client_name_1c"],
                "client_id": buyer["client_id"],
                "anchor_bought": anchor_name,
                "last_anchor_buy": buyer["last_anchor_buy"],
                "missing_pair": pair_name,
                "pair_lift": pair["lift"],
                "pair_confidence_pct": pair["confidence_pct"],
                "buyer_60d_usd_eq": round(buyer_60d_usd, 2),
            })

# Sort by buyer size × pair confidence (size matters most; high-confidence pairs second)
cross_sell_targets.sort(key=lambda x: -(x["buyer_60d_usd_eq"] * x["pair_confidence_pct"]))
cross_sell_targets = cross_sell_targets[:60]


# =============================================================================
# Assemble & emit
# =============================================================================

result = {
    "generated_at_tashkent": datetime.now(tk).strftime("%Y-%m-%d %H:%M"),
    "window_60d": {"start": W60_START.isoformat(), "end": today.isoformat()},
    "window_180d": {"start": W180_START.isoformat(), "end": today.isoformat()},
    "avg_fx_180d": round(avg_fx_180, 2),
    "total_orders_180d": total_orders_180,

    "block1_spike_attribution": {
        "dow_medians": {DOW_NAMES[k]: round(v, 2) for k, v in dow_median.items()},
        "high_quartile_days": spike_days,
    },
    "block2_inventory_gap": {
        "top30_oos_with_180d_history": oos_with_history,
        "n_oos_total": sum(1 for p in all_products_180 if p["stock_status"] == "out_of_stock"),
        "n_oos_with_meaningful_history_500plus_usd_eq": sum(
            1 for p in oos_with_history if p["usd_eq_180d"] >= 500
        ),
        "dormant_oos_over_30d_since_sale": dormant_oos[:20],
        "at_risk_low_stock_anchors": at_risk,
    },
    "block3_affinity": {
        "anchors_180d": [
            {"name_1c": a["name_1c"], "display": a["display"],
             "usd_eq_180d": a["usd_eq_180d"]}
            for a in anchors_180
        ],
        "anchor_co_purchase_pairs": affinity_results,
    },
    "block4_cross_sell": {
        "n_opportunities": len(cross_sell_targets),
        "top_60_opportunities": cross_sell_targets,
    },
}

print(json.dumps(result, ensure_ascii=False, default=str))
