"""Find top-of-Large promotion candidates and Heavy-peer product gaps.
Runs on Railway prod via railway ssh."""
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
W180_START = today - timedelta(days=179)
MONTHS = 180 / 30.0  # 6.0

# Proposal-B bucket thresholds (monthly USD-eq)
HEAVY_TH = 4120
LARGE_TH = 1721
# Top-of-Large = monthly USD-eq in [3000, 4120)
PROMO_MIN = 3000

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


# Sample FX over 180d for global conversion
fx_samples = [day_fx((W180_START + timedelta(days=i)).isoformat())
              for i in range(0, 180, 15)]
avg_fx = sum(fx_samples) / len(fx_samples)


# =============================================================================
# Step 1 — All clients with 180d spend → bucket assignment
# =============================================================================
cli_rows = conn.execute(
    f"""SELECT COALESCE(client_id, 'NAME:'||client_name_1c) ckey,
              client_name_1c,
              MAX(client_id) cid,
              SUM(total_sum) uzs,
              SUM(total_sum_currency) usd,
              COUNT(*) n_orders,
              COUNT(DISTINCT doc_date) n_active_days,
              MIN(doc_date) first_order, MAX(doc_date) last_order
         FROM real_orders
        WHERE doc_date BETWEEN ? AND ?
          AND COALESCE(is_approved,1)=1
          AND client_name_1c NOT IN ({ph})
        GROUP BY ckey, client_name_1c""",
    (W180_START.isoformat(), today.isoformat(), *PSEUDO),
).fetchall()

clients = []
for r in cli_rows:
    uzs = float(r["uzs"] or 0); usd = float(r["usd"] or 0)
    usd_eq = uzs / avg_fx + usd
    monthly = usd_eq / MONTHS
    if monthly >= HEAVY_TH:
        bucket = "Heavy"
    elif monthly >= LARGE_TH:
        bucket = "Large"
    elif monthly >= 621:
        bucket = "Medium"
    elif monthly >= 125:
        bucket = "Small"
    else:
        bucket = "Micro"
    clients.append({
        "ckey": r["ckey"], "name": r["client_name_1c"], "cid": r["cid"],
        "usd_eq_180d": round(usd_eq, 2),
        "monthly_usd_eq": round(monthly, 2),
        "bucket": bucket,
        "n_orders": r["n_orders"],
        "n_active_days": r["n_active_days"],
        "first_order": r["first_order"],
        "last_order": r["last_order"],
    })

clients.sort(key=lambda x: -x["monthly_usd_eq"])

heavy_clients = [c for c in clients if c["bucket"] == "Heavy"]
large_clients = [c for c in clients if c["bucket"] == "Large"]
promo_candidates = [c for c in clients if PROMO_MIN <= c["monthly_usd_eq"] < HEAVY_TH]

# =============================================================================
# Step 2 — Per-client product mix for candidates + heavy peers
# =============================================================================
def get_product_mix(client_name):
    rows = conn.execute(
        """SELECT ri.product_name_1c, p.name_display,
                  SUM(ri.quantity) qty,
                  SUM(ri.total_local) uzs,
                  SUM(ri.total_currency) usd,
                  COUNT(DISTINCT ro.id) n_orders
             FROM real_order_items ri
             JOIN real_orders ro ON ro.id = ri.real_order_id
             LEFT JOIN products p ON p.name = ri.product_name_1c
            WHERE ro.doc_date BETWEEN ? AND ?
              AND COALESCE(ro.is_approved,1)=1
              AND ro.client_name_1c = ?
            GROUP BY ri.product_name_1c""",
        (W180_START.isoformat(), today.isoformat(), client_name),
    ).fetchall()
    mix = []
    for r in rows:
        uzs = float(r["uzs"] or 0); usd = float(r["usd"] or 0)
        usd_eq = uzs / avg_fx + usd
        mix.append({
            "name_1c": r["product_name_1c"],
            "display": r["name_display"],
            "qty": float(r["qty"] or 0),
            "usd_eq": round(usd_eq, 2),
            "n_orders": r["n_orders"],
        })
    mix.sort(key=lambda x: -x["usd_eq"])
    return mix


# Pattern classification rules — keyword match on name_1c (Cyrillic)
def classify_pattern(mix):
    """Returns dominant pattern based on top 10 products by USD-eq."""
    cats = {"linoleum": 0.0, "paint": 0.0, "hardware": 0.0, "other": 0.0}
    for p in mix[:10]:
        n = (p["name_1c"] or "").upper()
        usd = p["usd_eq"]
        if "ЛИНОЛЕУМ" in n:
            cats["linoleum"] += usd
        elif any(k in n for k in ["ПФ-115", "ПФ-266", "ЯХТНЫЙ", "В/Э", "ЭЛЕРОН", "СТАРТ-ECO",
                                   "ХАЯТ", "ДЕКОР", "СИЛКОАТ", "АСТАР", "ЭМАЛЬ", "ДЕЛЮКС",
                                   "НЮМИКС", "ДЕКОАРТ", "ЛАК", "ГРУНТ"]):
            cats["paint"] += usd
        elif any(k in n for k in ["ПРОВОЛОКА", "ГВОЗДИ", "САМОРЕЗ", "ДЮБЕЛЬ",
                                   "СЕТКА", "ШЛИФ", "ЭЛЕКТРОД", "АНКЕР"]):
            cats["hardware"] += usd
        else:
            cats["other"] += usd
    total = sum(cats.values()) or 1
    shares = {k: v / total for k, v in cats.items()}
    # Mixed if top category < 0.55, else that category wins
    top_cat, top_share = max(shares.items(), key=lambda x: x[1])
    if top_share < 0.55:
        return "mixed", shares
    return top_cat, shares


# Get product mix for all heavy + candidates
print_progress = []
candidate_data = []
for c in promo_candidates:
    mix = get_product_mix(c["name"])
    pattern, shares = classify_pattern(mix)
    candidate_data.append({
        **c,
        "top_products": mix[:10],
        "all_product_set": set(p["name_1c"] for p in mix),
        "pattern": pattern,
        "pattern_shares": {k: round(v, 2) for k, v in shares.items()},
    })

heavy_data = []
for c in heavy_clients:
    mix = get_product_mix(c["name"])
    pattern, shares = classify_pattern(mix)
    heavy_data.append({
        **c,
        "top_products": mix[:10],
        "all_product_set": set(p["name_1c"] for p in mix),
        "pattern": pattern,
        "pattern_shares": {k: round(v, 2) for k, v in shares.items()},
    })


# =============================================================================
# Step 3 — For each candidate, compute Heavy-peer product gap
# =============================================================================
# Strategy: pool products that Heavy buyers of SAME pattern buy frequently,
# subtract what candidate already buys → gap list.
# Frequency = appears in >=3 Heavy peers' top-10 product sets.

heavy_by_pattern = defaultdict(list)
for h in heavy_data:
    heavy_by_pattern[h["pattern"]].append(h)

# For each pattern, compute "Heavy signature" = products bought by >= 30% of Heavy peers
pattern_signatures = {}
for pat, peers in heavy_by_pattern.items():
    if not peers:
        pattern_signatures[pat] = []
        continue
    threshold = max(2, int(0.30 * len(peers)))
    cnt = defaultdict(int)
    rev_assist = defaultdict(float)  # peer-side USD-eq sum
    for peer in peers:
        for p in peer["top_products"]:
            cnt[p["name_1c"]] += 1
            rev_assist[p["name_1c"]] += p["usd_eq"]
    sig = [
        {"name_1c": k, "n_heavy_peers": cnt[k],
         "share_of_pattern_heavy": round(cnt[k] / len(peers), 2),
         "peer_revenue_sum": round(rev_assist[k], 2)}
        for k in cnt if cnt[k] >= threshold
    ]
    sig.sort(key=lambda x: -x["share_of_pattern_heavy"])
    pattern_signatures[pat] = sig

# Per-candidate gap
for c in candidate_data:
    pat = c["pattern"]
    sig = pattern_signatures.get(pat, [])
    # Lookup product display names from products table for gap items
    gap = [s for s in sig if s["name_1c"] not in c["all_product_set"]]
    # Enrich with display + stock status
    for g in gap[:15]:
        r = conn.execute(
            "SELECT name_display, stock_status, COALESCE(stock_quantity,0) qty "
            "FROM products WHERE name = ?", (g["name_1c"],)
        ).fetchone()
        if r:
            g["display"] = r["name_display"]
            g["stock_status"] = r["stock_status"]
            g["stock_qty"] = float(r["qty"])
        else:
            g["display"] = None
            g["stock_status"] = None
            g["stock_qty"] = None
    c["heavy_peer_gap"] = gap[:10]
    # Cleanup: don't ship the set or 10-product full list in output, just top 5
    c["top_5_products"] = c.pop("top_products")[:5]
    c.pop("all_product_set", None)


# =============================================================================
# Step 4 — Pattern summary + heavy signature per pattern
# =============================================================================
pattern_summary = {}
for pat in ["linoleum", "paint", "hardware", "mixed", "other"]:
    cands = [c for c in candidate_data if c["pattern"] == pat]
    heavies = heavy_by_pattern.get(pat, [])
    sig = pattern_signatures.get(pat, [])
    # Enrich signature with display
    sig_top10 = []
    for s in sig[:10]:
        r = conn.execute(
            "SELECT name_display, stock_status FROM products WHERE name = ?",
            (s["name_1c"],)
        ).fetchone()
        sig_top10.append({
            **s,
            "display": r["name_display"] if r else None,
            "stock_status": r["stock_status"] if r else None,
        })
    pattern_summary[pat] = {
        "n_candidates": len(cands),
        "n_heavy_peers": len(heavies),
        "candidate_avg_monthly_usd_eq": round(
            sum(c["monthly_usd_eq"] for c in cands) / len(cands), 2
        ) if cands else 0,
        "heavy_avg_monthly_usd_eq": round(
            sum(h["monthly_usd_eq"] for h in heavies) / len(heavies), 2
        ) if heavies else 0,
        "candidate_gap_to_heavy_avg": round(HEAVY_TH - (
            sum(c["monthly_usd_eq"] for c in cands) / len(cands) if cands else 0
        ), 2),
        "signature_products_top10": sig_top10,
    }


# =============================================================================
# Step 5 — Projected revenue lift
# =============================================================================
# If 30% of candidates each close half their gap (i.e., add half of avg gap to monthly spend),
# net new monthly revenue across all upgrades:
n_candidates = len(candidate_data)
total_gap = sum(HEAVY_TH - c["monthly_usd_eq"] for c in candidate_data)
projection_30pct_half_gap = total_gap * 0.30 * 0.5
projection_50pct_full_upgrade = sum(HEAVY_TH - c["monthly_usd_eq"] for c in candidate_data) * 0.50


result = {
    "generated_at_tashkent": datetime.now(tk).strftime("%Y-%m-%d %H:%M"),
    "window_180d": {"start": W180_START.isoformat(), "end": today.isoformat()},
    "avg_fx": round(avg_fx, 2),
    "thresholds": {"Heavy": HEAVY_TH, "Large": LARGE_TH, "candidate_min": PROMO_MIN},
    "n_clients_180d": len(clients),
    "n_heavy_total": len(heavy_clients),
    "n_large_total": len(large_clients),
    "n_promo_candidates": n_candidates,
    "promo_candidates": candidate_data,
    "pattern_summary": pattern_summary,
    "projections": {
        "candidates_total_monthly_gap_usd": round(total_gap, 2),
        "if_30pct_close_half_gap_monthly": round(projection_30pct_half_gap, 2),
        "if_50pct_full_upgrade_monthly": round(projection_50pct_full_upgrade, 2),
    },
}

print(json.dumps(result, ensure_ascii=False, default=str))
