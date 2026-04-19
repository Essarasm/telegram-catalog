"""Simulate loyalty points with per-producer tier multipliers.

Calculates points for all months retroactively using the 3-tier
compensation model: low-margin producers earn fewer points,
high-margin producers earn more.

This is read-only — doesn't write to any tables.
"""
import logging
from backend.database import get_db

logger = logging.getLogger(__name__)

# Producer → points multiplier (from Session T 3-tier model)
TIER_HIGH = 2.0    # 2.0% commission tier → 2x points
TIER_STANDARD = 1.5  # 1.0% commission tier → 1.5x points
TIER_LOW = 1.0     # 0.5% commission tier → 1x points (baseline)

# Producers by tier (from Session T)
_HIGH_MARGIN = {
    'palizh', 'нюмикс', 'weber', 'qorasaroy', 'silkoat',
    'юнитинт', 'oscar', 'dekoart', 'ofm', 'соудал', 'colormix',
    'палиж', 'вебер', 'оскар', 'декоарт', 'силкоат', 'коррасарой',
}
_LOW_MARGIN = {
    'hayat', 'eleron', 'узкабель', 'lama', 'kripteks',
    'хаят', 'элерон', 'lama standart',
}

POINTS_PER_10K_UZS = 1
POINTS_PER_USD = 10


def _get_producer_tier(producer_name):
    """Determine point multiplier from producer name."""
    if not producer_name:
        return TIER_STANDARD
    low = producer_name.strip().lower()
    for p in _HIGH_MARGIN:
        if p in low:
            return TIER_HIGH
    for p in _LOW_MARGIN:
        if p in low:
            return TIER_LOW
    return TIER_STANDARD


def simulate_all_months(conn=None):
    """Calculate per-producer tiered points for Jan 2025 – Mar 2026.

    Returns list of monthly summaries + per-client details for the latest month.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db()

    try:
        # Get all months with real_orders data
        months = conn.execute(
            """SELECT DISTINCT substr(doc_date, 1, 7) as month
               FROM real_orders
               WHERE doc_date >= '2025-01' AND doc_date < '2026-04'
               ORDER BY month"""
        ).fetchall()

        if not months:
            return {"ok": True, "months": [], "message": "No order data found"}

        # Get discipline data from latest scoring
        score_data = {}
        try:
            scores = conn.execute(
                """SELECT client_id, on_time_rate, volume_bucket, debt_ratio
                   FROM client_scores
                   WHERE recalc_date = (SELECT MAX(recalc_date) FROM client_scores)"""
            ).fetchall()
            for s in scores:
                on_time = float(s["on_time_rate"] or 0)
                if on_time >= 0.95:
                    grade, mult = "A+", 2.0
                elif on_time >= 0.80:
                    grade, mult = "A", 1.5
                elif on_time >= 0.60:
                    grade, mult = "B", 1.0
                elif on_time >= 0.30:
                    grade, mult = "C", 0.5
                else:
                    grade, mult = "D", 0.0
                score_data[s["client_id"]] = {
                    "grade": grade, "multiplier": mult,
                    "bucket": s["volume_bucket"] or "Micro",
                }
        except Exception:
            pass

        monthly_summaries = []
        latest_clients = []

        for month_row in months:
            month = month_row["month"]

            # Get per-item data with producer info
            items = conn.execute(
                """SELECT ro.client_id, ro.client_name_1c,
                          roi.product_id,
                          COALESCE(roi.total_local, 0) as item_uzs,
                          COALESCE(roi.total_currency, 0) as item_usd,
                          p.producer_id, pr.name as producer_name
                   FROM real_order_items roi
                   JOIN real_orders ro ON ro.id = roi.real_order_id
                   LEFT JOIN products p ON p.id = roi.product_id
                   LEFT JOIN producers pr ON pr.id = p.producer_id
                   WHERE ro.doc_date LIKE ?
                     AND ro.client_id IS NOT NULL""",
                (f"{month}%",),
            ).fetchall()

            # Aggregate per client with tier multipliers
            client_points = {}
            tier_totals = {"high": 0, "standard": 0, "low": 0}

            for item in items:
                cid = item["client_id"]
                if cid not in client_points:
                    client_points[cid] = {
                        "client_name": item["client_name_1c"] or "",
                        "flat_points": 0,
                        "tiered_points": 0,
                        "purchase_uzs": 0,
                        "purchase_usd": 0,
                    }

                uzs = float(item["item_uzs"] or 0)
                usd = float(item["item_usd"] or 0)
                client_points[cid]["purchase_uzs"] += uzs
                client_points[cid]["purchase_usd"] += usd

                # Flat points (current model)
                flat = int(uzs / 10000) * POINTS_PER_10K_UZS + int(usd * POINTS_PER_USD)
                client_points[cid]["flat_points"] += flat

                # Tiered points
                tier_mult = _get_producer_tier(item["producer_name"])
                tiered = int(flat * tier_mult)
                client_points[cid]["tiered_points"] += tiered

                if tier_mult == TIER_HIGH:
                    tier_totals["high"] += flat
                elif tier_mult == TIER_LOW:
                    tier_totals["low"] += flat
                else:
                    tier_totals["standard"] += flat

            # Apply discipline multiplier
            total_flat = 0
            total_tiered = 0
            client_count = 0

            for cid, cp in client_points.items():
                sd = score_data.get(cid, {"grade": "C", "multiplier": 0.5, "bucket": "Micro"})
                disc_mult = sd["multiplier"]

                cp["discipline_grade"] = sd["grade"]
                cp["discipline_mult"] = disc_mult
                cp["bucket"] = sd["bucket"]
                cp["flat_effective"] = int(cp["flat_points"] * disc_mult)
                cp["tiered_effective"] = int(cp["tiered_points"] * disc_mult)

                total_flat += cp["flat_effective"]
                total_tiered += cp["tiered_effective"]
                client_count += 1

            monthly_summaries.append({
                "month": month,
                "clients": client_count,
                "flat_total": total_flat,
                "tiered_total": total_tiered,
                "difference": total_tiered - total_flat,
                "diff_pct": round((total_tiered / total_flat - 1) * 100, 1) if total_flat > 0 else 0,
                "tier_breakdown": tier_totals,
            })

            # Save client details for latest month
            if month == months[-1]["month"]:
                latest_clients = sorted(
                    [{"client_id": cid, **cp} for cid, cp in client_points.items()],
                    key=lambda x: -x["tiered_effective"]
                )

        return {
            "ok": True,
            "months": monthly_summaries,
            "latest_month": months[-1]["month"] if months else None,
            "latest_top_10": latest_clients[:10],
        }
    except Exception as e:
        logger.error(f"Points simulation failed: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}
    finally:
        if own_conn:
            conn.close()
