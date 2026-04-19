"""Session L: Loyalty points calculation engine.

Two-axis model:
  Axis 1 — Purchase points: proportional to monthly purchase volume
  Axis 2 — Discipline multiplier: based on 3-week payment rule

Formula: effective_points = purchase_points × multiplier + clean_sheet_bonus

Discipline grades:
  A+ (×2.0): on_time_rate >= 0.95 (pays almost everything on time)
  A  (×1.5): on_time_rate >= 0.80
  B  (×1.0): on_time_rate >= 0.60
  C  (×0.5): on_time_rate >= 0.30
  D  (×0.0): on_time_rate < 0.30 (chronic late payer)
"""
import logging
from datetime import date, datetime

from backend.database import get_db

logger = logging.getLogger(__name__)

POINTS_PER_10K_UZS = 1
POINTS_PER_USD = 10
CLEAN_SHEET_BONUS = 50

GRADE_THRESHOLDS = [
    (0.95, "A+", 2.0),
    (0.80, "A",  1.5),
    (0.60, "B",  1.0),
    (0.30, "C",  0.5),
    (0.00, "D",  0.0),
]


def _get_fx_rate(conn, month_str):
    """Get average FX rate for a month (UZS per USD)."""
    row = conn.execute(
        "SELECT AVG(rate) as avg_rate FROM daily_fx_rates WHERE rate_date LIKE ?",
        (f"{month_str}%",),
    ).fetchone()
    if row and row["avg_rate"]:
        return float(row["avg_rate"])
    row = conn.execute(
        "SELECT rate FROM daily_fx_rates ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    return float(row["rate"]) if row else 13000.0


def _grade_from_on_time_rate(on_time_rate):
    """Convert on_time_rate (0-1) to discipline grade + multiplier."""
    if on_time_rate < 0:
        return "C", 0.5
    for threshold, grade, mult in GRADE_THRESHOLDS:
        if on_time_rate >= threshold:
            return grade, mult
    return "D", 0.0


def calculate_monthly_points(month_str=None, conn=None):
    """Calculate loyalty points for all clients for a given month.

    Args:
        month_str: "YYYY-MM" format. Defaults to previous month.

    Returns dict with summary stats.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db()

    if not month_str:
        today = date.today()
        if today.month == 1:
            month_str = f"{today.year - 1}-12"
        else:
            month_str = f"{today.year}-{today.month - 1:02d}"

    try:
        fx_rate = _get_fx_rate(conn, month_str)

        # Get purchase volumes per client for this month
        purchases = conn.execute(
            """SELECT client_id, client_name_1c as client_name,
                      SUM(COALESCE(total_sum, 0)) as total_uzs,
                      SUM(COALESCE(total_sum_currency, 0)) as total_usd
               FROM real_orders
               WHERE doc_date LIKE ? AND client_id IS NOT NULL
               GROUP BY client_id""",
            (f"{month_str}%",),
        ).fetchall()

        if not purchases:
            return {"ok": True, "month": month_str, "scored": 0, "message": "No orders found"}

        # Get latest scoring data for discipline grades
        score_data = {}
        scores = conn.execute(
            """SELECT client_id, on_time_rate, volume_bucket, debt_ratio
               FROM client_scores
               WHERE recalc_date = (SELECT MAX(recalc_date) FROM client_scores)"""
        ).fetchall()
        for s in scores:
            score_data[s["client_id"]] = {
                "on_time_rate": float(s["on_time_rate"] or 0),
                "volume_bucket": s["volume_bucket"] or "Micro",
                "debt_ratio": float(s["debt_ratio"] or 0),
            }

        # Check for clean sheet (zero debt at end of month)
        debt_data = {}
        try:
            debts = conn.execute(
                """SELECT client_id, SUM(COALESCE(balance, 0)) as total_debt
                   FROM client_balances
                   WHERE client_id IS NOT NULL
                   GROUP BY client_id"""
            ).fetchall()
            for d in debts:
                debt_data[d["client_id"]] = float(d["total_debt"] or 0)
        except Exception:
            pass

        scored = 0
        grade_counts = {"A+": 0, "A": 0, "B": 0, "C": 0, "D": 0}
        total_points = 0

        for p in purchases:
            cid = p["client_id"]
            uzs = float(p["total_uzs"] or 0)
            usd = float(p["total_usd"] or 0)

            # Axis 1: purchase points
            points_from_uzs = int(uzs / 10000) * POINTS_PER_10K_UZS
            points_from_usd = int(usd * POINTS_PER_USD)
            purchase_points = points_from_uzs + points_from_usd

            # Axis 2: discipline grade
            sd = score_data.get(cid, {})
            on_time_rate = sd.get("on_time_rate", 0)
            grade, multiplier = _grade_from_on_time_rate(on_time_rate)
            bucket = sd.get("volume_bucket", "Micro")

            # Clean sheet bonus
            client_debt = debt_data.get(cid, 0)
            clean_sheet = CLEAN_SHEET_BONUS if client_debt <= 0.01 else 0

            # Effective points
            effective = int(purchase_points * multiplier) + clean_sheet

            conn.execute(
                """INSERT OR REPLACE INTO client_points_monthly
                   (client_id, client_name, month, purchase_uzs, purchase_usd,
                    purchase_points, discipline_grade, multiplier,
                    clean_sheet_bonus, effective_points, volume_bucket)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (cid, p["client_name"] or "", month_str,
                 uzs, usd, purchase_points, grade, multiplier,
                 clean_sheet, effective, bucket),
            )

            grade_counts[grade] = grade_counts.get(grade, 0) + 1
            total_points += effective
            scored += 1

        # Calculate per-bucket rankings
        buckets = conn.execute(
            """SELECT DISTINCT volume_bucket FROM client_points_monthly WHERE month = ?""",
            (month_str,),
        ).fetchall()

        for b in buckets:
            bucket_name = b["volume_bucket"]
            ranked = conn.execute(
                """SELECT id, client_id FROM client_points_monthly
                   WHERE month = ? AND volume_bucket = ?
                   ORDER BY effective_points DESC""",
                (month_str, bucket_name),
            ).fetchall()
            total_in_bucket = len(ranked)
            for rank, r in enumerate(ranked, 1):
                conn.execute(
                    "UPDATE client_points_monthly SET bucket_rank = ?, bucket_total = ? WHERE id = ?",
                    (rank, total_in_bucket, r["id"]),
                )

        conn.commit()

        return {
            "ok": True,
            "month": month_str,
            "scored": scored,
            "total_points": total_points,
            "fx_rate": fx_rate,
            "grades": grade_counts,
        }
    except Exception as e:
        logger.error(f"Points calculation failed: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}
    finally:
        if own_conn:
            conn.close()


def get_client_points(client_id, limit=6):
    """Get points history for a client (for Cabinet display)."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT month, purchase_points, discipline_grade, multiplier,
                      clean_sheet_bonus, effective_points, volume_bucket,
                      bucket_rank, bucket_total
               FROM client_points_monthly
               WHERE client_id = ?
               ORDER BY month DESC
               LIMIT ?""",
            (client_id, limit),
        ).fetchall()

        total = conn.execute(
            "SELECT SUM(effective_points) as total FROM client_points_monthly WHERE client_id = ?",
            (client_id,),
        ).fetchone()

        return {
            "ok": True,
            "total_points": int(total["total"] or 0) if total else 0,
            "months": [dict(r) for r in rows],
        }
    finally:
        conn.close()


def get_leaderboard(month_str=None, bucket=None, limit=10):
    """Get leaderboard for a month, optionally filtered by bucket."""
    conn = get_db()
    try:
        if not month_str:
            row = conn.execute(
                "SELECT MAX(month) as m FROM client_points_monthly"
            ).fetchone()
            month_str = row["m"] if row else None
            if not month_str:
                return {"ok": True, "month": None, "leaders": []}

        conditions = ["month = ?"]
        params = [month_str]
        if bucket:
            conditions.append("volume_bucket = ?")
            params.append(bucket)

        rows = conn.execute(
            f"""SELECT client_name, effective_points, purchase_points,
                       discipline_grade, multiplier, clean_sheet_bonus,
                       volume_bucket, bucket_rank, bucket_total
                FROM client_points_monthly
                WHERE {' AND '.join(conditions)}
                ORDER BY effective_points DESC
                LIMIT ?""",
            params + [limit],
        ).fetchall()

        return {
            "ok": True,
            "month": month_str,
            "leaders": [dict(r) for r in rows],
        }
    finally:
        conn.close()
