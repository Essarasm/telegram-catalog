"""Session G — Credit Scoring Engine.

Computes a 0–100 credit score for every Rassvet wholesale client based on
four factors:
  1. Payment Discipline  (40 pts) — FIFO payment-to-shipment allocation
  2. Debt Ratio          (25 pts) — current debt vs monthly volume
  3. Payment Consistency  (20 pts) — regularity of payment intervals
  4. Tenure              (15 pts) — length of relationship (logarithmic)

Volume buckets (Micro / Small / Medium / Large / Heavy) are orthogonal to
the score.  Credit limit = bucket_base_limit × (score / 100).

All monetary values are normalized to USD using daily_fx_rates.

See: obsidian-vault/Session G — Credit Scoring Algorithm Spec.md
"""
from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from backend.database import get_db

logger = logging.getLogger(__name__)

# ── Tuneable constants (from spec §12) ───────────────────────────

SCORING_LAG_BUFFER_DAYS = 1
DEFAULT_CREDIT_TERM_DAYS = 14
NEW_CLIENT_DEFAULT_SCORE = 50
VOLUME_BUCKET_LOOKBACK_MONTHS = 6
TENURE_CAP_MONTHS = 24
DEBT_RATIO_CLAMP_MAX = 3.0

# Volume bucket thresholds (monthly USD)
BUCKET_THRESHOLDS: List[Tuple[str, float, float]] = [
    # (bucket_name, min_monthly_usd, base_credit_limit_uzs)
    ("Micro",  0,      1_000_000),      # < $300/mo  → 1M UZS base
    ("Small",  300,    5_000_000),      # $300-1500  → 5M UZS base
    ("Medium", 1_500,  20_000_000),     # $1.5K-5K   → 20M UZS base
    ("Large",  5_000,  100_000_000),    # $5K-12K    → 100M UZS base
    ("Heavy",  12_000, 0),             # >$12K      → manual review
]

TIER_RANGES: List[Tuple[str, int, int]] = [
    ("Yangi",  0,  30),
    ("Oddiy", 31,  50),
    ("Yaxshi", 51, 70),
    ("A'lo",  71,  90),
    ("VIP",   91, 100),
]

# Seasonality index by month (1=Jan..12=Dec), derived from spec §2.2.
# Normalised so that August (peak) = 1.0.
# These are rough indices from the order volume analysis; the per-client
# baseline will refine them but we need global fallback for new clients.
SEASONAL_INDEX = {
    1: 0.53, 2: 0.55, 3: 0.53, 4: 0.65, 5: 0.83, 6: 0.90,
    7: 0.95, 8: 1.00, 9: 0.92, 10: 0.75, 11: 0.55, 12: 0.40,
}


# ── Helper: get latest FX rate ───────────────────────────────────

def _get_fx_rate(conn, for_date: str) -> float:
    """Return the USD/UZS rate for the given date (or nearest prior date).

    Falls back to 12_500 if no rates exist at all.
    """
    row = conn.execute(
        """SELECT rate FROM daily_fx_rates
           WHERE rate_date <= ? AND currency_pair = 'USD_UZS'
           ORDER BY rate_date DESC LIMIT 1""",
        (for_date,),
    ).fetchone()
    if row:
        return float(row["rate"])
    # fallback
    return 12_500.0


def _amount_to_usd(amount_local: float, amount_currency: float,
                    currency: str, fx_rate: float) -> float:
    """Convert a payment/order amount to USD."""
    if currency == "USD":
        return amount_currency if amount_currency else 0.0
    # UZS → USD
    if fx_rate and fx_rate > 0:
        return amount_local / fx_rate
    return 0.0


# ── Data loading ─────────────────────────────────────────────────

def _load_client_payments(conn) -> Dict[int, List[dict]]:
    """Load all client_payments grouped by client_id.

    Only payments with a matched client_id are included.
    Returns {client_id: [sorted list of payment dicts]}.
    """
    rows = conn.execute(
        """SELECT client_id, doc_date, currency, amount_local,
                  amount_currency, fx_rate
           FROM client_payments
           WHERE client_id IS NOT NULL
           ORDER BY client_id, doc_date"""
    ).fetchall()

    payments: Dict[int, List[dict]] = defaultdict(list)
    for r in rows:
        payments[r["client_id"]].append({
            "doc_date": r["doc_date"],
            "currency": r["currency"],
            "amount_local": float(r["amount_local"] or 0),
            "amount_currency": float(r["amount_currency"] or 0),
            "fx_rate": float(r["fx_rate"] or 0),
        })
    return payments


def _load_client_shipments(conn) -> Dict[int, List[dict]]:
    """Load all real_orders grouped by client_id.

    Returns {client_id: [sorted list of order dicts]}.
    """
    rows = conn.execute(
        """SELECT client_id, doc_date, currency, total_sum,
                  total_sum_currency, exchange_rate
           FROM real_orders
           WHERE client_id IS NOT NULL
           ORDER BY client_id, doc_date"""
    ).fetchall()

    shipments: Dict[int, List[dict]] = defaultdict(list)
    for r in rows:
        shipments[r["client_id"]].append({
            "doc_date": r["doc_date"],
            "currency": r["currency"] or "UZS",
            "total_sum": float(r["total_sum"] or 0),
            "total_sum_currency": float(r["total_sum_currency"] or 0),
            "exchange_rate": float(r["exchange_rate"] or 1),
        })
    return shipments


def _load_client_debts(conn) -> Dict[int, dict]:
    """Load latest debt snapshot per client_id.

    Returns {client_id: {debt_uzs, debt_usd, report_date}}.
    """
    rows = conn.execute(
        """SELECT client_id, debt_uzs, debt_usd, report_date
           FROM client_debts
           WHERE client_id IS NOT NULL
           ORDER BY report_date DESC"""
    ).fetchall()

    debts: Dict[int, dict] = {}
    for r in rows:
        cid = r["client_id"]
        if cid not in debts:  # keep only the latest per client
            debts[cid] = {
                "debt_uzs": float(r["debt_uzs"] or 0),
                "debt_usd": float(r["debt_usd"] or 0),
                "report_date": r["report_date"],
            }
    return debts


def _load_client_names(conn) -> Dict[int, str]:
    """Load client_id → display name mapping from allowed_clients."""
    rows = conn.execute(
        "SELECT id, COALESCE(company_name, name, '') as dname FROM allowed_clients"
    ).fetchall()
    return {r["id"]: r["dname"] for r in rows}


# ── Factor 1: Payment Discipline (FIFO) ─────────────────────────

def _compute_discipline(shipments: List[dict], payments: List[dict],
                        default_fx: float) -> Tuple[float, float]:
    """FIFO-allocate payments against shipments and compute on-time rate.

    Returns (discipline_score [0-40], on_time_rate [0-1]).
    """
    if not shipments:
        return 0.0, 0.0

    # Convert shipments to USD amounts with dates
    ship_queue = []
    for s in shipments:
        usd = _amount_to_usd(
            s["total_sum"], s["total_sum_currency"],
            s["currency"], s.get("exchange_rate") or default_fx,
        )
        if usd <= 0:
            continue
        ship_queue.append({
            "date": s["doc_date"],
            "amount_usd": usd,
            "remaining": usd,
            "paid_date": None,
        })

    if not ship_queue:
        return 0.0, 0.0

    # Sort payments by date for FIFO allocation
    pay_list = []
    for p in payments:
        usd = _amount_to_usd(
            p["amount_local"], p["amount_currency"],
            p["currency"], p.get("fx_rate") or default_fx,
        )
        if usd <= 0:
            continue
        pay_list.append({"date": p["doc_date"], "amount_usd": usd, "remaining": usd})

    # FIFO: allocate each payment to the oldest unpaid shipment
    for pay in pay_list:
        for ship in ship_queue:
            if ship["remaining"] <= 0:
                continue
            alloc = min(pay["remaining"], ship["remaining"])
            ship["remaining"] -= alloc
            pay["remaining"] -= alloc
            if ship["remaining"] <= 0.01:  # fully paid
                ship["paid_date"] = pay["date"]
            if pay["remaining"] <= 0.01:
                break

    # Evaluate each shipment
    on_time = 0
    late = 0
    unpaid = 0

    for ship in ship_queue:
        if ship["paid_date"] is None:
            unpaid += 1
            continue
        try:
            ship_dt = datetime.strptime(ship["date"], "%Y-%m-%d").date()
            pay_dt = datetime.strptime(ship["paid_date"], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            unpaid += 1
            continue

        days_to_pay = (pay_dt - ship_dt).days - SCORING_LAG_BUFFER_DAYS
        if days_to_pay <= DEFAULT_CREDIT_TERM_DAYS:
            on_time += 1
        else:
            late += 1

    total = on_time + late + unpaid
    if total == 0:
        return 0.0, 0.0

    on_time_rate = on_time / total
    discipline_score = on_time_rate * 40.0
    return discipline_score, on_time_rate


# ── Factor 2: Debt Ratio ────────────────────────────────────────

def _compute_debt_ratio(debt_info: Optional[dict],
                        monthly_volume_usd: float,
                        default_fx: float) -> Tuple[float, float]:
    """Compute debt score from current debt vs monthly volume.

    Returns (debt_score [0-25], debt_ratio [0-3]).
    """
    if not debt_info or monthly_volume_usd <= 0:
        return 25.0, 0.0  # no debt data → full marks (benefit of doubt)

    debt_usd = debt_info["debt_usd"] + (
        debt_info["debt_uzs"] / default_fx if default_fx > 0 else 0
    )

    if debt_usd <= 0:
        return 25.0, 0.0

    debt_ratio = debt_usd / monthly_volume_usd
    debt_ratio = min(debt_ratio, DEBT_RATIO_CLAMP_MAX)

    debt_score = (1 - debt_ratio / DEBT_RATIO_CLAMP_MAX) * 25.0
    return max(0.0, debt_score), debt_ratio


# ── Factor 3: Payment Consistency ────────────────────────────────

def _compute_consistency(payments: List[dict]) -> Tuple[float, float]:
    """Compute consistency score from inter-payment interval CV.

    Returns (consistency_score [0-20], cv).
    """
    if len(payments) < 3:
        # Not enough data for meaningful consistency — give neutral score
        return 10.0, 1.0

    # Compute inter-payment intervals in days
    dates = []
    for p in payments:
        try:
            dt = datetime.strptime(p["doc_date"], "%Y-%m-%d").date()
            dates.append(dt)
        except (ValueError, TypeError):
            continue

    if len(dates) < 3:
        return 10.0, 1.0

    dates.sort()
    intervals = []
    for i in range(1, len(dates)):
        gap = (dates[i] - dates[i - 1]).days
        if gap > 0:  # skip same-day payments
            intervals.append(gap)

    if len(intervals) < 2:
        return 10.0, 1.0

    mean_interval = sum(intervals) / len(intervals)
    if mean_interval <= 0:
        return 10.0, 1.0

    variance = sum((x - mean_interval) ** 2 for x in intervals) / len(intervals)
    std_dev = math.sqrt(variance)
    cv = std_dev / mean_interval

    # Cap CV at 2.0 to avoid extreme penalties
    cv = min(cv, 2.0)

    consistency_score = max(0.0, (1 - cv) * 20.0)
    return consistency_score, cv


# ── Factor 4: Tenure ─────────────────────────────────────────────

def _compute_tenure(shipments: List[dict]) -> Tuple[float, float]:
    """Compute tenure score using logarithmic curve.

    Returns (tenure_score [0-15], tenure_months).
    """
    if not shipments:
        return 0.0, 0.0

    # Find earliest shipment date
    earliest = None
    for s in shipments:
        try:
            dt = datetime.strptime(s["doc_date"], "%Y-%m-%d").date()
            if earliest is None or dt < earliest:
                earliest = dt
        except (ValueError, TypeError):
            continue

    if earliest is None:
        return 0.0, 0.0

    today = date.today()
    tenure_days = (today - earliest).days
    tenure_months = tenure_days / 30.44  # average days per month

    if tenure_months <= 0:
        return 0.0, 0.0

    # Formula: min(15, 15 × ln(1 + tenure_months/6) / ln(5))
    score = min(15.0, 15.0 * math.log(1 + tenure_months / 6) / math.log(5))
    return max(0.0, score), tenure_months


# ── Volume bucket classification ─────────────────────────────────

def _classify_bucket(monthly_volume_usd: float) -> Tuple[str, float]:
    """Classify client into volume bucket.

    Returns (bucket_name, base_credit_limit_uzs).
    """
    # Walk thresholds in reverse to find the highest matching bucket
    for name, min_usd, base_limit in reversed(BUCKET_THRESHOLDS):
        if monthly_volume_usd >= min_usd:
            return name, base_limit
    return "Micro", BUCKET_THRESHOLDS[0][2]


def _classify_tier(score: int) -> str:
    """Classify score into tier name."""
    for name, lo, hi in TIER_RANGES:
        if lo <= score <= hi:
            return name
    return "Yangi"


# ── Monthly volume computation ───────────────────────────────────

def _compute_monthly_volume_usd(shipments: List[dict], payments: List[dict],
                                  default_fx: float) -> float:
    """Compute average monthly volume (USD) over trailing 6 months.

    Uses both shipments (orders) and payments to get the most complete picture.
    """
    cutoff = date.today() - timedelta(days=VOLUME_BUCKET_LOOKBACK_MONTHS * 30)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    total_usd = 0.0
    # Sum shipments in the window
    for s in shipments:
        if s["doc_date"] >= cutoff_str:
            total_usd += _amount_to_usd(
                s["total_sum"], s["total_sum_currency"],
                s["currency"], s.get("exchange_rate") or default_fx,
            )

    # If no shipments, fall back to payments
    if total_usd <= 0:
        for p in payments:
            if p["doc_date"] >= cutoff_str:
                total_usd += _amount_to_usd(
                    p["amount_local"], p["amount_currency"],
                    p["currency"], p.get("fx_rate") or default_fx,
                )

    monthly = total_usd / VOLUME_BUCKET_LOOKBACK_MONTHS
    return monthly


# ── Main scoring function ────────────────────────────────────────

def score_single_client(
    client_id: int,
    shipments: List[dict],
    payments: List[dict],
    debt_info: Optional[dict],
    default_fx: float,
) -> dict:
    """Compute the full credit score for a single client.

    Returns a dict with all score components.
    """
    # Check if client has enough history
    has_payment = len(payments) > 0
    has_shipment = len(shipments) > 0

    if not has_payment and not has_shipment:
        # Brand new client — return default score
        return {
            "score": NEW_CLIENT_DEFAULT_SCORE,
            "tier": _classify_tier(NEW_CLIENT_DEFAULT_SCORE),
            "volume_bucket": "Micro",
            "monthly_volume_usd": 0.0,
            "credit_limit_uzs": BUCKET_THRESHOLDS[0][2] * 0.5,
            "discipline_score": 0.0,
            "debt_score": 25.0,
            "consistency_score": 10.0,
            "tenure_score": 0.0,
            "on_time_rate": 0.0,
            "debt_ratio": 0.0,
            "consistency_cv": 1.0,
            "tenure_months": 0.0,
        }

    # Monthly volume
    monthly_volume_usd = _compute_monthly_volume_usd(
        shipments, payments, default_fx
    )

    # Factor 1: Discipline (40 pts)
    discipline_score, on_time_rate = _compute_discipline(
        shipments, payments, default_fx
    )

    # Factor 2: Debt Ratio (25 pts)
    debt_score, debt_ratio = _compute_debt_ratio(
        debt_info, monthly_volume_usd, default_fx
    )

    # Factor 3: Consistency (20 pts)
    consistency_score, consistency_cv = _compute_consistency(payments)

    # Factor 4: Tenure (15 pts)
    tenure_score, tenure_months = _compute_tenure(shipments)

    # If client is very new (< 1 month), use default score
    if tenure_months < 1.0 and len(payments) < 2:
        return {
            "score": NEW_CLIENT_DEFAULT_SCORE,
            "tier": _classify_tier(NEW_CLIENT_DEFAULT_SCORE),
            "volume_bucket": "Micro",
            "monthly_volume_usd": monthly_volume_usd,
            "credit_limit_uzs": BUCKET_THRESHOLDS[0][2] * 0.5,
            "discipline_score": 0.0,
            "debt_score": 25.0,
            "consistency_score": 10.0,
            "tenure_score": 0.0,
            "on_time_rate": 0.0,
            "debt_ratio": 0.0,
            "consistency_cv": 1.0,
            "tenure_months": tenure_months,
        }

    # Final score
    raw_score = discipline_score + debt_score + consistency_score + tenure_score
    score = max(0, min(100, round(raw_score)))

    # Volume bucket & credit limit
    bucket, base_limit = _classify_bucket(monthly_volume_usd)
    if bucket == "Heavy":
        credit_limit_uzs = 0.0  # manual review
    else:
        credit_limit_uzs = base_limit * (score / 100.0)

    tier = _classify_tier(score)

    return {
        "score": score,
        "tier": tier,
        "volume_bucket": bucket,
        "monthly_volume_usd": monthly_volume_usd,
        "credit_limit_uzs": credit_limit_uzs,
        "discipline_score": round(discipline_score, 1),
        "debt_score": round(debt_score, 1),
        "consistency_score": round(consistency_score, 1),
        "tenure_score": round(tenure_score, 1),
        "on_time_rate": round(on_time_rate, 3),
        "debt_ratio": round(debt_ratio, 2),
        "consistency_cv": round(consistency_cv, 2),
        "tenure_months": round(tenure_months, 1),
    }


# ── Batch scoring (nightly run) ──────────────────────────────────

def run_nightly_scoring() -> dict:
    """Score ALL clients and upsert into client_scores.

    Returns summary stats.
    """
    conn = get_db()
    try:
        today_str = date.today().strftime("%Y-%m-%d")
        now_time = datetime.now().strftime("%H:%M:%S")
        default_fx = _get_fx_rate(conn, today_str)

        # Load all data
        all_payments = _load_client_payments(conn)
        all_shipments = _load_client_shipments(conn)
        all_debts = _load_client_debts(conn)
        client_names = _load_client_names(conn)

        # Collect all client IDs that have any financial activity
        all_client_ids = set(all_payments.keys()) | set(all_shipments.keys())

        scored = 0
        tier_counts: Dict[str, int] = defaultdict(int)
        bucket_counts: Dict[str, int] = defaultdict(int)

        for cid in all_client_ids:
            shipments = all_shipments.get(cid, [])
            payments = all_payments.get(cid, [])
            debt_info = all_debts.get(cid)
            client_name = client_names.get(cid, f"Client #{cid}")

            result = score_single_client(
                cid, shipments, payments, debt_info, default_fx
            )

            # Upsert into client_scores
            conn.execute(
                """INSERT INTO client_scores
                   (client_id, client_name, score, tier, volume_bucket,
                    monthly_volume_usd, credit_limit_uzs,
                    discipline_score, debt_score, consistency_score, tenure_score,
                    on_time_rate, debt_ratio, consistency_cv, tenure_months,
                    recalc_date, recalc_time)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(client_id, recalc_date) DO UPDATE SET
                    client_name=excluded.client_name,
                    score=excluded.score, tier=excluded.tier,
                    volume_bucket=excluded.volume_bucket,
                    monthly_volume_usd=excluded.monthly_volume_usd,
                    credit_limit_uzs=excluded.credit_limit_uzs,
                    discipline_score=excluded.discipline_score,
                    debt_score=excluded.debt_score,
                    consistency_score=excluded.consistency_score,
                    tenure_score=excluded.tenure_score,
                    on_time_rate=excluded.on_time_rate,
                    debt_ratio=excluded.debt_ratio,
                    consistency_cv=excluded.consistency_cv,
                    tenure_months=excluded.tenure_months,
                    recalc_time=excluded.recalc_time""",
                (
                    cid, client_name, result["score"], result["tier"],
                    result["volume_bucket"], result["monthly_volume_usd"],
                    result["credit_limit_uzs"],
                    result["discipline_score"], result["debt_score"],
                    result["consistency_score"], result["tenure_score"],
                    result["on_time_rate"], result["debt_ratio"],
                    result["consistency_cv"], result["tenure_months"],
                    today_str, now_time,
                ),
            )

            # Also update allowed_clients with latest score/limit
            conn.execute(
                """UPDATE allowed_clients
                   SET credit_score = ?, credit_limit = ?
                   WHERE id = ?""",
                (result["score"], result["credit_limit_uzs"], cid),
            )

            tier_counts[result["tier"]] += 1
            bucket_counts[result["volume_bucket"]] += 1
            scored += 1

        conn.commit()

        summary = {
            "ok": True,
            "scored": scored,
            "date": today_str,
            "fx_rate": default_fx,
            "tiers": dict(tier_counts),
            "buckets": dict(bucket_counts),
        }
        logger.info("Nightly scoring complete: %s", summary)
        return summary

    except Exception as e:
        logger.exception("Scoring failed: %s", e)
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


# ── Single-client score lookup (for /clientscore) ────────────────

def get_client_score(client_id: int) -> Optional[dict]:
    """Get the latest score for a client by ID."""
    conn = get_db()
    try:
        row = conn.execute(
            """SELECT * FROM client_scores
               WHERE client_id = ?
               ORDER BY recalc_date DESC LIMIT 1""",
            (client_id,),
        ).fetchone()
        if row:
            return dict(row)
        return None
    finally:
        conn.close()


def search_client_scores(name_substring: str, limit: int = 10) -> List[dict]:
    """Search for clients by name substring and return their latest scores.

    Uses case-insensitive LIKE matching on client_name.
    """
    conn = get_db()
    try:
        # Get the latest recalc_date
        latest = conn.execute(
            "SELECT MAX(recalc_date) as d FROM client_scores"
        ).fetchone()
        if not latest or not latest["d"]:
            return []

        pattern = f"%{name_substring}%"
        rows = conn.execute(
            """SELECT * FROM client_scores
               WHERE recalc_date = ? AND client_name LIKE ?
               ORDER BY score DESC
               LIMIT ?""",
            (latest["d"], pattern, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_scoring_summary() -> dict:
    """Get summary stats from the latest scoring run."""
    conn = get_db()
    try:
        latest = conn.execute(
            "SELECT MAX(recalc_date) as d FROM client_scores"
        ).fetchone()
        if not latest or not latest["d"]:
            return {"ok": False, "error": "No scoring data yet"}

        d = latest["d"]
        total = conn.execute(
            "SELECT COUNT(*) as c FROM client_scores WHERE recalc_date = ?",
            (d,),
        ).fetchone()["c"]

        avg_score = conn.execute(
            "SELECT AVG(score) as a FROM client_scores WHERE recalc_date = ?",
            (d,),
        ).fetchone()["a"]

        tier_rows = conn.execute(
            """SELECT tier, COUNT(*) as c FROM client_scores
               WHERE recalc_date = ? GROUP BY tier""",
            (d,),
        ).fetchall()
        tiers = {r["tier"]: r["c"] for r in tier_rows}

        bucket_rows = conn.execute(
            """SELECT volume_bucket, COUNT(*) as c FROM client_scores
               WHERE recalc_date = ? GROUP BY volume_bucket""",
            (d,),
        ).fetchall()
        buckets = {r["volume_bucket"]: r["c"] for r in bucket_rows}

        return {
            "ok": True,
            "date": d,
            "total_clients": total,
            "avg_score": round(avg_score, 1) if avg_score else 0,
            "tiers": tiers,
            "buckets": buckets,
        }
    finally:
        conn.close()
