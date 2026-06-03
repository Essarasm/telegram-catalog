"""Client-portfolio categorization — the live Level × Trajectory matrix.

Two orthogonal axes, computed over the CLEAN roster (pseudo-accounts excluded
via the canonical `pseudo_clients` list — one source of truth with the rest of
the app):

  Level     — trailing-12-month monthly USD-eq → Proposal B bucket
              (Micro/Small/Medium/Large/Heavy). Seasonally neutral by design.
  Trajectory— trailing-120d spend vs the SAME 120 calendar days last year,
              measured RELATIVE TO the business-wide YoY "tide", with the
              Rising/Stable/Sliding cut points taken from the data itself
              (quartiles of the relative-YoY distribution) — never hardcoded.

Cohorts are mutually exclusive: Dormant (no order in 60d) → New (<12mo
history) → Established. Only Established + active + ≥2 prior-window orders are
trajectory-rated; sparse-history established clients are reported separately so
the counts reconcile (no silent drop).

Decisions locked with Ulugbek 2026-06-01 (Session X):
  - level window = 12 months
  - bands anchored to data (quartile cuts), not assumed ±%
  - trajectory window widened to 120d + min-prior-orders filter to cut noise
  - New + Dormant are first-class segments, not footnotes
See memory `bucketing_schemes` (Proposal B is the SOLE size scheme) and
`.claude/rules/12-dual-source-columns.md`.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from backend.services.pseudo_clients import (
    sql_exclusion_clause,
    sql_exclusion_params,
)

TK = ZoneInfo("Asia/Tashkent")
FX_FALLBACK = 12_000.0

# Proposal B size thresholds (monthly USD-eq). The SOLE client-size scheme.
_SIZE_EDGES = [("Micro", 0), ("Small", 125), ("Medium", 621),
               ("Large", 1721), ("Heavy", 4120)]
BUCKETS = [name for name, _ in _SIZE_EDGES]
BANDS = ["Rising", "Stable", "Sliding"]

LEVEL_DAYS = 365        # size/bucket window
TRAJ_DAYS = 120         # trajectory current/prior window width
DORMANT_DAYS = 60       # no order in this many days → Dormant cohort
MIN_PRIOR_ORDERS = 2    # noise filter for trajectory eligibility


def _bucket(monthly_usd: float) -> str:
    b = "Micro"
    for name, edge in _SIZE_EDGES:
        if monthly_usd >= edge:
            b = name
    return b


def compute_portfolio(conn) -> dict:
    """Compute the full portfolio matrix. Caller owns the connection."""
    today = datetime.now(TK).date()
    level_start = today - timedelta(days=LEVEL_DAYS)
    cur_start = today - timedelta(days=TRAJ_DAYS)
    prior_start = today - timedelta(days=LEVEL_DAYS + TRAJ_DAYS)
    prior_end = today - timedelta(days=LEVEL_DAYS)
    active_cut = (today - timedelta(days=DORMANT_DAYS)).isoformat()

    excl = sql_exclusion_clause("client_name_1c")
    excl_params = sql_exclusion_params()

    # Single avg FX across the whole span so trajectory reflects VOLUME, not FX.
    fx_rows = conn.execute(
        "SELECT rate FROM daily_fx_rates WHERE currency_pair='USD_UZS' AND rate>0"
    ).fetchall()
    avg_fx = (sum(float(r["rate"]) for r in fx_rows) / len(fx_rows)) if fx_rows else FX_FALLBACK

    def window(start, end):
        rows = conn.execute(
            f"""SELECT COALESCE(client_id,'NAME:'||client_name_1c) ckey,
                       MAX(client_name_1c) name,
                       SUM(total_sum) uzs, SUM(total_sum_currency) usd, COUNT(*) n
                  FROM real_orders
                 WHERE doc_date BETWEEN ? AND ?
                   AND COALESCE(is_approved,1)=1 AND {excl}
                 GROUP BY ckey""",
            (start.isoformat(), end.isoformat(), *excl_params),
        ).fetchall()
        return {r["ckey"]: {"name": r["name"],
                            "usd_eq": float(r["uzs"] or 0) / avg_fx + float(r["usd"] or 0),
                            "n": r["n"]} for r in rows}

    hist = {r["ckey"]: (r["first_order"], r["last_order"]) for r in conn.execute(
        f"""SELECT COALESCE(client_id,'NAME:'||client_name_1c) ckey,
                   MIN(doc_date) first_order, MAX(doc_date) last_order
              FROM real_orders
             WHERE COALESCE(is_approved,1)=1 AND {excl}
             GROUP BY ckey""",
        excl_params,
    ).fetchall()}

    lvl = window(level_start, today)         # 12mo roster (the active base)
    cur = window(cur_start, today)           # current 120d
    pri = window(prior_start, prior_end)     # same 120d last year

    def monthly(ckey):
        return lvl[ckey]["usd_eq"] / 12.0

    def is_active(ckey):
        lo = hist.get(ckey, (None, None))[1]
        return bool(lo and lo >= active_cut)

    def is_established(ckey):
        fo = hist.get(ckey, (None, None))[0]
        return bool(fo and fo <= level_start.isoformat())

    # ---- Cohorts (mutually exclusive) over the 12mo active roster ----
    dormant, new_active, established = [], [], []
    for k in lvl:
        if not is_active(k):
            dormant.append(k)
        elif not is_established(k):
            new_active.append(k)
        else:
            established.append(k)

    # ---- Trajectory eligibility + business tide ----
    eligible = [k for k in established if pri.get(k, {}).get("n", 0) >= MIN_PRIOR_ORDERS]
    biz_cur = sum(cur.get(k, {}).get("usd_eq", 0.0) for k in eligible)
    biz_pri = sum(pri[k]["usd_eq"] for k in eligible)
    tide = ((biz_cur - biz_pri) / biz_pri * 100.0) if biz_pri > 0 else 0.0

    rel = {}
    for k in eligible:
        p = pri[k]["usd_eq"]
        c = cur.get(k, {}).get("usd_eq", 0.0)
        rel[k] = ((c - p) / p * 100.0) - tide if p > 0 else 0.0

    relvals = sorted(rel.values())

    def quantile(p):
        if not relvals:
            return 0.0
        i = max(0, min(len(relvals) - 1, int(round(p * (len(relvals) - 1)))))
        return relvals[i]

    rising_cut = round(quantile(0.75), 1)    # top quartile → Rising
    sliding_cut = round(quantile(0.25), 1)   # bottom quartile → Sliding

    def band(k):
        r = rel[k]
        if r >= rising_cut:
            return "Rising"
        if r <= sliding_cut:
            return "Sliding"
        return "Stable"

    # ---- Matrix + per-cell client call-lists ----
    matrix = {b: {bd: 0 for bd in BANDS} for b in BUCKETS}
    cells = {f"{b}|{bd}": [] for b in BUCKETS for bd in BANDS}
    for k in eligible:
        b, bd = _bucket(monthly(k)), band(k)
        matrix[b][bd] += 1
        cells[f"{b}|{bd}"].append({
            "name": lvl[k]["name"],
            "monthly_usd": round(monthly(k)),
            "yoy_rel_pct": round(rel[k], 1),
            "last_order": hist[k][1],
        })
    for key in cells:
        cells[key].sort(key=lambda x: -x["monthly_usd"])

    # ---- Size rows (repoints the existing size table to THIS roster) ----
    latest_score = conn.execute(
        "SELECT MAX(recalc_date) d FROM client_scores"
    ).fetchone()
    scores = {}
    if latest_score and latest_score["d"]:
        for r in conn.execute(
            "SELECT client_id, score FROM client_scores WHERE recalc_date=?",
            (latest_score["d"],),
        ).fetchall():
            if r["client_id"] is not None:
                scores[str(r["client_id"])] = r["score"]

    size_rows = []
    total_clients = len(lvl)
    for b in BUCKETS:
        members = [k for k in lvl if _bucket(monthly(k)) == b]
        vols = [monthly(k) for k in members]
        # roster keys come back as ints from SQLite COALESCE; scores is keyed by
        # str(client_id) — normalize both sides to str so the join lands.
        sc = [scores[str(k)] for k in members
              if str(k) in scores and scores[str(k)] is not None]
        size_rows.append({
            "bucket": b,
            "clients": len(members),
            "share_pct": round(len(members) / total_clients * 100, 1) if total_clients else 0,
            "vol_min": round(min(vols)) if vols else 0,
            "vol_max": round(max(vols)) if vols else 0,
            "vol_avg": round(sum(vols) / len(vols)) if vols else 0,
            "vol_total": round(sum(vols)),
            "avg_score": round(sum(sc) / len(sc), 1) if sc else None,
        })

    # ---- Cohort drill lists (cheap; lets the UI expand New / Dormant too) ----
    def cohort_list(keys):
        out = [{
            "name": lvl[k]["name"],
            "bucket": _bucket(monthly(k)),
            "monthly_usd": round(monthly(k)),
            "last_order": hist[k][1],
        } for k in keys]
        out.sort(key=lambda x: -x["monthly_usd"])
        return out

    return {
        "as_of": today.isoformat(),
        "avg_fx": round(avg_fx, 1),
        "windows": {
            "level_12mo": [level_start.isoformat(), today.isoformat()],
            "trajectory_cur": [cur_start.isoformat(), today.isoformat()],
            "trajectory_prior": [prior_start.isoformat(), prior_end.isoformat()],
        },
        "roster_total": total_clients,
        "size_rows": size_rows,
        "cohorts": {
            "established": len(established),
            "new_active": len(new_active),
            "dormant": len(dormant),
            "established_unrated": len(established) - len(eligible),
        },
        "tide_pct": round(tide, 1),
        "band_cuts": {"rising_at_or_above": rising_cut, "sliding_at_or_below": sliding_cut},
        "matrix": matrix,
        "cells": cells,
        "eligible_n": len(eligible),
        "cohort_lists": {
            "new_active": cohort_list(new_active),
            "dormant": cohort_list(dormant),
        },
    }
