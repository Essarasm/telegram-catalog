"""Merge duplicate active allowed_clients rows that share the same client_id_1c.

Background — 2026-05-14 audit found 30 active client_id_1c values appearing
across 92 allowed_clients rows. Pins set via /lokatsiya land on whichever
row search_clients() returns first, which is non-deterministic and often
the Master-xlsx-source row (source_sheet='Contacts') rather than the
1C-canonical (source_sheet='clients_upload'). Downstream readers using
client_id_1c-based lookups find the canonical row with no pin, producing
the "pin disappeared" perception even though the pin is sitting on a
sibling row of the same 1C entity.

Strategy — Approach E from 2026-05-14 analysis: 1C-canonical wins by
default, manual-flag conflicts.

For each duplicate cluster:
- Canonical = source_sheet='clients_upload' (1C daily import). If multiple
  upload rows in cluster, pick lowest id (oldest). If no upload row in
  cluster, flag as no-1C-source and skip.
- Soft fields (name, location, gps_*, ism_02/raqam_02/ism_03/raqam_03,
  viloyat/tuman/moljal/mijoz_holati/hajm/segment/eslatmalar, etc.) copied
  from non-canonical rows to canonical ONLY where canonical's value is
  NULL/empty (fill-only — never clobbers canonical's existing value).
- FK references in 21 tables (users.client_id, client_balances.client_id,
  real_orders.client_id, etc.) updated from non-canonical → canonical.
- Non-canonical rows soft-merged: status='merged_into:<canonical_id>'.
  Reversible by flipping status back to 'active' if needed.

Conflicts flagged (cluster skipped, printed for human decision):
- Both canonical and non-canonical have non-null gps_latitude AND the
  coords differ by more than 30m. Auto-merging would silently pick one
  pin and discard the other — human must decide which is correct.
- No source_sheet='clients_upload' row in cluster. Means the cluster is
  pure Master-xlsx duplicates with no 1C anchor — canonical choice is
  ambiguous, human must decide.

Phone slots (raqam_02 / raqam_03) treated as fill-only with overflow
detection: if both slots are taken on canonical and a non-canonical row
has another phone, set needs_review=1 instead of dropping it silently.

Each cluster's merge runs in a single transaction. The tool itself is
idempotent — re-running after a successful merge finds zero remaining
clusters for that 1C name.

Usage:
    python tools/merge_duplicate_1c_clients.py                  # dry-run (default)
    python tools/merge_duplicate_1c_clients.py --apply          # write
    python tools/merge_duplicate_1c_clients.py --cluster '<1c>' # single cluster
    python tools/merge_duplicate_1c_clients.py --db <path>      # alt DB
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from typing import Optional


# Soft fields copied fill-only from non-canonical → canonical.
# Identity / lifecycle / provenance columns are NOT in this list — they
# stay with their respective rows.
FILL_ONLY_COLUMNS = [
    # Display + free-text
    "name", "location", "notes", "company_name",
    # Master-owned soft fields (per memory feedback_master_fill_only)
    "segment", "hajm", "mijoz_holati", "eslatmalar",
    # NOTE: ism_02/ism_03 are deliberately NOT here — they are paired with
    # raqam_02/raqam_03 and owned solely by plan_phone_slots(), which keeps the
    # number and its contact-name together. Filling them independently here
    # decoupled name from number (Error Log #77).
    "viloyat", "tuman", "moljal",
    # Structured location
    "location_district_id", "location_moljal_id",
    # GPS (canonical pin info — most important for the pin-overwrite problem)
    "gps_latitude", "gps_longitude", "gps_address", "gps_region",
    "gps_district", "gps_set_at", "gps_set_by_tg_id", "gps_set_by_name",
    "gps_set_by_role",
    # Operational
    "matched_telegram_id", "credit_score", "credit_limit",
    "needs_verification",
    "last_master_synced_at",
]

# Phone slots — fill-only with overflow detection (raqam_02 first, then raqam_03).
PHONE_SLOT_COLUMNS = [("raqam_02", "ism_02"), ("raqam_03", "ism_03")]

# Columns never copied — identity, provenance, primary-source authoritative.
NEVER_COPY = {
    "id", "phone_normalized", "source_sheet", "client_id_1c", "status",
    "source_1c", "source_master", "master_row_id", "needs_review",
}

# Conflict threshold: pins within 30m of each other treated as same; further
# apart is a real disagreement requiring human decision.
GPS_CONFLICT_DISTANCE_KM = 0.030

# Tables with composite UNIQUE indexes (client_id, <other>). Naïve UPDATE of
# client_id collides with canonical's existing rows on the same <other> key.
# Pre-merge: delete non-canonical's rows that would collide, plus dedupe
# between sibling non-canonical rows. Both tables are recomputed nightly
# from financial activity that's about to be merged, so deletion is safe —
# next cron recompute fills the canonical row from the unified history.
COMPOSITE_UNIQUE_TABLES = [
    ("client_scores", "client_id", "recalc_date"),
    ("client_points_monthly", "client_id", "month"),
]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math
    R = 6371
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(d_lon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def find_fk_references(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Return [(table, column)] pairs where the column references
    allowed_clients.id by naming convention (client_id, linked_client_id)."""
    tables = [r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' AND name != 'allowed_clients' "
        "ORDER BY name"
    ).fetchall()]
    refs = []
    for t in tables:
        cols = conn.execute(f"PRAGMA table_info({t})").fetchall()
        for c in cols:
            if c["name"] in ("client_id", "linked_client_id"):
                refs.append((t, c["name"]))
    return refs


def pick_canonical(rows: list[sqlite3.Row]) -> Optional[sqlite3.Row]:
    """Choose the canonical row from a cluster. Prefer source_sheet=
    'clients_upload' (1C daily import), tie-break by lowest id."""
    upload_rows = [r for r in rows if r["source_sheet"] == "clients_upload"]
    if not upload_rows:
        return None  # No 1C anchor — flag for manual review
    return min(upload_rows, key=lambda r: r["id"])


def detect_gps_conflict(canonical: sqlite3.Row,
                        non_canon_rows: list[sqlite3.Row]) -> Optional[str]:
    """Return a conflict description string if canonical's pin disagrees
    with any non-canonical pin by more than GPS_CONFLICT_DISTANCE_KM."""
    if canonical["gps_latitude"] is None or canonical["gps_longitude"] is None:
        return None  # Canonical has no pin — fill-only copy is safe
    c_lat, c_lng = canonical["gps_latitude"], canonical["gps_longitude"]
    for r in non_canon_rows:
        if r["gps_latitude"] is None or r["gps_longitude"] is None:
            continue
        d = haversine_km(c_lat, c_lng, r["gps_latitude"], r["gps_longitude"])
        if d > GPS_CONFLICT_DISTANCE_KM:
            return (f"canonical id={canonical['id']} pin "
                    f"({c_lat}, {c_lng}) set by {canonical['gps_set_by_name']!r} "
                    f"at {canonical['gps_set_at']} VS non-canonical id={r['id']} "
                    f"pin ({r['gps_latitude']}, {r['gps_longitude']}) set by "
                    f"{r['gps_set_by_name']!r} at {r['gps_set_at']} "
                    f"— {d*1000:.0f}m apart, exceeds {GPS_CONFLICT_DISTANCE_KM*1000:.0f}m threshold")
    return None


def _norm_phone(v) -> str:
    """Whitespace-normalized phone string for dedupe comparison."""
    return str(v).strip() if v is not None else ""


def plan_phone_slots(canonical: sqlite3.Row,
                     non_canon_rows: list[sqlite3.Row]
                     ) -> tuple[dict, bool, list]:
    """Compute the canonical row's FINAL secondary-phone slots after merge.

    Collects every distinct phone across the canonical row's own secondary
    slots + all non-canonical rows (primary + secondaries), EXCLUDING the
    canonical primary (phone_normalized), preserving order (canonical's own
    secondaries first, then non-canonical by row order). The first two distinct
    numbers fill raqam_02 / raqam_03; >2 sets overflow.

    Returns (assignments, overflow, dropped):
    - assignments: {raqam_col: (phone, name)} for slots that should hold a value
    - overflow:    True if >2 distinct secondaries (excess dropped → needs_review)
    - dropped:     phones that didn't fit

    Slots NOT in `assignments` must be CLEARED (set NULL) by the caller — this
    is what removes a stale `raqam_02 == primary` duplicate left by older runs,
    and reclaims the slot for a genuinely distinct second number. Two prior
    defects this replaces (Error Log #77): (a) the old planner only filled
    *empty* slots, so a primary-dup blocked promotion forever (the
    `phone_moves_count: 0` symptom); (b) it could leave a number == primary in a
    slot.
    """
    primary = _norm_phone(canonical["phone_normalized"])
    seen = set()
    if primary:
        seen.add(primary)
    ordered: list[tuple[str, Optional[str]]] = []  # (phone, name)

    def consider(phone, name):
        p = _norm_phone(phone)
        if p and p not in seen:
            seen.add(p)
            ordered.append((p, (name or "").strip() or None))

    # Canonical's own secondaries first — preserve genuine ones, skip primary-dups.
    consider(canonical["raqam_02"], canonical["ism_02"])
    consider(canonical["raqam_03"], canonical["ism_03"])
    # Then every non-canonical phone, paired with its owning name field.
    for r in non_canon_rows:
        consider(r["phone_normalized"], r["name"])
        consider(r["raqam_02"], r["ism_02"])
        consider(r["raqam_03"], r["ism_03"])

    slot_cols = [c for c, _ in PHONE_SLOT_COLUMNS]  # ["raqam_02", "raqam_03"]
    assignments = {col: (ph, nm) for col, (ph, nm) in zip(slot_cols, ordered)}
    dropped = [p for p, _ in ordered[len(slot_cols):]]
    return assignments, bool(dropped), dropped


def plan_fill_only_copies(canonical: sqlite3.Row,
                          non_canon_rows: list[sqlite3.Row]) -> dict[str, tuple[str, int]]:
    """Plan fill-only column copies. Returns {column: (value, source_id)}
    for columns where canonical is NULL/empty and at least one non-canonical
    has a value. First non-canonical with a value wins."""
    plan = {}
    for col in FILL_ONLY_COLUMNS:
        cv = canonical[col]
        if cv is not None and cv != "":
            continue
        for r in non_canon_rows:
            v = r[col]
            if v is not None and v != "":
                plan[col] = (v, r["id"])
                break
    return plan


def merge_cluster(conn: sqlite3.Connection, cluster_1c: str,
                  rows: list[sqlite3.Row], fk_refs: list[tuple[str, str]],
                  apply: bool) -> dict:
    """Plan and optionally execute the merge of one duplicate cluster.
    Returns a result dict with status, details for reporting."""
    canonical = pick_canonical(rows)
    if canonical is None:
        return {
            "status": "flag_no_1c_source",
            "cluster_1c": cluster_1c,
            "rows": rows,
            "reason": "no source_sheet='clients_upload' row in cluster",
        }
    non_canon = [r for r in rows if r["id"] != canonical["id"]]

    gps_conflict = detect_gps_conflict(canonical, non_canon)
    if gps_conflict:
        return {
            "status": "flag_gps_conflict",
            "cluster_1c": cluster_1c,
            "rows": rows,
            "canonical_id": canonical["id"],
            "reason": gps_conflict,
        }

    # Plan the moves
    fill_plan = plan_fill_only_copies(canonical, non_canon)
    phone_slots, phone_overflow, phone_dropped = plan_phone_slots(canonical, non_canon)

    # Count FK rows that will be remapped per table
    non_canon_ids = [r["id"] for r in non_canon]
    placeholders = ",".join("?" * len(non_canon_ids))
    fk_updates = []
    total_fk_rows = 0
    for table, col in fk_refs:
        cnt = conn.execute(
            f"SELECT COUNT(*) AS n FROM {table} WHERE {col} IN ({placeholders})",
            non_canon_ids,
        ).fetchone()["n"]
        if cnt > 0:
            fk_updates.append((table, col, cnt))
            total_fk_rows += cnt

    if not apply:
        return {
            "status": "would_merge",
            "cluster_1c": cluster_1c,
            "rows": rows,
            "canonical_id": canonical["id"],
            "non_canon_ids": non_canon_ids,
            "fill_plan": fill_plan,
            "phone_slots": phone_slots,
            "phone_overflow": phone_overflow,
            "phone_dropped": phone_dropped,
            "fk_updates": fk_updates,
            "total_fk_rows": total_fk_rows,
        }

    # APPLY MODE — single transaction per cluster
    try:
        conn.execute("BEGIN")

        # 1. Fill-only column copies
        for col, (val, src_id) in fill_plan.items():
            conn.execute(
                f"UPDATE allowed_clients SET {col} = ? WHERE id = ?",
                (val, canonical["id"]),
            )

        # 2. Phone slots — write the FULL computed secondary state. Each slot is
        # either set to its assigned (number, name) pair or cleared to NULL.
        # Clearing is what removes a stale raqam_02==primary duplicate and frees
        # the slot for a genuinely distinct second number (Error Log #77).
        for raqam_col, _ism in PHONE_SLOT_COLUMNS:
            ism_col = raqam_col.replace("raqam_", "ism_")
            if raqam_col in phone_slots:
                phone, name = phone_slots[raqam_col]
                conn.execute(
                    f"UPDATE allowed_clients SET {raqam_col} = ?, {ism_col} = ? "
                    f"WHERE id = ?",
                    (phone, name, canonical["id"]),
                )
            else:
                conn.execute(
                    f"UPDATE allowed_clients SET {raqam_col} = NULL, "
                    f"{ism_col} = NULL WHERE id = ?",
                    (canonical["id"],),
                )

        # 3. needs_review flag if any phone overflowed
        if phone_overflow:
            conn.execute(
                "UPDATE allowed_clients SET needs_review = 1 WHERE id = ?",
                (canonical["id"],),
            )

        # 4a. Pre-FK-update dedupe for tables with composite UNIQUE constraints.
        # client_scores(client_id, recalc_date) + client_points_monthly(client_id, month):
        # both nightly-recomputed from financial activity, so deleting non-canonical
        # rows that would collide with canonical's existing rows (or with each other)
        # is safe — next cron fills the canonical row from the unified history.
        for table, client_col, other_col in COMPOSITE_UNIQUE_TABLES:
            # Step 1: delete non-canonical rows that collide with canonical's existing key
            conn.execute(
                f"DELETE FROM {table} WHERE {client_col} IN ({placeholders}) "
                f"AND {other_col} IN (SELECT {other_col} FROM {table} WHERE {client_col} = ?)",
                non_canon_ids + [canonical["id"]],
            )
            # Step 2: for keys only present in non-canonical, keep only one per key (lowest id)
            # by deleting all but the row with MIN(rowid) for each (non_canon_client_id, other_col).
            conn.execute(
                f"DELETE FROM {table} WHERE rowid NOT IN ("
                f"  SELECT MIN(rowid) FROM {table} WHERE {client_col} IN ({placeholders}) "
                f"  GROUP BY {other_col}"
                f") AND {client_col} IN ({placeholders})",
                non_canon_ids + non_canon_ids,
            )

        # 4b. FK reference updates across all 21 tables
        for table, col, _cnt in fk_updates:
            conn.execute(
                f"UPDATE {table} SET {col} = ? WHERE {col} IN ({placeholders})",
                [canonical["id"]] + non_canon_ids,
            )

        # 5. Soft-merge non-canonical rows
        merged_status = f"merged_into:{canonical['id']}"
        for r in non_canon:
            conn.execute(
                "UPDATE allowed_clients SET status = ? WHERE id = ?",
                (merged_status, r["id"]),
            )

        # 6. Audit row in admin_action_log
        import json as _json
        audit_args = _json.dumps({
            "cluster_1c": cluster_1c,
            "canonical_id": canonical["id"],
            "merged_ids": non_canon_ids,
            "fill_plan_columns": list(fill_plan.keys()),
            "phone_slot_count": len(phone_slots),
            "phone_overflow": phone_overflow,
            "phone_dropped": phone_dropped,
            "fk_update_count": sum(cnt for _, _, cnt in fk_updates),
        }, ensure_ascii=False)
        conn.execute(
            "INSERT INTO admin_action_log (telegram_id, user_name, command, args) "
            "VALUES (?, ?, ?, ?)",
            (0, "script:merge_duplicate_1c_clients.py",
             "merge_duplicate_1c_cluster", audit_args),
        )

        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        return {
            "status": "apply_failed",
            "cluster_1c": cluster_1c,
            "canonical_id": canonical["id"],
            "error": str(e),
        }

    return {
        "status": "merged",
        "cluster_1c": cluster_1c,
        "canonical_id": canonical["id"],
        "non_canon_ids": non_canon_ids,
        "fill_plan": fill_plan,
        "phone_slots": phone_slots,
        "phone_overflow": phone_overflow,
        "phone_dropped": phone_dropped,
        "fk_updates": fk_updates,
        "total_fk_rows": total_fk_rows,
    }


def format_row_summary(r: sqlite3.Row) -> str:
    pin = (f"pin=({r['gps_latitude']}, {r['gps_longitude']})"
           if r["gps_latitude"] else "no pin")
    return (f"id={r['id']:>6} name={r['name']!r:30s} "
            f"src={r['source_sheet']!r:20s} status={r['status']!r:10s} "
            f"phone={r['phone_normalized']!r:14s} {pin}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="Actually write (default: dry-run)")
    parser.add_argument("--cluster",
                        help="Only process this specific client_id_1c value (for testing)")
    parser.add_argument("--db",
                        default=os.environ.get("DATABASE_PATH", "/data/catalog.db"),
                        help="SQLite path (default: /data/catalog.db)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    fk_refs = find_fk_references(conn)
    print(f"FK landscape: {len(fk_refs)} (table, column) pairs reference allowed_clients.id")
    print(f"  {', '.join(f'{t}.{c}' for t, c in fk_refs[:8])}, ...")
    print()

    # Find clusters: active rows sharing a client_id_1c with 2+ siblings
    where_cluster = ""
    params = []
    if args.cluster:
        where_cluster = "AND client_id_1c = ?"
        params = [args.cluster]

    clusters = conn.execute(f"""
        SELECT client_id_1c
        FROM allowed_clients
        WHERE COALESCE(status, 'active') = 'active'
          AND client_id_1c IS NOT NULL AND client_id_1c != ''
          {where_cluster}
        GROUP BY client_id_1c
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC, client_id_1c
    """, params).fetchall()

    print(f"Mode: {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"DB: {args.db}")
    print(f"Clusters found: {len(clusters)}")
    print("=" * 80)
    print()

    # Safety gates (Error Log #75): client_id_1c is a non-unique NAME, so a
    # same-name cluster is only a real duplicate when its rows SHARE A PHONE.
    # Different-phone clusters are legitimate "two shops, same name" and must
    # NOT be merged. Confirmed-distinct names are hard-excluded; human-confirmed
    # same-shop names (e.g. Фуркат, which has no shared phone) bypass the phone
    # gate. Mirrors backend/services/consistency_audit.fuzzy_client_1c_dups.
    from backend.services.client_identity_reviewed import (
        CONFIRMED_DISTINCT_SHARED_NAMES,
        CONFIRMED_SAME_SHOP,
        normalize_1c,
    )

    def _one_phone_component(rows):
        # True iff every row links into ONE connected component via a shared
        # phone (union-find). Merging the whole cluster is only safe when it's
        # a single entity — a cluster with a phone-isolated row (e.g. a
        # separately-registered shop that happens to share the name) or two
        # disjoint phone groups must NOT be collapsed into one canonical.
        parent = {r["id"]: r["id"] for r in rows}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        phone_row = {}
        for r in rows:
            for col in ("phone_normalized", "raqam_02", "raqam_03"):
                ph = (str(r[col]).strip() if r[col] is not None else "")
                if not ph:
                    continue
                if ph in phone_row:
                    parent[find(r["id"])] = find(phone_row[ph])
                else:
                    phone_row[ph] = r["id"]
        return len({find(r["id"]) for r in rows}) == 1

    summary = {"merged": 0, "would_merge": 0,
               "flag_gps_conflict": 0, "flag_no_1c_source": 0,
               "apply_failed": 0,
               "skip_confirmed_distinct": 0, "skip_not_one_entity": 0}
    flagged_details = []

    for c in clusters:
        cid_1c = c["client_id_1c"]
        rows = conn.execute("""
            SELECT * FROM allowed_clients
            WHERE client_id_1c = ?
              AND COALESCE(status, 'active') = 'active'
            ORDER BY id
        """, (cid_1c,)).fetchall()

        # --- genuine-duplicate gates ---
        norm = normalize_1c(cid_1c)
        if norm in CONFIRMED_DISTINCT_SHARED_NAMES:
            summary["skip_confirmed_distinct"] += 1
            continue
        if norm not in CONFIRMED_SAME_SHOP and not _one_phone_component(rows):
            summary["skip_not_one_entity"] += 1
            continue

        result = merge_cluster(conn, cid_1c, rows, fk_refs, args.apply)
        summary[result["status"]] = summary.get(result["status"], 0) + 1

        print(f"--- cluster: client_id_1c={cid_1c!r}  ({len(rows)} rows) ---")
        for r in rows:
            marker = "  *" if r["id"] == result.get("canonical_id") else "   "
            print(f"{marker} {format_row_summary(r)}")

        if result["status"] == "would_merge":
            print(f"   → would merge {len(result['non_canon_ids'])} non-canonical "
                  f"into canonical id={result['canonical_id']}")
            if result["fill_plan"]:
                print(f"   → fill-only copies: "
                      f"{', '.join(f'{k}<-id{v[1]}' for k,v in result['fill_plan'].items())}")
            if result["phone_slots"]:
                print(f"   → phone slots: "
                      f"{', '.join(f'{c}={ph}' for c,(ph,_) in result['phone_slots'].items())}")
            if result["phone_overflow"]:
                print(f"   → phone OVERFLOW (dropped {len(result['phone_dropped'])}): "
                      f"needs_review will be set on canonical")
            if result["fk_updates"]:
                fk_summary = ", ".join(f"{t}.{c}={n}" for t, c, n in result["fk_updates"])
                print(f"   → FK updates: {fk_summary} (total {result['total_fk_rows']} rows)")
        elif result["status"] == "merged":
            print(f"   ✓ merged into canonical id={result['canonical_id']}; "
                  f"{result['total_fk_rows']} FK rows updated")
        elif result["status"] == "flag_gps_conflict":
            print(f"   ⚠ GPS CONFLICT — SKIPPED")
            print(f"   reason: {result['reason']}")
            flagged_details.append(result)
        elif result["status"] == "flag_no_1c_source":
            print(f"   ⚠ NO 1C SOURCE — SKIPPED")
            print(f"   reason: {result['reason']}")
            flagged_details.append(result)
        elif result["status"] == "apply_failed":
            print(f"   ✗ APPLY FAILED: {result['error']}")

        print()

    print("=" * 80)
    print("SUMMARY")
    for status, count in summary.items():
        if count > 0:
            print(f"  {status}: {count}")

    if flagged_details:
        print()
        print(f"⚠ {len(flagged_details)} cluster(s) need manual review:")
        for f in flagged_details:
            print(f"  - {f['cluster_1c']!r}: {f['reason'][:120]}")

    if not args.apply and summary.get("would_merge", 0) > 0:
        print()
        print(f"Dry-run only — re-run with --apply to merge "
              f"{summary['would_merge']} cluster(s).")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
