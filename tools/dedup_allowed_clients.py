"""One-shot dedup migration for the 20× allowed_clients explosion.

Scope (May 2026):
  - 40,940 total rows; 2,148 distinct phones; 18 historical re-imports of
    the master xlsx Contacts+Usto sheets created 38,741 phantom copies.
  - Each phone group has 1 lowest-id "rich" survivor + 19 hollow copies.
  - 46 phones have matched_telegram_id on a non-survivor row → naive delete
    would log out 46 users.
  - 107 phones have GPS pins on a non-survivor row → naive delete would
    erase 107 location records.

Strategy:
  1. Pre-flight DB backup to /data/catalog.db.pre_dedup_YYYYMMDD_HHMMSS.bak
  2. Acquire write lock (BEGIN IMMEDIATE) so no concurrent insert sneaks in
     between survivor-map build and the delete sweep.
  3. Build {survivor_id → [loser_ids]} per phone_normalized (non-empty).
  4. For each phone group, COALESCE-merge soft fields from siblings into
     the survivor (preserving telegram links, GPS pins, master-sync state,
     and every editable field the bot/agent ever wrote).
  5. Rewire FKs: users, real_orders, client_balances, client_payments,
     client_debts, orders → loser_id becomes survivor_id.
  6. Soft-delete losers: status='merged'. Reversible.
  7. Add UNIQUE partial index on phone_normalized for active+non-empty rows
     so future re-imports that try to dup will fail loudly.
  8. Run heal_all_finance_tables() to re-resolve any orphan rows whose
     client_name_1c now matches a clean survivor's client_id_1c.
  9. Verification queries — fail visibly if state isn't what we expect.

Excluded from this migration (separate concern, smaller blast radius):
  - 51 empty-phone rows (agent_panel + client_master:contacts + bot_from_1c)
    — they hold 1,241 FK rows and aren't duplicates.
  - 109 single-phone rows (recent additions in the partial 20th id-bucket)
    — already unique.
  - The few bucket-0 fuzzy client_id_1c manual-entry duplicates (n=3-4 each)
    — separate manual-entry drift class, not the bulk pattern.

Usage:
    railway ssh python3 - --dry-run < tools/dedup_allowed_clients.py   # preview
    railway ssh python3 -            < tools/dedup_allowed_clients.py   # execute

Idempotent: re-running after success is a no-op (no phone group has >1 row).
"""
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime

# Soft fields merged from non-survivor siblings into the survivor when the
# survivor's value is NULL/empty. Order does not matter; each is COALESCEd
# independently. `phone_normalized` and `id` are NOT in this list — they
# define the group. `status` and `source_sheet` are also not merged — the
# survivor keeps its own (status='active' baseline; source_sheet identifies
# the canonical row's import path).
MERGE_FIELDS = [
    "name", "client_id_1c", "company_name",
    "location", "viloyat", "tuman", "moljal",
    "ism_02", "raqam_02", "ism_03", "raqam_03",
    "gps_latitude", "gps_longitude", "gps_address",
    "gps_region", "gps_district",
    "gps_set_at", "gps_set_by_tg_id", "gps_set_by_name", "gps_set_by_role",
    "matched_telegram_id",
    "credit_score", "credit_limit", "notes",
    "source_master", "master_row_id", "last_master_synced_at",
    "needs_review", "needs_verification",
    "segment", "hajm", "mijoz_holati", "eslatmalar",
    "location_district_id", "location_moljal_id",
    "source_1c",
]

# FK tables to rewire. Each table has a `client_id` column referring to
# allowed_clients.id. Discovered via Round 1 audit; if new tables get added
# later, extend this list.
FK_TABLES = [
    "users",
    "real_orders",
    "client_balances",
    "client_payments",
    "client_debts",
    "orders",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Print what would change; do not write.")
    ap.add_argument("--db", default=os.environ.get("DATABASE_PATH", "/data/catalog.db"))
    args = ap.parse_args()

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"FATAL: db not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    print(f"\n=== dedup_allowed_clients.py ===")
    print(f"DB:      {db_path}")
    print(f"Mode:    {'DRY-RUN (no writes)' if args.dry_run else 'EXECUTE'}")
    print(f"Started: {datetime.now().isoformat(timespec='seconds')}")

    # 1. Pre-flight backup (skip in dry-run)
    if not args.dry_run:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{db_path}.pre_dedup_{ts}.bak"
        print(f"\n[1] Backing up DB → {backup_path}")
        shutil.copy2(db_path, backup_path)
        size_mb = os.path.getsize(backup_path) / 1024 / 1024
        print(f"    backup size: {size_mb:.1f} MB")
    else:
        print("\n[1] Backup skipped (dry-run)")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    # 2. Acquire write lock immediately so concurrent writers can't insert
    # new rows that would invalidate the survivor map mid-flight. Readers
    # continue normally (WAL).
    if not args.dry_run:
        print("\n[2] Acquiring BEGIN IMMEDIATE write lock")
        conn.execute("BEGIN IMMEDIATE")
    else:
        print("\n[2] Lock skipped (dry-run uses default deferred read txn)")

    # 3. Pre-flight counts (so we can verify post-state)
    pre = conn.execute(
        """SELECT COUNT(*) AS total,
                  SUM(CASE WHEN COALESCE(status,'active')='active' THEN 1 ELSE 0 END) AS active,
                  SUM(CASE WHEN status='merged' THEN 1 ELSE 0 END) AS merged
             FROM allowed_clients"""
    ).fetchone()
    print(f"\n[3] Pre-flight counts: total={pre['total']}, "
          f"active={pre['active']}, merged={pre['merged']}")

    # 4. Build survivor map: for each phone with >1 active row, MIN(id) wins.
    print("\n[4] Building survivor map (phone_normalized → min(id))")
    survivor_rows = conn.execute(
        """SELECT phone_normalized,
                  MIN(id) AS survivor_id,
                  COUNT(*) AS n_rows,
                  GROUP_CONCAT(id) AS all_ids
             FROM allowed_clients
            WHERE COALESCE(status,'active')='active'
              AND phone_normalized IS NOT NULL
              AND phone_normalized != ''
            GROUP BY phone_normalized
           HAVING COUNT(*) > 1"""
    ).fetchall()

    n_groups = len(survivor_rows)
    n_losers = sum(r["n_rows"] - 1 for r in survivor_rows)
    print(f"    {n_groups} phone groups with >1 active row; "
          f"{n_losers} loser rows to merge+delete")

    if n_groups == 0:
        print("\nNo duplicates to dedup. Already clean. Exiting.")
        if not args.dry_run:
            conn.execute("ROLLBACK")
        conn.close()
        return

    # Build {survivor_id: [loser_ids]} for the rewire/merge passes.
    group_plan: dict[int, list[int]] = {}
    for r in survivor_rows:
        ids = sorted(int(x) for x in r["all_ids"].split(","))
        survivor_id = ids[0]
        loser_ids = ids[1:]
        group_plan[survivor_id] = loser_ids

    # 5. Field-merge — for each survivor, pull non-NULL/non-empty values
    # from siblings into any field where survivor is currently NULL/empty.
    # We execute one UPDATE per survivor with N COALESCE expressions, where
    # each COALESCE reads from the loser-id with the freshest non-NULL value
    # for that field (ORDER BY id ASC = oldest first; matches "rich first
    # row" pattern from the Round-2 diagnostic).
    print(f"\n[5] Field-merge phase ({len(MERGE_FIELDS)} fields × "
          f"{n_groups} groups)")
    if args.dry_run:
        # Aggregate counter — for each field, how many groups would have
        # a survivor.field=NULL/'' AND ≥1 sibling.field non-NULL? These are
        # the merges that would fire. Lets us cross-check against the
        # Round-2 diagnostic numbers (e.g. matched_telegram_id=46, gps=107)
        # before committing to execute.
        print(f"    Aggregate per-field merge counts:")
        for f in MERGE_FIELDS:
            n = conn.execute(f"""
                SELECT COUNT(DISTINCT g.phone_normalized) FROM (
                    SELECT phone_normalized, MIN(id) AS survivor_id
                      FROM allowed_clients
                     WHERE COALESCE(status,'active')='active' AND phone_normalized != ''
                     GROUP BY phone_normalized HAVING COUNT(*) > 1
                ) g
                JOIN allowed_clients surv ON surv.id = g.survivor_id
                JOIN allowed_clients sib ON sib.phone_normalized = g.phone_normalized
                    AND sib.id != g.survivor_id
                    AND COALESCE(sib.status,'active')='active'
                WHERE (surv.{f} IS NULL OR surv.{f} = '')
                  AND sib.{f} IS NOT NULL AND sib.{f} != ''
            """).fetchone()[0]
            if n:
                print(f"      {f}: {n} groups would merge")

        # Sample 3 groups to show what merges would happen.
        print(f"\n    Sample of 3 groups (raw):")
        sample = list(group_plan.items())[:3]
        for survivor_id, loser_ids in sample:
            survivor = conn.execute(
                "SELECT * FROM allowed_clients WHERE id = ?", (survivor_id,)
            ).fetchone()
            print(f"\n    Group survivor id={survivor_id} losers={loser_ids[:3]}{'...' if len(loser_ids)>3 else ''}")
            for f in MERGE_FIELDS:
                if survivor[f] in (None, ""):
                    sibling_val = conn.execute(
                        f"SELECT {f} FROM allowed_clients WHERE id IN "
                        f"({','.join('?'*len(loser_ids))}) "
                        f"  AND {f} IS NOT NULL AND {f} != '' "
                        f"ORDER BY id LIMIT 1",
                        loser_ids,
                    ).fetchone()
                    if sibling_val and sibling_val[0] not in (None, ""):
                        print(f"      will copy {f} = {sibling_val[0]!r:.60} "
                              f"(survivor was {survivor[f]!r})")
    else:
        merged_field_count = 0
        for i, (survivor_id, loser_ids) in enumerate(group_plan.items()):
            if i % 200 == 0:
                print(f"    progress: {i}/{n_groups}")
            survivor = conn.execute(
                "SELECT * FROM allowed_clients WHERE id = ?", (survivor_id,)
            ).fetchone()
            updates = []
            params = []
            for f in MERGE_FIELDS:
                # Skip if survivor already has a non-empty value (preserves
                # any operator edits that landed on the survivor row).
                if survivor[f] not in (None, ""):
                    continue
                # Pull the first non-NULL sibling value (lowest id wins).
                placeholders = ",".join("?" * len(loser_ids))
                sib = conn.execute(
                    f"SELECT {f} FROM allowed_clients WHERE id IN ({placeholders}) "
                    f"  AND {f} IS NOT NULL AND {f} != '' "
                    f"ORDER BY id LIMIT 1",
                    loser_ids,
                ).fetchone()
                if sib and sib[0] not in (None, ""):
                    updates.append(f"{f} = ?")
                    params.append(sib[0])
                    merged_field_count += 1
            if updates:
                params.append(survivor_id)
                conn.execute(
                    f"UPDATE allowed_clients SET {', '.join(updates)} WHERE id = ?",
                    params,
                )
        print(f"    field-merges executed: {merged_field_count}")

    # 6. FK rewire — UPDATE every dependent table to point at survivor.
    print(f"\n[6] FK rewire across {len(FK_TABLES)} tables")
    rewire_total = 0
    for tbl in FK_TABLES:
        # Build a single UPDATE per table using a CASE expression keyed on
        # current client_id. For 1,978 groups × handful of losers each, the
        # CASE list is large — chunk to avoid SQLite query-length limits.
        # Simpler: one UPDATE per group. ~1,978 statements, fast on indexed
        # client_id columns.
        rewired_in_tbl = 0
        for survivor_id, loser_ids in group_plan.items():
            if not loser_ids:
                continue
            placeholders = ",".join("?" * len(loser_ids))
            if args.dry_run:
                row = conn.execute(
                    f"SELECT COUNT(*) AS n FROM {tbl} WHERE client_id IN ({placeholders})",
                    loser_ids,
                ).fetchone()
                rewired_in_tbl += row["n"]
            else:
                cur = conn.execute(
                    f"UPDATE {tbl} SET client_id = ? WHERE client_id IN ({placeholders})",
                    [survivor_id] + loser_ids,
                )
                rewired_in_tbl += cur.rowcount
        print(f"    {tbl}: {'would rewire' if args.dry_run else 'rewired'} "
              f"{rewired_in_tbl} rows")
        rewire_total += rewired_in_tbl
    print(f"    total: {rewire_total} FK rows {'planned' if args.dry_run else 'rewired'}")

    # 7. Soft-delete losers
    print(f"\n[7] Soft-delete {n_losers} loser rows (status='merged')")
    if not args.dry_run:
        all_loser_ids = [lid for losers in group_plan.values() for lid in losers]
        # Chunk the IN-clause to stay under SQLite's parameter limit (~999).
        CHUNK = 500
        deleted_total = 0
        for i in range(0, len(all_loser_ids), CHUNK):
            chunk = all_loser_ids[i:i+CHUNK]
            placeholders = ",".join("?" * len(chunk))
            cur = conn.execute(
                f"UPDATE allowed_clients SET status = 'merged' "
                f"WHERE id IN ({placeholders})",
                chunk,
            )
            deleted_total += cur.rowcount
        print(f"    soft-deleted: {deleted_total}")
    else:
        print(f"    would soft-delete: {n_losers}")

    # 8. Add UNIQUE partial index — locks the door so future re-imports
    # that try to dup will fail with sqlite3.IntegrityError instead of
    # silently piling on. WHERE clause excludes empty-phone rows (the 51
    # phoneless allowed_clients rows that aren't duplicates).
    print(f"\n[8] Add UNIQUE partial index on phone_normalized")
    if not args.dry_run:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_allowed_phone_unique "
            "ON allowed_clients(phone_normalized) "
            "WHERE phone_normalized IS NOT NULL "
            "  AND phone_normalized != '' "
            "  AND COALESCE(status,'active') = 'active'"
        )
        print("    created idx_allowed_phone_unique")
    else:
        print("    would CREATE UNIQUE INDEX idx_allowed_phone_unique")

    # 9. Heal orphan FKs that previously couldn't resolve because of the
    # duplication confusion. ORPHAN_ON_IMPORT pattern — every importer
    # writing client_name_1c rows must resolve client_id; the daily heal
    # cron normally covers this, run it explicitly here so post-migration
    # state is clean before the next nightly audit.
    print(f"\n[9] Heal orphan FKs (heal_all_finance_tables)")
    if not args.dry_run:
        try:
            sys.path.insert(0, "/app")
            from backend.services import client_identity
            healed = client_identity.heal_all_finance_tables(conn)
            print(f"    heal results: {dict(healed)}")
        except Exception as e:
            print(f"    heal skipped (non-fatal): {e}")
    else:
        print("    would call client_identity.heal_all_finance_tables()")

    # 10. Commit (or rollback in dry-run)
    if args.dry_run:
        print("\n[10] DRY-RUN: rolling back (no writes committed)")
        conn.execute("ROLLBACK") if conn.in_transaction else None
    else:
        print("\n[10] Committing transaction")
        conn.commit()

    # 11. Post-flight verification — assertions that must hold
    print("\n[11] Post-flight verification")
    post = conn.execute(
        """SELECT COUNT(*) AS total,
                  SUM(CASE WHEN COALESCE(status,'active')='active' THEN 1 ELSE 0 END) AS active,
                  SUM(CASE WHEN status='merged' THEN 1 ELSE 0 END) AS merged
             FROM allowed_clients"""
    ).fetchone()
    print(f"    counts: total={post['total']}, active={post['active']}, merged={post['merged']}")

    # No active phone should have >1 row after dedup
    remaining_dups = conn.execute(
        """SELECT COUNT(*) AS n FROM (
             SELECT phone_normalized
               FROM allowed_clients
              WHERE COALESCE(status,'active')='active'
                AND phone_normalized != ''
              GROUP BY phone_normalized
             HAVING COUNT(*) > 1
           )"""
    ).fetchone()["n"]
    print(f"    remaining phone duplicates: {remaining_dups} "
          f"(expected: 0 in execute mode, {n_groups} in dry-run)")

    if not args.dry_run:
        assert remaining_dups == 0, \
            f"FATAL: {remaining_dups} phone duplicates remain after dedup"
        # FK rewire safety: no FK row should reference a merged client_id
        for tbl in FK_TABLES:
            stranded = conn.execute(
                f"SELECT COUNT(*) AS n FROM {tbl} t "
                f"JOIN allowed_clients ac ON ac.id = t.client_id "
                f"WHERE ac.status = 'merged'"
            ).fetchone()["n"]
            print(f"    {tbl}: {stranded} FK rows still pointing at merged "
                  f"(expected: 0)")
            assert stranded == 0, \
                f"FATAL: {tbl} has {stranded} stranded FK rows"

    print(f"\nDone: {datetime.now().isoformat(timespec='seconds')}")
    conn.close()


if __name__ == "__main__":
    main()
