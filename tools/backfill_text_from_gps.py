#!/usr/bin/env python3
"""One-off Layer 2 backfill — fill viloyat/tuman from gps_region/gps_district
for clients whose GPS pin was set before Layer 2 shipped.

After today's deploy, every NEW agent pin auto-backfills text columns. This
script handles the historical inventory of GPS-set clients (#679, #1302, etc.)
that had their pins set under the old write path.

Fill-only: never overwrites existing text values. Same semantics as the live
backfill_text_from_gps helper.

Usage:
    python tools/backfill_text_from_gps.py                  # dry-run (default)
    python tools/backfill_text_from_gps.py --apply          # write
    python tools/backfill_text_from_gps.py --db <path>      # alt DB
"""
import argparse
import os
import sqlite3
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="Apply changes (default is dry-run)")
    parser.add_argument("--db", default=os.getenv("DATABASE_PATH", "/data/catalog.db"))
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== Layer 2 text-from-GPS backfill ({mode}) ===")
    print(f"DB: {args.db}")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    # Candidates: any row where GPS reverse-geocode has data AND at least
    # one text column is missing. Status filter excludes tombstones.
    candidates = conn.execute("""
        SELECT id, name, client_id_1c,
               viloyat, tuman,
               gps_region, gps_district, gps_address,
               gps_set_by_name, gps_set_at
          FROM allowed_clients
         WHERE COALESCE(status, 'active') NOT LIKE 'merged%'
           AND (gps_region IS NOT NULL OR gps_district IS NOT NULL)
           AND (
                (viloyat IS NULL OR viloyat = '')
             OR (tuman IS NULL OR tuman = '')
           )
         ORDER BY id
    """).fetchall()

    print(f"\nCandidates: {len(candidates)} rows with GPS but missing text")
    if not candidates:
        print("Nothing to do.")
        return

    filled_v = 0
    filled_t = 0
    changed_rows = 0
    skipped_no_fill = 0

    for r in candidates:
        d = dict(r)
        new_v = d["viloyat"] if (d["viloyat"] and d["viloyat"].strip()) else d["gps_region"]
        new_t = d["tuman"]   if (d["tuman"]   and d["tuman"].strip())   else d["gps_district"]

        v_changed = (new_v != d["viloyat"]) and new_v is not None
        t_changed = (new_t != d["tuman"])   and new_t is not None

        if not (v_changed or t_changed):
            skipped_no_fill += 1
            continue

        changed_rows += 1
        if v_changed:
            filled_v += 1
        if t_changed:
            filled_t += 1

        name = d.get("client_id_1c") or d.get("name") or f"#{d['id']}"
        marker = "WOULD UPDATE" if not args.apply else "UPDATING"
        print(f"  {marker} #{d['id']:>5} {name[:35]:<35} "
              f"viloyat: {str(d['viloyat']):<15} -> {str(new_v):<15} | "
              f"tuman: {str(d['tuman']):<15} -> {str(new_t):<15}")

        if args.apply:
            conn.execute(
                "UPDATE allowed_clients SET "
                "viloyat = COALESCE(NULLIF(viloyat, ''), ?), "
                "tuman = COALESCE(NULLIF(tuman, ''), ?) "
                "WHERE id = ?",
                (d["gps_region"], d["gps_district"], d["id"]),
            )

    if args.apply:
        conn.commit()

    print(f"\n--- Summary ({mode}) ---")
    print(f"  Candidates examined:    {len(candidates)}")
    print(f"  Rows changed:           {changed_rows}")
    print(f"  Skipped (no fill):      {skipped_no_fill}")
    print(f"  viloyat backfills:      {filled_v}")
    print(f"  tuman backfills:        {filled_t}")
    if not args.apply:
        print(f"\n  (dry-run — re-run with --apply to write)")

    conn.close()


if __name__ == "__main__":
    main()
