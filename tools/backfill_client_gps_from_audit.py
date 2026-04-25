"""One-shot backfill: rebuild allowed_clients.gps_* from location_attempts.

Context — Apr 2026 column-overload bug. Before the gps_* split, the bot
location handler wrote canonical "lat,lng|addr" to allowed_clients.location.
The startup `import_clients` (and v1 master importer) then overwrote that
column with free-text from clients_data.csv on every deploy, destroying the
agent-set GPS. The zero-data-loss audit table `location_attempts` preserved
the lat/lng + reverse_geocode_json for every successful tap, so we rebuild
gps_* from there — one row per linked_client_id, taking the most recent
successful tap.

Idempotent. Safe to re-run. Only writes rows where gps_latitude IS NULL or
where the audit row is newer than gps_set_at.

Usage:
    python tools/backfill_client_gps_from_audit.py            # dry-run
    python tools/backfill_client_gps_from_audit.py --apply    # write
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="Write changes (default: dry-run, prints only)")
    parser.add_argument("--db", default=os.environ.get("DATABASE_PATH", "/data/catalog.db"))
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """SELECT la.linked_client_id, la.linked_client_1c,
                  la.telegram_id, la.first_name, la.is_agent,
                  la.latitude, la.longitude, la.reverse_geocode_json,
                  la.received_at
             FROM location_attempts la
            WHERE la.processed_ok = 1
              AND la.linked_client_id IS NOT NULL
              AND la.id IN (
                    SELECT MAX(id) FROM location_attempts
                     WHERE processed_ok = 1 AND linked_client_id IS NOT NULL
                  GROUP BY linked_client_id
              )
         ORDER BY la.linked_client_id"""
    ).fetchall()

    if not rows:
        print("No successful location_attempts rows found — nothing to backfill.")
        return 0

    written = 0
    skipped_already_fresh = 0
    for r in rows:
        client_id = r["linked_client_id"]
        ac = conn.execute(
            "SELECT gps_latitude, gps_set_at FROM allowed_clients WHERE id = ?",
            (client_id,),
        ).fetchone()
        if ac is None:
            print(f"  [skip] client_id={client_id} not in allowed_clients (deleted?)")
            continue

        if ac["gps_latitude"] is not None and ac["gps_set_at"] is not None:
            if ac["gps_set_at"] >= r["received_at"]:
                skipped_already_fresh += 1
                continue

        geo = {}
        if r["reverse_geocode_json"]:
            try:
                geo = json.loads(r["reverse_geocode_json"]) or {}
            except json.JSONDecodeError:
                geo = {}

        addr = geo.get("address") or ""
        region = geo.get("region") or ""
        district = geo.get("district") or ""
        setter_role = "agent" if r["is_agent"] else "client"

        action = "WRITE" if args.apply else "DRY-RUN"
        print(f"  [{action}] client_id={client_id:5d} ({(r['linked_client_1c'] or '')[:30]:30s}) "
              f"({r['latitude']:.6f},{r['longitude']:.6f}) addr={addr!r} "
              f"by={r['first_name']!r} role={setter_role} at={r['received_at']}")

        if args.apply:
            conn.execute(
                "UPDATE allowed_clients SET "
                "gps_latitude = ?, gps_longitude = ?, gps_address = ?, "
                "gps_region = ?, gps_district = ?, gps_set_at = ?, "
                "gps_set_by_tg_id = ?, gps_set_by_name = ?, gps_set_by_role = ? "
                "WHERE id = ?",
                (r["latitude"], r["longitude"], addr, region, district,
                 r["received_at"], r["telegram_id"], r["first_name"],
                 setter_role, client_id),
            )
            written += 1

    if args.apply:
        conn.commit()

    print()
    print(f"Summary: {written} written, {skipped_already_fresh} already-fresh "
          f"(gps_set_at >= audit), out of {len(rows)} candidate clients.")
    if not args.apply:
        print("Dry-run only — re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
