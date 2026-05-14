"""One-off: restore БАХРОМ СПИТАМЕН-ШОХ (allowed_clients.id=283) GPS pin
from location_attempts row 147 — Дилшод Каххоров's original 2026-05-08 share.

Context — 2026-05-14 pre-deploy-gap incident:
- 2026-05-08 10:01:43, Дилшод (tg 466241432) shared БАХРОМ's location in the
  driver group. allowed_clients.id=283 was pinned at 39.654565, 66.879446.
  location_attempts row 147 captured the raw share.
- 2026-05-14 13:18, Умиджон's location share overwrote that pin. The
  auto_overwrite_snapshot code in bot/handlers/location.py was NOT yet
  deployed at 13:18 (deploy a1923f21 landed later that afternoon), so the
  restore-pin endpoint can't recover this case — its admin_action_log read
  returns no snapshot for client 283.
- Дилшод's original lat/lng/address/region/district survives in
  location_attempts row 147 (the immutable insert-first audit table). This
  script reads it and writes it back atomically, with a
  restore_from_location_attempts audit row so the round-trip is forensically
  visible.

If a similar pre-deploy-gap case appears in the future, this script is the
template for the deferred restore-from-audit-row UI variant. The trigger to
build that UI is the second occurrence — first occurrence is fixed by hand.

Usage:
    python tools/restore_bakhrom_pin.py            # dry-run, prints plan
    python tools/restore_bakhrom_pin.py --apply    # writes
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys


CLIENT_ID = 283
SOURCE_ATTEMPT_ID = 147

EXPECTED_TELEGRAM_ID = 466241432
EXPECTED_LAT = 39.654565
EXPECTED_LNG = 66.879446
RESTORED_ROLE = "agent"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Actually write (default: dry-run)")
    parser.add_argument("--db", default=os.environ.get("DATABASE_PATH", "/data/catalog.db"),
                        help="SQLite path (default: /data/catalog.db)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    attempt = conn.execute(
        "SELECT id, received_at, telegram_id, first_name, latitude, longitude, "
        "linked_client_id, reverse_geocode_json, processed_ok "
        "FROM location_attempts WHERE id = ?",
        (SOURCE_ATTEMPT_ID,),
    ).fetchone()
    if attempt is None:
        print(f"ABORT: location_attempts row id={SOURCE_ATTEMPT_ID} not found.")
        return 2
    if int(attempt["linked_client_id"] or 0) != CLIENT_ID:
        print(f"ABORT: row {SOURCE_ATTEMPT_ID} linked_client_id="
              f"{attempt['linked_client_id']}, expected {CLIENT_ID}.")
        return 2
    if int(attempt["telegram_id"] or 0) != EXPECTED_TELEGRAM_ID:
        print(f"ABORT: row {SOURCE_ATTEMPT_ID} telegram_id={attempt['telegram_id']}, "
              f"expected {EXPECTED_TELEGRAM_ID} (Дилшод Каххоров).")
        return 2
    if abs(float(attempt["latitude"]) - EXPECTED_LAT) > 1e-6 or \
       abs(float(attempt["longitude"]) - EXPECTED_LNG) > 1e-6:
        print(f"ABORT: row {SOURCE_ATTEMPT_ID} lat/lng "
              f"({attempt['latitude']}, {attempt['longitude']}) drifted from "
              f"expected ({EXPECTED_LAT}, {EXPECTED_LNG}).")
        return 2

    geo = {}
    if attempt["reverse_geocode_json"]:
        try:
            geo = json.loads(attempt["reverse_geocode_json"]) or {}
        except json.JSONDecodeError:
            geo = {}

    current = conn.execute(
        "SELECT id, name, client_id_1c, gps_latitude, gps_longitude, gps_address, "
        "gps_region, gps_district, gps_set_at, gps_set_by_tg_id, gps_set_by_name, "
        "gps_set_by_role FROM allowed_clients WHERE id = ?",
        (CLIENT_ID,),
    ).fetchone()
    if current is None:
        print(f"ABORT: allowed_clients.id={CLIENT_ID} not found.")
        return 2

    new_values = {
        "gps_latitude": float(attempt["latitude"]),
        "gps_longitude": float(attempt["longitude"]),
        "gps_address": geo.get("address"),
        "gps_region": geo.get("region"),
        "gps_district": geo.get("district"),
        "gps_set_at": attempt["received_at"],
        "gps_set_by_tg_id": int(attempt["telegram_id"]),
        "gps_set_by_name": attempt["first_name"],
        "gps_set_by_role": RESTORED_ROLE,
    }

    audit_args = {
        "client_id": CLIENT_ID,
        "client_name": current["name"],
        "client_id_1c": current["client_id_1c"],
        "source_attempt_id": SOURCE_ATTEMPT_ID,
        "source_received_at": attempt["received_at"],
        "restored_lat": new_values["gps_latitude"],
        "restored_lng": new_values["gps_longitude"],
        "restored_address": new_values["gps_address"],
        "restored_region": new_values["gps_region"],
        "restored_district": new_values["gps_district"],
        "restored_set_by_tg_id": new_values["gps_set_by_tg_id"],
        "restored_set_by_name": new_values["gps_set_by_name"],
        "restored_set_by_role": new_values["gps_set_by_role"],
        "overwritten_lat": current["gps_latitude"],
        "overwritten_lng": current["gps_longitude"],
        "overwritten_address": current["gps_address"],
        "overwritten_set_at": current["gps_set_at"],
        "overwritten_set_by_tg_id": current["gps_set_by_tg_id"],
        "overwritten_set_by_name": current["gps_set_by_name"],
        "overwritten_set_by_role": current["gps_set_by_role"],
        "reason": ("pre-deploy-gap recovery: Умиджон's 2026-05-14 13:18 "
                   "overwrite predated the auto_overwrite_snapshot code "
                   "(deploy a1923f21), so the restore-pin endpoint had no "
                   "admin_action_log snapshot to read. Restored from "
                   "location_attempts row 147 (Дилшод's original "
                   "2026-05-08 share)."),
    }

    print(f"DB: {args.db}")
    print(f"Mode: {'APPLY' if args.apply else 'DRY-RUN'}")
    print()
    print(f"Source: location_attempts.id={SOURCE_ATTEMPT_ID}")
    print(f"  received_at={attempt['received_at']}")
    print(f"  telegram_id={attempt['telegram_id']} ({attempt['first_name']})")
    print(f"  lat,lng={attempt['latitude']}, {attempt['longitude']}")
    print(f"  reverse_geocode={geo}")
    print()
    print(f"Target: allowed_clients.id={CLIENT_ID} ({current['name']})")
    print(f"  CURRENT gps_latitude={current['gps_latitude']}")
    print(f"  CURRENT gps_longitude={current['gps_longitude']}")
    print(f"  CURRENT gps_set_at={current['gps_set_at']}")
    print(f"  CURRENT gps_set_by={current['gps_set_by_name']} "
          f"(tg={current['gps_set_by_tg_id']}, role={current['gps_set_by_role']})")
    print()
    print("Planned UPDATE values:")
    for k, v in new_values.items():
        print(f"  {k} = {v!r}")
    print()
    print("Planned admin_action_log row:")
    print(f"  command = 'restore_from_location_attempts'")
    print(f"  user_name = 'script:restore_bakhrom_pin.py'")
    print(f"  args = {json.dumps(audit_args, ensure_ascii=False)}")
    print()

    if not args.apply:
        print("Dry-run only — no changes written. Re-run with --apply to commit.")
        conn.close()
        return 0

    try:
        conn.execute(
            "INSERT INTO admin_action_log (telegram_id, user_name, command, args) "
            "VALUES (?, ?, ?, ?)",
            (0, "script:restore_bakhrom_pin.py", "restore_from_location_attempts",
             json.dumps(audit_args, ensure_ascii=False)),
        )
        audit_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        conn.execute(
            "UPDATE allowed_clients SET "
            "gps_latitude = ?, gps_longitude = ?, gps_address = ?, "
            "gps_region = ?, gps_district = ?, gps_set_at = ?, "
            "gps_set_by_tg_id = ?, gps_set_by_name = ?, gps_set_by_role = ? "
            "WHERE id = ?",
            (new_values["gps_latitude"], new_values["gps_longitude"],
             new_values["gps_address"], new_values["gps_region"],
             new_values["gps_district"], new_values["gps_set_at"],
             new_values["gps_set_by_tg_id"], new_values["gps_set_by_name"],
             new_values["gps_set_by_role"], CLIENT_ID),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        print(f"FAILED — rolled back: {e}", file=sys.stderr)
        return 1

    print(f"OK — wrote admin_action_log.id={audit_id} + UPDATEd allowed_clients.id={CLIENT_ID}.")

    verify = conn.execute(
        "SELECT gps_latitude, gps_longitude, gps_set_at, gps_set_by_name "
        "FROM allowed_clients WHERE id = ?",
        (CLIENT_ID,),
    ).fetchone()
    print(f"Verify: id={CLIENT_ID} now pinned at "
          f"{verify['gps_latitude']}, {verify['gps_longitude']} "
          f"set_at={verify['gps_set_at']} by {verify['gps_set_by_name']}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
