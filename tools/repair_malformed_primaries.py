#!/usr/bin/env python3
"""One-off repair for MULTI_PHONE_CELL_MISALIGNMENT primary corruption.

Background: the 2026-05-29 multi-phone-cell parser (`_walk_digits_into_phones`)
blindly sliced 9-digit windows, so cells whose digit count wasn't a clean
multiple of 9 produced primaries with invalid operator prefixes (e.g. an active
client whose real number 915194019 was overwritten with 549009591). ~88 active
clients ended up unreachable by their primary number. The parser is fixed in
`import_clients._walk_digits_into_phones`; this tool repairs the rows that were
already corrupted before the fix deployed.

Repair strategy, per active client whose `phone_normalized` is not a valid UZ
mobile, in priority order:
  1. The most recent VALID `old_phone` in `phone_history` — the number that was
     there immediately before corruption (recovers the "lost" cases).
  2. A valid `raqam_02` / `raqam_03` slot (the displaced number landed there).
If neither yields a valid number, the row is flagged `needs_review` for a human.

Guards:
  - UNIQUE(phone_normalized): if the candidate already belongs to another active
    client, skip + flag needs_review (a real cross-client collision to triage).
  - Every change is logged to `phone_history` (reason=backfill_repair_...), so
    the malformed value is never lost (zero-data-loss rule).

Usage (run from the repo root so `backend` imports resolve):
    python3 tools/repair_malformed_primaries.py            # dry-run (default)
    python3 tools/repair_malformed_primaries.py --apply     # execute
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.services.import_clients import is_valid_uz_mobile  # noqa: E402

DB_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")
APPLY = "--apply" in sys.argv


def _valid_candidate(c, row):
    """Return (number, source) for the best valid replacement, or (None, None)."""
    cid = row["id"]
    # 1. Most recent valid old_phone from history (the pre-corruption value).
    for h in c.execute(
        "SELECT old_phone FROM phone_history WHERE client_id=? ORDER BY changed_at DESC",
        (cid,),
    ):
        if is_valid_uz_mobile(h["old_phone"]):
            return h["old_phone"], "history"
    # 2. A valid secondary slot.
    for slot in ("raqam_02", "raqam_03"):
        if is_valid_uz_mobile(row[slot]):
            return row[slot], slot
    return None, None


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    active = c.execute(
        """SELECT id, phone_normalized, raqam_02, raqam_03, name, client_id_1c
             FROM allowed_clients
            WHERE COALESCE(status,'active')='active'"""
    ).fetchall()
    targets = [r for r in active if not is_valid_uz_mobile(r["phone_normalized"])]

    repairs, collisions, unrepairable = [], [], []
    for r in targets:
        cand, src = _valid_candidate(c, r)
        label = r["client_id_1c"] or r["name"] or "?"
        if not cand:
            unrepairable.append((r["id"], r["phone_normalized"], label))
            continue
        clash = c.execute(
            "SELECT id FROM allowed_clients WHERE phone_normalized=? AND id<>?",
            (cand, r["id"]),
        ).fetchone()
        if clash:
            collisions.append((r["id"], r["phone_normalized"], cand, clash["id"], label))
            continue
        repairs.append((r["id"], r["phone_normalized"], cand, src, label))

    print(f"DB: {DB_PATH}")
    print(f"Active clients with malformed primary: {len(targets)}")
    print(f"  repairable: {len(repairs)}  | collisions: {len(collisions)}  | "
          f"unrepairable: {len(unrepairable)}\n")

    print("== REPAIRS (malformed -> restored) ==")
    for cid, bad, cand, src, label in repairs:
        print(f"  id={cid:<6} {bad} -> {cand}  (from {src})  | {label}")
    if collisions:
        print("\n== COLLISIONS (candidate already another client's primary — flag needs_review) ==")
        for cid, bad, cand, other, label in collisions:
            print(f"  id={cid:<6} {bad} -> {cand} BLOCKED by id={other}  | {label}")
    if unrepairable:
        print("\n== UNREPAIRABLE (no valid number anywhere — flag needs_review) ==")
        for cid, bad, label in unrepairable:
            print(f"  id={cid:<6} {bad}  | {label}")

    if not APPLY:
        print("\nDRY-RUN — no changes written. Re-run with --apply to execute.")
        db.close()
        return

    for cid, bad, cand, src, label in repairs:
        c.execute(
            "INSERT INTO phone_history (client_id, old_phone, new_phone, reason, changed_by) "
            "VALUES (?, ?, ?, ?, ?)",
            (cid, bad, cand, "backfill_repair_malformed_primary", "repair_tool"),
        )
        c.execute("UPDATE allowed_clients SET phone_normalized=? WHERE id=?", (cand, cid))
        # Drop any slot now duplicating the promoted primary.
        c.execute("UPDATE allowed_clients SET raqam_02=NULL WHERE id=? AND raqam_02=?", (cid, cand))
        c.execute("UPDATE allowed_clients SET raqam_03=NULL WHERE id=? AND raqam_03=?", (cid, cand))
    for cid, bad, cand, other, label in collisions:
        c.execute("UPDATE allowed_clients SET needs_review=1 WHERE id=?", (cid,))
    for cid, bad, label in unrepairable:
        c.execute("UPDATE allowed_clients SET needs_review=1 WHERE id=?", (cid,))
    db.commit()
    print(f"\nAPPLIED: {len(repairs)} repaired, "
          f"{len(collisions) + len(unrepairable)} flagged needs_review.")
    db.close()


if __name__ == "__main__":
    main()
