#!/usr/bin/env python3
"""Client Identity Anchoring — Phase 0b APPLY.

Backfills ``onec_card_id`` onto existing allowed_clients rows and merges the
card-id-proven duplicates, reusing the tested Approach-E merge engine
(``tools/merge_duplicate_1c_clients.py``: 21-table FK remap, composite-UNIQUE
pre-dedup, soft-merge status='merged_into:<id>', admin_action_log audit).

Driven by a spec JSON (produced locally from the export + a prod snapshot):
  { "merge":    [{card_id, ids:[...], phones:[ordered real phones], name, approved}],
    "backfill": {"<row_id>": "<card_id>", ...},   # single-match rows
    "hold":     ["Прочие:304", "Прочие:470"] }    # NOT merged (informational)

Per merge cluster:
  1. Fetch the live ACTIVE rows for the card's expected ids. If the live set
     drifted from the spec (a row already merged/gone/added), SKIP + report —
     never merge a cluster whose shape changed since the snapshot.
  2. engine.merge_cluster(apply): FK remap + fill-only soft fields + soft-merge.
  3. Reconcile the survivor's phones to the card's authoritative real numbers
     (drops the garbled/rotated phone that spawned the dupe) + stamp onec_card_id.

Backfill rows: fill-only onec_card_id stamp (skips any row already anchored).

DEFAULT IS DRY-RUN. Pass --apply to write (backs up the DB first). Idempotent:
merged rows leave the active set; backfilled rows skip the fill-only guard.
"""
import argparse
import importlib.util
import json
import os
import shutil
import sqlite3
import sys


def _load_engine():
    """Load the merge engine module from <cwd>/tools/ (works on the container,
    where the deployed app ships tools/ alongside backend/)."""
    here = os.getcwd()
    sys.path.insert(0, here)  # so the engine's `import backend...` resolves
    path = os.path.join(here, "tools", "merge_duplicate_1c_clients.py")
    if not os.path.exists(path):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "merge_duplicate_1c_clients.py")
    spec = importlib.util.spec_from_file_location("merge_engine", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fetch_active(conn, ids):
    ph = ",".join("?" * len(ids))
    rows = conn.execute(
        f"SELECT * FROM allowed_clients WHERE id IN ({ph}) "
        f"AND COALESCE(status,'active') NOT LIKE 'merged%'",
        ids,
    ).fetchall()
    return rows


def _reconcile_anchor_and_phones(conn, canonical_id, card_id, card_phones,
                                 cluster_rows, approved, apply):
    """Make the survivor's phones the card's authoritative 1C numbers and stamp
    onec_card_id. The card is the source of truth:
      - card has phones → primary = card[0], secondaries = card[1:3]; the garbled
        numbers that spawned the dupe are dropped.
      - card has no phones → keep the survivor's existing primary untouched.
    Every dropped phone is logged to phone_history (zero silent loss). For a NEW
    (unverified) cluster, dropping any number also flags needs_review so a human
    can confirm it was a garble and not a genuine second line. Registry-approved
    clusters keep exactly the human-confirmed numbers (= the card phones), so a
    drop there is a known garble and is not flagged.
    Returns (final_primary, secs, flagged)."""
    old_primary = conn.execute(
        "SELECT phone_normalized FROM allowed_clients WHERE id=?",
        (canonical_id,),
    ).fetchone()[0]

    if card_phones:
        primary = card_phones[0]
        secs = card_phones[1:3]
    else:
        primary = old_primary
        secs = []
    kept = {primary, *secs}

    # Every phone that existed across the cluster (survivor + merged rows).
    all_phones = set()
    for r in cluster_rows:
        for col in ("phone_normalized", "raqam_02", "raqam_03"):
            v = r[col]
            if v:
                all_phones.add(str(v).strip())
    dropped = [p for p in all_phones if p not in kept]
    flagged = (not approved) and bool(dropped)

    if not apply:
        return primary, secs, flagged

    try:
        conn.execute(
            "UPDATE allowed_clients SET phone_normalized=?, raqam_02=?, ism_02=NULL, "
            "raqam_03=?, ism_03=NULL, onec_card_id=?%s WHERE id=?"
            % (", needs_review=1" if flagged else ""),
            (primary, secs[0] if len(secs) > 0 else None,
             secs[1] if len(secs) > 1 else None, card_id, canonical_id),
        )
    except sqlite3.IntegrityError:
        # New primary collides with another active row's phone — keep the old
        # primary, still stamp the anchor + secondaries, and flag for review.
        conn.rollback()
        primary = old_primary
        fb_secs = [p for p in card_phones if p != old_primary][:2]
        secs = fb_secs
        conn.execute(
            "UPDATE allowed_clients SET raqam_02=?, ism_02=NULL, raqam_03=?, "
            "ism_03=NULL, onec_card_id=?, needs_review=1 WHERE id=?",
            (fb_secs[0] if len(fb_secs) > 0 else None,
             fb_secs[1] if len(fb_secs) > 1 else None, card_id, canonical_id),
        )
        flagged = True
    # phone_history: primary change + any non-garble dropped number.
    if primary != old_primary:
        conn.execute(
            "INSERT INTO phone_history (client_id, old_phone, new_phone, reason, changed_by) "
            "VALUES (?, ?, ?, ?, ?)",
            (canonical_id, old_primary, primary,
             "phase0b card-anchor reconcile (drop garbled primary)", "script:phase0b"),
        )
    for p in dropped:
        if p == old_primary:
            continue  # already logged above as the primary change
        conn.execute(
            "INSERT INTO phone_history (client_id, old_phone, new_phone, reason, changed_by) "
            "VALUES (?, ?, ?, ?, ?)",
            (canonical_id, p, primary,
             "phase0b dropped non-card phone (card-anchor reconcile)", "script:phase0b"),
        )
    conn.commit()
    return primary, secs, flagged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write (default: dry-run)")
    ap.add_argument("--db", default=os.environ.get("DATABASE_PATH", "/data/catalog.db"))
    ap.add_argument("--spec", default="/tmp/phase0b_spec.json")
    args = ap.parse_args()

    spec = json.load(open(args.spec, encoding="utf-8"))
    engine = _load_engine()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    fk_refs = engine.find_fk_references(conn)

    print("=" * 78)
    print("PHASE 0b APPLY — %s" % ("APPLY (writing)" if args.apply else "DRY-RUN"))
    print("DB: %s   |   FK tables: %d   |   merge: %d   backfill: %d   hold: %s"
          % (args.db, len(fk_refs), len(spec["merge"]), len(spec["backfill"]),
             ",".join(spec.get("hold", []))))
    print("=" * 78)

    if args.apply:
        bak = args.db + ".pre-phase0b"
        shutil.copy(args.db, bak)
        print("DB backed up → %s\n" % bak)

    merged = skipped = flagged = 0
    for t in spec["merge"]:
        card_id, ids, phones = t["card_id"], t["ids"], t["phones"]
        rows = _fetch_active(conn, ids)
        live_ids = sorted(r["id"] for r in rows)
        tag = "approved" if t["approved"] else "NEW"
        if live_ids != sorted(ids):
            skipped += 1
            print("  SKIP   %-14s [%s] live ids %s != spec %s (drift/already-merged)"
                  % (card_id, tag, live_ids, sorted(ids)))
            continue

        res = engine.merge_cluster(conn, card_id, rows, fk_refs, apply=args.apply)
        st = res["status"]
        if st in ("flag_gps_conflict", "flag_no_1c_source"):
            flagged += 1
            print("  FLAG   %-14s [%s] %s — %s"
                  % (card_id, tag, st, res.get("reason", "")))
            continue
        if st == "apply_failed":
            flagged += 1
            print("  FAIL   %-14s [%s] %s" % (card_id, tag, res.get("error")))
            continue

        can = res["canonical_id"]
        primary, secs, flagged = _reconcile_anchor_and_phones(
            conn, can, card_id, phones, rows, t["approved"], args.apply)
        merged += 1
        verb = "MERGED" if args.apply else "would-merge"
        print("  %-10s %-14s [%s] %s  survivor=id%d  merged=%s  fk_rows=%d  phones→%s%s"
              % (verb, card_id, tag, t["name"][:28], can, res["non_canon_ids"],
                 res.get("total_fk_rows", 0), [primary] + list(secs),
                 "  ⚠needs_review(non-card phone preserved)" if flagged else ""))

    # ── Backfill single-match rows (fill-only onec_card_id stamp) ─────────────
    bf_done = bf_skip = 0
    for rid, card_id in spec["backfill"].items():
        rid = int(rid)
        cur = conn.execute(
            "SELECT onec_card_id, status FROM allowed_clients WHERE id=?", (rid,)
        ).fetchone()
        if cur is None or str(cur["status"] or "active").startswith("merged"):
            bf_skip += 1
            continue
        if cur["onec_card_id"]:
            bf_skip += 1
            continue
        if args.apply:
            try:
                conn.execute(
                    "UPDATE allowed_clients SET onec_card_id=? WHERE id=?",
                    (card_id, rid),
                )
            except sqlite3.IntegrityError:
                bf_skip += 1
                continue
        bf_done += 1
    if args.apply:
        conn.commit()

    print("\n" + "-" * 78)
    print("Merge clusters: merged=%d  skipped(drift)=%d  flagged=%d"
          % (merged, skipped, flagged))
    print("Backfill: %s=%d  skipped=%d"
          % ("stamped" if args.apply else "would-stamp", bf_done, bf_skip))
    if not args.apply:
        print("\nDRY-RUN — nothing written. Re-run with --apply to execute.")
    else:
        anchored = conn.execute(
            "SELECT COUNT(*) FROM allowed_clients WHERE onec_card_id IS NOT NULL "
            "AND onec_card_id!='' AND COALESCE(status,'active') NOT LIKE 'merged%'"
        ).fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(*) FROM allowed_clients "
            "WHERE COALESCE(status,'active') NOT LIKE 'merged%'"
        ).fetchone()[0]
        print("Post-apply: %d/%d active rows anchored." % (anchored, active))
    conn.close()


if __name__ == "__main__":
    main()
