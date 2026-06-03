#!/usr/bin/env python3
"""Client Identity Anchoring — Phase 0b dry-run (READ-ONLY).

Backfills ``onec_card_id`` onto existing allowed_clients rows by matching them
against the 1C Контрагенты export, then surfaces the proven duplicates — two+
active rows resolving to the SAME card — as merge candidates, cross-checked
against the human verdict registry (``client_identity_reviewed``).

Changes NOTHING. Produces the plan that the (later) --apply step would execute.

Inputs:
  --export   path to the 1C Контрагенты .xls (default: ~/Downloads/Clients 02.06.2026.xls)
  --snapshot path to a JSON dump of active allowed_clients rows (default: /tmp/prod_ac.json)
             shape: [{id,name,client_id_1c,phone_normalized,raqam_02,raqam_03,
                      onec_card_id,gps_latitude,credit_score,credit_limit,
                      status,linked_users}, ...]

Matching precedence per DB row:
  phone (any of phone_normalized/raqam_02/raqam_03 ∈ a card's phones) > name
  (normalize_1c(client_id_1c) == normalize_1c(card name)). Multiple phone-cards
  for one row → 'ambiguous' (never auto-assigned). No match → 'unmatched'.
"""
import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.services.import_clients import _iter_rows_from_xls, parse_phone_cell
from backend.services.client_identity_reviewed import (
    normalize_1c, CONFIRMED_SAME_SHOP, CONFIRMED_DISTINCT_SHARED_NAMES,
)

_ID_PAIR_RE = re.compile(r"ids?\s+(\d+(?:\s*\+\s*\d+)+)", re.IGNORECASE)


def _approved_id_groups():
    """Set of frozenset(id,...) the registry has confirmed as the SAME shop."""
    groups = []
    for val in CONFIRMED_SAME_SHOP.values():
        m = _ID_PAIR_RE.search(val or "")
        if m:
            ids = frozenset(int(x) for x in re.findall(r"\d+", m.group(1)))
            if len(ids) >= 2:
                groups.append(ids)
    return groups


def _phones_of(card_phone_raw):
    return {p["digits"] for p in parse_phone_cell(card_phone_raw or "")}


def load_cards(export_path):
    _hdr, rows = _iter_rows_from_xls(open(export_path, "rb").read())
    cards = {}
    for r in rows:
        cid = r.get("onec_card_id")
        if not cid:
            continue
        cards[cid] = {
            "onec_card_id": cid,
            "name": str(r.get("name") or "").strip(),
            "phones": _phones_of(r.get("phone")),
        }
    return cards


def build_indexes(cards):
    by_phone, by_name = {}, {}
    for cid, c in cards.items():
        for ph in c["phones"]:
            by_phone.setdefault(ph, set()).add(cid)
        nk = normalize_1c(c["name"])
        if nk:
            by_name.setdefault(nk, set()).add(cid)
    return by_phone, by_name


def assign(row, by_phone, by_name):
    db_phones = {row.get(k) for k in ("phone_normalized", "raqam_02", "raqam_03")
                 if row.get(k)}
    phone_cards = set()
    for ph in db_phones:
        phone_cards |= by_phone.get(ph, set())
    if len(phone_cards) == 1:
        return next(iter(phone_cards)), "phone"
    if len(phone_cards) > 1:
        return None, "ambiguous(%d cards)" % len(phone_cards)
    name_cards = by_name.get(normalize_1c(row.get("client_id_1c") or ""), set())
    if len(name_cards) == 1:
        return next(iter(name_cards)), "name"
    if len(name_cards) > 1:
        return None, "ambiguous(%d name-cards)" % len(name_cards)
    return None, "unmatched"


def curated_tags(row):
    tags = []
    if row.get("gps_latitude") is not None:
        tags.append("gps")
    if row.get("credit_score") is not None:
        tags.append("credit_score")
    if row.get("credit_limit") is not None:
        tags.append("credit_limit")
    if row.get("linked_users"):
        tags.append("linked_user")
    return tags


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--export", default=os.path.expanduser("~/Downloads/Clients 02.06.2026.xls"))
    ap.add_argument("--snapshot", default="/tmp/prod_ac.json")
    args = ap.parse_args()

    cards = load_cards(args.export)
    by_phone, by_name = build_indexes(cards)
    rows = json.load(open(args.snapshot, encoding="utf-8"))

    via = {"phone": 0, "name": 0}
    unmatched, ambiguous = [], []
    clusters = {}  # onec_card_id -> [rows]
    for r in rows:
        cid, how = assign(r, by_phone, by_name)
        if cid is None:
            (ambiguous if how.startswith("ambiguous") else unmatched).append((r, how))
            continue
        via[how] += 1
        r["_assigned"] = cid
        clusters.setdefault(cid, []).append(r)

    merge_clusters = {c: rs for c, rs in clusters.items() if len(rs) >= 2}
    approved_groups = _approved_id_groups()

    def classify(ids_in_cluster):
        idset = set(ids_in_cluster)
        for g in approved_groups:
            if g & idset and len(g & idset) >= 2:
                return "APPROVED (CONFIRMED_SAME_SHOP)"
        return "NEW — needs review (not in registry)"

    print("=" * 78)
    print("PHASE 0b DRY-RUN — Client Identity Anchoring (READ-ONLY, no writes)")
    print("=" * 78)
    print("Export cards: %d   |   active DB rows: %d" % (len(cards), len(rows)))
    print("Backfill coverage: phone=%d  name=%d  unmatched=%d  ambiguous=%d"
          % (via["phone"], via["name"], len(unmatched), len(ambiguous)))
    print("Distinct cards assigned: %d   |   merge clusters (>=2 rows/card): %d"
          % (len(clusters), len(merge_clusters)))
    print()

    n_approved = sum(1 for rs in merge_clusters.values()
                     if "APPROVED" in classify([r["id"] for r in rs]))
    n_new = len(merge_clusters) - n_approved
    print("Merge clusters: %d APPROVED (in registry) + %d NEW (need review)\n"
          % (n_approved, n_new))

    # Where did every row land? (for anomaly tracing)
    assigned_of = {r["id"]: r.get("_assigned") for r in rows}
    unmatched_ids = {r["id"]: how for r, how in unmatched}
    ambiguous_ids = {r["id"]: how for r, how in ambiguous}

    def trace(i):
        if assigned_of.get(i):
            return "card %s" % assigned_of[i]
        if i in ambiguous_ids:
            return ambiguous_ids[i]
        if i in unmatched_ids:
            return "unmatched"
        return "NOT IN SNAPSHOT (already merged/absent)"

    print("── MERGE CANDIDATES (>=2 active rows share one card) ──")
    if not merge_clusters:
        print("  (none)")
    for cid, rs in sorted(merge_clusters.items(), key=lambda kv: -len(kv[1])):
        ids = [r["id"] for r in rs]
        verdict = classify(ids)
        print("\n  %s   →  %d rows  [%s]" % (cid, len(rs), verdict))
        print("    card name: %s" % cards[cid]["name"])
        print("    card phones: %s" % (", ".join(sorted(cards[cid]["phones"])) or "(none)"))
        for r in rs:
            ph = [p for p in (r.get("phone_normalized"), r.get("raqam_02"),
                              r.get("raqam_03")) if p]
            ct = curated_tags(r)
            print("      id=%-6s %-40s phones=%s%s"
                  % (r["id"], (r.get("client_id_1c") or r.get("name") or "")[:40],
                     ",".join(ph), ("  CURATED:" + "+".join(ct)) if ct else ""))

    # Anomaly checks against the registry.
    print("\n── REGISTRY CROSS-CHECK ──")
    merged_idsets = [set(r["id"] for r in rs) for rs in merge_clusters.values()]
    missing = []
    for g in approved_groups:
        if not any(g & ms and len(g & ms) >= 2 for ms in merged_idsets):
            missing.append(g)
    if missing:
        print("  ⚠ CONFIRMED_SAME_SHOP groups NOT reproduced as a same-card cluster")
        print("    (export shows them on different cards, OR a row didn't match):")
        for g in missing:
            print("      ids %s:" % "+".join(str(i) for i in sorted(g)))
            for i in sorted(g):
                print("        id=%s → %s" % (i, trace(i)))
    else:
        print("  ✓ every CONFIRMED_SAME_SHOP id-group reproduced as a same-card cluster")

    # Distinct-shared-name rows that wrongly collapsed onto one card.
    distinct_collisions = []
    for cid, rs in merge_clusters.items():
        names = {normalize_1c(r.get("client_id_1c") or "") for r in rs}
        for nm in names:
            if nm in CONFIRMED_DISTINCT_SHARED_NAMES:
                distinct_collisions.append((cid, nm))
    if distinct_collisions:
        print("  ⚠ CONFIRMED_DISTINCT (legit multi-shop) names landed on one card "
              "— investigate before merging:")
        for cid, nm in distinct_collisions:
            print("      %s  ← %s" % (cid, nm))
    else:
        print("  ✓ no CONFIRMED_DISTINCT name collapsed onto a single card")

    print("\n── UNMATCHED (no card; would stay anchor-less) ──  count=%d" % len(unmatched))
    for r, _how in unmatched[:25]:
        print("    id=%-6s %-40s phone=%s"
              % (r["id"], (r.get("client_id_1c") or r.get("name") or "")[:40],
                 r.get("phone_normalized")))
    if len(unmatched) > 25:
        print("    ... +%d more" % (len(unmatched) - 25))

    if ambiguous:
        print("\n── AMBIGUOUS (matched >1 card; never auto-assigned) ──  count=%d"
              % len(ambiguous))
        for r, how in ambiguous[:25]:
            print("    id=%-6s %-40s %s"
                  % (r["id"], (r.get("client_id_1c") or r.get("name") or "")[:40], how))

    print("\n" + "=" * 78)
    print("DRY-RUN ONLY — nothing was written. Review the plan above.")
    print("=" * 78)


if __name__ == "__main__":
    main()
