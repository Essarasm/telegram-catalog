#!/usr/bin/env python3
"""Analyze and auto-resolve unmatched stock import names.

Reads unmatched_import_names from production DB, finds likely matches
using progressively looser strategies, and generates resolution commands.

Run: python3 tools/resolve_unmatched.py [--apply]

Without --apply: prints analysis and suggested /aliases link commands.
With --apply: auto-links high-confidence matches (>= 0.85 similarity).
"""
import os
import sys
import sqlite3
import re
from difflib import get_close_matches

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DATABASE_PATH = os.getenv("DATABASE_PATH", "data/catalog.db")
AUTO_APPLY = "--apply" in sys.argv


def normalize(name):
    """Normalize for matching: lowercase, collapse whitespace, strip dots."""
    if not name:
        return ""
    n = name.strip().lower()
    n = re.sub(r'\s+', ' ', n)
    n = re.sub(r'\.(\s|$)', r'\1', n)
    n = re.sub(r'\s*/\s*', '/', n)
    return n


def extract_keywords(name):
    """Extract significant keywords (length >= 3) from a product name."""
    norm = normalize(name)
    norm = re.sub(r'[/\\()«»"\'\-]', ' ', norm)
    words = [w for w in norm.split() if len(w) >= 3]
    return set(words)


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    try:
        unmatched = conn.execute(
            "SELECT id, name, name_lower, occurrences, source "
            "FROM unmatched_import_names WHERE resolved = 0 "
            "ORDER BY occurrences DESC"
        ).fetchall()
    except Exception:
        print("unmatched_import_names table not found. Run /stock first.")
        conn.close()
        return

    if not unmatched:
        print("No unmatched names to resolve.")
        conn.close()
        return

    products = conn.execute(
        "SELECT id, name, name_display FROM products WHERE is_active = 1 AND name IS NOT NULL"
    ).fetchall()

    # Build lookup indices
    exact_index = {}
    norm_index = {}
    keyword_index = {}
    for p in products:
        exact_index[p["name"].strip()] = p
        n = normalize(p["name"])
        norm_index[n] = p
        for kw in extract_keywords(p["name"]):
            keyword_index.setdefault(kw, []).append(p)

    norm_keys = list(norm_index.keys())

    auto_resolved = 0
    manual_needed = []
    new_products = []

    print(f"Unmatched names: {len(unmatched)}")
    print(f"Active products: {len(products)}")
    print(f"{'='*60}\n")

    for row in unmatched:
        name = row["name"]
        name_norm = normalize(name)
        best_match = None
        match_method = None
        confidence = 0

        # Strategy 1: fuzzy match on full normalized name (>= 0.85)
        close = get_close_matches(name_norm, norm_keys, n=1, cutoff=0.85)
        if close:
            best_match = norm_index[close[0]]
            match_method = "fuzzy_high"
            confidence = 0.9

        # Strategy 2: keyword overlap — if unmatched shares 3+ keywords with a product
        if not best_match:
            um_keywords = extract_keywords(name)
            best_overlap = 0
            for p in products:
                p_keywords = extract_keywords(p["name"])
                overlap = len(um_keywords & p_keywords)
                if overlap > best_overlap and overlap >= 3:
                    best_overlap = overlap
                    best_match = p
                    match_method = f"keyword({overlap})"
                    confidence = min(0.5 + overlap * 0.1, 0.85)

        # Strategy 3: lower fuzzy threshold (>= 0.7) — suggest but don't auto-apply
        if not best_match:
            close = get_close_matches(name_norm, norm_keys, n=1, cutoff=0.7)
            if close:
                best_match = norm_index[close[0]]
                match_method = "fuzzy_low"
                confidence = 0.7

        if best_match and confidence >= 0.85 and AUTO_APPLY:
            conn.execute(
                "INSERT OR IGNORE INTO product_aliases (alias_name, alias_name_lower, product_id, source) "
                "VALUES (?, ?, ?, ?)",
                (name.strip(), name_norm, best_match["id"], f"auto_resolve_{match_method}"),
            )
            conn.execute(
                "UPDATE unmatched_import_names SET resolved = 1, resolved_product_id = ?, resolved_at = datetime('now') "
                "WHERE id = ?",
                (best_match["id"], row["id"]),
            )
            auto_resolved += 1
            print(f"  AUTO [{match_method}] {name}")
            print(f"    -> [{best_match['id']}] {best_match['name']}")
        elif best_match:
            manual_needed.append((row, best_match, match_method, confidence))
        else:
            new_products.append(row)

    if AUTO_APPLY:
        conn.commit()

    if manual_needed:
        print(f"\nSUGGESTED LINKS ({len(manual_needed)}):")
        print("Copy-paste these into the Admin group:\n")
        for row, match, method, conf in manual_needed:
            print(f"  # {row['name']} [{method}, {conf:.0%}]")
            print(f"  /aliases link {row['name']} {match['id']}")
            print(f"  # -> {match['name']}\n")

    if new_products:
        print(f"\nNEW PRODUCTS ({len(new_products)}) — not in catalog:")
        for row in new_products:
            print(f"  [{row['occurrences']}x] {row['name']}")

    print(f"\n{'='*60}")
    print(f"Auto-resolved: {auto_resolved}")
    print(f"Manual review: {len(manual_needed)}")
    print(f"New products:  {len(new_products)}")

    conn.close()


if __name__ == "__main__":
    main()
