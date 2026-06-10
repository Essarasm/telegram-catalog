"""Backfill products.weight from the authoritative sales-derived weight.

Root fix for Error Log #89 (corrupted products.weight). Resolves every active
product's weight through backend.services.product_weight.authoritative_weight:
  kg-unit → 1.0 ; sales-derived (1C сумма веса / qty) when grossly off ; else
  keep / name-parse.

Reversible: writes a before/after snapshot to data/weight_backfill_<ts>.json
before committing. To revert, run with --revert <snapshot.json>.

Usage (inside Railway container):
  python3 tools/backfill_product_weights.py            # dry-run, prints summary
  python3 tools/backfill_product_weights.py --apply    # writes + snapshot
  python3 tools/backfill_product_weights.py --revert data/weight_backfill_X.json
"""
import json
import os
import sys
import sqlite3
from datetime import datetime, timezone

DB = os.getenv("DATABASE_PATH", "/data/catalog.db")


def _conn():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c


def revert(path):
    snap = json.load(open(path))
    conn = _conn()
    n = 0
    for row in snap["changes"]:
        conn.execute("UPDATE products SET weight = ? WHERE id = ?",
                     (row["old"], row["id"]))
        n += 1
    conn.commit()
    conn.close()
    print(f"Reverted {n} products from {path}")


def run(apply=False):
    # Import the helper the same way the importers do, so backfill and live
    # writes share one precedence definition.
    sys.path.insert(0, os.getcwd())
    from backend.services.product_weight import (
        compute_sales_weights, authoritative_weight, is_kg_unit,
    )

    conn = _conn()
    sales = compute_sales_weights(conn)
    prods = conn.execute(
        "SELECT id, name, weight, unit FROM products WHERE is_active = 1"
    ).fetchall()

    changes = []
    by_source = {"kg_unit": 0, "sales": 0, "name_parse_or_keep": 0}
    no_signal = 0
    for p in prods:
        pid, name, cur, unit = p["id"], p["name"], p["weight"], p["unit"]
        sw = sales.get(pid)
        target = authoritative_weight(cur, unit, sw, name=name)
        if target is None:
            no_signal += 1
            continue
        # Attribute the source for reporting.
        if is_kg_unit(unit):
            src = "kg_unit"
        elif sw and sw > 0:
            src = "sales"
        else:
            src = "name_parse_or_keep"
        cur_r = round(cur, 4) if cur is not None else None
        if cur_r != round(target, 4):
            changes.append({"id": pid, "name": name[:60], "unit": unit,
                            "old": cur, "new": round(target, 4), "source": src})
            by_source[src] += 1

    summary = {
        "db": DB,
        "active_products": len(prods),
        "products_with_sales_weight": sum(1 for p in prods if p["id"] in sales),
        "active_without_sales_weight": sum(1 for p in prods if p["id"] not in sales),
        "changes_total": len(changes),
        "changes_by_source": by_source,
        "left_unchanged_no_signal": no_signal,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    print("\nSample changes:")
    for ch in changes[:25]:
        print(f"  [{ch['source']:>18}] {ch['old']} -> {ch['new']}  ({ch['unit']}) {ch['name']}")

    if not apply:
        print("\nDRY RUN — no writes. Re-run with --apply to commit.")
        conn.close()
        return

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snap_path = os.path.join(os.path.dirname(DB) or ".", f"weight_backfill_{ts}.json")
    json.dump({"ts": ts, "summary": summary, "changes": changes},
              open(snap_path, "w"), ensure_ascii=False)
    for ch in changes:
        conn.execute("UPDATE products SET weight = ? WHERE id = ?",
                     (ch["new"], ch["id"]))
    conn.commit()
    conn.close()
    print(f"\nAPPLIED {len(changes)} changes. Snapshot: {snap_path}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "--revert":
        revert(args[1])
    else:
        run(apply="--apply" in args)
