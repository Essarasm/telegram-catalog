"""Authoritative product weight — kg per the product's OWN sold unit.

`products.weight` has three possible writer-owned sources, which over time
collided and left the column unreliable (Error Log #89). This module is the
shared read-helper mandated by `.claude/rules/12-dual-source-columns.md`: every
weight writer and reader must resolve the value through here, never by trusting
one raw source inline.

Precedence (highest confidence first):

1. **Definitional** — the product is sold by the kilogram (`unit ∈ {кг, kg}`),
   so 1 unit weighs 1 kg. weight = 1.0, unconditionally. This is what makes
   inbound tonnage (`supply_qty × weight`) correct for kg-sold goods, and it
   fixes the classic bug where the name's PACK weight ("/20 кг/" → 20) leaked
   into a kg-sold item.

2. **Sales-derived** — 1C outbound docs carry `сумма веса` (total_weight) and a
   quantity per line. `SUM(total_weight) / SUM(quantity)` across every shipment
   of a product is its real kg-per-unit. It comes straight from 1C, is
   independent of `products.weight`, and is stable across container restarts —
   so anchoring to it is what kills the import flip-flop.

3. **Name-parse** — last-resort guess from the product name. Extracts the pack
   weight and is wrong whenever the item is sold by the kg, so it is used only
   for products that have never been shipped (no sales signal at all).

The Excel/1C "Вес" price-list column is deliberately NOT trusted when a sales
signal exists — it is the source that contained the garbage (a plinth end-cap
at 48 kg). It survives only as a fallback for never-shipped products.
"""
from backend.services.parse_weight import parse_weight_from_name

# Units that mean "sold by the kilogram" → 1 unit == 1 kg by definition.
KG_UNITS = {"кг", "kg"}

# A candidate weight is "grossly wrong" relative to ground truth if it is off by
# more than this multiplicative factor in either direction.
GROSS_FACTOR = 2.0


def is_kg_unit(unit) -> bool:
    return bool(unit) and str(unit).strip().lower() in KG_UNITS


def within_factor(a, b, factor: float = GROSS_FACTOR) -> bool:
    """True if a and b agree within `factor`× in either direction."""
    if not a or not b or a <= 0 or b <= 0:
        return False
    return max(a / b, b / a) <= factor


def compute_sales_weights(conn) -> dict:
    """Map product_id → sales-derived kg-per-unit for every product that has
    at least one shipped line with total_weight>0 and quantity>0.

    SUM/SUM is a quantity-weighted average, so a single noisy line is damped by
    the rest of the product's shipment history.
    """
    rows = conn.execute(
        """
        SELECT product_id,
               SUM(total_weight) AS tw,
               SUM(quantity)     AS q
        FROM real_order_items
        WHERE product_id IS NOT NULL
          AND total_weight > 0
          AND quantity > 0
        GROUP BY product_id
        """
    ).fetchall()
    out = {}
    for r in rows:
        pid, tw, q = r["product_id"], r["tw"], r["q"]
        if q and q > 0 and tw and tw > 0:
            out[pid] = tw / q
    return out


def _norm_name(name: str) -> str:
    import re
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def compute_sales_weights_by_name(conn) -> dict:
    """Map normalized product_name_1c → sales-derived kg/unit.

    For importers that write weight before product_id links exist — the full
    catalog rebuild (import_products) does DELETE FROM products and resets IDs,
    so product_id-keyed lookups are stale mid-import. real_order_items keeps its
    1C name, so we anchor by name instead.
    """
    rows = conn.execute(
        """
        SELECT product_name_1c AS n,
               SUM(total_weight) AS tw,
               SUM(quantity)     AS q
        FROM real_order_items
        WHERE product_name_1c IS NOT NULL
          AND total_weight > 0
          AND quantity > 0
        GROUP BY product_name_1c
        """
    ).fetchall()
    out = {}
    for r in rows:
        if r["q"] and r["q"] > 0 and r["tw"] and r["tw"] > 0:
            out[_norm_name(r["n"])] = r["tw"] / r["q"]
    return out


def suggest_weight(unit, sales_w, name):
    """Ground-truth weight a product SHOULD have, ignoring its current value.

    Used by the Product Cleanup tab to render a suggestion, and by the backfill
    to compute targets. Returns (weight_kg, source) or (None, "none").
    """
    if is_kg_unit(unit):
        return 1.0, "kg_unit"
    if sales_w and sales_w > 0:
        return round(sales_w, 4), "sales"
    if name:
        pw = parse_weight_from_name(name)
        if pw and pw > 0:
            return round(pw, 4), "name_parse"
    return None, "none"


def authoritative_weight(existing, unit, sales_w, name=None, excel_candidate=None):
    """Resolve the weight a writer should persist for a product.

    `existing`        — the weight currently stored (None for a brand-new row).
    `excel_candidate` — a weight a writer is proposing from the Excel/1C column
                        (None when the writer has no such value, e.g. backfill).

    Rules, in order:
    - kg-sold unit → 1.0 (definitional, overrides everything).
    - sales signal present → it is authoritative and wins outright. This is what
      makes re-imports idempotent (the sales value is stable across restarts) and
      kills the Excel↔name-parse flip-flop. The Excel candidate is ignored — it
      is the untrusted source. (A manually-confirmed weight is therefore NOT
      preserved against sales; if a per-product manual lock is ever needed, add a
      weight_override column the way client_balance_overrides did — Error Log #61.)
    - no sales signal → accept the Excel candidate, else keep existing, else
      fall back to name-parse.
    """
    if is_kg_unit(unit):
        return 1.0

    if sales_w and sales_w > 0:
        return round(sales_w, 4)

    # No sales signal — fall back to the weaker sources.
    if excel_candidate and excel_candidate > 0:
        return float(excel_candidate)
    if existing and existing > 0:
        return round(existing, 4)
    if name:
        pw = parse_weight_from_name(name)
        if pw and pw > 0:
            return round(pw, 4)
    return existing
