"""Brand-family → (category_id, producer_id) classifier shared by import flows.

Used by:
  - /prices auto-add (backend/services/update_prices.py) — products in the
    XLS but not in the DB
  - /realorders ingest_unmatched_skus (backend/services/import_real_orders.py)
    — product names appearing in shipments before they exist in the catalog

Both call sites previously diverged: real-orders ingestion classified by
brand prefix, /prices dumped everything into "Yangi mahsulotlar" + "Boshqa".
This module is the single source for the classification map.

Map values are (category_id, producer_id) tuples referencing rows seeded from
Rassvet_Master.xlsx. classify_or_default() verifies the IDs still exist before
returning them so a fresh DB without the master import doesn't FK-error.
"""
from typing import Optional, Tuple

BRAND_FAMILY_MAP = {
    # OSCAR products: Лак, Жидкое стекло, Олиф
    "БТ Лак OSCAR":          (10, 21),  # Laklar & Oliflar, Oscar
    "Жидкое стекло OSCAR":   (10, 21),
    "Олиф OSCAR":            (10, 21),
    # ДЕЛЮКС water emulsions
    "ДЕЛЮКС в/э":            (17, 4),   # Suv Emulsiya & Gruntovka, De Luxe
    "ДЕЛЮКС ЧЕРНЫЙ":         (17, 4),
    # ДекоАРТ Эмаль variants
    "ДекоАРТ Эмаль":         (1, 5),    # Bo'yoq & Emal, Dekoart
    "Декор Лак":             (10, 5),   # Laklar & Oliflar, Dekoart
    "Декор DEKOCENTO":       (14, 5),   # Qorishma & Suvoq, Dekoart
    # ДЕКОАРТ Универсал Эмульсия
    "ДЕКОАРТ Универсал":     (17, 5),   # Suv Emulsiya & Gruntovka, Dekoart
    "ДЕКОАРТ СТРОНГ ФАСАД":  (17, 5),
    "ДЕКОАРТ KF":            (17, 5),
    # ДЕКАСТАР
    "ДЕКАСТАР":              (17, 5),
    # Скоч Травертин
    "Скоч Травертин":        (15, 21),  # Qurilish Mollari, Oscar
    # АНТИМОРОЗ
    "АНТИМОРОЗ":             (3, 12),   # Boshqa Mahsulot, Gogle
    # Электрод MONOLIT
    "Электрод  MONOLIT":     (7, 36),   # Elektrodlar, Xitoy
    # Кафель
    "Кафель":                (3, 36),
    # ЛЕСКА-ЖИЛКА
    "ЛЕСКА-ЖИЛКА":           (3, 36),
    # СИЛКОАТ
    "СИЛКОАТ":               (14, 30),  # Qorishma & Suvoq, Silkcoat
}

_SORTED_PREFIXES = sorted(BRAND_FAMILY_MAP.keys(), key=len, reverse=True)


def classify_by_prefix(name_1c: str) -> Optional[Tuple[int, int]]:
    """Return (category_id, producer_id) by longest-prefix-first match, or None."""
    if not name_1c:
        return None
    upper = name_1c.upper()
    for prefix in _SORTED_PREFIXES:
        if upper.startswith(prefix.upper()):
            return BRAND_FAMILY_MAP[prefix]
    return None


def classify_or_default(
    conn,
    name_1c: str,
    default_cat_id: int,
    default_prod_id: int,
) -> Tuple[int, int, bool]:
    """Classify a 1C product name. Returns (cat_id, prod_id, auto_classified).

    auto_classified is True when a brand-prefix match succeeded AND the
    target category/producer IDs both exist; False when the caller's
    default (typically "Yangi mahsulotlar" + "Boshqa") is used.

    The FK existence check guards against fresh DBs where Rassvet_Master
    hasn't been imported yet — auto-classification silently degrades to
    the default rather than raising a foreign-key error.
    """
    hit = classify_by_prefix(name_1c)
    if hit is None:
        return default_cat_id, default_prod_id, False

    cat_id, prod_id = hit
    cat_ok = conn.execute(
        "SELECT 1 FROM categories WHERE id = ?", (cat_id,)
    ).fetchone()
    prod_ok = conn.execute(
        "SELECT 1 FROM producers WHERE id = ?", (prod_id,)
    ).fetchone()
    if cat_ok and prod_ok:
        return cat_id, prod_id, True
    return default_cat_id, default_prod_id, False
