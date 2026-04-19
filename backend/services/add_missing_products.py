"""Add missing products from unmatched import names to the catalog.

For each unmatched name:
1. Auto-detect category from name patterns (Кафель → tiles, Декор покр → coatings)
2. Auto-detect producer from known brand prefixes
3. Generate Latin display name via transliteration + cleanup
4. Insert into products table
5. Mark as resolved in unmatched_import_names
6. Add to product_aliases
"""
import re
import logging
import unicodedata

from backend.database import get_db, transliterate_to_latin, build_search_text

logger = logging.getLogger(__name__)

# Category detection from 1C name patterns
_CATEGORY_PATTERNS = [
    (r'^кафель\b', 'Qurilish Mollari'),
    (r'^линолеум\b', 'Linoleum'),
    (r'^электрод\b', 'Elektrodlar'),
    (r'^плинтус\b|^заглушка\s+плинтус|^соед\.?\s+плинтус|^угол\s+плинтус', 'Plintus va Aksessuarlar'),
    (r'^пленка\b', 'Plyonka'),
    (r'^саморез\b|^дюбель\b|^гвоздь\b|^шуруп\b', 'Samorez & Mix'),
    (r'^проволк|^сим\b', 'Mix & Sim'),
    (r'^декор\s+покр|^декор\s+лак|^штукатурк', 'Qorishma & Suvoq'),
    (r'^грунт|^праймер|^в/э\b|^водоэмульс|^эмульсия', 'Suv emulsiya va Gruntovka'),
    (r'^эмаль\b|^краск|^пф-\d|^нц\b', "Bo'yoq & Emal"),
    (r'^лак\b|^олиф', 'LAKLAR VA OLIFLAR'),
    (r'^клей\b|^елим|^yelim', 'Yelim'),
    (r'^герметик\b|^силикон', 'Germetik'),
    (r'^пена\b|^монт.*пена', 'Montaj penasi'),
    (r'^растворител|^ацетон|^уайт', 'Eritgich'),
    (r'^коллер|^колер', 'Koller'),
    (r'^бурч|^профил|^уголок', 'Burchak va Profil'),
    (r'^наждач|^шлиф|^шкурк', 'Terilar, silliqlash kamarlari'),
]

# Producer detection from known 1C brand prefixes
_PRODUCER_PATTERNS = [
    (r'\bвэбер\b|\bweber\b', 'Weber'),
    (r'\bхаят\b|\bhayat\b', 'Hayat'),
    (r'\bгамма\b|\bgamma\b', 'Gamma'),
    (r'\bделюкс\b|\bdelux\b', 'De Luxe'),
    (r'\bдекозар\b|\bdekozar\b', 'Dekozar'),
    (r'\bдекоарт\b|\bdekoart\b', 'Dekoart'),
    (r'\bгугле\b|\bgogle\b', 'Gogle'),
    (r'\bакфикс\b|\bakfix\b', 'Akfix'),
    (r'\bидеал\b|\bideal\b', 'Ideal'),
    (r'\bсомофикс\b|\bsomofix\b', 'Somofix'),
    (r'\bоптима\b|\boptima\b', 'Optima'),
    (r'\bмегамикс\b|\bmegamix\b', 'Megamix'),
    (r'\bсветомикс\b', 'Svetomix'),
    (r'\bсемикс\b|\bcemix\b', 'Cemix'),
    (r'\bэлерон\b|\beleron\b', 'Eleron'),
    (r'\bосмо\b|\bosmo\b', 'Osmo'),
    (r'\basmaco\b|\bасмако\b', 'Asmaco'),
    (r'\barsenal\b|\bарсенал\b', 'Arsenal'),
    (r'\boliver\b|\bоливер\b', 'Oliver'),
    (r'\bduafix\b|\bдуафикс\b', 'Duafix'),
    (r'\bformula\b|\bформула\b', 'Formula'),
    (r'\bsilkoat\b|\bсилкоат\b', 'Silkoat'),
    (r'\bpolisan\b|\bполисан\b', 'Polisan'),
    (r'\bпуфа\b|\bpufa\b', 'Pufa'),
    (r'\bосмо\b', 'Osmo'),
    (r'\btravertin\b|\bтравертин\b', 'Dekoart'),
    (r'\bвершина\b', 'Vershina'),
    (r'\bэкос\b', 'Ekos'),
    (r'\bбумеранг\b', 'Bumerang'),
    (r'\boscar\b|\bоскар\b', 'Oscar'),
]


def _detect_category(name_1c):
    """Detect category from 1C product name."""
    lower = name_1c.strip().lower()
    for pattern, category in _CATEGORY_PATTERNS:
        if re.search(pattern, lower):
            return category
    return 'Boshqa Mahsulot'


def _detect_producer(name_1c):
    """Detect producer from known brand patterns in the name."""
    lower = name_1c.strip().lower()
    for pattern, producer in _PRODUCER_PATTERNS:
        if re.search(pattern, lower):
            return producer
    return None


def _make_latin_name(name_1c):
    """Generate a Latin display name from 1C Cyrillic name.

    Steps:
    1. Remove producer prefix (if detected)
    2. Transliterate remaining Cyrillic to Latin
    3. Title-case the result
    4. Keep numbers, slashes, size codes as-is
    """
    name = name_1c.strip()

    # Remove known producer prefixes from the display name
    producer = _detect_producer(name)
    if producer:
        for pattern, _ in _PRODUCER_PATTERNS:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE).strip()

    # Transliterate Cyrillic parts, keep Latin/numbers as-is
    result = []
    for ch in name:
        if '\u0400' <= ch <= '\u04ff':
            result.append(transliterate_to_latin(ch))
        else:
            result.append(ch)
    latin = ''.join(result)

    # Clean up: collapse spaces, strip leading/trailing junk
    latin = re.sub(r'\s+', ' ', latin).strip()
    latin = re.sub(r'^[\s\-/]+', '', latin)

    # Title case but preserve size codes like /30*30/ and units like /3 кг/
    words = latin.split()
    titled = []
    for w in words:
        if w.startswith('/') or w.startswith('(') or re.match(r'^\d', w):
            titled.append(w)
        else:
            titled.append(w.capitalize())
    return ' '.join(titled)


def add_missing_from_unmatched(conn=None):
    """Add all unresolved unmatched names as new products. Returns summary."""
    own_conn = conn is None
    if own_conn:
        conn = get_db()

    try:
        unmatched = conn.execute(
            "SELECT id, name FROM unmatched_import_names WHERE resolved = 0 ORDER BY name"
        ).fetchall()

        if not unmatched:
            return {"ok": True, "added": 0, "message": "No unmatched names to add"}

        # Build category/producer ID lookups
        cat_rows = conn.execute("SELECT id, name FROM categories").fetchall()
        cat_by_name = {r["name"]: r["id"] for r in cat_rows}

        prod_rows = conn.execute("SELECT id, name FROM producers").fetchall()
        prod_by_name = {r["name"]: r["id"] for r in prod_rows}

        added = []
        for row in unmatched:
            name_1c = row["name"].strip()

            # Detect metadata
            category_name = _detect_category(name_1c)
            producer_name = _detect_producer(name_1c)
            latin_name = _make_latin_name(name_1c)

            # Resolve category ID
            cat_id = cat_by_name.get(category_name)
            if not cat_id:
                cat_id = cat_by_name.get('Boshqa Mahsulot', 3)

            # Resolve producer ID (create if new)
            prod_id = None
            if producer_name:
                prod_id = prod_by_name.get(producer_name)
                if not prod_id:
                    conn.execute(
                        "INSERT OR IGNORE INTO producers (name, product_count) VALUES (?, 0)",
                        (producer_name,),
                    )
                    p = conn.execute("SELECT id FROM producers WHERE name = ?", (producer_name,)).fetchone()
                    if p:
                        prod_id = p["id"]
                        prod_by_name[producer_name] = prod_id

            if not prod_id:
                # Use "Boshqa" or first available
                prod_id = prod_by_name.get('Boshqa') or list(prod_by_name.values())[0]

            # Extract weight from name if possible
            weight = None
            wm = re.search(r'/(\d+[\.,]?\d*)\s*(?:кг|kg)/i', name_1c, re.IGNORECASE)
            if wm:
                weight = float(wm.group(1).replace(',', '.'))

            # Extract unit
            unit = "шт"
            if re.search(r'кг|kg', name_1c, re.IGNORECASE):
                unit = "кг"
            elif re.search(r'л\b|литр', name_1c, re.IGNORECASE):
                unit = "л"
            elif re.search(r'бочк', name_1c, re.IGNORECASE):
                unit = "бочка"
            elif re.search(r'рулон', name_1c, re.IGNORECASE):
                unit = "рулон"

            # Build search text
            search_text = build_search_text(name_1c, latin_name, producer_name, unit, category_name)

            # Insert product
            conn.execute(
                """INSERT INTO products (name, name_display, category_id, producer_id,
                                         unit, weight, search_text, is_active)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
                (name_1c, latin_name, cat_id, prod_id, unit, weight, search_text),
            )
            new_pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            # Add alias
            alias_lower = unicodedata.normalize("NFC", name_1c.strip().lower())
            conn.execute(
                "INSERT OR IGNORE INTO product_aliases (alias_name, alias_name_lower, product_id, source) "
                "VALUES (?, ?, ?, 'add_missing')",
                (name_1c, alias_lower, new_pid),
            )

            # Mark resolved
            conn.execute(
                "UPDATE unmatched_import_names SET resolved = 1, resolved_product_id = ?, resolved_at = datetime('now') WHERE id = ?",
                (new_pid, row["id"]),
            )

            added.append({
                "id": new_pid,
                "name_1c": name_1c[:50],
                "name_latin": latin_name[:50],
                "category": category_name,
                "producer": producer_name or "Boshqa",
            })

        # Update producer product counts
        conn.execute("""
            UPDATE producers SET product_count = (
                SELECT COUNT(*) FROM products WHERE producer_id = producers.id AND is_active = 1
            )
        """)

        conn.commit()

        return {
            "ok": True,
            "added": len(added),
            "products": added,
        }
    finally:
        if own_conn:
            conn.close()
