"""Import products from the Catalog Clean sheet into SQLite.

Reads the 'Catalog Clean' sheet from the FINAL xlsx file.
Columns: A=Kategoriya, B=Ishlab chiqaruvchi, C=Mahsulot nomi,
         D=Og'irligi, E=Birlik, F=Narx UZS, G=Narx USD

Transliterates Cyrillic product/producer names to Latin for standardized display.
"""
import sys
import os
import re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from openpyxl import load_workbook
from backend.database import get_db, init_db


# ── Cyrillic → Latin transliteration table ──────────────────────
CYRILLIC_MAP = {
    'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E',
    'Ё': 'Yo', 'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K',
    'Л': 'L', 'М': 'M', 'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R',
    'С': 'S', 'Т': 'T', 'У': 'U', 'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts',
    'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Shch', 'Ъ': '', 'Ы': 'Y', 'Ь': '',
    'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya',
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e',
    'ё': 'yo', 'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k',
    'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r',
    'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts',
    'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '',
    'э': 'e', 'ю': 'yu', 'я': 'ya',
}


def transliterate(text):
    """Convert Cyrillic characters to Latin equivalents."""
    if not text:
        return text
    result = []
    for ch in text:
        if ch in CYRILLIC_MAP:
            result.append(CYRILLIC_MAP[ch])
        else:
            result.append(ch)
    return ''.join(result)


def generate_display_name(full_name, producer):
    """Create a shorter display name for mobile UI.

    1. Strip redundant brand/producer mentions (since producer is shown in nav)
    2. Transliterate Cyrillic -> Latin
    3. Truncate to <=35 chars if needed
    """
    name = full_name.strip()

    # First: strip producer name from the beginning (case-insensitive)
    if producer:
        producer_upper = producer.strip().upper()
        name_upper = name.upper()
        if name_upper.startswith(producer_upper):
            name = name[len(producer_upper):].strip()
            name = re.sub(r'^[\s\-\u2013\u2014/\\:,.]+', '', name)

    # Also try common brand prefixes that may differ from the producer name
    brand_prefixes = [
        '\u041f\u041e\u041b\u0418\u0421\u0410\u041d', '\u0421\u041e\u0411\u0421\u0410\u041d',
        '\u0421\u0418\u041b\u041a\u041e\u0410\u0422', '\u041f\u0410\u041b\u0418\u0416',
        '\u0425\u0410\u042f\u0422', '\u0412\u0415\u0411\u0415\u0420',
        '\u041d\u042e\u041c\u0418\u041a\u0421', '\u0421\u041e\u0423\u0414\u0410\u041b',
        '\u041e\u0421\u041a\u0410\u0420', '\u0413\u0410\u041c\u041c\u0410',
        '\u0414\u0415\u041b\u042e\u041a\u0421', '\u0414\u0415 \u041b\u042e\u041a\u0421',
        '\u0410\u041a\u0424\u0418\u041a\u0421',
        '\u0421\u041e\u041c\u041e \u0424\u0418\u041a\u0421',
        '\u041c\u0410\u0422\u0422\u0420\u041e\u0421',
        '\u0414\u0415\u041a\u041e\u0410\u0420\u0422',
        '\u0414\u0410\u0419\u0421\u041e\u041d',
        '\u041c\u0415\u0413\u0410\u041c\u0418\u041a\u0421',
        '\u0413\u0423\u0413\u041b\u0415', '\u0422\u0418\u0422\u0410\u041d',
        '\u041b\u0415\u041e\u041d',
    ]
    name_upper = name.upper()
    for prefix in brand_prefixes:
        if name_upper.startswith(prefix):
            name = name[len(prefix):].strip()
            name = re.sub(r'^[\s\-\u2013\u2014/\\:,.]+', '', name)
            break

    # Remove excessive quotes
    name = name.replace('"', '').replace("'", '')

    # Transliterate to Latin
    name = transliterate(name)

    # Clean up multiple spaces
    name = re.sub(r'\s+', ' ', name).strip()

    # If still too long, truncate at a sensible point
    if len(name) > 35:
        cut = name[:32].rfind(' ')
        if cut > 15:
            name = name[:cut] + '\u2026'
        else:
            name = name[:32] + '\u2026'

    return name if name else transliterate(full_name[:30])


def import_from_catalog_clean(xlsx_path: str):
    """Import all products from the Catalog Clean sheet."""
    init_db()
    conn = get_db()

    print(f"Loading {xlsx_path}...")
    wb = load_workbook(xlsx_path, read_only=True, data_only=True)

    if 'Catalog Clean' not in wb.sheetnames:
        print("ERROR: 'Catalog Clean' sheet not found!")
        return
    ws = wb['Catalog Clean']

    # Clear existing data
    conn.execute("DELETE FROM products")
    conn.execute("DELETE FROM producers")
    conn.execute("DELETE FROM categories")

    cat_map = {}   # name -> id
    prod_map = {}  # name -> id
    imported = 0
    skipped = 0

    rows = list(ws.iter_rows(min_row=2, values_only=False))
    print(f"Processing {len(rows)} rows...")

    for row in rows:
        category = row[0].value   # A
        producer = row[1].value   # B
        name = row[2].value       # C
        weight_val = row[3].value if len(row) > 3 else None  # D
        unit = row[4].value if len(row) > 4 else 'sht'       # E
        price_uzs = row[5].value if len(row) > 5 else None    # F
        price_usd = row[6].value if len(row) > 6 else None    # G

        if not category or not producer or not name:
            skipped += 1
            continue

        category = str(category).strip()
        producer = str(producer).strip()
        name = str(name).strip()
        unit = str(unit).strip() if unit else 'sht'

        # Transliterate producer name to Latin
        producer_latin = transliterate(producer)

        # Ensure category exists (categories are already in Latin/Uzbek)
        if category not in cat_map:
            conn.execute(
                "INSERT OR IGNORE INTO categories (name, sort_order) VALUES (?, ?)",
                (category, len(cat_map) + 1)
            )
            cat_id = conn.execute(
                "SELECT id FROM categories WHERE name = ?", (category,)
            ).fetchone()[0]
            cat_map[category] = cat_id

        # Ensure producer exists - use transliterated name
        if producer_latin not in prod_map:
            conn.execute(
                "INSERT OR IGNORE INTO producers (name) VALUES (?)",
                (producer_latin,)
            )
            prod_id = conn.execute(
                "SELECT id FROM producers WHERE name = ?", (producer_latin,)
            ).fetchone()[0]
            prod_map[producer_latin] = prod_id

        # Parse prices
        p_usd = 0
        p_uzs = 0
        try:
            if price_usd is not None and price_usd != '' and price_usd != 0:
                p_usd = float(price_usd)
        except (ValueError, TypeError):
            pass
        try:
            if price_uzs is not None and price_uzs != '' and price_uzs != 0:
                p_uzs = float(price_uzs)
        except (ValueError, TypeError):
            pass

        # Parse weight
        weight = None
        try:
            if weight_val is not None and weight_val != '':
                weight = float(weight_val)
        except (ValueError, TypeError):
            pass

        # Generate display name (producer-stripped, transliterated)
        display_name = generate_display_name(name, producer)

        # Full name also transliterated for search
        name_latin = transliterate(name)

        conn.execute(
            """INSERT INTO products
               (name, name_display, category_id, producer_id, unit,
                price_usd, price_uzs, weight, is_active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)""",
            (name_latin, display_name, cat_map[category], prod_map[producer_latin],
             unit, p_usd, p_uzs, weight)
        )
        imported += 1

    # Update denormalized counts
    conn.execute("""
        UPDATE categories SET product_count = (
            SELECT COUNT(*) FROM products WHERE products.category_id = categories.id AND is_active = 1
        )
    """)
    conn.execute("""
        UPDATE producers SET product_count = (
            SELECT COUNT(*) FROM products WHERE products.producer_id = producers.id AND is_active = 1
        )
    """)

    conn.commit()

    # Summary
    total_cats = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    total_prods = conn.execute("SELECT COUNT(*) FROM producers").fetchone()[0]
    total_products = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    usd_count = conn.execute("SELECT COUNT(*) FROM products WHERE price_usd > 0").fetchone()[0]
    uzs_count = conn.execute("SELECT COUNT(*) FROM products WHERE price_uzs > 0").fetchone()[0]

    print(f"\nImport complete:")
    print(f"  Categories: {total_cats}")
    print(f"  Producers: {total_prods}")
    print(f"  Products: {total_products} (USD: {usd_count}, UZS: {uzs_count})")
    print(f"  Skipped: {skipped}")

    # Category breakdown
    rows = conn.execute("""
        SELECT c.name, c.product_count
        FROM categories c ORDER BY c.product_count DESC
    """).fetchall()
    print("\n  Category breakdown:")
    for r in rows:
        print(f"    {r['product_count']:>4}  {r['name']}")

    # Top producers
    rows = conn.execute("""
        SELECT name, product_count FROM producers ORDER BY product_count DESC LIMIT 10
    """).fetchall()
    print("\n  Top 10 producers:")
    for r in rows:
        print(f"    {r['product_count']:>4}  {r['name']}")

    # Show sample display names
    rows = conn.execute("""
        SELECT p.name, p.name_display, pr.name as producer_name
        FROM products p JOIN producers pr ON p.producer_id = pr.id
        LIMIT 15
    """).fetchall()
    print("\n  Sample display names:")
    for r in rows:
        print(f"    [{r['producer_name']}] {r['name'][:40]} -> {r['name_display']}")

    conn.close()
    wb.close()


if __name__ == "__main__":
    # Check multiple possible locations
    candidates = [
        os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'products.xlsx'),
        os.path.join(os.path.dirname(__file__), '..', '..', '..', '09.03.26 List of products (Inventory) - FINAL.xlsx'),
        '/sessions/clever-vibrant-hamilton/mnt/Catalogue:Telegram app/09.03.26 List of products (Inventory) - FINAL.xlsx',
        './data/products.xlsx',
    ]
    xlsx_path = None
    for c in candidates:
        if os.path.exists(c):
            xlsx_path = os.path.abspath(c)
            break
    if xlsx_path:
        import_from_catalog_clean(xlsx_path)
    else:
        print("ERROR: No xlsx file found. Checked:", [os.path.abspath(c) for c in candidates])
        sys.exit(1)
