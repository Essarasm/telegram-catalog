import sqlite3
import os

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")

# Ensure the data directory exists (needed for Railway volume mount)
os.makedirs(os.path.dirname(DATABASE_PATH) or ".", exist_ok=True)


def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            name_uz TEXT,
            sort_order INTEGER DEFAULT 0,
            product_count INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS producers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            product_count INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            name_display TEXT,
            category_id INTEGER NOT NULL,
            producer_id INTEGER NOT NULL,
            unit TEXT DEFAULT 'sht',
            price_usd REAL DEFAULT 0,
            price_uzs REAL DEFAULT 0,
            weight REAL,
            image_path TEXT,
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY (category_id) REFERENCES categories(id),
            FOREIGN KEY (producer_id) REFERENCES producers(id)
        );

        CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id);
        CREATE INDEX IF NOT EXISTS idx_products_producer ON products(producer_id);
        CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);
        CREATE INDEX IF NOT EXISTS idx_products_active ON products(is_active);
        CREATE INDEX IF NOT EXISTS idx_products_cat_prod ON products(category_id, producer_id);

        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            phone TEXT,
            first_name TEXT,
            last_name TEXT,
            username TEXT,
            latitude REAL,
            longitude REAL,
            registered_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS allowed_clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_normalized TEXT NOT NULL,
            name TEXT,
            location TEXT,
            source_sheet TEXT,
            client_id_1c TEXT,
            company_name TEXT,
            status TEXT DEFAULT 'active',
            matched_telegram_id INTEGER,
            credit_score INTEGER,
            credit_limit REAL,
            notes TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_allowed_phone ON allowed_clients(phone_normalized);

        CREATE TABLE IF NOT EXISTS cart_items (
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (user_id, product_id)
        );
        CREATE INDEX IF NOT EXISTS idx_cart_user ON cart_items(user_id);

        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            telegram_id INTEGER NOT NULL,
            report_type TEXT NOT NULL DEFAULT 'other',
            note TEXT,
            status TEXT NOT NULL DEFAULT 'new',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
        CREATE INDEX IF NOT EXISTS idx_reports_product ON reports(product_id);
        CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status);

        CREATE TABLE IF NOT EXISTS product_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            request_text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_product_requests_status ON product_requests(status);

        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            client_name TEXT,
            client_phone TEXT,
            total_usd REAL DEFAULT 0,
            total_uzs REAL DEFAULT 0,
            item_count INTEGER DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'submitted',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_orders_telegram ON orders(telegram_id);
        CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);

        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER,
            product_name TEXT NOT NULL,
            producer_name TEXT,
            quantity INTEGER NOT NULL,
            unit TEXT,
            price REAL DEFAULT 0,
            currency TEXT DEFAULT 'USD',
            FOREIGN KEY (order_id) REFERENCES orders(id)
        );
        CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);

        -- Search analytics: log every search query
        CREATE TABLE IF NOT EXISTS search_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            query TEXT NOT NULL,
            results_count INTEGER DEFAULT 0,
            category_id INTEGER,
            producer_id INTEGER,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_search_logs_query ON search_logs(query);
        CREATE INDEX IF NOT EXISTS idx_search_logs_user ON search_logs(telegram_id);
        CREATE INDEX IF NOT EXISTS idx_search_logs_created ON search_logs(created_at);
        CREATE INDEX IF NOT EXISTS idx_search_logs_results ON search_logs(results_count);

        -- Search analytics: track product clicks from search results
        CREATE TABLE IF NOT EXISTS search_clicks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_log_id INTEGER,
            telegram_id INTEGER,
            product_id INTEGER NOT NULL,
            action TEXT DEFAULT 'click',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (search_log_id) REFERENCES search_logs(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
        CREATE INDEX IF NOT EXISTS idx_search_clicks_search ON search_clicks(search_log_id);
        CREATE INDEX IF NOT EXISTS idx_search_clicks_product ON search_clicks(product_id);
        CREATE INDEX IF NOT EXISTS idx_search_clicks_action ON search_clicks(action);
    """)
    conn.commit()

    # Migrations: add columns if missing (safe for existing DBs)
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    for col, coltype in [("latitude", "REAL"), ("longitude", "REAL"),
                          ("is_approved", "INTEGER DEFAULT 0"),
                          ("client_id", "INTEGER")]:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} {coltype}")

    # Migration: add client_id_1c and company_name to allowed_clients
    ac_cols = {row[1] for row in conn.execute("PRAGMA table_info(allowed_clients)").fetchall()}
    for col, coltype in [("client_id_1c", "TEXT"), ("company_name", "TEXT")]:
        if col not in ac_cols:
            conn.execute(f"ALTER TABLE allowed_clients ADD COLUMN {col} {coltype}")

    # Create index on client_id_1c (after migration ensures column exists)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_allowed_1c ON allowed_clients(client_id_1c)")

    # Migration: add stock columns to products
    prod_cols = {row[1] for row in conn.execute("PRAGMA table_info(products)").fetchall()}
    for col, coltype in [
        ("stock_quantity", "REAL DEFAULT NULL"),
        ("stock_status", "TEXT DEFAULT NULL"),
        ("stock_updated_at", "TEXT DEFAULT NULL"),
    ]:
        if col not in prod_cols:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {coltype}")
            prod_cols.add(col)

    # Migration: add search_text column to products (cross-language search index)
    if "search_text" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN search_text TEXT DEFAULT ''")
        # Populate search_text for existing products
        _rebuild_search_text(conn)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_search_text ON products(search_text)")

    conn.commit()
    conn.close()


# ── Cyrillic ↔ Latin transliteration for search ─────────────────

_CYR2LAT = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e',
    'ё': 'yo', 'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k',
    'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r',
    'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts',
    'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '',
    'э': 'e', 'ю': 'yu', 'я': 'ya',
}


def transliterate_to_latin(text):
    """Transliterate Cyrillic text to Latin (lowercase)."""
    result = []
    for ch in text.lower():
        result.append(_CYR2LAT.get(ch, ch))
    return ''.join(result)


def build_search_text(name_cyrillic, name_display, producer_name):
    """Build a combined search index string for a product.

    Combines: original 1C name (Cyrillic) + display name (Latin) +
    transliterated version of Cyrillic + producer name.
    All lowercased for case-insensitive LIKE matching.
    """
    parts = []
    if name_cyrillic:
        cyr = name_cyrillic.strip().lower()
        parts.append(cyr)
        parts.append(transliterate_to_latin(cyr))
    if name_display:
        parts.append(name_display.strip().lower())
    if producer_name:
        parts.append(producer_name.strip().lower())
    return ' '.join(parts)


def _rebuild_search_text(conn):
    """Populate search_text for all products (used during migration)."""
    rows = conn.execute(
        """SELECT p.id, p.name, p.name_display, pr.name as producer_name
           FROM products p
           JOIN producers pr ON pr.id = p.producer_id"""
    ).fetchall()
    for r in rows:
        st = build_search_text(r["name"], r["name_display"], r["producer_name"])
        conn.execute("UPDATE products SET search_text = ? WHERE id = ?", (st, r["id"]))
    conn.commit()


if __name__ == "__main__":
    init_db()
    print("Database initialized.")
