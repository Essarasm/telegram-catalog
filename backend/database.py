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
    """)
    conn.commit()

    # Migrations: add columns if missing (safe for existing DBs)
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    for col, coltype in [("latitude", "REAL"), ("longitude", "REAL"),
                          ("is_approved", "INTEGER DEFAULT 0"),
                          ("client_id", "INTEGER")]:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} {coltype}")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print("Database initialized.")
