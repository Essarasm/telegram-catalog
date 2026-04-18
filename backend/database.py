import sqlite3
import os

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/catalog.db")

# Ensure the data directory exists (needed for Railway volume mount)
os.makedirs(os.path.dirname(DATABASE_PATH) or ".", exist_ok=True)


class _DictRow(dict):
    """Dict-like row that supports BOTH r["name"] and r[0] access.
    Also supports .get() (unlike sqlite3.Row). Eliminates the
    SQLITE_ROW_NO_GET bug class while keeping integer-index compat."""
    __slots__ = ('_values',)
    def __init__(self, cursor, row):
        cols = [col[0] for col in cursor.description]
        super().__init__(zip(cols, row))
        self._values = row
    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)


def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = _DictRow
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_sibling_client_ids(conn, client_id):
    """Return all allowed_clients.id values sharing the same client_id_1c.

    One real-world client (shop) can have up to 5 phone registrations
    (owner, relatives, workers). Each phone gets its own allowed_clients row,
    but they all share the same client_id_1c (1C name). Financial data may be
    linked to any one of these IDs.

    This function resolves all sibling IDs so that regardless of which phone
    a user registered with, they see the full financial picture.

    Returns a list of IDs (always includes the input client_id).
    If client_id_1c is NULL or the client doesn't exist, returns [client_id].
    """
    if not client_id:
        return []
    row = conn.execute(
        "SELECT client_id_1c FROM allowed_clients WHERE id = ?", (client_id,)
    ).fetchone()
    if not row or not row["client_id_1c"]:
        return [client_id]
    siblings = conn.execute(
        "SELECT id FROM allowed_clients WHERE client_id_1c = ? AND COALESCE(status, 'active') != 'merged'",
        (row["client_id_1c"],),
    ).fetchall()
    ids = [s["id"] for s in siblings]
    if client_id not in ids:
        ids.append(client_id)
    return ids


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

        -- Financial data: client balance snapshots from 1C оборотно-сальдовая
        CREATE TABLE IF NOT EXISTS client_balances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name_1c TEXT NOT NULL,
            client_id INTEGER,
            currency TEXT NOT NULL DEFAULT 'UZS',
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            opening_debit REAL DEFAULT 0,
            opening_credit REAL DEFAULT 0,
            period_debit REAL DEFAULT 0,
            period_credit REAL DEFAULT 0,
            closing_debit REAL DEFAULT 0,
            closing_credit REAL DEFAULT 0,
            imported_at TEXT DEFAULT (datetime('now')),
            UNIQUE(client_name_1c, period_start, currency)
        );
        CREATE INDEX IF NOT EXISTS idx_cb_client_name ON client_balances(client_name_1c);
        CREATE INDEX IF NOT EXISTS idx_cb_client_id ON client_balances(client_id);
        CREATE INDEX IF NOT EXISTS idx_cb_period ON client_balances(period_start);
        CREATE INDEX IF NOT EXISTS idx_cb_currency ON client_balances(currency);

        -- Debtors snapshot: current debt per client from 1C "Дебиторская задолженность"
        CREATE TABLE IF NOT EXISTS client_debts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name_1c TEXT NOT NULL,
            client_id INTEGER,
            debt_uzs REAL DEFAULT 0,
            debt_usd REAL DEFAULT 0,
            last_transaction_date TEXT,
            last_transaction_no TEXT,
            aging_0_30 REAL DEFAULT 0,
            aging_31_60 REAL DEFAULT 0,
            aging_61_90 REAL DEFAULT 0,
            aging_91_120 REAL DEFAULT 0,
            aging_120_plus REAL DEFAULT 0,
            report_date TEXT NOT NULL,
            imported_at DATETIME DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_client_debts_client_id ON client_debts(client_id);
        CREATE INDEX IF NOT EXISTS idx_client_debts_report_date ON client_debts(report_date);

        -- Demand signals: track orders on out-of-stock products
        CREATE TABLE IF NOT EXISTS demand_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            order_item_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            telegram_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            stock_status_at_order TEXT NOT NULL DEFAULT 'out_of_stock',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
        CREATE INDEX IF NOT EXISTS idx_demand_signals_product ON demand_signals(product_id);
        CREATE INDEX IF NOT EXISTS idx_demand_signals_created ON demand_signals(created_at);

        -- Real orders from 1C "Реализация товаров" (actual shipments / invoices)
        -- Separate from `orders` (wish-list orders submitted via the app).
        CREATE TABLE IF NOT EXISTS real_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_number_1c TEXT NOT NULL UNIQUE,
            doc_date TEXT NOT NULL,
            doc_time TEXT,
            client_name_1c TEXT NOT NULL,
            client_id INTEGER,
            contract TEXT,
            storage_location TEXT,
            payment_account TEXT,
            sale_agent TEXT,
            responsible_person TEXT,
            comment TEXT,
            currency TEXT DEFAULT 'UZS',
            exchange_rate REAL DEFAULT 1,
            total_sum REAL DEFAULT 0,
            total_sum_currency REAL DEFAULT 0,
            total_weight REAL DEFAULT 0,
            item_count INTEGER DEFAULT 0,
            imported_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_real_orders_client_id ON real_orders(client_id);
        CREATE INDEX IF NOT EXISTS idx_real_orders_client_name ON real_orders(client_name_1c);
        CREATE INDEX IF NOT EXISTS idx_real_orders_doc_date ON real_orders(doc_date);
        CREATE INDEX IF NOT EXISTS idx_real_orders_doc_number ON real_orders(doc_number_1c);

        CREATE TABLE IF NOT EXISTS real_order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            real_order_id INTEGER NOT NULL,
            line_no INTEGER,
            product_name_1c TEXT NOT NULL,
            product_id INTEGER,
            quantity REAL DEFAULT 0,
            price REAL DEFAULT 0,
            sum_local REAL DEFAULT 0,
            vat REAL DEFAULT 0,
            total_local REAL DEFAULT 0,
            price_currency REAL DEFAULT 0,
            sum_currency REAL DEFAULT 0,
            total_currency REAL DEFAULT 0,
            cost REAL DEFAULT 0,
            total_cost REAL DEFAULT 0,
            stock_remainder REAL DEFAULT 0,
            storage_location TEXT,
            weight_per_unit REAL DEFAULT 0,
            total_weight REAL DEFAULT 0,
            FOREIGN KEY (real_order_id) REFERENCES real_orders(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_real_order_items_order ON real_order_items(real_order_id);
        CREATE INDEX IF NOT EXISTS idx_real_order_items_product ON real_order_items(product_id);
        CREATE INDEX IF NOT EXISTS idx_real_order_items_name ON real_order_items(product_name_1c);

        -- ─────────────────────────────────────────────────────────────
        -- Session F renewal: Daily Upload Checklist system
        -- ─────────────────────────────────────────────────────────────

        -- Checklist schedule: one row per upload type (8 rows seeded).
        CREATE TABLE IF NOT EXISTS daily_upload_schedule (
            upload_type TEXT PRIMARY KEY,
            display_name_ru TEXT NOT NULL,
            display_name_uz TEXT NOT NULL,
            command TEXT NOT NULL,
            expected_count_per_day INTEGER DEFAULT 1,
            required_weekdays TEXT DEFAULT '1,2,3,4,5,6',
            sort_order INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        );

        -- Per-day tracking row for each upload type.
        -- Status: 'pending' | 'done' | 'failed' | 'skipped'
        -- (done-late is intentionally NOT included in v1; deferred.)
        CREATE TABLE IF NOT EXISTS daily_uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_date TEXT NOT NULL,
            upload_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            actual_count INTEGER DEFAULT 0,
            uploaded_at TEXT,
            uploaded_by_user_id INTEGER,
            uploaded_by_name TEXT,
            row_count INTEGER DEFAULT 0,
            file_names TEXT,
            skip_reason TEXT,
            notes TEXT,
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(upload_date, upload_type)
        );
        CREATE INDEX IF NOT EXISTS idx_daily_uploads_date ON daily_uploads(upload_date);
        CREATE INDEX IF NOT EXISTS idx_daily_uploads_type ON daily_uploads(upload_type);
        CREATE INDEX IF NOT EXISTS idx_daily_uploads_status ON daily_uploads(status);

        -- Manually-entered daily FX rate for Касса + reporting normalization.
        CREATE TABLE IF NOT EXISTS daily_fx_rates (
            rate_date TEXT NOT NULL,
            currency_pair TEXT NOT NULL DEFAULT 'USD_UZS',
            rate REAL NOT NULL,
            source TEXT DEFAULT 'manual',
            uploaded_by_user_id INTEGER,
            uploaded_by_name TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (rate_date, currency_pair)
        );

        -- Holidays (manual entry only, empty seed).
        CREATE TABLE IF NOT EXISTS holidays (
            holiday_date TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            added_by_user_id INTEGER,
            added_at TEXT DEFAULT (datetime('now'))
        );

        -- Касса (cash) payments — 1C "Приходный кассовый ордер" journal.
        -- Built in Session F renewal; Session G will layer credit scoring on top.
        CREATE TABLE IF NOT EXISTS client_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_number_1c TEXT NOT NULL UNIQUE,
            doc_date TEXT NOT NULL,
            doc_time TEXT,
            author TEXT,
            received_from TEXT,            -- Принято от (physical deliverer)
            basis TEXT,                    -- Основание
            attachment TEXT,               -- Приложение
            corr_account TEXT,             -- Корреспондирующий счет (40.10 / 40.11)
            client_name_1c TEXT,           -- Субконто1 (actual client receivable)
            client_id INTEGER,
            subconto2 TEXT,
            subconto3 TEXT,
            currency TEXT DEFAULT 'UZS',   -- 'UZS' if acc=40.10, 'USD' if 40.11
            amount_local REAL DEFAULT 0,   -- Сумма (UZS column regardless of currency)
            amount_currency REAL DEFAULT 0,-- ВалСумма (USD amount for 40.11)
            fx_rate REAL DEFAULT 0,        -- Курс at time of receipt
            cashflow_category TEXT,        -- Движение денежных средств
            imported_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_client_payments_doc_date ON client_payments(doc_date);
        CREATE INDEX IF NOT EXISTS idx_client_payments_client_name ON client_payments(client_name_1c);
        CREATE INDEX IF NOT EXISTS idx_client_payments_client_id ON client_payments(client_id);
        CREATE INDEX IF NOT EXISTS idx_client_payments_currency ON client_payments(currency);

        -- ─────────────────────────────────────────────────────────────
        -- Session G: Credit scoring engine
        -- ─────────────────────────────────────────────────────────────

        -- Nightly scoring snapshots — one row per client per recalc date.
        CREATE TABLE IF NOT EXISTS client_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            client_name TEXT NOT NULL,
            score INTEGER NOT NULL DEFAULT 0,
            tier TEXT NOT NULL DEFAULT 'Yangi',
            volume_bucket TEXT NOT NULL DEFAULT 'Micro',
            monthly_volume_usd REAL DEFAULT 0,
            credit_limit_uzs REAL DEFAULT 0,
            -- per-factor scores
            discipline_score REAL DEFAULT 0,
            debt_score REAL DEFAULT 0,
            consistency_score REAL DEFAULT 0,
            tenure_score REAL DEFAULT 0,
            -- underlying metrics (for /clientscore display)
            on_time_rate REAL DEFAULT 0,
            debt_ratio REAL DEFAULT 0,
            consistency_cv REAL DEFAULT 0,
            tenure_months REAL DEFAULT 0,
            -- metadata
            recalc_date TEXT NOT NULL,
            recalc_time TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(client_id, recalc_date)
        );
        CREATE INDEX IF NOT EXISTS idx_client_scores_client_id ON client_scores(client_id);
        CREATE INDEX IF NOT EXISTS idx_client_scores_recalc_date ON client_scores(recalc_date);
        CREATE INDEX IF NOT EXISTS idx_client_scores_score ON client_scores(score);

        -- ─────────────────────────────────────────────────────────────
        -- Session F follow-up: Supply & Returns ingestion pipeline
        -- ─────────────────────────────────────────────────────────────

        -- Supply orders from 1C "Поступление товаров" (warehouse receipts + client returns)
        CREATE TABLE IF NOT EXISTS supply_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_number TEXT NOT NULL,
            doc_date TEXT NOT NULL,
            doc_time TEXT,
            author TEXT,
            counterparty_name TEXT NOT NULL,
            doc_type TEXT NOT NULL DEFAULT 'supply',
            contract TEXT,
            counterparty_account TEXT,
            warehouse TEXT,
            vat_rate TEXT,
            receipt_type TEXT,
            supplier_advance REAL DEFAULT 0,
            supplier_advance_offset REAL DEFAULT 0,
            invoice_ref TEXT,
            responsible_person TEXT,
            exchange_rate REAL DEFAULT 1,
            currency TEXT DEFAULT 'UZS',
            total_sum REAL DEFAULT 0,
            total_sum_currency REAL DEFAULT 0,
            item_count INTEGER DEFAULT 0,
            source_file TEXT,
            imported_at TEXT DEFAULT (datetime('now')),
            UNIQUE(doc_number, doc_date)
        );
        CREATE INDEX IF NOT EXISTS idx_supply_orders_doc_date ON supply_orders(doc_date);
        CREATE INDEX IF NOT EXISTS idx_supply_orders_counterparty ON supply_orders(counterparty_name);
        CREATE INDEX IF NOT EXISTS idx_supply_orders_doc_type ON supply_orders(doc_type);
        CREATE INDEX IF NOT EXISTS idx_supply_orders_warehouse ON supply_orders(warehouse);

        CREATE TABLE IF NOT EXISTS supply_order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supply_order_id INTEGER NOT NULL,
            line_no INTEGER,
            product_name_raw TEXT NOT NULL,
            matched_product_id INTEGER,
            quantity REAL DEFAULT 0,
            price REAL DEFAULT 0,
            sum_local REAL DEFAULT 0,
            vat REAL DEFAULT 0,
            total_local REAL DEFAULT 0,
            base_price REAL DEFAULT 0,
            markup_pct REAL DEFAULT 0,
            markup_sum REAL DEFAULT 0,
            excise_pct REAL DEFAULT 0,
            excise_sum REAL DEFAULT 0,
            sum_currency REAL DEFAULT 0,
            price_currency REAL DEFAULT 0,
            unit TEXT,
            FOREIGN KEY (supply_order_id) REFERENCES supply_orders(id) ON DELETE CASCADE,
            UNIQUE(supply_order_id, line_no)
        );
        CREATE INDEX IF NOT EXISTS idx_supply_order_items_order ON supply_order_items(supply_order_id);
        CREATE INDEX IF NOT EXISTS idx_supply_order_items_product ON supply_order_items(matched_product_id);

        -- ─────────────────────────────────────────────────────────────
        -- Session M: Location hierarchy for delivery logistics
        -- Single table with type + parent_id for Viloyat → District → Mo'ljal
        -- ─────────────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL CHECK(type IN ('viloyat', 'district', 'moljal')),
            parent_id INTEGER,
            client_count INTEGER DEFAULT 0,
            sort_order INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY (parent_id) REFERENCES locations(id),
            UNIQUE(name, type, parent_id)
        );
        CREATE INDEX IF NOT EXISTS idx_locations_type ON locations(type);
        CREATE INDEX IF NOT EXISTS idx_locations_parent ON locations(parent_id);
        CREATE INDEX IF NOT EXISTS idx_locations_active ON locations(is_active);
    """)
    conn.commit()

    # Seed daily_upload_schedule with the 8 checklist items (idempotent).
    _seed_daily_upload_schedule(conn)

    # Seed location hierarchy (idempotent).
    _seed_locations(conn)

    # Migration: add delivery_type to orders
    order_cols = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
    if "delivery_type" not in order_cols:
        conn.execute("ALTER TABLE orders ADD COLUMN delivery_type TEXT DEFAULT 'delivery'")

    # Migration: create order_feedback table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            user_id INTEGER,
            feedback_text TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (user_id) REFERENCES users(telegram_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_order_feedback_order ON order_feedback(order_id)")

    # Migration: create unmatched_registrations table (for pending review of unlinked users)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS unmatched_registrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL UNIQUE,
            phone TEXT,
            first_name TEXT,
            last_name TEXT,
            username TEXT,
            notification_message_id INTEGER,
            status TEXT DEFAULT 'pending',
            linked_client_name TEXT,
            resolved_at TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_unmatched_status ON unmatched_registrations(status)")

    # Migrations: add columns if missing (safe for existing DBs)
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    for col, coltype in [("latitude", "REAL"), ("longitude", "REAL"),
                          ("is_approved", "INTEGER DEFAULT 0"),
                          ("client_id", "INTEGER"),
                          ("location_address", "TEXT"),
                          ("location_updated", "TEXT"),
                          ("location_region", "TEXT"),
                          ("location_district", "TEXT"),
                          ("location_set_by_tg_id", "INTEGER"),
                          ("location_set_by_name", "TEXT"),
                          ("location_set_by_role", "TEXT"),
                          ("dismiss_status", "TEXT")]:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} {coltype}")

    # Migration: add client_id_1c and company_name to allowed_clients
    ac_cols = {row[1] for row in conn.execute("PRAGMA table_info(allowed_clients)").fetchall()}
    for col, coltype in [("client_id_1c", "TEXT"), ("company_name", "TEXT"),
                          ("location_district_id", "INTEGER"), ("location_moljal_id", "INTEGER")]:
        if col not in ac_cols:
            conn.execute(f"ALTER TABLE allowed_clients ADD COLUMN {col} {coltype}")

    # Create index on client_id_1c (after migration ensures column exists)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_allowed_1c ON allowed_clients(client_id_1c)")

    # Migration: add location columns to orders
    order_cols = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
    for col, coltype in [("location_district_id", "INTEGER"), ("location_moljal_id", "INTEGER"),
                          ("latitude", "REAL"), ("longitude", "REAL"), ("location_address", "TEXT")]:
        if col not in order_cols:
            conn.execute(f"ALTER TABLE orders ADD COLUMN {col} {coltype}")

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

    # Migration: add reminder_count_per_day to daily_upload_schedule.
    # Decoupled from expected_count_per_day: the checklist counts 1 upload
    # as "done", while the EOD reminder still nags if the operator skipped
    # the afternoon batch (e.g. cash: expected=1, reminder=2).
    sched_cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(daily_upload_schedule)").fetchall()}
    if "reminder_count_per_day" not in sched_cols:
        conn.execute(
            "ALTER TABLE daily_upload_schedule ADD COLUMN "
            "reminder_count_per_day INTEGER DEFAULT NULL"
        )
        # Seed: cash gets reminder_count=2 (afternoon nudge).
        conn.execute(
            "UPDATE daily_upload_schedule SET reminder_count_per_day = 2 "
            "WHERE upload_type = 'cash'"
        )

    # Migration: add client_id to orders so wish-list orders are scoped
    # to the correct client (fixes /testclient cross-contamination).
    order_cols2 = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
    if "client_id" not in order_cols2:
        conn.execute("ALTER TABLE orders ADD COLUMN client_id INTEGER")
        # Backfill: set client_id from users.client_id for each order's telegram_id
        conn.execute("""
            UPDATE orders SET client_id = (
                SELECT u.client_id FROM users u WHERE u.telegram_id = orders.telegram_id
            ) WHERE client_id IS NULL
        """)

    # Migration: track sales-group message ids + supplementary order link
    order_cols3 = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
    for col in ("sales_group_message_id", "sales_group_doc_message_id", "parent_order_id"):
        if col not in order_cols3:
            conn.execute(f"ALTER TABLE orders ADD COLUMN {col} INTEGER")

    # Manager-confirmed orders: the 1C-exported Excel that replaces the
    # wishlist order once sales managers + Uncle finalize it in 1C.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS confirmed_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wishlist_order_id INTEGER,
            file_name TEXT,
            telegram_file_id TEXT,
            confirmed_by_tg_id INTEGER,
            confirmed_by_name TEXT,
            total_uzs REAL DEFAULT 0,
            total_usd REAL DEFAULT 0,
            item_count INTEGER DEFAULT 0,
            items_json TEXT,
            doc_number_1c TEXT,
            doc_date TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_confirmed_orders_wishlist
        ON confirmed_orders(wishlist_order_id)
    """)

    # Agent flag + placed_by attribution. Enables the agent dashboard:
    #   is_agent=1 users see a daily/monthly stats card in their Cabinet
    #   orders.placed_by_telegram_id records who physically placed the
    #   order, even when /testclient impersonation is active (order.client_id
    #   stays the client's id; placed_by_telegram_id is the agent's tg id).
    user_cols_agent = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "is_agent" not in user_cols_agent:
        conn.execute("ALTER TABLE users ADD COLUMN is_agent INTEGER DEFAULT 0")
    order_cols4 = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
    if "placed_by_telegram_id" not in order_cols4:
        conn.execute("ALTER TABLE orders ADD COLUMN placed_by_telegram_id INTEGER")
        # Backfill: every existing order's placed_by = its telegram_id
        conn.execute(
            "UPDATE orders SET placed_by_telegram_id = telegram_id "
            "WHERE placed_by_telegram_id IS NULL"
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_orders_placed_by "
        "ON orders(placed_by_telegram_id)"
    )

    # Track when a product last had positive stock (for active product detection)
    prod_cols = {row[1] for row in conn.execute("PRAGMA table_info(products)").fetchall()}
    if "stock_last_positive_at" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN stock_last_positive_at TEXT")
        conn.execute(
            "UPDATE products SET stock_last_positive_at = stock_updated_at "
            "WHERE stock_quantity > 0 AND stock_updated_at IS NOT NULL"
        )

    # Product alias table: maps 1C name variants to canonical product IDs.
    # Seeded from Rassvet_Master Ibrat.xlsx + supply history. Self-improving:
    # each successful fuzzy match in /stock or /prices auto-adds an alias.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS product_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alias_name TEXT NOT NULL,
            alias_name_lower TEXT NOT NULL,
            product_id INTEGER NOT NULL,
            source TEXT DEFAULT 'manual',
            confirmed INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_product_aliases_name
        ON product_aliases(alias_name_lower)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_product_aliases_product
        ON product_aliases(product_id)
    """)

    # Unmatched import names: logged when /stock or /prices can't match a name.
    # Admin reviews via /aliases command and links them manually.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS unmatched_import_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            name_lower TEXT NOT NULL,
            source TEXT DEFAULT 'stock',
            occurrences INTEGER DEFAULT 1,
            resolved INTEGER DEFAULT 0,
            resolved_product_id INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            resolved_at TEXT
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unmatched_names_lower
        ON unmatched_import_names(name_lower)
    """)

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

# Latin → Cyrillic (reverse transliteration for search)
_LAT2CYR_MULTI = [
    ('shch', 'щ'), ('sh', 'ш'), ('ch', 'ч'), ('zh', 'ж'),
    ('ts', 'ц'), ('kh', 'х'), ('yo', 'ё'), ('yu', 'ю'), ('ya', 'я'),
]
_LAT2CYR_SINGLE = {
    'a': 'а', 'b': 'б', 'v': 'в', 'g': 'г', 'd': 'д', 'e': 'е',
    'z': 'з', 'i': 'и', 'y': 'й', 'k': 'к', 'l': 'л', 'm': 'м',
    'n': 'н', 'o': 'о', 'p': 'п', 'r': 'р', 's': 'с', 't': 'т',
    'u': 'у', 'f': 'ф', 'x': 'х', 'c': 'ц', 'w': 'в', 'h': 'х',
}

# Common phonetic misspellings in construction materials search
_PHONETIC_ALIASES = {
    'siment': 'цемент', 'cement': 'цемент', 'tsement': 'цемент',
    'simplex': 'симплекс', 'emal': 'эмаль', 'gruntofka': 'грунтовка',
    'gruntovka': 'грунтовка', 'grunt': 'грунт', 'linolium': 'линолеум',
    'linoleum': 'линолеум', 'yelim': 'елим', 'plintus': 'плинтус',
    'samorez': 'саморез', 'dyubel': 'дюбель', 'dubel': 'дюбель',
    'germetik': 'герметик', 'koler': 'колер', 'koller': 'коллер',
    'pena': 'пена', 'penoplast': 'пенопласт',
    'gips': 'гипс', 'gipso': 'гипсо', 'gipsokarton': 'гипсокартон',
    'lak': 'лак', 'olifa': 'олифа', 'kraska': 'краска',
    'shpatel': 'шпатель', 'shpatlevka': 'шпатлевка',
    'mix': 'микс', 'sim': 'сим', 'profil': 'профиль',
    'burchak': 'бурчак', 'elektrod': 'электрод',
}

# Uzbek Latin special characters → ASCII equivalents for search normalization
_UZ_NORMALIZE = {
    "o'": "o", "g'": "g", "sh": "sh",
    "O'": "o", "G'": "g", "Sh": "sh",
    "'": "",  # lone apostrophe (used in o'ram, g'isht)
}


def normalize_uzbek(text):
    """Normalize Uzbek special characters for search matching.
    o' → o, g' → g so that 'oram' matches 'o\\'ram'."""
    t = text
    for src, dst in _UZ_NORMALIZE.items():
        t = t.replace(src, dst)
    return t


def transliterate_to_latin(text):
    """Transliterate Cyrillic text to Latin (lowercase)."""
    result = []
    for ch in text.lower():
        result.append(_CYR2LAT.get(ch, ch))
    return ''.join(result)


def transliterate_to_cyrillic(text):
    """Transliterate Latin text to Cyrillic (lowercase).
    Handles multi-char digraphs first (sh→ш, ch→ч, etc)."""
    t = text.lower()
    # Check phonetic alias table first (exact word match)
    if t in _PHONETIC_ALIASES:
        return _PHONETIC_ALIASES[t]
    # Multi-char replacements (longest first)
    for lat, cyr in _LAT2CYR_MULTI:
        t = t.replace(lat, cyr)
    # Single-char replacements
    result = []
    for ch in t:
        result.append(_LAT2CYR_SINGLE.get(ch, ch))
    return ''.join(result)


def build_search_text(name_cyrillic, name_display, producer_name, unit=None, category_name=None):
    """Build a combined search index string for a product.

    Combines: original 1C name (Cyrillic) + display name (Latin) +
    transliterated versions (Cyrillic→Latin AND Latin→Cyrillic) +
    producer name + unit + category + Uzbek-normalized variants.
    All lowercased for case-insensitive LIKE matching.
    """
    parts = []
    if name_cyrillic:
        cyr = name_cyrillic.strip().lower()
        parts.append(cyr)
        parts.append(transliterate_to_latin(cyr))
    if name_display:
        disp = name_display.strip().lower()
        parts.append(disp)
        norm = normalize_uzbek(disp)
        if norm != disp:
            parts.append(norm)
        # Reverse transliterate Latin display name → Cyrillic
        cyr_rev = transliterate_to_cyrillic(disp)
        if cyr_rev != disp:
            parts.append(cyr_rev)
    if producer_name:
        prod = producer_name.strip().lower()
        parts.append(prod)
        norm = normalize_uzbek(prod)
        if norm != prod:
            parts.append(norm)
    if unit:
        parts.append(unit.strip().lower())
    if category_name:
        cat = category_name.strip().lower()
        parts.append(cat)
        norm = normalize_uzbek(cat)
        if norm != cat:
            parts.append(norm)
    return ' '.join(parts)


def _rebuild_search_text(conn):
    """Populate search_text for all products (used during migration)."""
    rebuild_all_search_text(conn)


def rebuild_all_search_text(conn=None):
    """Rebuild search_text for ALL products. Call after name/unit/category changes.

    Can be called with an existing connection or will create its own.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db()
    rows = conn.execute(
        """SELECT p.id, p.name, p.name_display, pr.name as producer_name,
                  p.unit, c.name as category_name
           FROM products p
           JOIN producers pr ON pr.id = p.producer_id
           JOIN categories c ON c.id = p.category_id"""
    ).fetchall()
    count = 0
    for r in rows:
        st = build_search_text(
            r["name"], r["name_display"], r["producer_name"],
            r["unit"], r["category_name"]
        )
        conn.execute("UPDATE products SET search_text = ? WHERE id = ?", (st, r["id"]))
        count += 1
    conn.commit()
    if own_conn:
        conn.close()
    return count


def _seed_locations(conn):
    """Seed the location hierarchy: Viloyat → District → Mo'ljal.

    Idempotent (UNIQUE constraint on name+type+parent_id prevents duplicates).
    Data curated from Client Master 16.03.26.xlsx analysis (Session M, 2026-04-13).
    """
    # Check if already seeded
    existing = conn.execute("SELECT COUNT(*) FROM locations").fetchone()[0]
    if existing > 0:
        return

    # ── Viloyats ──
    viloyats = [
        ("Samarqand", 10), ("Jizzax", 20), ("Qashqadaryo", 30),
        ("Navoiy", 40), ("Sughd (Tajikistan)", 50), ("Toshkent", 60),
        ("Syrdaryo", 70), ("Buxoro", 80),
    ]
    viloyat_ids = {}
    for name, sort in viloyats:
        conn.execute(
            "INSERT OR IGNORE INTO locations (name, type, parent_id, sort_order) VALUES (?, 'viloyat', NULL, ?)",
            (name, sort),
        )
        row = conn.execute(
            "SELECT id FROM locations WHERE name = ? AND type = 'viloyat'", (name,)
        ).fetchone()
        viloyat_ids[name] = row[0]

    sam_id = viloyat_ids["Samarqand"]

    # ── Districts (Shahar/Tuman) ── mapped to viloyat
    districts_samarkand = [
        ("Samarqand shahar", 10), ("Urgut tuman", 20), ("Payariq tuman", 30),
        ("Pastdarg'om tuman", 40), ("Jombay tuman", 50), ("Bulung'ur tuman", 60),
        ("Ishtixon tuman", 70), ("Kattaqo'rg'on", 80), ("Oqdaryo tuman", 90),
        ("Toyloq tuman", 100), ("Qo'shrabot tuman", 110), ("Narpay tuman", 120),
        ("Nurobod tuman", 130), ("Kattaqo'rg'on tuman", 140), ("Paxtachi tuman", 150),
        ("Samarqand tuman", 160), ("Kattaqo'rg'on shahar", 170),
    ]
    districts_other = [
        ("G'allaorol tuman", viloyat_ids.get("Jizzax", sam_id), 10),
        ("Xatirchi tuman", viloyat_ids.get("Navoiy", sam_id), 10),
        ("Jizzax", viloyat_ids.get("Jizzax", sam_id), 20),
        ("Baxmal", viloyat_ids.get("Jizzax", sam_id), 30),
        ("Qarshi", viloyat_ids.get("Qashqadaryo", sam_id), 10),
        ("G'uzor", viloyat_ids.get("Qashqadaryo", sam_id), 20),
        ("Shahrisabz", viloyat_ids.get("Qashqadaryo", sam_id), 30),
        ("Panjakent", viloyat_ids.get("Sughd (Tajikistan)", sam_id), 10),
        ("Navoiy", viloyat_ids.get("Navoiy", sam_id), 20),
        ("Davlatobod", viloyat_ids.get("Navoiy", sam_id), 30),
        ("Toshkent", viloyat_ids.get("Toshkent", sam_id), 10),
        ("Chordara", viloyat_ids.get("Syrdaryo", sam_id), 10),
        ("Buxoro", viloyat_ids.get("Buxoro", sam_id), 10),
    ]

    district_ids = {}
    for name, sort in districts_samarkand:
        conn.execute(
            "INSERT OR IGNORE INTO locations (name, type, parent_id, sort_order) VALUES (?, 'district', ?, ?)",
            (name, sam_id, sort),
        )
        row = conn.execute(
            "SELECT id FROM locations WHERE name = ? AND type = 'district' AND parent_id = ?",
            (name, sam_id),
        ).fetchone()
        district_ids[name] = row[0]

    for name, vid, sort in districts_other:
        conn.execute(
            "INSERT OR IGNORE INTO locations (name, type, parent_id, sort_order) VALUES (?, 'district', ?, ?)",
            (name, vid, sort),
        )
        row = conn.execute(
            "SELECT id FROM locations WHERE name = ? AND type = 'district' AND parent_id = ?",
            (name, vid),
        ).fetchone()
        district_ids[name] = row[0]

    # ── Mo'ljals ── mapped to district
    # Curated from Client Master: 163 raw values → 148 after merging duplicates.
    # Sorted by client_count descending (most popular first = lower sort_order).
    sam_shahar_id = district_ids["Samarqand shahar"]

    # All mo'ljals mapped to Samarqand shahar (the 106 unique ones from the data).
    # Mo'ljals for rural tumans are very sparse in the data (few clients have them),
    # so we seed only the city ones initially. Admin can add more via API.
    samarkand_moljals = [
        "Chelak", "Juma", "Loyish", "Titova", "Kirpichka", "Mikrorayon",
        "Mingchinor", "Charxin", "Dagbet", "Selskiy", "Jartepa",
        "Metall Bozor", "Nariman", "Super", "Dallager", "Mirbozor",
        "Metan", "Ulugbek", "Oqtosh", "Afsona", "Oqdaryo", "Sogdiana",
        "Gorgaz", "Payshanba shaharchasi", "Motrid", "Trikotajka",
        "Erkin Savdo", "Lenin Bayroq", "Pavarot", "Kaftarxona",
        "G'o's shaharchasi", "DSK", "Ravonak", "Oqmachit", "Juma Bozor",
        "Sattepo", "Taksomotorniy", "Bo'g'izag'on", "Zarmitan", "Sochak",
        "Xazora", "Al-Buxoriy", "Aziz Bozor", "Saexat", "Temir Bozor",
        "Raysentr", "Kolxoz Pobeda", "Marxabo", "Gagarina", "Nagorniy",
        "Limonadka", "Xishrav", "Xujasoat", "Bionur", "Go'zalkent shaharchasi",
        "Arabxona", "Frunze", "Siyob Bozor", "Avtovokzal", "Ovoshnoy",
        "Geofizika", "BAM", "Elektroset", "Tabachka", "Kildon shaharchasi",
        "Sadriddin Ayni", "Yangiqo'rg'on", "Xatirchi", "Chayniy", "Rajab Amin",
        "Jush", "Moshin Bozor", "Gelyon", "Rudakiy", "Yuksalish", "Jom",
        "Yangi Bozor", "Kadan", "Panjob", "Ingichka shaharchasi", "Doshkolniy",
        "Bakaleya", "Ziyovuddin", "Stroy Bazar", "Suzangaron", "Vokzal",
        "Namozgox", "Shoxizinda", "Andijoni", "Za Liniya", "Shurboicha",
        "Oktyabrskaya", "Farxod Poselka", "Quyi turkman",
        "Marjonbuloq shaharchasi", "Galabotir", "Yangi-Ariq", "Qoratepa",
        "Med Kolledj", "Taxta Bozor", "Payshanba Qishloq",
        "Yangi Zapchast Bozor", "Obl Gai", "Karasinka", "O'ramas shaharchasi",
        "Kakandskaya", "Spitamen Shox", "Korasuv", "Aeroport", "Andoq",
        "Qush Chinor", "Oqsoy", "Beshkapa MFY", "Bahrin MFY", "Chimboyobod",
        "Mo'minobod", "Qoradaryo shaharchasi", "Po'latdarxon", "Respublika",
        "Pulimug'ob aholi punkti", "Krytyy Rynok", "Melnichniy", "Yangiariq",
        "Ikar", "Plemsavxoz", "Gor bolnitsa", "Nurbulok", "Pendjikentskaya",
        "Obl bolnitsa", "Chumchuqli", "Tankoviy", "Ohalik MFY", "Chorshanba",
        "Kumushkent shaharchasi", "Pishchevoy", "Samgasi", "Navbogchiyon MFY",
        "Mo'lyon", "Nayman MFY", "G'azira", "Travmatologiya", "Lo'blaxur",
        "Krenkelya", "Primkent", "23 fevral", "To'rtayg'ir", "Chortut", "Badal",
    ]

    # Some Mo'ljals that appear outside Samarkand city (in rural tumans)
    rural_moljals = {
        "Urgut tuman": ["Chelak", "Charxin"],
        "Payariq tuman": ["Loyish", "Mingchinor"],
        "Jombay tuman": ["Jartepa"],
        "Bulung'ur tuman": ["Oqtosh"],
    }

    sort_idx = 10
    for name in samarkand_moljals:
        conn.execute(
            "INSERT OR IGNORE INTO locations (name, type, parent_id, sort_order) VALUES (?, 'moljal', ?, ?)",
            (name, sam_shahar_id, sort_idx),
        )
        sort_idx += 10

    for district_name, moljal_names in rural_moljals.items():
        d_id = district_ids.get(district_name)
        if not d_id:
            continue
        for name in moljal_names:
            conn.execute(
                "INSERT OR IGNORE INTO locations (name, type, parent_id, sort_order) VALUES (?, 'moljal', ?, ?)",
                (name, d_id, 10),
            )

    conn.commit()
    loc_count = conn.execute("SELECT COUNT(*) FROM locations").fetchone()[0]
    print(f"[database] Seeded {loc_count} location entries (viloyats + districts + mo'ljals)")


def _seed_daily_upload_schedule(conn):
    """Seed the 9 checklist items. Idempotent (INSERT OR IGNORE by PK)."""
    rows = [
        # (upload_type, ru, uz, command, expected, required_weekdays, sort_order)
        ("balances_uzs", "Оборотка 40.10 (UZS)", "Aylanma 40.10 (UZS)", "/balances", 1, "1,2,3,4,5,6", 10),
        ("balances_usd", "Оборотка 40.11 (USD)", "Aylanma 40.11 (USD)", "/balances", 1, "1,2,3,4,5,6", 20),
        ("stock",        "Остаток",             "Qoldiq",             "/stock",    1, "1,2,3,4,5,6", 30),
        ("prices",       "Цены",                "Narxlar",            "/prices",   1, "1,2,3,4,5,6", 40),
        ("debtors",      "Дебиторы",            "Qarzdorlar",         "/debtors",  1, "1,2,3,4,5,6", 50),
        ("realorders",   "Реализация",          "Realizatsiya",       "/realorders", 1, "1,2,3,4,5,6", 60),
        ("cash",         "Касса",               "Kassa",              "/cash",     2, "1,2,3,4,5,6", 70),
        ("fxrate",       "Курс валют",          "Valyuta kursi",      "/fxrate",   1, "1,2,3,4,5,6", 80),
        ("supply",       "Поступление/Возврат", "Kirim/Qaytarish",    "/supply",   1, "1,2,3,4,5,6", 90),
        ("clients",      "Клиенты",             "Mijozlar",           "/clients",  1, "1,2,3,4,5,6", 100),
    ]
    for r in rows:
        conn.execute(
            """INSERT OR IGNORE INTO daily_upload_schedule
               (upload_type, display_name_ru, display_name_uz, command,
                expected_count_per_day, required_weekdays, sort_order, is_active)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
            r,
        )
    conn.commit()


if __name__ == "__main__":
    init_db()
    print("Database initialized.")
