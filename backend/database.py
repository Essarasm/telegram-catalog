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
    # Unicode-aware LOWER — SQLite's built-in is ASCII-only and silently
    # fails on Cyrillic ("Сардор".lower() is a no-op in built-in LOWER).
    # Registering Python str.lower here matches bot/shared.py's connection
    # and closes the SQLITE_LOWER_ASCII_ONLY class of bugs (Error Log #18).
    # Per-file registrations (e.g. in client_search.py) stay as defense in
    # depth — calling create_function twice with the same name is harmless.
    conn.create_function("LOWER", 1, lambda s: s.lower() if s else s)
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
        "SELECT id FROM allowed_clients WHERE client_id_1c = ? AND COALESCE(status, 'active') NOT LIKE 'merged%'",
        (row["client_id_1c"],),
    ).fetchall()
    ids = [s["id"] for s in siblings]
    if client_id not in ids:
        ids.append(client_id)
    return ids


def gather_sibling_phones(conn, client_id):
    """Return a deduped, ordered list of phones across all sibling rows.

    Walks sibling allowed_clients rows (same client_id_1c) and collects
    phone_normalized + raqam_02 + raqam_03 from each, preserving the
    acting row's primary phone first and dropping empties / duplicates.

    Used by /cabinet/client-info and /agent/switch-client so the agent
    panel + cabinet phone block see the same multi-phone shape.
    """
    if not client_id:
        return []
    sibling_ids = get_sibling_client_ids(conn, client_id)
    if not sibling_ids:
        return []
    placeholders = ",".join("?" * len(sibling_ids))
    rows = conn.execute(
        f"SELECT id, phone_normalized, raqam_02, raqam_03 "
        f"FROM allowed_clients WHERE id IN ({placeholders})",
        sibling_ids,
    ).fetchall()
    ordered = sorted(rows, key=lambda r: 0 if r["id"] == client_id else 1)
    phones = []
    for r in ordered:
        for raw in (r["phone_normalized"], r["raqam_02"], r["raqam_03"]):
            p = (raw or "").strip()
            if p and p not in phones:
                phones.append(p)
    return phones


SCHEMA_VERSION = 18  # 2026-05-27: v18 captures the 1C real-orders col-0 approval marker (V = approved/shipped, X = pending) into real_orders.is_approved + first_pending_at. Until v18 the importer ignored col 0 and treated all rows as if shipped, inflating customer-app deliveries / top buyers / revenue with pending orders that hadn't yet been approved by Alisher. New columns are additive and nullable; legacy rows stay NULL (treat as "approved-or-unknown" in downstream filters). Earlier: v17 = reminder_fire_log; v16 = client_balance_overrides; v15 = composite UNIQUE on real_orders + client_payments.


def init_db():
    conn = get_db()

    # Schema version tracking
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL,
            applied_at TEXT DEFAULT (datetime('now')),
            description TEXT
        )
    """)
    current = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] or 0

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

        -- Per-client callback log: when an admin schedules a follow-up call.
        -- Append-only history (every reschedule writes a new row); latest row
        -- per client wins on read. callback_date may be NULL = explicit clear.
        CREATE TABLE IF NOT EXISTS client_callbacks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name_1c TEXT NOT NULL,
            callback_date TEXT,
            set_by_telegram_id INTEGER,
            set_by_name TEXT NOT NULL,
            note TEXT,
            set_at DATETIME DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_client_callbacks_client ON client_callbacks(client_name_1c, set_at DESC);

        -- Daily aggregated snapshot of debtors. Written after every /debtors
        -- import. Per-client client_debts is truncate-replace; this preserves
        -- aggregate history for trend charts.
        --   *_total = raw 1C debt (includes structural pseudo-accounts)
        --   *_real  = after pseudo_clients.SYSTEM_NON_CLIENT_NAMES exclusion
        CREATE TABLE IF NOT EXISTS client_debt_snapshots_daily (
            report_date TEXT PRIMARY KEY,
            n_clients_total INTEGER DEFAULT 0,
            n_clients_real INTEGER DEFAULT 0,
            debt_uzs_total REAL DEFAULT 0,
            debt_usd_total REAL DEFAULT 0,
            debt_uzs_real REAL DEFAULT 0,
            debt_usd_real REAL DEFAULT 0,
            aging_uzs_0_30 REAL DEFAULT 0,
            aging_uzs_31_60 REAL DEFAULT 0,
            aging_uzs_61_90 REAL DEFAULT 0,
            aging_uzs_91_120 REAL DEFAULT 0,
            aging_uzs_120_plus REAL DEFAULT 0,
            snapshot_at TEXT DEFAULT (datetime('now'))
        );

        -- Collection-attempt log: every dispatcher phone call to a debtor
        -- before/while a truck heads out. Snapshots debt + aging at call time
        -- so history survives later debt-table reimports.
        CREATE TABLE IF NOT EXISTS collection_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            dispatcher_name TEXT,
            dispatcher_tg_id INTEGER,
            call_at TEXT DEFAULT (datetime('now')),
            outcome TEXT NOT NULL,
            agreed_amount_uzs REAL,
            agreed_amount_usd REAL,
            notes TEXT,
            debt_uzs_at_call REAL,
            debt_usd_at_call REAL,
            oldest_aging_at_call TEXT,
            destination_tuman_id INTEGER,
            destination_lat REAL,
            destination_lng REAL,
            included_in_route INTEGER DEFAULT 0,
            actual_collected_uzs REAL,
            actual_collected_usd REAL
        );
        CREATE INDEX IF NOT EXISTS idx_ca_client ON collection_attempts(client_id, call_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ca_call_at ON collection_attempts(call_at DESC);

        -- ─────────────────────────────────────────────────────────────
        -- Session M Phase 3: Driver route planner (delivery + return-leg collections)
        -- ─────────────────────────────────────────────────────────────
        --
        -- A `delivery_route` is one planned trip: warehouse → ≤8 delivery stops
        -- → optional debtor pickups on the return leg → warehouse. The dispatcher
        -- picks truck + driver + delivery clients; the planner orders them with
        -- nearest-neighbor on haversine and suggests return-leg debtors that fit
        -- a time budget. Per-stop driver-completion fields are nullable now
        -- (Phase 2 hook) so the schema doesn't change when we wire that up.
        CREATE TABLE IF NOT EXISTS delivery_routes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (datetime('now')),
            created_by_tg_id INTEGER,
            created_by_name TEXT,
            truck_type TEXT NOT NULL,            -- 'labo' | 'jac' | 'foton' | 'isuzu'
            truck_capacity_t REAL,               -- 1.0 | 2.5 | 3.0 | 7.0
            driver_tg_id INTEGER,                -- users.telegram_id (agent_role='agent')
            driver_name TEXT,
            status TEXT DEFAULT 'planned',       -- 'planned' | 'dispatched' | 'completed' | 'cancelled'
            origin_lat REAL NOT NULL,
            origin_lng REAL NOT NULL,
            return_buffer_km REAL,
            return_time_budget_min REAL,
            total_distance_km REAL,
            estimated_minutes REAL,
            maps_url TEXT,                       -- Yandex Maps URL (rtext=… &rtt=auto)
            driver_brief TEXT,                   -- Telegram-ready HTML
            dispatched_at TEXT,
            completed_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_delivery_routes_driver ON delivery_routes(driver_tg_id);
        CREATE INDEX IF NOT EXISTS idx_delivery_routes_status ON delivery_routes(status);
        CREATE INDEX IF NOT EXISTS idx_delivery_routes_created ON delivery_routes(created_at DESC);

        CREATE TABLE IF NOT EXISTS route_stops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            route_id INTEGER NOT NULL,
            sequence_order INTEGER NOT NULL,     -- 0 = warehouse start; 1..N = stops; last = warehouse return
            kind TEXT NOT NULL,                  -- 'origin' | 'delivery' | 'collection' | 'return'
            client_id INTEGER,                   -- allowed_clients.id (NULL for origin/return)
            client_id_1c TEXT,
            client_display_name TEXT,
            phone_normalized TEXT,
            lat REAL NOT NULL,
            lng REAL NOT NULL,
            address TEXT,
            leg_distance_km REAL,                -- distance from previous stop
            detour_minutes REAL,                 -- collection stops: minutes added vs direct return
            debt_uzs REAL,
            debt_usd REAL,
            oldest_aging_bucket TEXT,
            collection_attempt_id INTEGER,       -- collection_attempts.id (back-link)
            stop_status TEXT,                    -- NULL | 'visited' | 'skipped' | 'failed' (Phase 2)
            collected_uzs REAL,
            collected_usd REAL,
            completed_at TEXT,
            FOREIGN KEY (route_id) REFERENCES delivery_routes(id) ON DELETE CASCADE,
            UNIQUE(route_id, sequence_order)
        );
        CREATE INDEX IF NOT EXISTS idx_route_stops_route ON route_stops(route_id);
        CREATE INDEX IF NOT EXISTS idx_route_stops_client ON route_stops(client_id);

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

        -- Full audit trail of every /fxrate set event. daily_fx_rates keeps
        -- one canonical row per day (latest wins for analytics); this table
        -- preserves every individual set so the agent panel can show both
        -- rates when the rate is updated during the day.
        CREATE TABLE IF NOT EXISTS daily_fx_rate_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rate_date TEXT NOT NULL,
            currency_pair TEXT NOT NULL DEFAULT 'USD_UZS',
            rate REAL NOT NULL,
            set_at TEXT DEFAULT (datetime('now')),
            set_by_user_id INTEGER,
            set_by_name TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fx_events_date_set_at
            ON daily_fx_rate_events(rate_date, set_at DESC);

        -- Audit of every agent → client switch (bot /testclient + mini app).
        -- Used to render the "Recent clients" list on the agent home screen.
        CREATE TABLE IF NOT EXISTS agent_client_switches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_telegram_id INTEGER NOT NULL,
            client_id INTEGER NOT NULL,
            switched_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_agent_switches_agent
            ON agent_client_switches(agent_telegram_id, switched_at DESC);

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

        -- Session N: client-facing "payment received" notifications.
        -- Flow: /cash import queues one row per (telegram_id × currency leg);
        -- /debtors import fires grouped messages per client for today's rows;
        -- 18:00 sweeper demotes stale pending → missed_notifications.
        CREATE TABLE IF NOT EXISTS pending_payment_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            client_id INTEGER NOT NULL,
            client_name_1c TEXT,
            kassa_doc_no TEXT NOT NULL,
            kassa_date TEXT NOT NULL,
            currency TEXT NOT NULL,        -- 'UZS' | 'USD'
            amount REAL NOT NULL,
            queued_at TEXT DEFAULT (datetime('now')),
            UNIQUE(telegram_id, kassa_doc_no, kassa_date, currency, amount)
        );
        CREATE INDEX IF NOT EXISTS idx_pending_notif_client ON pending_payment_notifications(client_id);
        CREATE INDEX IF NOT EXISTS idx_pending_notif_kassa_date ON pending_payment_notifications(kassa_date);
        CREATE INDEX IF NOT EXISTS idx_pending_notif_telegram ON pending_payment_notifications(telegram_id);

        CREATE TABLE IF NOT EXISTS sent_payment_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            client_id INTEGER NOT NULL,
            client_name_1c TEXT,
            kassa_doc_no TEXT NOT NULL,
            kassa_date TEXT NOT NULL,
            currency TEXT NOT NULL,
            amount REAL NOT NULL,
            debt_uzs_after REAL,
            debt_usd_after REAL,
            telegram_message_id INTEGER,
            sent_at TEXT DEFAULT (datetime('now')),
            UNIQUE(telegram_id, kassa_doc_no, kassa_date, currency, amount)
        );
        CREATE INDEX IF NOT EXISTS idx_sent_notif_sent_at ON sent_payment_notifications(sent_at);
        CREATE INDEX IF NOT EXISTS idx_sent_notif_client ON sent_payment_notifications(client_id);

        -- reason ∈ {'unmatched_name','no_telegram_bind','bot_send_failed','balance_missing_after_24h'}
        CREATE TABLE IF NOT EXISTS missed_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kassa_doc_no TEXT,
            kassa_date TEXT,
            client_name_1c TEXT,
            client_id INTEGER,
            telegram_id INTEGER,
            currency TEXT,
            amount REAL,
            reason TEXT NOT NULL,
            detail TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            resolved_at TEXT,
            resolved_by TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_missed_notif_created ON missed_notifications(created_at);
        CREATE INDEX IF NOT EXISTS idx_missed_notif_resolved ON missed_notifications(resolved_at);
        CREATE INDEX IF NOT EXISTS idx_missed_notif_reason ON missed_notifications(reason);

        -- ─────────────────────────────────────────────────────────────
        -- Derived-shipment journal — built from 1C "Реализация товаров"
        -- xls exports (Фактические заказы). One row per shipment document.
        -- Pair with client_payments to derive clean running balances that
        -- don't inherit the pre-2020 historical noise in client_balances.
        -- ─────────────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS derived_shipments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_number TEXT NOT NULL,
            doc_date TEXT NOT NULL,              -- YYYY-MM-DD
            client_name_1c TEXT NOT NULL,
            client_id INTEGER,                    -- matched to allowed_clients
            uzs_amount REAL DEFAULT 0,            -- Σ "Сумма" across item rows
            usd_amount REAL DEFAULT 0,            -- Σ "СуммаВал" across item rows
            item_count INTEGER DEFAULT 0,
            currency_marker TEXT,                 -- 'USD' / 'UZS' from col 25
            imported_at TEXT DEFAULT (datetime('now')),
            UNIQUE(doc_number, doc_date)
        );
        CREATE INDEX IF NOT EXISTS idx_derived_shipments_client_id ON derived_shipments(client_id);
        CREATE INDEX IF NOT EXISTS idx_derived_shipments_client_name ON derived_shipments(client_name_1c);
        CREATE INDEX IF NOT EXISTS idx_derived_shipments_doc_date ON derived_shipments(doc_date);

        -- ─────────────────────────────────────────────────────────────
        -- Durable audit log for EVERY inbound location message.
        -- Insert-first (before any processing) so that even if routing,
        -- geocoding, or DB update fails, the raw lat/lng is recoverable.
        -- Implements Ulugbek's "no client data should ever be lost" rule.
        -- ─────────────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS location_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT DEFAULT (datetime('now')),
            telegram_id INTEGER,
            first_name TEXT,
            username TEXT,
            chat_id INTEGER,
            chat_type TEXT,             -- 'private', 'group', 'supergroup', 'channel'
            latitude REAL,
            longitude REAL,
            is_forward INTEGER DEFAULT 0,
            forward_from_id INTEGER,
            forward_from_chat_id INTEGER,
            is_agent INTEGER,
            linked_client_id INTEGER,
            linked_client_1c TEXT,
            reverse_geocode_json TEXT,  -- JSON blob of Nominatim response
            processed_ok INTEGER DEFAULT 0,
            error_reason TEXT,
            raw_message_json TEXT       -- full aiogram Message.model_dump_json()
        );
        CREATE INDEX IF NOT EXISTS idx_location_attempts_received_at ON location_attempts(received_at);
        CREATE INDEX IF NOT EXISTS idx_location_attempts_telegram_id ON location_attempts(telegram_id);
        CREATE INDEX IF NOT EXISTS idx_location_attempts_processed_ok ON location_attempts(processed_ok);

        -- ─────────────────────────────────────────────────────────────
        -- Per-client location-decision queue (2026-05-28, Session M).
        -- When an agent/driver sends a pin >100m from the existing pin,
        -- we DON'T overwrite — we insert a row here, dispatch a comparison
        -- message to AGENT_APPROVAL_GROUP_CHAT_ID with [keep old / use new]
        -- buttons, and let admin decide. Within-threshold incoming pins
        -- are silently ignored (no row here, audit-only in location_attempts).
        -- ─────────────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS pending_location_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (datetime('now')),
            client_id INTEGER NOT NULL,
            client_name TEXT,
            client_id_1c TEXT,
            -- Prior pin snapshot (the one currently on allowed_clients.gps_*)
            prior_lat REAL,
            prior_lng REAL,
            prior_address TEXT,
            prior_region TEXT,
            prior_district TEXT,
            prior_set_at TEXT,
            prior_set_by_tg_id INTEGER,
            prior_set_by_name TEXT,
            prior_set_by_role TEXT,
            -- Incoming pin snapshot (what the agent just sent)
            incoming_lat REAL,
            incoming_lng REAL,
            incoming_address TEXT,
            incoming_region TEXT,
            incoming_district TEXT,
            incoming_by_tg_id INTEGER,
            incoming_by_name TEXT,
            incoming_by_role TEXT,
            incoming_attempt_id INTEGER,    -- FK to location_attempts.id
            -- Distance + source path for forensics
            distance_m REAL,
            source_path TEXT,                -- 'driver_lokatsiya' | 'mini_app_dm'
            -- Dispatch + decision state
            dispatched_chat_id INTEGER,
            dispatched_message_id INTEGER,
            status TEXT DEFAULT 'pending',   -- 'pending' | 'keep_old' | 'use_new' | 'superseded'
            decided_at TEXT,
            decided_by_tg_id INTEGER,
            decided_by_name TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pending_loc_dec_status ON pending_location_decisions(status);
        CREATE INDEX IF NOT EXISTS idx_pending_loc_dec_client ON pending_location_decisions(client_id);

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

    # Audit-first table for agent-initiated new-shop registrations from the
    # agent panel. Per the zero-data-loss rule, every attempt is inserted
    # here BEFORE allowed_clients is touched; status flips to 'created' or
    # 'linked_existing' once the path resolves. Phone-collision links to the
    # already-whitelisted row instead of creating a duplicate.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agent_client_registrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_telegram_id INTEGER NOT NULL,
            shop_name TEXT NOT NULL,
            first_name TEXT,
            last_name TEXT,
            venue TEXT,
            phone_raw TEXT,
            phone_normalized TEXT,
            gps_latitude REAL,
            gps_longitude REAL,
            status TEXT DEFAULT 'pending',
            linked_client_id INTEGER,
            error_message TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    # Additive migration for DBs created with the v1 schema (shop_name only).
    acr_cols = {row[1] for row in conn.execute("PRAGMA table_info(agent_client_registrations)").fetchall()}
    for col, coltype in [("first_name", "TEXT"), ("last_name", "TEXT"), ("venue", "TEXT")]:
        if col not in acr_cols:
            conn.execute(f"ALTER TABLE agent_client_registrations ADD COLUMN {col} {coltype}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_reg_agent ON agent_client_registrations(agent_telegram_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_reg_phone ON agent_client_registrations(phone_normalized)")

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
            ac_cols.add(col)

    # Create index on client_id_1c (after migration ensures column exists)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_allowed_1c ON allowed_clients(client_id_1c)")

    # Phase 1a of Client Data Workflow — sync-guarantee columns.
    # All NULL by default; populated progressively as the pipeline fills them in.
    # See obsidian-vault/Client Data Workflow — Design v0.1.md for semantics.
    for col, coltype in [
        ("source_1c", "TEXT"),              # last 1C Контрагенты import id that touched this row
        ("source_master", "TEXT"),          # last Client Master upload id that touched ✏️ columns
        ("master_row_id", "INTEGER"),        # FK to Client Master xlsx row (for round-tripping)
        ("needs_review", "INTEGER DEFAULT 0"),       # 1 = conflict between sources, human must resolve
        ("needs_verification", "INTEGER DEFAULT 0"), # 1 = location data looks implausible
        ("segment", "TEXT DEFAULT 'shop'"),  # 'shop' | 'usto' | 'other'
        ("hajm", "TEXT"),                    # client volume tag (Master-owned, e.g. 'Katta' / 'Kichik')
        ("mijoz_holati", "TEXT"),            # client status annotation (Master-owned)
        ("eslatmalar", "TEXT"),              # free-form operator notes (Master-owned)
        ("ism_02", "TEXT"), ("raqam_02", "TEXT"),  # secondary contact
        ("ism_03", "TEXT"), ("raqam_03", "TEXT"),  # tertiary contact
        ("viloyat", "TEXT"),                 # Master-owned; mirror of location/GPS reverse-geocode
        ("tuman", "TEXT"),
        ("moljal", "TEXT"),
        ("last_master_synced_at", "TEXT"),
        # Canonical client GPS — separate from `location` so importers cannot
        # destroy agent/client-set coordinates. Written by bot location handler
        # only; read by GET /api/client-location. Apr 2026 — Session M follow-up.
        ("gps_latitude", "REAL"),
        ("gps_longitude", "REAL"),
        ("gps_address", "TEXT"),
        ("gps_region", "TEXT"),
        ("gps_district", "TEXT"),
        ("gps_set_at", "TEXT"),
        ("gps_set_by_tg_id", "INTEGER"),
        ("gps_set_by_name", "TEXT"),
        ("gps_set_by_role", "TEXT"),
        # Manual agent assignment — overrides the auto-derived
        # latest-`agent_client_switches` agent in the Debtors List Agent column.
        # NULL = no manual override → fall back to auto-derive.
        ("assigned_agent_tg_id", "INTEGER"),
        ("assigned_agent_set_at", "TEXT"),
        ("assigned_agent_set_by_tg_id", "INTEGER"),
        ("assigned_agent_set_by_name", "TEXT"),
    ]:
        if col not in ac_cols:
            conn.execute(f"ALTER TABLE allowed_clients ADD COLUMN {col} {coltype}")
            ac_cols.add(col)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_allowed_segment ON allowed_clients(segment)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_allowed_needs_review ON allowed_clients(needs_review)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_allowed_master_row ON allowed_clients(master_row_id)")

    # Phone audit trail — every phone edit via Master upload or manual correction
    # records here so replaced phones can be traced/recovered. Never pruned.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS phone_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            old_phone TEXT,
            new_phone TEXT,
            reason TEXT,
            changed_by TEXT,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES allowed_clients(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_phone_history_client ON phone_history(client_id)")

    # Client Master upload audit — one row per uploaded xlsx, links to archived file.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS master_upload_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            archived_file_path TEXT,
            uploaded_by_user_id INTEGER,
            uploaded_by_name TEXT,
            row_count INTEGER,
            inserted_count INTEGER,
            updated_count INTEGER,
            conflict_count INTEGER,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Generic admin-action audit log — records who ran destructive/irreversible
    # commands so post-incident forensics can answer "who did X and when".
    conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_action_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            user_name TEXT,
            chat_id INTEGER,
            command TEXT NOT NULL,
            args TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_admin_action_log_created ON admin_action_log(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_admin_action_log_cmd ON admin_action_log(command)")

    # Mini-App initData HMAC failure log (Phase A user-auth — 2026-05-13).
    # Populated by backend/services/user_auth.assert_init_data on every 401.
    # Trigger to escalate from Phase A (writes only) to Phase B (reads too):
    # any non-zero rows over a 2-week window or a confirmed leak incident.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hmac_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            claimed_telegram_id INTEGER,
            parsed_telegram_id INTEGER,
            path TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hmac_audit_created ON hmac_audit_log(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hmac_audit_reason ON hmac_audit_log(reason)")

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

    # Migration: add lifecycle columns to products
    # lifecycle: 'active' (supplied in 2026), 'aging' (Jul-Dec 2025), 'stale' (Jan-Jun 2025), 'never' (no supply)
    # Catalog browse shows only active+aging; search can surface stale+never via fuzzy match.
    # last_interest_alert_at tracks the 60-day cooldown on demand-signal alerts.
    # popularity_score: count of distinct real_orders containing this product, last 180 days.
    # Used to rank search results within the same match tier.
    for col, coltype in [
        ("lifecycle", "TEXT DEFAULT 'active'"),
        ("last_interest_alert_at", "TEXT DEFAULT NULL"),
        ("popularity_score", "INTEGER DEFAULT 0"),
    ]:
        if col not in prod_cols:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {coltype}")
            prod_cols.add(col)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_lifecycle ON products(lifecycle)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_real_order_items_name ON real_order_items(product_name_1c)")

    # units_score: weighted blend of avg units shipped/day — 0.6×(last 30d) + 0.4×(prior 60d).
    # Drives default catalog sort (top sellers float, cold items sink). Recomputed by
    # tools/update_units_score.py at startup, after each /realorders import, and daily 04:30.
    if "units_score" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN units_score REAL DEFAULT 0")
        prod_cols.add("units_score")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_units_score ON products(units_score DESC)")

    cat_cols = {row[1] for row in conn.execute("PRAGMA table_info(categories)").fetchall()}
    if "units_score" not in cat_cols:
        conn.execute("ALTER TABLE categories ADD COLUMN units_score REAL DEFAULT 0")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_categories_units_score ON categories(units_score DESC)")

    # Interest-click tracking for hidden (stale/never) products — drives the demand-signal alert.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS product_interest_clicks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            telegram_id INTEGER NOT NULL,
            search_query TEXT,
            match_score REAL,
            clicked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_interest_clicks_product_time ON product_interest_clicks(product_id, clicked_at)")

    # Support thread routing: when the bot forwards a client's support-request
    # to the Admin group, the Telegram message_id of the forwarded message is
    # recorded so admin replies can be routed back to the original client.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS support_threads (
            admin_message_id INTEGER PRIMARY KEY,
            client_telegram_id INTEGER NOT NULL,
            client_message_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_support_threads_client ON support_threads(client_telegram_id)")

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

    # Migration: store the frozen original Sotuv-group message text so
    # post-order feedback (CartPage → /api/feedback) can be appended to the
    # same message via editMessageText instead of posting a separate "Yangi
    # fikr-mulohaza" notification. Re-submissions always overlay the
    # original text + latest comment (no stacking).
    order_cols4 = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
    if "sales_group_message_text" not in order_cols4:
        conn.execute("ALTER TABLE orders ADD COLUMN sales_group_message_text TEXT")

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

    # Role tier (admin / cashier / agent / worker). NULL = no panel access.
    # is_agent stays writable for backwards compat — we keep it in lockstep
    # with the role column (any non-null role means panel access).
    if "agent_role" not in user_cols_agent:
        conn.execute("ALTER TABLE users ADD COLUMN agent_role TEXT")
        # Backfill: existing is_agent=1 rows become 'agent'. Specific seed
        # IDs override (admin = Ulu; the three field workers).
        conn.execute(
            "UPDATE users SET agent_role = 'agent' "
            "WHERE is_agent = 1 AND agent_role IS NULL"
        )
        # Hard-coded admin (Ulu). Insert a placeholder row if the user has
        # never opened the mini-app yet, so /makeagent admin works idempotently.
        conn.execute(
            "INSERT OR IGNORE INTO users (telegram_id, is_approved, is_agent, agent_role) "
            "VALUES (652836922, 1, 1, 'admin')"
        )
        conn.execute(
            "UPDATE users SET is_approved = 1, is_agent = 1, agent_role = 'admin' "
            "WHERE telegram_id = 652836922"
        )
        # Field workers (Ibrohim / Behruz / Murodov). They were is_agent=1
        # under the legacy single-bit model — demote to 'worker'.
        for worker_id in (7887515376, 8433825091, 8708128443):
            conn.execute(
                "INSERT OR IGNORE INTO users (telegram_id, is_approved, is_agent, agent_role) "
                "VALUES (?, 1, 1, 'worker')",
                (worker_id,),
            )
            conn.execute(
                "UPDATE users SET is_agent = 1, agent_role = 'worker' "
                "WHERE telegram_id = ?",
                (worker_id,),
            )
    # Sync env-based cashier whitelist into the DB on every startup so the
    # env list stays an authoritative fallback (matches the Session B 4-layer
    # auth pattern: DB first, env override second).
    import os as _os_role
    _cashier_env = _os_role.getenv("CASHIER_IDS", "")
    if _cashier_env:
        for _raw in _cashier_env.split(","):
            _raw = _raw.strip()
            if not _raw.isdigit():
                continue
            _cid = int(_raw)
            conn.execute(
                "INSERT OR IGNORE INTO users (telegram_id, is_approved, is_agent, agent_role) "
                "VALUES (?, 1, 1, 'cashier')",
                (_cid,),
            )
            # Only promote to cashier if currently unset / agent — never
            # overwrite an explicit admin / worker designation.
            conn.execute(
                "UPDATE users SET is_agent = 1, agent_role = 'cashier' "
                "WHERE telegram_id = ? AND (agent_role IS NULL OR agent_role = 'agent')",
                (_cid,),
            )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_users_agent_role ON users(agent_role)"
    )

    # view_as_role: per-user role override for admin self-testing. NULL = no
    # override (use real agent_role). Set via /role bot command to see the
    # Mini App / panel as another role without losing the real admin role.
    user_cols_view_as = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "view_as_role" not in user_cols_view_as:
        conn.execute("ALTER TABLE users ADD COLUMN view_as_role TEXT")

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

    # Block A — agent vehicle profile (free-text) + delivery dispatch schema.
    # vehicle: free-text descriptor (Labo / Жигули / JAC / Foton / Isuzu /
    # boshqa). Optional/nullable — blank for office-only agents (Шухрат).
    # Backfill is operational: admin sets via the agent vehicle endpoint or
    # direct SQL; no hardcoded telegram_ids in this migration.
    user_cols_vehicle = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "vehicle" not in user_cols_vehicle:
        conn.execute("ALTER TABLE users ADD COLUMN vehicle TEXT")
    # vehicle_capacity_tons: estimated cargo capacity for delivery-matching
    # logic (dispatch picker label, future order batching). Optional —
    # admin/office-only agents leave it null. Stored as REAL with one
    # meaningful decimal (0.3 = Жигули, 1.0 = Labo, 5.0 = Isuzu).
    if "vehicle_capacity_tons" not in user_cols_vehicle:
        conn.execute("ALTER TABLE users ADD COLUMN vehicle_capacity_tons REAL")

    # orders.assigned_agent_id / assigned_at / delivery_status — Block A
    # adds the schema only; admin dispatch flow ships in Block B. Atomic
    # claim via WHERE delivery_status='open' (TOCTOU_FIRST_WRITE_WINS,
    # Error Log #37). Existing orders are tagged 'legacy' so they don't
    # appear in the dispatch queue; new orders default to 'open'.
    order_cols_dispatch = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
    if "assigned_agent_id" not in order_cols_dispatch:
        conn.execute("ALTER TABLE orders ADD COLUMN assigned_agent_id INTEGER")
    if "assigned_at" not in order_cols_dispatch:
        conn.execute("ALTER TABLE orders ADD COLUMN assigned_at TEXT")
    if "delivery_status" not in order_cols_dispatch:
        # SQLite ALTER TABLE doesn't support CHECK; the application enforces
        # the enum {open, assigned, in_transit, delivered, cancelled}.
        conn.execute(
            "ALTER TABLE orders ADD COLUMN delivery_status TEXT DEFAULT 'open'"
        )
        # Backfill historical orders to 'legacy' so they don't pollute the
        # dispatch queue. Only orders created after this migration get 'open'.
        conn.execute(
            "UPDATE orders SET delivery_status = 'legacy' "
            "WHERE delivery_status IS NULL OR delivery_status = 'open'"
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_orders_delivery_status "
        "ON orders(delivery_status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_orders_assigned_agent "
        "ON orders(assigned_agent_id)"
    )

    # Block C — agent self-registration queue. Audit-first per zero-data-loss
    # rule. Each form submission inserts a row immediately; admin approval
    # flips status to 'approved' and inserts/updates the matching users row
    # with agent_role='agent'. notify_message_id tracks the admin-group
    # message so the approval handler can edit it post-decision.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pending_agents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            phone_raw TEXT,
            phone_normalized TEXT NOT NULL,
            vehicle TEXT,
            status TEXT DEFAULT 'pending',
            requested_at TEXT DEFAULT (datetime('now')),
            approved_by_telegram_id INTEGER,
            approved_at TEXT,
            rejected_by_telegram_id INTEGER,
            rejected_at TEXT,
            reject_reason TEXT,
            notify_message_id INTEGER,
            error_message TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pending_agents_telegram "
        "ON pending_agents(telegram_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pending_agents_phone "
        "ON pending_agents(phone_normalized)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pending_agents_status "
        "ON pending_agents(status)"
    )
    # Additive: capture self-reported vehicle capacity on the audit row too,
    # so approval-side logic can copy it through to users.vehicle_capacity_tons
    # without re-asking the agent.
    pa_cols = {row[1] for row in conn.execute("PRAGMA table_info(pending_agents)").fetchall()}
    if "vehicle_capacity_tons" not in pa_cols:
        conn.execute("ALTER TABLE pending_agents ADD COLUMN vehicle_capacity_tons REAL")

    # One-time backfill: seed daily_fx_rate_events from existing daily_fx_rates
    # so the agent FX banner has history from day one. Runs only when the
    # events table is empty; subsequent /fxrate sets append normally.
    fx_events_count = conn.execute(
        "SELECT COUNT(*) FROM daily_fx_rate_events"
    ).fetchone()[0]
    if fx_events_count == 0:
        conn.execute(
            """INSERT INTO daily_fx_rate_events
               (rate_date, currency_pair, rate, set_at, set_by_user_id, set_by_name)
               SELECT rate_date, currency_pair, rate, created_at,
                      uploaded_by_user_id, uploaded_by_name
               FROM daily_fx_rates"""
        )

    # Session L: loyalty points system
    conn.execute("""
        CREATE TABLE IF NOT EXISTS client_points_monthly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            client_name TEXT NOT NULL,
            month TEXT NOT NULL,
            purchase_uzs REAL DEFAULT 0,
            purchase_usd REAL DEFAULT 0,
            purchase_points INTEGER DEFAULT 0,
            discipline_grade TEXT DEFAULT 'C',
            multiplier REAL DEFAULT 1.0,
            clean_sheet_bonus INTEGER DEFAULT 0,
            effective_points INTEGER DEFAULT 0,
            volume_bucket TEXT DEFAULT 'Micro',
            bucket_rank INTEGER,
            bucket_total INTEGER,
            calculated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(client_id, month)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_client_points_month
        ON client_points_monthly(month)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_client_points_client
        ON client_points_monthly(client_id)
    """)

    # Track when a product last had positive stock (for active product detection)
    prod_cols = {row[1] for row in conn.execute("PRAGMA table_info(products)").fetchall()}
    if "stock_last_positive_at" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN stock_last_positive_at TEXT")
        conn.execute(
            "UPDATE products SET stock_last_positive_at = stock_updated_at "
            "WHERE stock_quantity > 0 AND stock_updated_at IS NOT NULL"
        )

    # Track when a product last appeared in any /stock upload (regardless of qty).
    # Drives the "rolling 60-day active set" rule used by /cleanupinactive.
    if "stock_last_seen_at" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN stock_last_seen_at TEXT")
        # Bootstrap: best available proxy is stock_last_positive_at (last time
        # the row had qty > 0 in an upload). Imperfect but immediately usable.
        conn.execute(
            "UPDATE products SET stock_last_seen_at = stock_last_positive_at "
            "WHERE stock_last_positive_at IS NOT NULL"
        )

    # Stamp the moment a product flips from positive stock → 0. Drives the
    # daily 09:00 inventory delta ("BUGUN TUGAGAN"). Left NULL on bootstrap so
    # historical stockouts don't all flood the first delta message after deploy;
    # populates organically as /stock imports flip items going forward.
    if "stockout_at" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN stockout_at TEXT")

    # Stamp the moment a product flips from 0 → positive stock. Drives the
    # daily 09:00 "♻️ BUGUN TO'LDIRILDI" line. Same NULL-on-bootstrap rule as
    # stockout_at — populates organically on the next /stock import that
    # restocks an item.
    if "restocked_at" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN restocked_at TEXT")

    # Latest supplier per product — drives /zakazlar Phase 1 (per-supplier
    # reorder view in inventory group). Stamped during /supply import (the
    # most recent counterparty seen for each matched product). Backfilled in
    # v12 migration block from supply_order_items + counterparty_name lookup.
    # NULL = product never appeared in any /supply import (~65% of catalog
    # at v12 launch — surfaces in /zakazlar under "(noma'lum supplier)" bucket).
    if "latest_supplier_id" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN latest_supplier_id INTEGER")
    if "latest_supplied_at" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN latest_supplied_at TEXT")

    # When this product row was inserted. Stamped by /prices auto-add and
    # /realorders ingest_unmatched_skus. NULL on rows that pre-date this
    # column — those legacy rows are still surfaced in the admin
    # "Yangi mahsulotlar" review queue but with "import date unknown".
    if "created_at" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN created_at TEXT")

    # 1 when the (category, producer) was assigned by brand-prefix match
    # (backend.services.product_classifier); 0 when the importer fell back
    # to "Yangi mahsulotlar" + "Boshqa". Cleared to 0 when an admin
    # manually reassigns the product (review-queue flow). Used to sort
    # the review queue: unclassified rows go first.
    if "auto_classified" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN auto_classified INTEGER DEFAULT 0")

    # Number of pieces per wholesale pack (col K "Qadoqdagi soni" in
    # Rassvet_Master.xlsx). NULL = pack size not yet curated; UI falls
    # back to per-piece display. When set, mini-app catalog shows a
    # derived per-pack price line and the product-detail page renders a
    # secondary "Add 1 pack" button.
    if "package_quantity" not in prod_cols:
        conn.execute("ALTER TABLE products ADD COLUMN package_quantity INTEGER")

    # Audit trail for products.package_quantity writes. Currently populated by
    # tools/backfill_pack_qty.py (supply_gcd source); future writes from the
    # admin cleanup-tab review queue should also append here. Every row is
    # reversible: read old_value, UPDATE products SET package_quantity = old.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pack_qty_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            old_value INTEGER,
            new_value INTEGER NOT NULL,
            source TEXT NOT NULL,
            confidence TEXT NOT NULL,
            supply_count INTEGER NOT NULL,
            applied_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pack_qty_audit_product ON pack_qty_audit(product_id)")

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

    # Cashbook Phase 1 (2026-04-29): bot/Mini-App payment intake.
    # intake_payments — confirmed/pending payments captured outside 1C, sits
    # parallel to client_payments (the 1C kassa import) until reconciliation
    # is clean for ~2 weeks, then becomes source of truth for collected money.
    # payment_intake_raw — audit-first row written before any matching, per
    # the zero-data-loss rule. dedicated_cards seeded for P2P (Phase 2).
    # payment_reconciliation populated nightly from Phase 3 onward.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dedicated_cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            card_number TEXT NOT NULL,
            holder_first_name TEXT NOT NULL,
            holder_last_name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            retired_at TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dedicated_cards_active ON dedicated_cards(active)")

    # Seed initial P2P destination cards (idempotent — checks card_number)
    _seed_dedicated_cards(conn)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS payment_intake_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            submitted_at TEXT NOT NULL DEFAULT (datetime('now')),
            submitter_telegram_id INTEGER NOT NULL,
            submitter_role TEXT NOT NULL,
            raw_payload TEXT NOT NULL,
            processed_payment_id INTEGER,
            notes TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_payment_intake_raw_submitter ON payment_intake_raw(submitter_telegram_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_payment_intake_raw_submitted ON payment_intake_raw(submitted_at)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS intake_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            currency TEXT NOT NULL CHECK(currency IN ('UZS', 'USD')),
            channel TEXT NOT NULL,
            card_id INTEGER,
            handover_agent_id INTEGER,
            submitter_telegram_id INTEGER NOT NULL,
            submitter_role TEXT NOT NULL,
            confirmed_by_telegram_id INTEGER,
            submitted_at TEXT NOT NULL DEFAULT (datetime('now')),
            confirmed_at TEXT,
            rejected_at TEXT,
            reject_reason TEXT,
            status TEXT NOT NULL CHECK(status IN ('pending_handover', 'pending_review', 'confirmed', 'rejected')),
            screenshot_file_id TEXT,
            notes TEXT,
            source_intake_raw_id INTEGER NOT NULL,
            replaces_payment_id INTEGER,
            gross_uzs REAL,
            accepted_pct REAL,
            fx_rate_uzs_per_usd REAL,
            kassa_date TEXT,
            FOREIGN KEY (client_id) REFERENCES allowed_clients(id),
            FOREIGN KEY (card_id) REFERENCES dedicated_cards(id),
            FOREIGN KEY (source_intake_raw_id) REFERENCES payment_intake_raw(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_status ON intake_payments(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_client ON intake_payments(client_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_submitter ON intake_payments(submitter_telegram_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_submitted ON intake_payments(submitted_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_confirmed ON intake_payments(confirmed_at)")

    # 2026-05-08 — bank-transfer migration. The original schema had
    # CHECK(channel IN ('cash_direct','cash_via_agent','p2p')) which
    # rejects 'bank_transfer'. SQLite has no DROP CONSTRAINT, so existing
    # DBs need a one-time table rebuild. Channel validation now lives in
    # backend/services/payment_intake.py. Also adds three columns for the
    # bank-transfer flow: gross UZS, accepted %, FX rate. This is the only
    # place we deviate from CLAUDE.md's "additive ALTER only" rule —
    # logged in obsidian-vault/🚨 Rule Violations.md.
    existing_ip_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='intake_payments'"
    ).fetchone()
    existing_ip_sql_text = (existing_ip_sql["sql"] if existing_ip_sql else "") or ""
    needs_channel_check_drop = (
        "CHECK(channel IN ('cash_direct', 'cash_via_agent', 'p2p'))" in existing_ip_sql_text
    )
    ip_cols = {row[1] for row in conn.execute("PRAGMA table_info(intake_payments)").fetchall()}

    if needs_channel_check_drop:
        pre_count = conn.execute("SELECT COUNT(*) AS n FROM intake_payments").fetchone()["n"]
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("""
            CREATE TABLE intake_payments_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL CHECK(currency IN ('UZS', 'USD')),
                channel TEXT NOT NULL,
                card_id INTEGER,
                handover_agent_id INTEGER,
                submitter_telegram_id INTEGER NOT NULL,
                submitter_role TEXT NOT NULL,
                confirmed_by_telegram_id INTEGER,
                submitted_at TEXT NOT NULL DEFAULT (datetime('now')),
                confirmed_at TEXT,
                rejected_at TEXT,
                reject_reason TEXT,
                status TEXT NOT NULL CHECK(status IN ('pending_handover', 'pending_review', 'confirmed', 'rejected')),
                screenshot_file_id TEXT,
                notes TEXT,
                source_intake_raw_id INTEGER NOT NULL,
                replaces_payment_id INTEGER,
                gross_uzs REAL,
                accepted_pct REAL,
                fx_rate_uzs_per_usd REAL,
                kassa_date TEXT,
                FOREIGN KEY (client_id) REFERENCES allowed_clients(id),
                FOREIGN KEY (card_id) REFERENCES dedicated_cards(id),
                FOREIGN KEY (source_intake_raw_id) REFERENCES payment_intake_raw(id)
            )
        """)
        legacy_cols = [
            "id", "client_id", "amount", "currency", "channel", "card_id",
            "handover_agent_id", "submitter_telegram_id", "submitter_role",
            "confirmed_by_telegram_id", "submitted_at", "confirmed_at",
            "rejected_at", "reject_reason", "status", "screenshot_file_id",
            "notes", "source_intake_raw_id",
        ]
        if "replaces_payment_id" in ip_cols:
            legacy_cols.append("replaces_payment_id")
        cols_csv = ", ".join(legacy_cols)
        conn.execute(
            f"INSERT INTO intake_payments_new ({cols_csv}) "
            f"SELECT {cols_csv} FROM intake_payments"
        )
        post_count = conn.execute(
            "SELECT COUNT(*) AS n FROM intake_payments_new"
        ).fetchone()["n"]
        if post_count != pre_count:
            conn.execute("DROP TABLE intake_payments_new")
            conn.execute("PRAGMA foreign_keys=ON")
            raise RuntimeError(
                f"intake_payments rebuild aborted: row count mismatch "
                f"(was {pre_count}, copied {post_count})"
            )
        conn.execute("DROP TABLE intake_payments")
        conn.execute("ALTER TABLE intake_payments_new RENAME TO intake_payments")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_status ON intake_payments(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_client ON intake_payments(client_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_submitter ON intake_payments(submitter_telegram_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_submitted ON intake_payments(submitted_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_confirmed ON intake_payments(confirmed_at)")
        conn.execute("PRAGMA foreign_keys=ON")
        ip_cols = {row[1] for row in conn.execute("PRAGMA table_info(intake_payments)").fetchall()}
    else:
        if "replaces_payment_id" not in ip_cols:
            conn.execute("ALTER TABLE intake_payments ADD COLUMN replaces_payment_id INTEGER")
        if "gross_uzs" not in ip_cols:
            conn.execute("ALTER TABLE intake_payments ADD COLUMN gross_uzs REAL")
        if "accepted_pct" not in ip_cols:
            conn.execute("ALTER TABLE intake_payments ADD COLUMN accepted_pct REAL")
        if "fx_rate_uzs_per_usd" not in ip_cols:
            conn.execute("ALTER TABLE intake_payments ADD COLUMN fx_rate_uzs_per_usd REAL")
        # 2026-05-28 — kassa_date for back-dated cashier intake (Z, back-date
        # picker). NULL = same as date(submitted_at) (today); set = cash-flow
        # date overrides submitted_at for reconciliation. /bugunpul keeps
        # filtering by submitted_at so back-dated rows show up in the day
        # they were recorded.
        if "kassa_date" not in ip_cols:
            conn.execute("ALTER TABLE intake_payments ADD COLUMN kassa_date TEXT")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_replaces ON intake_payments(replaces_payment_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intake_payments_kassa_date ON intake_payments(kassa_date)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS payment_reconciliation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reconcile_date TEXT NOT NULL,
            bot_payment_id INTEGER,
            kassa_doc_no TEXT,
            match_status TEXT NOT NULL CHECK(match_status IN ('matched', 'bot_only', 'kassa_only')),
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (bot_payment_id) REFERENCES intake_payments(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_payment_reconciliation_date ON payment_reconciliation(reconcile_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_payment_reconciliation_status ON payment_reconciliation(match_status)")

    # ─────────────────────────────────────────────────────────────
    # Session Z: Cashbook Phase 2 — legal-entity bank transfer routing
    # Procurement categories drive Stage 1 of Option 3 (legal→legal):
    # agent picks a category, uncle picks the supplier in Stage 2.
    # Seeded from Uncle/Suppliers_Master.xlsx column F (locked 2026-05-01).
    # ─────────────────────────────────────────────────────────────

    conn.execute("""
        CREATE TABLE IF NOT EXISTS procurement_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label_uz TEXT NOT NULL UNIQUE,
            label_ru TEXT NOT NULL,
            label_en TEXT NOT NULL,
            sort_order INTEGER DEFAULT 0,
            is_freetext INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_procurement_categories_sort ON procurement_categories(sort_order)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_1c TEXT NOT NULL UNIQUE,
            legal_name TEXT,
            accountant_phone TEXT,
            activity_uzs REAL DEFAULT 0,
            activity_usd REAL DEFAULT 0,
            periods INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_suppliers_active ON suppliers(is_active)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS supplier_categories (
            supplier_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            PRIMARY KEY (supplier_id, category_id),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE CASCADE,
            FOREIGN KEY (category_id) REFERENCES procurement_categories(id) ON DELETE CASCADE
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_supplier_categories_cat ON supplier_categories(category_id)")

    # Seed (idempotent — re-runs are safe)
    from backend.services.seed_procurement import seed_procurement
    seed_procurement(conn)

    # ─────────────────────────────────────────────────────────────
    # Session Z: Cashbook Phase 2 — live transaction record
    # legal_transfers = one row per legal-entity bank transfer request
    # (born at agent Stage 1 submit, lives through 7-stage flow to faktura close)
    # legal_transfer_events = audit chain (every state transition logged)
    # ─────────────────────────────────────────────────────────────

    conn.execute("""
        CREATE TABLE IF NOT EXISTS legal_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            submitted_by_telegram_id INTEGER NOT NULL,
            amount_uzs REAL NOT NULL,
            category_id INTEGER NOT NULL,
            category_freetext TEXT,
            legal_entity_name TEXT NOT NULL,
            legal_entity_inn TEXT NOT NULL,
            guvohnoma_photo_url TEXT,
            supplier_id INTEGER,
            agreement_url TEXT,
            transfer_proof_url TEXT,
            doverennost_url TEXT,
            faktura_url TEXT,
            status TEXT NOT NULL DEFAULT 'submitted'
                CHECK(status IN ('submitted', 'supplier_assigned', 'agreement_received',
                                  'awaiting_client_transfer', 'transfer_proof_uploaded',
                                  'supplier_confirmed', 'doverennost_received', 'closed', 'cancelled')),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (client_id) REFERENCES allowed_clients(id),
            FOREIGN KEY (category_id) REFERENCES procurement_categories(id),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_legal_transfers_status ON legal_transfers(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_legal_transfers_client ON legal_transfers(client_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_legal_transfers_supplier ON legal_transfers(supplier_id)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS legal_transfer_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legal_transfer_id INTEGER NOT NULL,
            from_status TEXT,
            to_status TEXT NOT NULL,
            actor_telegram_id INTEGER NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (legal_transfer_id) REFERENCES legal_transfers(id) ON DELETE CASCADE
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_legal_transfer_events_transfer ON legal_transfer_events(legal_transfer_id)")

    # Migration: add extra_doc_url to legal_transfers (Stage 1 mandatory file)
    lt_cols = {row[1] for row in conn.execute("PRAGMA table_info(legal_transfers)").fetchall()}
    if "extra_doc_url" not in lt_cols:
        conn.execute("ALTER TABLE legal_transfers ADD COLUMN extra_doc_url TEXT")

    # v9 (2026-05-06): supplier curation cleanup. Existing prod rows preserved
    # for FK integrity (legal_transfers.supplier_id); only flip is_active and
    # absorb the merged legal_name. Idempotent — guarded by current < 9.
    if current < 9:
        retired_curation_2026_05_06 = (
            "EAST COLOR /BUILD TECHNO TRADE/",
            "PAINTERA",
            "R O Y A L",
            "Саморез TAGERT",
            'СП ООО "RANGLI B O\' Y O Q"',
        )
        conn.executemany(
            "UPDATE suppliers SET is_active=0 WHERE name_1c=?",
            [(n,) for n in retired_curation_2026_05_06],
        )
        # Detach Stage-2 picker links for both retired-this-pass and merge-source
        # suppliers so they no longer appear in the legal-transfer dropdown.
        detach_names = retired_curation_2026_05_06 + ("ЭКОС /КораСарой/",)
        placeholders = ",".join(["?"] * len(detach_names))
        conn.execute(
            f"""DELETE FROM supplier_categories
                WHERE supplier_id IN (
                  SELECT id FROM suppliers WHERE name_1c IN ({placeholders})
                )""",
            detach_names,
        )
        # Absorb merged legal_name into the surviving RANGLI BO'YOQ row.
        conn.execute(
            "UPDATE suppliers SET legal_name=? WHERE name_1c=? AND (legal_name IS NULL OR legal_name='')",
            ('СП ООО "RANGLI B O\' Y O Q"', "RANGLI BO'YOQ"),
        )

    # v10 (2026-05-06): decouple is_active from mini_app_label. Kept-but-
    # untagged suppliers (real inventory contributors with no Stage-2 picker
    # exposure) should stay is_active=1 so they don't false-trigger the
    # retired_seen alert in import_supply. is_active=0 now means truly retired.
    if current < 10:
        kept_active_2026_05_06 = (
            "PUFA MIX",
            "ПРОЧИЕ",
            "КораСарой/ЭКОС/",
            "ШЛИФ ШКУРКА",
            "ПалИЖ КОЛЛЕР",
            "НОРА ойти",
            "MASHXAD",
        )
        conn.executemany(
            "UPDATE suppliers SET is_active=1 WHERE name_1c=?",
            [(n,) for n in kept_active_2026_05_06],
        )

    # v11 (2026-05-07): seed 5 ADD_AS_NEW suppliers (ПЛИНТУС, ORIGINAL COLORMIX,
    # УГОЛОК, ГУДФИКС, СЕМИКС). seed_procurement() does the INSERT OR IGNORE
    # automatically on init_db; no explicit migration UPDATE needed.

    # v12 (2026-05-07): backfill products.latest_supplier_id + latest_supplied_at
    # from supply_order_items + supply_orders.counterparty_name. ROW_NUMBER()
    # window function picks the most-recent doc per product so the supplier_id
    # ties back to the right doc_date (a plain GROUP BY would pick an arbitrary
    # supplier when a product has multiple supply sources). Re-runs harmlessly
    # on every init.
    if current < 12:
        conn.execute(
            """
            UPDATE products
               SET latest_supplier_id = sub.supplier_id,
                   latest_supplied_at = sub.last_date
              FROM (
                SELECT pid, supplier_id, last_date FROM (
                  SELECT soi.matched_product_id AS pid,
                         s.id AS supplier_id,
                         so.doc_date AS last_date,
                         ROW_NUMBER() OVER (
                             PARTITION BY soi.matched_product_id
                             ORDER BY so.doc_date DESC, so.id DESC
                         ) AS rn
                    FROM supply_order_items soi
                    JOIN supply_orders so ON so.id = soi.supply_order_id
                    JOIN suppliers s ON s.name_1c = so.counterparty_name
                   WHERE soi.matched_product_id IS NOT NULL
                ) WHERE rn = 1
              ) AS sub
             WHERE products.id = sub.pid
            """
        )

    # v13 (2026-05-07 afternoon): 4 synthetic country-of-origin suppliers added
    # to seed (Саморез КИТАЙ, ЛИНОЛЕУМ РОССИЯ, ЛИНОЛЕУМ КАЗАХСТАН, Гвозди /БУХОРО/).
    # seed_procurement() inserts them via INSERT OR IGNORE on this init. We then
    # re-run the same backfill UPDATE as v12 so products supplied through these
    # channels (~130 across the 4) get stamped with their new latest_supplier_id
    # and surface in /zakazlar named buckets instead of (noma'lum). Idempotent.
    if current < 13:
        conn.execute(
            """
            UPDATE products
               SET latest_supplier_id = sub.supplier_id,
                   latest_supplied_at = sub.last_date
              FROM (
                SELECT pid, supplier_id, last_date FROM (
                  SELECT soi.matched_product_id AS pid,
                         s.id AS supplier_id,
                         so.doc_date AS last_date,
                         ROW_NUMBER() OVER (
                             PARTITION BY soi.matched_product_id
                             ORDER BY so.doc_date DESC, so.id DESC
                         ) AS rn
                    FROM supply_order_items soi
                    JOIN supply_orders so ON so.id = soi.supply_order_id
                    JOIN suppliers s ON s.name_1c = so.counterparty_name
                   WHERE soi.matched_product_id IS NOT NULL
                ) WHERE rn = 1
              ) AS sub
             WHERE products.id = sub.pid
            """
        )

    # v15 (2026-05-10): rebuild real_orders + client_payments to swap
    # column-level UNIQUE(doc_number_1c) -> composite UNIQUE(doc_number_1c, doc_date).
    # 1C cycles document numbers per year, so the column-level UNIQUE was a
    # latent IntegrityError waiting on a year-rollover collision (Error Log #20).
    # client_payments was previously fixed only via the manual /api/finance/
    # migrate-payments-unique endpoint; real_orders had the same bug with no
    # migration shipped. Both fixed here, idempotently. Detection via
    # sqlite_master.sql introspection — already-migrated tables no-op.
    # foreign_keys=OFF is critical: real_order_items references real_orders(id)
    # ON DELETE CASCADE, so dropping real_orders without it would wipe items.
    if current < 15:
        ro_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='real_orders'"
        ).fetchone()
        cp_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='client_payments'"
        ).fetchone()
        ro_sql = (ro_row["sql"] if ro_row else "") or ""
        cp_sql = (cp_row["sql"] if cp_row else "") or ""
        ro_needs = (
            "doc_number_1c TEXT NOT NULL UNIQUE" in ro_sql
            and "UNIQUE(doc_number_1c, doc_date)" not in ro_sql
        )
        cp_needs = (
            "doc_number_1c TEXT NOT NULL UNIQUE" in cp_sql
            and "UNIQUE(doc_number_1c, doc_date)" not in cp_sql
        )

        if ro_needs or cp_needs:
            conn.execute("PRAGMA foreign_keys=OFF")
            try:
                if ro_needs:
                    pre = conn.execute("SELECT COUNT(*) AS n FROM real_orders").fetchone()["n"]
                    conn.execute("""
                        CREATE TABLE real_orders_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            doc_number_1c TEXT NOT NULL,
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
                            imported_at TEXT DEFAULT (datetime('now')),
                            UNIQUE(doc_number_1c, doc_date)
                        )
                    """)
                    ro_cols = (
                        "id, doc_number_1c, doc_date, doc_time, client_name_1c, "
                        "client_id, contract, storage_location, payment_account, "
                        "sale_agent, responsible_person, comment, currency, "
                        "exchange_rate, total_sum, total_sum_currency, total_weight, "
                        "item_count, imported_at"
                    )
                    conn.execute(
                        f"INSERT INTO real_orders_new ({ro_cols}) "
                        f"SELECT {ro_cols} FROM real_orders"
                    )
                    post = conn.execute("SELECT COUNT(*) AS n FROM real_orders_new").fetchone()["n"]
                    if post != pre:
                        conn.execute("DROP TABLE real_orders_new")
                        raise RuntimeError(
                            f"real_orders rebuild aborted: row count mismatch "
                            f"(was {pre}, copied {post})"
                        )
                    conn.execute("DROP TABLE real_orders")
                    conn.execute("ALTER TABLE real_orders_new RENAME TO real_orders")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_real_orders_client_id ON real_orders(client_id)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_real_orders_client_name ON real_orders(client_name_1c)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_real_orders_doc_date ON real_orders(doc_date)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_real_orders_doc_number ON real_orders(doc_number_1c)")
                    print(f"[init_db v15] real_orders rebuilt with composite UNIQUE: {pre} rows preserved")

                if cp_needs:
                    pre = conn.execute("SELECT COUNT(*) AS n FROM client_payments").fetchone()["n"]
                    conn.execute("""
                        CREATE TABLE client_payments_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            doc_number_1c TEXT NOT NULL,
                            doc_date TEXT NOT NULL,
                            doc_time TEXT,
                            author TEXT,
                            received_from TEXT,
                            basis TEXT,
                            attachment TEXT,
                            corr_account TEXT,
                            client_name_1c TEXT,
                            client_id INTEGER,
                            subconto2 TEXT,
                            subconto3 TEXT,
                            currency TEXT DEFAULT 'UZS',
                            amount_local REAL DEFAULT 0,
                            amount_currency REAL DEFAULT 0,
                            fx_rate REAL DEFAULT 0,
                            cashflow_category TEXT,
                            imported_at TEXT DEFAULT (datetime('now')),
                            UNIQUE(doc_number_1c, doc_date)
                        )
                    """)
                    cp_cols = (
                        "id, doc_number_1c, doc_date, doc_time, author, received_from, "
                        "basis, attachment, corr_account, client_name_1c, client_id, "
                        "subconto2, subconto3, currency, amount_local, amount_currency, "
                        "fx_rate, cashflow_category, imported_at"
                    )
                    conn.execute(
                        f"INSERT INTO client_payments_new ({cp_cols}) "
                        f"SELECT {cp_cols} FROM client_payments"
                    )
                    post = conn.execute("SELECT COUNT(*) AS n FROM client_payments_new").fetchone()["n"]
                    if post != pre:
                        conn.execute("DROP TABLE client_payments_new")
                        raise RuntimeError(
                            f"client_payments rebuild aborted: row count mismatch "
                            f"(was {pre}, copied {post})"
                        )
                    conn.execute("DROP TABLE client_payments")
                    conn.execute("ALTER TABLE client_payments_new RENAME TO client_payments")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_client_payments_doc_date ON client_payments(doc_date)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_client_payments_client_name ON client_payments(client_name_1c)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_client_payments_client_id ON client_payments(client_id)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_client_payments_currency ON client_payments(currency)")
                    print(f"[init_db v15] client_payments rebuilt with composite UNIQUE: {pre} rows preserved")
            finally:
                conn.execute("PRAGMA foreign_keys=ON")

    # v14 (2026-05-10): three missing indexes the foundation audit surfaced.
    # All additive — no rows touched, easily droppable if anything goes sideways.
    #  - orders.client_id: every cabinet load full-scans without it
    #  - users.client_id: used in import-loop joins, latent quadratic risk
    #  - allowed_clients.phone_normalized UNIQUE partial: previously created
    #    only by tools/dedup_allowed_clients.py (Error Log #41). Fresh deploys
    #    that skipped the tool re-accumulated dupes silently. If active dupes
    #    still exist, we skip the unique index and log loudly — operator must
    #    run the dedup tool before retrying. Better than IntegrityError on init.
    if current < 14:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_client_id ON users(client_id)")
        dupe = conn.execute(
            "SELECT phone_normalized, COUNT(*) AS n "
            "FROM allowed_clients "
            "WHERE phone_normalized IS NOT NULL "
            "  AND phone_normalized != '' "
            "  AND COALESCE(status,'active') = 'active' "
            "GROUP BY phone_normalized HAVING n > 1 "
            "LIMIT 1"
        ).fetchone()
        if dupe is None:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_allowed_phone_unique "
                "ON allowed_clients(phone_normalized) "
                "WHERE phone_normalized IS NOT NULL "
                "  AND phone_normalized != '' "
                "  AND COALESCE(status,'active') = 'active'"
            )
        else:
            print(
                f"[init_db v14] WARNING: skipping idx_allowed_phone_unique — "
                f"duplicate active rows exist (e.g. phone={dupe[0]!r}, count={dupe[1]}). "
                f"Run tools/dedup_allowed_clients.py and redeploy to land the unique index."
            )

    # v16 (2026-05-21): admin-managed balance overrides. Per-client authoritative
    # value that get_effective_debt() reads BEFORE the daily-upload picker. Solves
    # the structural "1C Дебиторская excludes credit-balance clients → cabinet
    # shows 0 when client actually has money in their favor" gap (Бахтиёр case).
    # Override is intentionally soft — no FK cascade — so a client_id retirement
    # doesn't silently nuke an audit-trail row. Audit fields (set_by_user_id,
    # set_by_name, source, set_at) make every override traceable to a human
    # decision + the 1C document/report it was verified against.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS client_balance_overrides (
            client_id INTEGER PRIMARY KEY,
            debt_uzs REAL NOT NULL DEFAULT 0,
            debt_usd REAL NOT NULL DEFAULT 0,
            source TEXT,
            reason TEXT,
            set_by_user_id INTEGER,
            set_by_name TEXT,
            set_at TEXT NOT NULL DEFAULT (datetime('now')),
            expires_at TEXT,
            notes TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_balance_overrides_set_at ON client_balance_overrides(set_at)")

    # v18 (2026-05-27): real_orders.is_approved + first_pending_at — capture
    # the 1C col-0 marker (V/X) at import time so customer app + top buyers
    # + revenue queries can filter to approved-only. Additive ALTER, NULL
    # default; new uploads populate, legacy rows stay NULL.
    ro_cols = {row[1] for row in conn.execute("PRAGMA table_info(real_orders)").fetchall()}
    if "is_approved" not in ro_cols:
        conn.execute("ALTER TABLE real_orders ADD COLUMN is_approved INTEGER")
    if "first_pending_at" not in ro_cols:
        conn.execute("ALTER TABLE real_orders ADD COLUMN first_pending_at TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_real_orders_is_approved ON real_orders(is_approved)")

    # reminder_fire_log — bot/reminders.py uses this to dedup catch-up fires
    # on restart (Error Log #32 vs #NN). Each successful fire stamps a row;
    # the startup catch-up grace window only fires if today's slot is absent.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reminder_fire_log (
            reminder_name TEXT NOT NULL,
            fire_date     TEXT NOT NULL,
            fired_at_utc  TEXT NOT NULL,
            PRIMARY KEY(reminder_name, fire_date)
        )
    """)
    # Prune rows older than 30 days — keeps the table bounded (~15 rows/day).
    conn.execute(
        "DELETE FROM reminder_fire_log WHERE fire_date < date('now', '-30 days')"
    )

    # Stamp schema version if newer
    if current < SCHEMA_VERSION:
        conn.execute(
            "INSERT INTO schema_version (version, description) VALUES (?, ?)",
            (SCHEMA_VERSION, "v17 = reminder_fire_log table — dedup catch-up fires for daily reminders. After 2026-05-20's CRON_RESTART_PAST_FIRE_DROPS_DAY mitigation (catch-up within 4h grace), the new failure mode was the opposite: restart-after-fire re-fires every morning reminder. v17 records each successful fire so the catch-up branch can skip slots already fired today. Earlier history: v15 = real_orders + client_payments composite UNIQUE; v16 = client_balance_overrides."),
        )

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


def _seed_dedicated_cards(conn):
    """Seed the initial P2P destination cards. Idempotent by card_number —
    if a row with that number exists (active or retired), don't touch it.
    Admin can later add/retire cards via the /cards bot command without
    init_db re-introducing retired ones.
    """
    initial = [
        ("9860180107087421", "Iskandar", "Ibragimov"),
        ("5614682417512581", "Dildora",  "Rahmatova"),
    ]
    for num, first, last in initial:
        existing = conn.execute(
            "SELECT 1 FROM dedicated_cards WHERE card_number = ? LIMIT 1",
            (num,),
        ).fetchone()
        if existing:
            continue
        conn.execute(
            """INSERT INTO dedicated_cards
                  (card_number, holder_first_name, holder_last_name)
               VALUES (?, ?, ?)""",
            (num, first, last),
        )
    conn.commit()


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
