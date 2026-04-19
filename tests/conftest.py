"""Shared fixtures for the test suite."""
import os
import sys
import sqlite3
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_test_dir = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(_test_dir, "test.db")

_counter = 0


@pytest.fixture
def db():
    """Fresh temp DB with full schema. Yields a connection."""
    global _counter
    _counter += 1
    db_path = os.path.join(_test_dir, f"test_{_counter}.db")
    os.environ["DATABASE_PATH"] = db_path

    from backend import database
    database.DATABASE_PATH = db_path

    database.init_db()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.create_function("LOWER", 1, lambda s: s.lower() if s else s)
    yield conn
    conn.close()
    try:
        os.unlink(db_path)
    except OSError:
        pass


@pytest.fixture
def seed_products(db):
    """Insert a minimal set of test products."""
    db.execute("INSERT OR IGNORE INTO categories (id, name) VALUES (1, 'Краски')")
    db.execute("INSERT OR IGNORE INTO producers (id, name, product_count) VALUES (1, 'Weber', 5)")
    db.execute("INSERT OR IGNORE INTO producers (id, name, product_count) VALUES (2, 'Gamma', 3)")

    products = [
        (1, "ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/", "Standart Oq", 1, 1, 17.0, None, "шт", 10, 1),
        (2, "ГАММА ЭКО БОРДО /2,5 кг/", "Eko Bordo", 1, 2, 5.0, None, "шт", 2.5, 1),
        (3, "ГАММА ЭКО ЗЕЛЕН /2,5 кг/", "Eko Yashil", 1, 2, 5.0, None, "шт", 2.5, 1),
        (4, "Электрод ARSENAL-2 /15кг/", "Elektrod 2", 1, 1, None, 9.5, "кг", 15, 1),
        (5, "ПУФА МИКС №8 /10кг/", "Pufa Mix 8", 1, 1, None, 3.2, "шт", 10, 1),
    ]
    for pid, name, name_disp, cat_id, prod_id, price_usd, price_uzs, unit, weight, active in products:
        db.execute(
            """INSERT OR REPLACE INTO products (id, name, name_display, category_id, producer_id,
                                     price_usd, price_uzs, unit, weight, is_active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (pid, name, name_disp, cat_id, prod_id, price_usd, price_uzs, unit, weight, active),
        )
    db.commit()
    return db
