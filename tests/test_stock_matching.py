"""Tests for stock importer name matching and normalization."""
from backend.services.update_stock import normalize_name


class TestNormalizeName:
    def test_basic_lowercase(self):
        assert normalize_name("ВЭБЕР") == "вэбер"

    def test_trailing_dots(self):
        assert normalize_name("грунт.") == "грунт"
        assert normalize_name("грунт. ") == "грунт"

    def test_spaces_around_slashes(self):
        # Leading/trailing slashes get stripped as punctuation
        result = normalize_name("/ 20 кг /")
        assert "20 кг" in result

    def test_trailing_weight_suffix(self):
        result = normalize_name("Дюбель гвоздь 8х60 /500 шт/ 5кг")
        assert "5кг" not in result
        assert "дюбель" in result

    def test_cyrillic_x_to_latin(self):
        result = normalize_name("8х60")
        assert "x" in result
        assert "х" not in result

    def test_whitespace_collapse(self):
        assert normalize_name("ВЭБЕР   в/э   ВНУТР") == "вэбер в/э внутр"

    def test_empty_string(self):
        assert normalize_name("") == ""
        assert normalize_name(None) == ""

    def test_mixed_punctuation(self):
        result = normalize_name("—ВЭБЕР—")
        assert result == "вэбер"


class TestAliasLookup:
    def test_alias_table_seeded(self, seed_products):
        db = seed_products
        db.execute(
            "INSERT INTO product_aliases (alias_name, alias_name_lower, product_id, source) "
            "VALUES (?, ?, ?, ?)",
            ("ВЭБЕР СТАНДАРТ 10", "вэбер стандарт 10", 1, "test"),
        )
        db.commit()
        row = db.execute(
            "SELECT product_id FROM product_aliases WHERE alias_name_lower = ?",
            ("вэбер стандарт 10",),
        ).fetchone()
        assert row is not None
        assert row["product_id"] == 1

    def test_alias_unique_constraint(self, seed_products):
        db = seed_products
        db.execute(
            "INSERT INTO product_aliases (alias_name, alias_name_lower, product_id, source) "
            "VALUES (?, ?, ?, ?)",
            ("Test", "test", 1, "test"),
        )
        db.commit()
        db.execute(
            "INSERT OR IGNORE INTO product_aliases (alias_name, alias_name_lower, product_id, source) "
            "VALUES (?, ?, ?, ?)",
            ("Test", "test", 2, "test"),
        )
        db.commit()
        row = db.execute("SELECT product_id FROM product_aliases WHERE alias_name_lower = 'test'").fetchone()
        assert row["product_id"] == 1

    def test_unmatched_logging(self, seed_products):
        db = seed_products
        db.execute(
            "INSERT INTO unmatched_import_names (name, name_lower, source) VALUES (?, ?, ?)",
            ("НОВЫЙ ТОВАР", "новый товар", "stock"),
        )
        db.commit()
        row = db.execute("SELECT * FROM unmatched_import_names WHERE name_lower = 'новый товар'").fetchone()
        assert row is not None
        assert row["occurrences"] == 1
        assert row["resolved"] == 0
