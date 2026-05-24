"""Tests for backend.services.order_diff."""

import pytest

from backend.services.order_diff import (
    compute_order_diff,
    format_diff_for_client,
    format_diff_for_sotuv,
)


def _create_order(db, order_id: int, telegram_id: int = 100):
    db.execute(
        "INSERT INTO orders (id, telegram_id, client_name, status) VALUES (?, ?, ?, 'submitted')",
        (order_id, telegram_id, "Test Client"),
    )
    db.commit()


def _add_item(db, order_id: int, product_id: int, product_name: str,
              quantity: int, price: float, currency: str = "USD"):
    db.execute(
        """INSERT INTO order_items (order_id, product_id, product_name, quantity, price, currency)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (order_id, product_id, product_name, quantity, price, currency),
    )
    db.commit()


def _real(name: str, qty: float, *, price_uzs: float = 0, price_usd: float = 0):
    return {
        "name": name,
        "qty": qty,
        "price_uzs": price_uzs,
        "price_usd": price_usd,
        "total_uzs": price_uzs * qty,
        "total_usd": price_usd * qty,
    }


# ── Match / no-diff ──────────────────────────────────────────────────


def test_no_diff_when_all_match(seed_products):
    db = seed_products
    _create_order(db, 1)
    _add_item(db, 1, 1, "Standart Oq", 10, 17.0, "USD")
    real = [_real("ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/", 10, price_usd=17.0)]

    diff = compute_order_diff(1, real)
    assert diff["has_diff"] is False
    assert diff["counts"] == {"changed": 0, "missing": 0, "added": 0}
    assert format_diff_for_client(diff) == ""
    assert format_diff_for_sotuv(diff) == ""


def test_sub_epsilon_price_change_is_not_diff(seed_products):
    db = seed_products
    _create_order(db, 1)
    _add_item(db, 1, 1, "Standart Oq", 10, 17.00, "USD")
    real = [_real("ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/", 10, price_usd=17.001)]

    diff = compute_order_diff(1, real)
    assert diff["has_diff"] is False


# ── Single-axis changes ──────────────────────────────────────────────


def test_qty_change_detected(seed_products):
    db = seed_products
    _create_order(db, 1)
    _add_item(db, 1, 1, "Standart Oq", 10, 17.0, "USD")
    real = [_real("ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/", 8, price_usd=17.0)]

    diff = compute_order_diff(1, real)
    assert diff["has_diff"] is True
    assert diff["counts"]["changed"] == 1
    line = diff["lines"][0]
    assert line["type"] == "changed"
    assert line["wish_qty"] == 10
    assert line["real_qty"] == 8
    assert line["qty_delta"] == -2

    msg = format_diff_for_client(diff)
    assert "10→8 dona" in msg
    assert "−2" in msg  # signed delta uses U+2212


def test_uzs_price_change_detected(seed_products):
    db = seed_products
    _create_order(db, 1)
    _add_item(db, 1, 2, "Eko Bordo", 5, 12_000, "UZS")
    real = [_real("ГАММА ЭКО БОРДО /2,5 кг/", 5, price_uzs=12_500)]

    diff = compute_order_diff(1, real)
    assert diff["has_diff"] is True
    assert diff["counts"]["changed"] == 1
    msg = format_diff_for_client(diff)
    assert "12 000 → 12 500 so'm" in msg


def test_usd_price_change_detected(seed_products):
    db = seed_products
    _create_order(db, 1)
    _add_item(db, 1, 1, "Standart Oq", 10, 17.0, "USD")
    real = [_real("ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/", 10, price_usd=18.5)]

    diff = compute_order_diff(1, real)
    assert diff["has_diff"] is True
    msg = format_diff_for_client(diff)
    assert "$17.00 → $18.50" in msg


# ── Missing / added ─────────────────────────────────────────────────


def test_missing_item_detected(seed_products):
    db = seed_products
    _create_order(db, 1)
    _add_item(db, 1, 1, "Standart Oq", 10, 17.0, "USD")
    _add_item(db, 1, 2, "Eko Bordo", 5, 12_000, "UZS")
    real = [_real("ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/", 10, price_usd=17.0)]

    diff = compute_order_diff(1, real)
    assert diff["counts"] == {"changed": 0, "missing": 1, "added": 0}
    missing = next(L for L in diff["lines"] if L["type"] == "missing")
    assert missing["name"] == "Eko Bordo"
    assert missing["wish_qty"] == 5

    msg = format_diff_for_client(diff)
    assert "Eko Bordo" in msg
    assert "(yo'q)" in msg


def test_added_item_with_known_product(seed_products):
    db = seed_products
    _create_order(db, 1)
    _add_item(db, 1, 1, "Standart Oq", 10, 17.0, "USD")
    real = [
        _real("ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/", 10, price_usd=17.0),
        _real("ГАММА ЭКО БОРДО /2,5 кг/", 3, price_uzs=12_000),
    ]

    diff = compute_order_diff(1, real)
    assert diff["counts"] == {"changed": 0, "missing": 0, "added": 1}
    added = next(L for L in diff["lines"] if L["type"] == "added")
    # Resolved via products.name → Latin display picked up
    assert added["name"] == "Eko Bordo"
    assert added["real_qty"] == 3


def test_added_item_with_unknown_product(seed_products):
    db = seed_products
    _create_order(db, 1)
    _add_item(db, 1, 1, "Standart Oq", 10, 17.0, "USD")
    real = [
        _real("ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/", 10, price_usd=17.0),
        _real("Совершенно новый товар БРЕНДА X", 2, price_usd=5.0),
    ]

    diff = compute_order_diff(1, real)
    assert diff["counts"]["added"] == 1
    added = next(L for L in diff["lines"] if L["type"] == "added")
    # No product_id resolution → fall back to the raw 1C name
    assert added["name"] == "Совершенно новый товар БРЕНДА X"


# ── Totals ──────────────────────────────────────────────────────────


def test_totals_delta_dual_currency(seed_products):
    db = seed_products
    _create_order(db, 1)
    _add_item(db, 1, 1, "Standart Oq", 10, 17.0, "USD")       # 170 USD
    _add_item(db, 1, 2, "Eko Bordo", 5, 12_000, "UZS")        # 60 000 UZS
    real = [
        _real("ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/", 8, price_usd=17.0),   # 136 USD
        _real("ГАММА ЭКО БОРДО /2,5 кг/", 5, price_uzs=12_500),         # 62 500 UZS
    ]

    diff = compute_order_diff(1, real)
    assert pytest.approx(diff["totals_delta"]["usd"], abs=0.01) == -34.0
    assert pytest.approx(diff["totals_delta"]["uzs"], abs=1) == 2_500


# ── Mixed + integration shape ───────────────────────────────────────


def test_mixed_diff_summary(seed_products):
    db = seed_products
    _create_order(db, 1)
    _add_item(db, 1, 1, "Standart Oq", 10, 17.0, "USD")    # changed
    _add_item(db, 1, 2, "Eko Bordo", 5, 12_000, "UZS")     # missing
    _add_item(db, 1, 3, "Eko Yashil", 4, 12_000, "UZS")    # matches
    real = [
        _real("ВЭБЕР в/э ВНУТР СТАНДАРТ /10 кг/", 8, price_usd=17.0),
        _real("ГАММА ЭКО ЗЕЛЕН /2,5 кг/", 4, price_uzs=12_000),
        _real("Электрод ARSENAL-2 /15кг/", 2, price_usd=9.5),  # added (known)
    ]

    diff = compute_order_diff(1, real)
    assert diff["counts"] == {"changed": 1, "missing": 1, "added": 1}
    msg = format_diff_for_client(diff, cabinet_url="https://example.test/?v=16")
    # Header present
    assert "Buyurtmangiz 1C ga kiritildi" in msg
    # All three types represented
    assert "Standart Oq" in msg
    assert "Eko Bordo" in msg
    assert "Elektrod 2" in msg
    # Cabinet link
    assert "https://example.test/?v=16" in msg
    # Sotuv echo carries the right count
    echo = format_diff_for_sotuv(diff)
    assert "3 ta farq" in echo


def test_truncation_caps_at_15_lines(seed_products):
    db = seed_products
    _create_order(db, 1)
    # All 5 wish-list products missing; we exceed cap by using duplicate
    # synthetic added lines.
    _add_item(db, 1, 1, "Standart Oq", 1, 1.0, "USD")
    _add_item(db, 1, 2, "Eko Bordo", 1, 1.0, "USD")
    _add_item(db, 1, 3, "Eko Yashil", 1, 1.0, "USD")
    _add_item(db, 1, 4, "Elektrod 2", 1, 1.0, "USD")
    _add_item(db, 1, 5, "Pufa Mix 8", 1, 1.0, "USD")
    real = [_real(f"Unknown #{i}", 1, price_usd=1.0) for i in range(20)]

    diff = compute_order_diff(1, real)
    # 5 missing + 20 added = 25 lines
    assert diff["counts"]["missing"] == 5
    assert diff["counts"]["added"] == 20

    msg = format_diff_for_client(diff)
    assert "…va yana 10 ta farq" in msg  # 25 − 15 cap = 10 truncated


# ── HTML safety ─────────────────────────────────────────────────────


def test_html_escapes_product_name(seed_products, db):
    # Insert a product with HTML-sensitive characters directly.
    db.execute(
        """INSERT INTO products (id, name, name_display, category_id, producer_id,
                                 price_usd, unit, weight, is_active)
           VALUES (99, '<script>&', '<script>&', 1, 1, 1.0, 'шт', 1, 1)""",
    )
    db.commit()

    _create_order(db, 1)
    _add_item(db, 1, 99, "<script>&", 5, 1.0, "USD")
    real = []  # missing

    diff = compute_order_diff(1, real)
    msg = format_diff_for_client(diff)
    assert "&lt;script&gt;&amp;" in msg
    assert "<script>" not in msg.replace("&lt;script&gt;", "")
