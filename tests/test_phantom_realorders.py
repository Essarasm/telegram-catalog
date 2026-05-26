"""Tests for list_phantom_realorders + delete_realorder_by_doc_number.

Covers the audit + deletion path introduced after the 2026-05-23 ГУЛШОДА
phantom incident. /realorders is upsert-by-doc_number_1c so a 1C deletion
never reaches us; sales annotates non-shipments with 'ortilmagan' /
'НЕ ОТГРУЖЕНО', which is the detection signal.
"""
import pytest

from backend.services.import_real_orders import (
    list_phantom_realorders,
    delete_realorder_by_doc_number,
)


def _insert(db, **kwargs):
    cols = ", ".join(kwargs.keys())
    qs = ", ".join("?" for _ in kwargs)
    cur = db.execute(
        f"INSERT INTO real_orders ({cols}) VALUES ({qs})",
        tuple(kwargs.values()),
    )
    db.commit()
    return cur.lastrowid


def test_audit_flags_uzbek_and_russian_markers(db):
    _insert(db, doc_number_1c="A1", doc_date="2026-05-23",
            client_name_1c="ГУЛШОДА ОПА ЧЕЛАК", currency="USD",
            total_sum=0, total_sum_currency=164.4, item_count=2,
            comment="ortilmagan dostavka Yangi Klient")
    _insert(db, doc_number_1c="A2", doc_date="2026-05-23",
            client_name_1c="Феруз Чархин", currency="USD",
            total_sum=0, total_sum_currency=58.2, item_count=1,
            comment="DOSTAVKA, ORTILMAGAN")
    _insert(db, doc_number_1c="A3", doc_date="2026-05-14",
            client_name_1c="Рахим Ургут", currency="USD",
            total_sum=0, total_sum_currency=2205.81, item_count=3,
            comment="НЕ ОТГРУЖЕНО, доставка")
    # Negative — normal delivery comment
    _insert(db, doc_number_1c="A4", doc_date="2026-05-23",
            client_name_1c="Boshqa Mijoz", currency="USD",
            total_sum=0, total_sum_currency=100.0, item_count=1,
            comment="DOSTAVKA, FOTON")

    rows = list_phantom_realorders(days=60)
    docs = {r["doc_number_1c"] for r in rows}
    assert docs == {"A1", "A2", "A3"}


def test_audit_excludes_pseudo_clients(db):
    # Same marker, but Наличка-class pseudo — those rows are corrections /
    # returns, not deliverable phantoms. Filter must drop them.
    _insert(db, doc_number_1c="P1", doc_date="2026-05-19",
            client_name_1c="Наличка СКЛАД", currency="USD",
            total_sum=0, total_sum_currency=1.4, item_count=1,
            comment="НЕ ОТГРУЖЕНО, 550 000 Клин олиб кетган")
    _insert(db, doc_number_1c="P2", doc_date="2026-05-23",
            client_name_1c="Real Client", currency="USD",
            total_sum=0, total_sum_currency=200, item_count=1,
            comment="ortilmagan dostavka")

    rows = list_phantom_realorders(days=60)
    assert [r["doc_number_1c"] for r in rows] == ["P2"]


def test_audit_respects_day_window(db):
    _insert(db, doc_number_1c="OLD", doc_date="2026-01-01",
            client_name_1c="Eski Mijoz", currency="USD",
            total_sum=0, total_sum_currency=50, item_count=1,
            comment="ortilmagan dostavka")
    _insert(db, doc_number_1c="NEW", doc_date="2026-05-23",
            client_name_1c="Yangi Mijoz", currency="USD",
            total_sum=0, total_sum_currency=50, item_count=1,
            comment="ortilmagan dostavka")

    # 30 days from "today" (test fixtures don't freeze the clock) — OLD must
    # be far enough back to fall out, NEW close enough to land. Default
    # `date('now')` in the SQL uses real today, so this assertion is robust
    # only as long as tests run between 2026-02-01 and 2027-01-01. The
    # logic itself is what we want to assert.
    rows_30 = list_phantom_realorders(days=30)
    docs_30 = {r["doc_number_1c"] for r in rows_30}
    rows_365 = list_phantom_realorders(days=365)
    docs_365 = {r["doc_number_1c"] for r in rows_365}
    # NEW is always in window; OLD only in the wider one.
    assert "OLD" not in docs_30
    assert "OLD" in docs_365


def test_delete_removes_row_and_line_items(db):
    real_order_id = _insert(
        db, doc_number_1c="D1", doc_date="2026-05-23",
        client_name_1c="Test", currency="USD",
        total_sum=0, total_sum_currency=100.0, item_count=2,
        comment="ortilmagan",
    )
    db.execute(
        """INSERT INTO real_order_items
           (real_order_id, line_no, product_name_1c, quantity, price,
            sum_local, vat, total_local, price_currency, sum_currency,
            total_currency, cost, total_cost, stock_remainder,
            weight_per_unit, total_weight)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (real_order_id, 1, "Prod A", 5, 10, 0, 0, 0, 10, 50, 50, 0, 0, 0, 0, 0),
    )
    db.execute(
        """INSERT INTO real_order_items
           (real_order_id, line_no, product_name_1c, quantity, price,
            sum_local, vat, total_local, price_currency, sum_currency,
            total_currency, cost, total_cost, stock_remainder,
            weight_per_unit, total_weight)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (real_order_id, 2, "Prod B", 5, 10, 0, 0, 0, 10, 50, 50, 0, 0, 0, 0, 0),
    )
    db.commit()

    res = delete_realorder_by_doc_number("D1")
    assert res["ok"] is True
    assert res["deleted"]["doc_number_1c"] == "D1"
    assert res["items_deleted"] == 2

    # Verify cascade — items gone too.
    n_ro = db.execute(
        "SELECT COUNT(*) FROM real_orders WHERE doc_number_1c='D1'"
    ).fetchone()[0]
    n_items = db.execute(
        "SELECT COUNT(*) FROM real_order_items WHERE real_order_id=?",
        (real_order_id,),
    ).fetchone()[0]
    assert n_ro == 0
    assert n_items == 0


def test_delete_missing_doc_returns_error(db):
    res = delete_realorder_by_doc_number("NONEXISTENT")
    assert res["ok"] is False
    assert "not found" in res["error"].lower()


def test_delete_empty_doc_number(db):
    res = delete_realorder_by_doc_number("")
    assert res["ok"] is False
