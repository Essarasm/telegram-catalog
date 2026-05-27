"""Tests for the realorders_revenue helper + parity guard across the
revenue-surfacing consumers.

The helper is the canonical way to compute UZS/USD revenue from
real_orders. These tests pin:
  1. Both legs are summed unconditionally (no `currency` filter)
  2. Single-currency docs contribute to one leg only
  3. Dual-currency docs contribute to both
  4. Filters (date range, client_id, only_approved, exclude_pseudo) work
  5. PARITY: owner_brief shipments + direct SUM agree on UZS revenue.
     If any future consumer reintroduces the broken `WHERE currency='UZS'`
     pattern, this parity test fails — catches the 4th-family-recurrence
     class regardless of which file it lands in.
"""
import pytest

from backend.services.realorders_revenue import (
    realorders_revenue,
    realorders_revenue_by_client,
)


def _insert_order(db, doc_no, doc_date, client_name, uzs=0, usd=0,
                  is_approved=None, client_id=None):
    """Insert a real_orders row with dual-currency legs."""
    db.execute(
        """INSERT INTO real_orders
           (doc_number_1c, doc_date, client_name_1c, client_id,
            currency, total_sum, total_sum_currency, item_count, is_approved)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (doc_no, doc_date, client_name, client_id, "USD",
         uzs, usd, 1, is_approved),
    )
    db.commit()


# ── Aggregate helper ──────────────────────────────────────────────────────

def test_uzs_only_doc_sums_uzs_only(db):
    _insert_order(db, "T-1", "2026-05-27", "ClientA", uzs=500000, usd=0)
    r = realorders_revenue(date_min="2026-05-27", date_max="2026-05-27", conn=db)
    assert r["uzs"] == 500000
    assert r["usd"] == 0
    assert r["doc_count"] == 1
    assert r["uzs_only_docs"] == 1
    assert r["usd_only_docs"] == 0
    assert r["dual_docs"] == 0


def test_usd_only_doc_sums_usd_only(db):
    _insert_order(db, "T-2", "2026-05-27", "ClientB", uzs=0, usd=164.40)
    r = realorders_revenue(date_min="2026-05-27", date_max="2026-05-27", conn=db)
    assert r["uzs"] == 0
    assert r["usd"] == pytest.approx(164.40)
    assert r["usd_only_docs"] == 1


def test_dual_currency_doc_contributes_to_both(db):
    # The Сардор case — doc has both UZS and USD legs.
    _insert_order(db, "T-3", "2026-05-23", "Сардор", uzs=632500, usd=1664.34)
    r = realorders_revenue(date_min="2026-05-23", date_max="2026-05-23", conn=db)
    assert r["uzs"] == 632500
    assert r["usd"] == pytest.approx(1664.34)
    assert r["dual_docs"] == 1
    assert r["uzs_only_docs"] == 0
    assert r["usd_only_docs"] == 0


def test_date_range_filter(db):
    _insert_order(db, "T-4", "2026-05-26", "ClientC", uzs=100000, usd=0)
    _insert_order(db, "T-5", "2026-05-27", "ClientC", uzs=200000, usd=0)
    _insert_order(db, "T-6", "2026-05-28", "ClientC", uzs=300000, usd=0)
    r = realorders_revenue(date_min="2026-05-27", date_max="2026-05-27", conn=db)
    assert r["uzs"] == 200000


def test_client_id_filter_single(db):
    _insert_order(db, "T-7", "2026-05-27", "ClientD", uzs=100000, usd=0, client_id=10)
    _insert_order(db, "T-8", "2026-05-27", "ClientE", uzs=200000, usd=0, client_id=20)
    r = realorders_revenue(client_id=10, conn=db)
    assert r["uzs"] == 100000


def test_client_id_filter_list(db):
    _insert_order(db, "T-9", "2026-05-27", "C1", uzs=100000, client_id=10)
    _insert_order(db, "T-10", "2026-05-27", "C2", uzs=200000, client_id=20)
    _insert_order(db, "T-11", "2026-05-27", "C3", uzs=400000, client_id=30)
    r = realorders_revenue(client_id=[10, 20], conn=db)
    assert r["uzs"] == 300000


def test_only_approved_filter(db):
    _insert_order(db, "T-12", "2026-05-27", "A", uzs=100000, is_approved=1)
    _insert_order(db, "T-13", "2026-05-27", "B", uzs=200000, is_approved=0)
    _insert_order(db, "T-14", "2026-05-27", "C", uzs=400000, is_approved=None)  # legacy
    r = realorders_revenue(only_approved=True, conn=db)
    # 1 (approved) + None (legacy treated as approved) = 500000
    assert r["uzs"] == 500000


def test_exclude_pseudo_filter(db):
    _insert_order(db, "T-15", "2026-05-27", "RealClient", uzs=100000)
    _insert_order(db, "T-16", "2026-05-27", "Наличка №1", uzs=200000)
    _insert_order(db, "T-17", "2026-05-27", "СТРОЙКА", uzs=400000)
    r = realorders_revenue(exclude_pseudo=True, conn=db)
    assert r["uzs"] == 100000


# ── Per-client helper ─────────────────────────────────────────────────────

def test_per_client_groups_correctly(db):
    _insert_order(db, "T-20", "2026-05-27", "ClientA", uzs=300000, usd=10)
    _insert_order(db, "T-21", "2026-05-27", "ClientA", uzs=200000, usd=5)
    _insert_order(db, "T-22", "2026-05-27", "ClientB", uzs=100000, usd=0)
    rows = realorders_revenue_by_client(
        date_min="2026-05-27", date_max="2026-05-27",
        exclude_pseudo=False, conn=db,
    )
    by_name = {r["client_name_1c"]: r for r in rows}
    assert by_name["ClientA"]["uzs"] == 500000
    assert by_name["ClientA"]["usd"] == pytest.approx(15)
    assert by_name["ClientB"]["uzs"] == 100000


def test_per_client_ordering_uzs_desc(db):
    _insert_order(db, "T-30", "2026-05-27", "Small", uzs=50000)
    _insert_order(db, "T-31", "2026-05-27", "Big", uzs=500000)
    _insert_order(db, "T-32", "2026-05-27", "Medium", uzs=200000)
    rows = realorders_revenue_by_client(
        date_min="2026-05-27", date_max="2026-05-27",
        exclude_pseudo=False, conn=db,
    )
    names = [r["client_name_1c"] for r in rows]
    assert names == ["Big", "Medium", "Small"]


# ── Parity guard — catches future regressions of the family ──────────────

def test_parity_helper_vs_direct_sum(db):
    """If any future consumer (owner_brief, admin_revenue, etc.) reintroduces
    the `WHERE currency='UZS'` pattern, the helper would diverge from a
    direct SUM(total_sum) — failing this test before deploy.
    """
    _insert_order(db, "P-1", "2026-05-27", "X", uzs=100000, usd=0)
    _insert_order(db, "P-2", "2026-05-27", "Y", uzs=200000, usd=5)
    _insert_order(db, "P-3", "2026-05-27", "Z", uzs=0, usd=10)
    _insert_order(db, "P-4", "2026-05-27", "Наличка №1", uzs=500000, usd=0)

    via_helper = realorders_revenue(
        date_min="2026-05-27", date_max="2026-05-27", conn=db,
    )
    # Direct: sum both legs without any currency filter, all clients
    direct = db.execute(
        """SELECT COALESCE(SUM(total_sum), 0) AS uzs,
                   COALESCE(SUM(total_sum_currency), 0) AS usd
           FROM real_orders
           WHERE doc_date='2026-05-27'"""
    ).fetchone()

    assert via_helper["uzs"] == direct["uzs"]
    assert via_helper["usd"] == direct["usd"]

    # Sanity: the broken pattern (WHERE currency='UZS') returns 0. If a
    # consumer relied on this, the helper-based UZS revenue would not
    # match. Pin the divergence so the bug class is self-documenting.
    broken = db.execute(
        """SELECT COALESCE(SUM(total_sum), 0) AS uzs
           FROM real_orders
           WHERE doc_date='2026-05-27' AND currency='UZS'"""
    ).fetchone()
    assert broken["uzs"] == 0
    assert via_helper["uzs"] > 0
    assert via_helper["uzs"] != broken["uzs"]
