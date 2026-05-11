"""Test the owner daily morning brief — data gathering + rendering.

Pins the spec from Notion Command Center feature backlog A2 (2026-05-11):
  - Cash UZS uses corr_account 40.10*; USD uses 40.11*
  - Top clients by UZS cash receipts (40.10 only)
  - Overdue debtors via client_debts aging_91_120 + aging_120_plus buckets
  - Out-of-stock via products.stock_quantity = 0 AND is_active = 1
  - Silent regulars: ≥3 docs in last 7 days but 0 yesterday
  - is_quiet_day → True only when ALL fields are zero/empty
"""
from __future__ import annotations

from datetime import date

import pytest

from backend.services.owner_brief import (
    gather_brief,
    is_quiet_day,
    render_brief,
)


@pytest.fixture
def seed_brief_data(db):
    """Seed enough data for a realistic brief."""
    yesterday = "2026-05-10"
    today = "2026-05-11"
    week_ago = "2026-05-04"

    # ── Cash: 3 UZS payments + 1 USD payment yesterday ──
    cash_rows = [
        # (doc_number, doc_date, corr_account, client, amount_local, amount_currency)
        ("K-001", yesterday, "40.10", "AGROFAS",     250_000_000, 0),
        ("K-002", yesterday, "40.10", "ALMAZ",       180_000_000, 0),
        ("K-003", yesterday, "40.10", "KORZINKA",    150_000_000, 0),
        ("K-004", yesterday, "40.11", "ALMAZ",                 0, 100),
        # Today's payment — must NOT be in yesterday's brief
        ("K-099", today,     "40.10", "AGROFAS",       5_000_000, 0),
    ]
    for doc_no, dt, corr, name, amt_local, amt_curr in cash_rows:
        db.execute(
            """INSERT INTO client_payments
               (doc_number_1c, doc_date, corr_account, client_name_1c,
                amount_local, amount_currency, currency)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (doc_no, dt, corr, name, amt_local, amt_curr,
             'UZS' if corr == '40.10' else 'USD'),
        )

    # ── Shipments (real_orders): 2 UZS + 1 USD yesterday ──
    ship_rows = [
        ("R-001", yesterday, "AGROFAS",   "UZS", 800_000_000, 0),
        ("R-002", yesterday, "ALMAZ",     "UZS", 1_000_000_000, 0),
        ("R-003", yesterday, "KORZINKA",  "USD", 0, 2_100),
        # Today's — excluded from yesterday's brief
        ("R-099", today,     "AGROFAS",   "UZS", 50_000_000, 0),
    ]
    for doc_no, dt, name, curr, total_uzs, total_usd in ship_rows:
        db.execute(
            """INSERT INTO real_orders
               (doc_number_1c, doc_date, client_name_1c,
                currency, total_sum, total_sum_currency)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (doc_no, dt, name, curr, total_uzs, total_usd),
        )

    # ── Debtors: 1 chunky overdue, 1 small overdue, 1 not overdue ──
    db.execute(
        """INSERT INTO client_debts
           (client_name_1c, debt_uzs, aging_91_120, aging_120_plus, report_date)
           VALUES (?, ?, ?, ?, ?)""",
        ("TURONBANK", 52_000_000, 30_000_000, 22_000_000, today),
    )
    db.execute(
        """INSERT INTO client_debts
           (client_name_1c, debt_uzs, aging_91_120, aging_120_plus, report_date)
           VALUES (?, ?, ?, ?, ?)""",
        ("SmallShop", 10_000_000, 5_000_000, 0, today),  # below threshold
    )
    db.execute(
        """INSERT INTO client_debts
           (client_name_1c, debt_uzs, aging_91_120, aging_120_plus, report_date)
           VALUES (?, ?, ?, ?, ?)""",
        ("FreshClient", 80_000_000, 0, 0, today),  # has debt but not overdue 91+
    )

    # ── Out-of-stock: 2 active products at 0, 1 inactive at 0, 1 active >0 ──
    db.execute(
        """INSERT INTO products (id, name, category_id, producer_id, is_active, stock_quantity)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (101, "ProdA",   1, 1, 1, 0),
    )
    db.execute(
        """INSERT INTO products (id, name, category_id, producer_id, is_active, stock_quantity)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (102, "ProdB",   1, 1, 1, 0),
    )
    db.execute(
        """INSERT INTO products (id, name, category_id, producer_id, is_active, stock_quantity)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (103, "Retired", 1, 1, 0, 0),
    )
    db.execute(
        """INSERT INTO products (id, name, category_id, producer_id, is_active, stock_quantity)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (104, "InStock", 1, 1, 1, 25.0),
    )

    # ── Silent regular: ATLAS had 4 docs in week_ago..yesterday but 0 yesterday ──
    # Plus a control: BUSY had 4 docs in the window INCLUDING yesterday → not silent.
    for i, dt in enumerate(["2026-05-04", "2026-05-05", "2026-05-06", "2026-05-08"]):
        db.execute(
            """INSERT INTO real_orders
               (doc_number_1c, doc_date, client_name_1c, currency, total_sum)
               VALUES (?, ?, ?, ?, ?)""",
            (f"R-A-{i}", dt, "ATLAS GROUP", "UZS", 100_000_000),
        )
    for i, dt in enumerate(["2026-05-04", "2026-05-08", "2026-05-09", "2026-05-10"]):
        db.execute(
            """INSERT INTO real_orders
               (doc_number_1c, doc_date, client_name_1c, currency, total_sum)
               VALUES (?, ?, ?, ?, ?)""",
            (f"R-B-{i}", dt, "BUSY CLIENT", "UZS", 50_000_000),
        )

    db.commit()
    return db


def test_gather_brief_collects_yesterday_cash(seed_brief_data):
    """UZS via 40.10 + USD via 40.11, today's K-099 excluded."""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    assert data["for_date"] == "2026-05-10"
    # 250M + 180M + 150M = 580M UZS, 3 payments
    assert data["cash_uzs_total"] == 580_000_000.0
    assert data["cash_uzs_count"] == 3
    # 1 USD payment for $100
    assert data["cash_usd_total"] == 100.0
    assert data["cash_usd_count"] == 1


def test_gather_brief_collects_yesterday_shipments(seed_brief_data):
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    # 3 UZS shipments on yesterday: R-001 (AGROFAS, 800M) + R-002 (ALMAZ, 1B)
    # + R-B-3 (BUSY CLIENT, 50M — they appear on yesterday so they aren't
    # flagged as silent regulars later). Total: 1.85B UZS.
    assert data["ship_uzs_total"] == 1_850_000_000.0
    assert data["ship_uzs_count"] == 3
    # 1 USD shipment: $2,100
    assert data["ship_usd_total"] == 2100.0
    assert data["ship_usd_count"] == 1


def test_gather_brief_top_clients_ranked_uzs_only(seed_brief_data):
    """Top clients ranked by UZS cash receipts; the USD payment (ALMAZ) doesn't
    artificially inflate ALMAZ's rank because we filter by 40.10 only."""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    assert len(data["top_clients"]) == 3
    assert data["top_clients"][0]["name"] == "AGROFAS"
    assert data["top_clients"][0]["total_uzs"] == 250_000_000.0
    assert data["top_clients"][1]["name"] == "ALMAZ"
    assert data["top_clients"][1]["total_uzs"] == 180_000_000.0
    assert data["top_clients"][2]["name"] == "KORZINKA"


def test_gather_brief_overdue_debtors_above_threshold(seed_brief_data):
    """TURONBANK is the only one matching: debt > 50M AND aging_91+ > 0.
    SmallShop has aging_91+ but debt < 50M threshold → excluded.
    FreshClient has debt > 50M but no aging_91+ → excluded."""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10),
                        debt_threshold_uzs=50_000_000.0)
    assert len(data["overdue_debtors"]) == 1
    assert data["overdue_debtors"][0]["name"] == "TURONBANK"
    assert data["overdue_debtors"][0]["overdue_91p"] == 52_000_000.0


def test_gather_brief_out_of_stock_active_only(seed_brief_data):
    """ProdA, ProdB count; Retired (inactive) doesn't; InStock (>0) doesn't."""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    assert data["out_of_stock_count"] == 2


def test_gather_brief_silent_regulars(seed_brief_data):
    """ATLAS GROUP had 4 docs in window, 0 yesterday → flagged.
    BUSY CLIENT had 4 docs in window + 1 yesterday → not flagged."""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    names = {s["name"] for s in data["silent_regulars"]}
    assert "ATLAS GROUP" in names
    assert "BUSY CLIENT" not in names


def test_is_quiet_day_true_on_empty(db):
    """Fresh DB with no data → quiet day."""
    data = gather_brief(db, for_date=date(2026, 5, 10))
    assert is_quiet_day(data) is True


def test_is_quiet_day_false_on_any_activity(seed_brief_data):
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    assert is_quiet_day(data) is False


def test_render_brief_contains_key_sections(seed_brief_data):
    """Smoke test: rendered brief mentions UZS/USD totals + top clients + anomalies."""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    text = render_brief(data, today=date(2026, 5, 11))

    # Header
    assert "2026-05-11" in text
    assert "2026-05-10" in text  # for_date

    # Kassa section
    assert "Kassa" in text
    assert "580" in text or "580.0M" in text  # UZS total

    # Realizatsiya section
    assert "Realizatsiya" in text
    assert "1.85B" in text  # UZS shipments (800M + 1B + 50M from BUSY)

    # Top clients
    assert "AGROFAS" in text
    assert "ALMAZ" in text
    assert "KORZINKA" in text

    # Anomalies
    assert "TURONBANK" in text
    assert "30+ kun" in text  # overdue label
    assert "2 mahsulot" in text  # out-of-stock count
    assert "ATLAS GROUP" in text


def test_render_brief_quiet_day_compact(db):
    """Quiet day → brief mentions '0 to'lov' and '0 hujjat' but no anomalies block."""
    data = gather_brief(db, for_date=date(2026, 5, 10))
    text = render_brief(data, today=date(2026, 5, 11))
    assert "0 to'lov" in text
    assert "0 hujjat" in text
    # No anomaly header on a fully quiet day
    assert "Diqqat" not in text
