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

    # ── Debtors: 1 chunky overdue, 1 small overdue, 1 not overdue,
    #           +1 PSEUDO-ACCOUNT (should be filtered out — Error Log #36)
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
    # Pseudo-account with massive overdue debt — must be filtered out.
    # 2026-05-11 production output showed this exact pattern as a false positive.
    db.execute(
        """INSERT INTO client_debts
           (client_name_1c, debt_uzs, aging_91_120, aging_120_plus, report_date)
           VALUES (?, ?, ?, ?, ?)""",
        ("Наличка №3", 587_000_000, 300_000_000, 217_000_000, today),
    )

    # ── Stock: 4 active products — 2 stocked-out yesterday (count),
    #           1 stocked-out earlier (don't count), 1 currently 0 but
    #           never tracked (don't count — would be noise). Plus
    #           1 inactive at 0 (don't count) and 1 active >0 (don't count).
    db.execute(
        """INSERT INTO products
           (id, name, category_id, producer_id, is_active, stock_quantity, stockout_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (101, "FellYesterdayA", 1, 1, 1, 0, yesterday),
    )
    db.execute(
        """INSERT INTO products
           (id, name, category_id, producer_id, is_active, stock_quantity, stockout_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (102, "FellYesterdayB", 1, 1, 1, 0, f"{yesterday} 14:30:00"),  # with time
    )
    db.execute(
        """INSERT INTO products
           (id, name, category_id, producer_id, is_active, stock_quantity, stockout_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (103, "FellLastWeek",   1, 1, 1, 0, "2026-05-04"),  # not yesterday
    )
    db.execute(
        """INSERT INTO products
           (id, name, category_id, producer_id, is_active, stock_quantity, stockout_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (104, "NeverTracked",   1, 1, 1, 0, None),  # default-0, never tracked
    )
    db.execute(
        """INSERT INTO products
           (id, name, category_id, producer_id, is_active, stock_quantity, stockout_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (105, "RetiredOut",     1, 1, 0, 0, yesterday),  # inactive
    )
    db.execute(
        """INSERT INTO products
           (id, name, category_id, producer_id, is_active, stock_quantity, stockout_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (106, "InStock",        1, 1, 1, 25.0, None),  # has stock
    )

    # ── Silent regulars: ATLAS = silent (≥3/week, 0 yesterday),
    #                    BUSY = active yesterday (control),
    #                    Наличка №2 = PSEUDO (must be filtered)
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
    # Pseudo-account that LOOKS like a silent regular — must be filtered.
    # 2026-05-11 production output showed Наличка №2 with "17 docs / 0 yesterday".
    for i, dt in enumerate(["2026-05-04", "2026-05-05", "2026-05-06",
                            "2026-05-07", "2026-05-08", "2026-05-09"]):
        db.execute(
            """INSERT INTO real_orders
               (doc_number_1c, doc_date, client_name_1c, currency, total_sum)
               VALUES (?, ?, ?, ?, ?)""",
            (f"R-N-{i}", dt, "Наличка №2", "UZS", 30_000_000),
        )

    # Also seed a pseudo-account cash payment to test the top_clients filter.
    # 2026-05-11 production showed pseudo accounts as silent regulars but
    # we should also block them from top_clients if they happen to have
    # large 40.10 entries.
    db.execute(
        """INSERT INTO client_payments
           (doc_number_1c, doc_date, corr_account, client_name_1c,
            amount_local, amount_currency, currency)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("K-N-001", yesterday, "40.10", "Наличка №3",
         999_999_999, 0, "UZS"),
    )

    db.commit()
    return db


def test_gather_brief_collects_yesterday_cash(seed_brief_data):
    """UZS via 40.10 + USD via 40.11, today's K-099 excluded.
    NOTE: the seeded pseudo-account K-N-001 (999M UZS to Наличка №3) IS
    included in cash_uzs_total because the kassa totals are the company's
    actual collected money — pseudo-accounts are real cash flows even if
    not attributable to a single client. The filter only applies to
    per-client aggregations (top_clients, debtors, silent regulars)."""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    assert data["for_date"] == "2026-05-10"
    # 250M + 180M + 150M + 999.999M = 1579.999M UZS, 4 payments
    # (Наличка №3's 999M IS counted in totals — see docstring)
    assert data["cash_uzs_total"] == 1_579_999_999.0
    assert data["cash_uzs_count"] == 4
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
    artificially inflate ALMAZ's rank because we filter by 40.10 only.
    Also: pseudo-account Наличка №3 (999M UZS in fixture) MUST be filtered
    out — without the pseudo filter, it would dominate the top-3 list."""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    assert len(data["top_clients"]) == 3
    names = [c["name"] for c in data["top_clients"]]
    assert "Наличка №3" not in names, "pseudo-account leaked into top_clients"
    assert data["top_clients"][0]["name"] == "AGROFAS"
    assert data["top_clients"][0]["total_uzs"] == 250_000_000.0
    assert data["top_clients"][1]["name"] == "ALMAZ"
    assert data["top_clients"][1]["total_uzs"] == 180_000_000.0
    assert data["top_clients"][2]["name"] == "KORZINKA"


def test_gather_brief_overdue_debtors_above_threshold(seed_brief_data):
    """TURONBANK is the only one matching: debt > 50M AND aging_91+ > 0.
    SmallShop has aging_91+ but debt < 50M threshold → excluded.
    FreshClient has debt > 50M but no aging_91+ → excluded.
    Наличка №3 has both but is a pseudo-account → must be filtered out.
    (Pre-2026-05-11 fix the brief showed Наличка №3 as a top overdue debtor
    — false positive caught by the production /morningbrief test run.)"""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10),
                        debt_threshold_uzs=50_000_000.0)
    names = [d["name"] for d in data["overdue_debtors"]]
    assert "Наличка №3" not in names, "pseudo-account leaked into overdue_debtors"
    assert len(data["overdue_debtors"]) == 1
    assert data["overdue_debtors"][0]["name"] == "TURONBANK"
    assert data["overdue_debtors"][0]["overdue_91p"] == 52_000_000.0


def test_gather_brief_out_of_stock_yesterday_only(seed_brief_data):
    """Only products that went out YESTERDAY count:
      - FellYesterdayA, FellYesterdayB (stockout_at=2026-05-10) → counted (2)
      - FellLastWeek (stockout_at=2026-05-04) → NOT counted
      - NeverTracked (stockout_at=NULL, default 0) → NOT counted (this was
        the 741-row noise in the 2026-05-11 production output)
      - RetiredOut (is_active=0) → NOT counted
      - InStock (stock_quantity=25) → NOT counted"""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    assert data["out_of_stock_count"] == 2


def test_gather_brief_out_of_stock_excludes_stockout_null(db):
    """A product with stock_quantity=0 but stockout_at=NULL must NOT be
    counted as a "new stockout yesterday" — that's the default-0 case
    (never tracked) and would inflate the count meaninglessly."""
    db.execute(
        """INSERT INTO products
           (id, name, category_id, producer_id, is_active, stock_quantity, stockout_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (200, "NeverTracked", 1, 1, 1, 0, None),
    )
    db.commit()
    data = gather_brief(db, for_date=date(2026, 5, 10))
    assert data["out_of_stock_count"] == 0


def test_gather_brief_silent_regulars(seed_brief_data):
    """ATLAS GROUP had 4 docs in window, 0 yesterday → flagged.
    BUSY CLIENT had 4 docs in window + 1 yesterday → not flagged.
    Наличка №2 had 6 docs in window, 0 yesterday → IS silent BUT pseudo;
    must be filtered. (Pre-2026-05-11 fix the brief showed Наличка №2,
    №3, СКЛАД all as silent regulars — three false positives in one run.)"""
    data = gather_brief(seed_brief_data, for_date=date(2026, 5, 10))
    names = {s["name"] for s in data["silent_regulars"]}
    assert "ATLAS GROUP" in names
    assert "BUSY CLIENT" not in names
    assert "Наличка №2" not in names, "pseudo-account leaked into silent_regulars"


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

    # Kassa section (totals include the pseudo Наличка №3 999M — see fixture docstring)
    assert "Kassa" in text
    assert "1.58B" in text  # UZS total: 580M (real) + 999.999M (pseudo) ≈ 1.58B

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
    assert "2 mahsulot" in text  # new-stockouts-yesterday count
    assert "Kecha" in text  # renamed from "Inventarda" to "Kecha" for accuracy
    assert "ATLAS GROUP" in text
    # Pseudo-accounts must NOT appear anywhere in the rendered brief.
    assert "Наличка" not in text, "pseudo-account leaked into rendered output"


def test_render_brief_quiet_day_compact(db):
    """Quiet day → brief mentions '0 to'lov' and '0 hujjat' but no anomalies block."""
    data = gather_brief(db, for_date=date(2026, 5, 10))
    text = render_brief(data, today=date(2026, 5, 11))
    assert "0 to'lov" in text
    assert "0 hujjat" in text
    # No anomaly header on a fully quiet day
    assert "Diqqat" not in text
