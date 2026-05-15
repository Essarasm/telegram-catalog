"""Phase 3 reconciler tests — verifies per-row matching produces the
expected match_status across the canonical scenarios sampled on
2026-05-13: aggregate clean, per-day clean, decimal-typo orphan,
wire-transfer orphan, and uncoded-row orphan.

Each test seeds a synthetic client_id with explicit intake_payments +
client_payments rows, then runs `reconcile_payments` and asserts every
row got the expected match_status.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from backend.services.payment_reconciler import (
    _match_client,
    _within_tolerance,
    get_intake_match_status,
    reconcile_payments,
)


def _seed_fx(db, rate_date: str, rate: float = 12200.0) -> None:
    db.execute(
        """INSERT OR REPLACE INTO daily_fx_rates (rate_date, currency_pair, rate)
           VALUES (?, 'USD_UZS', ?)""",
        (rate_date, rate),
    )


def _seed_client(db, cid: int = 7777) -> None:
    db.execute(
        """INSERT OR IGNORE INTO allowed_clients (id, phone_normalized, name)
           VALUES (?, ?, ?)""",
        (cid, f"99890{cid}", f"Test {cid}"),
    )


def _seed_raw(db) -> int:
    cur = db.execute(
        """INSERT INTO payment_intake_raw
           (submitter_telegram_id, submitter_role, raw_payload)
           VALUES (1, 'cashier', '{}')"""
    )
    return cur.lastrowid


def _seed_intake(
    db, *, client_id: int, amount: float, currency: str, submitted_at: str,
) -> int:
    raw_id = _seed_raw(db)
    cur = db.execute(
        """INSERT INTO intake_payments
           (client_id, amount, currency, channel, status, submitted_at,
            confirmed_at, submitter_telegram_id, submitter_role,
            source_intake_raw_id)
           VALUES (?, ?, ?, 'cash_direct', 'confirmed', ?, ?, 1, 'cashier', ?)""",
        (client_id, amount, currency, submitted_at, submitted_at, raw_id),
    )
    return cur.lastrowid


_doc_counter = 80000


def _seed_onec(
    db, *, client_id: int, currency: str, amount_local: float,
    amount_currency: float, doc_date: str,
) -> str:
    global _doc_counter
    _doc_counter += 1
    doc_no = str(_doc_counter)
    corr = "40.10" if currency == "UZS" else "40.11"
    db.execute(
        """INSERT INTO client_payments
           (doc_number_1c, doc_date, corr_account, client_id, currency,
            amount_local, amount_currency)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (doc_no, doc_date, corr, client_id, currency, amount_local, amount_currency),
    )
    return doc_no


def _isodate(days_ago: int) -> str:
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def _isodatetime(days_ago: int) -> str:
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


def test_within_tolerance_floor():
    # Exact match
    assert _within_tolerance(100.0, 100.0)
    # Within $2 floor on small amounts
    assert _within_tolerance(50.0, 51.5)
    # Outside $2 floor
    assert not _within_tolerance(50.0, 53.0)


def test_within_tolerance_percentage():
    # 1.5% diff on large amount — pass
    assert _within_tolerance(1000.0, 985.0)
    # 3% diff on large amount — fail
    assert not _within_tolerance(1000.0, 970.0)


def test_aggregate_match_bahrom_pattern(db):
    """Phase 2.5 parity: Bahrom-style — Alisher's internal FX drift causes
    per-day diffs to exceed tolerance, but window aggregate is within."""
    _seed_client(db, 7777)
    _seed_fx(db, _isodate(4), 12150.0)
    _seed_fx(db, _isodate(3), 12200.0)

    # May 11 (4 days ago): cashier 1,791,000 UZS → $147.41 @ 12150
    # 1C same day: 1,183,600 UZS + $57 → $97.42 + $57 = $154.42
    iid_11 = _seed_intake(db, client_id=7777, amount=1791000, currency="UZS",
                          submitted_at=_isodatetime(4))
    _seed_onec(db, client_id=7777, currency="UZS",
               amount_local=1183600, amount_currency=0, doc_date=_isodate(4))
    _seed_onec(db, client_id=7777, currency="USD",
               amount_local=0, amount_currency=57, doc_date=_isodate(4))

    # May 12 (3 days ago): cashier 517,000 UZS + $200 → $242.38
    # 1C same day: $242.72
    iid_12a = _seed_intake(db, client_id=7777, amount=517000, currency="UZS",
                           submitted_at=_isodatetime(3))
    iid_12b = _seed_intake(db, client_id=7777, amount=200, currency="USD",
                           submitted_at=_isodatetime(3))
    _seed_onec(db, client_id=7777, currency="USD",
               amount_local=0, amount_currency=242.72, doc_date=_isodate(3))

    summary = reconcile_payments(db, lookback_days=30)
    assert summary["aggregate_matches"] >= 1
    statuses = get_intake_match_status(db, [iid_11, iid_12a, iid_12b])
    assert statuses == {iid_11: "matched", iid_12a: "matched", iid_12b: "matched"}


def test_per_day_match_nargiza_pattern(db):
    """Phase 3 upgrade: aggregate fails, but per-day clears matched days
    while keeping the genuinely-uncoded day flagged."""
    _seed_client(db, 7788)
    _seed_fx(db, _isodate(10), 12150.0)
    _seed_fx(db, _isodate(9), 12150.0)
    _seed_fx(db, _isodate(3), 12200.0)

    # Day -10 — cashier-only $96 (Alisher hasn't coded yet)
    iid_orphan = _seed_intake(db, client_id=7788, amount=1168000,
                              currency="UZS", submitted_at=_isodatetime(10))

    # Day -9 — clean match: 876,000 UZS both sides
    iid_d9 = _seed_intake(db, client_id=7788, amount=876000, currency="UZS",
                          submitted_at=_isodatetime(9))
    _seed_onec(db, client_id=7788, currency="UZS",
               amount_local=876000, amount_currency=0, doc_date=_isodate(9))

    # Day -3 — clean match: 536,000 UZS + $250 cashier vs $294 1C
    iid_d3a = _seed_intake(db, client_id=7788, amount=536000, currency="UZS",
                           submitted_at=_isodatetime(3))
    iid_d3b = _seed_intake(db, client_id=7788, amount=250, currency="USD",
                           submitted_at=_isodatetime(3))
    _seed_onec(db, client_id=7788, currency="USD",
               amount_local=0, amount_currency=294, doc_date=_isodate(3))

    reconcile_payments(db, lookback_days=30)
    statuses = get_intake_match_status(db, [iid_orphan, iid_d9, iid_d3a, iid_d3b])
    assert statuses[iid_orphan] == "bot_only"
    assert statuses[iid_d9] == "matched"
    assert statuses[iid_d3a] == "matched"
    assert statuses[iid_d3b] == "matched"


def test_decimal_typo_stays_bot_only(db):
    """Nasim-style: cashier recorded $29,413 (typo for $294.13). 1C has
    $294.13. Per-day fails, aggregate fails. Stays bot_only — admin
    attention warranted."""
    _seed_client(db, 7799)
    _seed_fx(db, _isodate(11), 12150.0)

    iid = _seed_intake(db, client_id=7799, amount=29413, currency="USD",
                      submitted_at=_isodatetime(11))
    _seed_onec(db, client_id=7799, currency="USD",
               amount_local=0, amount_currency=294.13, doc_date=_isodate(11))

    reconcile_payments(db, lookback_days=30)
    statuses = get_intake_match_status(db, [iid])
    assert statuses[iid] == "bot_only"


def test_wire_transfer_marked_kassa_only(db):
    """Sobir-style: 1C has a wire-transfer entry from before the cashier
    started using the system. The wire row is outside the cashier-active
    date band and gets classified as kassa_only without disrupting the
    actual cashier-period aggregate."""
    _seed_client(db, 7711)
    _seed_fx(db, _isodate(20), 12150.0)
    _seed_fx(db, _isodate(2), 12200.0)

    # Old wire transfer — 1C only, far outside cashier activity
    wire_doc = _seed_onec(db, client_id=7711, currency="UZS",
                          amount_local=1000000, amount_currency=0,
                          doc_date=_isodate(20))

    # Recent cashier + matching 1C
    iid = _seed_intake(db, client_id=7711, amount=1470000, currency="UZS",
                      submitted_at=_isodatetime(2))
    _seed_onec(db, client_id=7711, currency="UZS",
               amount_local=1470000, amount_currency=0, doc_date=_isodate(2))

    reconcile_payments(db, lookback_days=30)
    statuses = get_intake_match_status(db, [iid])
    assert statuses[iid] == "matched"

    # Wire row should be kassa_only
    row = db.execute(
        "SELECT match_status FROM payment_reconciliation WHERE kassa_doc_no = ?",
        (wire_doc,),
    ).fetchone()
    assert row["match_status"] == "kassa_only"


def test_reconcile_is_idempotent(db):
    """Re-running the same day should not duplicate rows."""
    _seed_client(db, 7722)
    _seed_fx(db, _isodate(2), 12200.0)
    iid = _seed_intake(db, client_id=7722, amount=500, currency="USD",
                      submitted_at=_isodatetime(2))
    _seed_onec(db, client_id=7722, currency="USD",
               amount_local=0, amount_currency=500, doc_date=_isodate(2))

    summary1 = reconcile_payments(db, lookback_days=30)
    summary2 = reconcile_payments(db, lookback_days=30)
    assert summary1["matched_rows"] == summary2["matched_rows"]
    count = db.execute(
        "SELECT COUNT(*) AS n FROM payment_reconciliation WHERE bot_payment_id = ?",
        (iid,),
    ).fetchone()["n"]
    assert count == 1


def test_empty_intake_skips_classification(db):
    """A client with no cashier rows shouldn't get spurious bot_only
    entries — there's nothing to flag."""
    _seed_client(db, 7733)
    _seed_fx(db, _isodate(2), 12200.0)
    # 1C-only client (e.g. legacy ledger entry, no cashier activity)
    _seed_onec(db, client_id=7733, currency="USD",
               amount_local=0, amount_currency=100, doc_date=_isodate(2))

    reconcile_payments(db, lookback_days=30)
    # The 1C row is classified kassa_only (nothing for cashier to match)
    row = db.execute(
        """SELECT match_status FROM payment_reconciliation
           WHERE kassa_doc_no IS NOT NULL
             AND bot_payment_id IS NULL""",
    ).fetchone()
    assert row is not None
    assert row["match_status"] == "kassa_only"


def test_match_client_pure_function():
    """The _match_client helper is pure — verify it directly without DB."""
    # Aggregate match (Bahrom)
    intake = [
        {"id": 1, "usdeq": 147.41, "date": "2026-05-11"},
        {"id": 2, "usdeq": 42.38, "date": "2026-05-12"},
        {"id": 3, "usdeq": 200.0, "date": "2026-05-12"},
    ]
    onec = [
        {"doc_no": "A", "usdeq": 97.42, "date": "2026-05-11"},
        {"doc_no": "B", "usdeq": 50.0, "date": "2026-05-11"},
        {"doc_no": "C", "usdeq": 7.0, "date": "2026-05-11"},
        {"doc_no": "D", "usdeq": 242.72, "date": "2026-05-12"},
    ]
    result = _match_client(intake, onec)
    assert result["used_aggregate"] is True
    assert set(result["matched_intake"].keys()) == {1, 2, 3}
    assert not result["bot_only_ids"]
    assert not result["kassa_only_doc_nos"]
