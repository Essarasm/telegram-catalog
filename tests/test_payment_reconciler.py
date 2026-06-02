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
    amount_currency: float, doc_date: str, attachment: str | None = None,
) -> str:
    global _doc_counter
    _doc_counter += 1
    doc_no = str(_doc_counter)
    corr = "40.10" if currency == "UZS" else "40.11"
    db.execute(
        """INSERT INTO client_payments
           (doc_number_1c, doc_date, corr_account, client_id, currency,
            amount_local, amount_currency, attachment)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (doc_no, doc_date, corr, client_id, currency, amount_local,
         amount_currency, attachment),
    )
    return doc_no


_ro_counter = 90000


def _seed_realorder(
    db, *, client_id: int, doc_date: str, comment: str,
    client_name_1c: str = "RO client",
) -> str:
    global _ro_counter
    _ro_counter += 1
    doc_no = str(_ro_counter)
    db.execute(
        """INSERT INTO real_orders
           (doc_number_1c, doc_date, client_id, client_name_1c, comment)
           VALUES (?, ?, ?, ?, ?)""",
        (doc_no, doc_date, client_id, client_name_1c, comment),
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


# ---------------------------------------------------------------------------
# Discount handling — Part A (discount-aware matching) + Part B (unbooked
# discount detection). Discounts live ONLY in 1C (cashier never records them);
# real_orders.comment is the sole independent signal a discount was granted.
# ---------------------------------------------------------------------------

from backend.services.payment_reconciler import (  # noqa: E402
    _is_discount_note,
    _parse_discount_amount,
    find_unrecorded_discounts,
    get_yesterday_client_totals,
)


def test_is_discount_note_all_writing_styles():
    # ASCII + Cyrillic, upper/lower/mixed — casefold catches all of them
    for s in ("SKIDKA", "skidka", "Skidka", "скидка", "СКИДКА",
              "СКИДКА ЭЛЕРОНГА", "НАЛИЧКА-СКИДКА 25000", "daftar skidka 25 000"):
        assert _is_discount_note(s), s
    # Non-discounts (incl. the SQLite-ASCII-fold trap: Cyrillic upper)
    for s in ("NALICHKA", "выручка", "KARTA", "", None, "БОНУС-2025"):
        assert not _is_discount_note(s), s


def test_parse_discount_amount_formats():
    assert _parse_discount_amount("СКИДКА-40000") == (40000.0, "UZS")
    assert _parse_discount_amount("skidka 30 000") == (30000.0, "UZS")
    assert _parse_discount_amount("skidka 15 000 sum qarz 200$") == (15000.0, "UZS")
    assert _parse_discount_amount("ДАФТАР-3427500/СКИДКА 22500/") == (22500.0, "UZS")
    assert _parse_discount_amount("SKIDKA - 12$") == (12.0, "USD")
    assert _parse_discount_amount("dostavka skidka - $18") == (18.0, "USD")
    # No clean number next to the token → None (caller shows raw comment)
    assert _parse_discount_amount("skidka berildi") is None
    assert _parse_discount_amount("NALICHKA") is None
    assert _parse_discount_amount(None) is None


def test_parse_discount_amount_usd_and_decimals():
    # у.е. (условные единицы) = USD — previously mis-parsed as UZS
    assert _parse_discount_amount("скидка-187у.е.") == (187.0, "USD")
    assert _parse_discount_amount("СКИДКА 7у.е.") == (7.0, "USD")
    assert _parse_discount_amount("бартер скидка 5 y.e.") == (5.0, "USD")
    # Comma / period decimals with $ — previously truncated the cents + currency
    assert _parse_discount_amount("skidka 74,4 $") == (74.4, "USD")
    assert _parse_discount_amount("скидка - 146,80$") == (146.8, "USD")
    assert _parse_discount_amount("SKIDKA - 2.6$") == (2.6, "USD")
    assert _parse_discount_amount("skidka - $4,5") == (4.5, "USD")
    # 3-digit group after comma is a thousands separator, not cents
    assert _parse_discount_amount("skidka 2,750 sum") == (2750.0, "UZS")
    # UZS still wins with no currency cue, decimals untouched
    assert _parse_discount_amount("skidka 50 000") == (50000.0, "UZS")


def test_discount_excluded_from_match_cash_plus_discount(db):
    """Part A: cashier collected the real cash; 1C has the same cash PLUS a
    phantom skidka row. Including skidka would falsely inflate the 1C side —
    excluding it makes the client reconcile cleanly."""
    _seed_client(db, 8801)
    _seed_fx(db, _isodate(2), 12000.0)
    # Cashier: 1,200,000 UZS ($100)
    iid = _seed_intake(db, client_id=8801, amount=1200000, currency="UZS",
                       submitted_at=_isodatetime(2))
    # 1C: 1,200,000 cash + 50,000 skidka (phantom). Without exclusion the 1C
    # side = $104.17 vs cashier $100 → 4% mismatch.
    _seed_onec(db, client_id=8801, currency="UZS", amount_local=1200000,
               amount_currency=0, doc_date=_isodate(2), attachment="NALICHKA")
    _seed_onec(db, client_id=8801, currency="UZS", amount_local=50000,
               amount_currency=0, doc_date=_isodate(2), attachment="SKIDKA")

    reconcile_payments(db, lookback_days=30)
    statuses = get_intake_match_status(db, [iid])
    assert statuses[iid] == "matched"


def test_discount_only_day_is_not_a_mismatch(db):
    """Part A: a day with ONLY a skidka row (no cash either side) must not
    surface as a mismatch in the morning report."""
    _seed_client(db, 8802)
    today = _isodate(0)
    yesterday = _isodate(1)
    _seed_fx(db, yesterday, 12000.0)
    _seed_onec(db, client_id=8802, currency="UZS", amount_local=30000,
               amount_currency=0, doc_date=yesterday, attachment="скидка")

    detail = get_yesterday_client_totals(db, today)
    assert detail["mismatched"] == []
    assert len(detail["discounts"]) == 1
    assert detail["discounts"][0]["client_id"] == 8802


def test_get_yesterday_totals_strips_discount_from_sum(db):
    """Part A: the report's per-client 1C total excludes the skidka row, so
    cashier == 1C and the client is counted matched, not mismatched."""
    _seed_client(db, 8803)
    today = _isodate(0)
    yesterday = _isodate(1)
    _seed_fx(db, yesterday, 12000.0)
    _seed_intake(db, client_id=8803, amount=600000, currency="UZS",
                 submitted_at=_isodatetime(1))
    _seed_onec(db, client_id=8803, currency="UZS", amount_local=600000,
               amount_currency=0, doc_date=yesterday, attachment="VYRUCHKA")
    _seed_onec(db, client_id=8803, currency="UZS", amount_local=25000,
               amount_currency=0, doc_date=yesterday, attachment="SKIDKA")

    detail = get_yesterday_client_totals(db, today)
    assert detail["matched_clients"] == 1
    assert detail["mismatched"] == []
    assert len(detail["discounts"]) == 1


def test_unrecorded_discount_flagged(db):
    """Part B: real order says skidka, but no 1C касса skidka exists in
    [D, D+1] → flagged as unbooked."""
    _seed_client(db, 8804)
    yesterday = _isodate(1)
    _seed_realorder(db, client_id=8804, doc_date=yesterday,
                    comment="DAFTAR SKIDKA 25 000", client_name_1c="Ikrom")

    result = find_unrecorded_discounts(db, _isodate(0))
    assert len(result) == 1
    assert result[0]["client_id"] == 8804
    assert result[0]["amount"] == 25000.0
    assert result[0]["currency"] == "UZS"
    assert result[0]["age_days"] == 1


def test_booked_discount_not_flagged(db):
    """Part B: real order says skidka AND 1C касса has a matching skidka row
    within [D, D+1] → not flagged."""
    _seed_client(db, 8805)
    yesterday = _isodate(1)
    today = _isodate(0)
    _seed_realorder(db, client_id=8805, doc_date=yesterday,
                    comment="СКИДКА - 30 000")
    # Alisher booked it the next morning (D+1) — within the window
    _seed_onec(db, client_id=8805, currency="UZS", amount_local=30000,
               amount_currency=0, doc_date=today, attachment="SKIDKA")

    result = find_unrecorded_discounts(db, today)
    assert result == []


def test_unrecorded_discount_outside_window_still_flagged(db):
    """Part B: a 1C skidka booked TWO days after the real order is outside
    [D, D+1], so the real order is (correctly) still flagged at report time."""
    _seed_client(db, 8806)
    d_order = _isodate(3)
    d_booked = _isodate(1)  # 2 days later — outside [D, D+1]
    _seed_realorder(db, client_id=8806, doc_date=d_order, comment="skidka 20000")
    _seed_onec(db, client_id=8806, currency="UZS", amount_local=20000,
               amount_currency=0, doc_date=d_booked, attachment="SKIDKA")

    result = find_unrecorded_discounts(db, _isodate(0))
    assert len(result) == 1
    assert result[0]["client_id"] == 8806


def test_non_discount_realorder_ignored(db):
    """Part B: real orders without a skidka note are never flagged."""
    _seed_client(db, 8807)
    _seed_realorder(db, client_id=8807, doc_date=_isodate(1),
                    comment="DOSTAVKA Dilshod aka")
    result = find_unrecorded_discounts(db, _isodate(0))
    assert result == []
