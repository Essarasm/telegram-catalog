"""Smoke test for `set_fx_rate` (the /fxrate command's write path).

Unlike the four 1C XLS importers, /fxrate is a text-only command — operator
sends `/fxrate 12650` or `/fxrate 01/04/2026 11230`, the parser is in the bot
handler, and only the validated `(rate, rate_date)` reaches `set_fx_rate`.

This file pins the persistence-layer guarantees:
  - INSERT OR REPLACE on `(rate_date, currency_pair)` — re-submitting the
    same day overwrites; never duplicates the snapshot
  - `daily_fx_rate_events` APPENDS every call (history log, not deduplicated)
  - Default `rate_date` = today (Tashkent); default `currency_pair` = "USD_UZS"
  - Returns the canonical dict shape that downstream callers depend on
  - Triggers `daily_uploads` bookkeeping (`record_upload("fxrate", ...)`)

Per memory `daily_fx_rates coverage + 12,000 UZS/USD fallback anchor`:
this is the table every UZS↔USD report joins to. A silent regression in the
upsert keying would either blow up the unique-snapshot model (duplicate rows
per day) or hide updates (operator changes the rate but the read returns the
old value).
"""
from backend.services.daily_uploads import (
    get_latest_fx_rate,
    set_fx_rate,
    tashkent_today_str,
)


def test_set_fx_rate_basic_insert_then_get(db):
    """Single insert + read round-trip. Pin both the canonical return shape
    and the get/set agreement."""
    result = set_fx_rate(
        rate=12650.0,
        user_id=42,
        user_name="cashier",
        rate_date="2026-05-15",
    )
    assert result == {
        "rate_date": "2026-05-15",
        "rate": 12650.0,
        "currency_pair": "USD_UZS",
    }, f"unexpected return shape: {result}"

    latest = get_latest_fx_rate()
    assert latest is not None
    assert latest["rate_date"] == "2026-05-15"
    assert latest["rate"] == 12650.0
    assert latest["currency_pair"] == "USD_UZS"
    assert latest["source"] == "manual"
    assert latest["uploaded_by_user_id"] == 42
    assert latest["uploaded_by_name"] == "cashier"


def test_set_fx_rate_replace_same_date(db):
    """Re-submitting the same (rate_date, currency_pair) must REPLACE,
    not duplicate — operator typo'd 12650 then corrected to 12700."""
    set_fx_rate(rate=12650, user_name="first", rate_date="2026-05-15")
    set_fx_rate(rate=12700, user_name="second", rate_date="2026-05-15")

    # daily_fx_rates: exactly one row, with the corrected value
    rows = db.execute(
        "SELECT rate, uploaded_by_name FROM daily_fx_rates WHERE rate_date = ?",
        ("2026-05-15",),
    ).fetchall()
    assert len(rows) == 1, f"expected 1 row after replace, got {len(rows)}"
    assert rows[0]["rate"] == 12700
    assert rows[0]["uploaded_by_name"] == "second"


def test_set_fx_rate_events_append_not_replace(db):
    """`daily_fx_rate_events` is the audit trail — every call must append,
    even when the snapshot table replaces. Used to investigate "who changed
    today's rate and when" after a wrong submission.
    """
    set_fx_rate(rate=12650, user_id=1, user_name="alpha", rate_date="2026-05-15")
    set_fx_rate(rate=12700, user_id=2, user_name="beta",  rate_date="2026-05-15")
    set_fx_rate(rate=12690, user_id=3, user_name="gamma", rate_date="2026-05-15")

    events = db.execute(
        """SELECT rate, set_by_name FROM daily_fx_rate_events
           WHERE rate_date = ? ORDER BY id""",
        ("2026-05-15",),
    ).fetchall()
    assert len(events) == 3, f"expected 3 events, got {len(events)}"
    assert [e["set_by_name"] for e in events] == ["alpha", "beta", "gamma"]
    assert [e["rate"] for e in events] == [12650, 12700, 12690]


def test_set_fx_rate_defaults_to_today_tashkent(db):
    """When rate_date is omitted, `tashkent_today_str()` is used —
    pins the operator's mental model: `/fxrate 12650` without a date
    means "today" in Tashkent time, even if the server is elsewhere."""
    expected = tashkent_today_str()
    result = set_fx_rate(rate=12500)
    assert result["rate_date"] == expected, (
        f"expected today's Tashkent date {expected}, got {result['rate_date']}"
    )

    latest = get_latest_fx_rate()
    assert latest["rate_date"] == expected
    assert latest["rate"] == 12500


def test_set_fx_rate_records_upload_for_daily_dashboard(db):
    """`set_fx_rate` calls `record_upload('fxrate', ...)` so the daily
    uploads dashboard / `/today` checklist knows fxrate has happened.
    Without this side-effect, fxrate would always show as ⏳ pending.
    """
    set_fx_rate(rate=12650, user_id=99, user_name="checker", rate_date="2026-05-15")
    rows = db.execute(
        "SELECT upload_type, row_count FROM daily_uploads WHERE upload_date = ?",
        ("2026-05-15",),
    ).fetchall()
    fx_rows = [r for r in rows if r["upload_type"] == "fxrate"]
    assert len(fx_rows) >= 1, "set_fx_rate must mark fxrate as uploaded for /today"
    assert fx_rows[-1]["row_count"] == 1


def test_set_fx_rate_separate_currency_pairs_coexist(db):
    """Different currency_pair values must coexist — the UNIQUE key is the
    (rate_date, currency_pair) composite, not rate_date alone.
    Future-proofs the helper for EUR/RUB tracking without code changes.
    """
    set_fx_rate(rate=12650, rate_date="2026-05-15", currency_pair="USD_UZS")
    set_fx_rate(rate=13800, rate_date="2026-05-15", currency_pair="EUR_UZS")

    pairs = db.execute(
        "SELECT currency_pair, rate FROM daily_fx_rates WHERE rate_date = ? ORDER BY currency_pair",
        ("2026-05-15",),
    ).fetchall()
    assert len(pairs) == 2
    assert pairs[0]["currency_pair"] == "EUR_UZS"
    assert pairs[0]["rate"] == 13800
    assert pairs[1]["currency_pair"] == "USD_UZS"
    assert pairs[1]["rate"] == 12650
