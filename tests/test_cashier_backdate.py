"""Back-dated cashier intake — `kassa_date` is a nullable column that
overrides `date(submitted_at)` as the cash-flow date when the cashier
records yesterday's late-delivery cash today.

Covers:
- create_intake_payment accepts and stores kassa_date
- Reconciliation grouping uses COALESCE(kassa_date, date(submitted_at))
  so back-dated rows bucket to the actual cash-flow date
- _format_backdate_banner produces the human banner for /qabul confirm
- _backdate_options returns 7 days, today excluded, in iso+label form
"""
from datetime import date, datetime, timedelta

import pytest

from backend.services import payment_intake


def _seed_client(conn, name="Test Client", phone="998900000099"):
    conn.execute(
        "INSERT INTO allowed_clients "
        "(phone_normalized, name, source_sheet, status, segment, client_id_1c) "
        "VALUES (?, ?, 'test', 'active', 'shop', ?)",
        (phone, name, name),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _insert_payment(conn, client_id, amount, currency, kassa_date=None):
    """Mimic the cashier-finalize INSERT path with optional back-date."""
    raw_id = payment_intake.insert_intake_raw(
        conn,
        submitter_telegram_id=275116966,  # Aunt's tg_id
        submitter_role="cashier",
        payload={
            "channel": "cash_direct",
            "client_id": client_id,
            "amount": amount,
            "currency": currency,
            **({"kassa_date": kassa_date} if kassa_date else {}),
        },
    )
    return payment_intake.create_intake_payment(
        conn,
        raw_id=raw_id,
        client_id=client_id,
        amount=amount,
        currency=currency,
        channel="cash_direct",
        status="confirmed",
        submitter_telegram_id=275116966,
        submitter_role="cashier",
        confirmed_by_telegram_id=275116966,
        kassa_date=kassa_date,
    )


def test_create_intake_payment_stores_kassa_date(db):
    cid = _seed_client(db)
    pid = _insert_payment(db, cid, 500_000.0, "UZS", kassa_date="2026-05-25")
    db.commit()

    row = db.execute(
        "SELECT kassa_date, date(submitted_at) AS sdate "
        "FROM intake_payments WHERE id = ?",
        (pid,),
    ).fetchone()
    assert row["kassa_date"] == "2026-05-25"
    # submitted_at remains 'today' — that's how /bugunpul still finds it.
    assert row["sdate"] != "2026-05-25"


def test_create_intake_payment_default_kassa_date_is_null(db):
    cid = _seed_client(db)
    pid = _insert_payment(db, cid, 100_000.0, "UZS")  # no kassa_date arg
    db.commit()

    row = db.execute(
        "SELECT kassa_date FROM intake_payments WHERE id = ?", (pid,)
    ).fetchone()
    assert row["kassa_date"] is None


def test_reconciliation_buckets_to_kassa_date(db):
    """A back-dated row written today should group to its kassa_date when
    queried via COALESCE — that's what payment_reconciler relies on."""
    cid = _seed_client(db)
    # Back-dated to 2026-05-20:
    _insert_payment(db, cid, 700_000.0, "UZS", kassa_date="2026-05-20")
    # Same-day (kassa_date NULL) — buckets to today via date(submitted_at):
    _insert_payment(db, cid, 300_000.0, "UZS")
    db.commit()

    rows = db.execute(
        """SELECT COALESCE(ip.kassa_date, date(ip.submitted_at)) AS d,
                  SUM(ip.amount) AS total
           FROM intake_payments ip
           WHERE ip.status = 'confirmed' AND ip.client_id = ?
           GROUP BY d
           ORDER BY d""",
        (cid,),
    ).fetchall()
    by_day = {r["d"]: r["total"] for r in rows}
    assert by_day["2026-05-20"] == 700_000.0
    # The same-day row buckets to today's date — match dynamically because
    # the test runner's clock could be any date. Just assert the second
    # bucket isn't 05-20 and carries the same-day amount.
    other_days = [d for d in by_day if d != "2026-05-20"]
    assert len(other_days) == 1
    assert by_day[other_days[0]] == 300_000.0


def test_format_backdate_banner_yesterday():
    from bot.handlers.cashier import _format_backdate_banner

    today = date.today()
    yesterday = (today - timedelta(days=1)).isoformat()
    banner = _format_backdate_banner(yesterday)
    assert banner is not None
    assert "kecha" in banner
    assert "📅" in banner


def test_format_backdate_banner_n_days_ago():
    from bot.handlers.cashier import _format_backdate_banner

    today = date.today()
    four_ago = (today - timedelta(days=4)).isoformat()
    banner = _format_backdate_banner(four_ago)
    assert banner is not None
    assert "4 kun oldin" in banner


def test_format_backdate_banner_today_returns_none():
    from bot.handlers.cashier import _format_backdate_banner

    today = date.today().isoformat()
    assert _format_backdate_banner(today) is None


def test_format_backdate_banner_invalid_returns_none():
    from bot.handlers.cashier import _format_backdate_banner

    assert _format_backdate_banner("") is None
    assert _format_backdate_banner("not-a-date") is None
    assert _format_backdate_banner(None) is None


def test_backdate_options_excludes_today_and_is_ordered():
    from bot.handlers.cashier import _backdate_options

    opts = _backdate_options(7)
    assert len(opts) == 7
    today_iso = date.today().isoformat()
    isos = [iso for iso, _ in opts]
    assert today_iso not in isos
    # Newest first (yesterday at index 0).
    assert isos == sorted(isos, reverse=True)
    # All labels contain a 2-letter weekday abbrev + dd.mm.
    for iso, label in opts:
        assert "." in label
        d = datetime.strptime(iso, "%Y-%m-%d").date()
        assert d < date.today()
