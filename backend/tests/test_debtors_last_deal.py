"""Regression test for the debt-dashboard "last deal" date.

Origin: 2026-06-16 — КАМОЛИДДИН ЖУМА БОЗОР showed last deal 2026-05-08 on the
debt-management dashboard despite a real shipment the day before. Root cause:
the dashboard read `client_debts.last_transaction_date`, a truncate-replace
snapshot of the most-recent /debtors export (then dated 2026-06-13), which is
structurally blind to a /realorders shipment uploaded after it. Fix: the
"last deal" date is now the freshest of `client_debts.last_transaction_date`
and the client's latest approved `real_orders.doc_date`.

Family: Error Log #78 / daily_incremental_blind_to_backdated.
"""
import os
import sqlite3
import tempfile

import pytest


@pytest.fixture
def setup_db(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_catalog.db")
        monkeypatch.setenv("DATABASE_PATH", db_path)
        monkeypatch.setenv("ADMIN_API_KEY", "test-admin-key")
        import backend.database as db_mod
        monkeypatch.setattr(db_mod, "DATABASE_PATH", db_path)
        # admin_auth captures ADMIN_API_KEY into a module constant at import
        # time, so setenv alone is import-order-dependent in the full suite.
        import backend.admin_auth as auth_mod
        monkeypatch.setattr(auth_mod, "_CURRENT", "test-admin-key")
        # apply_debtors_import() fires payment notifications on a background
        # thread that touches the DB; under the full suite it can outlive the
        # tmpdir and error during teardown. Neutralize it for these tests.
        import backend.services.payment_notifications as pn
        monkeypatch.setattr(pn, "fire_pending_for_today_async", lambda *a, **k: None)
        db_mod.init_db()
        yield db_path


def _client(db_path, cid, name):
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT INTO allowed_clients (id, client_id_1c, name, phone_normalized)
           VALUES (?, ?, ?, ?)""",
        (cid, name, name, f"99890{cid:07d}"),
    )
    conn.commit()
    conn.close()


def _debt(db_path, cid, name, last_tx_date, report_date="2026-06-13"):
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT INTO client_debts
               (client_name_1c, client_id, debt_uzs, debt_usd,
                last_transaction_date, last_transaction_no, report_date)
           VALUES (?, ?, 0, 100, ?, '1', ?)""",
        (name, cid, last_tx_date, report_date),
    )
    conn.commit()
    conn.close()


def _real_order(db_path, doc, cid, name, doc_date, is_approved=1):
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT INTO real_orders
               (doc_number_1c, doc_date, client_name_1c, client_id, is_approved)
           VALUES (?, ?, ?, ?, ?)""",
        (doc, doc_date, name, cid, is_approved),
    )
    conn.commit()
    conn.close()


def _get_items(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.routers.admin_debtors import router

    app = FastAPI()
    app.include_router(router)
    resp = TestClient(app).get("/api/admin/debtors-list?admin_key=test-admin-key")
    assert resp.status_code == 200, resp.text
    return {it["client_name"]: it for it in resp.json()["items"]}


def test_realorders_fresher_than_debts_wins(setup_db, monkeypatch):
    # The origin bug: debts snapshot says May 8, real_orders shipped Jun 15.
    _client(setup_db, 816, "КАМОЛИДДИН ЖУМА БОЗОР")
    _debt(setup_db, 816, "КАМОЛИДДИН ЖУМА БОЗОР", "2026-05-08")
    _real_order(setup_db, "4690", 816, "КАМОЛИДДИН ЖУМА БОЗОР", "2026-06-15")
    it = _get_items(monkeypatch)["КАМОЛИДДИН ЖУМА БОЗОР"]
    assert it["last_transaction_date"] == "2026-06-15"
    assert it["last_deal_date"] == "2026-06-15"
    assert it["last_debt_tx_date"] == "2026-05-08"


def test_debts_fresher_than_realorders_wins(setup_db, monkeypatch):
    # A non-shipment ledger event (return/adjustment) can leave the debtors
    # date ahead of the last real_order — never regress below it.
    _client(setup_db, 5, "Debt Newer")
    _debt(setup_db, 5, "Debt Newer", "2026-06-14")
    _real_order(setup_db, "100", 5, "Debt Newer", "2026-06-01")
    it = _get_items(monkeypatch)["Debt Newer"]
    assert it["last_transaction_date"] == "2026-06-14"


def test_no_realorder_falls_back_to_debts(setup_db, monkeypatch):
    _client(setup_db, 6, "No Deal")
    _debt(setup_db, 6, "No Deal", "2026-05-08")
    it = _get_items(monkeypatch)["No Deal"]
    assert it["last_transaction_date"] == "2026-05-08"
    assert it["last_deal_date"] is None


def test_unapproved_realorder_ignored(setup_db, monkeypatch):
    # is_approved=0 (V-marked / deleted doc) must not count as a deal.
    _client(setup_db, 7, "Unapproved")
    _debt(setup_db, 7, "Unapproved", "2026-05-08")
    _real_order(setup_db, "200", 7, "Unapproved", "2026-06-15", is_approved=0)
    it = _get_items(monkeypatch)["Unapproved"]
    assert it["last_transaction_date"] == "2026-05-08"


def test_legacy_null_approved_realorder_counts(setup_db, monkeypatch):
    # COALESCE(is_approved,1)=1 — legacy NULL rows are treated as shipped.
    _client(setup_db, 8, "Legacy Null")
    _debt(setup_db, 8, "Legacy Null", "2026-05-08")
    _real_order(setup_db, "300", 8, "Legacy Null", "2026-06-15", is_approved=None)
    it = _get_items(monkeypatch)["Legacy Null"]
    assert it["last_transaction_date"] == "2026-06-15"


# --- Staleness guard: an older /debtors report must not overwrite a newer one
# (Error Log #102: 06-13 export applied 7s after 06-15 reverted the table). ---

def _fake_parse(report_date):
    """A minimal parse_debtors_xls() result with one client."""
    return {
        "ok": True,
        "report_date": report_date,
        "title": f"test {report_date}",
        "clients": [{
            "client_name_1c": "X", "debt_uzs": 0, "debt_usd": 100,
            "last_transaction_date": report_date, "last_transaction_no": "1",
            "aging_0_30": 0, "aging_31_60": 0, "aging_61_90": 0,
            "aging_91_120": 0, "aging_120_plus": 0,
        }],
    }


def _apply(monkeypatch, report_date, force=False):
    import backend.services.import_debts as imp
    monkeypatch.setattr(imp, "parse_debtors_xls", lambda _b: _fake_parse(report_date))
    return imp.apply_debtors_import(b"ignored", force=force)


def _loaded_report_date(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT MAX(report_date) FROM client_debts").fetchone()[0]
    finally:
        conn.close()


def test_older_report_is_blocked(setup_db, monkeypatch):
    assert _apply(monkeypatch, "2026-06-15")["ok"] is True
    res = _apply(monkeypatch, "2026-06-13")  # older — must refuse
    assert res.get("stale_blocked") is True
    assert res["current_report_date"] == "2026-06-15"
    assert _loaded_report_date(setup_db) == "2026-06-15"  # table untouched


def test_newer_report_proceeds(setup_db, monkeypatch):
    assert _apply(monkeypatch, "2026-06-15")["ok"] is True
    assert _apply(monkeypatch, "2026-06-16")["ok"] is True
    assert _loaded_report_date(setup_db) == "2026-06-16"


def test_same_date_refresh_allowed(setup_db, monkeypatch):
    assert _apply(monkeypatch, "2026-06-15")["ok"] is True
    assert _apply(monkeypatch, "2026-06-15")["ok"] is True  # idempotent


def test_force_overrides_staleness(setup_db, monkeypatch):
    assert _apply(monkeypatch, "2026-06-15")["ok"] is True
    assert _apply(monkeypatch, "2026-06-13", force=True)["ok"] is True
    assert _loaded_report_date(setup_db) == "2026-06-13"  # deliberate rollback
