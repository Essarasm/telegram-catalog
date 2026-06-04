"""Client Identity Anchoring — Phase 4 follow-up: finance relink via rename map.

relink_orphan_finance_rows() reclaims orphan finance rows still keyed by a
client's OLD 1C name after a rename (client_name_history old→new), with a guard
that skips an old name another active client now reuses (#75 name collision).
"""
from backend.services.client_search import relink_orphan_finance_rows


def _bal(db, name, cid=None, period="2026-01-01"):
    db.execute(
        "INSERT INTO client_balances (client_name_1c, currency, period_start, "
        "period_end, client_id) VALUES (?, 'UZS', ?, ?, ?)",
        (name, period, period, cid),
    )


def test_relink_reclaims_old_name_orphans(db):
    db.execute("INSERT INTO allowed_clients (id, client_id_1c, name, phone_normalized, status) "
               "VALUES (80, 'NEW NOM', 'NEW NOM', '900800080', 'active')")
    db.execute("INSERT INTO client_name_history (client_id, old_name, new_name, reason, changed_by) "
               "VALUES (80, 'ESKI NOM', 'NEW NOM', 'test', 't')")
    _bal(db, "ESKI NOM", period="2026-01-01")   # orphan under old name
    _bal(db, "NEW NOM", period="2026-02-01")    # orphan under current name

    counts = relink_orphan_finance_rows(db, 80, "NEW NOM")
    assert counts["client_balances"] == 2
    assert db.execute("SELECT COUNT(*) FROM client_balances WHERE client_id=80").fetchone()[0] == 2


def test_relink_skips_reused_old_name(db):
    db.execute("INSERT INTO allowed_clients (id, client_id_1c, name, phone_normalized, status) "
               "VALUES (81, 'NEW2', 'NEW2', '900800081', 'active')")
    db.execute("INSERT INTO allowed_clients (id, client_id_1c, name, phone_normalized, status) "
               "VALUES (82, 'ESKI2', 'ESKI2', '900800082', 'active')")  # reuses the old name
    db.execute("INSERT INTO client_name_history (client_id, old_name, new_name, reason, changed_by) "
               "VALUES (81, 'ESKI2', 'NEW2', 'test', 't')")
    _bal(db, "ESKI2")   # orphan under the reused name — must NOT go to 81

    counts = relink_orphan_finance_rows(db, 81, "NEW2")
    assert counts["client_balances"] == 0
    assert db.execute("SELECT client_id FROM client_balances WHERE client_name_1c='ESKI2'"
                      ).fetchone()[0] is None
