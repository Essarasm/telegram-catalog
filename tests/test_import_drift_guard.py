"""#74 importer identity-drift guard (`_upsert_client_from_row`).

A phone/raqam-match upsert that would rewrite `client_id_1c` on a row carrying
curated state (pin / credit / linked user) is held in
`client_identity_drift_queue` instead of mutating the curated row.
"""
from backend.services.import_clients import _upsert_client_from_row


def _seed(db, cid, name_1c, phone, gps=None, credit=None, limit=None, user_tg=None):
    db.execute(
        "INSERT INTO allowed_clients (id, client_id_1c, name, phone_normalized, "
        "gps_latitude, credit_score, credit_limit, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'active')",
        (cid, name_1c, name_1c, phone, gps, credit, limit),
    )
    if user_tg:
        db.execute("INSERT INTO users (telegram_id, client_id) VALUES (?, ?)", (user_tg, cid))


def _upsert(db, phone, cid_1c, name=None):
    return _upsert_client_from_row(
        db, raw_phone_str=phone, client_name=name or cid_1c, location="",
        source="clients_upload", cid_1c=cid_1c, company="", changed_by_tag="test",
    )


def _queue(db):
    return db.execute(
        "SELECT * FROM client_identity_drift_queue WHERE resolved = 0"
    ).fetchall()


def test_drift_held_on_curated_phone_match(db):
    # САРДОР row has a pin; 1C reassigns its phone to Мурод → must be HELD.
    _seed(db, 1302, "САРДОР Пищевой", "979310404", gps=39.6)
    out, rid = _upsert(db, "979310404", "Мурод ака Вокзал")
    assert out == "drift_held" and rid == 1302
    # client_id_1c left untouched
    assert db.execute("SELECT client_id_1c FROM allowed_clients WHERE id=1302"
                      ).fetchone()[0] == "САРДОР Пищевой"
    q = _queue(db)
    assert len(q) == 1
    assert q[0]["existing_client_id_1c"] == "САРДОР Пищевой"
    assert q[0]["incoming_client_id_1c"] == "Мурод ака Вокзал"
    assert "gps" in q[0]["curated_state"]
    # row flagged for review
    assert db.execute("SELECT needs_review FROM allowed_clients WHERE id=1302"
                      ).fetchone()[0] == 1


def test_drift_hold_is_idempotent(db):
    # Re-running the same drift (e.g. the daily import) must NOT stack duplicate
    # queue rows — that is what produced client 722's double rows (Error Log #86 #3).
    _seed(db, 1302, "САРДОР Пищевой", "979310404", gps=39.6)
    out1, _ = _upsert(db, "979310404", "Мурод ака Вокзал")
    out2, _ = _upsert(db, "979310404", "Мурод ака Вокзал")
    assert out1 == "drift_held" and out2 == "drift_held"
    assert len(_queue(db)) == 1          # one unresolved hold, not two


def test_credit_and_linked_user_also_trigger_hold(db):
    _seed(db, 700, "OLD NAME", "900000001", credit=82, limit=820000, user_tg=555)
    out, _ = _upsert(db, "900000001", "NEW NAME")
    assert out == "drift_held"
    cs = _queue(db)[0]["curated_state"]
    assert "credit_score" in cs and "linked_user" in cs


def test_no_hold_when_row_has_no_curated_state(db):
    # No pin / credit / user → not a drift target; normal update path runs.
    _seed(db, 800, "OLD NAME", "900000002")
    out, _ = _upsert(db, "900000002", "NEW NAME")
    assert out != "drift_held"
    assert _queue(db) == []


def test_no_hold_on_cid_1c_match(db):
    # Matched by client_id_1c (same name) — can't be drift even if curated.
    _seed(db, 900, "SAME NAME", "900000003", gps=40.0)
    out, _ = _upsert(db, "900000099", "SAME NAME")  # different phone → cid match
    assert out != "drift_held"
    assert _queue(db) == []


def test_no_hold_when_incoming_name_equals_existing(db):
    # Curated row, phone match, SAME client_id_1c → legitimate re-import.
    _seed(db, 1000, "STABLE NAME", "900000004", gps=41.0)
    out, _ = _upsert(db, "900000004", "STABLE NAME")
    assert out != "drift_held"
    assert _queue(db) == []
