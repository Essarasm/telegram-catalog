"""Client Identity Anchoring — Phase 3: stale pending-1C-adoption audit.

A registration-sourced allowed_clients row that the daily 1C import never
adopted (no onec_card_id) and that's older than 14 days is a stranded pending
row (phone mismatch with the 1C card, or no card ever created) — surfaced for
an admin to create/link the 1C card.
"""
from backend.services.consistency_audit import run_audit


def _reg_row(db, cid, phone, source, tg, age_days, card=None):
    db.execute(
        "INSERT INTO allowed_clients (id, name, phone_normalized, source_sheet, "
        "status, matched_telegram_id, onec_card_id) VALUES (?, ?, ?, ?, 'active', ?, ?)",
        (cid, f"Shop{cid}", phone, source, tg, card),
    )
    db.execute(
        "INSERT INTO users (telegram_id, registered_at, client_id) "
        "VALUES (?, datetime('now', ?), ?)",
        (tg, f"-{age_days} days", cid),
    )


def test_pending_adoption_flags_only_stranded(db):
    _reg_row(db, 1, "900000001", "bot_new_client", 7001, 30)            # stranded → flag
    _reg_row(db, 2, "900000002", "bot_new_client", 7002, 30, card="Прочие:9")  # adopted → no
    _reg_row(db, 3, "900000003", "bot_new_client", 7003, 2)            # too recent → no
    _reg_row(db, 4, "900000004", "clients_upload", 7004, 30)           # 1C-sourced → no
    _reg_row(db, 5, "900000005", "agent_panel", 7005, 20)             # stranded agent → flag
    db.commit()

    res = run_audit()
    p = res.get("pending_onec_adoption")
    assert p is not None
    assert p["count"] == 2
    assert {r["id"] for r in p["sample"]} == {1, 5}


def test_pending_adoption_clean_when_all_adopted(db):
    _reg_row(db, 10, "900000010", "bot_new_client", 7010, 30, card="Прочие:10")
    db.commit()
    assert "pending_onec_adoption" not in run_audit()
