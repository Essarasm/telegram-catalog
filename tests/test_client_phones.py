"""Client Identity Anchoring — Phase 1 (client_phones one-to-many mirror).

client_phones mirrors the allowed_clients phone slots (phone_normalized = primary,
raqam_02/03 = secondaries) one-way, maintained by sync_client_phones() on every
phone write (importer + fill_empty_slot) and read via get_client_phones().
"""
from backend.services.phone_slots import (
    sync_client_phones, get_client_phones, backfill_client_phones, fill_empty_slot,
)
from backend.services.import_clients import _upsert_client_from_row


def _seed(db, cid, primary, r02=None, r03=None, status="active"):
    db.execute(
        "INSERT INTO allowed_clients (id, phone_normalized, raqam_02, raqam_03, name, status) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (cid, primary, r02, r03, f"C{cid}", status),
    )


# ── backfill ────────────────────────────────────────────────────────────────

def test_backfill_three_phones_primary_first(db):
    _seed(db, 1, "901111111", "902222222", "903333333")
    backfill_client_phones(db)
    ph = get_client_phones(db, 1)
    assert [p["phone"] for p in ph] == ["901111111", "902222222", "903333333"]
    assert ph[0]["is_primary"] and not ph[1]["is_primary"] and not ph[2]["is_primary"]


def test_backfill_dedups_primary_repeated_in_slot(db):
    _seed(db, 2, "905555555", "905555555", None)  # raqam_02 == primary (stale dup)
    backfill_client_phones(db)
    assert [p["phone"] for p in get_client_phones(db, 2)] == ["905555555"]


def test_backfill_only_primary(db):
    _seed(db, 3, "907777777")
    backfill_client_phones(db)
    assert [p["phone"] for p in get_client_phones(db, 3)] == ["907777777"]


def test_backfill_skips_merged_rows(db):
    _seed(db, 4, "908888888", status="merged_into:1")
    backfill_client_phones(db)
    assert get_client_phones(db, 4) == []


def test_get_client_phones_empty_for_unknown(db):
    assert get_client_phones(db, 9999) == []


# ── sync ──────────────────────────────────────────────────────────────────────

def test_sync_idempotent_no_duplicates(db):
    _seed(db, 5, "909999999", "910000000")
    sync_client_phones(db, 5)
    sync_client_phones(db, 5)  # second run must not duplicate
    assert len(get_client_phones(db, 5)) == 2


def test_sync_reflects_added_then_removed_slot(db):
    _seed(db, 6, "911111111")
    sync_client_phones(db, 6)
    assert len(get_client_phones(db, 6)) == 1
    db.execute("UPDATE allowed_clients SET raqam_02=? WHERE id=6", ("912222222",))
    sync_client_phones(db, 6)
    assert [p["phone"] for p in get_client_phones(db, 6)] == ["911111111", "912222222"]
    db.execute("UPDATE allowed_clients SET raqam_02=NULL WHERE id=6")
    sync_client_phones(db, 6)
    assert [p["phone"] for p in get_client_phones(db, 6)] == ["911111111"]


# ── writer wiring ─────────────────────────────────────────────────────────────

def test_importer_insert_populates_client_phones(db):
    out, rid = _upsert_client_from_row(
        db, raw_phone_str="93 356 12 12, эри 97 918 33 33", client_name="Yangi",
        location="", source="clients_upload", cid_1c="Yangi", company="",
        changed_by_tag="test",
    )
    assert out == "inserted"
    ph = get_client_phones(db, rid)
    assert [p["phone"] for p in ph] == ["933561212", "979183333"]
    assert ph[0]["is_primary"]


def test_fill_empty_slot_syncs_mirror(db):
    _seed(db, 7, "913333333")
    sync_client_phones(db, 7)
    assert fill_empty_slot(db, 7, "914444444") == "filled_02"
    assert [p["phone"] for p in get_client_phones(db, 7)] == ["913333333", "914444444"]
