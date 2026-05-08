"""Tests for agent-initiated shop registration."""
import pytest

from backend.services.agent_register import register_new_shop


def _make_agent(db, telegram_id, role="agent"):
    db.execute(
        "INSERT INTO users (telegram_id, phone, is_approved, is_agent, agent_role) "
        "VALUES (?, ?, 1, 1, ?)",
        (telegram_id, f"99890{telegram_id}", role),
    )


def test_register_creates_new_whitelist_row(db):
    _make_agent(db, 1001)
    r = register_new_shop(db, 1001, "Дустлик", "+998 90 111 22 33", 41.31, 69.27)
    assert r["status"] == "created"
    assert r["client_id"] > 0

    row = db.execute(
        "SELECT name, phone_normalized, source_sheet, segment, "
        "gps_latitude, gps_longitude, gps_set_by_role, gps_set_by_tg_id "
        "FROM allowed_clients WHERE id = ?",
        (r["client_id"],),
    ).fetchone()
    assert row["name"] == "Дустлик"
    assert row["phone_normalized"] == "901112233"
    assert row["source_sheet"] == "agent_panel"
    assert row["segment"] == "shop"
    assert row["gps_latitude"] == 41.31
    assert row["gps_longitude"] == 69.27
    assert row["gps_set_by_role"] == "agent"
    assert row["gps_set_by_tg_id"] == 1001


def test_register_writes_audit_row(db):
    _make_agent(db, 1002)
    r = register_new_shop(db, 1002, "Айгуль", "+998901234567", 41.0, 69.0)
    assert r["status"] == "created"
    audit = db.execute(
        "SELECT agent_telegram_id, shop_name, phone_normalized, status, linked_client_id "
        "FROM agent_client_registrations ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert audit["agent_telegram_id"] == 1002
    assert audit["shop_name"] == "Айгуль"
    assert audit["phone_normalized"] == "901234567"
    assert audit["status"] == "created"
    assert audit["linked_client_id"] == r["client_id"]


def test_register_phone_collision_links_existing_row(db):
    _make_agent(db, 1003)
    # Pre-existing whitelisted shop with the same phone
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, status, client_id_1c) "
        "VALUES (?, ?, 'active', ?)",
        ("905554433", "Old Shop", "OLD-1C-42"),
    )
    existing_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

    r = register_new_shop(db, 1003, "New Name", "+998 90 555 44 33", 41.0, 69.0)
    assert r["status"] == "linked_existing"
    assert r["client_id"] == existing_id
    assert r["client"]["name"] == "Old Shop"
    assert r["client"]["client_id_1c"] == "OLD-1C-42"

    # Audit row marked correctly
    audit = db.execute(
        "SELECT status, linked_client_id FROM agent_client_registrations "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert audit["status"] == "linked_existing"
    assert audit["linked_client_id"] == existing_id

    # No duplicate created
    n = db.execute(
        "SELECT COUNT(*) FROM allowed_clients WHERE phone_normalized = ?",
        ("905554433",),
    ).fetchone()[0]
    assert n == 1


def test_register_collision_matches_secondary_phone_slot(db):
    _make_agent(db, 1004)
    # Pre-existing shop with the phone parked in raqam_03 slot
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, raqam_03, status) "
        "VALUES (?, ?, ?, 'active')",
        ("900000000", "Shop", "907777777"),
    )
    existing_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

    r = register_new_shop(db, 1004, "Other", "+998 90 777 77 77", 41.0, 69.0)
    assert r["status"] == "linked_existing"
    assert r["client_id"] == existing_id


def test_register_validates_inputs(db):
    _make_agent(db, 1005)
    # Short shop name
    r = register_new_shop(db, 1005, "A", "+998901234567", 41.0, 69.0)
    assert r["status"] == "failed"
    # Phone too short
    r = register_new_shop(db, 1005, "Shop", "12345", 41.0, 69.0)
    assert r["status"] == "failed"
    # Missing GPS
    r = register_new_shop(db, 1005, "Shop", "+998901234567", None, None)
    assert r["status"] == "failed"
    # No allowed_clients row created on failure
    n = db.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]
    assert n == 0


def test_register_skips_merged_clients_in_collision_check(db):
    """A merged-status row with the same phone shouldn't block a new
    registration — merged rows are tombstones, not active clients."""
    _make_agent(db, 1006)
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, status) "
        "VALUES (?, ?, 'merged')",
        ("908888888", "Tombstone"),
    )
    r = register_new_shop(db, 1006, "Active Shop", "+998 90 888 88 88", 41.0, 69.0)
    assert r["status"] == "created"
