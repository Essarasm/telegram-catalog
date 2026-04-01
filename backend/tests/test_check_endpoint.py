"""Tests for /api/users/check endpoint — verifies the 4-layer auth fallback.

Layer 1: SQLite DB (users table)
Layer 2: JSON backup file (read by _find_user_in_backup)
Layer 3: Telegram CloudStorage (client-side, untestable here)
Layer 4: approved_overrides.json + ALWAYS_APPROVED_IDS env var
"""
import json
import os
import sqlite3
import tempfile
import pytest

# We need to set env vars BEFORE importing the router module, because
# users.py reads ALWAYS_APPROVED_IDS and approved_overrides.json at import time.
# For tests we control these via fixtures that patch the module-level state.


@pytest.fixture
def tmp_dirs():
    """Create temp directories for DB and backup files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_catalog.db")
        backup_path = os.path.join(tmpdir, "users_backup.json")
        yield {"dir": tmpdir, "db": db_path, "backup": backup_path}


@pytest.fixture
def setup_db(tmp_dirs, monkeypatch):
    """Create a minimal users table in a temp SQLite DB."""
    db_path = tmp_dirs["db"]
    backup_path = tmp_dirs["backup"]

    # Patch env vars before importing modules
    monkeypatch.setenv("DATABASE_PATH", db_path)
    monkeypatch.setenv("USERS_BACKUP_PATH", backup_path)

    # Patch database module
    import backend.database as db_mod
    monkeypatch.setattr(db_mod, "DATABASE_PATH", db_path)

    # Also patch backup module
    import backend.services.backup_users as backup_mod
    monkeypatch.setattr(backup_mod, "BACKUP_PATH", backup_path)
    monkeypatch.setattr(backup_mod, "DATABASE_PATH", db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            phone TEXT,
            first_name TEXT,
            last_name TEXT,
            username TEXT,
            latitude REAL,
            longitude REAL,
            is_approved INTEGER DEFAULT 0,
            client_id INTEGER,
            registered_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS allowed_clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_normalized TEXT NOT NULL,
            name TEXT,
            location TEXT,
            source_sheet TEXT,
            client_id_1c TEXT,
            company_name TEXT,
            status TEXT DEFAULT 'active',
            matched_telegram_id INTEGER,
            credit_score INTEGER,
            credit_limit REAL,
            notes TEXT
        )
    """)
    conn.commit()
    conn.close()
    return tmp_dirs


def _get_test_client(setup_db, monkeypatch):
    """Create a FastAPI test client with patched DB paths."""
    from fastapi.testclient import TestClient
    from backend.routers.users import router
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _insert_user(db_path, telegram_id, phone, first_name="Test", is_approved=1):
    """Insert a user directly into the test DB."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO users (telegram_id, phone, first_name, is_approved) VALUES (?, ?, ?, ?)",
        (telegram_id, phone, first_name, is_approved),
    )
    conn.commit()
    conn.close()


def _write_backup(backup_path, users):
    """Write a JSON backup file."""
    with open(backup_path, "w") as f:
        json.dump(users, f)


class TestCheckLayer1_DB:
    """Layer 1: User found in SQLite DB."""

    def test_known_approved_user(self, setup_db, monkeypatch):
        _insert_user(setup_db["db"], 12345, "+998901234567", "Alice", is_approved=1)
        client = _get_test_client(setup_db, monkeypatch)
        resp = client.get("/api/users/check?telegram_id=12345")
        data = resp.json()
        assert data["registered"] is True
        assert data["approved"] is True
        assert data["phone"] == "+998901234567"

    def test_known_unapproved_user(self, setup_db, monkeypatch):
        _insert_user(setup_db["db"], 67890, "+998909876543", "Bob", is_approved=0)
        client = _get_test_client(setup_db, monkeypatch)
        resp = client.get("/api/users/check?telegram_id=67890")
        data = resp.json()
        assert data["registered"] is True
        assert data["approved"] is False

    def test_unknown_user_no_backup(self, setup_db, monkeypatch):
        """User not in DB and no backup file → not registered."""
        client = _get_test_client(setup_db, monkeypatch)
        resp = client.get("/api/users/check?telegram_id=99999")
        data = resp.json()
        assert data["registered"] is False
        assert data["approved"] is False


class TestCheckLayer2_JSONBackup:
    """Layer 2: User not in DB but found in JSON backup → self-healing re-insert."""

    def test_fallback_to_backup(self, setup_db, monkeypatch):
        """User missing from DB but present in backup → registered=True, re-inserted into DB."""
        _write_backup(setup_db["backup"], [
            {
                "telegram_id": 11111,
                "phone": "+998901112233",
                "first_name": "Charlie",
                "last_name": "",
                "username": "charlie",
                "latitude": None,
                "longitude": None,
                "is_approved": 1,
                "client_id": None,
            }
        ])
        client = _get_test_client(setup_db, monkeypatch)
        resp = client.get("/api/users/check?telegram_id=11111")
        data = resp.json()
        assert data["registered"] is True
        assert data["approved"] is True
        assert data["phone"] == "+998901112233"

        # Verify self-healing: user should now be in the DB
        conn = sqlite3.connect(setup_db["db"])
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM users WHERE telegram_id = 11111").fetchone()
        conn.close()
        assert row is not None
        assert row["phone"] == "+998901112233"
        assert row["is_approved"] == 1

    def test_backup_unapproved_user(self, setup_db, monkeypatch):
        """User in backup but not approved → registered=True, approved=False."""
        _write_backup(setup_db["backup"], [
            {
                "telegram_id": 22222,
                "phone": "+998902223344",
                "first_name": "Diana",
                "last_name": "",
                "username": "",
                "latitude": None,
                "longitude": None,
                "is_approved": 0,
                "client_id": None,
            }
        ])
        client = _get_test_client(setup_db, monkeypatch)
        resp = client.get("/api/users/check?telegram_id=22222")
        data = resp.json()
        assert data["registered"] is True
        assert data["approved"] is False

    def test_backup_file_missing(self, setup_db, monkeypatch):
        """No backup file at all → user not found."""
        client = _get_test_client(setup_db, monkeypatch)
        resp = client.get("/api/users/check?telegram_id=33333")
        data = resp.json()
        assert data["registered"] is False

    def test_backup_file_corrupted(self, setup_db, monkeypatch):
        """Corrupted backup file → graceful fallback, user not found."""
        with open(setup_db["backup"], "w") as f:
            f.write("{bad json!!")
        client = _get_test_client(setup_db, monkeypatch)
        resp = client.get("/api/users/check?telegram_id=44444")
        data = resp.json()
        assert data["registered"] is False


class TestCheckLayer4_Overrides:
    """Layer 4: ALWAYS_APPROVED_IDS override."""

    def test_override_approves_db_user(self, setup_db, monkeypatch):
        """User in DB as unapproved, but in ALWAYS_APPROVED → approved=True."""
        import backend.routers.users as users_mod
        original = users_mod._ALWAYS_APPROVED.copy()
        users_mod._ALWAYS_APPROVED.add(55555)
        try:
            _insert_user(setup_db["db"], 55555, "+998905556677", "Eve", is_approved=0)
            client = _get_test_client(setup_db, monkeypatch)
            resp = client.get("/api/users/check?telegram_id=55555")
            data = resp.json()
            assert data["registered"] is True
            assert data["approved"] is True
        finally:
            users_mod._ALWAYS_APPROVED = original

    def test_override_for_unknown_user(self, setup_db, monkeypatch):
        """User not in DB or backup, but in ALWAYS_APPROVED → registered=False but approved=True."""
        import backend.routers.users as users_mod
        original = users_mod._ALWAYS_APPROVED.copy()
        users_mod._ALWAYS_APPROVED.add(66666)
        try:
            client = _get_test_client(setup_db, monkeypatch)
            resp = client.get("/api/users/check?telegram_id=66666")
            data = resp.json()
            # Not registered (no phone/data), but override says approved
            assert data["registered"] is False
            assert data["approved"] is True
        finally:
            users_mod._ALWAYS_APPROVED = original


class TestBackupUsersPersistence:
    """Test save_user_to_backup atomic writes."""

    def test_save_creates_backup_file(self, setup_db, monkeypatch):
        from backend.services.backup_users import save_user_to_backup
        save_user_to_backup({
            "telegram_id": 77777,
            "phone": "+998907778899",
            "first_name": "Frank",
            "is_approved": 1,
        })
        assert os.path.exists(setup_db["backup"])
        with open(setup_db["backup"]) as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["telegram_id"] == 77777

    def test_save_merges_with_existing(self, setup_db, monkeypatch):
        """Saving a new user preserves existing users in backup."""
        from backend.services.backup_users import save_user_to_backup
        _write_backup(setup_db["backup"], [
            {"telegram_id": 88888, "phone": "+998908889900", "first_name": "Grace", "is_approved": 1}
        ])
        save_user_to_backup({
            "telegram_id": 99999,
            "phone": "+998909990011",
            "first_name": "Hank",
            "is_approved": 0,
        })
        with open(setup_db["backup"]) as f:
            data = json.load(f)
        ids = {u["telegram_id"] for u in data}
        assert ids == {88888, 99999}

    def test_save_preserves_approved_status(self, setup_db, monkeypatch):
        """Re-saving a user who was approved should not downgrade to unapproved."""
        from backend.services.backup_users import save_user_to_backup
        _write_backup(setup_db["backup"], [
            {"telegram_id": 11111, "phone": "+998901112233", "first_name": "Ivy", "is_approved": 1}
        ])
        # Save same user with is_approved=0 — should keep 1
        save_user_to_backup({
            "telegram_id": 11111,
            "phone": "+998901112233",
            "first_name": "Ivy Updated",
            "is_approved": 0,
        })
        with open(setup_db["backup"]) as f:
            data = json.load(f)
        user = [u for u in data if u["telegram_id"] == 11111][0]
        assert user["is_approved"] == 1  # preserved!
        assert user["first_name"] == "Ivy Updated"  # but other fields updated

    def test_save_skips_without_telegram_id(self, setup_db, monkeypatch):
        """save_user_to_backup with no telegram_id should be a no-op."""
        from backend.services.backup_users import save_user_to_backup
        save_user_to_backup({"phone": "+998901234567"})
        assert not os.path.exists(setup_db["backup"])
