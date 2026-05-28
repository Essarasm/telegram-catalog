"""Per-client location-decision queue — dispatch + callback flow.

Tests `dispatch_location_decision` (queue insert + message dispatch) and the
keep_old / use_new callback handlers. Telegram send is monkey-patched out so
the test runs offline.

Origin: Session M 2026-05-28.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _seed_client(db, *, cid=1, lat=39.65, lng=66.97, setter_tg=111,
                  setter_name="Musobek", setter_role="agent"):
    db.execute(
        "INSERT INTO allowed_clients (id, name, client_id_1c, phone_normalized, "
        "gps_latitude, gps_longitude, gps_address, gps_region, gps_district, "
        "gps_set_at, gps_set_by_tg_id, gps_set_by_name, gps_set_by_role, status) "
        "VALUES (?,?,?,?,?,?,?,?,?,datetime('now'),?,?,?,'active')",
        (cid, "Тест Mижоз", "Тест Mижоз", f"99800000{cid:04d}", lat, lng,
         "Тест манзил", "Samarqand viloyati", "Тест tuman",
         setter_tg, setter_name, setter_role),
    )
    db.commit()


def _patch_dispatch_send(monkeypatch, ok=True, msg_id=4242):
    """Replace httpx.post so dispatch_location_decision doesn't hit Telegram."""
    sent = {}

    class _FakeResp:
        def json(self):
            if ok:
                return {"ok": True, "result": {"message_id": msg_id}}
            return {"ok": False, "description": "test stub"}

    def _fake_post(url, **kwargs):
        sent["url"] = url
        sent["json"] = kwargs.get("json")
        return _FakeResp()

    monkeypatch.setattr("httpx.post", _fake_post)
    # dispatch short-circuits when BOT_TOKEN is empty (tests have no .env);
    # patch the module-level reference to a non-empty value.
    monkeypatch.setattr("bot.handlers.location_decisions.BOT_TOKEN", "test-token")
    return sent


# ── dispatch_location_decision ───────────────────────────────────────


def test_dispatch_inserts_pending_row(db, monkeypatch):
    from bot.handlers.location_decisions import dispatch_location_decision
    _seed_client(db, cid=1)
    _patch_dispatch_send(monkeypatch)

    prior = db.execute("SELECT * FROM allowed_clients WHERE id=1").fetchone()
    pld_id = dispatch_location_decision(
        db, client_id=1, client_name="Тест Mижоз", client_id_1c="Тест Mижоз",
        prior_row=prior,
        incoming_lat=39.70, incoming_lng=66.99,
        incoming_geo={"address": "Yangi joy", "region": "Sam viloyati", "district": "Чардара"},
        incoming_by_tg_id=222, incoming_by_name="Bektimur", incoming_by_role="agent",
        incoming_attempt_id=42, distance_m=4321.0, source_path="driver_lokatsiya",
    )
    assert pld_id is not None

    row = db.execute("SELECT * FROM pending_location_decisions WHERE id=?", (pld_id,)).fetchone()
    assert row is not None
    assert row["status"] == "pending"
    assert row["client_id"] == 1
    assert row["incoming_lat"] == 39.70
    assert row["incoming_by_name"] == "Bektimur"
    assert row["source_path"] == "driver_lokatsiya"
    assert row["distance_m"] == 4321.0
    assert row["dispatched_message_id"] == 4242  # stub returned this


def test_dispatch_send_failure_leaves_pending_row_intact(db, monkeypatch):
    from bot.handlers.location_decisions import dispatch_location_decision
    _seed_client(db, cid=2)
    _patch_dispatch_send(monkeypatch, ok=False)

    prior = db.execute("SELECT * FROM allowed_clients WHERE id=2").fetchone()
    pld_id = dispatch_location_decision(
        db, client_id=2, client_name="X", client_id_1c="X",
        prior_row=prior, incoming_lat=39.7, incoming_lng=66.9,
        incoming_geo={"address": "", "region": "", "district": ""},
        incoming_by_tg_id=3, incoming_by_name="Y", incoming_by_role="agent",
        incoming_attempt_id=1, distance_m=500.0, source_path="mini_app_dm",
    )
    assert pld_id is not None
    row = db.execute("SELECT * FROM pending_location_decisions WHERE id=?", (pld_id,)).fetchone()
    assert row["status"] == "pending"
    assert row["dispatched_message_id"] is None


# ── Callback handlers (keep_old / use_new) ────────────────────────────


def _seed_pending(db, *, client_id=1, incoming_lat=39.70, incoming_lng=66.99):
    cur = db.execute(
        "INSERT INTO pending_location_decisions "
        "(client_id, client_name, client_id_1c, prior_lat, prior_lng, "
        " prior_set_by_tg_id, prior_set_by_name, prior_set_by_role, "
        " incoming_lat, incoming_lng, incoming_address, incoming_region, "
        " incoming_district, incoming_by_tg_id, incoming_by_name, "
        " incoming_by_role, incoming_attempt_id, distance_m, source_path) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (client_id, "Тест", "Тест", 39.65, 66.97, 111, "Musobek", "agent",
         incoming_lat, incoming_lng, "Yangi", "Sam viloyati", "Чардара",
         222, "Bektimur", "agent", 42, 4321.0, "driver_lokatsiya"),
    )
    db.commit()
    return cur.lastrowid


def _make_cb(*, tg_id=999, first_name="Admin", data="locdec:keep:1"):
    """Build a minimal CallbackQuery-like mock."""
    cb = MagicMock()
    cb.from_user.id = tg_id
    cb.from_user.first_name = first_name
    cb.from_user.last_name = None
    cb.from_user.username = None
    cb.data = data
    cb.message.chat.id = -1003967758004  # AGENT_APPROVAL_GROUP_CHAT_ID
    cb.message.text = "🔀 Lokatsiya..."
    cb.message.html_text = "🔀 Lokatsiya..."

    async def _answer(*a, **k):
        return None

    async def _edit(*a, **k):
        return None

    cb.answer = MagicMock(side_effect=_answer)
    cb.message.edit_text = MagicMock(side_effect=_edit)
    return cb


def test_keep_old_marks_decision_no_write(db, monkeypatch):
    import asyncio
    import bot.handlers.location_decisions as ld
    import sqlite3
    db_path = db.execute("PRAGMA database_list").fetchone()["file"]

    def _fresh():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        c.create_function("LOWER", 1, lambda s: s.lower() if s else s)
        return c

    monkeypatch.setattr(ld, "get_db", _fresh)
    _seed_client(db, cid=1)
    pld_id = _seed_pending(db, client_id=1)

    cb = _make_cb(data=f"locdec:keep:{pld_id}")
    asyncio.run(ld.cb_keep_old(cb))

    row = db.execute("SELECT * FROM pending_location_decisions WHERE id=?",
                     (pld_id,)).fetchone()
    assert row["status"] == "keep_old"
    assert row["decided_by_tg_id"] == 999

    # allowed_clients pin must be UNCHANGED.
    client = db.execute("SELECT gps_latitude, gps_longitude FROM allowed_clients WHERE id=1").fetchone()
    assert client["gps_latitude"] == 39.65  # original, not the incoming 39.70
    assert client["gps_longitude"] == 66.97


def test_use_new_writes_incoming_pin(db, monkeypatch):
    import asyncio
    import bot.handlers.location_decisions as ld
    import sqlite3
    db_path = db.execute("PRAGMA database_list").fetchone()["file"]

    def _fresh():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        c.create_function("LOWER", 1, lambda s: s.lower() if s else s)
        return c

    monkeypatch.setattr(ld, "get_db", _fresh)
    _seed_client(db, cid=1)
    pld_id = _seed_pending(db, client_id=1, incoming_lat=39.80, incoming_lng=66.50)

    cb = _make_cb(data=f"locdec:use:{pld_id}")
    asyncio.run(ld.cb_use_new(cb))

    row = db.execute("SELECT * FROM pending_location_decisions WHERE id=?",
                     (pld_id,)).fetchone()
    assert row["status"] == "use_new"
    assert row["decided_by_tg_id"] == 999

    client = db.execute(
        "SELECT gps_latitude, gps_longitude, gps_set_by_tg_id, gps_set_by_name "
        "FROM allowed_clients WHERE id=1"
    ).fetchone()
    assert client["gps_latitude"] == 39.80   # NEW
    assert client["gps_longitude"] == 66.50  # NEW
    # Attribution stays with the original incoming agent, not the approving admin.
    assert client["gps_set_by_tg_id"] == 222
    assert client["gps_set_by_name"] == "Bektimur"


def test_use_new_writes_snapshot_to_admin_action_log(db, monkeypatch):
    import asyncio
    import bot.handlers.location_decisions as ld
    import sqlite3
    db_path = db.execute("PRAGMA database_list").fetchone()["file"]

    def _fresh():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        c.create_function("LOWER", 1, lambda s: s.lower() if s else s)
        return c

    monkeypatch.setattr(ld, "get_db", _fresh)
    _seed_client(db, cid=1)
    pld_id = _seed_pending(db, client_id=1)

    cb = _make_cb(data=f"locdec:use:{pld_id}")
    asyncio.run(ld.cb_use_new(cb))

    snap = db.execute(
        "SELECT command, args FROM admin_action_log WHERE command='manual_pin_replacement'"
    ).fetchone()
    assert snap is not None
    assert f'"pld_id": {pld_id}' in snap["args"]


def test_already_decided_callback_no_op(db, monkeypatch):
    import asyncio
    import bot.handlers.location_decisions as ld
    import sqlite3
    db_path = db.execute("PRAGMA database_list").fetchone()["file"]

    def _fresh():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        c.create_function("LOWER", 1, lambda s: s.lower() if s else s)
        return c

    monkeypatch.setattr(ld, "get_db", _fresh)
    _seed_client(db, cid=1)
    pld_id = _seed_pending(db, client_id=1)
    db.execute("UPDATE pending_location_decisions SET status='keep_old' WHERE id=?",
               (pld_id,))
    db.commit()

    cb = _make_cb(data=f"locdec:use:{pld_id}")
    asyncio.run(ld.cb_use_new(cb))

    # Status still keep_old; pin unchanged.
    row = db.execute("SELECT status FROM pending_location_decisions WHERE id=?",
                     (pld_id,)).fetchone()
    assert row["status"] == "keep_old"
    client = db.execute("SELECT gps_latitude FROM allowed_clients WHERE id=1").fetchone()
    assert client["gps_latitude"] == 39.65  # unchanged


def test_non_admin_callback_refused(db, monkeypatch):
    """Callbacks from non-admin chats must be rejected."""
    import asyncio
    import bot.handlers.location_decisions as ld
    import sqlite3
    db_path = db.execute("PRAGMA database_list").fetchone()["file"]

    def _fresh():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        c.create_function("LOWER", 1, lambda s: s.lower() if s else s)
        return c

    monkeypatch.setattr(ld, "get_db", _fresh)
    _seed_client(db, cid=1)
    pld_id = _seed_pending(db, client_id=1)

    cb = _make_cb(data=f"locdec:use:{pld_id}")
    cb.message.chat.id = 99999  # random chat, not admin

    # is_admin_cb consults ADMIN_IDS too; clear it just to be safe.
    import bot.shared as shared
    saved = shared.ADMIN_IDS
    shared.ADMIN_IDS = set()
    try:
        asyncio.run(ld.cb_use_new(cb))
    finally:
        shared.ADMIN_IDS = saved

    row = db.execute("SELECT status FROM pending_location_decisions WHERE id=?",
                     (pld_id,)).fetchone()
    assert row["status"] == "pending"  # never changed
