"""Client Identity Anchoring — Phase 2a: resolve_client() pure resolver.

Verifies the resolve-or-hold precedence: onec_card_id → telegram_id →
client_phones → name (tiebreaker only), and the cardinal rule that only an
unmatched signal set yields `create` while signal conflicts yield `hold`.
"""
from backend.services.client_resolver import resolve_client, resolve_for_registration
from backend.services.phone_slots import get_client_phones


def _client(db, cid, name="C", card=None, phones=()):
    # Seed the authoritative slots (the resolver reads these) AND the
    # client_phones mirror (so get_client_phones-based assertions still hold).
    phones = list(phones)
    db.execute(
        "INSERT INTO allowed_clients (id, name, client_id_1c, onec_card_id, "
        "phone_normalized, raqam_02, raqam_03, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'active')",
        (cid, name, name, card,
         phones[0] if len(phones) > 0 else "",
         phones[1] if len(phones) > 1 else None,
         phones[2] if len(phones) > 2 else None),
    )
    for i, p in enumerate(phones):
        db.execute(
            "INSERT INTO client_phones (client_id, phone_normalized, is_primary, source) "
            "VALUES (?, ?, ?, 'test')",
            (cid, p, 1 if i == 0 else 0),
        )


# ── tier 1: onec_card_id (definitive) ────────────────────────────────────────

def test_card_id_definitive(db):
    _client(db, 1, card="Прочие:1056", phones=["901111111"])
    v = resolve_client(db, onec_card_id="Прочие:1056")
    assert v["action"] == "matched" and v["client_id"] == 1 and v["matched_via"] == "onec_card_id"


def test_card_precedence_over_phone(db):
    _client(db, 10, card="Прочие:300", phones=["901010101"])
    _client(db, 11, phones=["902020202"])
    v = resolve_client(db, onec_card_id="Прочие:300", phones=["901010101"])
    assert v["client_id"] == 10 and v["matched_via"] == "onec_card_id"


# ── tier 2: telegram_id (remembered identity) ────────────────────────────────

def test_telegram_link_remembered(db):
    _client(db, 2, phones=["902222222"])
    db.execute("INSERT INTO users (telegram_id, client_id) VALUES (555, 2)")
    v = resolve_client(db, telegram_id=555)
    assert v["action"] == "matched" and v["client_id"] == 2 and v["matched_via"] == "telegram_id"


def test_telegram_link_to_merged_ignored(db):
    db.execute("INSERT INTO allowed_clients (id, name, phone_normalized, status) "
               "VALUES (3, 'X', '903333333', 'merged_into:1')")
    db.execute("INSERT INTO users (telegram_id, client_id) VALUES (556, 3)")
    assert resolve_client(db, telegram_id=556)["action"] == "create"


# ── tier 3: client_phones ────────────────────────────────────────────────────

def test_single_phone_match(db):
    _client(db, 4, phones=["904444444"])
    v = resolve_client(db, phones=["904444444"])
    assert v["action"] == "matched" and v["client_id"] == 4 and v["matched_via"] == "phone"


def test_phone_input_is_normalized(db):
    _client(db, 4, phones=["904444444"])
    assert resolve_client(db, phones=["+998 90 444 44 44"])["client_id"] == 4


def test_phone_excludes_merged(db):
    db.execute("INSERT INTO allowed_clients (id, name, phone_normalized, status) "
               "VALUES (12, 'M', '901112222', 'merged_into:1')")
    db.execute("INSERT INTO client_phones (client_id, phone_normalized, is_primary, source) "
               "VALUES (12, '901112222', 1, 'test')")
    assert resolve_client(db, phones=["901112222"])["action"] == "create"


# ── conflict / ambiguity → hold ──────────────────────────────────────────────

def test_strong_signal_phone_conflict_holds(db):
    _client(db, 7, card="Прочие:200", phones=["907777777"])
    _client(db, 8, phones=["908888888"])
    v = resolve_client(db, onec_card_id="Прочие:200", phones=["908888888"])
    assert v["action"] == "hold" and 7 in v["candidates"] and 8 in v["candidates"]


def test_strong_signal_phone_agrees_matched(db):
    _client(db, 7, card="Прочие:200", phones=["907777777"])
    v = resolve_client(db, onec_card_id="Прочие:200", phones=["907777777"])
    assert v["action"] == "matched" and v["client_id"] == 7


def test_multi_phone_candidates_name_tiebreak(db):
    # Distinct primaries (the active-phone UNIQUE forbids a shared primary), but
    # both carry 905550000 as a secondary → two phone candidates for that number.
    _client(db, 5, name="ALPHA", phones=["905550001", "905550000"])
    _client(db, 6, name="BETA", phones=["905550002", "905550000"])
    v = resolve_client(db, phones=["905550000"], name="BETA")
    assert v["action"] == "matched" and v["client_id"] == 6 and v["matched_via"] == "phone+name"


def test_multi_phone_candidates_no_tiebreak_holds(db):
    _client(db, 5, name="ALPHA", phones=["905550001", "905550000"])
    _client(db, 6, name="BETA", phones=["905550002", "905550000"])
    v = resolve_client(db, phones=["905550000"], name="GAMMA")
    assert v["action"] == "hold" and set(v["candidates"]) == {5, 6}


# ── create (never match on name alone) ───────────────────────────────────────

def test_no_signal_creates(db):
    assert resolve_client(db, phones=["909990000"], name="NEWBIE")["action"] == "create"


def test_name_only_never_matches(db):
    _client(db, 9, name="Улугбек Ургут", phones=["901230000"])
    # same name, different phone not in client_phones → create, NOT a name match (#75)
    assert resolve_client(db, phones=["907654321"], name="Улугбек Ургут")["action"] == "create"


# ── resolve_for_registration (channel-B write path) ──────────────────────────

def test_reg_matched_existing_phone(db):
    _client(db, 20, name="SHOP", phones=["920000001"])
    res = resolve_for_registration(db, telegram_id=111, phone="920000001",
                                   name="SHOP", source="bot_new_client")
    assert res["action"] == "matched" and res["client_id"] == 20
    assert db.execute("SELECT matched_telegram_id FROM allowed_clients WHERE id=20"
                      ).fetchone()[0] == 111


def test_reg_creates_new_and_syncs_phones(db):
    res = resolve_for_registration(db, telegram_id=112, phone="920000002",
                                   name="NEW SHOP", source="bot_new_client")
    assert res["action"] == "created" and res["client_id"]
    cid = res["client_id"]
    row = db.execute("SELECT phone_normalized, matched_telegram_id, source_sheet "
                     "FROM allowed_clients WHERE id=?", (cid,)).fetchone()
    assert row[0] == "920000002" and row[1] == 112 and row[2] == "bot_new_client"
    assert [p["phone"] for p in get_client_phones(db, cid)] == ["920000002"]


def test_reg_hold_on_ambiguous_phone_no_insert(db):
    _client(db, 21, name="A", phones=["920000010", "920000099"])
    _client(db, 22, name="B", phones=["920000011", "920000099"])
    before = db.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]
    res = resolve_for_registration(db, telegram_id=113, phone="920000099",
                                   name="C", source="bot_approved")
    assert res["action"] == "hold" and res["client_id"] is None
    assert db.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0] == before
    assert db.execute("SELECT COUNT(*) FROM client_identity_drift_queue "
                      "WHERE curated_state LIKE 'resolve_hold%'").fetchone()[0] >= 1


def test_reg_link_by_1c_sets_cid(db):
    res = resolve_for_registration(db, telegram_id=114, phone="920000003",
                                   name="X", client_id_1c="МОЙ 1С", source="bot_linked")
    cid = res["client_id"]
    assert db.execute("SELECT client_id_1c FROM allowed_clients WHERE id=?",
                      (cid,)).fetchone()[0] == "МОЙ 1С"


# ── channel (D): agent self-registration routed through the resolver ─────────

def test_agent_register_creates_and_syncs(db):
    from backend.services.agent_register import register_new_shop
    res = register_new_shop(db, agent_tg_id=900, first_name="Ali", last_name="Valiev",
                            venue="Bozor", phone_raw="998 91 111 22 33", lat=39.6, lng=66.9)
    assert res["status"] == "created" and res["client_id"]
    assert [p["phone"] for p in get_client_phones(db, res["client_id"])] == ["911112233"]


def test_agent_register_links_existing(db):
    db.execute("INSERT INTO allowed_clients (id, name, phone_normalized, status) "
               "VALUES (50, 'Shop', '922223344', 'active')")
    db.execute("INSERT INTO client_phones (client_id, phone_normalized, is_primary, source) "
               "VALUES (50, '922223344', 1, 'test')")
    from backend.services.agent_register import register_new_shop
    res = register_new_shop(db, agent_tg_id=900, first_name="Ali", last_name="Valiev",
                            venue="Bozor", phone_raw="922223344", lat=39.6, lng=66.9)
    assert res["status"] == "linked_existing" and res["client_id"] == 50


def test_agent_register_holds_on_ambiguous(db):
    # Shared number 933339999 sits in raqam_02 of two different shops (distinct
    # primaries) → resolving it returns two candidates → hold.
    db.execute("INSERT INTO allowed_clients (id, name, phone_normalized, raqam_02, status) "
               "VALUES (51, 'A', '933330001', '933339999', 'active'), "
               "       (52, 'B', '933330002', '933339999', 'active')")
    before = db.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]
    from backend.services.agent_register import register_new_shop
    res = register_new_shop(db, agent_tg_id=900, first_name="Ali", last_name="Valiev",
                            venue="Bozor", phone_raw="933339999", lat=39.6, lng=66.9)
    assert res["status"] == "held"
    assert db.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0] == before
