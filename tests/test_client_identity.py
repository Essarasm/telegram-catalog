"""Unit tests for backend.services.client_identity.

Phase 3a (per Session F refactor plan): module exists alongside legacy
_try_match_client. No callers migrated yet — tests verify the new module's
behavior in isolation.
"""
from backend.services import client_identity as ci


# ── Atomic helpers ────────────────────────────────────────────────────────────


def test_is_excluded_recognizes_structural_placeholders():
    assert ci.is_excluded("Наличка №1") is True
    assert ci.is_excluded("Организации (переч.)") is True
    assert ci.is_excluded("СТРОЙКА") is True
    assert ci.is_excluded("ИСПРАВЛЕНИЕ") is True
    assert ci.is_excluded("В О З В Р А Т ПОСТАВЩИКУ") is True


def test_is_excluded_does_not_flag_real_clients():
    # СТЕКЛОПЛАСТИК was wrongly listed as pseudo-client until Apr 28 2026 —
    # this test guards against re-adding it.
    assert ci.is_excluded("СТЕКЛОПЛАСТИК") is False
    assert ci.is_excluded("Бахтиер /Хушвахт ТИТОВА/") is False
    assert ci.is_excluded("ШАРОФ ака Лоиш") is False


def test_is_excluded_handles_empty_input():
    assert ci.is_excluded(None) is False
    assert ci.is_excluded("") is False


def test_canonicalize_alias_returns_unchanged_when_map_empty():
    # ALIAS_MAP is empty until Layer 4 ships.
    assert ci.canonicalize_alias("ХУШВАХТ ТИТОВА") == "ХУШВАХТ ТИТОВА"
    assert ci.canonicalize_alias("any name") == "any name"


def test_canonicalize_alias_applies_map_when_populated(monkeypatch):
    monkeypatch.setitem(ci.ALIAS_MAP, "ХУШВАХТ ТИТОВА", "БАХТИЕР /ХУШВАХТ ТИТОВА/")
    try:
        assert ci.canonicalize_alias("ХУШВАХТ ТИТОВА") == "БАХТИЕР /ХУШВАХТ ТИТОВА/"
        assert ci.canonicalize_alias("not in map") == "not in map"
    finally:
        ci.ALIAS_MAP.pop("ХУШВАХТ ТИТОВА", None)


def test_match_by_client_id_1c_finds_registered_client(db):
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, status) "
        "VALUES (?, ?, ?, 'active')",
        ("123456789", "Test Client", "TEST CLIENT 1C", ),
    )
    db.commit()
    cid = ci.match_by_client_id_1c("TEST CLIENT 1C", db)
    assert cid is not None


def test_match_by_client_id_1c_skips_merged(db):
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, status) "
        "VALUES (?, ?, ?, 'merged')",
        ("123456789", "Test Client", "TEST CLIENT 1C"),
    )
    db.commit()
    cid = ci.match_by_client_id_1c("TEST CLIENT 1C", db)
    assert cid is None


def test_match_by_client_id_1c_picks_oldest_sibling(db):
    # Multi-phone client → multiple allowed_clients rows share one client_id_1c.
    # Resolver must pick the oldest (lowest id) for determinism.
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, status) "
        "VALUES (?, ?, ?, 'active')",
        ("100000001", "Client Phone1", "MULTI PHONE CLIENT"),
    )
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, status) "
        "VALUES (?, ?, ?, 'active')",
        ("100000002", "Client Phone2", "MULTI PHONE CLIENT"),
    )
    db.commit()
    cid = ci.match_by_client_id_1c("MULTI PHONE CLIENT", db)
    assert cid is not None
    # Verify it's the lower id (oldest sibling)
    other = db.execute(
        "SELECT MIN(id) FROM allowed_clients WHERE client_id_1c = ?",
        ("MULTI PHONE CLIENT",),
    ).fetchone()[0]
    assert cid == other


def test_match_by_name_fallback_lower_trim(db):
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, status) "
        "VALUES (?, ?, 'active')",
        ("999999999", "Иванов И.И."),
    )
    db.commit()
    # Mixed case + leading whitespace should still match
    cid = ci.match_by_name_fallback("  ИВАНОВ И.И.  ", db)
    assert cid is not None


# ── resolve_client_id pipeline ────────────────────────────────────────────────


def test_resolve_returns_unresolved_for_empty(db):
    assert ci.resolve_client_id(None, db) == ci.MatchResult(None, "unresolved")
    assert ci.resolve_client_id("", db) == ci.MatchResult(None, "unresolved")


def test_resolve_returns_excluded_for_pseudo_client(db):
    result = ci.resolve_client_id("Наличка №1", db)
    assert result.status == "excluded"
    assert result.client_id is None
    assert result.is_excluded


def test_resolve_returns_unresolved_when_name_not_in_allowed_clients(db):
    result = ci.resolve_client_id("СУХРОБ НАРИМАН", db)
    assert result.status == "unresolved"
    assert result.client_id is None
    assert result.is_unresolved


def test_resolve_matches_via_client_id_1c(db):
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, status) "
        "VALUES (?, ?, ?, 'active')",
        ("111111111", "Tester", "TEST 1C NAME"),
    )
    db.commit()
    result = ci.resolve_client_id("TEST 1C NAME", db)
    assert result.status == "matched"
    assert result.client_id is not None
    assert result.is_matched


def test_resolve_matches_via_name_fallback(db):
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, status) "
        "VALUES (?, ?, 'active')",
        ("222222222", "Fallback Tester"),
    )
    db.commit()
    # No client_id_1c set, so step 4 misses; step 5 picks it up
    result = ci.resolve_client_id("FALLBACK TESTER", db)
    assert result.status == "matched"
    assert result.client_id is not None


def test_resolve_applies_alias_before_match(db, monkeypatch):
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, status) "
        "VALUES (?, ?, ?, 'active')",
        ("333333333", "Real Client", "БАХТИЕР /ХУШВАХТ ТИТОВА/"),
    )
    db.commit()
    monkeypatch.setitem(ci.ALIAS_MAP, "ХУШВАХТ ТИТОВА", "БАХТИЕР /ХУШВАХТ ТИТОВА/")
    try:
        # Input is the shorthand left-name; alias maps to canonical; canonical matches.
        result = ci.resolve_client_id("ХУШВАХТ ТИТОВА", db)
        assert result.status == "matched"
        assert result.client_id is not None
    finally:
        ci.ALIAS_MAP.pop("ХУШВАХТ ТИТОВА", None)


def test_resolve_excluded_check_runs_before_alias(db):
    # Even if an alias would map an excluded name to a real one, exclusion wins.
    # (Defensive — current ALIAS_MAP doesn't contain pseudo-clients, but this
    # locks the order so future map edits can't accidentally re-include them.)
    result = ci.resolve_client_id("Наличка №1", db)
    assert result.status == "excluded"


# ── Heal ──────────────────────────────────────────────────────────────────────


def test_heal_finance_orphans_no_op_on_empty_table(db):
    healed = ci.heal_finance_orphans(db, "client_balances")
    assert healed == 0


def test_heal_finance_orphans_links_matching_orphans(db):
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, status) "
        "VALUES (?, ?, ?, 'active')",
        ("444444444", "Heal Tester", "HEAL TESTER 1C"),
    )
    # Insert orphan finance rows that should be heal-able
    db.execute(
        "INSERT INTO client_debts (client_name_1c, client_id, debt_uzs, debt_usd, report_date) "
        "VALUES (?, NULL, 1000, 0, '2026-04-27')",
        ("HEAL TESTER 1C",),
    )
    db.commit()
    healed = ci.heal_finance_orphans(db, "client_debts")
    assert healed == 1
    # Verify the row is now linked
    row = db.execute(
        "SELECT client_id FROM client_debts WHERE client_name_1c = ?",
        ("HEAL TESTER 1C",),
    ).fetchone()
    assert row[0] is not None


def test_heal_finance_orphans_does_not_touch_already_linked_rows(db):
    db.execute(
        "INSERT INTO allowed_clients (id, phone_normalized, name, client_id_1c, status) "
        "VALUES (?, ?, ?, ?, 'active')",
        (5000, "555555555", "Already Linked", "ALREADY LINKED 1C"),
    )
    # Insert a row with WRONG client_id (e.g. a typo) — heal should NOT overwrite
    db.execute(
        "INSERT INTO client_debts (client_name_1c, client_id, debt_uzs, debt_usd, report_date) "
        "VALUES (?, 9999, 1000, 0, '2026-04-27')",
        ("ALREADY LINKED 1C",),
    )
    db.commit()
    ci.heal_finance_orphans(db, "client_debts")
    # client_id should still be 9999 (the wrong-but-existing link was preserved)
    row = db.execute(
        "SELECT client_id FROM client_debts WHERE client_name_1c = ?",
        ("ALREADY LINKED 1C",),
    ).fetchone()
    assert row[0] == 9999


def test_heal_finance_orphans_does_not_link_excluded_names(db):
    # Insert orphan row for a structural placeholder. Heal must NOT link it.
    # (Note: heal SQL filters by `client_name_1c IN allowed_clients.client_id_1c`,
    # so if Наличка isn't in allowed_clients, it won't link by virtue of that.
    # This test makes the invariant explicit.)
    db.execute(
        "INSERT INTO client_debts (client_name_1c, client_id, debt_uzs, debt_usd, report_date) "
        "VALUES (?, NULL, 1000, 0, '2026-04-27')",
        ("Наличка №1",),
    )
    db.commit()
    ci.heal_finance_orphans(db, "client_debts")
    row = db.execute(
        "SELECT client_id FROM client_debts WHERE client_name_1c = ?",
        ("Наличка №1",),
    ).fetchone()
    assert row[0] is None


def test_heal_finance_orphans_rejects_non_finance_table(db):
    import pytest
    with pytest.raises(ValueError):
        ci.heal_finance_orphans(db, "products")


def test_heal_finance_orphans_alias_phase_links_left_name_to_canonical(db, monkeypatch):
    # Real client registered under canonical name
    db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, status) "
        "VALUES (?, ?, ?, 'active')",
        ("666666666", "Bakhtier", "БАХТИЕР /ХУШВАХТ ТИТОВА/"),
    )
    # Orphan finance row written under the shorthand left-name (Alisher's habit)
    db.execute(
        "INSERT INTO client_debts (client_name_1c, client_id, debt_uzs, debt_usd, report_date) "
        "VALUES (?, NULL, 1000, 0, '2026-04-27')",
        ("ХУШВАХТ ТИТОВА",),
    )
    db.commit()
    monkeypatch.setitem(ci.ALIAS_MAP, "ХУШВАХТ ТИТОВА", "БАХТИЕР /ХУШВАХТ ТИТОВА/")
    try:
        healed = ci.heal_finance_orphans(db, "client_debts")
        assert healed == 1
        row = db.execute(
            "SELECT client_id FROM client_debts WHERE client_name_1c = ?",
            ("ХУШВАХТ ТИТОВА",),
        ).fetchone()
        assert row[0] is not None
    finally:
        ci.ALIAS_MAP.pop("ХУШВАХТ ТИТОВА", None)


def test_heal_all_finance_tables_returns_per_table_counts(db):
    counts = ci.heal_all_finance_tables(db)
    assert set(counts.keys()) == {
        "client_balances", "client_payments", "real_orders", "client_debts",
    }
    assert all(v == 0 for v in counts.values())


# ── Mutator chokepoint ────────────────────────────────────────────────────────


def test_mutate_allowed_clients_then_heal_runs_mutation_and_heal_in_order(db):
    # The mutation ADDs an allowed_clients row. The heal then picks up an
    # orphan that was waiting for that row to exist. Verifies both steps fire
    # and produce the right outcome.
    db.execute(
        "INSERT INTO client_debts (client_name_1c, client_id, debt_uzs, debt_usd, report_date) "
        "VALUES (?, NULL, 5000, 0, '2026-04-27')",
        ("CHOKEPOINT TESTER",),
    )
    db.commit()

    def add_client(conn):
        conn.execute(
            "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, status) "
            "VALUES (?, ?, ?, 'active')",
            ("777777777", "Chokepoint Tester", "CHOKEPOINT TESTER"),
        )
        return {"added": 1}

    result = ci.mutate_allowed_clients_then_heal(db, add_client)
    assert result["mutate_result"] == {"added": 1}
    assert result["orphans_healed"]["client_debts"] == 1

    # Verify the orphan got linked to the just-inserted client
    row = db.execute(
        "SELECT client_id FROM client_debts WHERE client_name_1c = ?",
        ("CHOKEPOINT TESTER",),
    ).fetchone()
    assert row[0] is not None


def test_mutate_allowed_clients_then_heal_does_not_commit(db):
    # Caller is responsible for conn.commit() — verify wrapper leaves
    # transaction uncommitted so caller can roll back if needed.
    def add_client(conn):
        conn.execute(
            "INSERT INTO allowed_clients (phone_normalized, name, status) "
            "VALUES (?, ?, 'active')",
            ("888888888", "Pre-Commit Test"),
        )

    ci.mutate_allowed_clients_then_heal(db, add_client)
    # Without commit, a rollback should remove the change
    db.rollback()
    row = db.execute(
        "SELECT id FROM allowed_clients WHERE phone_normalized = ?",
        ("888888888",),
    ).fetchone()
    assert row is None
