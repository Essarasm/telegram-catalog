"""Tests for client_identity.auto_link_unbound_clients — the post-import
sweep that links unbound admin/agent/bot-created allowed_clients rows
(client_id_1c IS NULL) to 1C cards now visible in the finance tables."""
from backend.services import client_identity


def _seed_unbound(conn, name, source_sheet='admin_panel', phone=''):
    conn.execute(
        "INSERT INTO allowed_clients "
        "(phone_normalized, name, source_sheet, status, segment) "
        "VALUES (?, ?, ?, 'active', 'shop')",
        (phone, name, source_sheet),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _seed_balance(conn, client_name_1c):
    """Insert a minimal client_balances row to make the 1C name visible."""
    conn.execute(
        "INSERT INTO client_balances "
        "(client_name_1c, period_start, period_end, currency, "
        " opening_debit, opening_credit, period_debit, period_credit, "
        " closing_debit, closing_credit) "
        "VALUES (?, '2026-05-01', '2026-05-31', 'UZS', "
        " 0, 0, 0, 0, 0, 0)",
        (client_name_1c,),
    )


def test_exact_match_links(db):
    """Admin creates 'ОOO Тест Шоп'; same Cyrillic name appears in
    client_balances later. Sweep links them."""
    ac_id = _seed_unbound(db, 'ООО Тест Шоп')
    _seed_balance(db, 'ООО Тест Шоп')
    db.commit()

    result = client_identity.auto_link_unbound_clients(db)

    assert len(result['linked']) == 1
    assert result['linked'][0]['id'] == ac_id
    assert result['linked'][0]['client_id_1c'] == 'ООО Тест Шоп'
    row = db.execute(
        "SELECT client_id_1c, status FROM allowed_clients WHERE id = ?",
        (ac_id,),
    ).fetchone()
    assert row['client_id_1c'] == 'ООО Тест Шоп'
    assert row['status'] == 'active'  # status untouched


def test_cross_script_match_links(db):
    """Admin types 'OOO Test Shop' (Latin) — 1C card is Cyrillic
    'ООО Тест Шоп'. Normalizer transliterates and they collide."""
    ac_id = _seed_unbound(db, 'OOO Test Shop')
    _seed_balance(db, 'ООО Тест Шоп')
    db.commit()

    result = client_identity.auto_link_unbound_clients(db)

    assert len(result['linked']) == 1
    assert result['linked'][0]['id'] == ac_id


def test_no_match_leaves_unbound(db):
    _seed_unbound(db, 'ООО Несуществующий')
    _seed_balance(db, 'ООО Другой')
    db.commit()

    result = client_identity.auto_link_unbound_clients(db)

    assert result['linked'] == []
    assert result['total_unbound'] == 1


def test_short_name_skipped(db):
    """First-name-only rows (bot_approved with name='Ali') don't match."""
    ac_id = _seed_unbound(db, 'Ali', source_sheet='bot_approved')
    _seed_balance(db, 'Ali')
    db.commit()

    result = client_identity.auto_link_unbound_clients(db)

    assert result['linked'] == []
    assert result['low_quality_skipped'] == 1
    row = db.execute(
        "SELECT client_id_1c FROM allowed_clients WHERE id = ?",
        (ac_id,),
    ).fetchone()
    assert row['client_id_1c'] is None


def test_ambiguous_match_skipped(db):
    """Two 1C names normalize to the same form (case-flip) — sweep refuses
    to pick one. Mislink would corrupt users.client_id (read by 7 modules)."""
    _seed_unbound(db, 'ооо альфа')
    _seed_balance(db, 'ООО АЛЬФА')
    _seed_balance(db, 'ООО Альфа')  # different bytes, same normalized form
    db.commit()

    result = client_identity.auto_link_unbound_clients(db)

    assert result['linked'] == []
    assert result['ambiguous_skipped'] == 1


def test_already_owned_1c_skipped(db):
    """If another allowed_clients row already owns the target client_id_1c,
    skip to avoid creating a duplicate."""
    # Existing canonical row already owning the 1C card.
    db.execute(
        "INSERT INTO allowed_clients "
        "(phone_normalized, name, client_id_1c, source_sheet, status) "
        "VALUES ('998900000000', 'ООО Дубль', 'ООО Дубль', 'client_master', 'active')",
    )
    ac_id = _seed_unbound(db, 'ООО Дубль')
    _seed_balance(db, 'ООО Дубль')
    db.commit()

    result = client_identity.auto_link_unbound_clients(db)

    assert result['linked'] == []
    assert result['ambiguous_skipped'] == 1
    row = db.execute(
        "SELECT client_id_1c FROM allowed_clients WHERE id = ?",
        (ac_id,),
    ).fetchone()
    assert row['client_id_1c'] is None


def test_merged_status_excluded_from_pending(db):
    """Soft-merged rows must never be picked up by the sweep — they're
    tombstones for already-canonicalized clients."""
    db.execute(
        "INSERT INTO allowed_clients "
        "(phone_normalized, name, source_sheet, status) "
        "VALUES ('998900000001', 'ООО Сoft', 'admin_panel', 'merged_into:99')",
    )
    _seed_balance(db, 'ООО Сoft')
    db.commit()

    result = client_identity.auto_link_unbound_clients(db)

    assert result['total_unbound'] == 0
    assert result['linked'] == []


def test_agent_panel_source_is_swept(db):
    """The sweep heals the existing agent backlog, not just admin entries."""
    ac_id = _seed_unbound(
        db, 'ООО Агент Шоп', source_sheet='agent_panel',
        phone='998901112222',
    )
    _seed_balance(db, 'ООО Агент Шоп')
    db.commit()

    result = client_identity.auto_link_unbound_clients(db)

    assert len(result['linked']) == 1
    assert result['linked'][0]['source_sheet'] == 'agent_panel'
    assert result['linked'][0]['id'] == ac_id


def test_pseudo_clients_excluded_as_candidates(db):
    """Structural placeholders (СТРОЙКА, ИСПРАВЛЕНИЕ, etc.) must not be
    used as match targets. The is_excluded gate filters them out before
    the candidate index is built. Using a confirmed entry from
    pseudo_clients.SYSTEM_NON_CLIENT_NAMES."""
    _seed_unbound(db, 'СТРОЙКА', source_sheet='admin_panel')
    _seed_balance(db, 'СТРОЙКА')
    db.commit()

    result = client_identity.auto_link_unbound_clients(db)

    # The 1C-name 'СТРОЙКА' is pseudo, so the candidate index skips it.
    # Even if the pending row passes the quality gate (Cyrillic, len ≥ 4),
    # there's no candidate to match.
    assert result['linked'] == []
