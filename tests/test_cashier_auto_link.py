"""Auto-link path: cashier's fresh entry matches an agent's pending_handover
row → UPDATE in place instead of inserting a duplicate. Replaces the
deprecated queue-and-confirm UX. Defensive against the Murod-style
double-entry pattern (Error Log #60)."""
from backend.services import payment_intake


def _seed_client(conn, name="Test Client", phone="998900000001"):
    conn.execute(
        "INSERT INTO allowed_clients "
        "(phone_normalized, name, source_sheet, status, segment, client_id_1c) "
        "VALUES (?, ?, 'test', 'active', 'shop', ?)",
        (phone, name, name),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _seed_pending(conn, client_id, amount, currency, agent_tg=1001):
    """Mimic the agent mini-app submit: raw audit row + pending_handover."""
    raw_id = payment_intake.insert_intake_raw(
        conn,
        submitter_telegram_id=agent_tg,
        submitter_role="agent",
        payload={"channel": "cash_via_agent", "client_id": client_id,
                 "amount": amount, "currency": currency},
    )
    pid = payment_intake.create_intake_payment(
        conn,
        raw_id=raw_id,
        client_id=client_id,
        amount=amount,
        currency=currency,
        channel="cash_via_agent",
        status="pending_handover",
        submitter_telegram_id=agent_tg,
        submitter_role="agent",
        handover_agent_id=agent_tg,
    )
    return pid


def test_find_matching_pending_exact_match(db):
    cid = _seed_client(db)
    pid = _seed_pending(db, cid, 1_200_000.0, "UZS")
    db.commit()

    match = payment_intake.find_matching_pending(db, cid, 1_200_000.0, "UZS")
    assert match is not None
    assert match["id"] == pid
    assert match["channel"] == "cash_via_agent"


def test_find_matching_pending_amount_mismatch_returns_none(db):
    cid = _seed_client(db)
    _seed_pending(db, cid, 1_200_000.0, "UZS")
    db.commit()
    # Cashier's amount differs by 1 → no auto-link, fresh insert expected.
    assert payment_intake.find_matching_pending(db, cid, 1_200_001.0, "UZS") is None


def test_find_matching_pending_currency_mismatch_returns_none(db):
    cid = _seed_client(db)
    _seed_pending(db, cid, 100.0, "USD")
    db.commit()
    assert payment_intake.find_matching_pending(db, cid, 100.0, "UZS") is None


def test_find_matching_pending_fifo_on_ties(db):
    cid = _seed_client(db)
    older = _seed_pending(db, cid, 500_000.0, "UZS", agent_tg=1001)
    # Force a strictly later submitted_at so SQLite ORDER BY is deterministic.
    db.execute(
        "UPDATE intake_payments SET submitted_at = datetime('now', '-1 hour') WHERE id = ?",
        (older,),
    )
    newer = _seed_pending(db, cid, 500_000.0, "UZS", agent_tg=1002)
    db.commit()

    match = payment_intake.find_matching_pending(db, cid, 500_000.0, "UZS")
    assert match["id"] == older, f"FIFO expected (older={older}), got {match['id']} (newer={newer})"


def test_find_matching_pending_ignores_confirmed(db):
    cid = _seed_client(db)
    pid = _seed_pending(db, cid, 200_000.0, "UZS")
    payment_intake.confirm_payment(db, pid, confirmer_telegram_id=275116966)
    db.commit()
    # Already-confirmed row shouldn't be a candidate for auto-link.
    assert payment_intake.find_matching_pending(db, cid, 200_000.0, "UZS") is None


def test_find_matching_pending_outside_window_returns_none(db):
    cid = _seed_client(db)
    pid = _seed_pending(db, cid, 750_000.0, "UZS")
    db.execute(
        "UPDATE intake_payments SET submitted_at = datetime('now', '-48 hours') WHERE id = ?",
        (pid,),
    )
    db.commit()
    # Default window is 24h — 48h-old pending must not match.
    assert payment_intake.find_matching_pending(db, cid, 750_000.0, "UZS") is None


def test_link_pending_to_cashier_flips_status(db):
    cid = _seed_client(db)
    pid = _seed_pending(db, cid, 300_000.0, "UZS", agent_tg=1001)
    # Insert a separate cashier audit raw row (simulating _direct_finalize).
    cashier_raw = payment_intake.insert_intake_raw(
        db, submitter_telegram_id=275116966, submitter_role="cashier",
        payload={"links_pending": pid},
    )
    db.commit()

    row = payment_intake.link_pending_to_cashier(
        db, pending_payment_id=pid, cashier_telegram_id=275116966,
        audit_raw_id=cashier_raw,
    )
    assert row["status"] == "confirmed"
    assert row["confirmed_by_telegram_id"] == 275116966
    assert row["handover_agent_id"] == 1001  # preserved
    # Cashier raw should back-link to the same canonical payment_id.
    raw = db.execute(
        "SELECT processed_payment_id FROM payment_intake_raw WHERE id = ?",
        (cashier_raw,),
    ).fetchone()
    assert raw["processed_payment_id"] == pid
    assert "linked_by_cashier:275116966" in (row["notes"] or "")


def test_link_pending_to_cashier_race_raises(db):
    cid = _seed_client(db)
    pid = _seed_pending(db, cid, 400_000.0, "UZS")
    # Race: another flow flipped the status before we got here.
    payment_intake.confirm_payment(db, pid, confirmer_telegram_id=999)
    cashier_raw = payment_intake.insert_intake_raw(
        db, submitter_telegram_id=275116966, submitter_role="cashier",
        payload={"links_pending": pid},
    )
    db.commit()

    try:
        payment_intake.link_pending_to_cashier(
            db, pending_payment_id=pid, cashier_telegram_id=275116966,
            audit_raw_id=cashier_raw,
        )
    except ValueError as e:
        assert "status=" in str(e)
    else:
        raise AssertionError("expected ValueError on non-pending row")


def test_find_matching_pending_resolves_siblings(db):
    # Two phones, same client_id_1c — agent submits under one, cashier
    # records under the other. Sibling resolution must still match.
    phone_a = "998900000010"
    phone_b = "998900000011"
    name_1c = "Sibling Client"
    db.execute(
        "INSERT INTO allowed_clients "
        "(phone_normalized, name, source_sheet, status, segment, client_id_1c) "
        "VALUES (?, ?, 'test', 'active', 'shop', ?)",
        (phone_a, name_1c, name_1c),
    )
    a_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    db.execute(
        "INSERT INTO allowed_clients "
        "(phone_normalized, name, source_sheet, status, segment, client_id_1c) "
        "VALUES (?, ?, 'test', 'active', 'shop', ?)",
        (phone_b, name_1c, name_1c),
    )
    b_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

    pid = _seed_pending(db, a_id, 600_000.0, "UZS")
    db.commit()

    # Cashier searches under sibling phone b — must still find pending under a.
    match = payment_intake.find_matching_pending(db, b_id, 600_000.0, "UZS")
    assert match is not None
    assert match["id"] == pid
