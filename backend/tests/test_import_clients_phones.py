"""Tests for the multi-phone-cell handling in import_clients.

Covers the MULTI_PHONE_CELL_TRUNCATION fix: parse_phone_cell extracts each
phone from a 1C 'Телефоны контрагента' cell, and the upsert routes the
primary into phone_normalized + extras into raqam_02/raqam_03 (fill-only).
"""
import os
import sqlite3
import tempfile

import pytest

# Import lazily so DATABASE_PATH env-var redirection in conftest takes effect first.
from backend.services.import_clients import (
    parse_phone_cell,
    normalize_phone,
    is_valid_uz_mobile,
    UZ_MOBILE_OPERATOR_CODES,
    _upsert_client_from_row,
)


# ---------- parse_phone_cell ----------

@pytest.mark.parametrize("raw, expected_digits", [
    # The motivating case: husband-marked secondary, primary first.
    ("93 356 12 12, эри 97 918 33 33", ["933561212", "979183333"]),
    # Comma-separated, no annotations.
    ("99 062 76 56, 93 831 76 56", ["990627656", "938317656"]),
    # Country-code prefix on primary, plain secondary, hyphen-annotated tail.
    ("+998 90 605 79 36, 90 194 34 74 - Гульноза", ["906057936", "901943474"]),
    # Three phones, mixed prefixes + a stray "+" between digits.
    ("+998 97 396 26 00, 77031 1116+  88 392 01 01",
     ["973962600", "770311116", "883920101"]),
    # No comma — annotation 'Хабибулло' between two phones acts as separator.
    ("90 675 05 05 Хабибулло 88 665 05 05", ["906750505", "886650505"]),
    # Three phones with mixed separators (comma + semicolon).
    ("+998 93 350 00 30, +998 979181068 ;901970592",
     ["933500030", "979181068", "901970592"]),
    # Hyphen-tagged annotations after each phone.
    ("90 199 51 51 - дадаси, 97 892 87 77 - укаси", ["901995151", "978928777"]),
    # Cyrillic name immediately glued to a phone (no space) — regex still splits.
    ("+998 97 390 02 09Дилшод акаси, 979289978 Абдулатиф",
     ["973900209", "979289978"]),
    # Parens around an area code in the secondary.
    ("+998 90 656 74 74, (95) 500 38 89  982227474",
     ["906567474", "955003889", "982227474"]),
    # Single phone, no extras.
    ("97 918 33 33", ["979183333"]),
    # Dash-space between phones (no comma) — the load-bearing edge case that
    # caused the row-41334 false-positive insert in the 2026-05-29 backfill.
    # Two real phones: 93 333 80 70 + 97 931 04 04.
    ("93 333 8070- 97 931 04 04", ["933338070", "979310404"]),
    # Dashes inside a phone (no trailing space) must stay intra-phone.
    ("90-194-34-74", ["901943474"]),
    # Dash followed by an annotation (not a digit) — stays one phone.
    ("90 194 34 74 - Гульноза", ["901943474"]),
    # Second phone wrapped in parens — no comma between them.
    ("97 927 17 77 (99 548 47 67)", ["979271777", "995484767"]),
    # Same number twice glued together — dedupe keeps one.
    ("+998 99 455 00 57 994550057 Шарофиддин", ["994550057"]),
    # Two phones glued by a single space (no clean separator).
    ("99 165 73 80 952657380", ["991657380", "952657380"]),
    # Period as inter-phone separator.
    ("+99897-776-22-26 . 91 532 33 23", ["977762226", "915323323"]),
    # Empty / None.
    ("", []),
    (None, []),
    # Too-short — no phone-shaped run.
    ("123 45", []),
    # Duplicate phones in the same cell — only first occurrence kept.
    ("93 356 12 12, 93 356 12 12", ["933561212"]),
])
def test_parse_phone_cell(raw, expected_digits):
    result = parse_phone_cell(raw)
    assert [p["digits"] for p in result] == expected_digits


# ---------- MULTI_PHONE_CELL_MISALIGNMENT regression matrix ----------
# A 1C cell's concatenated digits aren't always a clean multiple of 9. Before
# the operator-prefix anchoring fix, _walk_digits_into_phones blindly sliced
# fixed 9-digit windows, so any leading stray digit rotated every slice into an
# invalid-prefix number — overwriting ~88 live primaries with garbage like
# 549009591 (2026-06). These cases lock in the realignment behaviour.

@pytest.mark.parametrize("raw, expected_digits", [
    # Stray leading digit before the real number — must re-anchor, not slice.
    ("5 915194019", ["915194019"]),
    # Leading zero (common 1C artefact).
    ("0 915194019", ["915194019"]),
    # Russian "8" trunk prefix glued to a mobile.
    ("8 915 194 019", ["915194019"]),
    # Multi-digit junk glued by a single space (one piece, misaligned).
    ("1234 915194019", ["915194019"]),
    # Two clean numbers glued with no separator — both valid, both kept.
    ("915194019901234567", ["915194019", "901234567"]),
    # Two numbers glued where the trailing slice is invalid — drop the garbage,
    # keep the valid primary (was: emitted 549009591 into raqam_02).
    ("915194019549009591", ["915194019"]),
    # 998 country code followed by a NON-operator pair → not stripped as a
    # country code; "99" is itself a valid operator, so it reads as a 99-number
    # rather than being dropped (sensible fallback on ambiguous input).
    ("998123456789", ["998123456"]),
])
def test_parse_phone_cell_misalignment(raw, expected_digits):
    result = parse_phone_cell(raw)
    assert [p["digits"] for p in result] == expected_digits


def test_misaligned_primary_is_recovered_even_with_mid_stray():
    """A stray digit *between* two phones is ambiguous and can't be perfectly
    unglued, but the PRIMARY must still recover correctly — that's the field
    that makes or breaks reachability. (The secondary is best-effort.)"""
    phones = parse_phone_cell("99 165 73 80 9 952657380")
    assert phones[0]["digits"] == "991657380"      # primary correct
    assert is_valid_uz_mobile(phones[0]["digits"])  # and not corrupt


def test_no_parsed_primary_is_ever_an_invalid_prefix():
    """Invariant: across the full existing corpus of cell shapes, the PRIMARY
    (phones[0]) always starts with a known operator code. A corrupt primary is
    the exact failure that made a client unreachable by their main number."""
    corpus = [
        "93 356 12 12, эри 97 918 33 33", "+998 90 605 79 36, 90 194 34 74",
        "5 915194019", "0 915194019", "8 915 194 019", "1234 915194019",
        "915194019549009591", "99 165 73 80 952657380",
        "+99897-776-22-26 . 91 532 33 23",
    ]
    for raw in corpus:
        phones = parse_phone_cell(raw)
        if phones:
            assert is_valid_uz_mobile(phones[0]["digits"]), (
                f"{raw!r} produced invalid primary {phones[0]['digits']!r}")


def test_is_valid_uz_mobile_helper():
    assert is_valid_uz_mobile("915194019")     # 91 operator
    assert not is_valid_uz_mobile("549009591")  # 54 not an operator
    assert not is_valid_uz_mobile("91519401")   # only 8 digits
    assert not is_valid_uz_mobile("")
    assert not is_valid_uz_mobile(None)
    assert "90" in UZ_MOBILE_OPERATOR_CODES and "54" not in UZ_MOBILE_OPERATOR_CODES


def test_parse_phone_cell_annotation_husband():
    """The Гулноза case: 'эри' precedes the husband's secondary phone."""
    result = parse_phone_cell("93 356 12 12, эри 97 918 33 33")
    assert result[0]["annotation"] == ""        # primary has no preceding text
    assert "эри" in result[1]["annotation"]      # husband marker captured


def test_parse_phone_cell_annotation_name():
    """A contact name in the same piece as a phone lands in that phone's annotation."""
    # "Дилшод акаси" sits in the same piece as 973900209 (both before the comma)
    # so it describes the PRIMARY, not the secondary.
    result = parse_phone_cell("+998 97 390 02 09 Дилшод акаси, 979289978 Абдулатиф")
    assert "Дилшод" in result[0]["annotation"]
    assert "Абдулатиф" in result[1]["annotation"]


def test_normalize_phone_backcompat():
    """normalize_phone still returns a single 9-digit primary for all callers."""
    # Single phone, formatted variations.
    assert normalize_phone("+998-90-123-45-67") == "901234567"
    assert normalize_phone("998 90 123 45 67") == "901234567"
    assert normalize_phone("901234567") == "901234567"
    # Multi-phone cell — primary wins (THIS is the bug being fixed: previously
    # the old normalize_phone returned the LAST phone's last 9 digits).
    assert normalize_phone("93 356 12 12, эри 97 918 33 33") == "933561212"
    # Empty / None.
    assert normalize_phone("") == ""
    assert normalize_phone(None) == ""


# ---------- _upsert_client_from_row ----------

@pytest.fixture
def conn():
    """In-memory SQLite with the minimum schema the upsert touches."""
    c = sqlite3.connect(":memory:")
    c.execute("""
        CREATE TABLE allowed_clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_normalized TEXT NOT NULL,
            name TEXT,
            location TEXT,
            source_sheet TEXT,
            client_id_1c TEXT,
            company_name TEXT,
            onec_card_id TEXT,
            status TEXT DEFAULT 'active',
            needs_review INTEGER DEFAULT 0,
            raqam_02 TEXT, ism_02 TEXT,
            raqam_03 TEXT, ism_03 TEXT,
            gps_latitude REAL, credit_score INTEGER, credit_limit REAL
        )
    """)
    # users + drift queue: the #74 guard's _curated_state() reads users and
    # the upsert writes held rows to client_identity_drift_queue.
    c.execute("CREATE TABLE users (telegram_id INTEGER PRIMARY KEY, client_id INTEGER)")
    c.execute("""
        CREATE TABLE client_identity_drift_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            allowed_client_id INTEGER NOT NULL,
            phone_normalized TEXT,
            existing_client_id_1c TEXT,
            incoming_client_id_1c TEXT,
            incoming_name TEXT,
            curated_state TEXT,
            matched_via TEXT,
            detected_at TEXT DEFAULT (datetime('now')),
            resolved INTEGER DEFAULT 0,
            resolution TEXT, resolved_at TEXT, resolved_by TEXT
        )
    """)
    c.execute("""
        CREATE TABLE phone_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            old_phone TEXT, new_phone TEXT,
            reason TEXT, changed_by TEXT,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # real_orders is only touched by _resolve_cid_1c_tiebreaker, which exits
    # safely when the table is empty.
    c.execute("CREATE TABLE real_orders (client_name_1c TEXT, doc_date TEXT)")
    yield c
    c.close()


def test_insert_multi_phone_cell_lands_extras(conn):
    """A fresh client with 2-phone cell → primary + raqam_02 + ism_02 written."""
    outcome, cid = _upsert_client_from_row(
        conn,
        raw_phone_str="93 356 12 12, эри 97 918 33 33",
        client_name="Гулноза ойти ТАЙЛОК/круг-2/",
        location="", source="clients_upload",
        cid_1c="Гулноза ойти ТАЙЛОК/круг-2/", company="",
        changed_by_tag="test",
    )
    assert outcome == "inserted"
    row = conn.execute(
        "SELECT phone_normalized, raqam_02, ism_02, raqam_03, ism_03 FROM allowed_clients WHERE id=?",
        (cid,),
    ).fetchone()
    assert row[0] == "933561212"
    assert row[1] == "979183333"
    assert row[2] == "эри"
    assert row[3] is None
    assert row[4] is None


def test_update_extras_are_fill_only(conn):
    """Existing raqam_02 must NOT be overwritten when the cell carries an extra."""
    # Pre-seed a row with an existing secondary slot.
    conn.execute(
        "INSERT INTO allowed_clients (id, phone_normalized, name, raqam_02, ism_02, client_id_1c) "
        "VALUES (1, '933561212', 'Гулноза', '999000000', 'old-secondary', 'Гулноза ойти')"
    )
    outcome, cid = _upsert_client_from_row(
        conn,
        raw_phone_str="93 356 12 12, эри 97 918 33 33",
        client_name="Гулноза", location="", source="clients_upload",
        cid_1c="Гулноза ойти", company="",
        changed_by_tag="test",
    )
    assert outcome == "updated"
    row = conn.execute(
        "SELECT raqam_02, ism_02 FROM allowed_clients WHERE id=?", (cid,)
    ).fetchone()
    # Fill-only: existing non-null secondary preserved.
    assert row[0] == "999000000"
    assert row[1] == "old-secondary"


def test_fallback_lookup_by_client_id_1c(conn):
    """When primary phone doesn't match an existing row but client_id_1c does,
    upsert hits the existing row instead of creating a duplicate."""
    # Pre-seed Гулноза with the post-manual-fix shape (primary moved).
    conn.execute(
        "INSERT INTO allowed_clients (id, phone_normalized, name, raqam_02, ism_02, client_id_1c) "
        "VALUES (40845, '933561212', 'Гулноза ойти ТАЙЛОК/круг-2/', '979183333', 'эри', "
        " 'Гулноза ойти ТАЙЛОК/круг-2/')"
    )
    # Now simulate /clients running with a hypothetical cell where parser produces
    # a DIFFERENT primary digit (here: we contrive a single-phone cell with a
    # number that doesn't match the existing primary but the 1C name does match).
    outcome, cid = _upsert_client_from_row(
        conn,
        raw_phone_str="91 000 00 00",  # phone not in DB
        client_name="Гулноза ойти ТАЙЛОК/круг-2/",
        location="", source="clients_upload",
        cid_1c="Гулноза ойти ТАЙЛОК/круг-2/", company="",
        changed_by_tag="test",
    )
    assert outcome == "updated"
    assert cid == 40845  # fallback found the existing row by client_id_1c
    # Primary swapped + phone_history logged.
    row = conn.execute(
        "SELECT phone_normalized FROM allowed_clients WHERE id=40845"
    ).fetchone()
    assert row[0] == "910000000"
    hist = conn.execute(
        "SELECT old_phone, new_phone FROM phone_history WHERE client_id=40845"
    ).fetchone()
    assert hist == ("933561212", "910000000")


def test_fallback_lookup_by_raqam_02(conn):
    """When the new primary matches an existing row's raqam_02, treat as same client."""
    conn.execute(
        "INSERT INTO allowed_clients (id, phone_normalized, raqam_02, name, client_id_1c) "
        "VALUES (1, '933561212', '979183333', 'X', 'X')"
    )
    outcome, cid = _upsert_client_from_row(
        conn,
        raw_phone_str="97 918 33 33",  # was secondary, now treated as primary
        client_name="X", location="", source="clients_upload",
        cid_1c="X", company="",
        changed_by_tag="test",
    )
    assert outcome == "updated"
    assert cid == 1


def test_cross_client_phone_collision_freezes_identity(conn):
    """Phone-match brings up an existing row whose cid_1c differs from incoming
    → identity (name/location/company) must NOT be overwritten. cid_1c gets the
    activity tiebreaker; needs_review flagged for human adjudication.

    Origin: the 2026-05-29 САРДОР/Мурод collision. Both 1C clients have a row
    whose primary phone normalizes to 933338070; pre-fix we silently
    overwrote one with the other on every /clients run."""
    conn.execute(
        "INSERT INTO allowed_clients (id, phone_normalized, name, client_id_1c) "
        "VALUES (957, '933338070', 'Мурод ака Вокзал', 'Мурод ака Вокзал')"
    )
    outcome, cid = _upsert_client_from_row(
        conn,
        raw_phone_str="93 333 80 70",   # matches Мурод's primary
        client_name="САРДОР Пищевой",     # but the incoming row is for a different client
        location="some-other-place", source="clients_upload",
        cid_1c="САРДОР Пищевой", company="",
        changed_by_tag="test",
    )
    assert outcome == "updated"
    assert cid == 957
    row = conn.execute(
        "SELECT name, location, client_id_1c, needs_review FROM allowed_clients WHERE id=957"
    ).fetchone()
    # Identity frozen — name/location keep the existing client's values.
    assert row[0] == "Мурод ака Вокзал"
    assert row[1] is None  # location not overwritten
    # cid_1c: existing has no real_orders activity in the test DB, so the
    # tiebreaker's default fallthrough returns the new value. The needs_review
    # flag is what catches operator attention regardless.
    assert row[3] == 1  # needs_review set


def test_duplicate_primary_in_same_upload_skipped_by_caller(conn):
    """Within one upload, the caller's seen-set dedup prevents reprocessing.
    (The upsert itself doesn't dedupe — that's the caller's job, mirroring
    the pre-fix behavior.)"""
    _upsert_client_from_row(
        conn, raw_phone_str="93 356 12 12", client_name="A",
        location="", source="", cid_1c="A", company="",
        changed_by_tag="test",
    )
    # Second call with same primary would update the existing row (not insert).
    outcome, _ = _upsert_client_from_row(
        conn, raw_phone_str="93 356 12 12", client_name="B",
        location="", source="", cid_1c="A", company="",
        changed_by_tag="test",
    )
    assert outcome == "updated"
    count = conn.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]
    assert count == 1
