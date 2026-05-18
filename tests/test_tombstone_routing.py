"""Regression tests for the tombstone-routing pattern (Error Log #56).

Background — 2026-05-15: the merge tool soft-deletes duplicate
allowed_clients rows by setting `status='merged_into:<canonical_id>'`.
The literal value is parameterized — a naïve `!= 'merged'` filter would
NOT exclude such rows (because `'merged_into:768' != 'merged'` is TRUE),
and tombstones would silently leak into reads.

For 3 days after the merge (2026-05-15→18), 41 callsites in the codebase
used `!= 'merged'` (exact equality). 1C importers wrote new finance
data onto tombstones; 198 clients showed 0 balance in Cabinet when the
filter was corrected. See Error Log #56 for the full incident.

These tests lock the post-fix behavior in. Each test sets up a tombstone
plus its canonical, then calls a representative code path, and asserts
the canonical is returned / acted on (and the tombstone is excluded).

If anyone re-introduces a `!= 'merged'` filter on `allowed_clients.status`
anywhere this code touches, one of these tests will fail.
"""
import pytest


# ── Fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
def tombstone_pair(db):
    """Set up a canonical + tombstone pair for the same client_id_1c.

    Returns (canonical_id, tombstone_id, client_id_1c). The tombstone's
    `status='merged_into:<canonical_id>'` matches the merge tool's format.
    """
    cid_1c = "ТЕСТ /Сифат Гагарин/"
    cur = db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, "
        "source_sheet, status) VALUES (?, ?, ?, ?, ?)",
        ("940474747", cid_1c, cid_1c, "clients_upload", "active")
    )
    canonical_id = cur.lastrowid
    cur = db.execute(
        "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, "
        "source_sheet, status) VALUES (?, ?, ?, ?, ?)",
        ("999999999", "Test stub", cid_1c, "Contacts",
         f"merged_into:{canonical_id}")
    )
    tombstone_id = cur.lastrowid
    db.commit()
    return canonical_id, tombstone_id, cid_1c


# ── get_sibling_client_ids: must exclude tombstones ─────────────────────


def test_get_sibling_client_ids_excludes_tombstones(tombstone_pair, db):
    """The canonical sibling helper (used by Cabinet/finance/etc. — 7 callers
    per memory project_users_client_id_blast_radius) must skip tombstones.

    Before Error Log #56's fix, `!= 'merged'` (exact equality) let
    'merged_into:N' tombstones through. After the fix, only the canonical
    is returned.
    """
    from backend.database import get_sibling_client_ids
    canonical_id, tombstone_id, _ = tombstone_pair

    ids = get_sibling_client_ids(db, canonical_id)
    assert canonical_id in ids
    assert tombstone_id not in ids, (
        "Tombstone leaked through get_sibling_client_ids — the read-side "
        "filter likely regressed to `!= 'merged'` (exact equality). "
        "Should be `NOT LIKE 'merged%'`. See Error Log #56."
    )


# ── match_by_client_id_1c: must return canonical, not tombstone ─────────


def test_match_by_client_id_1c_returns_canonical(tombstone_pair, db):
    """The canonical-by-name lookup in client_identity must skip tombstones.

    Used by every 1C importer's name-resolution path. Pre-fix, this could
    return the tombstone's id (writing new finance data onto it). Post-fix,
    only the canonical is returned.
    """
    from backend.services import client_identity
    canonical_id, tombstone_id, cid_1c = tombstone_pair

    resolved = client_identity.match_by_client_id_1c(cid_1c, db)
    assert resolved == canonical_id, (
        f"match_by_client_id_1c returned {resolved}, expected canonical "
        f"{canonical_id}. If it returned the tombstone ({tombstone_id}), "
        f"the filter regressed. See Error Log #56."
    )


# ── consistency_audit: must not re-flag tombstones as fuzzy-dups ────────


def test_consistency_audit_excludes_tombstones_from_fuzzy_dups(tombstone_pair, db):
    """The audit's fuzzy_client_1c_dups check counts active rows only.

    Pre-fix, the audit reported 329 already-merged clusters every night
    for 3 days as if they were still duplicates. Post-fix, only genuine
    active duplicates surface.

    The audit calls `backend.database.get_db()` internally — the `db`
    fixture has already pointed DATABASE_PATH at the temp file, so the
    audit's fresh connection sees our fixture data.
    """
    from backend.services import consistency_audit
    # Ensure fixture writes are flushed for the audit's fresh connection
    db.commit()

    result = consistency_audit.run_audit(fix=False)

    fuzzy = result.get("fuzzy_client_1c_dups")
    assert not fuzzy or fuzzy.get("count", 0) == 0, (
        f"Audit re-flagged the tombstoned pair as a duplicate. "
        f"fuzzy_client_1c_dups={fuzzy}. The audit's status filter "
        f"regressed. See Error Log #56."
    )


# ── heal_finance_orphans: must remap tombstone-pointer rows to canonical ─


def test_heal_finance_orphans_remaps_tombstone_pointers(tombstone_pair, db):
    """Phase 3 of heal_finance_orphans (added 2026-05-18 in commit 7425b3b)
    redirects rows whose client_id points at a tombstone to the canonical id
    parsed from the status suffix. This is the structural fix that
    auto-resolves the orphan accumulation if the pattern recurs.
    """
    from backend.services import client_identity
    canonical_id, tombstone_id, cid_1c = tombstone_pair

    # Plant a client_debts row pointing at the tombstone (simulating the
    # 2026-05-15→18 importer regression).
    db.execute(
        "INSERT INTO client_debts (client_name_1c, client_id, debt_uzs, "
        "debt_usd, report_date) VALUES (?, ?, ?, ?, ?)",
        (cid_1c, tombstone_id, 1000000, 500, "2026-05-16")
    )
    db.commit()

    # Pre-heal: 1 row points at the tombstone, 0 at the canonical
    pre_t = db.execute(
        "SELECT COUNT(*) FROM client_debts WHERE client_id = ?",
        (tombstone_id,)
    ).fetchone()[0]
    pre_c = db.execute(
        "SELECT COUNT(*) FROM client_debts WHERE client_id = ?",
        (canonical_id,)
    ).fetchone()[0]
    assert pre_t == 1
    assert pre_c == 0

    healed = client_identity.heal_finance_orphans(db, "client_debts")
    db.commit()

    # Post-heal: 0 rows on tombstone, 1 on canonical
    post_t = db.execute(
        "SELECT COUNT(*) FROM client_debts WHERE client_id = ?",
        (tombstone_id,)
    ).fetchone()[0]
    post_c = db.execute(
        "SELECT COUNT(*) FROM client_debts WHERE client_id = ?",
        (canonical_id,)
    ).fetchone()[0]
    assert healed >= 1, "heal_finance_orphans should have remapped 1+ rows"
    assert post_t == 0, (
        f"Tombstone-pointer row not remapped. heal Phase 3 may have "
        f"regressed. See Error Log #56."
    )
    assert post_c == 1


# ── format_audit_message: severity tiering + spike detection ────────────


def test_format_audit_message_tiers_findings_by_severity():
    """Large finding counts must render as 🔴 CRITICAL, not buried as 🔵 INFO.
    Signal-fatigue prevention — see Error Log #56."""
    from backend.services.consistency_audit import format_audit_message

    findings = {
        "fuzzy_client_1c_dups": {"count": 329, "sample": []},  # critical
        "stale_needs_review": {"count": 8},                     # warning
        "recent_phone_changes_7d": {"count": 3},                # info
    }
    msg = format_audit_message(findings)
    assert msg is not None
    # CRITICAL block must come before WARNING which must come before INFO
    crit_pos = msg.find("🔴")
    warn_pos = msg.find("🟡")
    info_pos = msg.find("🔵")
    assert crit_pos != -1, "No CRITICAL tier emoji rendered"
    assert warn_pos != -1, "No WARNING tier emoji rendered"
    assert info_pos != -1, "No INFO tier emoji rendered"
    assert crit_pos < warn_pos < info_pos, (
        "Severity tiers not rendered in CRITICAL → WARNING → INFO order"
    )
    # 329 should be CRITICAL, 8 should be WARNING, 3 should be INFO
    assert "🔴" in msg.split("dublikat")[0] or "dublikat" in msg.split("🔴")[1]


def test_format_audit_message_marks_spike_on_zero_to_many():
    """A finding that goes 0→329 in one day is the exact pattern Error
    Log #56 would have caught with diff-based detection. Must mark
    SPIKE."""
    from backend.services.consistency_audit import format_audit_message

    findings = {"fuzzy_client_1c_dups": {"count": 329, "sample": []}}
    prior = {"fuzzy_client_1c_dups": {"count": 0}}
    msg = format_audit_message(findings, prior_findings=prior)
    assert msg is not None
    assert "📈" in msg and "SPIKE" in msg, (
        "No SPIKE marker rendered despite 0→329 growth (Error Log #56 "
        "would have surfaced with this exact pattern)"
    )


def test_format_audit_message_no_spike_on_drift_in_large_baseline():
    """329→339 is operational drift in a large baseline (3% growth), not
    a spike. SPIKE should require either large absolute delta with
    significant relative growth, or growth from a clean baseline."""
    from backend.services.consistency_audit import format_audit_message

    findings = {"fuzzy_client_1c_dups": {"count": 339, "sample": []}}
    prior = {"fuzzy_client_1c_dups": {"count": 329}}
    msg = format_audit_message(findings, prior_findings=prior)
    assert msg is not None
    assert "SPIKE" not in msg, (
        "SPIKE fired on 329→339 (3% growth) — would create alert fatigue "
        "on large baselines. Threshold requires delta >= 10 AND >= 50% growth."
    )


def test_format_audit_message_no_spike_on_small_growth():
    """2→3 row growth is not a spike — would generate too much noise."""
    from backend.services.consistency_audit import format_audit_message

    findings = {"trend_currency_drift": {"count": 3, "sample": []}}
    prior = {"trend_currency_drift": {"count": 2}}
    msg = format_audit_message(findings, prior_findings=prior)
    assert msg is not None
    assert "SPIKE" not in msg, (
        "SPIKE marker fired on a 1-row growth — threshold should require "
        "delta >= 10 AND >=50% growth."
    )


def test_snapshot_round_trip(tmp_path, monkeypatch):
    """save_snapshot then load_prior_snapshot should round-trip the counts.
    Sanity check for the diff-detection plumbing."""
    from backend.services import consistency_audit

    snap_path = str(tmp_path / "audit_snapshot.json")
    monkeypatch.setattr(consistency_audit, "_snapshot_path", lambda: snap_path)

    findings = {
        "fuzzy_client_1c_dups": {"count": 5, "sample": [{"x": 1}]},
        "_table_sizes": {"clients": 1827},
    }
    consistency_audit.save_snapshot(findings)
    loaded = consistency_audit.load_prior_snapshot()
    assert loaded == {"fuzzy_client_1c_dups": {"count": 5}}, (
        "Snapshot did not round-trip the counts (or leaked _table_sizes / samples)"
    )
