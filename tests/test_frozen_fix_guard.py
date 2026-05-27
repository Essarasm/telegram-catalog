"""Frozen-GPS guard — `_frozen_fix_prior` detects a stale/cached fix being
re-sent across consecutive clients.

Origin: 2026-05-01 Juma incident (Error Log #68). Agent Musobek's phone
returned the same frozen coordinate (39.684788, 66.898633) for ШУХРАТ, then
Санжар, then ХУРШИД within 35 minutes — silently planting one wrong pin on
three shops. The guard holds such a pin for confirmation instead of writing
it. See bot/handlers/location.py.
"""
from __future__ import annotations

FROZEN = (39.684788, 66.898633)


def _ins(db, *, rid, tg, lat, lng, client_id, age_min=1):
    db.execute(
        "INSERT INTO location_attempts (id, telegram_id, latitude, longitude, "
        "linked_client_id, received_at) VALUES (?,?,?,?,?, datetime('now', ?))",
        (rid, tg, lat, lng, client_id, f"-{age_min} minutes"),
    )
    db.commit()


def test_flags_same_coord_different_client(db):
    """The actual incident: same frozen coord, next client → flagged."""
    from bot.handlers.location import _frozen_fix_prior
    _ins(db, rid=1, tg=810, lat=FROZEN[0], lng=FROZEN[1], client_id=1917)  # ШУХРАТ
    hit = _frozen_fix_prior(db, 810, before_audit_id=2, client_id=1731,    # ХУРШИД
                            lat=FROZEN[0], lng=FROZEN[1])
    assert hit is not None
    assert hit["linked_client_id"] == 1917


def test_allows_same_client_repin(db):
    """Same agent correcting their OWN pin at the same spot must pass."""
    from bot.handlers.location import _frozen_fix_prior
    _ins(db, rid=1, tg=810, lat=FROZEN[0], lng=FROZEN[1], client_id=1917)
    assert _frozen_fix_prior(db, 810, 2, 1917, FROZEN[0], FROZEN[1]) is None


def test_allows_different_coord(db):
    """A genuine fresh fix (different coords) for the next client passes."""
    from bot.handlers.location import _frozen_fix_prior
    _ins(db, rid=1, tg=810, lat=39.70, lng=66.65, client_id=1917)
    assert _frozen_fix_prior(db, 810, 2, 1731, FROZEN[0], FROZEN[1]) is None


def test_first_pin_has_no_prior(db):
    from bot.handlers.location import _frozen_fix_prior
    assert _frozen_fix_prior(db, 810, 2, 1731, FROZEN[0], FROZEN[1]) is None


def test_ignores_prior_beyond_2h_window(db):
    """An exact-coord match from yesterday is coincidence, not a frozen fix."""
    from bot.handlers.location import _frozen_fix_prior
    _ins(db, rid=1, tg=810, lat=FROZEN[0], lng=FROZEN[1], client_id=1917, age_min=180)
    assert _frozen_fix_prior(db, 810, 2, 1731, FROZEN[0], FROZEN[1]) is None


def test_other_agents_prior_ignored(db):
    """The guard compares against the SAME agent's stream only."""
    from bot.handlers.location import _frozen_fix_prior
    _ins(db, rid=1, tg=999, lat=FROZEN[0], lng=FROZEN[1], client_id=1917)
    assert _frozen_fix_prior(db, 810, 2, 1731, FROZEN[0], FROZEN[1]) is None


def test_zero_audit_id_short_circuits(db):
    from bot.handlers.location import _frozen_fix_prior
    _ins(db, rid=1, tg=810, lat=FROZEN[0], lng=FROZEN[1], client_id=1917)
    assert _frozen_fix_prior(db, 810, 0, 1731, FROZEN[0], FROZEN[1]) is None
