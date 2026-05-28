"""Pin-conflict classifier — `evaluate_location_conflict` + `haversine_m`.

Origin: Session M 2026-05-28. Per-client location-decision queue replaces
the prior hard-block (driver_location.py) / silent-overwrite (location.py)
asymmetry with a unified verdict-based branch.
"""
from __future__ import annotations

import pytest

from backend.services.location_compare import (
    DEFAULT_THRESHOLD_M,
    evaluate_location_conflict,
    haversine_m,
)


SAM = (39.6537, 66.9758)  # Samarkand reference point


def test_haversine_self_is_zero():
    assert haversine_m(SAM[0], SAM[1], SAM[0], SAM[1]) == 0.0


def test_haversine_known_distance():
    # 1° latitude ≈ 111 km
    d = haversine_m(0.0, 0.0, 1.0, 0.0)
    assert 110_000 < d < 112_000


def test_haversine_symmetric():
    a = haversine_m(SAM[0], SAM[1], 39.68, 66.90)
    b = haversine_m(39.68, 66.90, SAM[0], SAM[1])
    assert abs(a - b) < 1e-6


# ── evaluate_location_conflict ───────────────────────────────────────


def test_first_write_when_no_prior():
    verdict, d = evaluate_location_conflict(None, None, SAM[0], SAM[1])
    assert verdict == "first_write"
    assert d is None


def test_first_write_when_prior_lat_missing():
    verdict, d = evaluate_location_conflict(None, 66.9758, SAM[0], SAM[1])
    assert verdict == "first_write"
    assert d is None


def test_within_threshold_byte_identical():
    """Bektimur #279 case — incoming = existing pin to 6dp."""
    verdict, d = evaluate_location_conflict(SAM[0], SAM[1], SAM[0], SAM[1])
    assert verdict == "within_threshold"
    assert d == 0.0


def test_within_threshold_small_drift():
    """Bektimur #280 case — ~5 m GPS jitter."""
    verdict, d = evaluate_location_conflict(
        39.717221, 66.754369, 39.717179, 66.75417,
    )
    assert verdict == "within_threshold"
    assert d < 20  # ~5 m actual


def test_within_threshold_at_50m():
    # ~50 m offset (0.00045° latitude ≈ 50 m)
    verdict, d = evaluate_location_conflict(SAM[0], SAM[1], SAM[0] + 0.00045, SAM[1])
    assert verdict == "within_threshold"
    assert 40 < d < 60


def test_dispatch_when_far():
    """Дилшод/Рамиз 2026-05-15 case — 3.57 km off."""
    verdict, d = evaluate_location_conflict(
        39.650126, 66.889776, 39.680813, 66.901943,
    )
    assert verdict == "dispatch_for_review"
    assert 3000 < d < 4000


def test_boundary_just_under_threshold():
    """Just inside the 100m threshold — still within."""
    # ~90 m
    verdict, _ = evaluate_location_conflict(SAM[0], SAM[1], SAM[0] + 0.00081, SAM[1])
    assert verdict == "within_threshold"


def test_boundary_just_over_threshold():
    """Just outside the 100m threshold — dispatch."""
    # ~120 m
    verdict, _ = evaluate_location_conflict(SAM[0], SAM[1], SAM[0] + 0.00108, SAM[1])
    assert verdict == "dispatch_for_review"


def test_custom_threshold_50m():
    """A caller can tighten or loosen the threshold."""
    # Same ~50 m offset, but now threshold is 25 m → dispatch.
    verdict, _ = evaluate_location_conflict(
        SAM[0], SAM[1], SAM[0] + 0.00045, SAM[1], threshold_m=25,
    )
    assert verdict == "dispatch_for_review"


def test_default_threshold_is_100():
    assert DEFAULT_THRESHOLD_M == 100.0
