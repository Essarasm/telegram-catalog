"""Pin-conflict classifier for the per-client location-decision queue.

Both `bot/handlers/driver_location.py` (driver-group `/lokatsiya`) and
`bot/handlers/location.py` (Mini-App private DM) route their incoming
pins through `evaluate_location_conflict()` before any write to
`allowed_clients.gps_*`. The result determines whether to write directly,
silently ignore, or escalate to the agent-approval group for admin pick.

Origin: Session M 2026-05-28 — Bektimur's two blocked attempts surfaced
the asymmetry between today's HARD-BLOCK (driver group) and SILENT-
OVERWRITE (Mini App) paths. This module unifies both behind one rule:
within 100m → silent ignore; >100m → admin pick; first write → write.
"""
from __future__ import annotations

import math


DEFAULT_THRESHOLD_M = 100.0


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance between two (lat, lng) pairs, in metres."""
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def evaluate_location_conflict(
    prior_lat: float | None,
    prior_lng: float | None,
    new_lat: float,
    new_lng: float,
    threshold_m: float = DEFAULT_THRESHOLD_M,
) -> tuple[str, float | None]:
    """Classify an incoming pin against the existing one.

    Returns (verdict, distance_m) where verdict is one of:
      - 'first_write' — no prior pin; caller should UPDATE directly.
      - 'within_threshold' — within threshold of prior; caller should
        silent-ignore (no UPDATE, audit-only).
      - 'dispatch_for_review' — beyond threshold; caller should insert
        a pending_location_decisions row + dispatch comparison message.

    distance_m is None for 'first_write', else the haversine distance.
    """
    if prior_lat is None or prior_lng is None:
        return ("first_write", None)
    d = haversine_m(prior_lat, prior_lng, new_lat, new_lng)
    if d <= threshold_m:
        return ("within_threshold", d)
    return ("dispatch_for_review", d)
