"""Producer → tier classification (Session T 3-tier compensation model).

Single source of truth used by:
  - backend/routers/agent.py /commission endpoint (live earned commission)
  - backend/services/points_simulation.py (loyalty-points multiplier)

When Session T's "Walk all 43 active suppliers + assign supplier→tier" TODO
lands, swap the inline sets for a DB lookup against `producers.commission_tier`.
"""

TIER_HIGH = "high"
TIER_STANDARD = "standard"
TIER_LOW = "low"

# Commission rate (decimal) per tier.
RATE = {
    TIER_HIGH: 0.02,      # 2.0%
    TIER_STANDARD: 0.01,  # 1.0%
    TIER_LOW: 0.005,      # 0.5%
}

# Loyalty-points multiplier per tier (consumed by points_simulation.py).
POINTS_MULTIPLIER = {
    TIER_HIGH: 2.0,
    TIER_STANDARD: 1.5,
    TIER_LOW: 1.0,
}

# Producer-name substrings (lowercase, contains-match) → tier.
_HIGH_MARGIN = {
    'palizh', 'нюмикс', 'weber', 'qorasaroy', 'silkoat',
    'юнитинт', 'oscar', 'dekoart', 'ofm', 'соудал', 'colormix',
    'палиж', 'вебер', 'оскар', 'декоарт', 'силкоат', 'коррасарой',
}
_LOW_MARGIN = {
    'hayat', 'eleron', 'узкабель', 'lama', 'kripteks',
    'хаят', 'элерон', 'lama standart',
}


def producer_tier(producer_name):
    """Return tier name for a producer string. None / unknown → standard."""
    if not producer_name:
        return TIER_STANDARD
    s = producer_name.strip().lower()
    for p in _HIGH_MARGIN:
        if p in s:
            return TIER_HIGH
    for p in _LOW_MARGIN:
        if p in s:
            return TIER_LOW
    return TIER_STANDARD


def commission_rate(producer_name):
    """Return decimal commission rate (0.005 / 0.01 / 0.02) for a producer."""
    return RATE[producer_tier(producer_name)]


def points_multiplier(producer_name):
    """Return point multiplier (1.0 / 1.5 / 2.0) for a producer."""
    return POINTS_MULTIPLIER[producer_tier(producer_name)]
