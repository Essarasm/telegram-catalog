"""
Extract weight (in kg) from product name as a fallback
when the weight field in the database is missing or looks
like a rounded integer that contradicts the name.

Handles patterns like:
  "Грунтовка акриловая 0.75 кг"   → 0.75
  "Клей обойный /2,5кг/"          → 2.5
  "Краска ВД 10кг"                → 10.0
  "Лак ПФ 0,9л"                   → 0.9 (liters treated as kg-equivalent)
"""
import re

# Matches weight/volume numbers followed by unit, with or without /slashes/
_WEIGHT_RE = re.compile(
    r'(?:/\s*)?'                          # optional opening slash
    r'(\d+[,.]?\d*)'                      # capture: the number (e.g. 0.75 or 2,5 or 10)
    r'\s*'
    r'(кг|kg|гр|gr|г|мл|ml|л|l)'         # unit
    r'\.?'
    r'(?:\s*/)?',                          # optional closing slash
    re.IGNORECASE
)

# Conversion to kg
_TO_KG = {
    'кг': 1,    'kg': 1,
    'гр': 0.001, 'gr': 0.001, 'г': 0.001,
    'л': 1,     'l': 1,
    'мл': 0.001, 'ml': 0.001,
}


def parse_weight_from_name(name: str) -> float | None:
    """Return weight in kg parsed from product name, or None if not found."""
    if not name:
        return None

    m = _WEIGHT_RE.search(name)
    if not m:
        return None

    num_str = m.group(1).replace(',', '.')
    unit = m.group(2).lower()

    try:
        value = float(num_str)
    except ValueError:
        return None

    multiplier = _TO_KG.get(unit, 1)
    result = round(value * multiplier, 4)

    return result if result > 0 else None
