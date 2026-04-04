"""
Extract weight (in kg) from product name as a fallback
when the weight field in the database is missing or looks
like a rounded integer that contradicts the name.

Handles patterns like:
  "Грунтовка акриловая 0.75 кг"   → 0.75
  "Клей обойный /2,5кг/"          → 2.5
  "Краска ВД 10кг"                → 10.0
  "Лак ПФ 0,9л"                   → 0.9 (liters treated as kg-equivalent)

Does NOT match model numbers like ПФ-181 or 8110.
"""
import re

# Two-char+ units are safe (кг, гр, мл, kg, gr, ml) — no ambiguity
# Single-char units (г, л, l) need a word boundary after them
# to avoid matching first letters of words like "Голуб", "ГИДРОИЗОЛЯЦИЯ"
#
# The number must be preceded by /, whitespace, or start-of-string
# (not a hyphen or letter — avoids model numbers like ПФ-181)
_WEIGHT_RE = re.compile(
    r'(?:^|(?<=[/\s(]))'                     # number preceded by / or whitespace or ( or start
    r'(\d+[,.]?\d*)'                          # capture: the number
    r'\s*'
    r'(кг|kg|гр|gr|мл|ml'                    # multi-char units (safe)
    r'|г(?=[.\s/)\],;!?\-]|$)'              # г only if followed by boundary
    r'|л(?=[.\s/)\],;!?\-]|$)'              # л only if followed by boundary
    r'|l(?=[.\s/)\],;!?\-]|$)'              # l only if followed by boundary
    r')'
    r'\.?'
    r'(?:\s*/)?',                              # optional closing slash
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
