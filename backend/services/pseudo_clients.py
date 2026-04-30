"""Centralized list of 1C pseudo-client names that should be excluded from
all client-level analytics.

These are NOT real clients — they're accounting buckets inside 1C for:
- Unassigned cash ("Наличка" — walk-in / unattributed sales)
- Bank-transfer settlements ("Организации (переч.)")
- Operational adjustments ("ИСПРАВЛЕНИЕ", "В О З В Р А Т ПОСТАВЩИКУ")
- Internal work-orders ("СТРОЙКА", "СТЕКЛОПЛАСТИК" — in-house projects)

Including these in per-client receivables or credit-scoring creates false
signals (Apr 2025 "Наличка №1" showed $147K debt that is actually just
unassigned cash waiting for client attribution).

Use the `is_pseudo_client()` helper for matching — it handles Cyrillic
normalization and whitespace variations.
"""
from __future__ import annotations
import re
import unicodedata


def _normalize(s: str) -> str:
    """Cyrillic-aware lowercase + NFC + whitespace collapse. Mirrors the
    normalizer in import_real_orders.py so lists stay interchangeable."""
    if not s:
        return ""
    s = unicodedata.normalize("NFC", str(s)).strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    return s


# Canonical list. Add new entries here when new pseudo-accounts surface
# in 1C reconciliation. Keep the original-case string; normalization is
# applied at match time.
SYSTEM_NON_CLIENT_NAMES = [
    "ИСПРАВЛЕНИЕ",
    "ИСПРАВЛЕНИЕ СКЛАД 2",
    "Наличка №1",
    "Наличка №2",
    "Наличка №3",
    "Наличка СКЛАД",
    "Наличка - Магазин",
    "Организации (переч.)",
    "СТРОЙКА",
    "В О З В Р А Т ПОСТАВЩИКУ",
    # Below: one-off non-clients confirmed in finances_client_merge_map.md
    # (validated personally with Ulugbek, 2026-04-22 loyalty rebuild work).
    # Promoting from memory-only knowledge to code-enforced exclusion so
    # is_pseudo_client() and client_identity.is_excluded() agree.
    "ПАРВИЗ SILKCOAT ФИРМЕННЫЙ МАГАЗИН",  # closed client
    "ЖАМШЕД УРГУТ",                         # single $600 receipt, no pattern
    "ДИЛДОРА МАХМУДОВА СУПЕР",              # 4 duplicate receipts 2025-01-04
    "САМАВТО",                              # vendor/internal, not customer
    "1",                                    # single-char data-entry error

    # 2026-04-30 Group 2 review — uncle classified 51 names as non-clients
    # across four sub-patterns documented in memory:
    #   finances_1c_return_markers.md
    #   finances_1c_supplier_bonus_pattern.md
    #   finances_1c_card_rename_pattern.md (KRIPTEKS aliases)
    # Source doc: Uncle/Группа2_проверка_RU.docx (also EN + UZ).

    # Structural — retail-customer returns. Pairs with В О З В Р А Т ПОСТАВЩИКУ
    # above, which covers the supplier-side direction.
    "В О З В Р А Т",

    # 1C placeholders / defunct cards — uncle marked "null it":
    "<...>",                                # literal placeholder garbage
    "DEKS",                                 # defunct; the real account is "DEKS - БОНУС"

    # Supplier-bonus accumulator accounts (booked when suppliers grant
    # rebates per purchase; identified by "БОНУС" / "BONUS" in the name):
    "DEKS - БОНУС",
    "СОМОФИКС БОНУС",

    # Products / brands / materials / suppliers — 1C ledger accounts that
    # appear as `client_name_1c` in oborotnaya but are not actual customers:
    "DELUX Самандар ака",
    "EAST COLOR /BUILD TECHNO TRADE/",
    "GAMMA COLOR SERVICE",
    "GOOGLE",
    "LAMA STANDART",
    "PAINTERA",
    "R O Y A L",
    "SILKCOAT PAINT",
    "SIMPLEX BIZNES",
    "ZIP КОЛЛЕР",
    "АКФИКС",
    "ДЕКОАРТ",
    "КАРБИД",
    "ЛИНОЛЕУМ САНФА",
    "ЛОПАТКИ /РАЗНЫЕ/",
    "ПРОЧИЕ",
    "Растворитель",
    "СЕНТИФОН",
    "СОУДАЛ /ПОЛИСАН/",
    "УЗКАБЕЛЬ",
    "ШЛИФ ШКУРКА",
    "ЭЛЕКТРОД",
    "ЭМАЛЬ НЦ-132П",
    "Ташкент Трубный з-д",                  # supplier (pipe factory)
    "САМОРЕЗ  OFM",
    "ШЛАНГ ПОЛИВНОЙ",
    "KRIPTEKS - METAL",                     # same 1C card as ГВОЗДИ /KRIPTEKS-METAL/ — Alisher renamed
    "ЭКОС /КораСарой/",
    "MASHXAD",
    "PUFA MIX",
    "WEBER",
    "ДЕКОПЛАСТ",
    "НАЦИОНАЛ КЕРАМИК",
    "НОРА ойти",
    "НЮМИКС",
    "ПалИЖ КОЛЛЕР",
    "СОБСАН",
    "СОМО FIX",
    "ЦЕМЕНТ",
    "ЭЛЕРОН ЭЛИТ СЕРВИС",
    "FUBER",
    "ГВОЗДИ /KRIPTEKS-METAL/",              # alias of KRIPTEKS - METAL above
    "КораСарой/ЭКОС/",
    "Саморез TAGERT",
    "СП ООО \"RANGLI B O' Y O Q\"",
    "RANGLI BO'YOQ",
]

_NORMALIZED = frozenset(_normalize(s) for s in SYSTEM_NON_CLIENT_NAMES)


def is_pseudo_client(name: str) -> bool:
    """True if the given 1C client name is a known pseudo-account that should
    be excluded from per-client analytics."""
    if not name:
        return False
    return _normalize(name) in _NORMALIZED


def sql_exclusion_clause(column_name: str = "client_name_1c") -> str:
    """Return a SQL fragment that excludes pseudo-clients.

    Use like: `WHERE client_id IS NOT NULL AND {clause}`.
    Returns: `column NOT IN ('...', '...', ...)`
    """
    placeholders = ",".join("?" for _ in SYSTEM_NON_CLIENT_NAMES)
    return f"{column_name} NOT IN ({placeholders})"


def sql_exclusion_params() -> tuple:
    """Tuple of param values for the exclusion clause."""
    return tuple(SYSTEM_NON_CLIENT_NAMES)
