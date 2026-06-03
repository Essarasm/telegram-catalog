"""Definitive, human-confirmed client-identity decisions — hardcoded so the
consistency audit and the `recurring-family-investigation` skill never
re-litigate them.

Origin (2026-06-01): the daily Data Consistency Audit kept red-flagging
`client_id_1c dublikat` clusters every morning. A full investigation +
field review by Alisher (the bookkeeper) and Ulugbek established the
**definitive principle** behind almost all of them:

    `allowed_clients.client_id_1c` is a 1C *name label*, NOT a unique
    client key. Multiple genuinely-DIFFERENT shops legitimately share the
    same name string — Alisher, verbatim: "two shops literally next to
    each other" (e.g. ШАВКАТ vs АЗИЗ ТИТОВА, both `/НАПРОТИВ ТЕХНОГАЗ ОИЛ/`;
    several distinct "Улугбек Ургут" shopkeepers in different mahallas).

Therefore a same-`client_id_1c` cluster is only an *actual* duplicate that
needs merging when ≥2 sibling rows **share a phone number** (the real
signature of one person with two rows). Different-phone clusters are
legitimate and must never be flagged again.

This module is the single source of truth for those decisions. It is read
by:
  - `consistency_audit.fuzzy_client_1c_dups` — to exclude confirmed-distinct
    names from the daily audit (belt-and-suspenders on top of the phone
    heuristic).
  - `.claude/skills/recurring-family-investigation.md` — cites this file so
    any future trigger surfaces the answers immediately instead of
    re-investigating.

Pattern tag: `CLIENT_ID_1C_NONUNIQUE_NAME_COLLISION` (Error Log #75).
Do NOT add any of CONFIRMED_NOT_RENAMES to `client_identity.ALIAS_MAP` —
aliasing them would fuse two different shops' finances.
"""
from __future__ import annotations


def normalize_1c(name: str) -> str:
    """Canonical form for client_id_1c comparison: lowercase (Cyrillic-aware,
    unlike SQLite's ASCII-only LOWER) + collapse all internal/edge
    whitespace. Matches the normalization used by the audit's clustering so
    registry lookups line up with cluster keys."""
    if not name:
        return ""
    return " ".join(str(name).lower().split())


# ── Confirmed: ONE name, MULTIPLE genuinely-different shops ──────────────────
# Alisher + Ulugbek, 2026-06-01 (Client_Card_Check doc, Part B). These are
# legitimate name-collisions, NOT duplicates. The audit must never flag them.
_DISTINCT_SHARED_NAMES_RAW = [
    # NB: "АБДУЛЛО ЯНГИ-АРИК /ЯНГИ ЗАПЧ. БОЗОР/" (ids 23,24) was here — the
    # 2026-06-01 verdict said two shops, but it was made on CORRUPTED phone data
    # (the form showed Card 2 as 549009591, the MULTI_PHONE_CELL_MISALIGNMENT
    # garbage; real # is 915194019). Re-reviewed 2026-06-03 on corrected data
    # (both Telegram accounts = "ALISTROY 313") → confirmed ONE shop. Moved to
    # CONFIRMED_SAME_SHOP below (Error Log #81).
    # NB: "Мурод ака Вокзал" is deliberately NOT here. Its apparent 3rd row
    # (id 1302) was a #74 phone-upsert DRIFT artifact — 1302 is really
    # "САРДОР Пищевой" (reverted 2026-06-01). The live cluster is {957, 41090}
    # where 41090 is a hollow dup of 957 → a genuine dup the audit SHOULD flag
    # until 41090 is merged. See Error Log #74.
]
CONFIRMED_DISTINCT_SHARED_NAMES = {
    normalize_1c(n) for n in _DISTINCT_SHARED_NAMES_RAW
}


# ── Confirmed: SAME shop ────────────────────────────────────────────────────
# Alisher 2026-06-01 (Part B item 14): one shop, orders split across two rows
# (id 1151 = 1 order, id 40902 = 16 orders / 215M UZS). MERGE PENDING: the
# importer phone-upsert drift-guard (Error Log #74) is not yet in prod, and #74
# rule (b) says don't run merge_duplicate_1c_clients.py before that guard
# lands. Tracked in Session F Active TODOs.
# ── 2026-06-03 batch (Error Log #81): the MULTI_PHONE_CELL_MISALIGNMENT parser
# bug corrupted one row's primary phone in each of these clusters, so a later
# import couldn't phone-match and INSERTed a hollow duplicate. Ulugbek reviewed
# all 20 on corrected data (Client_Card_Check Part 2, 2026-06-03) → ALL one shop.
# Merge each (keep BOTH numbers); the corrupted/garbage number is dropped.
# Format: normalize_1c(name): "ids A+B — one shop; keep both real numbers".
_SAME_SHOP_2026_06_03 = {
    "АБДУЛЛО ЯНГИ-АРИК /ЯНГИ ЗАПЧ. БОЗОР/": "ids 23+24 — keep 915194019, 915490095 (supersedes 06-01 two-shops verdict)",
    "БЕКНАЗАР ЖАМБАЙ /Аль-Бухорий/": "ids 328+330 — keep 993039487, 997149487, 773108794",
    "БОБУР Жомбой /Кулбости МФЙ/": "ids 352+353 — keep 971573752, 995913752",
    "БОЙНАЗАР ака Нурабад": "ids 359+360 — keep 933595851, 973953069",
    "ДОСТОН ГУС УРГУТ": "ids 535+41038 — keep 942830030, 906550373 (corrupt row 535 is lower id)",
    "ЖАМШЕД АЛИКУЛОВ ПАЯРИК": "ids 577+578 — keep 996628833, 997545915",
    "Ислом ака Паярик/ Нариман": "ids 786+787 — keep 901591919, 941853636",
    "КОСИМ ОКДАРЁ": "ids 844+845 — keep 981906655, 997706655",
    "Максуд Наримон": "ids 870+871 — keep 886155678, 933550002",
    "МУРОД ака Супер": "ids 963+964 — keep 915350660, 982732224",
    "МУСОБЕК /БиоНУР/": "ids 978+979 — keep 906027773, 944780202",
    "МУХИДДИН АКА  /Жамбай - Охунбобоев КАНГЛИ мах./": "ids 991+992 — keep 938365515, 994503350",
    "МУХТОР АКА КУШРАБАД /ЗАРМИТАН/": "ids 1006+1007 — keep 937226669, 991048080",
    "НУРАЛИ  /Метан/": "ids 1053+1054 — keep 932208459, 942577091",
    "Сиддик ака СУПЕР": "ids 1310+1311 — keep 337075727, 937290853",
    "Фарход Булунгур /Фозил Юлдош/": "ids 1588+1589 — keep 915487774, 994227478",
    "Хуснидин Рахмонов ЖОМБОЙ": "ids 1750+1751 — keep 933491449, 997525894",
    "Шахзод Гофуров Паярик/9-совхоз/": "ids 1797+1798 — keep 931759986, 990930896",
    "Шухрат Туронов /Жомбой-ГАЗРА/": "ids 1935+1936 — keep 883920489, 888937701",
    "ЭЛЬЕР НАРИМАН": "ids 1957+1958 — keep 889142121, 945352424",
}
CONFIRMED_SAME_SHOP = {
    normalize_1c("Фуркат Галлаорол"): "ids 1151+40902 — one shop; merge pending #74 importer guard",
    **{normalize_1c(k): v for k, v in _SAME_SHOP_2026_06_03.items()},
}


# ── Confirmed: dormant pins that are NOT renames ────────────────────────────
# Alisher 2026-06-01 (Client_Card_Check Part A). For each, the "candidate
# new name" we guessed is a DIFFERENT shop (the old shop went dormant/closed;
# a different neighbour is the active trader). Keyed by allowed_clients.id.
# These must NOT be added to ALIAS_MAP and must NOT be re-proposed as rename
# candidates by any future dormant-pin investigation.
CONFIRMED_NOT_RENAMES = {
    146:  ("АКМАЛ ТИТОВА /МАГ TEPLOVIK/",        "≠ ШУХРАТ ТИТОВА — separate shop (dense ТИТОВА market)"),
    432:  ("ГОЛИБ КИРПИЧКА",                      "≠ Бобур /КИРПИЧКА/ — separate shop (dense КИРПИЧКА market)"),
    1769: ("ШАВКАТ ТИТОВА /НАПРОТИВ ТЕХНОГАЗ ОИЛ/", "≠ АЗИЗ ТИТОВА — literally two shops next to each other"),
    469:  ("Дамир Трикотажка",                    "≠ Азим Трикотажка — separate shop (old one dormant)"),
    1577: ("Фаррух Шурбоича /Бетонка/",           "≠ СУРЪАТ Шурбоича — separate shop"),
    1620: ("ФЕРУЗ /Маданият ТАЙЛОК/",             "≠ НОДИР /Тайлок Маданият/ — separate shop"),
    673:  ("Зафар АКА Автовокзал",                "≠ Азам Автовокзал — separate shop"),
    150:  ("АКОБИР ГОР ГАЗ /Маг Сан Техника/",    "≠ АКМАЛ ГОРГАЗ — separate shop (both active 2025)"),
    1924: ("ШУХРАТ ГАГАРИНА /МАГ МЕБ. ЖИХОЗ/",    "≠ ЖАМШЕД /СИФАТ ГАГАРИН/ — separate shop"),
    1220: ("РУСТАМ ТАНКОВЫЙ /АКФА ЦЕХ/",          "≠ АБДУСАЛОМ AKFA ЦЕХ — separate shop (Latin AKFA, overlapping dates)"),
    1944: ("Элдор Куйи Туркман к-к",              "legit LAPSED client (Alisher 2026-06-01), not a rename or closure: last "
                                                  "order 2024-10-18 — predates the real_orders window (starts 2025-01-03), so "
                                                  "he probes as 0-orders. Phone …3029 verified. Pin valid; leave as-is."),
}

# All 11 dormant-pin candidates are now RESOLVED (Alisher 2026-06-01).
# CAVEAT worth remembering: real_orders coverage starts 2025-01-03, so any
# client whose last order was in 2024 (like id 1944) shows 0 orders and looks
# "dormant" purely as a data-window artifact — check the order date before
# concluding dormancy/closure.
UNRESOLVED_DORMANT: dict = {}


def is_confirmed_distinct(client_id_1c: str) -> bool:
    """True if this name is a confirmed legitimate multi-shop name-collision
    (NOT a duplicate). Used by the audit to suppress it permanently."""
    return normalize_1c(client_id_1c) in CONFIRMED_DISTINCT_SHARED_NAMES


def is_confirmed_not_rename(allowed_client_id: int) -> bool:
    """True if this dormant-pin row was reviewed and confirmed NOT a rename."""
    return allowed_client_id in CONFIRMED_NOT_RENAMES
