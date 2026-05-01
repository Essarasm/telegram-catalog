"""Seed procurement_categories + suppliers + supplier_categories.

Source of truth for the Stage 1 dropdown (agent picks category) and the
Stage 2 picker (uncle picks supplier within the category) in Option 3 of
the Cashbook payment intake flow (legal-entity bank transfer).

Categories locked 2026-05-01 from Uncle/Suppliers_Master.xlsx column F
("For Mini App"). 13 categories: 12 product-typed + 1 free-text "Boshqa".

Suppliers seeded from the same Excel: name_1c, legal_name (col 8),
plus turnover (UZS + USD across all periods) carried from the
2026-04-30 Group 2 review (F handoff F-to-CC-2026-04-30-1734).

Idempotent — uses INSERT OR IGNORE on UNIQUE keys, safe to re-run on
every init_db().
"""

# ── Categories ───────────────────────────────────────────────────────────────
# (sort_order, label_uz, label_ru, label_en, is_freetext)
# sort_order matches the locked dropdown order: dominant category first,
# multi-supplier categories next, single-supplier alphabetical, "Boshqa" last.

CATEGORIES = [
    (1,  "Lak, bo'yoq",              "Лаки, краски",      "Paint, Lacquer",       0),
    (2,  "Burama mix va shuruplar",  "Саморезы и шурупы", "Self-tapping screws",  0),
    (3,  "Quruq aralashma",          "Сухие смеси",       "Dry mixes",            0),
    (4,  "Eritma",                   "Растворитель",      "Solvent",              0),
    (5,  "Elektrod",                 "Электрод",          "Electrode",            0),
    (6,  "Karbid",                   "Карбид",            "Carbide",              0),
    (7,  "Koller",                   "Коллер",            "Toner / Color mixer",  0),
    (8,  "Linoleum",                 "Линолеум",          "Linoleum",             0),
    (9,  "Metall mahsulot",          "Металлопродукция",  "Metal products",       0),
    (10, "Mixlar",                   "Гвозди",            "Nails",                0),
    (11, "Pena va silikon",          "Пена, силикон",     "Foam & silicone",      0),
    (12, "Sintepon",                 "Синтепон",          "Synthetic padding",    0),
    (13, "Boshqa",                   "Другое",            "Other",                1),
]


# ── Suppliers ────────────────────────────────────────────────────────────────
# (name_1c, mini_app_category_label_uz | None, legal_name | None, periods, uzs, usd)
# mini_app_category_label_uz=None → inactive (no Stage 1 category mapping)
# is_active is derived from category presence: tag → active, no tag → kept but inactive.

SUPPLIERS = [
    ("DELUX Самандар ака",                "Lak, bo'yoq",              None,                                                      48,  6_409_900_960,    181_712),
    ("EAST COLOR /BUILD TECHNO TRADE/",   "Lak, bo'yoq",              None,                                                      48,  1_184_955_360,    315_644),
    ("GAMMA COLOR SERVICE",               "Lak, bo'yoq",              '"GAMMA COLOR SERVICE" MCHJ',                              48,        607_200,    797_642),
    ("GOOGLE",                            "Lak, bo'yoq",              None,                                                      48,  5_596_747_080,    840_499),
    ("LAMA STANDART",                     "Lak, bo'yoq",              'СП ООО "LAMA STANDART"',                                  48,  3_398_087_064, 11_082_835),
    ("PAINTERA",                          "Lak, bo'yoq",              None,                                                      48,    883_147_200,    336_125),
    ("R O Y A L",                         "Lak, bo'yoq",              None,                                                      48,  4_325_678_400,     28_400),
    ("SILKCOAT PAINT",                    "Lak, bo'yoq",              'СП ООО «SILKCOAT PAINT»',                                 48, 10_609_796_400,  1_022_857),
    ("SIMPLEX BIZNES",                    "Lak, bo'yoq",              None,                                                      48,     13_305_600,    906_530),
    ("ZIP КОЛЛЕР",                        "Koller",                   None,                                                      48, 10_423_201_440,     12_189),
    ("АКФИКС",                            "Pena va silikon",          '"AKFIX 008" MCHJ',                                        48,     59_025_600,    775_309),
    ("ДЕКОАРТ",                           "Lak, bo'yoq",              None,                                                      48,  3_526_500_400,     26_214),
    ("КАРБИД",                            "Karbid",                   'СП «ОБОД ТУРМУШ ШОВОТ»',                                  48,     41_976_000,  1_606_002),
    ("ЛИНОЛЕУМ САНФА",                    "Linoleum",                 None,                                                      48,     61_068_000,  6_620_963),
    ("ЛОПАТКИ /РАЗНЫЕ/",                  None,                       None,                                                      48,      1_537_200,      3_600),
    ("ПРОЧИЕ",                            None,                       None,                                                      48,  5_348_283_332,    506_017),
    ("Растворитель",                      "Eritma",                   '"THE MIRAGE INVESTMENT" MCHJ',                            48,  1_155_197_400,    183_571),
    ("СЕНТИФОН",                          "Sintepon",                 'ООО "USEFUL BUSINESS GROUP"',                             48,      7_731_600,    197_240),
    ("СОУДАЛ /ПОЛИСАН/",                  "Lak, bo'yoq",              None,                                                      48,     24_037_800,    204_730),
    ("УЗКАБЕЛЬ",                          "Lak, bo'yoq",              '«UZKABEL» AJ QK',                                         48,  4_892_355_440, 17_118_211),
    ("ШЛИФ ШКУРКА",                       None,                       None,                                                      48,    226_555_200,    397_620),
    ("ЭЛЕКТРОД",                          "Elektrod",                 None,                                                      48,  5_754_485_300,    374_589),
    ("ЭМАЛЬ НЦ-132П",                     "Lak, bo'yoq",              None,                                                      48,      1_478_400,     45_203),
    ("Ташкент Трубный з-д",               "Metall mahsulot",          'СП ООО "Ташкентский трубный завод имени В.Л. Гальперина"', 42,              0,  1_236_130),
    ("САМОРЕЗ  OFM",                      "Burama mix va shuruplar",  None,                                                      38,     13_872_000,  1_519_099),
    ("ШЛАНГ ПОЛИВНОЙ",                    None,                       None,                                                      38,     25_800_000,    129_606),
    ("KRIPTEKS - METAL",                  None,                       None,                                                      29,     10_500_000,    544_592),
    ("ЭКОС /КораСарой/",                  None,                       None,                                                      29,    413_007_000,     25_565),
    ("ЭЛЕРОН ЭЛИТ СЕРВИС",                "Quruq aralashma",          None,                                                      24, 39_670_408_078,          0),
    ("НАЦИОНАЛ КЕРАМИК",                  None,                       None,                                                      24,  2_487_857_340,          0),
    ("ЦЕМЕНТ",                            None,                       None,                                                      24,  1_053_628_100,          0),
    ("ДЕКОПЛАСТ",                         None,                       None,                                                      24,     60_136_800,          0),
    ("СОБСАН",                            "Lak, bo'yoq",              None,                                                      24,              0,  1_857_766),
    ("PUFA MIX",                          None,                       None,                                                      24,              0,    950_635),
    ("MASHXAD",                           None,                       None,                                                      24,              0,    722_553),
    ("ПалИЖ КОЛЛЕР",                      None,                       None,                                                      24,              0,    472_292),
    ("WEBER",                             "Lak, bo'yoq",              None,                                                      24,              0,    358_313),
    ("СОМО FIX",                          None,                       None,                                                      24,              0,    234_906),
    ("НОРА ойти",                         None,                       None,                                                      24,              0,    107_287),
    ("НЮМИКС",                            "Lak, bo'yoq",              None,                                                      24,              0,     70_464),
    ("FUBER",                             "Quruq aralashma",          None,                                                      19, 11_328_602_796,      9_167),
    ("КораСарой/ЭКОС/",                   None,                       None,                                                      19,  2_801_289_000,    361_001),
    ("ГВОЗДИ /KRIPTEKS-METAL/",           "Mixlar",                   None,                                                      19,    105_388_800,  4_409_992),
    ("Саморез TAGERT",                    "Burama mix va shuruplar",  None,                                                      18,              0,  1_538_994),
    ('СП ООО "RANGLI B O\' Y O Q"',       "Lak, bo'yoq",              None,                                                      15,              0,    120_423),
    ("RANGLI BO'YOQ",                     "Lak, bo'yoq",              None,                                                       9,              0,  2_708_142),
]


def seed_procurement(conn):
    """Seed all three procurement tables. Idempotent.

    Strategy: INSERT OR IGNORE on UNIQUE keys (label_uz, name_1c, and
    composite (supplier_id, category_id)). Safe to call on every startup.
    """
    cur = conn.cursor()

    # 1. Categories
    for sort_order, label_uz, label_ru, label_en, is_freetext in CATEGORIES:
        cur.execute(
            """
            INSERT OR IGNORE INTO procurement_categories
                (label_uz, label_ru, label_en, sort_order, is_freetext)
            VALUES (?, ?, ?, ?, ?)
            """,
            (label_uz, label_ru, label_en, sort_order, is_freetext),
        )

    # Build label_uz → id map
    cat_map = {
        row["label_uz"]: row["id"]
        for row in cur.execute("SELECT id, label_uz FROM procurement_categories").fetchall()
    }

    # 2. Suppliers
    for name_1c, mini_app_label, legal_name, periods, uzs, usd in SUPPLIERS:
        is_active = 1 if mini_app_label else 0
        cur.execute(
            """
            INSERT OR IGNORE INTO suppliers
                (name_1c, legal_name, activity_uzs, activity_usd, periods, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (name_1c, legal_name, uzs, usd, periods, is_active),
        )

    # Build name_1c → id map
    sup_map = {
        row["name_1c"]: row["id"]
        for row in cur.execute("SELECT id, name_1c FROM suppliers").fetchall()
    }

    # 3. Supplier ↔ Category mapping (only for active suppliers with a category)
    for name_1c, mini_app_label, legal_name, periods, uzs, usd in SUPPLIERS:
        if not mini_app_label:
            continue
        sup_id = sup_map.get(name_1c)
        cat_id = cat_map.get(mini_app_label)
        if sup_id and cat_id:
            cur.execute(
                """
                INSERT OR IGNORE INTO supplier_categories (supplier_id, category_id)
                VALUES (?, ?)
                """,
                (sup_id, cat_id),
            )

    conn.commit()
