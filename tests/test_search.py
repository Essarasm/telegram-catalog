"""Tests for search transliteration, phonetic aliases, and ranking."""
import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.database import (
    transliterate_to_latin,
    transliterate_to_cyrillic,
    normalize_uzbek,
    build_search_text,
    rebuild_all_search_text,
)
from backend.routers.products import _score_match, _trigram_similarity

# admin_auth captures ADMIN_API_KEY at import time; conftest.py setdefault's it.
ADMIN_KEY = os.environ["ADMIN_API_KEY"]


class TestTransliterateLatin:
    def test_basic_cyrillic(self):
        assert transliterate_to_latin("цемент") == "tsement"

    def test_digraphs(self):
        assert transliterate_to_latin("шпатлевка") == "shpatlevka"

    def test_mixed_case(self):
        assert transliterate_to_latin("ВЭБЕР") == "veber"

    def test_non_cyrillic_passthrough(self):
        assert transliterate_to_latin("abc123") == "abc123"


class TestTransliterateCyrillic:
    def test_basic_latin(self):
        result = transliterate_to_cyrillic("tsement")
        assert result == "цемент"

    def test_phonetic_alias_exact(self):
        assert transliterate_to_cyrillic("siment") == "цемент"
        assert transliterate_to_cyrillic("emal") == "эмаль"
        assert transliterate_to_cyrillic("gruntovka") == "грунтовка"

    def test_digraphs_reversed(self):
        result = transliterate_to_cyrillic("shpatlevka")
        assert "ш" in result
        assert "sh" not in result

    def test_unknown_word_transliterated(self):
        result = transliterate_to_cyrillic("abcdef")
        assert result == "абцдеф"


class TestNormalizeUzbek:
    def test_apostrophe_o(self):
        assert normalize_uzbek("o'ram") == "oram"

    def test_apostrophe_g(self):
        assert normalize_uzbek("g'isht") == "gisht"

    def test_no_change(self):
        assert normalize_uzbek("cement") == "cement"


class TestBuildSearchText:
    def test_includes_cyrillic_and_latin(self):
        st = build_search_text("ВЭБЕР СТАНДАРТ", "Standart Oq", "Weber")
        assert "вэбер" in st
        assert "standart" in st
        assert "weber" in st

    def test_includes_transliterated(self):
        st = build_search_text("ЦЕМЕНТ М500", None, "Cemix")
        assert "tsement" in st

    def test_includes_reverse_transliterated(self):
        st = build_search_text(None, "Standart Oq", "Weber")
        assert "стандарт" in st or "станд" in st

    def test_includes_category(self):
        st = build_search_text("Краска", "Paint", "Brand", category_name="Эмали")
        assert "эмали" in st


class TestScoreMatch:
    def _product(self, name, name_display, search_text=None):
        return {
            "name": name,
            "name_display": name_display,
            "search_text": search_text or f"{name} {name_display}".lower(),
        }

    def test_exact_match_score_4(self):
        p = self._product("цемент", "Cement")
        assert _score_match("цемент", "tsement", "цемент", p) == 4

    def test_starts_with_score_3(self):
        p = self._product("цемент м500", "Cement M500")
        assert _score_match("цемент", "tsement", "цемент", p) == 3

    def test_any_word_starts_score_3(self):
        # As of 2026-04-20 (Session S Part 10) ANY word in the name starting
        # with the term scores 3, not just the first word.
        p = self._product("портланд цемент м500", "Portland Cement M500")
        assert _score_match("цемент", "tsement", "цемент", p) == 3

    def test_contains_score_2(self):
        # Score 2 now only fires for truly-embedded matches (term appears
        # inside a word, not at a word-boundary).
        p = self._product("суперцемент м500", "SuperCement M500")
        assert _score_match("цемент", "tsement", "цемент", p) == 2

    def test_no_match_score_0(self):
        p = self._product("краска белая", "White Paint")
        assert _score_match("цемент", "tsement", "цемент", p) == 0

    def test_cyrillic_variant_matches(self):
        p = self._product("эмаль белая", "Emal oq", "эмаль белая emal oq")
        assert _score_match("emal", "emal", "emal", p, search_cyrillic="эмаль") >= 2


class TestTrigramSimilarity:
    def test_identical(self):
        assert _trigram_similarity("цемент", "цемент") == 1.0

    def test_similar(self):
        sim = _trigram_similarity("сатин 53", "сатин-53")
        assert sim >= 0.5

    def test_different(self):
        sim = _trigram_similarity("цемент", "краска")
        assert sim < 0.2

    def test_empty(self):
        # Padded empty strings produce matching trigrams
        sim = _trigram_similarity("", "")
        assert isinstance(sim, float)


class TestClientSearchFuzzyFill:
    """`search_clients()` opts callers into typeahead-style fuzzy fill that
    survives client-name typos for the agent panel and (later) bot flows."""

    @pytest.fixture
    def seed_clients(self, db):
        # Three whitelisted (allowed_clients) + two 1C-only (client_balances)
        # rows. Names span Cyrillic + Latin to mirror real 1C data.
        db.execute(
            "INSERT INTO allowed_clients (id, phone_normalized, name, client_id_1c, status) "
            "VALUES (1, '998901112233', 'Сардор Маркет', 'СардорМаркет001', 'active')"
        )
        db.execute(
            "INSERT INTO allowed_clients (id, phone_normalized, name, client_id_1c, status) "
            "VALUES (2, '998901112244', 'Гулноза ойти ТАЙЛОК', 'ГулнозаТАЙЛОК', 'active')"
        )
        db.execute(
            "INSERT INTO allowed_clients (id, phone_normalized, name, client_id_1c, status) "
            "VALUES (3, '998901112255', 'STROY MARKET', 'STROYMARKET', 'active')"
        )
        db.execute(
            "INSERT INTO client_balances (client_name_1c, period_start, period_end, currency, client_id) "
            "VALUES ('Лочин Универсал', '2026-01-01', '2026-01-31', 'UZS', NULL)"
        )
        db.execute(
            "INSERT INTO client_balances (client_name_1c, period_start, period_end, currency, client_id) "
            "VALUES ('Ботир Дўкон', '2026-01-01', '2026-01-31', 'UZS', NULL)"
        )
        db.commit()
        return db

    def test_exact_match_tagged_exact(self, seed_clients):
        from backend.services.client_search import search_clients
        r = search_clients("Сардор", fuzzy=True)
        assert r["fuzzy_count"] == 0
        assert r["whitelisted"], "expected exact hit for 'Сардор'"
        assert all(c["match_type"] == "exact" for c in r["whitelisted"])

    def test_typo_triggers_fuzzy_in_whitelisted(self, seed_clients):
        from backend.services.client_search import search_clients
        # 'тайлоок' is a typo of 'ТАЙЛОК' (one inserted char). Word-level
        # trigram should score it above the 0.45 threshold.
        r = search_clients("тайлоок", fuzzy=True)
        fuzzy_wl = [c for c in r["whitelisted"] if c["match_type"] == "fuzzy"]
        assert fuzzy_wl, f"expected fuzzy whitelisted hit, got {r}"
        assert all("similarity" in c for c in fuzzy_wl)
        assert r["fuzzy_count"] >= 1

    def test_typo_triggers_fuzzy_in_new_1c(self, seed_clients):
        from backend.services.client_search import search_clients
        # 'лочинн' (typo of 'Лочин') has no LIKE match against 'Лочин Универсал',
        # but trigram word-level similarity catches it.
        r = search_clients("лочинн", fuzzy=True)
        fuzzy_new = [c for c in r["new_1c"] if c["match_type"] == "fuzzy"]
        assert fuzzy_new, f"expected fuzzy new_1c hit, got {r}"

    def test_digit_query_skips_fuzzy(self, seed_clients):
        from backend.services.client_search import search_clients
        # Cashier / bank-transfer also accept numeric client IDs.
        # Trigram on digit strings is meaningless — fuzzy must be skipped.
        r = search_clients("99890", fuzzy=True)
        assert r["fuzzy_count"] == 0

    def test_short_query_skips_fuzzy(self, seed_clients):
        from backend.services.client_search import search_clients
        r = search_clients("ст", fuzzy=True)
        assert r["fuzzy_count"] == 0

    def test_fuzzy_off_by_default(self, seed_clients):
        from backend.services.client_search import search_clients
        # Bot callers that haven't been adapted yet must keep getting
        # exact-only results — verify the opt-in default.
        r = search_clients("тайлоок")
        assert r["fuzzy_count"] == 0
        assert not r["whitelisted"] and not r["new_1c"]


class TestRoutePlannerFuzzy:
    """`/api/collections/clients/search` powers the admin dashboard's
    delivery-stop picker. It has its own SQL (GPS-aware shape) but shares
    the same fuzzy helpers, so the same typo-tolerance contract applies."""

    @pytest.fixture
    def client(self, db):
        db.execute(
            "INSERT INTO allowed_clients (id, phone_normalized, name, client_id_1c, "
            "company_name, status, gps_latitude, gps_longitude, tuman, viloyat) "
            "VALUES (10, '998901112233', 'Сардор Маркет', 'СардорМаркет001', "
            "'Сардор ООО', 'active', 41.31, 69.28, 'Юнусобод', 'Тошкент')"
        )
        db.execute(
            "INSERT INTO allowed_clients (id, phone_normalized, name, client_id_1c, "
            "company_name, status, gps_latitude, gps_longitude, tuman, viloyat) "
            "VALUES (11, '998901112244', 'Гулноза ойти ТАЙЛОК', 'ГулнозаТАЙЛОК', "
            "NULL, 'active', NULL, NULL, NULL, NULL)"
        )
        db.commit()
        from backend.routers.collections import router
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    def test_exact_returns_match_type_exact(self, client):
        r = client.get("/api/collections/clients/search",
                       params={"q": "Сардор", "admin_key": ADMIN_KEY})
        assert r.status_code == 200
        data = r.json()
        assert data["results"]
        assert all(item["match_type"] == "exact" for item in data["results"])
        assert data["fuzzy_count"] == 0

    def test_typo_triggers_fuzzy(self, client):
        r = client.get("/api/collections/clients/search",
                       params={"q": "тайлоок", "admin_key": ADMIN_KEY})
        assert r.status_code == 200
        data = r.json()
        fuzzy = [i for i in data["results"] if i["match_type"] == "fuzzy"]
        assert fuzzy, f"expected fuzzy hit, got {data}"
        assert data["fuzzy_count"] >= 1
        assert all("similarity" in i for i in fuzzy)

    def test_digit_query_skips_fuzzy(self, client):
        r = client.get("/api/collections/clients/search",
                       params={"q": "99890", "admin_key": ADMIN_KEY})
        assert r.status_code == 200
        assert r.json()["fuzzy_count"] == 0


class TestSuggestionsFuzzyFill:
    """`/api/search/suggestions` tags every item with match_type and fills
    short result sets with trigram-fuzzy hits so the frontend can render a
    section break."""

    @pytest.fixture
    def client(self, seed_products):
        rebuild_all_search_text(seed_products)
        from backend.routers.search import router
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    def test_exact_hits_tagged_exact(self, client):
        r = client.get("/api/search/suggestions", params={"q": "вэбер", "limit": 6})
        assert r.status_code == 200
        data = r.json()
        assert data["suggestions"], "expected at least one suggestion for 'вэбер'"
        assert all(s["match_type"] == "exact" for s in data["suggestions"])
        assert data["fuzzy_count"] == 0

    def test_typo_falls_back_to_fuzzy(self, client):
        # "стандрт" is a 1-edit typo of "стандарт" (word in seeded product 1).
        # LIKE-match misses it; trigram word-level similarity ≈ 0.545 — above
        # the 0.45 typeahead threshold, so fuzzy fill should produce a hit.
        r = client.get("/api/search/suggestions", params={"q": "стандрт", "limit": 6})
        assert r.status_code == 200
        data = r.json()
        assert data["fuzzy_count"] >= 1, f"expected fuzzy fill, got {data}"
        fuzzy_items = [s for s in data["suggestions"] if s["match_type"] == "fuzzy"]
        assert fuzzy_items, "no items tagged fuzzy"
        assert all("similarity" in s for s in fuzzy_items)
        assert {s["match_type"] for s in data["suggestions"]} <= {"exact", "fuzzy"}
