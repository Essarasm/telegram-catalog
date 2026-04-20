"""Tests for search transliteration, phonetic aliases, and ranking."""
from backend.database import (
    transliterate_to_latin,
    transliterate_to_cyrillic,
    normalize_uzbek,
    build_search_text,
)
from backend.routers.products import _score_match, _trigram_similarity


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
