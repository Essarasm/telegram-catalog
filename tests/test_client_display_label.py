"""client_display_label — #74 bot-search dual-name display sub-fix.

Append the alternate name ONLY for drift-like divergence (multi-word name
sharing no token with client_id_1c). Benign Telegram-label / token-sharing
divergence must stay quiet.
"""
from backend.services.client_search import client_display_label


def test_drift_like_divergence_shows_both():
    # The САРДОР → Мурод drift case: two distinct multi-word names, no shared token.
    assert client_display_label("Мурод ака Вокзал", "САРДОР Пищевой") == \
        "Мурод ака Вокзал (sotuvchi: САРДОР Пищевой)"


def test_single_word_telegram_label_quiet():
    # name is a one-word Telegram first-name → benign, no append.
    assert client_display_label("Аброр ЖУШ", "Abror") == "Аброр ЖУШ"


def test_shared_token_quiet():
    # Same person, name shares a token → variant, not drift → no append.
    assert client_display_label("Мурод ака Вокзал", "Мурод Иванов") == "Мурод ака Вокзал"


def test_identical_quiet():
    assert client_display_label("САРДОР Пищевой", "САРДОР Пищевой") == "САРДОР Пищевой"


def test_empty_name_returns_cid():
    assert client_display_label("Мурод ака Вокзал", "") == "Мурод ака Вокзал"
    assert client_display_label("Мурод ака Вокзал", None) == "Мурод ака Вокзал"


def test_empty_cid_returns_name():
    assert client_display_label("", "Abror") == "Abror"
    assert client_display_label(None, "Abror") == "Abror"
