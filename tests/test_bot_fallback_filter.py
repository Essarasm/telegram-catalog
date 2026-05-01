"""Regression — location pins, documents, and photos in a private DM must
not be swallowed by the `fallback` handler in bot/main.py.

April 2026 incident: the fallback was registered as a top-level
`@dp.message(F.chat.type == "private")` with no exclusions. In aiogram 3
the root dispatcher's own handlers run before any `include_router(...)`
sub-router, so every location pin an agent sent from inside a client's
shop was swallowed by the fallback. The `location_attempts` audit table
had zero rows in production; no client had GPS saved via the bot flow.

May 2026 incident: same shape-of-bug recurred with photos. Cashier
router's Stage 5a transfer-proof handler accepts photo replies in DM
but the fallback still ate every photo before sub-routers saw it.

The fix: keep `~F.location`, `~F.document`, AND `~F.photo` on the
fallback filter so sub-routers (`location_router`, `uploads_router`,
`cashier_router`) can handle those message shapes. If any exclusion is
dropped again, these tests fail.
"""
from pathlib import Path


def test_bot_main_fallback_filter_source_excludes_location_document_photo():
    src = Path(__file__).resolve().parents[1] / "bot" / "main.py"
    content = src.read_text(encoding="utf-8")
    assert (
        '@dp.message(F.chat.type == "private", ~F.location, ~F.document, ~F.photo)' in content
    ), (
        "bot/main.py fallback filter must keep ~F.location, ~F.document, and "
        "~F.photo so the location, uploads, and cashier routers can handle "
        "those message shapes in private DMs."
    )


def test_fallback_filter_semantics():
    """Evaluate the exact filter expression against synthetic messages."""
    import pytest
    pytest.importorskip("aiogram")

    from datetime import datetime
    from aiogram import F
    from aiogram.types import Chat, Document, Location, Message, PhotoSize, User

    private_chat = Chat(id=123, type="private")
    group_chat = Chat(id=-100, type="supergroup")
    sender = User(id=1, is_bot=False, first_name="A")

    def _msg(**kwargs) -> Message:
        base = dict(message_id=1, date=datetime.now(),
                    chat=private_chat, from_user=sender)
        base.update(kwargs)
        return Message(**base)

    # Mirror of the bot/main.py fallback filter exactly.
    fallback = (F.chat.type == "private") & ~F.location & ~F.document & ~F.photo

    loc = _msg(location=Location(latitude=1.0, longitude=2.0))
    doc = _msg(document=Document(file_id="x", file_unique_id="y", file_name="f.xls"))
    photo = _msg(photo=[PhotoSize(file_id="p", file_unique_id="q", width=100, height=100)])
    txt = _msg(text="hello")
    group_txt = _msg(chat=group_chat, text="hello")

    assert fallback.resolve(loc) is False, "location pin must pass through to location_router"
    assert fallback.resolve(doc) is False, "document must pass through to uploads_router or cashier_router"
    assert fallback.resolve(photo) is False, "photo must pass through to cashier_router (Stage 5a transfer proof)"
    assert fallback.resolve(txt) is True, "plain text in DM still hits fallback"
    assert fallback.resolve(group_txt) is False, "group-chat messages never hit the fallback"
