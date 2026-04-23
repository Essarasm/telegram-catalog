"""Regression — location pins in a private DM must not be swallowed by the
`fallback` handler in bot/main.py.

April 2026 incident: the fallback was registered as a top-level
`@dp.message(F.chat.type == "private")` with no exclusions. In aiogram 3
the root dispatcher's own handlers run before any `include_router(...)`
sub-router, so every location pin an agent sent from inside a client's
shop was swallowed by the fallback. The `location_attempts` audit table
had zero rows in production; no client had GPS saved via the bot flow.

The fix: add `~F.location` and `~F.document` to the fallback filter so
sub-routers (`location_router`, `uploads_router`) can handle those
message shapes. If the exclusion is ever dropped again, these tests fail.
"""
from pathlib import Path


def test_bot_main_fallback_filter_source_excludes_location_and_document():
    src = Path(__file__).resolve().parents[1] / "bot" / "main.py"
    content = src.read_text(encoding="utf-8")
    assert (
        '@dp.message(F.chat.type == "private", ~F.location, ~F.document)' in content
    ), (
        "bot/main.py fallback filter must keep ~F.location and ~F.document so "
        "the location and uploads routers can handle those in private DMs."
    )


def test_fallback_filter_semantics():
    """Evaluate the exact filter expression against synthetic messages."""
    import pytest
    pytest.importorskip("aiogram")

    from datetime import datetime
    from aiogram import F
    from aiogram.types import Chat, Document, Location, Message, User

    private_chat = Chat(id=123, type="private")
    group_chat = Chat(id=-100, type="supergroup")
    sender = User(id=1, is_bot=False, first_name="A")

    def _msg(**kwargs) -> Message:
        base = dict(message_id=1, date=datetime.now(),
                    chat=private_chat, from_user=sender)
        base.update(kwargs)
        return Message(**base)

    # Mirror of the bot/main.py fallback filter exactly.
    fallback = (F.chat.type == "private") & ~F.location & ~F.document

    loc = _msg(location=Location(latitude=1.0, longitude=2.0))
    doc = _msg(document=Document(file_id="x", file_unique_id="y", file_name="f.xls"))
    txt = _msg(text="hello")
    group_txt = _msg(chat=group_chat, text="hello")

    assert fallback.resolve(loc) is False, "location pin must pass through to location_router"
    assert fallback.resolve(doc) is False, "document must pass through to uploads_router"
    assert fallback.resolve(txt) is True, "plain text in DM still hits fallback"
    assert fallback.resolve(group_txt) is False, "group-chat messages never hit the fallback"
