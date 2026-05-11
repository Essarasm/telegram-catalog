"""Regression — message types that must NOT be swallowed by the `fallback`
handler in bot/main.py.

The dispatcher-level catch-all at bot/main.py:~522 fires for DMs that don't
match any earlier `@dp.message(Command(...))`. In aiogram 3 the dispatcher's
own handlers run BEFORE any `include_router(...)` sub-router, so anything
that matches the fallback filter is unreachable from sub-routers.

Each exclusion below pins a real incident:
  - ~F.location  (April 2026): every location pin from agents in client
                  shops was eaten — `location_attempts` audit had zero rows.
  - ~F.document  (April 2026): related — XLS uploads in DM swallowed
                  before uploads_router could parse them.
  - ~F.photo     (May 2026): cashier router's Stage 5a transfer-proof
                  handler accepts photo replies in DM; fallback was eating
                  every photo before sub-routers saw it.
  - ~F.text.startswith("/")  (2026-05-11): /morningbrief in admin_router
                              was unreachable from DM. Pattern: any future
                              router-level slash command added to a DM-active
                              router would silently fail without this filter.
                              Generalizing "exclude slash commands" once means
                              we don't have to add a new exclusion every time
                              a new admin/agent/cashier command gets added.

If any of these exclusions is dropped, the corresponding sub-router goes
silent in DM. These tests fail on regression.
"""
from pathlib import Path


def test_bot_main_fallback_filter_source_excludes_required_shapes():
    src = Path(__file__).resolve().parents[1] / "bot" / "main.py"
    content = src.read_text(encoding="utf-8")
    expected = (
        '@dp.message(F.chat.type == "private", ~F.location, ~F.document, ~F.photo,\n'
        '            ~F.text.startswith("/"))'
    )
    assert expected in content, (
        "bot/main.py fallback filter must keep ~F.location, ~F.document, "
        "~F.photo, AND ~F.text.startswith('/') so sub-routers can handle "
        "locations, documents, photos, and slash commands in private DMs. "
        "See test docstring for incident history."
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
    fallback = (
        (F.chat.type == "private")
        & ~F.location & ~F.document & ~F.photo
        & ~F.text.startswith("/")
    )

    loc = _msg(location=Location(latitude=1.0, longitude=2.0))
    doc = _msg(document=Document(file_id="x", file_unique_id="y", file_name="f.xls"))
    photo = _msg(photo=[PhotoSize(file_id="p", file_unique_id="q", width=100, height=100)])
    txt = _msg(text="hello")
    cmd = _msg(text="/morningbrief")
    cmd_with_arg = _msg(text="/morningbrief 2026-05-09")
    group_txt = _msg(chat=group_chat, text="hello")
    group_cmd = _msg(chat=group_chat, text="/morningbrief")

    assert fallback.resolve(loc) is False, "location pin must pass through to location_router"
    assert fallback.resolve(doc) is False, "document must pass through to uploads_router or cashier_router"
    assert fallback.resolve(photo) is False, "photo must pass through to cashier_router (Stage 5a transfer proof)"
    assert fallback.resolve(txt) is True, "plain text in DM still hits fallback"
    assert fallback.resolve(cmd) is False, "slash command in DM must pass through to admin_router etc."
    assert fallback.resolve(cmd_with_arg) is False, "slash command with arg must also pass through"
    assert fallback.resolve(group_txt) is False, "group-chat messages never hit the fallback"
    assert fallback.resolve(group_cmd) is False, "group-chat command messages never hit the fallback"
