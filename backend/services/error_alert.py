"""Send uncaught exceptions to the Admin Telegram group so silent crashes
don't rot in log files. Rate-limited and de-duplicated so a single recurring
error can't flood the group.
"""
import hashlib
import logging
import os
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_GROUP_CHAT_ID = os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051")

# Rate-limit: don't re-alert the same error signature within N seconds.
_SUPPRESS_WINDOW_SEC = 300  # 5 minutes
_recent_signatures: dict[str, float] = {}
_MAX_RECENT = 200


def _should_send(signature: str) -> bool:
    """Return True if this signature hasn't been alerted in the suppression window."""
    now = time.time()
    # Evict old entries
    if len(_recent_signatures) > _MAX_RECENT:
        stale = [k for k, t in _recent_signatures.items() if now - t > _SUPPRESS_WINDOW_SEC]
        for k in stale:
            del _recent_signatures[k]
    last = _recent_signatures.get(signature)
    if last and now - last < _SUPPRESS_WINDOW_SEC:
        return False
    _recent_signatures[signature] = now
    return True


def send_error_alert(
    source: str,              # e.g. "FastAPI /api/export" or "bot /clients"
    exc_type: str,            # e.g. "NameError"
    exc_message: str,         # e.g. "name 'agent_name' is not defined"
    traceback_tail: str = "", # last N lines of the traceback
    request_hint: str = "",   # url / method / body snippet if available
) -> bool:
    """Fire-and-forget alert. Returns True if a message was actually sent
    (False = rate-limited or missing config)."""
    if not BOT_TOKEN or not ADMIN_GROUP_CHAT_ID:
        return False

    # Signature = source + exc_type + first line of message (to de-dup
    # across slightly-different invocations of the same bug)
    first_line = (exc_message or "").split("\n", 1)[0][:200]
    signature = hashlib.sha1(f"{source}|{exc_type}|{first_line}".encode()).hexdigest()[:16]
    if not _should_send(signature):
        return False

    # Keep total message length under 3800 chars (safe for Telegram)
    tb = (traceback_tail or "")[-2400:]
    hint = (request_hint or "")[:400]
    text_parts = [
        "🚨 <b>Error Alert</b>",
        f"📍 <b>Source:</b> {source[:120]}",
        f"🏷 <b>Type:</b> <code>{exc_type[:80]}</code>",
        f"💬 <b>Message:</b> <code>{first_line}</code>",
    ]
    if hint:
        text_parts.append(f"🔗 <b>Request:</b> <code>{hint}</code>")
    if tb:
        text_parts.append("")
        text_parts.append("<b>Traceback:</b>")
        text_parts.append(f"<pre>{tb}</pre>")
    text_parts.append("")
    text_parts.append(f"<i>(suppressed for {_SUPPRESS_WINDOW_SEC}s on same signature)</i>")
    text = "\n".join(text_parts)

    try:
        resp = httpx.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": ADMIN_GROUP_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=5,
        )
        ok = resp.json().get("ok", False)
        if not ok:
            logger.warning(f"error_alert Telegram reply not ok: {resp.text[:200]}")
        return bool(ok)
    except Exception as e:
        logger.warning(f"error_alert failed to send: {e}")
        return False


def _html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def install_fastapi_handler(app) -> None:
    """Attach a global exception handler to a FastAPI app that fires an
    alert for every uncaught exception and still returns a 500 to the client."""
    import traceback as _tb
    from fastapi.responses import JSONResponse
    from starlette.requests import Request

    @app.exception_handler(Exception)
    async def _global_handler(request: Request, exc: Exception):
        try:
            tb_str = _html_escape("".join(_tb.format_exception(type(exc), exc, exc.__traceback__)))
            # Keep only the last ~25 lines of the traceback for brevity
            tb_tail = "\n".join(tb_str.strip().splitlines()[-25:])
            send_error_alert(
                source=f"FastAPI {request.method} {request.url.path}",
                exc_type=type(exc).__name__,
                exc_message=_html_escape(str(exc)),
                traceback_tail=tb_tail,
                request_hint=f"{request.method} {request.url.path}",
            )
        except Exception:
            # Never let the alerter crash the exception handler
            logger.exception("error_alert install_fastapi_handler failed")
        logger.exception(f"Unhandled exception on {request.method} {request.url.path}")
        return JSONResponse(
            {"error": "internal_server_error", "detail": type(exc).__name__},
            status_code=500,
        )


def install_aiogram_handler(dp) -> None:
    """Attach an aiogram error handler that alerts on any unhandled
    exception raised inside a bot handler. aiogram 3.x pattern."""
    import traceback as _tb

    @dp.errors()
    async def _on_bot_error(event) -> bool:
        try:
            exc = event.exception
            update = event.update
            # Best-effort context extraction
            src_bits = []
            chat_id = user_id = None
            msg_text = ""
            if update:
                msg = getattr(update, "message", None) or getattr(update, "edited_message", None)
                cb = getattr(update, "callback_query", None)
                if msg:
                    chat_id = getattr(msg.chat, "id", None) if msg.chat else None
                    user_id = getattr(msg.from_user, "id", None) if msg.from_user else None
                    msg_text = (msg.text or msg.caption or "")[:120]
                    src_bits.append(f"bot msg '{msg_text}'")
                elif cb:
                    chat_id = getattr(cb.message.chat, "id", None) if cb.message and cb.message.chat else None
                    user_id = getattr(cb.from_user, "id", None) if cb.from_user else None
                    src_bits.append(f"bot callback {cb.data}")
                else:
                    src_bits.append(f"bot update_id={getattr(update, 'update_id', '?')}")
            source = " | ".join(src_bits) or "bot (unknown)"

            tb_str = _html_escape("".join(_tb.format_exception(type(exc), exc, exc.__traceback__)))
            tb_tail = "\n".join(tb_str.strip().splitlines()[-25:])
            hint_bits = []
            if chat_id is not None:
                hint_bits.append(f"chat={chat_id}")
            if user_id is not None:
                hint_bits.append(f"user={user_id}")
            send_error_alert(
                source=source,
                exc_type=type(exc).__name__,
                exc_message=_html_escape(str(exc)),
                traceback_tail=tb_tail,
                request_hint=" ".join(hint_bits),
            )
        except Exception:
            logger.exception("error_alert install_aiogram_handler failed")
        # Log for server logs too
        try:
            logger.exception(f"Unhandled bot exception: {event.exception}")
        except Exception:
            pass
        # Return True so aiogram considers it handled (don't re-raise)
        return True
