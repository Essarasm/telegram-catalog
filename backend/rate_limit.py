"""Lightweight in-memory rate limiter for Mini App API endpoints.

Pure ASGI middleware — no external deps, no Redis. Good for a single-node
Railway deployment (which is where this lives). If we ever shard, replace
the in-memory dict with Redis INCR + EXPIRE.

Buckets:
  * `write`  — POST/PUT/DELETE on /api/* (expensive): 30 req / 60 s per key
  * `read`   — GET /api/* (cheap):                    120 req / 60 s per key
  * `export` — /api/export*, /api/admin/* exports:    6  req / 60 s per key
  * `auth`   — /api/users/register, /api/users/check: 20 req / 60 s per key

Keys are derived from `telegram_id` query/body param when present,
otherwise the client IP. Bot-to-API calls (which pass `admin_key`) bypass
limiting entirely — they're internal and trusted.

Exempt paths: /api/health*, /api/debug*, static assets, and anything
served by the frontend SPA mount.

When exceeded, responds 429 with a JSON body and `Retry-After` header.
"""
from __future__ import annotations

import json
import os
import time
from collections import deque
from typing import Deque, Dict, Tuple

# ── Config (overridable via env) ─────────────────────────────

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default

# (limit, window_seconds)
_BUCKETS: Dict[str, Tuple[int, int]] = {
    "write":  (_env_int("RL_WRITE_LIMIT", 30),   _env_int("RL_WRITE_WINDOW", 60)),
    "read":   (_env_int("RL_READ_LIMIT", 120),   _env_int("RL_READ_WINDOW", 60)),
    "export": (_env_int("RL_EXPORT_LIMIT", 6),   _env_int("RL_EXPORT_WINDOW", 60)),
    "auth":   (_env_int("RL_AUTH_LIMIT", 20),    _env_int("RL_AUTH_WINDOW", 60)),
}

# Master kill switch — set RATE_LIMIT_ENABLED=0 to disable all limiting
_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "1") != "0"


# ── State ────────────────────────────────────────────────────

# key → deque of timestamps (sliding window)
_buckets: Dict[str, Deque[float]] = {}
# bucket_name → count of 429s served (for /api/rate-limit-stats)
_hits: Dict[str, int] = {b: 0 for b in _BUCKETS}


# ── Helpers ──────────────────────────────────────────────────

def _classify(path: str, method: str) -> str | None:
    """Return bucket name or None for exempt paths."""
    if not path.startswith("/api/"):
        return None
    # Exempt internal / infra endpoints
    if path.startswith(("/api/health", "/api/debug", "/api/rate-limit-stats")):
        return None
    # Export & admin-heavy endpoints — tightest bucket
    if path.startswith("/api/export") or "/export" in path:
        return "export"
    # User registration / check — gate brute-force enumeration
    if path.startswith(("/api/users/register", "/api/users/check")):
        return "auth"
    # Writes
    if method in ("POST", "PUT", "DELETE", "PATCH"):
        return "write"
    # Everything else under /api
    return "read"


def _client_key(scope, query_string: bytes) -> str:
    """Prefer telegram_id from query; fall back to IP."""
    # Query-string parse (cheap, no urllib import cost on hot path)
    qs = query_string.decode("latin-1", errors="ignore")
    if "telegram_id=" in qs:
        for kv in qs.split("&"):
            if kv.startswith("telegram_id="):
                tid = kv[len("telegram_id="):]
                if tid.isdigit():
                    return f"tg:{tid}"
    # IP fallback
    client = scope.get("client")
    if client:
        return f"ip:{client[0]}"
    return "ip:unknown"


def _is_admin_call(query_string: bytes) -> bool:
    """Bot-to-API calls pass admin_key — don't rate-limit them."""
    return b"admin_key=" in query_string


def _allow(bucket: str, key: str) -> Tuple[bool, int]:
    """Check + record. Returns (allowed, retry_after_seconds)."""
    limit, window = _BUCKETS[bucket]
    now = time.time()
    cutoff = now - window
    k = f"{bucket}:{key}"
    dq = _buckets.get(k)
    if dq is None:
        dq = deque()
        _buckets[k] = dq
    # Drop expired
    while dq and dq[0] < cutoff:
        dq.popleft()
    if len(dq) >= limit:
        _hits[bucket] += 1
        retry = max(1, int(dq[0] + window - now))
        return False, retry
    dq.append(now)
    return True, 0


# ── Middleware ───────────────────────────────────────────────

class RateLimitMiddleware:
    """Pure ASGI rate limiter."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if not _ENABLED or scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        method = scope.get("method", "GET")
        bucket = _classify(path, method)
        if bucket is None:
            await self.app(scope, receive, send)
            return

        qs = scope.get("query_string", b"") or b""
        if _is_admin_call(qs):
            await self.app(scope, receive, send)
            return

        key = _client_key(scope, qs)
        ok, retry = _allow(bucket, key)
        if ok:
            await self.app(scope, receive, send)
            return

        # 429
        body = json.dumps({
            "detail": "rate_limited",
            "bucket": bucket,
            "retry_after": retry,
        }).encode()
        await send({
            "type": "http.response.start",
            "status": 429,
            "headers": [
                (b"content-type", b"application/json"),
                (b"retry-after", str(retry).encode()),
            ],
        })
        await send({"type": "http.response.body", "body": body})


def get_stats() -> dict:
    """Snapshot of rate-limiter state. Wired to /api/rate-limit-stats."""
    return {
        "enabled": _ENABLED,
        "buckets": {b: {"limit": l, "window_s": w} for b, (l, w) in _BUCKETS.items()},
        "active_keys": len(_buckets),
        "total_429s_by_bucket": dict(_hits),
    }
