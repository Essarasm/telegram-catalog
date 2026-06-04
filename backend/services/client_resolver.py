"""Client Identity Anchoring — Phase 2: the single resolve-or-hold chokepoint.

`resolve_client()` is the ONE place every client-creation path asks "which
allowed_clients row is this?". It is **read-only** — it never mutates. It
returns a verdict; the caller acts on it (UPDATE on `matched`, INSERT on
`create`, queue on `hold`). This separation keeps the resolution policy in one
auditable function while each channel keeps its own write semantics.

Precedence (most-stable signal first — see Client_Identity_Anchoring_Design):
  1. onec_card_id   → definitive (the 1C card anchor; Phase 0)
  2. linked telegram_id (users.client_id) → remembered identity
  3. client_phones match → candidate; confirms a stronger signal, or stands
     alone when it's the only signal; CONFLICTS → hold
  4. name → tiebreaker ONLY, never a sole basis for a match (#75: name is a
     non-unique label)

Cardinal rule: the ONLY verdict that authorises an INSERT is `create`. Any
ambiguity (phone disagrees with a stronger signal, or multiple phone candidates
a name can't break) returns `hold` → the caller queues it to
`client_identity_drift_queue` for review. No path may silently INSERT a
competing row.

Verdict dict:
  {"action": "matched"|"create"|"hold",
   "client_id": int|None,        # set when matched
   "matched_via": str|None,      # onec_card_id|telegram_id|phone|phone+name
   "reason": str,
   "candidates": [int, ...]}     # set when hold (the conflicting/ambiguous ids)
"""
from __future__ import annotations

from backend.services.phone_slots import _normalize
from backend.services.client_identity_reviewed import normalize_1c

_NOT_MERGED = "COALESCE(status,'active') NOT LIKE 'merged%'"


def _is_active(conn, client_id) -> bool:
    if not client_id:
        return False
    row = conn.execute(
        f"SELECT 1 FROM allowed_clients WHERE id = ? AND {_NOT_MERGED}",
        (client_id,),
    ).fetchone()
    return row is not None


def _client_name(conn, client_id) -> str:
    row = conn.execute(
        "SELECT client_id_1c, name FROM allowed_clients WHERE id = ?", (client_id,)
    ).fetchone()
    if not row:
        return ""
    return str(row[0] or row[1] or "")


def _verdict(action, client_id=None, matched_via=None, reason="", candidates=None):
    return {"action": action, "client_id": client_id, "matched_via": matched_via,
            "reason": reason, "candidates": candidates or []}


def resolve_client(conn, *, onec_card_id=None, telegram_id=None,
                   phones=None, name=None):
    """Resolve identity signals to a verdict. READ-ONLY. See module docstring."""
    onec_card_id = (onec_card_id or "").strip() or None
    name = (name or "").strip() or None
    norm_phones = []
    for p in (phones or []):
        n = _normalize(p)
        if n and n not in norm_phones:
            norm_phones.append(n)

    # ── 1. onec_card_id — definitive ─────────────────────────────────────────
    strong_id, strong_via = None, None
    if onec_card_id:
        row = conn.execute(
            f"SELECT id FROM allowed_clients WHERE onec_card_id = ? AND {_NOT_MERGED} "
            f"ORDER BY id LIMIT 1",
            (onec_card_id,),
        ).fetchone()
        if row:
            strong_id, strong_via = row[0], "onec_card_id"

    # ── 2. linked telegram_id — remembered identity ──────────────────────────
    if strong_id is None and telegram_id:
        row = conn.execute(
            "SELECT client_id FROM users WHERE telegram_id = ? AND client_id IS NOT NULL",
            (telegram_id,),
        ).fetchone()
        if row and _is_active(conn, row[0]):
            strong_id, strong_via = row[0], "telegram_id"

    # ── 3. client_phones candidates ──────────────────────────────────────────
    phone_ids = []
    if norm_phones:
        ph = ",".join("?" * len(norm_phones))
        rows = conn.execute(
            f"SELECT DISTINCT cp.client_id FROM client_phones cp "
            f"JOIN allowed_clients a ON a.id = cp.client_id "
            f"WHERE cp.phone_normalized IN ({ph}) AND {_NOT_MERGED.replace('status', 'a.status')} "
            f"ORDER BY cp.client_id",
            norm_phones,
        ).fetchall()
        phone_ids = [r[0] for r in rows]

    # A stronger signal is set: phone must agree, or it's a conflict (#74 drift).
    if strong_id is not None:
        if not phone_ids or phone_ids == [strong_id]:
            return _verdict("matched", strong_id, strong_via,
                            f"resolved via {strong_via}")
        others = [i for i in phone_ids if i != strong_id]
        if others:
            return _verdict("hold", None, None,
                            f"phone points to {others} but {strong_via} points to "
                            f"{strong_id} — conflict, held for review",
                            candidates=[strong_id, *others])
        return _verdict("matched", strong_id, strong_via, f"resolved via {strong_via}")

    # No stronger signal — phone stands alone.
    if len(phone_ids) == 1:
        return _verdict("matched", phone_ids[0], "phone", "resolved via single phone match")
    if len(phone_ids) > 1:
        # ── 4. name as tiebreaker only ───────────────────────────────────────
        if name:
            nk = normalize_1c(name)
            name_hits = [i for i in phone_ids if normalize_1c(_client_name(conn, i)) == nk]
            if len(name_hits) == 1:
                return _verdict("matched", name_hits[0], "phone+name",
                                "multiple phone candidates broken by name tiebreaker")
        return _verdict("hold", None, None,
                        f"phone matches multiple clients {phone_ids}, name cannot "
                        f"disambiguate — held for review", candidates=phone_ids)

    # No stable signal matched. Name is never a sole basis → create (a pending
    # row; the next 1C import adopts it by card id / phone). #75: matching on
    # name alone would fuse same-named distinct shops.
    return _verdict("create", None, None,
                    "no stable signal matched — create (name is not a sole basis)")
