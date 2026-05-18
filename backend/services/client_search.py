"""Shared client search used by both the bot's /testclient command and the
agent panel mini app.

Two-tier result:
  • whitelisted — already in allowed_clients, tap to link (fast path)
  • new_1c      — only in client_balances, needs allowed_clients insert first
"""
import unicodedata
from collections import OrderedDict
from typing import Optional

from backend.database import (
    get_db,
    normalize_uzbek,
    transliterate_to_cyrillic,
    transliterate_to_latin,
)
from backend.routers.products import _trigram_similarity

# Stricter than /api/products fallback (0.25). Typeahead fires after every
# keystroke and the agent panel result set is small, so noise tolerance is low.
CLIENT_FUZZY_MIN_SCORE = 0.45


def _normalize(q: str) -> str:
    return unicodedata.normalize("NFC", q).strip().lower()


def _register_unicode_lower(conn) -> None:
    """SQLite's built-in LOWER() only lowercases ASCII, so Cyrillic names
    like 'Сардор' never match a lowercased query. Register a Python LOWER
    that handles full Unicode (mirrors bot/shared.py's pattern)."""
    conn.create_function("LOWER", 1, lambda s: s.lower() if s else s)


def _query_variants(q: str) -> set:
    """Build the search variants for a normalized query string. Cyrillic and
    Latin transliterations plus the Uzbek-apostrophe normalization — mirrors
    the variant set used by /api/products fuzzy fallback so client search
    and product search behave consistently for cross-script typing."""
    variants = {q}
    try:
        variants.add(transliterate_to_latin(q))
    except Exception:
        pass
    try:
        variants.add(normalize_uzbek(q))
    except Exception:
        pass
    try:
        variants.add(transliterate_to_cyrillic(q))
    except Exception:
        pass
    return {v for v in variants if v}


def _best_trigram(variants: set, *fields: str) -> float:
    """Highest trigram similarity between any query variant and any non-empty
    field (or any ≥3-char word within those fields). Mirrors the per-word
    matching in _fuzzy_match_products so short queries can hit one word inside
    a multi-word client name."""
    best = 0.0
    for field in fields:
        if not field:
            continue
        text = field.lower()
        words = [w for w in text.split() if len(w) >= 3]
        for v in variants:
            if not v or len(v) < 2:
                continue
            sim = _trigram_similarity(v, text)
            if sim > best:
                best = sim
            for w in words:
                wsim = _trigram_similarity(v, w)
                if wsim > best:
                    best = wsim
    return best


def search_clients(
    query: str,
    limit: int = 30,
    new_limit: int = 15,
    fuzzy: bool = False,
    min_score: float = CLIENT_FUZZY_MIN_SCORE,
) -> dict:
    """Search allowed_clients + client_balances by name / client_id_1c.

    Returns:
        {
            "whitelisted": [
                {id, name, client_id_1c, phone, balance_count,
                 match_type: "exact"|"fuzzy", similarity?: float}
            ],
            "new_1c": [
                {client_name_1c, balance_count, latest_period,
                 match_type: "exact"|"fuzzy", similarity?: float}
            ],
            "fuzzy_count": int,  # total fuzzy hits across both lists
        }

    Whitelisted results are deduplicated by client_id_1c so multi-phone
    siblings roll up into one entry (the first matching allowed_clients row
    is returned as the canonical anchor).

    When fuzzy=True, after the exact LIKE pass, trigram-fill each list up to
    its limit using CLIENT_FUZZY_MIN_SCORE. Fuzzy is skipped for digit-only
    queries (phone fragments / client IDs — trigram on digit strings is
    meaningless) and for queries shorter than 3 chars.
    """
    q = _normalize(query)
    if not q:
        return {"whitelisted": [], "new_1c": [], "fuzzy_count": 0}
    search = f"%{q}%"

    conn = get_db()
    _register_unicode_lower(conn)
    try:
        matches = conn.execute(
            """SELECT ac.id, ac.name, ac.client_id_1c, ac.phone_normalized,
                      (SELECT COUNT(*) FROM client_balances
                       WHERE client_id = ac.id) as bal_count
               FROM allowed_clients ac
               WHERE (LOWER(ac.client_id_1c) LIKE ? OR LOWER(ac.name) LIKE ?
                  OR ac.id IN (
                      SELECT DISTINCT client_id FROM client_balances
                      WHERE LOWER(client_name_1c) LIKE ? AND client_id IS NOT NULL
                  ))
                 AND COALESCE(ac.status, 'active') NOT LIKE 'merged%'
                 AND ac.client_id_1c IS NOT NULL AND ac.client_id_1c != ''
               ORDER BY bal_count DESC
               LIMIT ?""",
            (search, search, search, limit),
        ).fetchall()

        cb_only = conn.execute(
            """SELECT DISTINCT cb.client_name_1c,
                      COUNT(*) as bal_count,
                      MAX(cb.period_end) as latest_period
               FROM client_balances cb
               WHERE LOWER(cb.client_name_1c) LIKE ?
                 AND (cb.client_id IS NULL
                      OR cb.client_id NOT IN (SELECT id FROM allowed_clients))
               GROUP BY cb.client_name_1c
               LIMIT ?""",
            (search, new_limit),
        ).fetchall()

        grouped = OrderedDict()
        for m in matches:
            cid = (m["client_id_1c"] or "").strip()
            key = cid if cid else f"__no1c_{m['id']}"
            if key in grouped:
                continue
            grouped[key] = {
                "id": m["id"],
                "name": m["name"],
                "client_id_1c": m["client_id_1c"],
                "phone": m["phone_normalized"] or "",
                "balance_count": m["bal_count"],
                "match_type": "exact",
            }

        new_1c = [
            {
                "client_name_1c": r["client_name_1c"],
                "balance_count": r["bal_count"],
                "latest_period": r["latest_period"],
                "match_type": "exact",
            }
            for r in cb_only
        ]

        fuzzy_count = 0
        # Skip fuzzy for digit-only queries (phone fragments / client IDs)
        # and very short queries — trigram is meaningless under both conditions.
        if fuzzy and len(q) >= 3 and not q.isdigit():
            variants = _query_variants(q)
            wl_need = limit - len(grouped)
            if wl_need > 0:
                exclude_ids = {entry["id"] for entry in grouped.values()}
                fuzzy_rows = conn.execute(
                    """SELECT ac.id, ac.name, ac.client_id_1c, ac.phone_normalized,
                              (SELECT COUNT(*) FROM client_balances
                               WHERE client_id = ac.id) as bal_count
                       FROM allowed_clients ac
                       WHERE COALESCE(ac.status, 'active') NOT LIKE 'merged%'
                         AND ac.client_id_1c IS NOT NULL AND ac.client_id_1c != ''""",
                ).fetchall()
                scored = []
                for r in fuzzy_rows:
                    if r["id"] in exclude_ids:
                        continue
                    sim = _best_trigram(variants, r["name"] or "", r["client_id_1c"] or "")
                    if sim >= min_score:
                        scored.append((sim, r))
                scored.sort(key=lambda x: -x[0])
                for sim, r in scored[:wl_need]:
                    cid = (r["client_id_1c"] or "").strip()
                    key = cid if cid else f"__no1c_{r['id']}"
                    if key in grouped:
                        continue
                    grouped[key] = {
                        "id": r["id"],
                        "name": r["name"],
                        "client_id_1c": r["client_id_1c"],
                        "phone": r["phone_normalized"] or "",
                        "balance_count": r["bal_count"],
                        "match_type": "fuzzy",
                        "similarity": round(sim, 3),
                    }
                    fuzzy_count += 1

            new_need = new_limit - len(new_1c)
            if new_need > 0:
                exclude_names = {entry["client_name_1c"] for entry in new_1c}
                fuzzy_cb = conn.execute(
                    """SELECT cb.client_name_1c,
                              COUNT(*) as bal_count,
                              MAX(cb.period_end) as latest_period
                       FROM client_balances cb
                       WHERE (cb.client_id IS NULL
                              OR cb.client_id NOT IN (SELECT id FROM allowed_clients))
                       GROUP BY cb.client_name_1c""",
                ).fetchall()
                scored_cb = []
                for r in fuzzy_cb:
                    name = r["client_name_1c"]
                    if not name or name in exclude_names:
                        continue
                    sim = _best_trigram(variants, name)
                    if sim >= min_score:
                        scored_cb.append((sim, r))
                scored_cb.sort(key=lambda x: -x[0])
                for sim, r in scored_cb[:new_need]:
                    new_1c.append({
                        "client_name_1c": r["client_name_1c"],
                        "balance_count": r["bal_count"],
                        "latest_period": r["latest_period"],
                        "match_type": "fuzzy",
                        "similarity": round(sim, 3),
                    })
                    fuzzy_count += 1
    finally:
        conn.close()

    return {
        "whitelisted": list(grouped.values()),
        "new_1c": new_1c,
        "fuzzy_count": fuzzy_count,
    }


def relink_orphan_finance_rows(conn, client_id: int, client_name_1c: str) -> dict:
    """Set client_id on any orphan (client_id IS NULL) rows across the four
    finance tables whose client_name_1c matches this client. Heals data that
    1C imports left unlinked. Returns per-table row counts relinked."""
    counts = {}
    for table in ("client_balances", "real_orders",
                  "client_payments", "client_debts"):
        cur = conn.execute(
            f"UPDATE {table} SET client_id = ? "
            f"WHERE client_name_1c = ? AND client_id IS NULL",
            (client_id, client_name_1c),
        )
        counts[table] = cur.rowcount
    return counts


def heal_finance_orphans_by_1c_name(conn, table: str) -> int:
    """Resolve client_id on orphan rows in one finance table via exact
    client_name_1c → allowed_clients.client_id_1c match. Safe: only
    touches rows where client_id IS NULL; never overwrites an existing
    link. Intended as a post-import cleanup pass to catch rows the
    per-row _try_match_client missed (stale match-cache, late-added
    allowed_clients entries, etc.)."""
    if table not in ("client_balances", "real_orders",
                     "client_payments", "client_debts"):
        raise ValueError(f"not a finance table: {table}")
    cur = conn.execute(
        f"""UPDATE {table} SET client_id = (
                SELECT ac.id FROM allowed_clients ac
                WHERE ac.client_id_1c = {table}.client_name_1c
                  AND COALESCE(ac.status, 'active') NOT LIKE 'merged%'
                ORDER BY ac.id LIMIT 1
            )
            WHERE client_id IS NULL
              AND client_name_1c IN (
                  SELECT client_id_1c FROM allowed_clients
                  WHERE COALESCE(status, 'active') NOT LIKE 'merged%'
              )"""
    )
    return cur.rowcount


def create_and_link_new_1c_client(client_name_1c: str) -> Optional[dict]:
    """Resolve a 1C-only client to an allowed_clients row, creating one if
    missing. Always relinks any orphan finance rows (client_id IS NULL) to
    the resolved id — previously this only ran on the create-new branch,
    leaving clients with existing allowed_clients rows but orphan imports
    silently invisible in the cabinet (reported: Гулноза ойти ТАЙЛОК).

    Returns the {id, name, client_id_1c} or None if client_name_1c has no
    corresponding client_balances rows.
    """
    conn = get_db()
    try:
        cb_exists = conn.execute(
            "SELECT COUNT(*) FROM client_balances WHERE client_name_1c = ?",
            (client_name_1c,),
        ).fetchone()[0]
        if not cb_exists:
            return None

        existing = conn.execute(
            "SELECT id FROM allowed_clients WHERE client_id_1c = ? LIMIT 1",
            (client_name_1c,),
        ).fetchone()
        if existing:
            new_id = existing["id"]
        else:
            conn.execute(
                "INSERT INTO allowed_clients (phone_normalized, name, "
                "client_id_1c, source_sheet, status) "
                "VALUES (?, ?, ?, ?, ?)",
                ("", client_name_1c, client_name_1c, "agent_panel", "active"),
            )
            new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        relink_orphan_finance_rows(conn, new_id, client_name_1c)
        conn.commit()
        return {
            "id": new_id,
            "name": client_name_1c,
            "client_id_1c": client_name_1c,
        }
    finally:
        conn.close()
