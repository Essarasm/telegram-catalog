"""Single source of truth for 1C-name → allowed_clients.id resolution.

Replaces the duplicated `_try_match_client` (was in import_balances + import_debts;
imported by import_cash + import_real_orders) and re-homes the heal pipeline
(was `heal_finance_orphans_by_1c_name` in client_search.py).

Pipeline:
    name_1c → exclusion check → alias canonicalization → exact client_id_1c match
            → LOWER(TRIM(name)) fallback → MatchResult

Layer 3 (insert-time exclusion enforcement across all 4 importers) and Layer 4
(loyalty merge map) plug in through this single chokepoint rather than scattered
across importers.

Module split rationale:
    - pseudo_clients.py — exclusion list (data + query-time SQL helpers)
    - client_search.py — agent-panel partial-name search (different concern)
    - client_identity.py (this) — match + heal + mutator chokepoint
"""
from dataclasses import dataclass
from typing import Optional, Callable, Any, Dict

from backend.services.pseudo_clients import is_pseudo_client


# ── Loyalty merge map (Layer 4 — populate from finances_client_merge_map.md) ──
#
# Empty for now. Populating later requires no importer changes — every caller
# already routes through canonicalize_alias() via the pipeline.
ALIAS_MAP: Dict[str, str] = {}


# ── Match result type ─────────────────────────────────────────────────────────

# Status values: 'matched' | 'excluded' | 'unresolved'
# Distinguishes "intentional NULL" (excluded) from "real bug" (unresolved) so
# importers can count + report them separately. Required for Layer 3.


@dataclass(frozen=True)
class MatchResult:
    """Result of resolve_client_id().

    client_id  — allowed_clients.id if matched, else None
    status     — 'matched' | 'excluded' | 'unresolved'
    """
    client_id: Optional[int]
    status: str

    @property
    def is_matched(self) -> bool:
        return self.status == 'matched'

    @property
    def is_excluded(self) -> bool:
        return self.status == 'excluded'

    @property
    def is_unresolved(self) -> bool:
        return self.status == 'unresolved'


# ── Atomic helpers (exposed for unit tests + edge cases) ──────────────────────


def is_excluded(name: Optional[str]) -> bool:
    """True if `name` is a 1C structural placeholder (Наличка / Организации /
    ИСПРАВЛЕНИЕ / СТРОЙКА / etc.). Delegates to pseudo_clients.is_pseudo_client.
    """
    if not name:
        return False
    return is_pseudo_client(name)


def canonicalize_alias(name: str) -> str:
    """Apply ALIAS_MAP. Returns name unchanged if not in map.

    The merge map captures cross-file divergences confirmed during the loyalty
    rebuild — same client written under different labels in Касса (Субконто 1)
    vs Реализация (Контрагент). See finances_client_merge_map.md for the
    rationale per entry.
    """
    return ALIAS_MAP.get(name, name)


def match_by_client_id_1c(name: str, conn) -> Optional[int]:
    """Exact match on allowed_clients.client_id_1c, skipping merged rows.

    Returns oldest id (deterministic) for multi-phone clients sharing one
    client_id_1c. Cabinet code expands via get_sibling_client_ids() so any
    sibling resolves to all phones' data — picking the oldest is fine.
    """
    row = conn.execute(
        "SELECT id FROM allowed_clients "
        "WHERE client_id_1c = ? "
        "AND COALESCE(status, 'active') != 'merged' "
        "ORDER BY id LIMIT 1",
        (name,),
    ).fetchone()
    return row[0] if row else None


def match_by_name_fallback(name: str, conn) -> Optional[int]:
    """LOWER(TRIM) match on allowed_clients.name, skipping merged.

    Used when client_id_1c is empty or doesn't match. Relies on the Unicode-
    aware LOWER registered globally in backend.database.get_db (per Error Log
    #18 prevention).
    """
    normalized = name.strip().lower()
    row = conn.execute(
        "SELECT id FROM allowed_clients "
        "WHERE LOWER(TRIM(name)) = ? "
        "AND COALESCE(status, 'active') != 'merged' "
        "ORDER BY id LIMIT 1",
        (normalized,),
    ).fetchone()
    return row[0] if row else None


# ── Canonical resolve pipeline (the entry point importers should use) ─────────


def resolve_client_id(name_1c: Optional[str], conn) -> MatchResult:
    """Resolve a 1C client name to allowed_clients.id via the canonical pipeline.

    Pipeline:
        1. None or empty → unresolved
        2. Structural exclusion (is_excluded) → excluded (intentional NULL)
        3. Alias canonicalization (canonicalize_alias)
        4. Exact match on canonical client_id_1c
        5. LOWER(TRIM(name)) fallback
        6. unresolved (real gap — name should be registered)

    Caller distinguishes excluded vs unresolved via MatchResult.status.
    """
    if not name_1c:
        return MatchResult(None, 'unresolved')

    if is_excluded(name_1c):
        return MatchResult(None, 'excluded')

    canonical = canonicalize_alias(name_1c)

    cid = match_by_client_id_1c(canonical, conn)
    if cid is not None:
        return MatchResult(cid, 'matched')

    cid = match_by_name_fallback(canonical, conn)
    if cid is not None:
        return MatchResult(cid, 'matched')

    return MatchResult(None, 'unresolved')


# ── Heal (post-import / post-mutation safety net) ─────────────────────────────

_FINANCE_TABLES = ('client_balances', 'client_payments', 'real_orders', 'client_debts')


def heal_finance_orphans(conn, table: str) -> int:
    """Resolve client_id on `client_id IS NULL` rows in one finance table.

    Idempotent. Safe — only touches NULL, never overwrites a link. Runs in two
    phases:
        1. Bulk SQL UPDATE for direct client_id_1c matches (fast, batch-mode)
        2. Per-alias SQL UPDATE for ALIAS_MAP left-name → canonical mapping
           (only fires when ALIAS_MAP is populated — Layer 4)

    Returns total rows healed.
    """
    if table not in _FINANCE_TABLES:
        raise ValueError(f"not a finance table: {table}")

    # Phase 1: direct exact-match heal (no aliases). Same SQL shape as the
    # original heal_finance_orphans_by_1c_name from client_search.py.
    cur = conn.execute(
        f"""UPDATE {table} SET client_id = (
                SELECT ac.id FROM allowed_clients ac
                WHERE ac.client_id_1c = {table}.client_name_1c
                  AND COALESCE(ac.status, 'active') != 'merged'
                ORDER BY ac.id LIMIT 1
            )
            WHERE client_id IS NULL
              AND client_name_1c IN (
                  SELECT client_id_1c FROM allowed_clients
                  WHERE COALESCE(status, 'active') != 'merged'
              )"""
    )
    healed = cur.rowcount

    # Phase 2: alias-aware heal (no-op when ALIAS_MAP empty).
    if ALIAS_MAP:
        for left_name, canonical in ALIAS_MAP.items():
            cur2 = conn.execute(
                f"""UPDATE {table} SET client_id = (
                        SELECT ac.id FROM allowed_clients ac
                        WHERE ac.client_id_1c = ?
                          AND COALESCE(ac.status, 'active') != 'merged'
                        ORDER BY ac.id LIMIT 1
                    )
                    WHERE client_id IS NULL AND client_name_1c = ?""",
                (canonical, left_name),
            )
            healed += cur2.rowcount

    return healed


def heal_all_finance_tables(conn) -> Dict[str, int]:
    """Heal across all 4 finance tables. Returns per-table heal counts."""
    return {t: heal_finance_orphans(conn, t) for t in _FINANCE_TABLES}


# ── Mutator chokepoint ────────────────────────────────────────────────────────


def mutate_allowed_clients_then_heal(
    conn,
    mutate_fn: Callable[..., Any],
    *args,
    **kwargs,
) -> Dict[str, Any]:
    """Run a function that mutates allowed_clients, then heal all 4 finance
    tables in the same transaction.

    Use from any path that touches allowed_clients (import_clients,
    apply_client_master_*, future mutators). Heal runs before commit so the
    mutation and heal are atomic from the DB's perspective.

    Caller is responsible for conn.commit() — wrapper does NOT commit.

    Args:
        conn:        open SQLite connection (caller commits/closes)
        mutate_fn:   callable; first arg must be `conn`
        *args, **kw: additional args passed to mutate_fn after conn

    Returns:
        {
            'mutate_result':   whatever mutate_fn returned,
            'orphans_healed':  {table_name: rows_healed},
        }
    """
    mutate_result = mutate_fn(conn, *args, **kwargs)
    orphans_healed = heal_all_finance_tables(conn)
    return {
        'mutate_result': mutate_result,
        'orphans_healed': orphans_healed,
    }
