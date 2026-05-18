# Soft-delete & status-column vocabulary contract

This document is the canonical registry of **structured status values** used
across the codebase — status strings that carry a variable suffix or
encoded payload, where a naïve `!= 'value'` filter would silently fail open.

The pre-commit hook (`scripts/git-hooks/pre-commit` Guard 6) reads this
file to know which suffix-style status values are already-vetted. Any
**new** suffix-style status value introduced in a commit triggers a
review prompt — see "Adding a new vocabulary entry" below.

The post-mortem in Error Log #56 (`TOMBSTONE_STATUS_FILTER_MISMATCH`)
explains why this contract exists. The 3-day, 198-client, $1k+
exposure incident on 2026-05-15 → 2026-05-18 was the result of
introducing the `merged_into:<id>` vocabulary in the merge tool
without sweeping the 41 readers that filtered with `!= 'merged'`.

---

## Active vocabulary

The pre-commit hook recognises an entry by the sentinel marker
`<!-- vocab-registered: <prefix> -->` immediately after the table row.
Prose examples without that marker do NOT count — they are
illustrative only.

### `allowed_clients.status`

| Value | Format | Writer | Required reader pattern |
|---|---|---|---|
| `'active'` (or NULL) | bare | default; init | `COALESCE(status, 'active') = 'active'` or no filter |
| `'merged_into:<canonical_id>'` | **suffix** | `tools/merge_duplicate_1c_clients.py` (Approach E, commit `2da34ab`, 2026-05-15) | **`COALESCE(status, 'active') NOT LIKE 'merged%'`** — never `!= 'merged'` (exact equality fails open against suffix) |
<!-- vocab-registered: merged_into -->

**Reader-pattern rationale**: the canonical id encoded in the suffix lets
the merge be reversible (`UPDATE … SET status='active'` un-merges). The
trade-off is that the literal value is parameterized — readers that use
exact equality won't recognize it. `NOT LIKE 'merged%'` covers both the
suffix format AND any future bare `'merged'` value if we ever add one.

**Last 41-callsite audit**: 2026-05-18 (commit `0f81ffd`, Session Ops).

### `orders.status`

| Value | Format | Writer | Required reader pattern |
|---|---|---|---|
| `'submitted'` | bare | Mini App order submit | terminal-status exclusion in audit |
| `'confirmed'` | bare | sales group reaction handler | — |
| `'delivered'` | bare | sales/delivery flow + backfill (`backfill_legacy_order_closure`) | excluded from `stuck_orders` audit |
| `'cancelled'` | bare | sales rejection + backfill | excluded from `stuck_orders` audit |

**No suffix format here yet.** Last reviewed: 2026-05-18 (commit `8c48a12`).

### `orders.sales_group_message_id`

| Sentinel | Meaning |
|---|---|
| `NULL` | order has not yet been broadcast to sales group |
| `-1` | sentinel for "post-hoc closure — was fulfilled OOB without broadcast." Set by `backfill_legacy_order_closure` (Session Ops 2026-05-18, applied to orders.id=2 launch-day Azim order). Treat as not-NULL for "stuck" detection. |
| any other integer | actual Telegram message_id of the sales-group post |

---

## Adding a new vocabulary entry

When you introduce a new status value with **any** of:
- a colon (`:`) followed by structured payload — e.g. `'archived_by:<user_id>'`
- a variable suffix — e.g. `'pending:<reason>'`
- a JSON-shaped value — e.g. `'{"flagged_by": …}'`
- any encoded payload that a naïve exact-equality filter would not handle

**You MUST**:

1. **Add a row** to the relevant column's table in this file (writer, format, required reader pattern, last-audit date).
2. **Sweep the codebase** for `!= '<bare_prefix>'` and `= '<bare_prefix>'` callsites against that column. Each one must be migrated to the pattern from your new row, IN THE SAME COMMIT.
3. **Update `backend/tests/test_tombstone_routing.py`** (or the relevant tombstone-aware test) with a regression case for your new value.
4. **Re-run** the pre-commit hook (Guard 6) — it will warn if step 1 was missed.

If you're certain step 2 sweep is unnecessary (e.g., the column is read-only or only one consumer exists), commit with `SKIP_STATUS_VOCABULARY_CHECK=1` after recording the justification in the commit message.

---

## Related Error Log entries

- **#56 `TOMBSTONE_STATUS_FILTER_MISMATCH`** — the origin incident.
- **#46 `WRONG_FALLBACK_FROM_COPY_PASTE`** — sibling pattern at the literal-value level (uncoordinated constants drift between writers and readers).
- **`feedback_column_overload`** memory — sibling at the column-format level (one column overloaded with two formats from different writers).
