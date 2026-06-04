"""Nightly data-consistency audit. Scans for drift conditions that silently
erode data quality over time, and returns a summary dict the scheduler posts
to the Admin group if anything worth attention is found.

Checks performed:
  1. Phone duplicates — same phone_normalized on >1 active allowed_clients row
  2. Orphaned real_orders — client_name_1c has no matching allowed_clients row
  3. Silent client_id_1c gaps — allowed_clients rows with recent activity
     but empty client_id_1c (blocks scoring + cashback + Master sync)
  4. Stale needs_review — flagged rows older than 30 days (auto-archive idea)
  5. Stuck orders — sales_group_message_id IS NULL older than 24 hours
     (the /resendmissed target — means notification failed silently)
  6. Phone history age — recent phone changes for awareness

Cheap: all queries are COUNT/INDEX-backed. Runs nightly via
bot/reminders.py `_run_consistency_audit`.
"""
from __future__ import annotations

from backend.database import get_db
from backend.services.pseudo_clients import (
    sql_exclusion_clause,
    sql_exclusion_params,
)


def run_audit(fix: bool = False) -> dict:
    """Returns a dict of findings. Keys are check names, values are dicts
    with at least {"count": N, "sample": [...]} when count > 0. Empty
    findings are omitted entirely so the caller can check `if result:` to
    decide whether to notify.

    When `fix=True`, the audit also auto-heals `healable_orphans` via
    `client_identity.heal_all_finance_tables()` before recording the
    finding. The reported count then reflects what was healed in this run
    (so persistent residuals after fix indicate a real new class of orphan
    that the heal SQL can't resolve — worth investigating).

    Default `fix=False` keeps backward compatibility for `/consistencycheck`
    on-demand admin command (alert-only). The 09:00 cron passes `fix=True`
    as a daily safety net layered on top of the per-mutator heal (Session F
    refactor phase 8)."""
    conn = get_db()
    result: dict = {}
    try:
        # 1. Phone duplicates (same phone_normalized on >1 active row)
        dups = conn.execute(
            """SELECT phone_normalized, COUNT(*) AS n,
                      GROUP_CONCAT(id || ':' || COALESCE(client_id_1c, name, '?'), ' | ') AS rows
               FROM allowed_clients
               WHERE phone_normalized != ''
                 AND COALESCE(status, 'active') = 'active'
               GROUP BY phone_normalized
               HAVING COUNT(*) > 1
               ORDER BY n DESC
               LIMIT 20"""
        ).fetchall()
        if dups:
            result["phone_duplicates"] = {
                "count": len(dups),
                "sample": [dict(r) for r in dups[:5]],
            }

        # 2. Orphaned real_orders — clients in real_orders but not in
        # allowed_clients. Pseudo-clients (Наличка / supplier-bonus / etc.)
        # are NOT real customers and are filtered upstream — without this
        # exclusion the audit re-flags them every night even though they're
        # already canonically listed in pseudo_clients.SYSTEM_NON_CLIENT_NAMES.
        pseudo_clause = sql_exclusion_clause("ro.client_name_1c")
        orphans = conn.execute(
            f"""SELECT client_name_1c, COUNT(*) AS n
               FROM real_orders ro
               WHERE ro.client_name_1c != ''
                 AND ro.client_id IS NULL
                 AND {pseudo_clause}
                 AND NOT EXISTS (
                   SELECT 1 FROM allowed_clients ac
                   WHERE ac.client_id_1c = ro.client_name_1c
                 )
               GROUP BY client_name_1c
               ORDER BY n DESC
               LIMIT 20""",
            sql_exclusion_params(),
        ).fetchall()
        if orphans:
            result["orphaned_real_orders"] = {
                "count": len(orphans),
                "sample": [dict(r) for r in orphans[:5]],
            }

        # 3. Silent client_id_1c gaps
        gaps = conn.execute(
            """SELECT ac.id, ac.name, ac.phone_normalized
               FROM allowed_clients ac
               WHERE (ac.client_id_1c IS NULL OR ac.client_id_1c = '')
                 AND COALESCE(ac.status, 'active') = 'active'
                 AND EXISTS (
                   SELECT 1 FROM real_orders ro
                   WHERE ro.client_id = ac.id
                     AND ro.doc_date >= date('now', '-180 days')
                 )
               ORDER BY ac.id
               LIMIT 20"""
        ).fetchall()
        if gaps:
            result["active_no_1c_name"] = {
                "count": len(gaps),
                "sample": [dict(r) for r in gaps[:5]],
            }

        # 4. Stale needs_review (flagged ≥30 days ago, still flagged)
        stale = conn.execute(
            """SELECT COUNT(*) AS n FROM allowed_clients
               WHERE (needs_review = 1 OR needs_verification = 1)
                 AND (last_master_synced_at IS NULL
                      OR last_master_synced_at < datetime('now', '-30 days'))"""
        ).fetchone()
        if stale and stale["n"]:
            result["stale_needs_review"] = {"count": stale["n"]}

        # 5. Stuck orders — never reached Sales group, older than 24h.
        # Exclude terminal statuses: cancelled (rejected) and delivered
        # (fulfilled — closed business, not stuck). The sentinel
        # sales_group_message_id=-1 is the "post-hoc closure" marker for
        # legacy orders that were fulfilled OOB before the broadcast was
        # wired up; treat as not-NULL so they don't surface here either.
        stuck = conn.execute(
            """SELECT id, client_name, created_at
               FROM orders
               WHERE sales_group_message_id IS NULL
                 AND created_at < datetime('now', '-1 day')
                 AND status NOT IN ('cancelled', 'delivered')
               ORDER BY id DESC
               LIMIT 20"""
        ).fetchall()
        if stuck:
            result["stuck_orders"] = {
                "count": len(stuck),
                "sample": [dict(r) for r in stuck[:5]],
            }

        # 6. Recent phone changes (last 7d), classified by direction so a
        #    parser-corruption wave reads as the incident it is, not benign
        #    churn. invalid->valid = correction, valid->valid = reshuffle,
        #    valid->invalid = CORRUPTION (the 2026-06 MULTI_PHONE_CELL_MISALIGNMENT
        #    class, Error Log). System repairs (backfill_repair_*) are excluded —
        #    this metric watches the IMPORT pipeline, not our own repair tool.
        from backend.services.import_clients import is_valid_uz_mobile
        ph_rows = conn.execute(
            """SELECT client_id, old_phone, new_phone FROM phone_history
               WHERE changed_at >= datetime('now', '-7 days')
                 AND reason NOT LIKE 'backfill_repair%'"""
        ).fetchall()
        if ph_rows:
            corr = resh = corrupt = 0
            corrupt_sample = []
            for r in ph_rows:
                old_ok = is_valid_uz_mobile(r["old_phone"])
                new_ok = is_valid_uz_mobile(r["new_phone"])
                if old_ok and not new_ok:
                    corrupt += 1
                    if len(corrupt_sample) < 3:
                        corrupt_sample.append({
                            "client_id": r["client_id"],
                            "old": r["old_phone"], "new": r["new_phone"],
                        })
                elif not old_ok and new_ok:
                    corr += 1
                elif old_ok and new_ok:
                    resh += 1
            result["recent_phone_changes_7d"] = {
                "count": len(ph_rows),
                "corrections": corr,
                "reshuffles": resh,
                "corruption": corrupt,
                "sample": corrupt_sample,
            }

        # 7. DB size snapshot for trend tracking. `clients` counts only
        # active rows so the daily summary reflects the canonical client
        # base (~2.2k post-dedup), not the soft-deleted tombstone count
        # (~38.7k merged rows from the May 2026 dedup migration).
        size_row = conn.execute(
            """SELECT
                 (SELECT COUNT(*) FROM allowed_clients
                   WHERE COALESCE(status,'active')='active') AS clients,
                 (SELECT COUNT(*) FROM real_orders) AS real_orders,
                 (SELECT COUNT(*) FROM product_interest_clicks) AS interest_clicks,
                 (SELECT COUNT(*) FROM search_logs) AS searches"""
        ).fetchone()
        if size_row:
            result["_table_sizes"] = dict(size_row)

        # 8. Heal-eligible orphans per finance table — rows with client_id IS
        # NULL whose client_name_1c DOES match an allowed_clients.client_id_1c.
        # Per-mutator heal (Session F phase 8) should drive these to 0; any
        # non-zero count means a new class of orphan is escaping the heal
        # path. When fix=True, we run the heal here as a defense-in-depth
        # safety net (09:00 cron) and report what got healed.
        if fix:
            from backend.services import client_identity
            healed = client_identity.heal_all_finance_tables(conn)
            conn.commit()
            healable = {t: n for t, n in healed.items() if n}
            if healable:
                result["healable_orphans"] = {
                    "count": sum(healable.values()),
                    "by_table": healable,
                    "auto_healed": True,
                }
        else:
            healable = {}
            for table in ("client_balances", "real_orders",
                          "client_payments", "client_debts"):
                row = conn.execute(
                    f"""SELECT COUNT(*) AS n FROM {table} t
                        WHERE t.client_id IS NULL
                          AND t.client_name_1c IN (
                              SELECT client_id_1c FROM allowed_clients
                              WHERE COALESCE(status, 'active') NOT LIKE 'merged%'
                                AND client_id_1c IS NOT NULL
                                AND client_id_1c != ''
                          )"""
                ).fetchone()
                if row and row["n"]:
                    healable[table] = row["n"]
            if healable:
                result["healable_orphans"] = {
                    "count": sum(healable.values()),
                    "by_table": healable,
                }

        # 8b. Tombstone-pointer orphans — rows whose client_id points at a
        # soft-merged (status LIKE 'merged_into:%') allowed_clients row.
        # Error Log #56 second-order finding: pre-fix importers wrote new
        # rows onto tombstones for ~3 days; the new heal logic (phase 3)
        # auto-resolves these, so any non-zero count here means a new
        # mutator path is still landing on tombstones.
        tombstoned = {}
        for table in ("client_balances", "real_orders",
                      "client_payments", "client_debts"):
            row = conn.execute(
                f"""SELECT COUNT(*) AS n FROM {table} t
                    WHERE t.client_id IN (
                        SELECT id FROM allowed_clients
                         WHERE status LIKE 'merged_into:%'
                    )"""
            ).fetchone()
            if row and row["n"]:
                tombstoned[table] = row["n"]
        if tombstoned:
            result["tombstoned_fk_pointers"] = {
                "count": sum(tombstoned.values()),
                "by_table": tombstoned,
            }

        # 9. Debt-USD coverage collapse — if client_debts has significant
        # row count but zero USD anywhere, the 1C report probably lost its
        # «В Валюте» column again (Error Log #20). Phase 1 blocks this at
        # import-time; the audit is a second safety net in case someone
        # runs `/debtors force` inadvertently.
        debt_row = conn.execute(
            """SELECT COUNT(*) AS rows,
                      COALESCE(SUM(debt_usd), 0) AS usd_total,
                      SUM(CASE WHEN debt_usd > 0 THEN 1 ELSE 0 END) AS usd_rows
               FROM client_debts"""
        ).fetchone()
        if debt_row and debt_row["rows"] >= 50 and debt_row["usd_rows"] == 0:
            result["debt_usd_coverage_zero"] = {
                "rows": debt_row["rows"],
                "usd_total": float(debt_row["usd_total"] or 0),
                "usd_rows": debt_row["usd_rows"],
            }

        # 10. Trend-coverage drift — clients who historically had BOTH UZS
        # and USD shipments but in the last 6 months one currency series went
        # silent. This is the 2026-04-23 regression class: the Cabinet's
        # spend-trend chart hides the silent currency, and the client thinks
        # their history vanished. False-positive rate: low — flagged client
        # had real history in the missing currency, so a true zero is rare.
        trend_drift = conn.execute(
            """WITH recent AS (
                 SELECT ro.client_id,
                        COUNT(DISTINCT ro.id) AS doc_count_recent,
                        SUM(COALESCE(ri.total_local, 0))    AS uzs_recent,
                        SUM(COALESCE(ri.total_currency, 0)) AS usd_recent
                   FROM real_orders ro
                   JOIN real_order_items ri ON ri.real_order_id = ro.id
                  WHERE ro.client_id IS NOT NULL
                    AND ro.doc_date >= date('now', '-180 days')
                  GROUP BY ro.client_id
               ),
               earlier AS (
                 SELECT ro.client_id,
                        SUM(COALESCE(ri.total_local, 0))    AS uzs_earlier,
                        SUM(COALESCE(ri.total_currency, 0)) AS usd_earlier
                   FROM real_orders ro
                   JOIN real_order_items ri ON ri.real_order_id = ro.id
                  WHERE ro.client_id IS NOT NULL
                    AND ro.doc_date <  date('now', '-180 days')
                  GROUP BY ro.client_id
               )
               SELECT r.client_id,
                      ac.client_id_1c,
                      r.doc_count_recent,
                      CASE WHEN r.uzs_recent = 0 THEN 'UZS' ELSE 'USD' END AS missing_currency,
                      ROUND(COALESCE(e.uzs_earlier, 0)) AS prior_uzs,
                      ROUND(COALESCE(e.usd_earlier, 0), 2) AS prior_usd
                 FROM recent r
            LEFT JOIN earlier e         ON e.client_id = r.client_id
            LEFT JOIN allowed_clients ac ON ac.id      = r.client_id
                WHERE r.doc_count_recent >= 10
                  AND (
                        (r.uzs_recent = 0 AND r.usd_recent > 0
                         AND COALESCE(e.uzs_earlier, 0) > 0)
                     OR (r.usd_recent = 0 AND r.uzs_recent > 0
                         AND COALESCE(e.usd_earlier, 0) > 0)
                      )
                ORDER BY r.doc_count_recent DESC
                LIMIT 20"""
        ).fetchall()
        if trend_drift:
            result["trend_currency_drift"] = {
                "count": len(trend_drift),
                "sample": [dict(r) for r in trend_drift[:5]],
            }

        # 11. Genuine duplicate allowed_clients rows — same client_id_1c AND a
        # SHARED PHONE across ≥2 sibling rows. The shared phone is the only
        # reliable signature of "one real person with two rows"; the merge
        # tool resolves these.
        #
        # Definitive principle (Alisher + Ulugbek, 2026-06-01, Error Log #67):
        # client_id_1c is a 1C *name label*, NOT a unique key. Many genuinely
        # DIFFERENT shops legitimately share a name ("two shops literally next
        # to each other"). A same-name cluster with all-distinct phones is
        # therefore NOT a duplicate and must never be flagged — that was the
        # 380-cluster daily false-alarm (audit also under-reported it: the old
        # query's `count` was `len()` of a `LIMIT 20` fetch, Error Log #56).
        #
        # We cluster + phone-test in Python because SQLite's LOWER() is
        # ASCII-only (won't lowercase Cyrillic) and the per-cluster phone
        # cross-match isn't expressible cheaply in one SQL pass.
        from collections import defaultdict

        from backend.services.client_identity_reviewed import (
            CONFIRMED_DISTINCT_SHARED_NAMES,
            normalize_1c,
        )

        ac_rows = conn.execute(
            """SELECT id, client_id_1c, phone_normalized, raqam_02, raqam_03
               FROM allowed_clients
               WHERE COALESCE(status, 'active') NOT LIKE 'merged%'
                 AND client_id_1c IS NOT NULL AND client_id_1c != ''"""
        ).fetchall()
        by_name: dict[str, list] = defaultdict(list)
        for r in ac_rows:
            by_name[normalize_1c(r["client_id_1c"])].append(r)

        flagged = []
        for norm, members in by_name.items():
            if len(members) < 2:
                continue
            # Confirmed legitimate multi-shop name-collision — never flag.
            if norm in CONFIRMED_DISTINCT_SHARED_NAMES:
                continue
            # Phone cross-match: does any phone appear on >1 sibling row?
            owners: dict[str, set] = defaultdict(set)
            for m in members:
                for ph in (m["phone_normalized"], m["raqam_02"], m["raqam_03"]):
                    ph = (str(ph).strip() if ph is not None else "")
                    if ph:
                        owners[ph].add(m["id"])
            if any(len(ids) >= 2 for ids in owners.values()):
                flagged.append((norm, members))

        if flagged:
            flagged.sort(key=lambda x: -len(x[1]))
            result["fuzzy_client_1c_dups"] = {
                "count": len(flagged),  # TRUE count (no LIMIT cap — Error Log #56)
                "sample": [
                    {
                        "norm": norm,
                        "n": len(members),
                        "rows": " | ".join(
                            f"{m['id']}:{m['client_id_1c']}" for m in members[:4]
                        ),
                    }
                    for norm, members in flagged[:5]
                ],
            }

        # 12. Identity-drift holds awaiting manual resolution (#74). The
        # import_clients drift-guard parks phone-match upserts that would
        # rewrite client_id_1c on a curated-state row. Unresolved rows here
        # mean a real client's daily 1C row is being held out — needs a human
        # to accept the rename or keep the existing identity. Table may not
        # exist on a not-yet-migrated DB, so guard the query.
        try:
            held = conn.execute(
                """SELECT allowed_client_id, existing_client_id_1c,
                          incoming_client_id_1c, curated_state
                   FROM client_identity_drift_queue
                   WHERE resolved = 0
                   ORDER BY detected_at DESC
                   LIMIT 20"""
            ).fetchall()
        except Exception:
            held = []
        if held:
            result["identity_drift_held"] = {
                "count": len(held),
                "sample": [dict(r) for r in held[:5]],
            }

        # 13. Mislinked users — a client-role (non-agent) approved user whose
        # phone matches a DIFFERENT *real* client than the one their account is
        # linked to. The Турдиев class (2026-06-02, Error Log): a manual
        # registration mispick (sales team replies with the wrong client) or an
        # import phone-collision attaches a Telegram account to the wrong shop,
        # so the user's orders / cabinet / location pins all flow to that wrong
        # shop (the users.client_id blast radius). Excludes: agents (they
        # legitimately repoint client_id via the act-as / switch-client flow),
        # bot_approved placeholder rows (client_id_1c NULL — e.g. a tester's
        # self-registration), merged/tombstoned rows, and siblings sharing the
        # same client_id_1c (multi-row clients). substr(...,-9) is the cheap
        # last-9-digit normalization; tools/high_conf_mislinks.py is the
        # authoritative offline version that also handles dirty phone strings.
        mislinked = conn.execute(
            """SELECT u.telegram_id, u.first_name,
                      u.client_id AS linked_id, linked.name AS linked_name,
                      m.id AS should_be_id, m.name AS should_be_name
               FROM users u
               JOIN allowed_clients linked ON linked.id = u.client_id
               JOIN allowed_clients m
                 ON substr(u.phone, -9) IN (m.phone_normalized, m.raqam_02, m.raqam_03)
               WHERE u.is_approved = 1
                 AND u.is_agent = 0
                 AND u.client_id IS NOT NULL
                 AND u.phone IS NOT NULL AND u.phone != ''
                 AND m.id != u.client_id
                 AND COALESCE(m.status, 'active') NOT LIKE 'merged%'
                 AND m.client_id_1c IS NOT NULL AND m.client_id_1c != ''
                 AND (linked.client_id_1c IS NULL
                      OR linked.client_id_1c != m.client_id_1c)
               ORDER BY u.telegram_id
               LIMIT 20"""
        ).fetchall()
        if mislinked:
            result["mislinked_users"] = {
                "count": len(mislinked),
                "sample": [dict(r) for r in mislinked[:5]],
            }

        # 14. Misattributed debt comments — `client_callbacks` (the debt-tab
        # comment/callback history) is keyed by `client_name_1c` (a non-unique,
        # mutable 1C name label), never by a stable client_id / allowed_clients.id.
        # That makes it a latent member of the identity family (Error Log #75:
        # client_id_1c is a name, not a key). Two failure shapes surface here:
        #   - commingled: one name shared by ≥2 distinct active shops (different
        #     phones) that both have comments → their histories merge silently.
        #   - orphaned: a comment's name matches no active client (rename / #74
        #     drift) → the comment is lost, or worse, attaches to whoever inherits
        #     the old name next.
        # Detection-only tripwire; the durable fix is to add a client_id FK to
        # client_callbacks + remap it in the merge tool. Table may be absent on a
        # not-yet-migrated DB, so guard the query.
        try:
            bad_cb = conn.execute(
                """SELECT 'commingled' AS kind, cc.client_name_1c AS name,
                          COUNT(*) AS callbacks
                     FROM client_callbacks cc
                    WHERE (SELECT COUNT(DISTINCT ac.phone_normalized)
                             FROM allowed_clients ac
                            WHERE ac.client_id_1c = cc.client_name_1c
                              AND COALESCE(ac.status,'active') NOT LIKE 'merged%'
                              AND ac.phone_normalized != '') > 1
                    GROUP BY cc.client_name_1c
                   UNION ALL
                   SELECT 'orphaned' AS kind, cc.client_name_1c AS name,
                          COUNT(*) AS callbacks
                     FROM client_callbacks cc
                    WHERE NOT EXISTS (
                            SELECT 1 FROM allowed_clients ac
                             WHERE ac.client_id_1c = cc.client_name_1c
                               AND COALESCE(ac.status,'active') NOT LIKE 'merged%')
                    GROUP BY cc.client_name_1c
                    ORDER BY callbacks DESC
                    LIMIT 20"""
            ).fetchall()
        except Exception:
            bad_cb = []
        if bad_cb:
            result["callbacks_misattributed"] = {
                "count": len(bad_cb),
                "sample": [dict(r) for r in bad_cb[:5]],
            }

        # 15. Pending 1C adoption (Client Identity Anchoring Phase 3). A
        # user-/agent-/admin-registered row is a "pending" row until the daily
        # 1C import adopts it (phone-matches the card and stamps onec_card_id).
        # Adoption is self-healing AS LONG AS the registration phone equals the
        # 1C card's phone. When they differ (or no 1C card is ever created), the
        # row sits card-less forever while 1C inserts its own row — a silent
        # stranded duplicate. The daily import runs every morning, so any
        # registration-sourced, card-less, active row older than 14 days has
        # demonstrably NOT been adopted → surface it for an admin to create/link
        # the 1C card. Date proxy = the linked user's registered_at (every
        # registration write path sets matched_telegram_id).
        pending = conn.execute(
            """SELECT ac.id, ac.name, ac.phone_normalized, ac.source_sheet,
                      u.registered_at
                 FROM allowed_clients ac
                 JOIN users u ON u.telegram_id = ac.matched_telegram_id
                WHERE COALESCE(ac.status, 'active') = 'active'
                  AND (ac.onec_card_id IS NULL OR ac.onec_card_id = '')
                  AND ac.source_sheet IN (
                        'bot_new_client', 'bot_approved', 'bot_linked',
                        'agent_panel', 'admin_panel', 'bot_added')
                  AND u.registered_at IS NOT NULL
                  AND u.registered_at < datetime('now', '-14 days')
                ORDER BY u.registered_at
                LIMIT 20"""
        ).fetchall()
        if pending:
            result["pending_onec_adoption"] = {
                "count": len(pending),
                "sample": [dict(r) for r in pending[:5]],
            }

    finally:
        conn.close()
    return result


# ── Severity thresholds ────────────────────────────────────────────────
# Each finding's count gets mapped to CRITICAL / WARNING / INFO using
# these tier-boundaries. The 2026-05-18 incident (Error Log #56) was a
# 329-row accumulation that was reported with the same visual weight as
# benign 2-row edge cases for 3 days. With tiers, the same alert would
# have rendered as 🔴 CRITICAL — impossible to tune out.
SEVERITY_THRESHOLDS = {
    # finding-key: (critical-at, warning-at)
    "phone_duplicates":         (10, 1),
    "orphaned_real_orders":     (10, 1),
    "active_no_1c_name":        (5, 1),
    "stale_needs_review":       (20, 5),
    "stuck_orders":             (1, 1),    # any stuck order is at least warning
    "healable_orphans":         (50, 1),
    "tombstoned_fk_pointers":   (10, 1),   # this is the #56 class — alarm early
    "debt_usd_coverage_zero":   (1, 1),    # schema-drift class, always critical
    "trend_currency_drift":     (5, 1),
    "fuzzy_client_1c_dups":     (10, 1),
    "identity_drift_held":      (5, 1),    # #74 — any held drift needs a human
    "mislinked_users":          (3, 1),    # Турдиев class — any wrong link needs a human
    "callbacks_misattributed":  (3, 1),    # name-keyed debt comments — commingled/orphaned
    "pending_onec_adoption":    (20, 1),   # registration rows the 1C import never adopted (phone mismatch / no card)
    # Informational only — never critical
    "recent_phone_changes_7d":  (None, None),
}


def _severity(key: str, item: dict) -> str:
    """Map a finding to CRITICAL / WARNING / INFO using its row count."""
    n = item.get("count", 0)
    # Phone-change severity is driven by the CORRUPTION sub-count, not the raw
    # total: a benign correction/reshuffle wave can be large and harmless, but
    # even one valid->invalid corruption is a parser regression worth a human.
    if key == "recent_phone_changes_7d":
        corrupt = item.get("corruption", 0)
        if corrupt >= 10:
            return "CRITICAL"
        if corrupt >= 1:
            return "WARNING"
        return "INFO"
    if key in SEVERITY_THRESHOLDS:
        crit, warn = SEVERITY_THRESHOLDS[key]
        if crit is None:
            return "INFO"
        if n >= crit:
            return "CRITICAL"
        if n >= warn:
            return "WARNING"
    return "INFO"


def _spike_marker(key: str, item: dict, prior: dict | None) -> str:
    """Return ' 📈 SPIKE +N' if this finding grew sharply since prior run.

    A spike is +10 rows AND ≥50% growth (so going from 2→3 doesn't fire
    but 329→339 from yesterday's run would). Empty string otherwise.
    """
    if not prior or key not in prior:
        return ""
    now = item.get("count", 0)
    then = prior.get(key, {}).get("count", 0) if isinstance(prior.get(key), dict) else 0
    delta = now - then
    if delta >= 10 and (then == 0 or delta / then >= 0.5):
        return f" 📈 <b>SPIKE +{delta}</b>"
    return ""


def _snapshot_path() -> str:
    """Filesystem path for yesterday's findings snapshot.

    Lives in /data/ (Railway persistent volume) so the snapshot survives
    deploys. Simple JSON; one file, overwritten each successful run.
    """
    import os
    base = os.environ.get("DATABASE_PATH", "./data/catalog.db")
    return os.path.join(os.path.dirname(base) or ".", "audit_snapshot.json")


def load_prior_snapshot() -> dict | None:
    """Read yesterday's audit findings if available. Failures are silent —
    spike detection just falls back to None (no markers shown)."""
    import json
    try:
        with open(_snapshot_path()) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def save_snapshot(findings: dict) -> None:
    """Persist today's findings for tomorrow's spike comparison.

    Strips _table_sizes (changes every minute, not useful for spike
    detection) and the sample arrays (only counts matter for diffs).
    """
    import json
    snapshot = {}
    for k, v in findings.items():
        if k.startswith("_"):
            continue
        if isinstance(v, dict):
            snapshot[k] = {"count": v.get("count", 0)}
    try:
        with open(_snapshot_path(), "w") as fh:
            json.dump(snapshot, fh)
    except OSError:
        pass  # don't break the audit on snapshot-write failure


def format_audit_message(findings: dict, prior_findings: dict | None = None) -> str | None:
    """Render findings into a Telegram-ready HTML message. Returns None
    if nothing to report (all checks passed).

    Each finding is tiered into one of three severities (CRITICAL,
    WARNING, INFO) using thresholds from SEVERITY_THRESHOLDS below. The
    Telegram output prefixes each finding with the tier emoji so 329-row
    surges visually outrank 2-row edge-cases — preventing the
    signal-fatigue that caused the 2026-05-15→18 incident (Error Log #56)
    to be tuned out as background noise for 3 days.

    When prior_findings is supplied (typically yesterday's snapshot), any
    finding that grew by ≥10 rows OR ≥50% gets a 📈 SPIKE marker. This
    is the diff-based detection that would have caught the 329-cluster
    accumulation on day 2 of the May incident.
    """
    # Strip _table_sizes (informational only, always present)
    significant = {k: v for k, v in findings.items() if not k.startswith("_")}
    if not significant:
        return None
    lines = ["🩺 <b>Data Consistency Audit</b>\n"]

    issue_labels = {
        "phone_duplicates":        "📞 Telefon dublikat",
        "orphaned_real_orders":    "🔗 Bog'lanmagan buyurtmalar (1C nomi mavjud emas)",
        "active_no_1c_name":       "⚠️ Faol mijoz + 1C nomi yo'q",
        "stale_needs_review":      "🕸 Eskirgan needs_review (>30 kun)",
        "stuck_orders":            "🚫 Sales guruhga yetmagan buyurtmalar (>24s)",
        "recent_phone_changes_7d": "📱 Oxirgi 7 kunda telefon o'zgarishi",
        "healable_orphans":        "🧩 Healable orphan qatorlar (client_id NULL, 1C nomi mavjud)",
        "tombstoned_fk_pointers":  "💀 Tombstone'ga ko'rsatuvchi finance qatorlari (merged_into rowga FK)",
        "debt_usd_coverage_zero":  "💵 client_debts: USD butunlay yo'q (В Валюте column?)",
        "trend_currency_drift":    "📉 Cabinet trend: valyuta yo'qoldi (oldin bor edi, oxirgi 6 oyda yo'q)",
        "fuzzy_client_1c_dups":    "👥 client_id_1c dublikat (bir xil telefon — birlashtirish kerak)",
        "identity_drift_held":     "🛑 Identity drift ushlab turilibdi (1C nomi o'zgargan — qo'lda hal qiling)",
        "mislinked_users":         "🔗 Mijoz noto'g'ri ulangan (foydalanuvchi telefoni boshqa mijozga tegishli)",
        "callbacks_misattributed": "📝 Qarz izohlari noto'g'ri biriktirilgan (nom bo'yicha — umumiy nom yoki yetim)",
        "pending_onec_adoption":   "⏳ Ro'yxatdan o'tgan, 1C kartasi biriktirilmagan (14+ kun — 1C karta yarating/ulang)",
    }

    # Group findings by severity. Render CRITICAL first so the most
    # urgent items aren't buried below benign INFO findings.
    grouped: dict[str, list[tuple[str, str, dict]]] = {
        "CRITICAL": [], "WARNING": [], "INFO": [],
    }
    for key, label in issue_labels.items():
        if key in findings:
            item = findings[key]
            tier = _severity(key, item)
            grouped[tier].append((key, label, item))

    for tier, prefix in (("CRITICAL", "🔴"), ("WARNING", "🟡"), ("INFO", "🔵")):
        for key, label, item in grouped[tier]:
            # Healable orphans surface auto-heal status so the cron's report
            # distinguishes "we just fixed N rows" from "alert: N rows need
            # investigation" (the on-demand /consistencycheck path).
            if key == "healable_orphans" and item.get("auto_healed"):
                label = label + " — auto-healed ✓"
            n = item.get("count", 0)
            spike_marker = _spike_marker(key, item, prior_findings)
            lines.append(f"{prefix} <b>{label}:</b> {n}{spike_marker}")
            # Phone changes: show the direction breakdown so corruption is
            # never hidden inside a benign-looking total.
            if key == "recent_phone_changes_7d":
                lines.append(
                    f"   ✅ to'g'rilangan: {item.get('corrections', 0)} · "
                    f"🔄 almashtirilgan: {item.get('reshuffles', 0)} · "
                    f"🛑 buzilgan: {item.get('corruption', 0)}"
                )
            sample = item.get("sample") or []
            for s in sample[:3]:
                # Compact sample rendering
                bits = []
                for k, v in s.items():
                    if v in (None, ""):
                        continue
                    bits.append(f"{k}={v}")
                line = " · ".join(bits)
                lines.append(f"   <code>{line[:120]}</code>")
            if len(sample) > 3:
                lines.append(f"   <i>... va yana {n - 3}</i>")
            lines.append("")

    sz = findings.get("_table_sizes") or {}
    if sz:
        lines.append("<b>Jadval hajmlari:</b>")
        lines.append(
            f"   clients={sz.get('clients', '?')} · "
            f"real_orders={sz.get('real_orders', '?')} · "
            f"interest_clicks={sz.get('interest_clicks', '?')} · "
            f"searches={sz.get('searches', '?')}"
        )

    lines.append("")
    lines.append("💡 <code>/reviewclients</code> — flaglarni yechish. "
                 "<code>/resendmissed</code> — yetmagan buyurtmalarni qayta yuborish.")

    return "\n".join(lines)
