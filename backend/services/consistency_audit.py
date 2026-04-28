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

        # 2. Orphaned real_orders — clients in real_orders but not in allowed_clients
        orphans = conn.execute(
            """SELECT client_name_1c, COUNT(*) AS n
               FROM real_orders ro
               WHERE ro.client_name_1c != ''
                 AND ro.client_id IS NULL
                 AND NOT EXISTS (
                   SELECT 1 FROM allowed_clients ac
                   WHERE ac.client_id_1c = ro.client_name_1c
                 )
               GROUP BY client_name_1c
               ORDER BY n DESC
               LIMIT 20"""
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

        # 5. Stuck orders — never reached Sales group, older than 24h
        stuck = conn.execute(
            """SELECT id, client_name, created_at
               FROM orders
               WHERE sales_group_message_id IS NULL
                 AND created_at < datetime('now', '-1 day')
                 AND status != 'cancelled'
               ORDER BY id DESC
               LIMIT 20"""
        ).fetchall()
        if stuck:
            result["stuck_orders"] = {
                "count": len(stuck),
                "sample": [dict(r) for r in stuck[:5]],
            }

        # 6. Recent phone changes (informational)
        recent_phones = conn.execute(
            """SELECT COUNT(*) AS n FROM phone_history
               WHERE changed_at >= datetime('now', '-7 days')"""
        ).fetchone()
        if recent_phones and recent_phones["n"]:
            result["recent_phone_changes_7d"] = {"count": recent_phones["n"]}

        # 7. DB size snapshot for trend tracking
        size_row = conn.execute(
            """SELECT
                 (SELECT COUNT(*) FROM allowed_clients) AS clients,
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
                              WHERE COALESCE(status, 'active') != 'merged'
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

        # 11. Fuzzy-duplicate allowed_clients — same client_id_1c modulo
        # whitespace/case. Typically caused by manual entry drift or
        # inconsistent 1C export spellings.
        fuzzy_dups = conn.execute(
            """SELECT LOWER(TRIM(client_id_1c)) AS norm,
                      COUNT(*) AS n,
                      GROUP_CONCAT(id || ':' || client_id_1c, ' | ') AS rows
               FROM allowed_clients
               WHERE COALESCE(status, 'active') != 'merged'
                 AND client_id_1c IS NOT NULL AND client_id_1c != ''
               GROUP BY LOWER(TRIM(client_id_1c))
               HAVING COUNT(*) > 1
               ORDER BY n DESC
               LIMIT 20"""
        ).fetchall()
        if fuzzy_dups:
            result["fuzzy_client_1c_dups"] = {
                "count": len(fuzzy_dups),
                "sample": [dict(r) for r in fuzzy_dups[:5]],
            }

    finally:
        conn.close()
    return result


def format_audit_message(findings: dict) -> str | None:
    """Render findings into a Telegram-ready HTML message. Returns None
    if nothing to report (all checks passed)."""
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
        "debt_usd_coverage_zero":  "💵 client_debts: USD butunlay yo'q (В Валюте column?)",
        "trend_currency_drift":    "📉 Cabinet trend: valyuta yo'qoldi (oldin bor edi, oxirgi 6 oyda yo'q)",
        "fuzzy_client_1c_dups":    "👥 client_id_1c dublikat (case/whitespace)",
    }

    for key, label in issue_labels.items():
        if key in findings:
            # Healable orphans surface auto-heal status so the cron's report
            # distinguishes "we just fixed N rows" from "alert: N rows need
            # investigation" (the on-demand /consistencycheck path).
            if key == "healable_orphans" and findings[key].get("auto_healed"):
                label = label + " — auto-healed ✓"
            item = findings[key]
            n = item.get("count", 0)
            lines.append(f"<b>{label}:</b> {n}")
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
