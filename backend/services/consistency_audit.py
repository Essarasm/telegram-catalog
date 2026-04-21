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


def run_audit() -> dict:
    """Returns a dict of findings. Keys are check names, values are dicts
    with at least {"count": N, "sample": [...]} when count > 0. Empty
    findings are omitted entirely so the caller can check `if result:` to
    decide whether to notify."""
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
    }

    for key, label in issue_labels.items():
        if key in findings:
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
