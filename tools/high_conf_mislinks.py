"""High-confidence user→client mislink finder (offline / authoritative version).

A client-role (non-agent) approved user whose Telegram phone matches a DIFFERENT
*real* client (client_id_1c IS NOT NULL) than the one their account is linked to
is almost certainly mislinked — their number is whitelisted, just pointing at the
wrong shop. This is the Турдиев class (2026-06-02): a manual registration mispick
or import phone-collision attaches a Telegram account to the wrong allowed_clients
row, so their orders / cabinet / location pins flow to that wrong shop.

This is the authoritative companion to the daily `consistency_audit.py`
`mislinked_users` check: it handles dirty multi-format phone strings, checks all
three phone slots (phone_normalized + raqam_02 + raqam_03), and treats sibling
rows (shared client_id_1c) as correct. Excludes bot_approved placeholder rows
(client_id_1c NULL — e.g. a tester's self-registration).

Run on prod:  railway ssh "python3 -" < tools/high_conf_mislinks.py
(or base64 the file in and run it — see feedback_railway_ssh_base64_script_run).

Read-only. Reports; does not mutate. Re-point a confirmed mislink with an
admin_action_log-logged UPDATE (see the 2026-06-02 Турдиев fix for the template).
"""
import sqlite3
import os
import json


def norm(p):
    if not p:
        return ""
    d = "".join(ch for ch in str(p) if ch.isdigit())
    return d[-9:]


def main():
    db = os.environ.get("DATABASE_PATH", "/data/catalog.db")
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row

    ac_cols = {r[1] for r in c.execute("PRAGMA table_info(allowed_clients)").fetchall()}
    phone_cols = [x for x in ("phone_normalized", "raqam", "raqam_02", "raqam_03", "phone")
                  if x in ac_cols]

    # phone -> [active client rows]; placeholder rows (client_id_1c NULL) are
    # NOT "real clients" — a phone matching only a placeholder is not a mislink.
    idx = {}
    for r in c.execute(
        f"SELECT id, name, client_id_1c, status, {','.join(phone_cols)} FROM allowed_clients"
    ).fetchall():
        if r["status"] and str(r["status"]).startswith("merged"):
            continue
        if not r["client_id_1c"]:
            continue
        for pc in phone_cols:
            n = norm(r[pc])
            if n:
                idx.setdefault(n, []).append(
                    {"id": r["id"], "name": r["name"], "client_id_1c": r["client_id_1c"]}
                )

    users = c.execute(
        "SELECT u.telegram_id, u.first_name, u.phone, u.client_id, u.is_agent, "
        "ac.name AS ac_name, ac.client_id_1c AS ac_c1c "
        "FROM users u LEFT JOIN allowed_clients ac ON ac.id = u.client_id "
        "WHERE u.is_approved = 1 AND u.client_id IS NOT NULL AND u.phone IS NOT NULL"
    ).fetchall()

    high, phone_unknown, ok = [], 0, 0
    for u in users:
        if u["is_agent"]:  # agents legitimately switch via act-as
            continue
        matches = idx.get(norm(u["phone"]), [])
        if not matches:
            phone_unknown += 1
            continue
        match_ids = {m["id"] for m in matches}
        match_c1c = {m["client_id_1c"] for m in matches if m["client_id_1c"]}
        if u["client_id"] in match_ids or (u["ac_c1c"] and u["ac_c1c"] in match_c1c):
            ok += 1
        else:
            high.append({
                "tg": u["telegram_id"], "user": u["first_name"], "phone": norm(u["phone"]),
                "linked_to": f'{u["client_id"]} {u["ac_name"]}',
                "phone_belongs_to": [f'{m["id"]} {m["name"]}' for m in matches],
            })

    print(f"HIGH-CONFIDENCE MISLINKS: {len(high)}")
    for h in high:
        print(json.dumps(h, ensure_ascii=False))
    print(f"\nsummary: {len(high)} mislinked | {phone_unknown} phone-not-whitelisted | {ok} ok")
    c.close()


if __name__ == "__main__":
    main()
