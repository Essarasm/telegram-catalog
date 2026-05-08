"""Direct verification before running the dedup migration.

Asks the question pointedly: are there any active sibling rows (i.e. rows
that the migration WILL soft-delete) holding load-bearing data that the
survivor doesn't have?

If every count is 0 → migration is safe with no field-merge needed.
If any count is >0 → the migration's field-merge step must fire correctly
                     for that field; we want to see real numbers.
"""
import sqlite3
import os

db = os.environ.get("DATABASE_PATH", "/data/catalog.db")
c = sqlite3.connect(db)
c.row_factory = sqlite3.Row


def q(label, sql):
    print(f"\n--- {label} ---")
    for r in c.execute(sql):
        print(dict(r))


# Direct check: how many sibling (non-survivor) active rows have a
# non-NULL value for each load-bearing field, where the survivor has NULL?
# A non-zero count means we need the field-merge step to fire.
LOAD_BEARING_FIELDS = [
    "matched_telegram_id",
    "gps_latitude",
    "gps_longitude",
    "client_id_1c",
    "company_name",
    "credit_score",
    "credit_limit",
    "last_master_synced_at",
    "source_master",
    "master_row_id",
    "needs_review",
    "needs_verification",
    "ism_02",
    "raqam_02",
    "ism_03",
    "raqam_03",
]


print("=== Pre-migration field-merge verification ===\n")
print("For each field, count of (sibling_id, survivor_id) pairs where:")
print("  - sibling has a non-NULL/non-empty value")
print("  - survivor (lowest id for that phone) has NULL/''")
print("  These are the rows where field-merge MUST fire.\n")

for f in LOAD_BEARING_FIELDS:
    row = c.execute(f"""
        WITH groups AS (
          SELECT phone_normalized, MIN(id) AS survivor_id
            FROM allowed_clients
           WHERE COALESCE(status,'active')='active'
             AND phone_normalized IS NOT NULL
             AND phone_normalized != ''
           GROUP BY phone_normalized
          HAVING COUNT(*) > 1
        )
        SELECT COUNT(*) AS n_siblings_with_value,
               COUNT(DISTINCT g.phone_normalized) AS n_phone_groups
          FROM groups g
          JOIN allowed_clients surv ON surv.id = g.survivor_id
          JOIN allowed_clients sib  ON sib.phone_normalized = g.phone_normalized
                                   AND sib.id != g.survivor_id
                                   AND COALESCE(sib.status,'active')='active'
         WHERE (surv.{f} IS NULL OR surv.{f} = '')
           AND sib.{f} IS NOT NULL
           AND sib.{f} != ''
    """).fetchone()
    flag = "*** WOULD MERGE ***" if row["n_siblings_with_value"] else "(no merge needed)"
    print(f"  {f}: siblings={row['n_siblings_with_value']}, "
          f"phones={row['n_phone_groups']}  {flag}")


# Cross-check: also count where survivor HAS the value (the "lucky" case
# where bot/app writes hit the lowest-id row directly).
print("\n=== Where load-bearing values actually live (survivor vs sibling) ===")
for f in ["matched_telegram_id", "gps_latitude", "client_id_1c"]:
    row = c.execute(f"""
        WITH groups AS (
          SELECT phone_normalized, MIN(id) AS survivor_id
            FROM allowed_clients
           WHERE COALESCE(status,'active')='active'
             AND phone_normalized IS NOT NULL
             AND phone_normalized != ''
           GROUP BY phone_normalized
          HAVING COUNT(*) > 1
        )
        SELECT
          SUM(CASE WHEN surv.{f} IS NOT NULL AND surv.{f} != ''
                   THEN 1 ELSE 0 END) AS survivor_has,
          SUM(CASE WHEN sib_has.cnt > 0 AND (surv.{f} IS NULL OR surv.{f} = '')
                   THEN 1 ELSE 0 END) AS sibling_has_only,
          SUM(CASE WHEN sib_has.cnt > 0 AND surv.{f} IS NOT NULL AND surv.{f} != ''
                   THEN 1 ELSE 0 END) AS both_have
          FROM groups g
          JOIN allowed_clients surv ON surv.id = g.survivor_id
          LEFT JOIN (
            SELECT phone_normalized, COUNT(*) AS cnt
              FROM allowed_clients
             WHERE COALESCE(status,'active')='active'
               AND {f} IS NOT NULL AND {f} != ''
               AND id NOT IN (
                 SELECT MIN(id) FROM allowed_clients
                  WHERE COALESCE(status,'active')='active'
                    AND phone_normalized != ''
                  GROUP BY phone_normalized
               )
             GROUP BY phone_normalized
          ) sib_has ON sib_has.phone_normalized = g.phone_normalized
    """).fetchone()
    print(f"\n  {f}:")
    print(f"    survivor has value:        {row['survivor_has']}")
    print(f"    sibling has, survivor empty: {row['sibling_has_only']}")
    print(f"    both have value:            {row['both_have']}")


c.close()
print("\nDone.")
