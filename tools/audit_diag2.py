"""Round 2 of the duplicate-explosion read-only diagnostic.

Round 1 confirmed: 40,940 rows, 20× duplication of 2,039 phones, identical
copies for the sampled phone. This round answers the merge-strategy and
edge-case questions before we design the dedup migration.
"""
import sqlite3
import os

db = os.environ.get("DATABASE_PATH", "/data/catalog.db")
c = sqlite3.connect(db)
c.row_factory = sqlite3.Row


def q(label, sql, params=()):
    print(f"\n--- {label} ---")
    for r in c.execute(sql, params):
        print(dict(r))


# 1. Across each phone group, how many DISTINCT client_id_1c values appear?
#    1 = clean (just delete dups). 2+ = master sync hit some copies, missed
#    others — survivor must merge fields before delete.
q("phone-group: distinct client_id_1c count distribution", """
  SELECT n_distinct_1c, COUNT(*) AS phone_groups FROM (
    SELECT phone_normalized,
           COUNT(DISTINCT COALESCE(client_id_1c, '__null__')) AS n_distinct_1c
      FROM allowed_clients
     WHERE COALESCE(status,'active')='active' AND phone_normalized != ''
     GROUP BY phone_normalized
  ) GROUP BY n_distinct_1c ORDER BY n_distinct_1c
""")

# 2. Same idea for other "soft" fields — if these split, merge logic must
#    cover them too. We pick the fields most likely to drift via master sync.
q("phone-group: distinct soft-field counts (where >1)", """
  SELECT 'client_id_1c' AS field, n, COUNT(*) AS phone_groups FROM (
    SELECT phone_normalized, COUNT(DISTINCT COALESCE(client_id_1c,'__null__')) AS n
      FROM allowed_clients WHERE COALESCE(status,'active')='active' AND phone_normalized!=''
      GROUP BY phone_normalized
  ) WHERE n>1 GROUP BY n
  UNION ALL
  SELECT 'company_name', n, COUNT(*) FROM (
    SELECT phone_normalized, COUNT(DISTINCT COALESCE(company_name,'__null__')) AS n
      FROM allowed_clients WHERE COALESCE(status,'active')='active' AND phone_normalized!=''
      GROUP BY phone_normalized
  ) WHERE n>1 GROUP BY n
  UNION ALL
  SELECT 'location', n, COUNT(*) FROM (
    SELECT phone_normalized, COUNT(DISTINCT COALESCE(location,'__null__')) AS n
      FROM allowed_clients WHERE COALESCE(status,'active')='active' AND phone_normalized!=''
      GROUP BY phone_normalized
  ) WHERE n>1 GROUP BY n
  UNION ALL
  SELECT 'matched_telegram_id', n, COUNT(*) FROM (
    SELECT phone_normalized, COUNT(DISTINCT COALESCE(matched_telegram_id,-1)) AS n
      FROM allowed_clients WHERE COALESCE(status,'active')='active' AND phone_normalized!=''
      GROUP BY phone_normalized
  ) WHERE n>1 GROUP BY n
  UNION ALL
  SELECT 'gps_latitude', n, COUNT(*) FROM (
    SELECT phone_normalized, COUNT(DISTINCT COALESCE(gps_latitude,-999)) AS n
      FROM allowed_clients WHERE COALESCE(status,'active')='active' AND phone_normalized!=''
      GROUP BY phone_normalized
  ) WHERE n>1 GROUP BY n
  ORDER BY field, n
""")

# 3. Sample a phone group that splits across client_id_1c (to see the shape)
q("sample of a SPLIT phone group", """
  WITH split_phone AS (
    SELECT phone_normalized
      FROM allowed_clients
     WHERE COALESCE(status,'active')='active' AND phone_normalized!=''
     GROUP BY phone_normalized
    HAVING COUNT(DISTINCT COALESCE(client_id_1c,'__null__')) > 1
     ORDER BY COUNT(*) DESC LIMIT 1
  )
  SELECT id, name, client_id_1c, phone_normalized, company_name,
         matched_telegram_id, source_sheet, source_master, last_master_synced_at
    FROM allowed_clients
   WHERE phone_normalized = (SELECT phone_normalized FROM split_phone)
   ORDER BY id
""")

# 4. Empty-phone rows — what's their content + are they referenced by FK?
q("empty-phone row counts by source_sheet", """
  SELECT source_sheet, COUNT(*) AS n,
         SUM(CASE WHEN COALESCE(client_id_1c,'')!='' THEN 1 ELSE 0 END) AS with_1c_name,
         SUM(CASE WHEN matched_telegram_id IS NOT NULL THEN 1 ELSE 0 END) AS with_tg
    FROM allowed_clients
   WHERE COALESCE(phone_normalized,'')='' AND COALESCE(status,'active')='active'
   GROUP BY source_sheet
   ORDER BY n DESC
""")

q("empty-phone rows with FK references", """
  SELECT 'users' AS tbl, COUNT(*) AS n FROM users u
   WHERE u.client_id IN (SELECT id FROM allowed_clients WHERE COALESCE(phone_normalized,'')='')
  UNION ALL
  SELECT 'real_orders', COUNT(*) FROM real_orders ro
   WHERE ro.client_id IN (SELECT id FROM allowed_clients WHERE COALESCE(phone_normalized,'')='')
  UNION ALL
  SELECT 'client_balances', COUNT(*) FROM client_balances cb
   WHERE cb.client_id IN (SELECT id FROM allowed_clients WHERE COALESCE(phone_normalized,'')='')
""")

# 5. Single-phone (n_per_phone=1) rows — confirm they're recent additions
#    (high ids in the partial 20th bucket) and not orphaned legitimate clients
q("single-phone rows: id distribution", """
  WITH singles AS (
    SELECT phone_normalized FROM allowed_clients
     WHERE COALESCE(status,'active')='active' AND phone_normalized!=''
     GROUP BY phone_normalized HAVING COUNT(*)=1
  )
  SELECT (id / 5000) AS bucket_5k, MIN(id) AS min_id, MAX(id) AS max_id, COUNT(*) AS n
    FROM allowed_clients
   WHERE phone_normalized IN (SELECT phone_normalized FROM singles)
   GROUP BY bucket_5k ORDER BY bucket_5k
""")

# 6. Source-sheet distribution by id-bucket — does each 2.1k bucket come
#    from the SAME source? Tells us which import path created the dups.
q("source_sheet distribution per id-bucket", """
  SELECT (id / 2100) AS bucket, source_sheet, COUNT(*) AS n
    FROM allowed_clients
   GROUP BY bucket, source_sheet
   ORDER BY bucket, n DESC
""")

# 7. FK rows pointing at duplicate-id with SAME phone as a survivor —
#    confirms safe rewire (every FK has a clean survivor target).
q("FK rewire safety check (sample 5 dup-zone real_orders)", """
  SELECT ro.id AS order_id, ro.client_id AS dup_client_id,
         ac.phone_normalized,
         (SELECT MIN(id) FROM allowed_clients ac2
           WHERE ac2.phone_normalized = ac.phone_normalized
             AND ac2.phone_normalized != ''
             AND COALESCE(ac2.status,'active')='active') AS survivor_id
    FROM real_orders ro
    JOIN allowed_clients ac ON ac.id = ro.client_id
   WHERE ro.client_id > 2100
   LIMIT 5
""")

c.close()
