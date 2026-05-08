"""One-off read-only diagnostic for the daily Data Consistency Audit.

Confirms the suspected ~20x duplication of allowed_clients and quantifies
FK exposure across dependent tables. Pure SELECT; no writes.
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


q("row counts", """
  SELECT COUNT(*) AS total,
         SUM(CASE WHEN COALESCE(status,'active')='active' THEN 1 ELSE 0 END) AS active,
         SUM(CASE WHEN status='merged' THEN 1 ELSE 0 END) AS merged,
         COUNT(DISTINCT phone_normalized) AS dist_phones,
         COUNT(DISTINCT COALESCE(client_id_1c,'')) AS dist_1c,
         COUNT(DISTINCT phone_normalized || '|' || COALESCE(client_id_1c,'')) AS dist_pair
    FROM allowed_clients
""")

q("id-range histogram (each ~2k bucket = one suspected import run)", """
  SELECT (id / 2100) AS bucket, MIN(id) AS min_id, MAX(id) AS max_id, COUNT(*) AS n
    FROM allowed_clients GROUP BY bucket ORDER BY bucket
""")

q("schema columns", "SELECT name FROM pragma_table_info('allowed_clients')")

q("rows-per-phone distribution", """
  SELECT n_per_phone, COUNT(*) AS phones FROM (
    SELECT phone_normalized, COUNT(*) AS n_per_phone
      FROM allowed_clients
     WHERE COALESCE(status,'active')='active' AND phone_normalized != ''
     GROUP BY phone_normalized
  ) GROUP BY n_per_phone ORDER BY n_per_phone
""")

q("FK exposure", """
  SELECT 'users' AS tbl, COUNT(*) AS n FROM users WHERE client_id IS NOT NULL
  UNION ALL SELECT 'real_orders', COUNT(*) FROM real_orders WHERE client_id IS NOT NULL
  UNION ALL SELECT 'client_balances', COUNT(*) FROM client_balances WHERE client_id IS NOT NULL
  UNION ALL SELECT 'client_payments', COUNT(*) FROM client_payments WHERE client_id IS NOT NULL
  UNION ALL SELECT 'client_debts', COUNT(*) FROM client_debts WHERE client_id IS NOT NULL
  UNION ALL SELECT 'orders', COUNT(*) FROM orders WHERE client_id IS NOT NULL
""")

q("FK rows in dup zone (id > 2100)", """
  SELECT 'users' AS tbl, COUNT(*) AS n FROM users WHERE client_id > 2100
  UNION ALL SELECT 'real_orders', COUNT(*) FROM real_orders WHERE client_id > 2100
  UNION ALL SELECT 'client_balances', COUNT(*) FROM client_balances WHERE client_id > 2100
  UNION ALL SELECT 'client_payments', COUNT(*) FROM client_payments WHERE client_id > 2100
  UNION ALL SELECT 'client_debts', COUNT(*) FROM client_debts WHERE client_id > 2100
  UNION ALL SELECT 'orders', COUNT(*) FROM orders WHERE client_id > 2100
""")

q("one full dup group", """
  SELECT id, name, client_id_1c, phone_normalized, location, source_sheet,
         status, needs_review, last_master_synced_at
    FROM allowed_clients
   WHERE phone_normalized = (
     SELECT phone_normalized FROM allowed_clients
      WHERE COALESCE(status,'active')='active' AND phone_normalized != ''
      GROUP BY phone_normalized HAVING COUNT(*) > 1
      ORDER BY COUNT(*) DESC LIMIT 1
   )
   ORDER BY id
""")

c.close()
