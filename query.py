import duckdb
import pandas as pd

con = duckdb.connect()
con.execute("INSTALL fts;")
con.execute("LOAD fts;")

# Only attach the persisted FTS DB; no need to attach DuckLake unless you’re reloading data
con.execute("ATTACH 'FTSDB.duckdb' AS fts_db;")

query = """
SELECT id, main_content, score
FROM (
  SELECT *,
         -- THREE-PART qualification: database.helper.function
         fts_db.fts_main_docs.match_bm25(id, ?) AS score
  FROM fts_db.main.docs
) q
WHERE score IS NOT NULL
ORDER BY score DESC
LIMIT 5;
"""
query_term = "bow ties"
df = con.execute(query, [query_term]).fetch_df()
print(df)