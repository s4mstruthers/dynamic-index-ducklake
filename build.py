import duckdb

con = duckdb.connect()

# 1) Install / load extensions
con.execute("INSTALL ducklake;")
con.execute("INSTALL postgres;")
con.execute("INSTALL fts;")
con.execute("LOAD fts;")

# 2) Attach external Postgres via DuckLake (source)
con.execute("""
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost user=samstruthers password=jofkud-caTha5-jezmud'
AS ducklake_db (DATA_PATH 'data_files/');
""")

# 3) Attach/create the persisted DuckDB file that will HOLD the FTS (target)
con.execute("ATTACH 'FTSDB.duckdb' AS fts_db;")

# 4) Materialize source text into the persisted DB (fts_db.main.docs)
con.execute("""
CREATE OR REPLACE TABLE fts_db.main.docs AS
SELECT id, title, main_content
FROM ducklake_db.data;
""")

# 5) Build (or rebuild) the FTS index INSIDE fts_db for persistence
#    This creates helper 'fts_main_docs' living in fts_db.main
con.execute("""
PRAGMA create_fts_index('fts_db.main.docs', 'id', 'title', 'main_content', overwrite=1);
""")

# Optional: detach/close after build
# con.execute("DETACH ducklake_db;")
# con.execute("DETACH fts_db;")
# con.close()