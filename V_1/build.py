import duckdb

# create a temporary connection which we will then attach to duckLake
con = duckdb.connect()

# 1) Install / load the required extensions
con.execute("INSTALL ducklake;")
con.execute("INSTALL postgres;")
con.execute("INSTALL fts;")
con.execute("LOAD fts;")

# Attach the external Postgres ducklake_catalog via DuckLake (source of data)
con.execute("""
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost user=samstruthers password=jofkud-caTha5-jezmud'
AS my_ducklake (DATA_PATH 'data_files/');
""")

# Attach/create the persisted DuckDB file that will hold the FTS index
con.execute("ATTACH 'FTSDB.duckdb' AS fts_db;")

# Import the source text into the duckDB table (fts_db.main.docs) -> annoyingly this is duplicating main_content into DuckDB
con.execute("""
CREATE OR REPLACE TABLE fts_db.main.docs AS
SELECT id, title, main_content
FROM my_ducklake.data;
""")

# Build (or rebuild) the FTS index INSIDE fts_db for persistence
# This creates helper schema 'fts_main_docs' which is in fts_db.main when we need to access it
con.execute("""
PRAGMA create_fts_index('fts_db.main.docs', 'id', 'title', 'main_content', overwrite=1);
""")

# Optional for now: detach/close after build
# con.execute("DETACH my_ducklake;")
# con.execute("DETACH fts_db;")
# con.close()