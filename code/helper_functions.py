from pathlib import Path
import re
import time
import duckdb
import pandas

# ---------------------------------------------------------------------
# Project Path Constants
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DUCKLAKE_FOLDER = BASE_DIR.parent / "ducklake"
DUCKLAKE_DATA = DUCKLAKE_FOLDER / "data_files"
DUCKLAKE_METADATA = DUCKLAKE_FOLDER / "metadata_catalog.ducklake"
TEST_FOLDER = BASE_DIR.parent / "test"

# Used for raw input data (webcrawls)
PARQUET_FOLDER = BASE_DIR.parent / "parquet" 

# ---------------------------------------------------------------------
# DuckLake Attachment
# ---------------------------------------------------------------------
def connect_ducklake(con):
    """
    Attach (or create) the DuckLake catalog as `my_ducklake`.
    """
    DUCKLAKE_DATA.mkdir(parents=True, exist_ok=True)

    # Load required extensions
    con.execute("INSTALL ducklake; LOAD ducklake;")

    metadata_path = DUCKLAKE_METADATA.as_posix()
    data_path = DUCKLAKE_DATA.as_posix()

    # ATTACH creates or opens the DuckLake catalog
    con.execute(
        f"ATTACH 'ducklake:{metadata_path}' AS my_ducklake (DATA_PATH '{data_path}');"
    )
    con.execute("USE my_ducklake;")

# ---------------------------------------------------------------------
# Sanity Inspection / Diagnostics
# ---------------------------------------------------------------------
def test_ducklake(con):
    """
    Inspect schema and preview table contents for DuckLake tables.
    """
    con.execute("USE my_ducklake")

    for tbl in ["dict", "docs", "postings"]:
        print(f"Describe {tbl}:")
        try:
            print(con.execute(f"DESCRIBE my_ducklake.{tbl}").fetch_df(), "\n")
            print(f"Top 2 rows in {tbl}:")
            print(con.execute(f"SELECT * FROM my_ducklake.{tbl} LIMIT 2").fetch_df(), "\n")
        except duckdb.CatalogException:
            print(f"Table {tbl} does not exist yet.\n")

# ---------------------------------------------------------------------
# Query Utilities
# ---------------------------------------------------------------------
def get_termid(con, term):
    """
    Retrieve the termid for a given term from `my_ducklake.dict`.
    """
    try:
        row = con.execute(
            "SELECT termid FROM my_ducklake.dict WHERE term = ?",
            [term],
        ).fetchone()
        return row[0] if row else None
        
    except duckdb.IOException:
        # Handle transient read errors during massive updates/checkpoints
        return None

def get_docid_count(con):
    """
    Retrieve the number of docid's which are stored in the index.
    """
    result = con.execute("SELECT COUNT(*) FROM my_ducklake.docs").fetchone()
    return result[0] if result else 0

# ---------------------------------------------------------------------
# Tokenization Utilities
# ---------------------------------------------------------------------
_WORD_RE = re.compile(r"[A-Za-z]+")

def tokenize(content: str) -> list[str]:
    """
    Tokenize input text into lowercase alphabetic terms.
    """
    if not content:
        return []
    return [m.group(0).lower() for m in _WORD_RE.finditer(content)]

def tokenize_query(con, query):
    """
    Tokenize a query string and map known tokens to termids.
    """
    tokens = tokenize(query)
    return [tid for term in tokens if (tid := get_termid(con, term)) is not None]

# ---------------------------------------------------------------------
# Data Ingest / Initialization
# ---------------------------------------------------------------------
def initialise_data(con, parquet="*", limit=None):
    """
    Create or replace `my_ducklake.data` from raw Parquet files.
    Defaults to importing ALL *.parquet files from /parquet/
    """
    con.execute("USE my_ducklake")

    # FIX 1: Point directly to PARQUET_FOLDER, not a subfolder
    webcrawl_dir = PARQUET_FOLDER.resolve()
    
    # If None or empty, default to "*"
    parquet_arg = str(parquet).strip() if parquet else "*"

    # Resolve file or directory source
    if parquet_arg.upper() in {"ALL", "*"}:
        src = (webcrawl_dir / "*.parquet").as_posix()
    else:
        # User specified a file or folder relative to parquet folder (or absolute)
        p = Path(parquet_arg)
        if p.is_absolute():
            src = (p / "*.parquet").as_posix() if p.is_dir() else p.as_posix()
        else:
            candidate = (webcrawl_dir / parquet_arg).resolve()
            if candidate.exists():
                src = (candidate / "*.parquet").as_posix() if candidate.is_dir() else candidate.as_posix()
            else:
                p2 = Path(parquet_arg).resolve()
                if p2.exists() and p2.is_file():
                    src = p2.as_posix()
                elif p2.exists() and p2.is_dir():
                    src = (p2 / "*.parquet").as_posix()
                else:
                    raise SystemExit(f"ERROR: Could not resolve parquet input: {parquet_arg}")

    con.execute("DROP TABLE IF EXISTS data")

    # Build SQL query based on limit
    sql = """
        CREATE OR REPLACE TABLE data AS
        SELECT
            CAST(docid AS BIGINT)      AS docid,
            CAST(main_content AS TEXT) AS content
        FROM read_parquet(?)
        ORDER BY docid
    """
    
    # FIX 2: Explicitly type 'params' as list[object] to fix Pylance error
    params: list[object] = [src]
    
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))

    con.execute(sql, params)

# ---------------------------------------------------------------------
# Incremental Import / Upsert
# ---------------------------------------------------------------------
def import_data(con, parquet):
    """
    Upsert Parquet data into `my_ducklake.data`.
    """
    # Standardized to use PARQUET_FOLDER constant
    src = (PARQUET_FOLDER / parquet).resolve().as_posix()

    con.execute("USE my_ducklake")
    con.execute("""
        CREATE TABLE IF NOT EXISTS data (
            docid   BIGINT,
            content TEXT
        )
    """)

    con.execute("""
        MERGE INTO data AS target
        USING (
            SELECT
                CAST(docid AS BIGINT)      AS docid,
                CAST(main_content AS TEXT) AS content
            FROM read_parquet(?)
        ) AS source
        ON (target.docid = source.docid)
        WHEN MATCHED THEN UPDATE SET content = source.content
        WHEN NOT MATCHED THEN INSERT (docid, content)
        VALUES (source.docid, source.content)
    """, [src])

# ---------------------------------------------------------------------
# DuckLake Maintenance / Cleanup
# ---------------------------------------------------------------------
def checkpoint_rewrite(con):
    """
    Implements all the ducklake maintenance functions bundled
    """
    print(f"--- CHECKPOINT triggered ---")
    start_ckpt = time.perf_counter()
    con.execute("""
                CALL ducklake_rewrite_data_files('my_ducklake', delete_threshold => 0.01);
                CHECKPOINT;
                """)
    end_ckpt = time.perf_counter()
    print(f"--- CHECKPOINT complete ({end_ckpt - start_ckpt:.4f}s) ---")