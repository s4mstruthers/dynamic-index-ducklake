from pathlib import Path
import re

# ---------------------------------------------------------------------
# Project Path Constants
# ---------------------------------------------------------------------
# Defines fixed directories relative to project layout.
# BASE_DIR -> /code/
# DUCKLAKE_FOLDER -> /ducklake/
# PARQUET_FOLDER -> /parquet/
# TEST_FOLDER -> /test/
BASE_DIR = Path(__file__).resolve().parent
DUCKLAKE_FOLDER = BASE_DIR.parent / "ducklake"
DUCKLAKE_DATA = DUCKLAKE_FOLDER / "data_files"
DUCKLAKE_METADATA = DUCKLAKE_FOLDER / "metadata_catalog.ducklake"
TEST_FOLDER = BASE_DIR.parent / "test"
PARQUET_FOLDER = BASE_DIR.parent / "parquet"

# ---------------------------------------------------------------------
# DuckLake Attachment
# ---------------------------------------------------------------------
def connect_ducklake(con):
    """
    Attach (or create) the DuckLake catalog as `my_ducklake`.

    - Ensures directories exist before attaching.
    - Automatically installs and loads required DuckDB extensions.
    - ATTACH creates the catalog if the file does not yet exist.
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
    Used for debugging schema correctness and verifying attach success.
    """
    con.execute("USE my_ducklake")

    for tbl in ["dict", "docs", "postings"]:
        print(f"Describe {tbl}:")
        print(con.execute(f"DESCRIBE my_ducklake.{tbl}").fetch_df(), "\n")
        print(f"Top 2 rows in {tbl}:")
        print(con.execute(f"SELECT * FROM my_ducklake.{tbl} LIMIT 2").fetch_df(), "\n")

# ---------------------------------------------------------------------
# Query Utilities
# ---------------------------------------------------------------------
def get_termid(con, term):
    """
    Retrieve the termid for a given term from `my_ducklake.dict`.
    Returns None if the term is not present.
    Uses parameterized query to prevent SQL injection.
    """
    row = con.execute(
        "SELECT termid FROM my_ducklake.dict WHERE term = ?",
        [term],
    ).fetchone()
    return row[0] if row else None

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
    Non-alphabetic characters are ignored.
    Example: "AI-driven systems (2025)!" → ["ai", "driven", "systems"]
    """
    if not content:
        return []
    return [m.group(0).lower() for m in _WORD_RE.finditer(content)]

def tokenize_query(con, query):
    """
    Tokenize a query string and map known tokens to termids.
    Terms not found in `dict` are discarded.
    """
    tokens = tokenize(query)
    return [tid for term in tokens if (tid := get_termid(con, term)) is not None]

# ---------------------------------------------------------------------
# Data Ingest / Initialization
# ---------------------------------------------------------------------
def initialise_data(con, parquet="metadata_0.parquet", limit=None):
    """
    Create or replace `my_ducklake.data` from one or more Parquet files.

    Supported 'parquet' inputs:
      - File name within /parquet/webcrawl_data/
      - Absolute or relative path to a single file
      - Directory path (imports all *.parquet files)
      - 'ALL' or '*' to import all files in /parquet/webcrawl_data/

    Ensures deterministic ordering by docid and optional row limiting.
    """
    con.execute("USE my_ducklake")

    webcrawl_dir = (PARQUET_FOLDER / "webcrawl_data").resolve()
    parquet_arg = str(parquet).strip() if parquet else "ALL"

    # Resolve file or directory source
    if parquet_arg.upper() in {"ALL", "*"}:
        src = (webcrawl_dir / "*.parquet").as_posix()
    else:
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

    # Create the data table (glob patterns supported by DuckDB)
    if limit is None:
        con.execute(
            """
            CREATE OR REPLACE TABLE data AS
            SELECT
                CAST(docid AS BIGINT)      AS docid,
                CAST(main_content AS TEXT) AS content
            FROM read_parquet(?)
            ORDER BY docid
            """,
            [src],
        )
    else:
        con.execute(
            """
            CREATE OR REPLACE TABLE data AS
            SELECT
                CAST(docid AS BIGINT)      AS docid,
                CAST(main_content AS TEXT) AS content
            FROM read_parquet(?)
            ORDER BY docid
            LIMIT ?
            """,
            [src, int(limit)],
        )

# ---------------------------------------------------------------------
# Incremental Import / Upsert
# ---------------------------------------------------------------------
def import_data(con, parquet):
    """
    Upsert Parquet data into `my_ducklake.data` via MERGE INTO.

    - Updates content if docid already exists.
    - Inserts new rows if docid not found.
    """
    src = (BASE_DIR.parent / "parquet" / parquet).resolve().as_posix()

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
def cleanup_old_files(con, older_than_days=7, dry_run=True, all_files=False):
    """
    Remove files that DuckLake scheduled for deletion (expired snapshots).

    Parameters:
      - older_than_days: Age threshold for cleanup.
      - dry_run: Only list files to be deleted (no changes).
      - all_files: Ignore the threshold and clean all eligible files.
    """
    try:
        days = int(older_than_days)
        if days <= 0:
            raise ValueError
    except Exception:
        raise SystemExit("ERROR: older_than_days must be a positive integer.")

    interval = f"{days} days"
    con.execute("USE my_ducklake")

    if all_files:
        con.execute("CALL ducklake_cleanup_old_files('my_ducklake', cleanup_all => true)")
        print("Executed cleanup of all scheduled-for-deletion files.")
        return

    if dry_run:
        con.execute(
            f"CALL ducklake_cleanup_old_files('my_ducklake', dry_run => true, older_than => now() - INTERVAL '{interval}')"
        )
        print(f"Dry run: listed scheduled deletions older than {interval}.")
    else:
        con.execute(
            f"CALL ducklake_cleanup_old_files('my_ducklake', older_than => now() - INTERVAL '{interval}')"
        )
        print(f"Deleted scheduled files older than {interval}.")

def cleanup_orphaned_files(con, older_than_days=7, dry_run=True, all_files=False):
    """
    Remove orphaned (untracked) data files from the DuckLake DATA_PATH.

    Parameters mirror cleanup_old_files:
      - older_than_days: Age threshold for cleanup.
      - dry_run: Only list files to be deleted.
      - all_files: Delete all orphans regardless of age.
    """
    try:
        days = int(older_than_days)
        if days <= 0:
            raise ValueError
    except Exception:
        raise SystemExit("ERROR: older_than_days must be a positive integer.")

    interval = f"{days} days"
    con.execute("USE my_ducklake")

    if all_files:
        con.execute("CALL ducklake_delete_orphaned_files('my_ducklake', cleanup_all => true)")
        print("Executed cleanup of all orphaned files.")
        return

    if dry_run:
        con.execute(
            f"CALL ducklake_delete_orphaned_files('my_ducklake', dry_run => true, older_than => now() - INTERVAL '{interval}')"
        )
        print(f"Dry run: listed orphaned files older than {interval}.")
    else:
        con.execute(
            f"CALL ducklake_delete_orphaned_files('my_ducklake', older_than => now() - INTERVAL '{interval}')"
        )
        print(f"Deleted orphaned files older than {interval}.")

def checkpoint(con):
    """
    Implements all the ducklake maintenance functions bundled
    """
    con.execute("CHECKPOINT;")

def rewrite_data_files(con, delete_threshold=None):
    """
    Only rewrites data files that have deletions, skipping other checkpoint operations.
    
    Parameters:
      - delete_threshold (float): Optional. A ratio (0.0 to 1.0) specifying the 
        minimum percentage of rows that must be deleted in a file for it to be 
        rewritten. If None, uses the system default.
    """
    if delete_threshold is not None:
        # Use a specific threshold if provided (e.g., 0.1 for 10% deleted)
        con.execute(f"CALL ducklake_rewrite_data_files('my_ducklake', delete_threshold => {delete_threshold});")
    else:
        # Run with default settings
        con.execute("CALL ducklake_rewrite_data_files('my_ducklake');")