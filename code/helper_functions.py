from pathlib import Path
import re
# ---------------------------------------------------------------------
# Paths (project layout constants)
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DUCKLAKE_FOLDER = BASE_DIR.parent / "ducklake"
DUCKLAKE_DATA = DUCKLAKE_FOLDER / "data_files"
DUCKLAKE_METADATA = DUCKLAKE_FOLDER / "metadata_catalog.ducklake"
TEST_FOLDER = BASE_DIR.parent / "test"
PARQUET_FOLDER = BASE_DIR.parent / "parquet"

# ---------------------------------------------------------------------
# DuckLake attach / extensions
# ---------------------------------------------------------------------
def connect_ducklake(con):
    """
    Attach the DuckLake catalog as `my_ducklake` and load required extensions.

    Notes:
    - Uses f-strings for ATTACH only because DuckDB parameter binding is limited
      for some DDL statements. The paths come from constants (not user input).
    - Does NOT call `USE my_ducklake` here; callers can decide their default schema.
    """
    sql = f"""
        INSTALL ducklake;
        LOAD ducklake;

        ATTACH 'ducklake:{DUCKLAKE_METADATA.as_posix()}'
          AS my_ducklake (DATA_PATH '{DUCKLAKE_DATA.as_posix()}');

        INSTALL fts;
        LOAD fts;
    """
    con.execute(sql)

# ---------------------------------------------------------------------
# Sanity inspection (still useful during development)
# ---------------------------------------------------------------------
def test_ducklake(con):
    """
    Schema + sample rows + storage diagnostics for DuckLake tables.
    Helps explain large on-disk size by listing fragments and snapshots.
    """
    con.execute("USE my_ducklake")

    # ---------------- Schema + top-2 -------------------------------
    describe = con.execute("DESCRIBE my_ducklake.dict").fetch_df()
    print("Describe dict:\n", describe, "\n")
    top_dict = con.execute("SELECT * FROM my_ducklake.dict LIMIT 2").fetch_df()
    print("Top 2 rows in dict:\n", top_dict, "\n")

    describe = con.execute("DESCRIBE my_ducklake.docs").fetch_df()
    print("Describe docs:\n", describe, "\n")
    top_docs = con.execute("SELECT * FROM my_ducklake.docs LIMIT 2").fetch_df()
    print("Top 2 rows in docs:\n", top_docs, "\n")

    describe = con.execute("DESCRIBE my_ducklake.postings").fetch_df()
    print("Describe postings:\n", describe, "\n")
    top_post = con.execute("SELECT * FROM my_ducklake.postings LIMIT 2").fetch_df()
    print("Top 2 rows in postings:\n", top_post, "\n")

    

# ---------------------------------------------------------------------
# Lookups used by tokenize_query (the only per-term lookup we still need)
# ---------------------------------------------------------------------
def get_termid(con, term):
    """
    Return termid for a given term from my_ducklake.dict, or None if missing.
    Parameterized to avoid injection; term is a Python string.
    """
    row = con.execute(
        "SELECT termid FROM my_ducklake.dict WHERE term = ?",
        [term],
    ).fetchone()
    return row[0] if row else None

# ---------------------------------------------------------------------
# Tokenization helpers
# ---------------------------------------------------------------------
# precompiled regex: match contiguous alphabetic sequences
_WORD_RE = re.compile(r"[A-Za-z]+")

def tokenize(content: str) -> list[str]:
    """
    Extract lowercase alphabetic words from `content`.
    Digits, punctuation, and symbols are ignored.
    Returns a list of terms (str).
    Example:
        "AI-driven systems (2025)!" -> ["ai", "driven", "systems"]
    """
    if not content:
        return []
    return [m.group(0).lower() for m in _WORD_RE.finditer(content)]

def tokenize_query(con, query):
    """
    Tokenize a raw query string and map to existing termids.
    Unknown terms are dropped. Returns a list[int-like].
    """
    tokens = tokenize(query)
    # Inline walrus: lookup each token's termid, keep non-None
    termids = [tid for term in tokens if (tid := get_termid(con, term)) is not None]
    return termids

# ---------------------------------------------------------------------
# Data ingest / upsert
# ---------------------------------------------------------------------
def initialise_data(con, parquet="metadata_0.parquet", limit=None):
    """
    Create or replace my_ducklake.data from a source Parquet.

    - Safe and parameterized: uses read_parquet(?) for the path and LIMIT ?.
    - Drops any existing data table first to guarantee a clean base.
    - Applies ORDER BY docid for deterministic ingest.
    - Optional `limit` for reduced initial loads.
    """
    src = (PARQUET_FOLDER / parquet).resolve().as_posix()

    con.execute("USE my_ducklake")
    con.execute("DROP TABLE IF EXISTS data")

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

def import_data(con, parquet):
    """
    Upsert from Parquet using MERGE INTO (DuckLake). Path is parameterized.
    """
    src = (BASE_DIR.parent / "parquet" / parquet).resolve().as_posix()

    con.execute("USE my_ducklake")
    con.execute("""
        CREATE TABLE IF NOT EXISTS data (
            docid   BIGINT,
            content TEXT
        )
    """)

    # NOTE: VALUES (...) is required on the INSERT branch.
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
# -----------------------
# DuckLake maintenance: cleanup
# -----------------------
def cleanup_old_files(con, older_than_days=7, dry_run=True, all_files=False):
    """
    Delete files that DuckLake has scheduled for deletion (expired snapshots).
    Safer default: dry_run=True. Set all_files=True to ignore the 'older_than' filter.
    """
    try:
        days = int(older_than_days)
        if days <= 0:
            raise ValueError
    except Exception:
        raise SystemExit("ERROR: older_than_days must be a positive integer (days).")
    interval = f"{days} days"

    con.execute("USE my_ducklake")

    if all_files:
        con.execute("CALL ducklake_cleanup_old_files('my_ducklake', cleanup_all => true)")
        print("Cleanup (scheduled-for-deletion, ALL files) executed.")
        return

    if dry_run:
        con.execute(
            f"CALL ducklake_cleanup_old_files('my_ducklake', dry_run => true, older_than => now() - INTERVAL '{interval}')"
        )
        print(f"Cleanup DRY RUN (scheduled-for-deletion, older_than={interval}) listed files.")
    else:
        con.execute(
            f"CALL ducklake_cleanup_old_files('my_ducklake', older_than => now() - INTERVAL '{interval}')"
        )
        print(f"Cleanup (scheduled-for-deletion, older_than={interval}) executed.")


def cleanup_orphaned_files(con, older_than_days=7, dry_run=True, all_files=False):
    """
    Delete orphaned files (untracked by DuckLake). Safer default: dry_run=True.
    Set all_files=True to ignore the 'older_than' filter.
    """
    try:
        days = int(older_than_days)
        if days <= 0:
            raise ValueError
    except Exception:
        raise SystemExit("ERROR: older_than_days must be a positive integer (days).")
    interval = f"{days} days"

    con.execute("USE my_ducklake")

    if all_files:
        con.execute("CALL ducklake_delete_orphaned_files('my_ducklake', cleanup_all => true)")
        print("Cleanup (orphans, ALL files) executed.")
        return

    if dry_run:
        con.execute(
            f"CALL ducklake_delete_orphaned_files('my_ducklake', dry_run => true, older_than => now() - INTERVAL '{interval}')"
        )
        print(f"Cleanup DRY RUN (orphans, older_than={interval}) listed files.")
    else:
        con.execute(
            f"CALL ducklake_delete_orphaned_files('my_ducklake', older_than => now() - INTERVAL '{interval}')"
        )
        print(f"Cleanup (orphans, older_than={interval}) executed.")

