# index_tools.py
# Build/replace BM25 index artifacts (dict/docs/postings) as Parquet,
# and load them into the DuckLake catalog. Also supports point updates.

from collections import Counter, defaultdict
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from helper_functions import PARQUET_FOLDER, tokenize

# ---------------------------------------------------------------------
# Tuning knobs (scale/perf)
# ---------------------------------------------------------------------
BATCH_SIZE = 50_000              # rows fetched per round-trip from DuckDB (unused in SQL mode)
DOCS_FLUSH_ROWS = 500_000        # docs rows per Parquet row group (unused in SQL mode)
POSTINGS_FLUSH_ROWS = 2_000_000  # postings rows per Parquet row group (unused in SQL mode)
PARQUET_COMPRESSION = "zstd"     # compression algo for all Parquet outputs
DETERMINISTIC_TERMIDS = False    # True => tid by lexicographic term (slower)

# ---------------------------------------------------------------------
# Parquet output locations (single-file artifacts; overwritten on build)
# ---------------------------------------------------------------------
PARQ_DIR  = PARQUET_FOLDER / "index"
PARQ_DICT = PARQ_DIR / "dict.parquet"
PARQ_DOCS = PARQ_DIR / "docs.parquet"
PARQ_POST = PARQ_DIR / "postings.parquet"

# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------
def _ensure_dirs():
    """Ensure Parquet output directory exists before writing artifacts."""
    PARQ_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# Public: full rebuild
# ---------------------------------------------------------------------
def build_index_to_parquet_from_ducklake(con):
    """
    Build index Parquets from my_ducklake.main.data using vectorized SQL.
    """
    _ensure_dirs()

    # 1. Create a transient view of all tokens (matching Python's regex [a-z]+)
    # We use regexp_extract_all to get a list, then UNNEST to explode it into rows.
    con.execute("USE my_ducklake")
    con.execute("""
        CREATE OR REPLACE TEMP VIEW v_token_stream AS 
        SELECT 
            docid, 
            UNNEST(regexp_extract_all(lower(content), '[a-z]+')) AS term
        FROM my_ducklake.data
    """)

    # 2. Build and Write Dictionary (dict.parquet)
    # Uses row_number() for deterministic IDs (sorted by term)
    print(f"Building dictionary -> {PARQ_DICT} ...")
    con.execute(f"""
        COPY (
            SELECT 
                row_number() OVER (ORDER BY term) AS termid, 
                term, 
                COUNT(DISTINCT docid) AS df
            FROM v_token_stream
            GROUP BY term
        ) TO '{PARQ_DICT}' (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}')
    """)

    # 3. Build and Write Docs Index (docs.parquet)
    # Calculates length directly from data to capture documents with 0 tokens safely
    print(f"Building docs index -> {PARQ_DOCS} ...")
    con.execute(f"""
        COPY (
            SELECT 
                docid, 
                len(regexp_extract_all(lower(content), '[a-z]+')) AS len
            FROM my_ducklake.data
        ) TO '{PARQ_DOCS}' (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}')
    """)

    # 4. Build and Write Postings (postings.parquet)
    # Joins the token stream with the Dictionary we just wrote
    print(f"Building postings -> {PARQ_POST} ...")
    con.execute(f"""
        COPY (
            SELECT 
                d.termid, 
                t.docid, 
                COUNT(*) AS tf
            FROM v_token_stream t
            JOIN '{PARQ_DICT}' d ON t.term = d.term
            GROUP BY d.termid, t.docid
        ) TO '{PARQ_POST}' (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}')
    """)
    
    # Cleanup view
    con.execute("DROP VIEW IF EXISTS v_token_stream")

# ---------------------------------------------------------------------
# Public: load Parquets into DuckLake
# ---------------------------------------------------------------------
def import_index_parquets_into_ducklake(con):
    """
    Replace my_ducklake.{dict,docs,postings} from local Parquet artifacts.
    Raises if any required artifact is missing.
    """
    for p in (PARQ_DICT, PARQ_DOCS, PARQ_POST):
        if not p.exists():
            raise FileNotFoundError(f"Missing parquet file: {p}")

    con.execute("USE my_ducklake")
    con.execute("BEGIN")
    try:
        con.execute(
            """
            CREATE OR REPLACE TABLE dict AS
            SELECT CAST(termid AS BIGINT) AS termid,
                   CAST(term   AS TEXT)   AS term,
                   CAST(df     AS BIGINT) AS df
            FROM read_parquet(?)
            """,
            [str(PARQ_DICT)],
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE docs AS
            SELECT CAST(docid AS BIGINT) AS docid,
                   CAST(len   AS BIGINT) AS len
            FROM read_parquet(?)
            """,
            [str(PARQ_DOCS)],
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE postings AS
            SELECT CAST(termid AS BIGINT) AS termid,
                   CAST(docid  AS BIGINT) AS docid,
                   CAST(tf     AS BIGINT) AS tf
            FROM read_parquet(?)
            """,
            [str(PARQ_POST)],
        )

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

# ---------------------------------------------------------------------
# Public: end-to-end rebuild
# ---------------------------------------------------------------------
def reindex(con):
    """
    Rebuild index from my_ducklake.main.data and publish into DuckLake.

    Steps:
      1) Stream data -> write Parquet artifacts (via optimized SQL)
      2) Load artifacts into my_ducklake.{dict,docs,postings}
    """
    

    build_index_to_parquet_from_ducklake(con)
    import_index_parquets_into_ducklake(con)

# ---------------------------------------------------------------------
# Point updates (delete/insert/modify)
# ---------------------------------------------------------------------
def delete(con, docid):
    """
    Delete a document and repair index structures with minimal churn.

    Strategy:
      - Capture touched termids, decrement df once per touched term.
      - Remove rows from postings/docs/data for the given docid.
      - Drop temp state and commit atomically.
    """
    con.execute("BEGIN")
    try:
        con.execute("DROP TABLE IF EXISTS touched_termids")
        con.execute("CREATE TEMP TABLE touched_termids(termid BIGINT)")

        con.execute(
            "INSERT INTO touched_termids "
            "SELECT DISTINCT termid FROM my_ducklake.postings WHERE docid = ?",
            [docid],
        )

        con.execute(
            """
            UPDATE my_ducklake.dict
            SET df = CASE WHEN df > 0 THEN df - 1 ELSE 0 END
            WHERE termid IN (SELECT termid FROM touched_termids)
            """
        )

        con.execute(
            """
            DELETE FROM my_ducklake.dict
            WHERE df = 0
              AND termid IN (SELECT termid FROM touched_termids)
            """
        )

        con.execute("DELETE FROM my_ducklake.postings WHERE docid = ?", [docid])
        con.execute("DELETE FROM my_ducklake.docs      WHERE docid = ?", [docid])
        con.execute("DELETE FROM my_ducklake.data      WHERE docid = ?", [docid])

        con.execute("DROP TABLE IF EXISTS touched_termids")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

def delete_N(con, N):
    """
    Delete N docs in one transaction and repair index structures in bulk.
    Strategy:
      - Choose top N docids into a TEMP table.
      - Derive touched termids and how many of the N docs they appear in.
      - Decrement df by those counts (clamped at 0).
      - Remove postings/docs/data rows in bulk.
      - Drop temp state and commit atomically.
    """
    con.execute("BEGIN")
    try:
        # Fresh temp tables
        con.execute("DROP TABLE IF EXISTS del_docids")
        con.execute("DROP TABLE IF EXISTS touched_termids")

        # Choose N random docids from docs
        con.execute("CREATE TEMP TABLE del_docids(docid BIGINT)")
        con.execute(
            "INSERT INTO del_docids "
            "SELECT docid FROM my_ducklake.docs LIMIT ?",
            [N],
        )

        # Compute touched termids with multiplicity (tf across all docs to be deleted) across the N docs
        con.execute("CREATE TEMP TABLE touched_termids(termid BIGINT, cnt BIGINT)")
        con.execute(
            """
            INSERT INTO touched_termids
            SELECT termid, COUNT(DISTINCT docid) AS cnt
            FROM my_ducklake.postings
            WHERE docid IN (SELECT docid FROM del_docids)
            GROUP BY termid
            """
        )

        # Decrement df for only touched termids (clamp at 0)
        con.execute(
            """
            UPDATE my_ducklake.dict AS d
            SET df = CASE
                        WHEN d.df > t.cnt THEN d.df - t.cnt
                        ELSE 0
                     END
            FROM touched_termids t
            WHERE d.termid = t.termid
            """
        )

        # Remove terms whose df reached 0 and hence dont need to be stored anymore from only those we touched
        con.execute(
            """
            DELETE FROM my_ducklake.dict
            WHERE df = 0
              AND termid IN (SELECT termid FROM touched_termids)
            """
        )

        # Bulk delete postings/docs/data for the selected docids
        con.execute(
            "DELETE FROM my_ducklake.postings WHERE docid IN (SELECT docid FROM del_docids)"
        )
        con.execute(
            "DELETE FROM my_ducklake.docs WHERE docid IN (SELECT docid FROM del_docids)"
        )
        con.execute(
            "DELETE FROM my_ducklake.data WHERE docid IN (SELECT docid FROM del_docids)"
        )

        # 6) Cleanup temp tables
        con.execute("DROP TABLE IF EXISTS touched_termids")
        con.execute("DROP TABLE IF EXISTS del_docids")

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

def delete_N_rand(con, N):
    """
    Delete N *random* docs in one transaction and repair index structures in bulk.
    Strategy:
      - Choose N random docids into a TEMP table.
      - Derive touched termids and how many of the N docs they appear in.
      - Decrement df by those counts (clamped at 0).
      - Remove postings/docs/data rows in bulk.
      - Drop temp state and commit atomically.
    """
    con.execute("BEGIN")
    try:
        # Fresh temp tables
        con.execute("DROP TABLE IF EXISTS del_docids")
        con.execute("DROP TABLE IF EXISTS touched_termids")

        # Choose N random docids from docs
        con.execute("CREATE TEMP TABLE del_docids(docid BIGINT)")
        con.execute(
            "INSERT INTO del_docids "
            "SELECT docid FROM my_ducklake.docs ORDER BY RANDOM() LIMIT ?",
            [N],
        )

        # Compute touched termids with multiplicity (tf across all docs to be deleted) across the N docs
        con.execute("CREATE TEMP TABLE touched_termids(termid BIGINT, cnt BIGINT)")
        con.execute(
            """
            INSERT INTO touched_termids
            SELECT termid, COUNT(DISTINCT docid) AS cnt
            FROM my_ducklake.postings
            WHERE docid IN (SELECT docid FROM del_docids)
            GROUP BY termid
            """
        )

        # Decrement df for only touched termids (clamp at 0)
        con.execute(
            """
            UPDATE my_ducklake.dict AS d
            SET df = CASE
                        WHEN d.df > t.cnt THEN d.df - t.cnt
                        ELSE 0
                     END
            FROM touched_termids t
            WHERE d.termid = t.termid
            """
        )

        # Remove terms whose df reached 0 and hence dont need to be stored anymore from only those we touched
        con.execute(
            """
            DELETE FROM my_ducklake.dict
            WHERE df = 0
              AND termid IN (SELECT termid FROM touched_termids)
            """
        )

        # Bulk delete postings/docs/data for the selected docids
        con.execute(
            "DELETE FROM my_ducklake.postings WHERE docid IN (SELECT docid FROM del_docids)"
        )
        con.execute(
            "DELETE FROM my_ducklake.docs WHERE docid IN (SELECT docid FROM del_docids)"
        )
        con.execute(
            "DELETE FROM my_ducklake.data WHERE docid IN (SELECT docid FROM del_docids)"
        )

        # 6) Cleanup temp tables
        con.execute("DROP TABLE IF EXISTS touched_termids")
        con.execute("DROP TABLE IF EXISTS del_docids")

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

def insert(con, doc, docid=None):
    """
    Insert a new document. 
    
    Guardrail:
    - If docid is None: Auto-increments.
    - If docid is Provided: It MUST NOT exist in the index yet.
      (This ensures updates only happen via modify(), which cleans up first).
    """

    # --- Guardrail: Prevent accidental corruption ---
    if docid is not None:
        # Check if this ID is already in use
        exists = con.execute(
            "SELECT 1 FROM my_ducklake.docs WHERE docid = ?", [docid]
        ).fetchone()
        
        if exists:
            raise ValueError(
                f"DocID {docid} already exists. You cannot overwrite it directly using insert(). "
                f"Please use modify(con, {docid}, content) instead, which handles cleanup safely."
            )

    con.execute("BEGIN")
    try:
        # 1. Stage the input
        con.execute("CREATE TEMP TABLE input_stage(docid BIGINT, content TEXT)")
        con.execute("INSERT INTO input_stage VALUES (?, ?)", [docid, doc])

        # 2. Resolve DocID and Length
        #    Logic: If input docid is NULL, calculate MAX+1.
        con.execute("""
            CREATE TEMP TABLE target_doc AS
            SELECT 
                COALESCE(i.docid, (SELECT COALESCE(MAX(d.docid), 0) + 1 FROM my_ducklake.docs d)) AS docid,
                i.content,
                len(regexp_extract_all(lower(i.content), '[a-z]+')) AS doc_len
            FROM input_stage i
        """)

        # 3. Tokenize and count TF (Pure SQL)
        con.execute("""
            CREATE TEMP TABLE doc_terms AS
            WITH raw_tokens AS (
                SELECT UNNEST(regexp_extract_all(lower(content), '[a-z]+')) AS term
                FROM input_stage
            )
            SELECT term, COUNT(*) AS tf
            FROM raw_tokens
            GROUP BY term
        """)

        # 4. Upsert Dictionary (merging with current index)
        #    We are adding counts to existing words or creating new ones.
        con.execute("""
            WITH base AS (SELECT COALESCE(MAX(termid), 0) AS max_id FROM my_ducklake.dict),
            annotated AS (
                SELECT 
                    dt.term,
                    d.termid,
                    ROW_NUMBER() OVER (PARTITION BY (d.termid IS NULL) ORDER BY dt.term) AS rn
                FROM doc_terms dt
                LEFT JOIN my_ducklake.dict d ON dt.term = d.term
            ),
            source AS (
                SELECT
                    term,
                    COALESCE(termid, (SELECT max_id FROM base) + rn) AS final_termid
                FROM annotated
            )
            MERGE INTO my_ducklake.dict AS tgt
            USING source
            ON (tgt.term = source.term)
            WHEN MATCHED THEN 
                UPDATE SET df = tgt.df + 1
            WHEN NOT MATCHED THEN 
                INSERT (termid, term, df) VALUES (source.final_termid, source.term, 1)
        """)

        # 5. Insert Docs
        con.execute("""
            INSERT INTO my_ducklake.docs (docid, len)
            SELECT docid, doc_len FROM target_doc
        """)

        # 6. Insert Postings
        con.execute("""
            INSERT INTO my_ducklake.postings (termid, docid, tf)
            SELECT d.termid, td.docid, dt.tf
            FROM doc_terms dt
            JOIN target_doc td ON 1=1
            JOIN my_ducklake.dict d ON dt.term = d.term
        """)

        # 7. Insert Content
        con.execute("""
            INSERT INTO my_ducklake.data (docid, content)
            SELECT docid, content FROM target_doc
        """)

        # 8. Retrieve final docid
        final_docid = con.execute("SELECT docid FROM target_doc").fetchone()[0]

        # Cleanup
        con.execute("DROP TABLE input_stage")
        con.execute("DROP TABLE target_doc")
        con.execute("DROP TABLE doc_terms")

        con.execute("COMMIT")
        return final_docid

    except Exception:
        con.execute("ROLLBACK")
        raise

def modify(con, docid, content):
    """
    Replace content for an existing docid by delete+insert in one transaction.

    Returns:
      - docid of the updated document (same as input).
    """
    con.execute("BEGIN")
    try:
        delete(con, docid)
        new_id = insert(con, content, docid=docid)
        con.execute("COMMIT")
        return new_id
    except Exception:
        con.execute("ROLLBACK")
        raise