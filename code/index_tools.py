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
    import os
    threads = max(1, (os.cpu_count() or 1))
    con.execute(f"PRAGMA threads={threads};")

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
    Insert/replace a document and upsert associated index rows.

    - Tokenizes content, computes TFs per term.
    - Assigns a new docid if not provided (MAX(docid)+1).
    - Upserts dict/df, docs/len, postings/tf, and data/content.
    Returns the final docid.
    """
    tokens = tokenize(doc)
    if not tokens:
        return None

    tf = Counter(tokens)
    distinct_terms = list(tf.keys())
    doc_len = len(tokens)

    con.execute("BEGIN")
    try:
        # Stage terms and tf counts for this document
        con.execute("CREATE TEMP TABLE src_terms(term TEXT, tf BIGINT)")
        con.executemany(
            "INSERT INTO src_terms(term, tf) VALUES (?, ?)",
            [(t, tf[t]) for t in distinct_terms],
        )

        # Resolve docid: fixed if provided, else MAX+1 from docs
        con.execute(
            """
            CREATE TEMP TABLE docid_sql AS
            SELECT
                COALESCE(?, (SELECT COALESCE(MAX(docid), 0) + 1 FROM my_ducklake.docs)) AS docid
            """,
            [docid],
        )

        # Upsert dict (assign new termids for unseen terms, bump df for seen)
        con.execute(
            """
            WITH base AS (SELECT COALESCE(MAX(termid), 0) AS base FROM my_ducklake.dict),
            dedup AS (SELECT DISTINCT term, tf FROM src_terms),
            annot AS (
                SELECT
                    d.termid AS existing_tid,
                    s.term,
                    s.tf,
                    CASE WHEN d.termid IS NULL
                         THEN (SELECT base FROM base) + ROW_NUMBER() OVER (ORDER BY s.term)
                         ELSE d.termid
                    END AS ins_tid
                FROM dedup s
                LEFT JOIN my_ducklake.dict d ON d.term = s.term
            )
            MERGE INTO my_ducklake.dict AS tgt
            USING annot AS a
            ON (tgt.term = a.term)
            WHEN MATCHED THEN UPDATE SET df = tgt.df + 1
            WHEN NOT MATCHED THEN INSERT (termid, term, df) VALUES (a.ins_tid, a.term, 1)
            """
        )

        # Upsert docs row (len)
        con.execute(
            """
            MERGE INTO my_ducklake.docs AS d
            USING (SELECT docid FROM docid_sql) AS s
            ON (d.docid = s.docid)
            WHEN MATCHED THEN UPDATE SET len = ?
            WHEN NOT MATCHED THEN INSERT (docid, len) VALUES (s.docid, ?)
            """,
            [doc_len, doc_len],
        )

        # Upsert postings for this doc
        con.execute(
            """
            WITH term_ids AS (
                SELECT d.termid, s.term, s.tf
                FROM src_terms s
                JOIN my_ducklake.dict d ON d.term = s.term
            ),
            src_post AS (
                SELECT t.termid, ds.docid, t.tf
                FROM term_ids t
                CROSS JOIN docid_sql ds
            )
            MERGE INTO my_ducklake.postings AS p
            USING src_post AS sp
            ON (p.termid = sp.termid AND p.docid = sp.docid)
            WHEN MATCHED THEN UPDATE SET tf = sp.tf
            WHEN NOT MATCHED THEN INSERT (termid, docid, tf) VALUES (sp.termid, sp.docid, sp.tf)
            """
        )

        # Upsert raw content
        con.execute(
            """
            MERGE INTO my_ducklake.data AS t
            USING (SELECT docid, ? AS content FROM docid_sql) AS s
            ON (t.docid = s.docid)
            WHEN MATCHED THEN UPDATE SET content = s.content
            WHEN NOT MATCHED THEN INSERT (docid, content) VALUES (s.docid, s.content)
            """,
            [doc],
        )

        final_docid = con.execute("SELECT docid FROM docid_sql").fetchone()[0]

        con.execute("DROP TABLE IF EXISTS src_terms")
        con.execute("DROP TABLE IF EXISTS docid_sql")

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