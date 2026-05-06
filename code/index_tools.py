# index_tools.py
# Build/replace BM25 index artifacts (dict/docs/postings) directly in DuckDB.
# Also supports point updates.

import time
import duckdb

# ---------------------------------------------------------------------
# Public: Full Rebuild
# ---------------------------------------------------------------------
def reindex(con, max_retries=5):
    """
    Rebuild index tables (dict, docs, postings) directly from my_ducklake.data
    using vectorized SQL.

    Retries up to max_retries times on DuckLake 1.0 IOException.
    Root cause: DuckLake 1.0 pre-registers parquet file UUIDs in the metadata
    catalog before the physical file is written to disk. Any write operation
    (CREATE TABLE AS or INSERT INTO) can hit a race condition where the engine
    tries to open a file that has been registered but not yet flushed. A brief
    pause + retry clears the condition.
    """
    for attempt in range(1, max_retries + 1):
        try:
            _reindex_impl(con)
            return
        except duckdb.IOException as e:
            if "Cannot open file" not in str(e) or attempt == max_retries:
                raise
            print(f"  [reindex attempt {attempt}/{max_retries}] "
                  f"DuckLake file-sync race condition — cleaning up and retrying in {attempt}s...")
            _drop_index_tables(con)
            time.sleep(attempt)  # progressive back-off: 1 s, 2 s, 3 s …


def _drop_index_tables(con):
    """Drop partially-built index tables so a retry starts from a clean state."""
    for obj in ("VIEW v_token_stream", "TABLE postings", "TABLE docs", "TABLE dict"):
        try:
            con.execute(f"DROP {obj} IF EXISTS")
        except Exception:
            pass
    try:
        con.execute("CHECKPOINT")
    except Exception:
        pass


def _reindex_impl(con):
    """
    Single attempt at a full index rebuild. Called by reindex(); not intended
    for direct use.
    """
    con.execute("USE my_ducklake")
    
    # REMOVED: con.execute("BEGIN") to prevent IO/visibility race conditions
    # with the ducklake extension during file creation.

    # 1. Create a transient view of all tokens (matching Python's regex [a-z]+)
    # We use regexp_extract_all to get a list, then UNNEST to explode it into rows.
    con.execute("""
        CREATE OR REPLACE TEMP VIEW v_token_stream AS 
        SELECT 
            docid, 
            UNNEST(regexp_extract_all(lower(content), '[a-z]+')) AS term
        FROM my_ducklake.data
    """)

    # 2. Build Dictionary Table
    # Uses row_number() for deterministic IDs (sorted by term)
    # FIX (DuckLake 1.0): Split CREATE TABLE (DDL) from INSERT (DML) to prevent
    # DuckLake from pre-registering a parquet UUID in the catalog before the
    # physical file exists, which causes a non-deterministic IOException on read-back.
    print("Building table -> my_ducklake.dict ...")
    con.execute("DROP TABLE IF EXISTS dict")
    con.execute("""
        CREATE TABLE dict (
            termid BIGINT,
            term   VARCHAR,
            df     BIGINT
        )
    """)
    con.execute("CHECKPOINT")
    con.execute("""
        INSERT INTO dict
        SELECT
            row_number() OVER (ORDER BY term) AS termid,
            term,
            COUNT(DISTINCT docid) AS df
        FROM v_token_stream
        GROUP BY term
    """)
    con.execute("CHECKPOINT")

    # 3. Build Docs Index Table
    # Calculates length directly from data to capture documents with 0 tokens safely
    print("Building table -> my_ducklake.docs ...")
    con.execute("DROP TABLE IF EXISTS docs")
    con.execute("""
        CREATE TABLE docs (
            docid BIGINT,
            len   BIGINT
        )
    """)
    con.execute("CHECKPOINT")
    con.execute("""
        INSERT INTO docs
        SELECT
            docid,
            len(regexp_extract_all(lower(content), '[a-z]+')) AS len
        FROM my_ducklake.data
    """)
    con.execute("CHECKPOINT")

    # 4. Build Postings Table
    # Joins the token stream with the Dictionary we just created
    print("Building table -> my_ducklake.postings ...")
    con.execute("DROP TABLE IF EXISTS postings")
    con.execute("""
        CREATE TABLE postings (
            termid BIGINT,
            docid  BIGINT,
            tf     BIGINT
        )
    """)
    con.execute("CHECKPOINT")
    con.execute("""
        INSERT INTO postings
        SELECT
            d.termid,
            t.docid,
            COUNT(*) AS tf
        FROM v_token_stream t
        JOIN my_ducklake.dict d ON t.term = d.term
        GROUP BY d.termid, t.docid
    """)
    con.execute("CHECKPOINT")

    # Cleanup view
    con.execute("DROP VIEW IF EXISTS v_token_stream")
    
    # REMOVED: con.execute("COMMIT")

# ---------------------------------------------------------------------
# Point updates (delete/insert/modify)
# ---------------------------------------------------------------------
def delete(con, docid):
    """
    Delete a document and repair index structures with minimal churn.
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
    """
    con.execute("BEGIN")
    try:
        con.execute("DROP TABLE IF EXISTS del_docids")
        con.execute("DROP TABLE IF EXISTS touched_termids")

        # Choose N docids
        con.execute("CREATE TEMP TABLE del_docids(docid BIGINT)")
        con.execute(
            "INSERT INTO del_docids "
            "SELECT docid FROM my_ducklake.docs LIMIT ?",
            [N],
        )

        # Compute touched termids
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

        # Decrement df
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

        # Cleanup zero df terms
        con.execute(
            """
            DELETE FROM my_ducklake.dict
            WHERE df = 0
              AND termid IN (SELECT termid FROM touched_termids)
            """
        )

        # Bulk delete
        con.execute("DELETE FROM my_ducklake.postings WHERE docid IN (SELECT docid FROM del_docids)")
        con.execute("DELETE FROM my_ducklake.docs WHERE docid IN (SELECT docid FROM del_docids)")
        con.execute("DELETE FROM my_ducklake.data WHERE docid IN (SELECT docid FROM del_docids)")

        con.execute("DROP TABLE IF EXISTS touched_termids")
        con.execute("DROP TABLE IF EXISTS del_docids")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

def delete_N_rand(con, N):
    """
    Delete N *random* docs in one transaction and repair index structures in bulk.
    """
    con.execute("BEGIN")
    try:
        con.execute("DROP TABLE IF EXISTS del_docids")
        con.execute("DROP TABLE IF EXISTS touched_termids")

        # Choose N random docids
        con.execute("CREATE TEMP TABLE del_docids(docid BIGINT)")
        con.execute(
            "INSERT INTO del_docids "
            "SELECT docid FROM my_ducklake.docs ORDER BY RANDOM() LIMIT ?",
            [N],
        )

        # Compute touched termids
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

        # Decrement df
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

        # Cleanup zero df terms
        con.execute(
            """
            DELETE FROM my_ducklake.dict
            WHERE df = 0
              AND termid IN (SELECT termid FROM touched_termids)
            """
        )

        # Bulk delete
        con.execute("DELETE FROM my_ducklake.postings WHERE docid IN (SELECT docid FROM del_docids)")
        con.execute("DELETE FROM my_ducklake.docs WHERE docid IN (SELECT docid FROM del_docids)")
        con.execute("DELETE FROM my_ducklake.data WHERE docid IN (SELECT docid FROM del_docids)")

        con.execute("DROP TABLE IF EXISTS touched_termids")
        con.execute("DROP TABLE IF EXISTS del_docids")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

def insert(con, doc, docid=None):
    """
    Insert a new document. 
    """
    if docid is not None:
        exists = con.execute(
            "SELECT 1 FROM my_ducklake.docs WHERE docid = ?", [docid]
        ).fetchone()
        
        if exists:
            raise ValueError(
                f"DocID {docid} already exists. Please use modify() for updates."
            )

    con.execute("BEGIN")
    try:
        # 1. Stage the input
        con.execute("CREATE TEMP TABLE input_stage(docid BIGINT, content TEXT)")
        con.execute("INSERT INTO input_stage VALUES (?, ?)", [docid, doc])

        # 2. Resolve DocID and Length
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