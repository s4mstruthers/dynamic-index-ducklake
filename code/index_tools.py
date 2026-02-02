# index_tools.py
# Build/replace BM25 index artifacts (dict/docs/postings) directly in DuckDB.
# Also supports point updates.

# ---------------------------------------------------------------------
# Tuning knobs (scale/perf)
# ---------------------------------------------------------------------
# Note: Parquet specific knobs (row group sizes, compression) removed.
# BATCH_SIZE kept if needed for other external fetch logic.
BATCH_SIZE = 50_000 

# ---------------------------------------------------------------------
# Public: Full Rebuild
# ---------------------------------------------------------------------
def reindex(con):
    """
    Rebuild index tables (dict, docs, postings) directly from my_ducklake.data
    using vectorized SQL.
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
    print("Building table -> my_ducklake.dict ...")
    con.execute("""
        CREATE OR REPLACE TABLE dict AS
        SELECT 
            row_number() OVER (ORDER BY term) AS termid, 
            term, 
            COUNT(DISTINCT docid) AS df
        FROM v_token_stream
        GROUP BY term
    """)

    # 3. Build Docs Index Table
    # Calculates length directly from data to capture documents with 0 tokens safely
    print("Building table -> my_ducklake.docs ...")
    con.execute("""
        CREATE OR REPLACE TABLE docs AS
        SELECT 
            docid, 
            len(regexp_extract_all(lower(content), '[a-z]+')) AS len
        FROM my_ducklake.data
    """)

    # 4. Build Postings Table
    # Joins the token stream with the Dictionary we just created
    print("Building table -> my_ducklake.postings ...")
    con.execute("""
        CREATE OR REPLACE TABLE postings AS
        SELECT 
            d.termid, 
            t.docid, 
            COUNT(*) AS tf
        FROM v_token_stream t
        JOIN my_ducklake.dict d ON t.term = d.term
        GROUP BY d.termid, t.docid
    """)
    
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