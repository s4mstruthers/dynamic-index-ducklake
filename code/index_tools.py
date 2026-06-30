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

    # Intentionally NOT wrapped in BEGIN/COMMIT: an explicit transaction around
    # these statements triggers IO/visibility race conditions with the ducklake
    # extension during file creation (see the retry logic in reindex()).

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
    # Uses row_number() for deterministic IDs (sorted by term).
    # DuckLake 1.0 note: CREATE TABLE (DDL) is kept separate from INSERT (DML) to
    # prevent DuckLake from pre-registering a parquet UUID in the catalog before
    # the physical file exists, which causes a non-deterministic IOException on
    # read-back.
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
    # Uses v_token_stream to avoid parsing content twice
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
            COUNT(*) AS len
        FROM v_token_stream
        GROUP BY docid
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

# ---------------------------------------------------------------------
# Point updates (delete/insert/modify)
# ---------------------------------------------------------------------
def delete(con, docid):
    """
    Delete a document and repair index structures with minimal churn.
    """
    con.execute("BEGIN")
    try:
        _delete_body(con, docid)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def _delete_body(con, docid):
    """
    Transaction-free core of delete(). The caller is responsible for the
    surrounding BEGIN/COMMIT so this can be reused inside modify() without
    nesting transactions.
    """
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

def delete_N(con, N, random=False):
    """
    Delete N documents in one transaction and repair the index structures in bulk.

    Args:
        N: Number of documents to delete.
        random: If True, delete a random sample of N documents; otherwise delete
                the first N by docid order. Random deletion is used by the
                performance harness to avoid bias from sequential docid layout.
    """
    selection = (
        "SELECT docid FROM my_ducklake.docs ORDER BY RANDOM() LIMIT ?" if random
        else "SELECT docid FROM my_ducklake.docs LIMIT ?"
    )

    con.execute("BEGIN")
    try:
        con.execute("DROP TABLE IF EXISTS del_docids")
        con.execute("DROP TABLE IF EXISTS touched_termids")

        # Choose the docids to delete
        con.execute("CREATE TEMP TABLE del_docids(docid BIGINT)")
        con.execute("INSERT INTO del_docids " + selection, [N])

        # Compute touched termids and how many of their docs are being removed
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

        # Decrement df by the number of deleted docs per term
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

        # Remove terms that no longer appear in any document
        con.execute(
            """
            DELETE FROM my_ducklake.dict
            WHERE df = 0
              AND termid IN (SELECT termid FROM touched_termids)
            """
        )

        # Bulk delete from the index and source tables
        con.execute("DELETE FROM my_ducklake.postings WHERE docid IN (SELECT docid FROM del_docids)")
        con.execute("DELETE FROM my_ducklake.docs     WHERE docid IN (SELECT docid FROM del_docids)")
        con.execute("DELETE FROM my_ducklake.data     WHERE docid IN (SELECT docid FROM del_docids)")

        con.execute("DROP TABLE IF EXISTS touched_termids")
        con.execute("DROP TABLE IF EXISTS del_docids")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

def insert(con, doc, docid=None):
    """
    Insert a new document and update the index in place.
    Returns the docid assigned to the new document.
    """
    con.execute("BEGIN")
    try:
        final_docid = _insert_body(con, doc, docid)
        con.execute("COMMIT")
        return final_docid
    except Exception:
        con.execute("ROLLBACK")
        raise


def _insert_body(con, doc, docid=None):
    """
    Transaction-free core of insert(). The caller manages BEGIN/COMMIT so this
    can be reused inside modify() without nesting transactions.
    """
    if docid is not None:
        exists = con.execute(
            "SELECT 1 FROM my_ducklake.docs WHERE docid = ?", [docid]
        ).fetchone()

        if exists:
            raise ValueError(
                f"DocID {docid} already exists. Please use modify() for updates."
            )

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
    # Only index documents that produced at least one token. This matches
    # reindex(), which builds `docs` from the token stream and therefore omits
    # token-less documents; the content is still stored in `data` below.
    con.execute("""
        INSERT INTO my_ducklake.docs (docid, len)
        SELECT docid, doc_len FROM target_doc
        WHERE doc_len > 0
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

    return final_docid


def modify(con, docid, content):
    """
    Replace the content of an existing document.

    Runs delete + insert within a SINGLE transaction by calling the
    transaction-free bodies directly, so the operation is atomic and does not
    nest BEGIN/COMMIT blocks.
    """
    con.execute("BEGIN")
    try:
        _delete_body(con, docid)
        new_id = _insert_body(con, content, docid=docid)
        con.execute("COMMIT")
        return new_id
    except Exception:
        con.execute("ROLLBACK")
        raise