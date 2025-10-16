from collections import Counter, defaultdict
from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from helper_functions import PARQUET_FOLDER, tokenize

# ---- knobs for scale ----
BATCH_SIZE = 50_000                  # rows per fetchmany from DuckDB
DOCS_FLUSH_ROWS = 500_000            # rows per Parquet row group for docs
POSTINGS_FLUSH_ROWS = 2_000_000      # rows per Parquet row group for postings
PARQUET_COMPRESSION = "zstd"         # good ratio/speed
DETERMINISTIC_TERMIDS = False        # True = slower, stable termids by lexicographic term order

# -------------------------------------------------------------------
# Parquet output locations (single files; overwritten on each build)
# -------------------------------------------------------------------
PARQ_DIR  = PARQUET_FOLDER / "index"
PARQ_DICT = PARQ_DIR / "dict.parquet"
PARQ_DOCS = PARQ_DIR / "docs.parquet"
PARQ_POST = PARQ_DIR / "postings.parquet"

# --------------- helpers -------------------
def _ensure_dirs():
    PARQ_DIR.mkdir(parents=True, exist_ok=True)


def _iter_data(con):
    """
    Stream (docid, content) rows from my_ducklake.main.data.
    Uses fetchmany(BATCH_SIZE) to keep round-trips modest.
    """
    con.execute("USE my_ducklake")
    con.execute("SELECT docid, content FROM main.data")
    fetch = con.fetchmany
    while True:
        batch = fetch(BATCH_SIZE)
        if not batch:
            break
        # fast tuple-unpack in Python loop
        for docid, content in batch:
            yield docid, content

def _build_index(rows):
    """
    Python inverted index. Returns three plain lists of tuples:
      dict_rows:    [(termid, term, df)]
      docs_rows:    [(docid, len)]
      postings_rows:[(termid, docid, tf)]
    """
    term_to_id = {}
    df_counter = defaultdict(int)
    docs_table = []
    postings_table = []

    next_tid = 1
    append_doc = docs_table.append
    append_post = postings_table.append

    # First pass: assign ids (arrival order), collect postings and df
    for docid, content in rows:
        text = "" if content is None else str(content)
        tokens = tokenize(text)

        append_doc((docid, len(tokens)))
        if not tokens:
            continue

        tf = Counter(tokens)
        for term, freq in tf.items():
            tid = term_to_id.setdefault(term, next_tid)
            if tid == next_tid:
                next_tid += 1
            append_post((tid, docid, freq))

        # iterate keys directly (no .keys())
        for term in tf:
            df_counter[term_to_id[term]] += 1

    # Optional second pass to make termids deterministic by term order
    if DETERMINISTIC_TERMIDS:
        # remap TIDs by lexicographic order of terms
        sorted_terms = sorted(term_to_id.keys())
        remap = {term_to_id[t]: i+1 for i, t in enumerate(sorted_terms)}
        # remap postings
        postings_table = [(remap[tid], docid, tf) for (tid, docid, tf) in postings_table]
        # rebuild df_counter under new ids
        df_counter = defaultdict(int, {remap[tid]: df_counter[tid] for tid in df_counter})
        # rebuild dict_table deterministically
        dict_table = [(i+1, t, df_counter.get(i+1, 0)) for i, t in enumerate(sorted_terms)]
    else:
        # build dict_table as encountered
        dict_table = [(tid, term, df_counter.get(tid, 0)) for term, tid in term_to_id.items()]

    return dict_table, docs_table, postings_table

# --------------- public API ----------------

def build_index_to_parquet_from_ducklake(con):
    """
    Stream ALL rows from my_ducklake.main.data, build index,
    and write Parquet incrementally (docs/postings). The dict is finalized at end.

    Memory profile: O(#distinct terms) + O(buffer sizes), not O(#postings).
    """

    _ensure_dirs()

    # Remove old files first to guarantee single file per artifact
    for p in (PARQ_DICT, PARQ_DOCS, PARQ_POST):
        try:
            p.unlink()
        except FileNotFoundError:
            pass

    # --------- state for dictionary and doc stats ----------
    term_to_id = {}                  # term -> termid
    df_counter = defaultdict(int)    # termid -> df
    next_tid = 1

    # --------- postings/doc buffers ----------
    docs_docid_buf = []
    docs_len_buf = []
    docs_rows_in_buf = 0

    post_tid_buf = []
    post_docid_buf = []
    post_tf_buf = []
    post_rows_in_buf = 0

    # --------- Parquet writers (streaming) ----------
    # docs schema
    docs_schema = pa.schema([
        pa.field("docid", pa.int64()),
        pa.field("len", pa.int64()),
    ])
    docs_writer = pq.ParquetWriter(
        where=str(PARQ_DOCS),
        schema=docs_schema,
        compression=PARQUET_COMPRESSION,
        write_statistics=True
    )

    # postings schema
    post_schema = pa.schema([
        pa.field("termid", pa.int64()),
        pa.field("docid", pa.int64()),
        pa.field("tf", pa.int64()),
    ])
    post_writer = pq.ParquetWriter(
        where=str(PARQ_POST),
        schema=post_schema,
        compression=PARQUET_COMPRESSION,
        write_statistics=True
    )

    # --------- local bindings to reduce attribute lookups ----------
    append_docs_id = docs_docid_buf.append
    append_docs_len = docs_len_buf.append
    append_post_tid = post_tid_buf.append
    append_post_doc = post_docid_buf.append
    append_post_tf  = post_tf_buf.append
    tokenize_fn = tokenize
    setdefault = dict.setdefault

    # --------- loop over data ----------
    for docid, content in _iter_data(con):
        text = "" if content is None else str(content)
        toks = tokenize_fn(text)

        # docs row
        append_docs_id(int(docid))
        append_docs_len(len(toks))
        docs_rows_in_buf += 1

        # flush docs if needed
        if docs_rows_in_buf >= DOCS_FLUSH_ROWS:
            table = pa.Table.from_arrays(
                [
                    pa.array(np.asarray(docs_docid_buf, dtype=np.int64)),
                    pa.array(np.asarray(docs_len_buf, dtype=np.int64)),
                ],
                schema=docs_schema
            )
            docs_writer.write_table(table)
            docs_docid_buf.clear()
            docs_len_buf.clear()
            docs_rows_in_buf = 0

        # empty doc? skip postings
        if not toks:
            continue

        tf = Counter(toks)

        # postings + df
        for term, freq in tf.items():
            tid = setdefault(term_to_id, term, next_tid)
            if tid == next_tid:
                next_tid += 1
            append_post_tid(tid)
            append_post_doc(int(docid))
            append_post_tf(int(freq))
            post_rows_in_buf += 1

        # df once per distinct term
        for term in tf:
            df_counter[term_to_id[term]] += 1

        # flush postings if needed
        if post_rows_in_buf >= POSTINGS_FLUSH_ROWS:
            table = pa.Table.from_arrays(
                [
                    pa.array(np.asarray(post_tid_buf, dtype=np.int64)),
                    pa.array(np.asarray(post_docid_buf, dtype=np.int64)),
                    pa.array(np.asarray(post_tf_buf, dtype=np.int64)),
                ],
                schema=post_schema
            )
            post_writer.write_table(table)
            post_tid_buf.clear()
            post_docid_buf.clear()
            post_tf_buf.clear()
            post_rows_in_buf = 0

    # --------- final flushes ----------
    if docs_rows_in_buf:
        table = pa.Table.from_arrays(
            [
                pa.array(np.asarray(docs_docid_buf, dtype=np.int64)),
                pa.array(np.asarray(docs_len_buf, dtype=np.int64)),
            ],
            schema=docs_schema
        )
        docs_writer.write_table(table)

    if post_rows_in_buf:
        table = pa.Table.from_arrays(
            [
                pa.array(np.asarray(post_tid_buf, dtype=np.int64)),
                pa.array(np.asarray(post_docid_buf, dtype=np.int64)),
                pa.array(np.asarray(post_tf_buf, dtype=np.int64)),
            ],
            schema=post_schema
        )
        post_writer.write_table(table)

    docs_writer.close()
    post_writer.close()

    # --------- build/write dict.parquet (small) ----------
    if DETERMINISTIC_TERMIDS:
        # stable: sort by term, remap ids
        sorted_terms = sorted(term_to_id.keys())
        remap = {term_to_id[t]: i + 1 for i, t in enumerate(sorted_terms)}
        # rebuild df under new ids
        df_counter = defaultdict(int, {remap[tid]: df_counter[tid] for tid in df_counter})
        termids = np.asarray([i + 1 for i in range(len(sorted_terms))], dtype=np.int64)
        terms = pa.array(sorted_terms, type=pa.string())
        dfs = np.asarray([df_counter.get(i + 1, 0) for i in range(len(sorted_terms))], dtype=np.int64)
    else:
        # fast: arrival order
        items = list(term_to_id.items())  # (term, tid)
        # unpack into aligned arrays
        terms_list = [t for t, _ in items]
        termids = np.asarray([tid for _, tid in items], dtype=np.int64)
        terms = pa.array(terms_list, type=pa.string())
        dfs = np.asarray([df_counter.get(tid, 0) for tid in termids], dtype=np.int64)

    dict_table = pa.Table.from_arrays(
        [pa.array(termids), terms, pa.array(dfs)],
        names=["termid", "term", "df"]
    )
    pq.write_table(
        dict_table,
        where=str(PARQ_DICT),
        compression=PARQUET_COMPRESSION,
        write_statistics=True,
        use_dictionary=True  # encode all suitable columns
    )

def import_index_parquets_into_ducklake(con):
    for p in (PARQ_DICT, PARQ_DOCS, PARQ_POST):
        if not p.exists():
            raise FileNotFoundError(f"Missing parquet file: {p}")

    con.execute("USE my_ducklake")
    con.execute("BEGIN")
    try:
        con.execute("""
            CREATE OR REPLACE TABLE dict AS
            SELECT CAST(termid AS BIGINT) AS termid,
                   CAST(term   AS TEXT)   AS term,
                   CAST(df     AS BIGINT) AS df
            FROM read_parquet(?)
        """, [str(PARQ_DICT)])

        con.execute("""
            CREATE OR REPLACE TABLE docs AS
            SELECT CAST(docid AS BIGINT) AS docid,
                   CAST(len   AS BIGINT) AS len
            FROM read_parquet(?)
        """, [str(PARQ_DOCS)])

        con.execute("""
            CREATE OR REPLACE TABLE postings AS
            SELECT CAST(termid AS BIGINT) AS termid,
                   CAST(docid  AS BIGINT) AS docid,
                   CAST(tf     AS BIGINT) AS tf
            FROM read_parquet(?)
        """, [str(PARQ_POST)])

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

def reindex(con):
    """
    Reindex my_ducklake.main.data:
      - Overwrite dict/docs/postings Parquet files
      - Replace DuckLake tables from those Parquets
    """
    import os
    # Let DuckDB parallelize downstream ops safely
    threads = max(1, (os.cpu_count() or 1))
    con.execute(f"PRAGMA threads={threads};")

    build_index_to_parquet_from_ducklake(con)
    import_index_parquets_into_ducklake(con)

# ----------------------- Update tools -------------------------------
# (unchanged below, but see NOTE on df overcount for insert with existing docid)

def delete(con, docid):
    con.execute("BEGIN")
    try:
        con.execute("""
            MERGE INTO my_ducklake.dict AS dict_tbl
            USING (
                SELECT DISTINCT termid
                FROM my_ducklake.postings
                WHERE docid = ?
            ) AS touched
            ON (dict_tbl.termid = touched.termid)
            WHEN MATCHED THEN UPDATE SET df = CASE WHEN dict_tbl.df > 0 THEN dict_tbl.df - 1 ELSE 0 END
        """, [docid])

        con.execute("""
            DELETE FROM my_ducklake.dict
            WHERE df = 0
              AND termid IN (SELECT DISTINCT termid FROM my_ducklake.postings WHERE docid = ?)
        """, [docid])

        con.execute("""
            MERGE INTO my_ducklake.postings AS p
            USING (SELECT ? AS docid) AS s
            ON (p.docid = s.docid)
            WHEN MATCHED THEN DELETE
        """, [docid])

        con.execute("""
            MERGE INTO my_ducklake.docs AS d
            USING (SELECT ? AS docid) AS s
            ON (d.docid = s.docid)
            WHEN MATCHED THEN DELETE
        """, [docid])

        con.execute("""
            MERGE INTO my_ducklake.data AS t
            USING (SELECT ? AS docid) AS s
            ON (t.docid = s.docid)
            WHEN MATCHED THEN DELETE
        """, [docid])

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

def insert(con, doc, docid=None):
    tokens = tokenize(doc)
    if not tokens:
        return None

    tf = Counter(tokens)
    distinct_terms = list(tf.keys())
    doc_len = len(tokens)

    con.execute("BEGIN")
    try:
        con.execute("CREATE TEMP TABLE src_terms(term TEXT, tf BIGINT)")
        con.executemany("INSERT INTO src_terms(term, tf) VALUES (?, ?)", [(t, tf[t]) for t in distinct_terms])

        con.execute("""
            CREATE TEMP TABLE docid_sql AS
            SELECT
                COALESCE(?, (SELECT COALESCE(MAX(docid), 0) + 1 FROM my_ducklake.docs)) AS docid
        """, [docid])

        con.execute("""
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
        """)

        con.execute("""
            MERGE INTO my_ducklake.docs AS d
            USING (SELECT docid FROM docid_sql) AS s
            ON (d.docid = s.docid)
            WHEN MATCHED THEN UPDATE SET len = ?
            WHEN NOT MATCHED THEN INSERT (docid, len) VALUES (s.docid, ?)
        """, [doc_len, doc_len])

        con.execute("""
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
        """)

        con.execute("""
            MERGE INTO my_ducklake.data AS t
            USING (SELECT docid, ? AS content FROM docid_sql) AS s
            ON (t.docid = s.docid)
            WHEN MATCHED THEN UPDATE SET content = s.content
            WHEN NOT MATCHED THEN INSERT (docid, content) VALUES (s.docid, s.content)
        """, [doc])

        final_docid = con.execute("SELECT docid FROM docid_sql").fetchone()[0]

        con.execute("DROP TABLE IF EXISTS src_terms")
        con.execute("DROP TABLE IF EXISTS docid_sql")

        con.execute("COMMIT")
        return final_docid
    except Exception:
        con.execute("ROLLBACK")
        raise

def modify(con, docid, content):
    con.execute("BEGIN")
    try:
        delete(con, docid)
        new_id = insert(con, content, docid=docid)
        con.execute("COMMIT")
        return new_id
    except Exception:
        con.execute("ROLLBACK")
        raise