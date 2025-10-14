# index_tools.py
from __future__ import annotations

import duckdb
import pandas as pd
from collections import Counter, defaultdict
from pathlib import Path

# Only import small utilities from helper_functions to avoid cycles.
# DO NOT import index_tools back in helper_functions at module top-level.
from helper_functions import PARQUET_FOLDER, tokenize

# -------------------------------------------------------------------
# Parquet output locations (single files; overwritten on each build)
# -------------------------------------------------------------------
PARQ_DIR: Path  = PARQUET_FOLDER / "index"
PARQ_DICT: Path = PARQ_DIR / "dict.parquet"
PARQ_DOCS: Path = PARQ_DIR / "docs.parquet"
PARQ_POST: Path = PARQ_DIR / "postings.parquet"


# ----------------------- internal helpers --------------------------

def _ensure_dirs() -> None:
    """
    Ensure the index output directory exists.
    Writes: .../parquet/index/{dict,docs,postings}.parquet
    """
    PARQ_DIR.mkdir(parents=True, exist_ok=True)


def _fetch_data_df(con: duckdb.DuckDBPyConnection, limit: int | None = None) -> pd.DataFrame:
    """
    Pull (docid, content) from DuckLake virtual table my_ducklake.main.data.
    NOTE: This assumes helper_functions.connect_ducklake() has already attached the DB.
    """
    con.execute("USE my_ducklake")
    sql = "SELECT docid, content FROM main.data"
    return con.execute(sql + (" LIMIT ?" if limit is not None else ""), (int(limit),) if limit else ()).fetchdf()


def _build_index(df: pd.DataFrame):
    """
    Build inverted index *purely in Python* (no PK/FK, no constraints).

    Produces three lists of rows:
      - dict_rows:    [(termid, term, df)]
      - docs_rows:    [(docid, len)]
      - postings_rows:[(termid, docid, tf)]

    Definitions:
      df = number of distinct documents containing the term.
      len = token count of the document.
      tf = term frequency of the term in the document.
    """
    term_to_id: dict[str, int] = {}
    df_counter: defaultdict[int, int] = defaultdict(int)  # termid -> df
    docs_rows: list[tuple[int, int]] = []
    postings_rows: list[tuple[int, int, int]] = []

    next_tid = 1

    for docid, content in zip(df["docid"].tolist(), df["content"].tolist()):
        text = "" if content is None else str(content)
        toks = tokenize(text)

        # docs.len
        docs_rows.append((int(docid), len(toks)))

        if not toks:
            continue

        tf = Counter(toks)  # per-document TF for each term

        # postings + assign termids
        for term, freq in tf.items():
            tid = term_to_id.setdefault(term, next_tid)
            if tid == next_tid:
                next_tid += 1
            postings_rows.append((tid, int(docid), int(freq)))

        # document frequency (once per distinct term)
        for term in tf.keys():
            df_counter[term_to_id[term]] += 1

    # finalize dict rows
    dict_rows = [(tid, term, int(df_counter.get(tid, 0))) for term, tid in term_to_id.items()]
    return dict_rows, docs_rows, postings_rows


def _overwrite_parquet(path: Path, frame: pd.DataFrame) -> None:
    """
    Overwrite a single Parquet file (delete previous file if it exists).
    Guarantees a single file per table.
    """
    if path.exists():
        path.unlink()
    frame.to_parquet(path, index=False)


# ----------------------- public API --------------------------------

def build_index_to_parquet_from_ducklake(con: duckdb.DuckDBPyConnection, limit: int | None = None) -> None:
    """
    Read `my_ducklake.main.data` → build index in memory → write
    single-file Parquets: dict.parquet, docs.parquet, postings.parquet.
    """
    _ensure_dirs()
    df = _fetch_data_df(con, limit)
    dict_rows, docs_rows, postings_rows = _build_index(df)

    _overwrite_parquet(PARQ_DICT, pd.DataFrame(dict_rows,  columns=["termid", "term", "df"]))
    _overwrite_parquet(PARQ_DOCS, pd.DataFrame(docs_rows,  columns=["docid", "len"]))
    _overwrite_parquet(PARQ_POST, pd.DataFrame(postings_rows, columns=["termid", "docid", "tf"]))


def import_index_parquets_into_ducklake(con: duckdb.DuckDBPyConnection) -> None:
    """
    Import the three Parquet files back into DuckLake as *tables* (no constraints).
    If you prefer virtual tables, convert these CREATE TABLE + INSERTs into CREATE VIEW ... AS read_parquet(...).
    """
    con.execute("USE my_ducklake")

    for p in (PARQ_DICT, PARQ_DOCS, PARQ_POST):
        if not p.exists():
            raise FileNotFoundError(f"Missing parquet file: {p}")

    # Drop & recreate (no constraints)
    con.execute("DROP TABLE IF EXISTS main.postings")
    con.execute("DROP TABLE IF EXISTS main.docs")
    con.execute("DROP TABLE IF EXISTS main.dict")

    con.execute("""
        CREATE TABLE main.dict (
            termid BIGINT,
            term   TEXT,
            df     BIGINT
        )
    """)
    con.execute("""
        CREATE TABLE main.docs (
            docid BIGINT,
            len   BIGINT
        )
    """)
    con.execute("""
        CREATE TABLE main.postings (
            termid BIGINT,
            docid  BIGINT,
            tf     BIGINT
        )
    """)

    con.execute("INSERT INTO main.dict SELECT * FROM read_parquet(?)", (str(PARQ_DICT),))
    con.execute("INSERT INTO main.docs SELECT * FROM read_parquet(?)", (str(PARQ_DOCS),))
    con.execute("INSERT INTO main.postings SELECT * FROM read_parquet(?)", (str(PARQ_POST),))


def build_and_import_from_ducklake(con: duckdb.DuckDBPyConnection, limit: int | None = None) -> None:
    """
    Convenience wrapper: build Parquets from DuckLake data, then import them as DuckLake tables.
    """
    build_index_to_parquet_from_ducklake(con, limit)
    import_index_parquets_into_ducklake(con)


# ----------------------- CRUD helpers (batch index maintenance) ----

def _next_id(con: duckdb.DuckDBPyConnection, table: str, col: str) -> int:
    """
    Return next integer id as (MAX(col)+1) or 1 if empty.
    """
    return int(con.execute(f"SELECT COALESCE(MAX({col}) + 1, 1) FROM my_ducklake.{table}").fetchone()[0])


def _exists(con: duckdb.DuckDBPyConnection, table: str, col: str, val) -> bool:
    """
    Return True if a row exists in my_ducklake.{table} where {col} = val.
    """
    return bool(con.execute(f"SELECT 1 FROM my_ducklake.{table} WHERE {col} = ? LIMIT 1", (val,)).fetchone())


def delete(con: duckdb.DuckDBPyConnection, docid: int):
    """
    Delete a document from data/docs/postings and update dict.df accordingly.
    Manages its own transaction (no nested BEGIN from caller).
    """
    con.execute("BEGIN")
    try:
        # decrement df once per distinct term in this doc
        con.execute("""
            UPDATE my_ducklake.dict
            SET df = CASE WHEN df > 0 THEN df - 1 ELSE 0 END
            WHERE termid IN (
                SELECT DISTINCT termid
                FROM my_ducklake.postings
                WHERE docid = ?
            )
        """, (docid,))

        # remove dict rows that hit 0 (only those touched)
        con.execute("""
            DELETE FROM my_ducklake.dict
            WHERE df = 0
              AND termid IN (
                  SELECT DISTINCT termid
                  FROM my_ducklake.postings
                  WHERE docid = ?
              )
        """, (docid,))

        # explicit cleanup (no cascades)
        con.execute("DELETE FROM my_ducklake.postings WHERE docid = ?", (docid,))
        con.execute("DELETE FROM my_ducklake.docs      WHERE docid = ?", (docid,))
        con.execute("DELETE FROM my_ducklake.data      WHERE docid = ?", (docid,))

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def insert(con: duckdb.DuckDBPyConnection, doc: str, docid: int | None = None):
    """
    Insert or upsert a document with text `doc`.

    Steps:
      1) Tokenize content; compute per-term TFs.
      2) For each distinct term: insert (termid, term, df=1) or df++ if exists.
      3) Insert/update docs(docid, len); allocate docid if not provided.
      4) Insert postings rows (termid, docid, tf).
      5) Insert/update data(docid, content).

    Returns the docid used.
    """
    tokens = tokenize(doc)
    if not tokens:
        return None

    tf = Counter(tokens)
    terms = list(tf.keys())

    con.execute("BEGIN")
    try:
        # 1) dict "upsert" per distinct term (manual)
        term_to_id: dict[str, int] = {}
        for t in terms:
            # Inline lookup to avoid import cycles
            tid_row = con.execute("SELECT termid FROM my_ducklake.dict WHERE term = ?", (t,)).fetchone()
            tid = int(tid_row[0]) if tid_row else None

            if tid is None:
                tid = _next_id(con, "dict", "termid")
                con.execute("INSERT INTO my_ducklake.dict (termid, term, df) VALUES (?, ?, 1)", (tid, t))
            else:
                con.execute("UPDATE my_ducklake.dict SET df = df + 1 WHERE termid = ?", (tid,))

            term_to_id[t] = tid

        # 2) docs row
        doc_len = len(tokens)
        if docid is None:
            docid = _next_id(con, "docs", "docid")
            con.execute("INSERT INTO my_ducklake.docs (docid, len) VALUES (?, ?)", (docid, doc_len))
        else:
            if _exists(con, "docs", "docid", docid):
                con.execute("UPDATE my_ducklake.docs SET len = ? WHERE docid = ?", (doc_len, docid))
            else:
                con.execute("INSERT INTO my_ducklake.docs (docid, len) VALUES (?, ?)", (docid, doc_len))

        # 3) postings rows
        rows = [(term_to_id[t], docid, tf[t]) for t in terms]
        con.executemany("INSERT INTO my_ducklake.postings (termid, docid, tf) VALUES (?, ?, ?)", rows)

        # 4) data row
        if _exists(con, "data", "docid", docid):
            con.execute("UPDATE my_ducklake.data SET content = ? WHERE docid = ?", (doc, docid))
        else:
            con.execute("INSERT INTO my_ducklake.data (docid, content) VALUES (?, ?)", (docid, doc))

        con.execute("COMMIT")
        return docid
    except Exception:
        con.execute("ROLLBACK")
        raise


def modify(con: duckdb.DuckDBPyConnection, docid: int, content: str):
    """
    Replace a document’s content by: delete(docid) then insert(content, docid).
    """
    delete(con, docid)
    return insert(con, content, docid=docid)