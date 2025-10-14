# indexing_tools.py
import duckdb
import pandas as pd
from collections import Counter, defaultdict
from pathlib import Path
from helper import PARQUET_FOLDER, tokenize

# Parquet outputs
PARQ_DIR  = PARQUET_FOLDER / "index"
PARQ_DICT = PARQ_DIR / "dict.parquet"
PARQ_DOCS = PARQ_DIR / "docs.parquet"
PARQ_POST = PARQ_DIR / "postings.parquet"

def _ensure_dirs():
    PARQ_DIR.mkdir(parents=True, exist_ok=True)

def _fetch_data_df(con: duckdb.DuckDBPyConnection, limit: int | None = None) -> pd.DataFrame:
    con.execute("USE my_ducklake")
    sql = "SELECT docid, content FROM main.data"
    if limit is not None:
        return con.execute(sql + " LIMIT ?", (int(limit),)).fetchdf()
    return con.execute(sql).fetchdf()

def _build_index(df: pd.DataFrame):
    """
    Build dict/docs/postings in Python (no constraints):
      dict(termid, term, df) — df = #docs containing the term
      docs(docid, len)
      postings(termid, docid, tf)
    """
    term_to_id: dict[str, int] = {}
    df_counter: defaultdict[int, int] = defaultdict(int)
    docs_rows: list[tuple[int, int]] = []
    postings_rows: list[tuple[int, int, int]] = []
    next_tid = 1

    for docid, content in zip(df["docid"].tolist(), df["content"].tolist()):
        text = "" if content is None else str(content)
        toks = tokenize(text)
        docs_rows.append((int(docid), len(toks)))
        if not toks:
            continue
        tf = Counter(toks)
        for term, freq in tf.items():
            tid = term_to_id.setdefault(term, next_tid)
            if tid == next_tid:
                next_tid += 1
            postings_rows.append((tid, int(docid), int(freq)))
        for term in tf.keys():
            df_counter[term_to_id[term]] += 1

    dict_rows = [(tid, term, int(df_counter.get(tid, 0))) for term, tid in term_to_id.items()]
    return dict_rows, docs_rows, postings_rows

def build_index_to_parquet_from_ducklake(con: duckdb.DuckDBPyConnection, limit: int | None = None) -> None:
    """Read my_ducklake.main.data → build index → write dict/docs/postings to Parquet."""
    _ensure_dirs()
    df = _fetch_data_df(con, limit)
    dict_rows, docs_rows, postings_rows = _build_index(df)
    pd.DataFrame(dict_rows,  columns=["termid", "term", "df"]).to_parquet(PARQ_DICT, index=False)
    pd.DataFrame(docs_rows,  columns=["docid", "len"]).to_parquet(PARQ_DOCS, index=False)
    pd.DataFrame(postings_rows, columns=["termid", "docid", "tf"]).to_parquet(PARQ_POST, index=False)

def import_index_parquets_into_ducklake(con: duckdb.DuckDBPyConnection) -> None:
    """
    Recreate dict/docs/postings in my_ducklake.main and import from Parquet.
    Leaves data as-is.
    """
    con.execute("USE my_ducklake")
    for p in (PARQ_DICT, PARQ_DOCS, PARQ_POST):
        if not Path(p).exists():
            raise FileNotFoundError(f"Missing parquet file: {p}")

    # Drop & recreate without constraints
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
    """One call: index data → write parquets → import dict/docs/postings (no constraints)."""
    build_index_to_parquet_from_ducklake(con, limit)
    import_index_parquets_into_ducklake(con)