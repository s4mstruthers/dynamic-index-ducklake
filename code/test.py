#!/usr/bin/env python3
import duckdb
import argparse
from helper_functions import connect_ducklake, test_ducklake, reset_and_reindex, tokenize, get_termid
from index_tools import insert as insert_doc, delete as delete_doc, modify as modify_doc

DBPREFIX = "my_ducklake"  # attached database/catalog name

# -----------------------
# small query helpers
# -----------------------
def q1(con, sql, params=()):
    return con.execute(sql, params).fetchone()

def qa(con, sql, params=()):
    return con.execute(sql, params).fetchall()

def dict_row(con, term):
    return q1(con, f"SELECT termid, term, df FROM {DBPREFIX}.dict WHERE term = ?", (term,))

def docs_row(con, docid):
    return q1(con, f"SELECT docid, len FROM {DBPREFIX}.docs WHERE docid = ?", (docid,))

def data_row(con, docid):
    return q1(con, f"SELECT docid, content FROM {DBPREFIX}.data WHERE docid = ?", (docid,))

def postings_for_doc(con, docid):
    return qa(con, f"SELECT termid, docid, tf FROM {DBPREFIX}.postings WHERE docid = ? ORDER BY termid", (docid,))

def assert_eq(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg} | expected={expected!r}, actual={actual!r}")

def assert_true(cond, msg):
    if not cond:
        raise AssertionError(msg)

# -----------------------
# tests: insert/modify/delete
# -----------------------
def run_tests():
    """
    Structured, non-destructive tests with a truth table summary.
    Subtests:
      T1  tokenize() behavior
      T2  insert doc1 -> docs/data/postings rows and counts
      T3  dict deltas after doc1
      T4  insert doc2 -> docs/data rows
      T5  dict deltas after doc2
      T6  modify doc2 -> docs/data/postings rows and counts
      T7  dict deltas after modify
      T8  cleanup (delete doc2 + doc1) succeeds
      T9  baseline restored (dict, docs, data, postings)
    """
    con = duckdb.connect()
    connect_ducklake(con)

    # ---------- helpers ----------
    results = []  # list[(label, bool)]
    def check(label: str, cond: bool):
        results.append((label, bool(cond)))

    def print_truth_table():
        width = max(len(label) for label, _ in results) if results else 10
        print("\nTEST SUMMARY")
        print("-" * (width + 10))
        for label, ok in results:
            print(f"{label:<{width}} : {'True' if ok else 'False'}")
        print("-" * (width + 10))

    def get_df(con, term):
        row = con.execute("SELECT df FROM my_ducklake.dict WHERE term = ?", (term,)).fetchone()
        return int(row[0]) if row else 0

    def docs_row(con, docid):
        return con.execute("SELECT docid, len FROM my_ducklake.docs WHERE docid = ?", (docid,)).fetchone()

    def data_row(con, docid):
        return con.execute("SELECT docid, content FROM my_ducklake.data WHERE docid = ?", (docid,)).fetchone()

    def postings_for_doc(con, docid):
        return con.execute(
            "SELECT termid, tf FROM my_ducklake.postings WHERE docid = ? ORDER BY termid",
            (docid,)
        ).fetchall()

    # ---------- test data ----------
    doc1 = "apple banana apple"
    doc2 = "banana cherry"
    new2 = "apple cherry cherry"

    # ---------- baseline snapshot ----------
    base = {
        "apple":  get_df(con, "apple"),
        "banana": get_df(con, "banana"),
        "cherry": get_df(con, "cherry"),
    }

    doc1_id = None
    doc2_id = None
    try:
        # ---------- T1: tokenize ----------
        t1 = tokenize(doc1) == ["apple", "banana", "apple"]
        check("T1 tokenize(doc1)", t1)

        # ---------- T2: insert doc1 ----------
        doc1_id = insert_doc(con, doc1)
        ok_docs = (row := docs_row(con, doc1_id)) is not None and row[1] == 3
        ok_data = (row := data_row(con, doc1_id)) is not None and row[1] == doc1
        p = postings_for_doc(con, doc1_id)
        # tf checks
        from collections import defaultdict
        tf_map = defaultdict(int)
        for tid, tf in p:
            tf_map[tid] = tf
        from helper_functions import get_termid
        a_id = get_termid(con, "apple")
        b_id = get_termid(con, "banana")
        ok_post = (len(p) == 2) and (tf_map.get(a_id) == 2) and (tf_map.get(b_id) == 1)
        check("T2 insert doc1: docs/data/postings", ok_docs and ok_data and ok_post)

        # ---------- T3: dict deltas after doc1 ----------
        check("T3 dict delta after doc1 (apple +1)",  get_df(con, "apple")  == base["apple"]  + 1)
        check("T3 dict delta after doc1 (banana +1)", get_df(con, "banana") == base["banana"] + 1)
        check("T3 dict delta after doc1 (cherry +0)", get_df(con, "cherry") == base["cherry"] + 0)

        # ---------- T4: insert doc2 ----------
        doc2_id = insert_doc(con, doc2)
        ok_docs2 = (row := docs_row(con, doc2_id)) is not None and row[1] == 2
        ok_data2 = (row := data_row(con, doc2_id)) is not None and row[1] == doc2
        check("T4 insert doc2: docs/data", ok_docs2 and ok_data2)

        # ---------- T5: dict deltas after doc2 ----------
        check("T5 dict delta after doc2 (banana +2 total)", get_df(con, "banana") == base["banana"] + 2)
        check("T5 dict delta after doc2 (cherry +1 total)", get_df(con, "cherry") == base["cherry"] + 1)
        check("T5 dict delta after doc2 (apple +1 total)",  get_df(con, "apple")  == base["apple"]  + 1)

        # ---------- T6: modify doc2 ----------
        modify_doc(con, doc2_id, new2)
        row = docs_row(con, doc2_id)
        ok_docs2b = row is not None and row[1] == 3
        row = data_row(con, doc2_id)
        ok_data2b = row is not None and row[1] == new2
        p2 = postings_for_doc(con, doc2_id)
        tf_map2 = {tid: tf for (tid, tf) in p2}
        c_id = get_termid(con, "cherry")
        ok_post2b = (tf_map2.get(a_id) == 1) and (tf_map2.get(c_id) == 2)
        check("T6 modify doc2: docs/data/postings", ok_docs2b and ok_data2b and ok_post2b)

        # ---------- T7: dict deltas after modify ----------
        check("T7 dict delta after modify (apple +2 total)",  get_df(con, "apple")  == base["apple"]  + 2)
        check("T7 dict delta after modify (banana +1 total)", get_df(con, "banana") == base["banana"] + 1)
        check("T7 dict delta after modify (cherry +1 total)", get_df(con, "cherry") == base["cherry"] + 1)

    finally:
        # ---------- T8: cleanup (delete doc2 + doc1) ----------
        ok_cleanup = True
        try:
            if doc2_id is not None:
                delete_doc(con, doc2_id)
            if doc1_id is not None:
                delete_doc(con, doc1_id)
        except Exception:
            ok_cleanup = False
        check("T8 cleanup deletes succeed", ok_cleanup)

        # ---------- T9: baseline restored ----------
        restored = (
            get_df(con, "apple")  == base["apple"]  and
            get_df(con, "banana") == base["banana"] and
            get_df(con, "cherry") == base["cherry"]
        )
        # and no stray rows
        stray = 0
        if doc1_id is not None:
            stray += con.execute("SELECT COUNT(*) FROM my_ducklake.docs WHERE docid = ?", (doc1_id,)).fetchone()[0]
            stray += con.execute("SELECT COUNT(*) FROM my_ducklake.data WHERE docid = ?", (doc1_id,)).fetchone()[0]
            stray += con.execute("SELECT COUNT(*) FROM my_ducklake.postings WHERE docid = ?", (doc1_id,)).fetchone()[0]
        if doc2_id is not None:
            stray += con.execute("SELECT COUNT(*) FROM my_ducklake.docs WHERE docid = ?", (doc2_id,)).fetchone()[0]
            stray += con.execute("SELECT COUNT(*) FROM my_ducklake.data WHERE docid = ?", (doc2_id,)).fetchone()[0]
            stray += con.execute("SELECT COUNT(*) FROM my_ducklake.postings WHERE docid = ?", (doc2_id,)).fetchone()[0]
        check("T9 baseline restored (dict/docs/data/postings)", restored and stray == 0)

        # ---------- summary ----------
        print_truth_table()

# -----------------------
# reindex + sanity
# -----------------------
def run_reindex(parquet: str, limit: int | None):
    con = duckdb.connect()
    connect_ducklake(con)
    reset_and_reindex(con, parquet=parquet, limit=limit)
    print(f"Reindexed from {parquet} (limit={limit})")

def run_sanity():
    con = duckdb.connect()
    connect_ducklake(con)
    test_ducklake(con)

# -----------------------
# ad-hoc query using BM25
# -----------------------
def run_query(query: str, top_n: int = 10, show_content: bool = False):
    from fts_tools import match_bm25
    con = duckdb.connect()
    connect_ducklake(con)

    results = match_bm25(con, query, top_n)
    if not results:
        print("No results.")
        return

    print(f"Top {len(results)} for query: {query!r}")
    for rank, (docid, score) in enumerate(results, 1):
        line = f"{rank:2d}. docid={docid}  score={score:.6f}"
        if show_content:
            row = con.execute("SELECT content FROM my_ducklake.data WHERE docid = ?", (docid,)).fetchone()
            if row and row[0] is not None:
                snippet = str(row[0])[:160].replace("\n", " ")
                line += f"  |  {snippet!r}"
        print(line)

# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["tests", "reindex", "query", "sanity"], default="query")
    ap.add_argument("--q", "--query", dest="query", type=str, help="query string for --mode query")
    ap.add_argument("--top", dest="top_n", type=int, default=10)
    ap.add_argument("--show-content", action="store_true")
    ap.add_argument("--parquet", type=str, default="metadata_0.parquet")
    ap.add_argument("--limit", type=int, default=None, help="limit docs during reindex/build")
    args = ap.parse_args()

    if args.mode == "tests":
        
        run_tests()
    elif args.mode == "reindex":
        run_reindex(args.parquet, args.limit)
    elif args.mode == "sanity":
        run_sanity()
    elif args.mode == "query":
        if not args.query:
            raise SystemExit("ERROR: provide --q 'your query'")
        run_query(args.query, top_n=args.top_n, show_content=args.show_content)