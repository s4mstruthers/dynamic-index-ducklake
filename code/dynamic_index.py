#!/usr/bin/env python3
import duckdb
import argparse
from helper_functions import connect_ducklake, test_ducklake, reset_and_reindex

#Importing testing function from tests.py
from test import run_tests # type: ignore


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
# Command Line Interface
# -----------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["test", "reindex", "query", "sanity"], default="query")
    ap.add_argument("--q", "--query", dest="query", type=str, help="query string for --mode query")
    ap.add_argument("--top", dest="top_n", type=int, default=10)
    ap.add_argument("--show-content", action="store_true")
    ap.add_argument("--parquet", type=str, default="metadata_0.parquet")
    ap.add_argument("--limit", type=int, default=None, help="limit docs during reindex/build")
    args = ap.parse_args()

    if args.mode == "test":
        run_tests()
    elif args.mode == "reindex":
        run_reindex(args.parquet, args.limit)
    elif args.mode == "sanity":
        run_sanity()
    elif args.mode == "query":
        if not args.query:
            raise SystemExit("ERROR: provide --q 'your query'")
        run_query(args.query, top_n=args.top_n, show_content=args.show_content)