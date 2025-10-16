#!/usr/bin/env python3
import argparse
import duckdb

# Core helpers
from helper_functions import (
    connect_ducklake,
    test_ducklake,
    initialise_data,
    import_data,
)

# Index operations
from index_tools import (
    reindex,  # for full rebuild (used for initialise)
)

# Testing
from test import run_tests  # type: ignore


# -----------------------
# Modes
# -----------------------
def run_test():
    run_tests()

def run_sanity():
    con = duckdb.connect()
    connect_ducklake(con)
    test_ducklake(con)

def run_query(query, top_n=10, show_content=False, qtype="disjunctive"):
    # Choose BM25 variant at call-time to avoid importing both if not needed
    if qtype == "conjunctive":
        from fts_tools import conjunctive_bm25 as bm25_runner
    else:
        from fts_tools import disjunctive_bm25 as bm25_runner

    con = duckdb.connect()
    connect_ducklake(con)

    results = bm25_runner(con, query, top_n)
    if not results:
        print("No results.")
        return

    print(f"Top {len(results)} for {qtype} BM25 query: {query!r}")
    for rank, (docid, score) in enumerate(results, 1):
        line = f"{rank:2d}. docid={docid}  score={score:.6f}"
        if show_content:
            row = con.execute(
                "SELECT content FROM my_ducklake.data WHERE docid = ?", (docid,)
            ).fetchone()
            if row and row[0] is not None:
                snippet = str(row[0])[:160].replace("\n", " ")
                line += f"  |  {snippet!r}"
        print(line)

def run_import(parquet):
    """
    Upsert content only (DuckLake MERGE). Does NOT rebuild the index.
    """
    con = duckdb.connect()
    connect_ducklake(con)
    import_data(con, parquet)
    print(f"Upserted data from {parquet} into my_ducklake.main.data (no index rebuild).")

def run_initialise(parquet, limit):
    con = duckdb.connect()
    connect_ducklake(con)
    initialise_data(con, parquet=parquet, limit=limit)  # only place where limit applies
    reindex(con)  # build full index from whatever is now in data
    print(f"Initialised data from {parquet} (limit={limit}) and reindexed.")

def run_reindex():
    con = duckdb.connect()
    connect_ducklake(con)
    reindex(con)
    print("Reindexed from current my_ducklake.main.data.")

# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="DuckLake dynamic index tooling")
    ap.add_argument(
        "--mode",
        choices=["test", "sanity", "query", "import", "initialise", "reindex"],
        default="query",
        help=(
            "test: run unit tests; "
            "sanity: basic validation; "
            "query: BM25 search; "
            "import: upsert data only; "
            "initialise: load data and build index; "
            "reindex: rebuild index only from current data"
        ),
    )
    ap.add_argument("--q", "--query", dest="query", type=str, help="query string for --mode query")
    ap.add_argument("--top", dest="top_n", type=int, default=10)
    ap.add_argument("--show-content", action="store_true")
    ap.add_argument("--qtype", choices=["conjunctive", "disjunctive"], default="disjunctive",
                    help="BM25 evaluation mode: conjunctive (AND) or disjunctive (OR). Default: disjunctive.")
    ap.add_argument("--parquet", type=str, default="metadata_0.parquet", help="source parquet for import/initialise")
    ap.add_argument("--limit", type=int, default=None, help="limit docs during initialise (ignored for reindex)")
    args = ap.parse_args()

    if args.mode == "test":
        run_test()
    elif args.mode == "sanity":
        run_sanity()
    elif args.mode == "query":
        if not args.query:
            raise SystemExit("ERROR: provide --q 'your query'")
        run_query(args.query, top_n=args.top_n, show_content=args.show_content, qtype=args.qtype)
    elif args.mode == "import":
        run_import(args.parquet)
    elif args.mode == "initialise":
        run_initialise(args.parquet, args.limit)
    elif args.mode == "reindex":
        run_reindex()