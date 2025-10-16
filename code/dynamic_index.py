#!/usr/bin/env python3
import argparse
import duckdb

# Core helpers
from helper_functions import (
    connect_ducklake,
    test_ducklake,
    initialise_data,
    import_data,
    cleanup_old_files,
    cleanup_orphaned_files,
)
from fts_tools import run_bm25_query

# Index operations
from index_tools import reindex  # for full rebuild (used for initialise)

# Testing
from test import run_tests  # type: ignore


# -----------------------
# Internal utilities
# -----------------------
def _parse_days(value):
    """
    Validate that --older-than is an integer number of days.
    Returns a string like '7 days' for safe SQL interpolation.
    """
    try:
        days = int(value)
        if days <= 0:
            raise ValueError
    except ValueError:
        raise SystemExit("ERROR: --older-than must be a positive integer number of days (e.g., 7).")
    return f"{days} days"


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
    con = duckdb.connect()
    connect_ducklake(con)
    run_bm25_query(con, query, top_n=top_n, show_content=show_content, qtype=qtype)

def run_import(parquet):
    """
    Upsert content only (DuckLake MERGE). Does NOT rebuild the index.
    """
    con = duckdb.connect()
    connect_ducklake(con)
    import_data(con, parquet)
    print(f"Upserted data from {parquet} into my_ducklake.main.data (no index rebuild).")

def run_initialise(parquet, limit):
    """
    Drop/recreate my_ducklake.data from the source parquet (with optional limit),
    then rebuild dict/docs/postings.
    """
    con = duckdb.connect()
    connect_ducklake(con)
    initialise_data(con, parquet=parquet, limit=limit)
    reindex(con)
    print(f"Initialised data from {parquet} (limit={limit}) and reindexed.")

def run_reindex():
    """
    Rebuild dict/docs/postings from current my_ducklake.data.
    """
    con = duckdb.connect()
    connect_ducklake(con)
    reindex(con)
    print("Reindexed from current my_ducklake.main.data.")

def run_cleanup(older_than_days=7, dry_run=True, all_files=False):
    con = duckdb.connect()
    connect_ducklake(con)
    cleanup_old_files(con, older_than_days=older_than_days, dry_run=dry_run, all_files=all_files)

def run_cleanup_orphans(older_than_days=7, dry_run=True, all_files=False):
    con = duckdb.connect()
    connect_ducklake(con)
    cleanup_orphaned_files(con, older_than_days=older_than_days, dry_run=dry_run, all_files=all_files)


# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="DuckLake dynamic index tooling")
    ap.add_argument(
        "--mode",
        choices=[
            "test",
            "sanity",
            "query",
            "import",
            "initialise",
            "reindex",
            "cleanup",
            "cleanup-orphans",
        ],
        default="query",
    )
    ap.add_argument("--q", "--query", dest="query", type=str, help="Query string for --mode query")
    ap.add_argument("--top", dest="top_n", type=int, default=10)
    ap.add_argument("--show-content", action="store_true")
    ap.add_argument(
        "--qtype",
        choices=["conjunctive", "disjunctive"],
        default="disjunctive",
        help="BM25 evaluation mode: conjunctive (AND) or disjunctive (OR).",
    )
    ap.add_argument("--parquet", type=str, default="metadata_0.parquet", help="Source parquet for import/initialise")
    ap.add_argument("--limit", type=int, default=None, help="Limit docs during initialise (ignored for reindex)")
    ap.add_argument(
        "--older-than",
        type=int,
        default=7,
        help="Number of days to keep data before cleanup (default: 7).",
    )
    ap.add_argument(
        "--cleanup-all",
        action="store_true",
        help="For cleanup modes: delete ALL eligible files (ignore --older-than).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="For cleanup modes: list files to delete without deleting them.",
    )
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
    elif args.mode == "cleanup":
        run_cleanup(older_than_days=args.older_than, dry_run=args.dry_run, all_files=args.cleanup_all)
    elif args.mode == "cleanup-orphans":
        run_cleanup_orphans(older_than_days=args.older_than, dry_run=args.dry_run, all_files=args.cleanup_all)