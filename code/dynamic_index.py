#!/usr/bin/env python3
# dynamic_index.py
# Command-line entrypoint for DuckLake-backed BM25 indexing/search.
# Orchestrates attach, ingest, reindex, query, cleanup, and tests.

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
from index_tools import reindex, delete
# Testing
from test_logic import run_tests  # type: ignore


# ---------------------------------------------------------------------
# Mode runners
# ---------------------------------------------------------------------
def run_test():
    """Execute internal test suite (functional smoke tests)."""
    run_tests()


def run_sanity():
    """Attach DuckLake and print schema/row previews for core tables."""
    con = duckdb.connect()
    connect_ducklake(con)
    test_ducklake(con)


def run_query(query, top_n=10, show_content=False, qtype="disjunctive"):
    """
    Run a BM25 query with AND/OR semantics.

    Parameters:
      - query: raw user query string
      - top_n: max results to show
      - show_content: append content snippet to each result
      - qtype: 'conjunctive' (AND) or 'disjunctive' (OR)

    Returns:
      (results, runtime_seconds) where runtime is BM25 SQL only.
    """
    con = duckdb.connect()
    connect_ducklake(con)
    results, runtime = run_bm25_query(
        con, query, top_n=top_n, show_content=show_content, qtype=qtype
    )
    return results, runtime


def run_import(parquet):
    """
    Upsert content only into `my_ducklake.data` (no index rebuild).

    Intended for incremental updates; follow with `--mode reindex`
    if you need postings/dict updates.
    """
    con = duckdb.connect()
    connect_ducklake(con)
    import_data(con, parquet)
    print(f"Upserted data from {parquet} into my_ducklake.main.data (no index rebuild).")


def run_initialise(parquet, limit):
    """
    Rebuild base data then reindex.

    - Drops and recreates `my_ducklake.data` from the provided parquet source.
    - Supports single file, directory, or ALL via helper.
    - Triggers full index rebuild (dict/docs/postings).
    """
    con = duckdb.connect()
    connect_ducklake(con)
    initialise_data(con, parquet=parquet, limit=limit)
    print("Imported parquet into data, now indexing ...")
    reindex(con)
    print(f"Initialised data from {parquet} (limit={limit}) and reindexed.")


def run_reindex():
    """Rebuild dict/docs/postings from current `my_ducklake.data`."""
    con = duckdb.connect()
    connect_ducklake(con)
    reindex(con)
    print("Reindexed from current my_ducklake.main.data.")


def run_cleanup(older_than_days=7, dry_run=True, all_files=False):
    """
    Remove files scheduled for deletion by DuckLake retention.

    - older_than_days: threshold for eligible files
    - dry_run: list-only
    - all_files: ignore threshold and clean everything eligible
    """
    con = duckdb.connect()
    connect_ducklake(con)
    cleanup_old_files(con, older_than_days=older_than_days, dry_run=dry_run, all_files=all_files)


def run_cleanup_orphans(older_than_days=7, dry_run=True, all_files=False):
    """
    Remove orphaned (untracked) files from the DuckLake DATA_PATH.

    Parameters mirror run_cleanup.
    """
    con = duckdb.connect()
    connect_ducklake(con)
    cleanup_orphaned_files(con, older_than_days=older_than_days, dry_run=dry_run, all_files=all_files)


def run_delete(docid):
    """
    Delete one document and cascade updates to index structures.

    - Removes from data/docs/postings
    - Adjusts dict.df accordingly
    """
    con = duckdb.connect()
    connect_ducklake(con)
    delete(con, docid)
    print(f"Deleted docid={docid} from my_ducklake (data/docs/postings; dict.df adjusted).")


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
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
            "delete",
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
    ap.add_argument(
        "--docid",
        type=int,
        help="Document ID (required for --mode delete).",
    )
    args = ap.parse_args()

    if args.mode == "test":
        run_test()
    elif args.mode == "sanity":
        run_sanity()
    elif args.mode == "query":
        if not args.query:
            raise SystemExit("ERROR: provide --q 'your query'")
        results, runtime = run_query(args.query, top_n=args.top_n, show_content=args.show_content, qtype=args.qtype)
        # Extra machine-readable line for scripting; BM25 SQL only.
        print(f"BM25_SQL_RUNTIME_SECONDS={runtime:.6f}")
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
    elif args.mode == "delete":
        if args.docid is None:
            raise SystemExit("ERROR: provide --docid <int> for --mode delete")
        run_delete(args.docid)