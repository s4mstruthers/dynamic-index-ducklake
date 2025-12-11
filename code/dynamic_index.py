#!/usr/bin/env python3
"""
dynamic_index.py

Unified entry point for DuckLake indexing, querying, performance testing, and analysis.

Directories for outputs (automatically created):
  - results/performance_results/ : CSV metrics from performance tests
  - results/performance_plots/   : PNG plots from tests and comparisons
  - results/query_terms/         : CSV files containing generated query terms

Modes:
  - sanity          : Inspect DuckLake schema and data.
  - query           : Run a specific BM25 query.
  - import          : Upsert parquet data into data table (no index rebuild).
  - initialise      : Rebuild data table from parquet and full reindex.
  - reindex         : Rebuild index artifacts from current data.
  - delete          : Delete a specific document ID.
  - checkpoint      : Manually trigger a DuckLake checkpoint and rewrite.
  - reset           : Hard reset: delete DB files and restore Parquets from backup.
  - perf-test       : Run the performance testing loop (generate queries -> measure -> delete -> repeat).
  - plot-comparison : Generate comparison plots from multiple performance result CSVs.
"""

import argparse
import csv
import glob
import io
import os
import re
import shutil
import sys
import time
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path

# Third-party libraries
import duckdb
import matplotlib.pyplot as plt
import numpy as np

# Internal Project Modules
from helper_functions import (
    BASE_DIR,
    PARQUET_FOLDER,
    DUCKLAKE_FOLDER,
    connect_ducklake,
    test_ducklake,
    initialise_data,
    import_data,
    get_docid_count,
    checkpoint_rewrite
)
from fts_tools import run_bm25_query
from index_tools import reindex, delete, delete_N, delete_N_rand

# ---------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------

# Define result directory structures relative to the project root (parent of code/)
PROJECT_ROOT = BASE_DIR.parent
RESULTS_DIR = PROJECT_ROOT / "results"
PERF_RESULTS_DIR = RESULTS_DIR / "performance_results"
PERF_PLOTS_DIR = RESULTS_DIR / "performance_plots"
QUERY_TERMS_DIR = RESULTS_DIR / "query_terms"

# Specific paths for Reset Logic
WEBCRAWL_DIR = PARQUET_FOLDER / "webcrawl_data"
BACKUP_DIR = PARQUET_FOLDER / "backup_parquets"
DUCKLAKE_METADATA_FILE = DUCKLAKE_FOLDER / "metadata_catalog.ducklake"
DUCKLAKE_DATA_FILES = DUCKLAKE_FOLDER / "data_files"

# Ensure output directories exist
for d in [PERF_RESULTS_DIR, PERF_PLOTS_DIR, QUERY_TERMS_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# System Reset Logic
# ---------------------------------------------------------------------

def run_hard_reset():
    """
    Performs a 'hard reset' of the environment to ensure a fresh state.
    1. Deletes DuckLake metadata and data folders (dict, docs, postings, data).
    2. Wipes 'webcrawl_data' folder.
    3. Restores parquet files from 'backup_parquets' to 'webcrawl_data'.
    """
    print("--- INITIATING HARD RESET ---")

    # 1. Clean DuckLake Files
    if DUCKLAKE_METADATA_FILE.exists():
        try:
            os.remove(DUCKLAKE_METADATA_FILE)
            print(f"Deleted: {DUCKLAKE_METADATA_FILE.name}")
        except OSError as e:
            print(f"[WARN] Could not delete metadata file: {e}")

    # Delete the data_files directory (contains main/dict, main/docs, etc.)
    # We remove the entire data_files structure to be clean.
    if DUCKLAKE_DATA_FILES.exists():
        try:
            shutil.rmtree(DUCKLAKE_DATA_FILES)
            print(f"Deleted folder: {DUCKLAKE_DATA_FILES.name} (and all subfolders)")
        except OSError as e:
            print(f"[WARN] Could not delete data_files folder: {e}")

    # 2. Clean Webcrawl Data
    if WEBCRAWL_DIR.exists():
        for f in WEBCRAWL_DIR.glob("*.parquet"):
            try:
                f.unlink()
            except OSError as e:
                print(f"[WARN] Failed to delete {f.name}: {e}")
        print(f"Cleared *.parquet from: {WEBCRAWL_DIR.name}")
    else:
        WEBCRAWL_DIR.mkdir(parents=True, exist_ok=True)

    # 3. Restore from Backup
    if not BACKUP_DIR.exists():
        print(f"[ERROR] Backup directory not found: {BACKUP_DIR}")
        print("Skipping restoration step. Please ensure backup_parquets exists.")
        return

    backup_files = list(BACKUP_DIR.glob("*.parquet"))
    if not backup_files:
        print(f"[WARN] No parquet files found in backup: {BACKUP_DIR}")
        return

    print(f"Restoring {len(backup_files)} parquet files from backup...")
    for f in backup_files:
        shutil.copy2(f, WEBCRAWL_DIR / f.name)
    
    print("--- HARD RESET COMPLETE ---")


# ---------------------------------------------------------------------
# Core Modes (Maintenance & Querying)
# ---------------------------------------------------------------------

def run_sanity():
    """Attach DuckLake and print schema/row previews for core tables."""
    con = duckdb.connect()
    connect_ducklake(con)
    test_ducklake(con)


def run_query(query, top_n=10, show_content=False, qtype="disjunctive"):
    """
    Run a BM25 query with AND/OR semantics.
    """
    con = duckdb.connect()
    connect_ducklake(con)
    results, runtime = run_bm25_query(
        con, query, top_n=top_n, show_content=show_content, qtype=qtype
    )
    # Machine-readable output for scripts
    print(f"BM25_SQL_RUNTIME_SECONDS={runtime:.6f}")
    return results, runtime


def run_import(parquet):
    """
    Upsert content only into `my_ducklake.data` (no index rebuild).
    """
    con = duckdb.connect()
    connect_ducklake(con)
    import_data(con, parquet)
    print(f"Upserted data from {parquet} into my_ducklake.main.data (no index rebuild).")


def run_initialise(parquet, limit):
    """
    Rebuild base data then reindex.
    """
    con = duckdb.connect()
    connect_ducklake(con)
    initialise_data(con, parquet=parquet, limit=limit)
    print(f"Imported parquet into data (limit={limit}), now indexing ...")
    reindex(con)
    print("Initialisation complete.")


def run_reindex():
    """Rebuild dict/docs/postings from current `my_ducklake.data`."""
    con = duckdb.connect()
    connect_ducklake(con)
    reindex(con)
    print("Reindexed from current my_ducklake.main.data.")


def run_delete(docid):
    """Delete one document and cascade updates to index structures."""
    con = duckdb.connect()
    connect_ducklake(con)
    delete(con, docid)
    print(f"Deleted docid={docid} from my_ducklake.")


def run_checkpoint():
    """
    Standalone checkpoint mode.
    Triggers the DuckLake rewrite logic manually.
    """
    con = duckdb.connect()
    connect_ducklake(con)
    print("Starting manual checkpoint...")
    checkpoint_rewrite(con)
    print("Manual checkpoint complete.")


# ---------------------------------------------------------------------
# Performance Testing Logic (Merged from performance_testing.py)
# ---------------------------------------------------------------------

def get_latest_query_file(pattern="queries_random_*.csv"):
    """Finds the most recently created queries file in the query terms directory."""
    # Search inside the defined QUERY_TERMS_DIR
    search_path = QUERY_TERMS_DIR / pattern
    files = glob.glob(str(search_path))
    if not files:
        return None
    return max(files, key=os.path.getctime)


def load_queries_from_csv(filepath):
    """Reads a single-column CSV of queries into a list."""
    queries = []
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Query file not found: {filepath}")
        
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        # Check header
        if header and header[0] == "query":
            pass 
        elif header:
            queries.append(header[0])

        for row in reader:
            if row:
                queries.append(row[0])
    return queries


def generate_random_queries(con, k=100, min_terms=1, max_terms=3, out_csv=None):
    """
    Generate k random bag-of-words queries by sampling terms from my_ducklake.dict.
    """
    k = max(1, int(k))
    min_terms = max(1, int(min_terms))
    max_terms = max(min_terms, int(max_terms))

    n_terms = con.execute("SELECT COUNT(*) FROM my_ducklake.dict").fetchone()[0]
    if n_terms == 0:
        raise SystemExit("ERROR: dict is empty. Populate my_ducklake.data and reindex first.")

    queries = []
    for _ in range(k):
        # Determine length for this query
        q_len = int(con.execute(
            "SELECT CAST(FLOOR(RANDOM() * (? - ? + 1)) + ? AS INTEGER)",
            [max_terms, min_terms, min_terms]
        ).fetchone()[0])

        # Sample terms
        rows = con.execute(
            "SELECT term FROM my_ducklake.dict ORDER BY RANDOM() LIMIT ?",
            [q_len]
        ).fetchall()
        terms = [r[0] for r in rows if r and r[0]]
        if not terms:
            continue # Should be rare
        queries.append(" ".join(terms))

    if out_csv:
        # Ensure parent dir exists (it should, based on constants)
        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["query"])
            for q in queries:
                w.writerow([q])

    return queries


def average_bm25_sql_time(con, queries, top_n=10, qtype="disjunctive"):
    """
    Return (avg_runtime, [all_runtimes]) for the given queries.
    Suppress stdout to keep the console clean during perf testing.
    """
    runtimes = []
    sink = io.StringIO()
    for q in queries:
        with redirect_stdout(sink):
            _, rt = run_bm25_query(con, q, top_n=top_n, show_content=False, qtype=qtype)
        runtimes.append(rt)
    avg = (sum(runtimes) / len(runtimes)) if runtimes else 0.0
    return avg, runtimes


def plot_single_result(results_csv, qtype, top_n, output_png=None, random=False, show=False):
    """
    Plot results for a single performance run (called at end of perf-test).
    """
    x_pct_deleted = []
    y_avg_runtime = []
    col_name = f"avg_bm25_sql_time_s_{qtype}_top{top_n}"

    with open(results_csv, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                pct_orig = float(row["percent_of_original"])
                avg_rt = float(row[col_name])
                x_pct_deleted.append(100.0 - pct_orig)
                y_avg_runtime.append(avg_rt)
            except (KeyError, ValueError):
                continue

    if not x_pct_deleted:
        print("[WARN] No data to plot.")
        return None

    # Determine default output path if not provided
    if not output_png:
        base_name = os.path.splitext(os.path.basename(results_csv))[0]
        output_png = PERF_PLOTS_DIR / f"{base_name}_plot.png"

    plt.figure()
    plt.plot(x_pct_deleted, y_avg_runtime, marker="o")
    plt.xlabel("% of index deleted")
    plt.ylabel(f"Avg BM25 SQL time (s) [{qtype}, top={top_n}]")
    
    title_suffix = "randomly deleted" if random else "sequentially deleted"
    plt.title(f"Avg Query Time vs % Index ({title_suffix})")

    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_png, dpi=150)
    print(f"Saved plot to: {output_png}")
    
    if show:
        plt.show()
    plt.close()

    return str(output_png)


def run_performance_test(args):
    """
    Orchestrate the performance testing loop.
    1. (Optional) Hard reset environment.
    2. Initialise/Reindex.
    3. Generate/Load queries.
    4. Loop: Measure avg time -> Log -> Delete batch -> Checkpoint (optional).
    5. Plot result.
    """
    # 0. Optional Hard Reset
    if args.reset:
        run_hard_reset()

    con = duckdb.connect()
    connect_ducklake(con)
    
    print("--- STARTING PERFORMANCE TEST ---")
    
    # 1. Ensure Data Exists
    # If we just reset, the data table is gone. We must re-initialise from the restored parquets.
    if args.reset:
        print("Reset detected: Initialising data from restored parquets...")
        # FIX: Pass "ALL" explicitly as a string
        initialise_data(con, parquet="ALL", limit=None)
        print("Data imported. Reindexing...")
        reindex(con)
    else:
        # Just reindex to be safe if not resetting
        print("Reindexing to ensure fresh index state...")
        reindex(con)

    original_count = get_docid_count(con)
    if original_count == 0:
        raise SystemExit("ERROR: Index empty. Populate my_ducklake.data first (or use --reset).")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 2. Setup Query Source
    queries = []
    queries_csv_used = ""

    if args.reuse_latest or args.reuse_file:
        target_file = args.reuse_file
        if args.reuse_latest:
            latest = get_latest_query_file()
            if latest:
                target_file = latest
            else:
                raise SystemExit("ERROR: --reuse-latest specified but no query files found.")
        
        if not target_file:
            raise SystemExit("ERROR: --reuse-file specified but no filename provided.")

        print(f"--- REUSING QUERIES FROM: {target_file} ---")
        queries = load_queries_from_csv(target_file)
        queries_csv_used = target_file
        if not queries:
             raise SystemExit(f"ERROR: No queries found in {target_file}")
    else:
        # Generate new
        queries_filename = args.queries_csv or f"queries_random_{ts}.csv"
        queries_csv_used = QUERY_TERMS_DIR / queries_filename
        print(f"--- GENERATING {args.query_count} NEW QUERIES ---")
        queries = generate_random_queries(
            con, k=args.query_count, out_csv=queries_csv_used
        )

    # 3. Setup Results CSV
    if args.results_csv:
        # Use exact user-provided filename
        results_filename = args.results_csv
    else:
        # Auto-generate filename with optional checkpoint tag
        base_name = f"performance_results_{ts}"
        if args.checkpoint_pct > 0:
            # Format float to remove trailing zero if it's an integer (e.g. 10.0 -> 10)
            ckpt_str = f"{int(args.checkpoint_pct)}" if args.checkpoint_pct.is_integer() else f"{args.checkpoint_pct}"
            base_name += f"_checkpoint{ckpt_str}"
        results_filename = f"{base_name}.csv"

    results_csv_path = PERF_RESULTS_DIR / results_filename
    
    with open(results_csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "iteration",
            "docs_remaining",
            "percent_of_original",
            f"avg_bm25_sql_time_s_{args.qtype}_top{args.top_n}",
            "query_count",
            "delete_batch",
        ])

    # 4. Execution Loop
    iteration = 0
    docs_remaining = original_count
    next_checkpoint_pct = args.checkpoint_pct

    while docs_remaining >= args.delete_batch:
        iteration += 1
        pct_orig = docs_remaining / original_count * 100.0
        pct_deleted = 100.0 - pct_orig
        
        print(f"ITER {iteration} | {pct_deleted:.2f}% deleted | Docs: {docs_remaining}")

        # Measure
        avg_rt, _ = average_bm25_sql_time(con, queries, top_n=args.top_n, qtype=args.qtype)

        # Log
        with open(results_csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                iteration,
                docs_remaining,
                round(pct_orig, 6),
                round(avg_rt, 6),
                len(queries),
                args.delete_batch,
            ])
        
        # Delete
        if args.random:
            delete_N_rand(con, args.delete_batch)
        else:
            delete_N(con, args.delete_batch)
            
        # Checkpoint?
        if args.checkpoint_pct > 0 and pct_deleted >= next_checkpoint_pct:
            checkpoint_rewrite(con) 
            next_checkpoint_pct += args.checkpoint_pct
        
        docs_remaining = get_docid_count(con)

    print(f"--- TEST COMPLETE ---")
    print(f"Results saved to: {results_csv_path}")

    # 5. Plot
    if args.plot:
        plot_single_result(results_csv_path, args.qtype, args.top_n, output_png=args.plot_file, random=args.random)

# ---------------------------------------------------------------------
# Plotting Comparison Logic (Merged from plot_multiple_results.py)
# ---------------------------------------------------------------------

def parse_label(filename):
    """Extracts a readable label from the filename."""
    match = re.search(r'_([a-zA-Z0-9]+)\.csv$', os.path.basename(filename))
    if match:
        return match.group(1).replace('_', ' ').replace('-', ' ')
    return os.path.basename(filename).replace('.csv', '')


def load_plot_data(csv_file, col_name):
    """Reads CSV and returns sorted arrays of (x_percent_deleted, y_runtime)."""
    x_vals = []
    y_vals = []
    
    try:
        with open(csv_file, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                try:
                    pct_orig = float(row["percent_of_original"])
                    avg_rt = float(row[col_name])
                    x_vals.append(100.0 - pct_orig)
                    y_vals.append(avg_rt)
                except (KeyError, ValueError):
                    continue
    except Exception as e:
        print(f"[ERROR] Could not read {csv_file}: {e}")
        return None, None

    if x_vals:
        # Sort for interpolation
        pairs = sorted(zip(x_vals, y_vals))
        x_vals = np.array([p[0] for p in pairs])
        y_vals = np.array([p[1] for p in pairs])
        return x_vals, y_vals
    return None, None


def run_plot_comparison(csv_files, qtype, top_n, out_raw, out_imp, show=False):
    """
    Generates two plots:
    1. Raw Query Times vs Deletion %
    2. Performance Improvement vs Baseline (requires 'checkpoint0' in one filename)
    """
    col_name = f"avg_bm25_sql_time_s_{qtype}_top{top_n}"
    
    # Prepend PERF_RESULTS_DIR if path is just a filename
    resolved_files = []
    for f in csv_files:
        p = Path(f)
        if not p.exists():
            # Try looking in the results dir
            candidate = PERF_RESULTS_DIR / f
            if candidate.exists():
                resolved_files.append(str(candidate))
            else:
                print(f"[WARN] File not found: {f}")
        else:
            resolved_files.append(str(p))

    if not resolved_files:
        print("No valid CSV files provided.")
        return

    datasets = {}
    for f in resolved_files:
        label = parse_label(f)
        x, y = load_plot_data(f, col_name)
        if x is not None:
            datasets[label] = (x, y)

    if not datasets:
        print("No valid data loaded from CSVs.")
        return

    # Resolve output paths
    out_raw_path = PERF_PLOTS_DIR / out_raw
    out_imp_path = PERF_PLOTS_DIR / out_imp

    # --- PLOT 1: Raw Times ---
    plt.figure(figsize=(10, 6))
    for label, (x, y) in datasets.items():
        plt.plot(x, y, marker="o", markersize=4, label=label)

    plt.xlabel("% of index deleted")
    plt.ylabel(f"Avg BM25 SQL time (s) [{qtype}, top={top_n}]")
    plt.title(f"Raw Query Times vs Deletion ({qtype})")
    plt.grid(True)
    plt.legend(title="Strategy")
    plt.tight_layout()
    plt.savefig(out_raw_path, dpi=150)
    print(f"Saved raw plot to: {out_raw_path}")

    # --- PLOT 2: Improvement (requires 'checkpoint0') ---
    baseline_key = next((k for k in datasets if "checkpoint0" in k.lower()), None)
    
    if baseline_key:
        print(f"Using baseline: {baseline_key}")
        base_x, base_y = datasets[baseline_key]

        plt.figure(figsize=(10, 6))
        plt.axhline(0, color='black', linewidth=1, linestyle='--')

        for label, (curr_x, curr_y) in datasets.items():
            if label == baseline_key:
                continue

            # Interpolate to match baseline X coordinates
            interp_y = np.interp(base_x, curr_x, curr_y)
            pct_improvement = ((base_y - interp_y) / base_y) * 100
            avg_imp = np.mean(pct_improvement)

            plt.plot(base_x, pct_improvement, marker=".", markersize=3, 
                     label=f"{label} (Avg Imp: {avg_imp:.1f}%)")

        plt.xlabel("% of index deleted")
        plt.ylabel("% Performance Improvement vs Baseline")
        plt.title(f"Performance Increase Relative to {baseline_key}")
        plt.grid(True)
        plt.legend(title="Strategy")
        plt.tight_layout()
        plt.savefig(out_imp_path, dpi=150)
        print(f"Saved improvement plot to: {out_imp_path}")
    else:
        print("[INFO] No 'checkpoint0' baseline found. Skipping improvement plot.")

    if show:
        plt.show()


# ---------------------------------------------------------------------
# Main CLI Entry Point
# ---------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DuckLake Dynamic Index Tooling")
    
    subparsers = parser.add_subparsers(dest="mode", required=True, help="Operation Mode")

    # --- Core Modes ---
    subparsers.add_parser("sanity", help="Inspect DuckLake schema/data")
    subparsers.add_parser("reindex", help="Rebuild index from data")
    subparsers.add_parser("checkpoint", help="Manually trigger DuckLake checkpoint rewrite")
    
    # Reset
    subparsers.add_parser("reset", help="Hard Reset: Wipe DB and restore Parquets from backup")

    # Query
    p_query = subparsers.add_parser("query", help="Run a BM25 query")
    p_query.add_argument("--q", "--query", dest="query", required=True, type=str)
    p_query.add_argument("--top", dest="top_n", type=int, default=10)
    p_query.add_argument("--show-content", action="store_true")
    p_query.add_argument("--qtype", choices=["conjunctive", "disjunctive"], default="disjunctive")

    # Data Loading
    p_import = subparsers.add_parser("import", help="Upsert Parquet data (no reindex)")
    p_import.add_argument("--parquet", type=str, required=True)

    p_init = subparsers.add_parser("initialise", help="Rebuild data from Parquet + Reindex")
    p_init.add_argument("--parquet", type=str, default="metadata_0.parquet")
    p_init.add_argument("--limit", type=int, default=None)

    # Delete
    p_del = subparsers.add_parser("delete", help="Delete a specific document")
    p_del.add_argument("--docid", type=int, required=True)

    # --- Performance Testing Mode ---
    p_perf = subparsers.add_parser("perf-test", help="Run performance testing loop")
    p_perf.add_argument("--query-count", type=int, default=100, help="Queries per iteration")
    p_perf.add_argument("--delete-batch", type=int, default=1000, help="Docs to delete per step")
    p_perf.add_argument("--qtype", choices=["conjunctive", "disjunctive"], default="disjunctive")
    p_perf.add_argument("--top", dest="top_n", type=int, default=10)
    p_perf.add_argument("--random", action="store_true", help="Randomize delete order")
    p_perf.add_argument("--checkpoint-pct", type=float, default=0.0, help="Checkpoint every N%% deleted")
    p_perf.add_argument("--reset", action="store_true", help="Run Hard Reset (Wipe & Restore) before starting test")
    
    p_perf.add_argument("--plot", action="store_true", help="Plot result immediately after test")
    p_perf.add_argument("--plot-file", type=str, help="Custom filename for the single run plot")
    
    # File handling for perf test
    p_perf.add_argument("--results-csv", type=str, help="Output CSV filename (saved in results/performance_results/)")
    p_perf.add_argument("--queries-csv", type=str, help="Output queries filename (saved in results/query_terms/)")
    p_perf.add_argument("--reuse-file", type=str, help="Reuse specific query file from results/query_terms/")
    p_perf.add_argument("--reuse-latest", action="store_true", help="Reuse latest query file found")

    # --- Plotting Comparison Mode ---
    p_plot = subparsers.add_parser("plot-comparison", help="Compare multiple result CSVs")
    p_plot.add_argument("csv_files", nargs='+', help="List of CSV files (names or paths)")
    p_plot.add_argument("--qtype", default="disjunctive")
    p_plot.add_argument("--top", dest="top_n", type=int, default=10)
    p_plot.add_argument("--out-raw", default="combined_times.png", help="Output filename for raw plot")
    p_plot.add_argument("--out-imp", default="combined_improvement.png", help="Output filename for improvement plot")
    p_plot.add_argument("--show", action="store_true")

    args = parser.parse_args()

    # Dispatcher
    if args.mode == "sanity":
        run_sanity()
    elif args.mode == "checkpoint":
        run_checkpoint()
    elif args.mode == "reset":
        run_hard_reset()
    elif args.mode == "query":
        run_query(args.query, args.top_n, args.show_content, args.qtype)
    elif args.mode == "import":
        run_import(args.parquet)
    elif args.mode == "initialise":
        run_initialise(args.parquet, args.limit)
    elif args.mode == "reindex":
        run_reindex()
    elif args.mode == "delete":
        run_delete(args.docid)
    elif args.mode == "perf-test":
        run_performance_test(args)
    elif args.mode == "plot-comparison":
        run_plot_comparison(args.csv_files, args.qtype, args.top_n, args.out_raw, args.out_imp, args.show)