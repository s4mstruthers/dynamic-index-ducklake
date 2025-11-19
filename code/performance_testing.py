#!/usr/bin/env python3
# performance_testing.py
# Reindex from current my_ducklake.data, generate (or reuse) random queries,
# measure avg BM25 SQL time (BM25-SQL-only), delete in batches, log metrics,
# and plot average runtime vs % of index deleted.

import argparse
import csv
import os
import glob
from datetime import datetime
import io
import sys
import time
from contextlib import redirect_stdout

import duckdb
import matplotlib.pyplot as plt

from helper_functions import connect_ducklake, get_docid_count, checkpoint
from index_tools import reindex, delete_N, delete_N_rand
from fts_tools import run_bm25_query


def get_latest_query_file(pattern="queries_random_*.csv"):
    """Finds the most recently created queries file matching the pattern."""
    files = glob.glob(pattern)
    if not files:
        return None
    # Sort by creation time (latest last)
    return max(files, key=os.path.getctime)


def load_queries_from_csv(filepath):
    """Reads a single-column CSV of queries into a list."""
    queries = []
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Query file not found: {filepath}")
        
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        # Simple check to skip header if it exists and looks like "query"
        if header and header[0] == "query":
            pass 
        elif header:
            # If first row isn't "query", treat it as data? 
            # For safety based on this script's generation, we assume header exists.
            # If users manually made a file without header, this line might skip the first query.
            # Adjusted to just append if it doesn't look like the standard header.
            queries.append(header[0])

        for row in reader:
            if row:
                queries.append(row[0])
    return queries


def generate_random_queries(con, k=100, min_terms=1, max_terms=3, out_csv=None):
    """
    Generate k random bag-of-words queries by sampling 1..3 terms uniformly
    from my_ducklake.dict. Writes CSV with header 'query'. Returns the list.
    """
    k = max(1, int(k))
    min_terms = max(1, int(min_terms))
    max_terms = max(min_terms, int(max_terms))

    # ensure dict has rows
    n_terms = con.execute("SELECT COUNT(*) FROM my_ducklake.dict").fetchone()[0]
    if n_terms == 0:
        raise SystemExit("ERROR: dict is empty after reindex. Ensure my_ducklake.data has content before running.")

    queries = []
    for _ in range(k):
        q_len = int(con.execute(
            "SELECT CAST(FLOOR(RANDOM() * (? - ? + 1)) + ? AS INTEGER)",
            [max_terms, min_terms, min_terms]
        ).fetchone()[0])

        rows = con.execute(
            "SELECT term FROM my_ducklake.dict ORDER BY RANDOM() LIMIT ?",
            [q_len]
        ).fetchall()
        terms = [r[0] for r in rows if r and r[0]]
        if not terms:
            raise RuntimeError("Failed to sample terms for a query (dict unexpectedly empty).")
        queries.append(" ".join(terms))

    if out_csv:
        os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["query"])
            for q in queries:
                w.writerow([q])

    return queries


def average_bm25_sql_time(con, queries, top_n=10, qtype="disjunctive"):
    """
    Return (avg_runtime_seconds, [per_query_runtime...]) using BM25-SQL-only timing.

    Suppresses any prints from run_bm25_query by redirecting stdout to a buffer.
    """
    runtimes = []
    sink = io.StringIO()
    for q in queries:
        # Silence run_bm25_query console output (it prints results + timing)
        with redirect_stdout(sink):
            _, rt = run_bm25_query(con, q, top_n=top_n, show_content=False, qtype=qtype)
        runtimes.append(rt)
    avg = (sum(runtimes) / len(runtimes)) if runtimes else 0.0
    return avg, runtimes


def plot_results_csv(results_csv, qtype, top_n, output_png=None, random=False, show=False):
    """
    Plot Average BM25 SQL runtime vs % of index deleted from the results CSV.
    - X: % deleted (100 - percent_of_original)
    - Y: avg_bm25_sql_time_s_<qtype>_top<top_n>
    Saves to PNG (output_png); optionally show() for interactive use.
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
            except (KeyError, ValueError):
                continue
            x_pct_deleted.append(100.0 - pct_orig)
            y_avg_runtime.append(avg_rt)

    if not x_pct_deleted:
        print("[WARN] No rows parsed for plotting; skipping plot.")
        return None

    output_png = output_png or (os.path.splitext(results_csv)[0] + "_plot.png")

    plt.figure()
    plt.plot(x_pct_deleted, y_avg_runtime, marker="o")
    plt.xlabel("% of index deleted")
    plt.ylabel(f"Avg BM25 SQL time (s) [{qtype}, top={top_n}]")
    
    if random:
        plt.title("Average query time vs % of index randomly deleted")
    else:
        plt.title("Average query time vs % of index sequentially deleted")

    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_png, dpi=150)
    if show:
        plt.show()
    plt.close()

    return output_png


def main():
    ap = argparse.ArgumentParser(description="DuckLake performance testing (silent BM25 output + plotting)")
    ap.add_argument("--query-count", type=int, default=100, help="Queries per iteration (default: 100)")
    ap.add_argument("--delete-batch", type=int, default=1000, help="Docs to delete per iteration (default: 1000)")
    ap.add_argument("--qtype", choices=["conjunctive", "disjunctive"], default="disjunctive",
                    help="BM25 semantics (default: disjunctive)")
    ap.add_argument("--top", type=int, default=10, help="BM25 top_n (default: 10)")
    ap.add_argument("--results-csv", type=str, default=None,
                    help="Results CSV path; default performance_results_<timestamp>.csv")
    ap.add_argument("--queries-csv", type=str, default=None,
                    help="Output CSV to save the generated queries; default queries_random_<timestamp>.csv")
    
    # --- REUSE QUERY OPTIONS ---
    ap.add_argument("--reuse-file", type=str, help="Specify a CSV file to reuse queries from (skips generation).")
    ap.add_argument("--reuse-latest", action="store_true", help="Automatically reuse the latest queries_random_*.csv.")
    # ------------------------------

    ap.add_argument("--plot", action="store_true", help="Generate a plot PNG at the end.")
    ap.add_argument("--plot-file", type=str, default=None, help="Custom plot output PNG path.")
    ap.add_argument("--random", type=bool, default=False, help="Randomise order of docids before deleting")
    ap.add_argument("--checkpoint-pct", type=float, default=0.0,
                    help="Run CHECKPOINT every N percent deleted. 0.0 to disable (default: 0.0)")
    
    args = ap.parse_args()

    # connect & reindex from current my_ducklake.data
    con = duckdb.connect()
    connect_ducklake(con)
    # Minimal console noise per your request
    reindex(con)

    original_count = get_docid_count(con)
    if original_count == 0:
        raise SystemExit("ERROR: Index has 0 documents after reindex. Populate my_ducklake.data first.")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_csv = args.results_csv or f"performance_results_{ts}.csv"
    
    # --- QUERY GENERATION OR REUSE LOGIC ---
    queries = []
    queries_csv_used = ""

    if args.reuse_latest or args.reuse_file:
        # Reuse logic
        target_file = args.reuse_file
        if args.reuse_latest:
            latest = get_latest_query_file()
            if latest:
                target_file = latest
            else:
                raise SystemExit("ERROR: --reuse-latest specified but no 'queries_random_*.csv' files found.")
        
        if not target_file:
            raise SystemExit("ERROR: --reuse-file specified but no filename provided.")
            
        print(f"--- REUSING QUERIES FROM: {target_file} ---")
        queries = load_queries_from_csv(target_file)
        queries_csv_used = target_file
        
        if not queries:
             raise SystemExit(f"ERROR: No queries found in {target_file}")
    else:
        # Generation logic
        queries_csv_used = args.queries_csv or f"queries_random_{ts}.csv"
        print(f"--- GENERATING {args.query_count} NEW QUERIES ---")
        queries = generate_random_queries(
            con, 
            k=args.query_count, 
            min_terms=1, 
            max_terms=3, 
            out_csv=queries_csv_used
        )
    # ---------------------------------------

    # results header
    os.makedirs(os.path.dirname(results_csv) or ".", exist_ok=True)
    with open(results_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "iteration",
            "docs_remaining",
            "percent_of_original",
            f"avg_bm25_sql_time_s_{args.qtype}_top{args.top}",
            "query_count",
            "delete_batch",
        ])

    iteration = 0
    docs_remaining = original_count
    
    # --- State tracker for checkpointing ---
    next_checkpoint_pct = args.checkpoint_pct

    # Only show iteration status (% deleted) in terminal
    while docs_remaining >= args.delete_batch:
        iteration += 1
        pct_orig = docs_remaining / original_count * 100.0
        pct_deleted = 100.0 - pct_orig
        print(f"ITER {iteration} | {pct_deleted:.2f}% deleted")

        avg_rt, _ = average_bm25_sql_time(con, queries, top_n=args.top, qtype=args.qtype)

        with open(results_csv, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                iteration,
                docs_remaining,
                round(pct_orig, 6),
                round(avg_rt, 6),
                len(queries),
                args.delete_batch,
            ])
        if args.random:
            delete_N_rand(con, args.delete_batch)
        else:
            delete_N(con, args.delete_batch)
            
        # --- Checkpoint logic ---
        if args.checkpoint_pct > 0 and pct_deleted >= next_checkpoint_pct:
            # Setting the default rewrite delete threshold
            con.execute("CALL my_ducklake.set_option('rewrite_delete_threshold', 0.01);")

            print(f"--- CHECKPOINT triggered at {pct_deleted:.2f}% deleted (>= {next_checkpoint_pct}%) ---")
            start_ckpt = time.perf_counter()
            
            # Call the checkpoint function from helper_functions
            checkpoint(con) 
            
            end_ckpt = time.perf_counter()
            print(f"--- CHECKPOINT complete ({end_ckpt - start_ckpt:.4f}s) ---")
            
            # Set the next checkpoint target
            next_checkpoint_pct += args.checkpoint_pct
        
        # Update doc count after delete and potential checkpoint
        docs_remaining = get_docid_count(con)

    # Plot if requested
    if args.plot:
        png = plot_results_csv(results_csv, args.qtype, args.top, output_png=args.plot_file, random=args.random, show=False)
        if png:
            print(f"PLOT {png}")

    # Final minimal summary
    print(f"RESULTS {results_csv}")
    print(f"QUERIES {queries_csv_used}")


if __name__ == "__main__":
    main()