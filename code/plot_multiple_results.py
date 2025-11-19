import argparse
import csv
import matplotlib.pyplot as plt
import os
import re
import numpy as np

def parse_label(filename):
    """Extracts a readable label from the filename."""
    match = re.search(r'_([a-zA-Z0-9]+)\.csv$', os.path.basename(filename))
    if match:
        return match.group(1).replace('_', ' ').replace('-', ' ')
    return os.path.basename(filename).replace('.csv', '')

def load_data(csv_file, col_name):
    """Reads the CSV and returns sorted arrays of (x_percent_deleted, y_runtime)."""
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

    # Ensure data is sorted by X for interpolation to work
    if x_vals:
        # Sort pairs based on x
        pairs = sorted(zip(x_vals, y_vals))
        x_vals = np.array([p[0] for p in pairs])
        y_vals = np.array([p[1] for p in pairs])
        return x_vals, y_vals
    return None, None

def plot_multiple_results(csv_files, qtype, top_n, output_raw="plot_raw_times.png", output_imp="plot_performance_increase.png", show=False):
    col_name = f"avg_bm25_sql_time_s_{qtype}_top{top_n}"
    
    # 1. Load all data into a dictionary
    datasets = {}
    for f in csv_files:
        label = parse_label(f)
        x, y = load_data(f, col_name)
        if x is not None:
            datasets[label] = (x, y)

    if not datasets:
        print("No valid data found.")
        return

    # ---------------------------------------------------------
    # PLOT 1: Raw Query Times (Original Plot)
    # ---------------------------------------------------------
    plt.figure(figsize=(10, 6))
    for label, (x, y) in datasets.items():
        plt.plot(x, y, marker="o", markersize=4, label=label)

    plt.xlabel("% of index deleted")
    plt.ylabel(f"Avg BM25 SQL time (s) [{qtype}, top={top_n}]")
    plt.title(f"Raw Query Times vs Deletion ({qtype})")
    plt.grid(True)
    plt.legend(title="Compaction Strategy")
    plt.tight_layout()
    plt.savefig(output_raw, dpi=150)
    print(f"Saved raw times plot to: {output_raw}")

    # ---------------------------------------------------------
    # PLOT 2: Performance Increase vs Baseline
    # ---------------------------------------------------------
    # Identify baseline
    baseline_key = next((k for k in datasets if "checkpoint0" in k.lower()), None)
    
    if not baseline_key:
        print("[WARN] Could not find 'checkpoint0' in filenames. Skipping performance increase plot.")
        return

    print(f"Using baseline: {baseline_key}")
    base_x, base_y = datasets[baseline_key]

    plt.figure(figsize=(10, 6))
    
    # Plot a reference line at 0% (no improvement)
    plt.axhline(0, color='black', linewidth=1, linestyle='--')

    for label, (curr_x, curr_y) in datasets.items():
        if label == baseline_key:
            continue # Don't compare baseline to itself

        # INTERPOLATION: Map current strategy's Y values onto the Baseline's X axis
        interp_y = np.interp(base_x, curr_x, curr_y)

        # Calculate % Performance Increase: (Baseline_Time - Strategy_Time) / Baseline_Time * 100
        pct_improvement = ((base_y - interp_y) / base_y) * 100

        # Calculate the Scalar Average Improvement for the legend
        avg_imp = np.mean(pct_improvement)

        plt.plot(base_x, pct_improvement, marker=".", markersize=3, 
                 label=f"{label} (Avg Imp: {avg_imp:.1f}%)")

    plt.xlabel("% of index deleted")
    plt.ylabel("% Performance Improvement vs Baseline")
    plt.title(f"Performance Increase Relative to {baseline_key}")
    plt.grid(True)
    plt.legend(title="Strategy (Avg Improvement)")
    plt.tight_layout()
    plt.savefig(output_imp, dpi=150)
    print(f"Saved improvement plot to: {output_imp}")

    if show:
        plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_files", nargs='+')
    parser.add_argument("--qtype", default="disjunctive")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--out-raw", default="combined_times.png", help="Output filename for raw times")
    
    # --- FIXED LINE BELOW (Changed % to %%) ---
    parser.add_argument("--out-imp", default="combined_improvement.png", help="Output filename for %% improvement")
    
    parser.add_argument("--show", action="store_true")

    args = parser.parse_args()

    plot_multiple_results(args.csv_files, args.qtype, args.top, args.out_raw, args.out_imp, args.show)