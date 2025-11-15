import argparse
import csv
import matplotlib.pyplot as plt
import os
import re

def plot_multiple_results(csv_files, qtype, top_n, output_png="combined_performance_plot.png", show=False):
    """
    Plots average BM25 SQL runtime vs % of index deleted from multiple CSV files
    on the same graph.
    
    Args:
        csv_files (list): A list of paths to the CSV result files.
        qtype (str): The query type (e.g., 'disjunctive', 'conjunctive') to match Y-axis.
        top_n (int): The top_n value (e.g., 10) to match Y-axis.
        output_png (str): Path to save the combined plot.
        show (bool): If True, display the plot interactively.
    """
    plt.figure(figsize=(10, 6)) # Adjust figure size for better readability

    col_name = f"avg_bm25_sql_time_s_{qtype}_top{top_n}"
    
    if not csv_files:
        print("[WARN] No CSV files provided for plotting.")
        return

    for csv_file in csv_files:
        x_pct_deleted = []
        y_avg_runtime = []
        
        # Extract label from filename (e.g., "checkpoint20" or "no_compaction")
        # You might need to adjust this regex based on your exact naming convention
        match = re.search(r'_([a-zA-Z0-9]+)\.csv$', os.path.basename(csv_file))
        if match:
            label = match.group(1).replace('_', ' ').replace('-', ' ') # e.g., "checkpoint20"
        else:
            label = os.path.basename(csv_file).replace('.csv', '') # Fallback to full filename

        try:
            with open(csv_file, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    try:
                        pct_orig = float(row["percent_of_original"])
                        avg_rt = float(row[col_name])
                    except (KeyError, ValueError) as e:
                        # print(f"[WARN] Skipping row in {csv_file} due to missing data or format error: {e}")
                        continue
                    x_pct_deleted.append(100.0 - pct_orig)
                    y_avg_runtime.append(avg_rt)

            if x_pct_deleted:
                plt.plot(x_pct_deleted, y_avg_runtime, marker="o", label=label)
            else:
                print(f"[WARN] No valid data found in {csv_file} for plotting.")

        except FileNotFoundError:
            print(f"[ERROR] CSV file not found: {csv_file}")
        except Exception as e:
            print(f"[ERROR] An error occurred while processing {csv_file}: {e}")

    plt.xlabel("% of index deleted")
    plt.ylabel(f"Avg BM25 SQL time (s) [{qtype}, top={top_n}]")
    plt.title("Comparison of Average Query Time vs % of Index Deleted")
    plt.grid(True)
    plt.legend(title="Compaction Strategy") # Add a legend to differentiate lines
    plt.tight_layout() # Adjust layout to prevent labels from overlapping

    plt.savefig(output_png, dpi=150)
    if show:
        plt.show()
    plt.close()
    
    print(f"Combined plot saved to {output_png}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Plot average query time from multiple performance testing CSVs on one graph."
    )
    parser.add_argument("csv_files", nargs='+', 
                        help="Paths to one or more CSV result files to plot. "
                             "E.g., 'results_20.csv results_10.csv'")
    parser.add_argument("--qtype", default="disjunctive", 
                        help="Query type used in the CSVs (e.g., 'disjunctive'). Must match CSV column name.")
    parser.add_argument("--top", type=int, default=10, 
                        help="Top N results used in the CSVs (e.g., 10). Must match CSV column name.")
    parser.add_argument("--output-png", default="combined_performance_plot.png",
                        help="Filename for the output PNG plot.")
    parser.add_argument("--show", action="store_true", 
                        help="Display the plot interactively after saving.")

    args = parser.parse_args()

    plot_multiple_results(args.csv_files, args.qtype, args.top, args.output_png, args.show)