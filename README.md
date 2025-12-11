# DuckLake Dynamic Indexing System

An ACID-compliant dynamic indexing and search system built on top of **DuckDB** and **DuckLake**. This project implements a BM25 Full-Text Search (FTS) engine that supports real-time updates (inserts/deletes) and persistent storage using Parquet artifacts managed by a custom metadata catalog.

## 🚀 Features

* **Dynamic Indexing**: Supports point-deletes, batch deletions, and upserts without requiring full index rebuilds.
* **BM25 Search**: Full SQL-based implementation of Okapi BM25 with support for both Conjunctive (AND) and Disjunctive (OR) query semantics.
* **DuckLake Integration**: Utilizes the `ducklake` extension for metadata management, atomic data rewrites, and maintenance.
* **Performance Benchmarking**: Built-in tooling to measure query latency degradation over time as the index accumulates deletions ("tombstones").
* **Visual Analytics**: Automated generation of performance plots comparing raw query times and improvements across different maintenance strategies.

---

## 📂 Project Structure

```

.
├── code/
│   ├── dynamic\_index.py      \# Main CLI entry point for all operations
│   ├── fts\_tools.py          \# BM25 scoring logic (Conjunctive/Disjunctive)
│   ├── index\_tools.py        \# Logic for building/repairing Parquet index artifacts
│   ├── helper\_functions.py   \# Database connection and path management
│   └── setup.sh              \# Environment setup script
├── ducklake/                 \# Managed storage area
│   ├── data\_files/           \# Physical data storage (managed by DuckLake)
│   └── metadata\_catalog.ducklake
├── parquet/
│   ├── webcrawl\_data/        \# Raw source documents (active input)
│   ├── index/                \# Generated index artifacts (dict, docs, postings)
│   └── backup\_parquets/      \# Clean state backups (REQUIRED for 'reset' command)
└── results/
├── performance\_results/  \# CSV logs from benchmark runs
├── performance\_plots/    \# Generated visualizations
└── query\_terms/          \# Generated query sets

```

---

## 🛠️ Installation & Setup

### Prerequisites
* **Conda** (Miniconda or Anaconda)
* **Linux/macOS** (Script uses bash)

### 1. Initialize Environment
Use the provided setup script to create the `dynamic-index-ducklake` Conda environment, install dependencies (`duckdb>=1.4.1`, `numpy`, `pyarrow`, `matplotlib`), and generate the required directory structure.

```bash
bash code/setup.sh
````

### 2\. Activate Environment

```bash
conda activate dynamic-index-ducklake
```

-----

## 💻 Usage

All operations are handled via the `dynamic_index.py` CLI tool. Ensure you are in the project root directory before running commands.

### Data Initialization

Load raw Parquet data into the system and build the initial inverted index.

```bash
# Initialize with all data found in parquet/webcrawl_data/
python code/dynamic_index.py initialise --parquet ALL

# Initialize with a specific limit (e.g., first 1000 docs)
python code/dynamic_index.py initialise --limit 1000
```

### Running Search Queries

Execute BM25 queries against the index using SQL-based scoring.

```bash
# Run a disjunctive (OR) query (default)
python code/dynamic_index.py query --q "artificial intelligence" --top 10

# Run a conjunctive (AND) query and show document content
python code/dynamic_index.py query --q "machine learning" --qtype conjunctive --show-content
```

### Dynamic Updates

Perform ACID-compliant modifications to the index.

```bash
# Delete a specific document by ID
python code/dynamic_index.py delete --docid 42

# Manually trigger a checkpoint/rewrite to clean up deleted rows
python code/dynamic_index.py checkpoint
```

### System Reset

Perform a "Hard Reset" to wipe the database and restore fresh data from `parquet/backup_parquets/`.

```bash
python code/dynamic_index.py reset
```

-----

## 📊 Performance Testing

The system includes a robust testing harness to analyze how deletions affect query performance.

### Run a Performance Loop

This command runs a loop of: *Measure Query Latency -\> Delete N Docs -\> Repeat*.

```bash
python code/dynamic_index.py perf-test \
    --query-count 100 \
    --delete-batch 1000 \
    --random \
    --plot
```

**Arguments:**

  * `--random`: Deletes documents in random order (vs. sequential).
  * `--checkpoint-pct`: Triggers a DuckLake checkpoint every N% of data deleted.
  * `--reset`: Wipes the database and restores from backup before starting.
  * `--plot`: Automatically generates a plot at the end of the run.

### Compare Results

Generate comparison plots from multiple test run CSVs (e.g., to compare performance with and without checkpoints).

```bash
python code/dynamic_index.py plot-comparison \
    results/performance_results/run1.csv \
    results/performance_results/run2.csv
```

-----

## ⚙️ Architecture Notes

1.  **Index Construction**: The index consists of three tables: `dict` (term dictionary), `docs` (document lengths), and `postings` (inverted list). These are initially built as Parquet artifacts.
2.  **Scoring**: Scoring is performed entirely within DuckDB using complex SQL queries that implement the BM25 probabilistic model.
3.  **Persistence**: The `ducklake` extension is used to attach the catalog. Data is stored in `ducklake/data_files`.
