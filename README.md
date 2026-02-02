# DuckLake Dynamic Indexing System

This project implements an ACID-compliant dynamic indexing and search system utilizing **DuckDB** and the **DuckLake** lakehouse infrastructure. It provides a BM25 Full-Text Search (FTS) engine capable of handling real-time updates (insertions and deletions) and persistent storage, managed entirely through DuckLake's metadata catalog.

## Overview

The system exploits the architecture of DuckLake to implement index maintenance using pure SQL, interacting with data tables without requiring imperative code for file management. Key capabilities include:

* **Dynamic Index Maintenance**: Supports document deletion and insertion while maintaining index consistency. Modifications are handled via DuckLake's "Merge-on-Read" strategy, which writes lightweight delete files rather than rewriting entire data files.
* **SQL-Based BM25 Search**: Implements the Okapi BM25 ranking function using vectorized SQL, supporting both Conjunctive (AND) and Disjunctive (OR) query semantics.
* **Lakehouse Integration**: Utilizes the `ducklake` extension to abstract metadata management. Tables are persisted as managed Parquet files, but interactions occur through a standard SQL interface.
* **Performance Analysis**: Includes tooling to benchmark query latency and analyze the trade-offs of the "Merge-on-Read" strategy as the index accumulates deleted rows.

## Project Structure

```text
.
├── code/
│   ├── dynamic_index.py      # Entry point for indexing, querying, and testing
│   ├── fts_tools.py          # BM25 scoring logic (Conjunctive/Disjunctive)
│   ├── index_tools.py        # Logic for creating and maintaining index tables
│   ├── helper_functions.py   # Database connection and path management
│   └── setup.sh              # Environment setup script
├── ducklake/                 # Managed storage area
│   ├── data_files/           # Physical data storage (managed by DuckLake)
│   └── metadata_catalog.ducklake
├── parquet/                  # Raw source documents (input corpus)
└── results/
    ├── performance_results/  # CSV metrics from benchmark runs
    ├── performance_plots/    # Generated visualizations
    └── query_terms/          # Generated query sets

```

## Installation and Setup

### Prerequisites

* **Conda** (Miniconda or Anaconda)
* **Linux/macOS**

### 1. Initialize Environment

Use the provided setup script to create the environment and install dependencies (including `duckdb`, `numpy`, `pyarrow`, and `matplotlib`).

```bash
bash code/setup.sh

```

### 2. Activate Environment

```bash
conda activate dynamic-index-ducklake

```

## Usage

All system operations are executed via the `dynamic_index.py` CLI tool.

### Data Initialization

This command initializes the `my_ducklake` catalog and builds the inverted index tables (`dict`, `docs`, `postings`) directly from the source Parquet files.

```bash
# Initialize with all data found in the parquet/ directory
python code/dynamic_index.py initialise --parquet "*"

# Initialize with a row limit for testing
python code/dynamic_index.py initialise --limit 1000

```

### Search Queries

Execute BM25 full-text search queries against the index.

```bash
# Run a disjunctive (OR) query (default)
python code/dynamic_index.py query --q "artificial intelligence" --top 10

# Run a conjunctive (AND) query and retrieve content
python code/dynamic_index.py query --q "machine learning" --qtype conjunctive --show-content

```

### Dynamic Updates

Perform ACID-compliant modifications to the index structure.

```bash
# Delete a specific document by ID
python code/dynamic_index.py delete --docid 42

# Manually trigger a checkpoint to rewrite data files and merge deletions
python code/dynamic_index.py checkpoint

```

### System Reset

The reset command wipes the internal DuckLake database files (`metadata_catalog.ducklake` and `data_files/`) but preserves the source Parquet files, allowing for a clean re-initialization.

```bash
python code/dynamic_index.py reset

```

## Performance Testing

The system includes a testing harness designed to measure the impact of the "Merge-on-Read" strategy on query latency.

### Performance Loop

This command executes a test loop: *Measure Latency -> Delete Batch -> Repeat*.

```bash
python code/dynamic_index.py perf-test \
    --query-count 100 \
    --delete-batch 10000 \
    --random \
    --plot

```

**Arguments:**

* `--random`: Deletes documents in a random order rather than sequentially.
* `--checkpoint-pct`: Triggers a DuckLake checkpoint (data rewrite) every N% of data deleted.
* `--reset`: Performs a hard reset of the database before starting the test.
* `--plot`: Automatically generates a performance plot upon completion.

### Comparative Analysis

Generate plots to compare the performance profiles of different maintenance strategies (e.g., comparing runs with different checkpoint intervals).

```bash
python code/dynamic_index.py plot-comparison \
    results/performance_results/run1.csv \
    results/performance_results/run2.csv

```

## Architecture Notes

1. **Index Construction**: The index consists of three primary tables created directly within the DuckLake catalog: `dict` (term dictionary), `docs` (document lengths), and `postings` (inverted list).
2. **Scoring**: The system uses a SQL-based implementation of the BM25 probabilistic model. The scoring function accounts for term frequency, inverse document frequency (smoothed), and document length normalization.
3. **Persistence**: Tables are persisted as managed Parquet files. Modifications utilize a Copy-on-Write mechanism for the catalog metadata and a Merge-on-Read strategy for data retrieval, ensuring efficient handling of updates without immediate file rewrites.