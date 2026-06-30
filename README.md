# DuckLake Dynamic Indexing System

An ACID-compliant dynamic indexing and search system built on **DuckDB** and the
**DuckLake** lakehouse format. It implements an Okapi BM25 Full-Text Search (FTS)
engine that supports real-time updates (insertions and deletions) and persistent
storage, managed entirely through DuckLake's SQL-based metadata catalog.

This repository accompanies my bachelor's thesis, included here as
[Bachelor_Thesis_Dynamic_Indexing_DuckLake.pdf](Bachelor_Thesis_Dynamic_Indexing_DuckLake.pdf).
The thesis evaluates the document-deletion workload and the cost of DuckLake's
Merge-on-Read strategy as the index accumulates deletions. Insertion, modification,
and conjunctive querying are included here as supporting functionality that rounds
out the system but was not part of the thesis benchmarks.

## Overview

The system uses DuckLake to maintain a search index in pure SQL, interacting with
data tables without imperative file-management code. Key capabilities:

* **Dynamic index maintenance** — Documents can be inserted and deleted while the
  index stays consistent. Modifications use DuckLake's *Merge-on-Read* strategy,
  writing lightweight delete files instead of rewriting whole data files.
* **SQL-based BM25 search** — The Okapi BM25 ranking function is implemented in
  vectorized SQL, supporting both conjunctive (AND) and disjunctive (OR) query
  semantics.
* **Lakehouse integration** — The `ducklake` extension abstracts metadata
  management. Tables are persisted as managed Parquet files but accessed through a
  standard SQL interface.
* **Performance analysis** — A benchmarking harness measures query latency and the
  trade-offs of the Merge-on-Read strategy as the index accumulates deleted rows.

## Project Structure

```text
.
├── code/
│   ├── dynamic_index.py      # CLI entry point: indexing, querying, perf testing, plotting
│   ├── fts_tools.py          # BM25 scoring logic (conjunctive / disjunctive)
│   ├── index_tools.py        # Index build and point-update logic (reindex, delete, insert)
│   └── helper_functions.py   # DuckLake connection, tokenization, data ingest, paths
├── ducklake/                 # Managed storage (created at runtime, git-ignored)
│   ├── data_files/           # Physical Parquet data managed by DuckLake
│   └── metadata_catalog.ducklake
├── parquet/                  # Raw source documents — input corpus (git-ignored)
│   ├── webcrawl_data/        # Raw web-crawl Parquet files
│   ├── index/                # Optional pre-built index artifacts
│   └── backup_parquets/      # Backups
├── results/                  # Benchmark outputs (created at runtime, git-ignored)
│   ├── performance_results/  # CSV metrics from benchmark runs
│   ├── performance_plots/    # Generated PNG visualizations
│   └── query_terms/          # Generated query sets
├── setup.sh                  # Environment + directory setup script
├── requirements.txt          # Python dependencies
└── README.md
```

> **Note:** `ducklake/`, `parquet/`, and `results/` are listed in `.gitignore`.
> They are created automatically by `setup.sh` (and at runtime), so they will not
> appear in a fresh clone until you run the setup script or add data.

## Installation and Setup

### Prerequisites

* **Python 3** (3.10+ recommended; the tokenizer uses `list[str]` type hints)
* **Linux or macOS**
* One of:
  * **Conda** (Miniconda or Anaconda) — recommended, used by `setup.sh`, or
  * **pip** — for a manual install

### Option A — Conda (recommended)

`setup.sh` creates a Conda environment named `dynamic-index-ducklake`, installs all
dependencies, and creates the project directory structure.

```bash
bash setup.sh
conda activate dynamic-index-ducklake
```

Re-run with `--reinstall` to rebuild the environment from scratch:

```bash
bash setup.sh --reinstall
```

### Option B — pip

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> **Dependencies:** `duckdb>=1.4.1`, `numpy`, `pandas`, and `matplotlib`.
> DuckDB 1.4.1+ is required because the DuckLake 1.0 catalog format depends on it.
> `pandas` is needed at runtime by DuckDB's `.fetch_df()` even though it is not
> imported directly.

## Usage

All operations run through the `dynamic_index.py` CLI:

```bash
python code/dynamic_index.py <command> [options]
```

### Command Summary

| Command           | Purpose                                                        |
| ----------------- | ------------------------------------------------------------- |
| `initialise`      | Rebuild the data table from Parquet and fully reindex          |
| `import`          | Upsert Parquet data into the data table (no reindex)           |
| `reindex`         | Rebuild the index (`dict`, `docs`, `postings`) from data       |
| `query`           | Run a BM25 search query                                        |
| `delete`          | Delete a document by ID and repair the index                   |
| `insert`          | Insert a new document and update the index in place            |
| `modify`          | Replace the content of an existing document                    |
| `checkpoint`      | Manually trigger a DuckLake rewrite/merge of delete files      |
| `reset`           | Wipe the DuckLake DB files (source Parquet preserved)          |
| `sanity`          | Inspect the DuckLake schema and preview table contents         |
| `perf-test`       | Run the deletion/latency benchmark loop                        |
| `plot-comparison` | Compare benchmark CSVs and generate plots                      |

### Data Initialization

Builds the `data` table and the inverted-index tables (`dict`, `docs`, `postings`)
directly from the source Parquet files in `parquet/`.

```bash
# Initialize from all *.parquet files found in parquet/
python code/dynamic_index.py initialise --parquet "*"

# Restrict to a subfolder (path is resolved relative to parquet/)
python code/dynamic_index.py initialise --parquet webcrawl_data

# Initialize with a row limit (useful for quick testing)
python code/dynamic_index.py initialise --limit 1000
```

### Incremental Import

Upserts documents into the `data` table **without** rebuilding the index. Run
`reindex` afterwards to refresh the index artifacts.

```bash
python code/dynamic_index.py import --parquet new_batch.parquet
python code/dynamic_index.py reindex
```

### Reindex

Rebuilds `dict`, `docs`, and `postings` from the current contents of the `data`
table.

```bash
python code/dynamic_index.py reindex
```

### Search Queries

Run BM25 full-text searches against the index.

```bash
# Disjunctive (OR) query — the default
python code/dynamic_index.py query --q "artificial intelligence" --top 10

# Conjunctive (AND) query, also printing matched content
python code/dynamic_index.py query --q "machine learning" --qtype conjunctive --show-content
```

**Arguments:**

* `--q` / `--query`: Query string (required).
* `--top`: Number of results to return (default: `10`).
* `--qtype`: `disjunctive` (default) or `conjunctive`.
* `--show-content`: Print a snippet of each matching document.

### Dynamic Updates

These commands modify a live index incrementally, without a full reindex.

```bash
# Insert a new document (a docid is assigned automatically)
python code/dynamic_index.py insert --content "the quick brown fox"

# Insert with an explicit docid
python code/dynamic_index.py insert --content "the quick brown fox" --docid 1001

# Replace the content of an existing document
python code/dynamic_index.py modify --docid 1001 --content "a slow grey wolf"

# Delete a specific document by ID (cascades to dict/docs/postings/data)
python code/dynamic_index.py delete --docid 42

# Manually merge accumulated delete files and rewrite data files
python code/dynamic_index.py checkpoint
```

**Arguments:**

* `insert` — `--content` / `--text` (required): document text to index;
  `--docid` (optional): explicit ID, otherwise the next available ID is assigned.
* `modify` — `--docid` (required): ID of the document to replace; `--content` /
  `--text` (required): the new document text. Implemented as a delete followed by a
  re-insert within a single transaction.
* `delete` — `--docid` (required): ID of the document to remove.

### Inspect (Sanity Check)

Attaches the catalog and prints the schema plus a two-row preview of each index
table — handy for confirming the index built correctly.

```bash
python code/dynamic_index.py sanity
```

### System Reset

Wipes the internal DuckLake files (`metadata_catalog.ducklake` and `data_files/`)
while preserving the source Parquet files, allowing a clean re-initialization.

```bash
python code/dynamic_index.py reset
```

### Upgrading the DuckLake Extension

If the DuckLake extension is upgraded to a new major version, an existing catalog
may become incompatible (e.g. `catalog version is 0.3, but the extension requires
version 1.0`). Because the catalog is fully derived from the source Parquet files,
the simplest fix is a reset and re-initialization — no source data is lost:

```bash
python code/dynamic_index.py reset
python code/dynamic_index.py initialise --parquet "*"
```

## Performance Testing

The benchmark harness measures the impact of the Merge-on-Read strategy on query
latency as documents are deleted.

### Performance Loop

Runs the loop: *measure latency → delete a batch → repeat*, logging results to a
CSV in `results/performance_results/`.

```bash
python code/dynamic_index.py perf-test \
    --query-count 100 \
    --delete-batch 10000 \
    --random \
    --plot
```

**Arguments:**

* `--query-count`: Queries measured per iteration (default: `100`).
* `--delete-batch`: Documents deleted per step (default: `10000`).
* `--qtype`: `disjunctive` (default) or `conjunctive`.
* `--top`: Results per query during measurement (default: `10`).
* `--random`: Delete documents in random order rather than sequentially.
* `--checkpoint-pct`: Trigger a DuckLake checkpoint every N% of data deleted.
* `--reset`: Hard-reset and re-initialize the database before starting.
* `--plot`: Generate a performance plot when the run completes.
* `--plot-file`: Custom filename for the single-run plot.
* `--results-csv`: Output CSV filename (saved in `results/performance_results/`).
* `--queries-csv`: Output filename for the generated query set.
* `--reuse-file`: Reuse a specific query file from `results/query_terms/`.
* `--reuse-latest`: Reuse the most recent generated query file.

### Comparative Analysis

Generates plots comparing different maintenance strategies (e.g. runs with
different checkpoint intervals). By default this produces a raw query-times plot.
Add `--cumulative` to also produce a cumulative total-query-cost plot, which shows
the aggregate query time paid over the full index lifetime.

```bash
# Raw query times comparison
python code/dynamic_index.py plot-comparison \
    results/performance_results/run1.csv \
    results/performance_results/run2.csv \
    --out-raw combined_results.png

# Include the cumulative cost plot
python code/dynamic_index.py plot-comparison \
    results/performance_results/run1.csv \
    results/performance_results/run2.csv \
    --cumulative \
    --out-raw combined_results.png \
    --out-cum combined_cumulative.png
```

**Arguments:**

* `csv_files`: One or more result CSVs (bare filenames are resolved against
  `results/performance_results/`).
* `--out-raw`: Output filename for the raw query-times plot (default:
  `combined_times.png`).
* `--cumulative`: Also generate the cumulative total-query-cost plot.
* `--out-cum`: Output filename for the cumulative plot (default:
  `combined_cumulative.png`).
* `--qtype`: Query type to plot — `disjunctive` (default) or `conjunctive`.
* `--top`: Results-per-query value used during the run being plotted (default:
  `10`).
* `--show`: Display the plots interactively in addition to saving them.

All plots are saved to `results/performance_plots/`.

## Architecture Notes

1. **Index structure** — The index consists of three tables in the DuckLake
   catalog: `dict` (term dictionary with document frequency), `docs` (document
   lengths), and `postings` (the inverted list of term→document→term-frequency).
2. **Scoring** — A SQL implementation of the BM25 probabilistic model accounts for
   term frequency, smoothed inverse document frequency, and document-length
   normalization. The parameters `k1 = 1.2` and `b = 0.75` are fixed for
   reproducibility.
3. **Persistence** — Tables are stored as managed Parquet files. Catalog metadata
   uses Copy-on-Write, while data retrieval uses Merge-on-Read, so updates avoid
   immediate full-file rewrites until a checkpoint is triggered.

## Testing

`tests/verify_index.py` checks index correctness in three layers and exits non-zero
if any check fails (so it can be wired into CI). It assumes an index already exists
(run `initialise` first).

```bash
# Structural invariants only — read-only, safe to run any time
python tests/verify_index.py

# Also run an insert -> modify -> delete cycle on a sentinel document
# (uses unique nonsense terms and removes the sentinel afterwards)
python tests/verify_index.py --behaviour

# Also run the reindex-parity check (rebuilds the index from scratch and
# confirms the incrementally maintained tables match)
python tests/verify_index.py --parity

# Everything
python tests/verify_index.py --all
```

The three layers are:

1. **Invariants** — internal consistency of the live index (`dict.df` equals the real
   document frequency, `docs.len` equals the summed term frequencies, no orphan
   postings, and `data`/`docs` row counts agree).
2. **Behaviour** — a full `insert → modify → delete` cycle, asserting the index
   reacts correctly at each step, then cleans up after itself.
3. **Parity** — the strongest test: snapshot the index, run a full `reindex` (the
   from-scratch source of truth), and confirm the two are identical. The comparison
   is keyed on the term *string* rather than `termid`, because `reindex` and `insert`
   assign term IDs differently.

## License

This project is released under the MIT License. See [LICENSE](LICENSE) for details.
```
