# DuckLake Dynamic Indexing System

A programmatic **full‑text search (FTS)** pipeline built on **DuckDB + DuckLake**.  
It supports **incremental imports**, **deterministic reindexing**, and **BM25** ranking over large corpora.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Environment Setup](#environment-setup)
  - [Conda](#conda)
  - [Python venv](#python-venv)
- [Project Overview](#project-overview)
- [Core Commands](#core-commands)
- [BM25 Details](#bm25-details)
- [Architecture](#architecture)
- [Folder Layout](#folder-layout)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Quick Start

### 1) Clone the repository
```bash
git clone https://github.com/s4mstruthers/dynamic-index-ducklake.git
cd dynamic-index-ducklake/code
```

### 2) Create the environment (Conda)
```bash
chmod +x ../create_dynamic_index_env.sh
../create_dynamic_index_env.sh
# or clean reinstall:
# ../create_dynamic_index_env.sh --reinstall
```

### 3) Activate
```bash
conda activate dynamic-index-ducklake
```

### 4) Sanity check
```bash
python dynamic_index.py --mode sanity
```

> The sanity command attaches DuckLake and prints quick schema previews of `dict`, `docs`, and `postings` if present.

---

## Environment Setup

### Conda
The repository includes a convenience script that:
- Creates a Conda environment with the **latest Python** available on your channels.
- Installs `duckdb>=1.4.1`, `numpy`, and `pyarrow` from `conda-forge`.
- Optionally recreates the environment when run with `--reinstall`.
- Creates the local directory structure:
  - `ducklake/data_files/`
  - `parquet/index/`
  - `parquet/webcrawl_data/`

To run:
```bash
cd dynamic-index-ducklake
./create_dynamic_index_env.sh
conda activate dynamic-index-ducklake
```

### Python venv
If you prefer a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate           # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt     # see pinned versions below
```
**Minimal `requirements.txt`:**
```
duckdb>=1.4.1
numpy>=1.26.0
pyarrow>=16.0.0
```

---

## Project Overview

The system keeps data in a DuckLake catalog and builds a BM25 index as Parquet artifacts (`dict.parquet`, `docs.parquet`, `postings.parquet`), which are then loaded back into DuckLake tables.

### Core Components
| File | Purpose |
|------|---------|
| `dynamic_index.py` | CLI entry point for all operations. |
| `helper_functions.py` | Attaches DuckLake, tokenization, ingest helpers, cleanup. |
| `index_tools.py` | Streaming index builder to Parquet + loaders into DuckLake. |
| `fts_tools.py` | BM25 query runners (conjunctive/AND, disjunctive/OR). |
| `test.py` | Functional smoke tests for validation. |

### DuckLake Catalog
- The metadata catalog file is `ducklake/metadata_catalog.ducklake`.
- **Creation/attach is automatic** via the DuckLake extension:
  ```sql
  INSTALL ducklake; LOAD ducklake;
  ATTACH 'ducklake:/absolute/path/to/ducklake/metadata_catalog.ducklake'
    AS my_ducklake (DATA_PATH '/absolute/path/to/ducklake/data_files');
  ```
- `DATA_PATH` holds the physical data files managed by DuckLake. The code ensures this directory exists before attach.

---

## Core Commands

Below commands are run from `dynamic-index-ducklake/code` with an active environment.

### Initialise (create/replace base data, then index)
Import a **single file**, a **directory**, or **all** Parquets:
```bash
# Single file in default folder
python dynamic_index.py --mode initialise --parquet metadata_0.parquet

# All files in default folder (parquet/webcrawl_data/*.parquet)
python dynamic_index.py --mode initialise --parquet ALL

# Absolute file / or an arbitrary directory of parquets
python dynamic_index.py --mode initialise --parquet /abs/path/to/file.parquet
python dynamic_index.py --mode initialise --parquet /abs/path/to/folder
```
Optional limit for a small test run:
```bash
python dynamic_index.py --mode initialise --parquet ALL --limit 50000
```

### Import (incremental upsert of raw data, no reindex)
```bash
python dynamic_index.py --mode import --parquet parquet/webcrawl_data/metadata_1.parquet
# Follow with a reindex if you need postings/dict updated:
python dynamic_index.py --mode reindex
```

### Query (BM25)
```bash
# OR semantics (default)
python dynamic_index.py --mode query --q "machine learning" --top 10 --show-content

# AND semantics
python dynamic_index.py --mode query --q "deep learning" --qtype conjunctive --top 10
```

### Delete a document
```bash
python dynamic_index.py --mode delete --docid 17998
```

### Cleanup storage
```bash
# Expired snapshots scheduled by DuckLake
python dynamic_index.py --mode cleanup --older-than 7 --dry-run

# Orphaned files (untracked)
python dynamic_index.py --mode cleanup-orphans --older-than 7 --dry-run
``

### Test / Sanity
```bash
python dynamic_index.py --mode test
python dynamic_index.py --mode sanity
```

---

## BM25 Details

We use the standard BM25 variant:
```
score(D, Q) = Σ_t∈Q idf(t) * ((k1 + 1) * f(t, D)) / (f(t, D) + k1 * (1 - b + b * (len(D) / avgdl)))
```
with:
- `idf(t) = ln((N - df + 0.5) / (df + 0.5))`
- `k1 = 1.2`, `b = 0.75`

Modes:
- **Conjunctive (AND):** document must contain **all** query terms.
- **Disjunctive (OR):** document may contain **any** query term.

---

## Architecture

```
Parquet → my_ducklake.main.data → build_index_to_parquet → dict/docs/postings → import_index_parquets_into_ducklake
```

| Step | Action | Source | Destination |
|------|--------|--------|-------------|
| 1 | Stream rows | `my_ducklake.main.data` | Python |
| 2 | Tokenize + TF/DF | Python | in-memory buffers |
| 3 | Write Parquets | Python → Parquet | `parquet/index/` |
| 4 | Load into DuckLake | DuckDB SQL | `my_ducklake.{dict,docs,postings}` |

---

## Folder Layout

```
project_root/
├── code/
│   ├── dynamic_index.py
│   ├── helper_functions.py
│   ├── index_tools.py
│   ├── fts_tools.py
│   ├── test.py
├── ducklake/
│   ├── data_files/
│   └── metadata_catalog.ducklake
├── parquet/
│   ├── index/
│   └── webcrawl_data/
```

---

## Troubleshooting

- **`Parser Error: syntax error at or near "DUCKLAKE"`**  
  There is no `CREATE DUCKLAKE ...` command. The catalog is created by `ATTACH 'ducklake:<path>' (...).` The code already uses this pattern.

- **CLI version mismatch** (e.g., `duckdb --version` shows 1.3.x but Python `duckdb.__version__` is 1.4.x):  
  Your shell is picking a different CLI on the PATH (e.g., Homebrew). Either install `duckdb-cli` in Conda, use `python -m duckdb --version`, or upgrade your system CLI (`brew upgrade duckdb`).

- **No results for queries**:  
  Check that you ran `--mode initialise` or `--mode reindex` after importing data, and that the `dict/docs/postings` tables exist in `my_ducklake`.

---

## License

**MIT License** — free to use, modify, and distribute with attribution.

---

**Author:** Sam Struthers  
**Repository:** https://github.com/s4mstruthers/dynamic-index-ducklake  
**Purpose:** BSc Computer Science Thesis — *Dynamic Full‑Text Search with DuckLake*
