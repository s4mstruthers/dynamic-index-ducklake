# DuckLake Dynamic Indexing System

An implementation of a **dynamic full-text indexing system** built on **DuckLake**, enabling document-level insertions, updates, and deletions without reindexing the entire corpus.

This project investigates **dynamic indexing in DuckLake**, utilizing its **transactional, snapshot-based storage engine** to efficiently modify large datasets. By leveraging **ACID transactions** and **SQL-driven data operations**, it allows precise index updates and supports **document takedowns or content revisions** without disrupting overall index integrity.

---

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/s4mstruthers/dynamic-index-ducklake.git
cd dynamic-index-ducklake
```

### 2. Create the Environment
Use the provided setup script to create a Conda environment and initialize required folders.

```bash
chmod +x setup.sh
./setup.sh
# To reinstall cleanly:
# ./setup.sh --reinstall
```

### 3. Activate the Environment
```bash
conda activate dynamic-index-ducklake
```

### 4. Verify Installation
```bash
python code/dynamic_index.py --mode sanity
```

---

## Overview

This project demonstrates an **incrementally updatable full-text search index** using **DuckLake** as the underlying data catalog.  
Traditional search engines often require full reindexing when the corpus changes. This implementation overcomes that limitation by using **transactional operations** and **snapshot isolation** provided by DuckLake.

### Key Advantages

- **Incremental Updates** – Add or modify documents without full index rebuilds.  
- **Takedown Support** – Remove individual documents while preserving consistency.  
- **Transactional Consistency** – Built on DuckLake’s ACID guarantees.  
- **Concurrent Access** – Multiple users can interact safely through snapshots.  
- **Efficient Storage** – Index stored as compressed Parquet files.  

---

## System Architecture

```
Parquet → my_ducklake.main.data → build_index_to_parquet → dict/docs/postings → import_index_parquets_into_ducklake
```

| Step | Action | Source | Destination |
|------|--------|--------|-------------|
| 1 | Load and tokenize data | Parquet | In-memory |
| 2 | Compute term frequencies and document stats | Python | Buffers |
| 3 | Write index structures | Memory | Parquet files |
| 4 | Load index into DuckLake | Parquet | SQL tables |

---

## Core Functionality

### Initialise
Rebuild the dataset and index from one or more Parquet files.
```bash
python code/dynamic_index.py --mode initialise --parquet ALL
```

### Import (Incremental Update)
Add or update new data without reindexing.
```bash
python code/dynamic_index.py --mode import --parquet parquet/webcrawl_data/metadata_1.parquet
```

### Delete (Document Removal)
Safely remove individual documents and update dictionary statistics.
```bash
python code/dynamic_index.py --mode delete --docid 17998
```

### Reindex (Full Rebuild)
Rebuild all index structures if necessary.
```bash
python code/dynamic_index.py --mode reindex
```

### Query
Perform BM25-based full-text queries.
```bash
# Disjunctive (OR semantics)
python code/dynamic_index.py --mode query --q "machine learning"

# Conjunctive (AND semantics)
python code/dynamic_index.py --mode query --q "deep learning" --qtype conjunctive
```

### Cleanup
Remove expired or orphaned DuckLake-managed data.
```bash
python code/dynamic_index.py --mode cleanup --older-than 7 --dry-run
python code/dynamic_index.py --mode cleanup-orphans
```

### Sanity / Testing
Run basic environment and functional tests.
```bash
python code/dynamic_index.py --mode sanity
python code/dynamic_index.py --mode test
```

---

## BM25 Ranking Model

The BM25 ranking formula used is:

```
score(D, Q) = Σ_t∈Q idf(t) * ((k1 + 1) * f(t, D)) / (f(t, D) + k1 * (1 - b + b * (len(D) / avgdl)))
```

Parameters:  
- `idf(t) = ln((N - df + 0.5) / (df + 0.5))`  
- `k1 = 1.2`  
- `b = 0.75`  

Modes:
- **Conjunctive (AND)** → documents must contain all query terms.  
- **Disjunctive (OR)** → documents may contain any query terms.  

---

## Components

| File | Description |
|------|--------------|
| `dynamic_index.py` | CLI entry point for all index and query operations. |
| `helper_functions.py` | Manages connections, tokenization, ingestion, and cleanup. |
| `index_tools.py` | Builds and maintains index structures (dictionary, docs, postings). |
| `fts_tools.py` | Handles BM25 scoring and query evaluation. |
| `test.py` | Provides system-level testing and validation utilities. |

---

## Folder Structure

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

## Dependencies

- Python ≥ 3.10  
- DuckDB ≥ 1.4  
- DuckLake extension  
- numpy  
- pyarrow  

---

## License

MIT License — free to use, modify, and distribute with attribution.

---

**Author:** Sam Struthers  
**Repository:** [https://github.com/s4mstruthers/dynamic-index-ducklake](https://github.com/s4mstruthers/dynamic-index-ducklake)  
**Topic:** *Dynamic Indexing in DuckLake — Efficient Incremental Updates and Document-Level Takedowns*
