# DuckLake Dynamic Indexing System

This project implements a **dynamic, programmatic full-text search (FTS) index** using **DuckDB + DuckLake**. 
It enables incremental data imports, efficient reindexing, and flexible BM25-based search across a large document corpus.

---

## Project Overview

The system consists of modular Python components for indexing, querying, and managing document data. 
It leverages DuckLake as the metadata catalog and DuckDB’s analytics engine for storage and retrieval.

### Core Components

| File | Description |
|------|--------------|
| `dynamic_index.py` | CLI entry point — handles import, indexing, querying, deletion, cleanup. |
| `helper_functions.py` | Provides utilities for tokenization, connecting to DuckLake, and I/O management. |
| `index_tools.py` | Implements index building (dictionary, documents, postings), and reindexing logic. |
| `fts_tools.py` | Provides BM25-based full-text search functions with conjunctive (AND) and disjunctive (OR) modes. |
| `test.py` | Contains functional test routines for validating setup and indexing. |

---

## Key Concepts

### DuckLake Catalog

The metadata catalog (e.g. `metadata_catalog.ducklake`) stores schema definitions and pointers to data files.

If it doesn’t exist, the system automatically creates one using:
```sql
CREATE DUCKLAKE 'metadata_catalog.ducklake' (DATA_PATH 'data/');
```

### Index Structure

The system builds three main tables:

| Table | Description |
|--------|-------------|
| `dict` | Contains unique terms (`termid`, `term`, `df`) |
| `docs` | Contains documents (`docid`, `len`) |
| `postings` | Contains postings (`termid`, `docid`, `tf`) |

These are stored as Parquet files and imported back into DuckLake.

---

## Functionality

### 1. Initialise
Create and index a dataset from a source Parquet file.

```bash
python dynamic_index.py --mode initialise --parquet extra/metadata_0.parquet
```

This:
1. Imports all rows from the Parquet file into `my_ducklake.main.data`
2. Tokenizes text content
3. Builds dict/docs/postings Parquet files
4. Imports those into DuckLake tables

---

### 2. Import (Incremental Update)
Add or update documents from a Parquet file without rebuilding the index.

```bash
python dynamic_index.py --mode import --parquet extra/metadata_1.parquet
```

---

### 3. Reindex
Rebuild the index from existing data.

```bash
python dynamic_index.py --mode reindex
```

---

### 4. Query
Perform BM25 ranking queries (with AND/OR semantics).

**Disjunctive (default, OR semantics):**
```bash
python dynamic_index.py --mode query --q "machine learning"
```

**Conjunctive (AND semantics):**
```bash
python dynamic_index.py --mode query --q "deep learning" --qtype conjunctive
```

Each result shows document ID, score, and optionally content preview (`--show-content`).

---

### 5. Delete
Remove a single document (and all related postings).

```bash
python dynamic_index.py --mode delete --docid 17998
```

Automatically updates dictionary statistics (`df`).

---

### 6. Cleanup
Remove old or orphaned data files.

```bash
python dynamic_index.py --mode cleanup --older-than 7 --dry-run
```

Or to remove orphans only:
```bash
python dynamic_index.py --mode cleanup-orphans
```

---

### 7. Sanity / Test Modes

Check setup or run internal tests.

```bash
python dynamic_index.py --mode sanity
python dynamic_index.py --mode test
```

---

## BM25 Querying (fts_tools.py)

BM25 computes relevance using:
```
score(D, Q) = Σ_t∈Q idf(t) * ((k1 + 1) * f(t, D)) / (f(t, D) + k1 * (1 - b + b * (len(D) / avgdl)))
```
with:
- `idf(t) = ln((N - df + 0.5) / (df + 0.5))`
- `k1 = 1.2`, `b = 0.75`

Modes:
- **Conjunctive**: only documents containing *all* query terms are returned.
- **Disjunctive**: any document containing *any* query term is scored.

---

## Architecture

```
Parquet → my_ducklake.main.data → build_index_to_parquet → dict/docs/postings → import_index_parquets_into_ducklake
```

| Step | Action | Source | Destination |
|------|--------|---------|-------------|
| 1 | Stream all data | `my_ducklake.main.data` | Python memory |
| 2 | Tokenize and compute TF/DF | Python | `DataFrame` |
| 3 | Save Parquets | pandas → Parquet | Disk |
| 4 | Import into DuckLake | DuckDB SQL | my_ducklake tables |

---

## Dependencies

- Python ≥ 3.10
- DuckDB ≥ 1.4
- DuckLake extension
- pandas
- spaCy (for tokenization)
- pyarrow / fastparquet

---

## Example Query Output

```
Top 5 for disjunctive BM25 query: 'machine learning' (raw BM25 scores)
 1. docid=42   score=9.128390  |  'Machine learning is a field of artificial intelligence...'
 2. docid=103  score=8.447201  |  'Deep learning techniques are subsets of machine learning...'
 3. docid=7    score=6.893223  |  'Supervised learning algorithms use labeled data...'
```

---

## Maintenance Notes

- Old `.ducklake` or `.parquet` files can be pruned using cleanup modes.
- If index corruption occurs, simply run:
  ```bash
  python dynamic_index.py --mode reindex
  ```
- For best performance on large datasets, ensure `PARQUET_ENGINE='pyarrow'` and `PARQUET_COMPRESSION='zstd'`.

---

## Tips for Large-Scale Use

- Use `PRAGMA threads=<n>` to leverage all CPU cores.
- Keep `BATCH_SIZE` high (e.g. 10,000) in `_iter_data()` for minimal roundtrips.
- Avoid scaling BM25 scores; they are raw and proportional to true relevance.

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
│   ├── ducklake/
│   │   ├── data_files/
|   |   |   ├── data/
|   |   |   ├── dict/
|   |   |   ├── docs/
|   |   |   ├── postings/
│   │   └── metadata_catalog.ducklake
│   ├── parquet/
│   |   ├── index/
│   │   |   ├── dict.parquet
│   │   |   ├── docs.parquet
│   │   |   ├── postings.parquet
│   │   ├── webcrawl_data/
|   |   |   ├── metadata_0.parquet
|   |   |   ├── metadata_1.parquet
|   |   |   ├── ...

```

---

## License

MIT License — free to use, modify, and distribute with attribution.

---

**Author:** Sam Struthers  
**Purpose:** BSc Computer Science Thesis — Dynamic Full-Text Search with DuckLake  
