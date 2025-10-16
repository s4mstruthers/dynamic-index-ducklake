# Bachelor Thesis DuckDB–DuckLake Indexing Framework

## Overview

This project integrates **DuckDB** and **DuckLake** to efficiently manage and index text documents using a combination of:
- **DuckLake-managed virtual tables** for persistent data storage
- **Python-based indexing tools** to build inverted indexes
- **BM25 ranking** for query retrieval and evaluation

All index structures are exported to **Parquet files** (`dict.parquet`, `docs.parquet`, `postings.parquet`) for efficient reuse and integration.

---

## Core Components

### DuckLake Virtual Database

The virtual database is attached using:

```python
connect_ducklake(con)
```

This connects the `my_ducklake` namespace exposing:

| Table | Description |
|-------|--------------|
| `my_ducklake.data` | Main dataset containing `(docid, content)` |
| `my_ducklake.dict` | Term dictionary (termid → term, df) |
| `my_ducklake.docs` | Document stats (docid → length) |
| `my_ducklake.postings` | Term–document mapping (termid, docid, tf) |

The `data` table is virtual and typically originates from `metadata_0.parquet`.

---

## Index Tables

| Table | Columns | Purpose |
|--------|----------|----------|
| **dict** | `termid, term, df` | Term dictionary (document frequency per term) |
| **docs** | `docid, len` | Document length statistics |
| **postings** | `termid, docid, tf` | Term–document frequency mapping |

---

## Indexing Flow

### 1. Initialize and Import Data

Imports raw data (`metadata_0.parquet`) into DuckLake as a managed table:

```bash
python dynamic_index.py --mode initialise --parquet metadata_0.parquet --limit 1000
```

This:
- Loads data into `my_ducklake.data`
- Builds new Parquet index files (`dict.parquet`, `docs.parquet`, `postings.parquet`)
- Imports them back into DuckLake as physical tables

---

### 2. Run Integrity Tests

```bash
python dynamic_index.py --mode test
```

Validates correctness by:
- Inserting sample documents
- Modifying and deleting them
- Ensuring all term/document statistics (`df`, `tf`, `len`) remain consistent

---

### 3. Query with BM25

Run a BM25 ranking query:

```bash
python dynamic_index.py --mode query --q "artificial intelligence" --top 5 --show-content
```

Example output:

```
Top 5 for query: 'artificial intelligence'
 1. docid=42  score=4.832  |  'Artificial intelligence is a branch of computer science...'
 2. docid=317 score=3.229  |  'AI applications are found in...'
```

---

### 4. Sanity Check

To inspect schema and sample rows:

```bash
python dynamic_index.py --mode sanity
```

Outputs `DESCRIBE` info and top-2 rows from `dict`, `docs`, and `postings`.

---

## Tools Overview

### `helper_functions.py`
- Connects DuckLake and installs extensions (`ducklake`, `fts`)
- Handles tokenization using **spaCy**
- Provides helper functions: `get_freq`, `get_dl`, `get_avgdl`, etc.
- Implements:
  - `initialise_data(con, parquet, limit)` – import initial dataset
  - `import_data(con, parquet)` – merge additional data into DuckLake

---

### `index_tools.py`
- Builds the inverted index using Python and Pandas.
- Writes `dict.parquet`, `docs.parquet`, and `postings.parquet`.
- Reimports them into DuckLake.
- Functions:
  - `build_index_to_parquet_from_ducklake(con)`
  - `import_index_parquets_into_ducklake(con)`
  - `reindex(con)` – rebuilds index from existing `data`.

---

### `fts_tools.py`
Implements **BM25 ranking** logic.

Functions:
- `idf(con, termid)`
- `tf(con, termid, docid)`
- `bm25_score(con, terms, docid)`
- `match_bm25(con, query, top_n)`

---

### `test.py`
- Runs comprehensive end-to-end tests verifying:
  - Insertions
  - Modifications
  - Deletions
  - Index integrity (tf, df, doc length)
- Non-destructive: restores data state after tests.

---

### `dynamic_index.py`
Main CLI driver for all operations.

| Mode | Description |
|-------|-------------|
| `--mode test` | Run correctness tests |
| `--mode sanity` | Print schema + sample rows |
| `--mode query` | Run BM25 ranking query |
| `--mode import` | Upsert new data only |
| `--mode initialise` | Load data and build index |
| `--mode reindex` | Rebuild index from current data |

---

## Command Reference

| Command | Description |
|----------|-------------|
| `python dynamic_index.py --mode test` | Run insert/modify/delete integrity tests |
| `python dynamic_index.py --mode reindex --parquet metadata_0.parquet --limit 5000` | Reset and rebuild index (optional limit) |
| `python dynamic_index.py --mode query --q "machine learning" --top 5 --show-content` | Run BM25 ranking query |
| `python dynamic_index.py --mode sanity` | Print schema + sample rows for each table |

---

## Important Notes
- DuckLake tables are **virtual** and backed by Parquet files.
- All database modifications are **transactionally safe** (`BEGIN` / `COMMIT`).
- The test suite (`test.py`) is **non-destructive**—it restores the database after validation.
- Indexing can handle large datasets by streaming data in batches via generators.

---

## Example Workflow

```bash
# 1. Full rebuild from base metadata
python dynamic_index.py --mode initialise --parquet metadata_0.parquet --limit 1000

# 2. Verify structure
python dynamic_index.py --mode sanity

# 3. Query documents
python dynamic_index.py --mode query --q "artificial intelligence" --top 5 --show-content

# 4. Validate indexing integrity
python dynamic_index.py --mode test
```

---

## Citation
This project was developed as part of a **Bachelor Thesis** on scalable text indexing using **DuckDB + DuckLake** integration.
