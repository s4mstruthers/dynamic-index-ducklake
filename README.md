# DuckLake Dynamic Indexing System

This project implements a **dynamic, programmatic full-text search (FTS) index** using **DuckDB + DuckLake**. 
It enables incremental data imports, efficient reindexing, and flexible BM25-based search across a large document corpus.

---

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/s4mstruthers/dynamic-index-ducklake.git
cd dynamic-index-ducklake
```

### 2. Create the Environment
Use the provided setup script to create a Conda environment and directory structure.

```bash
chmod +x create_dynamic_index_env.sh
./create_dynamic_index_env.sh
# Or to reinstall cleanly:
# ./create_dynamic_index_env.sh --reinstall
```

### 3. Activate the Environment
```bash
conda activate dynamic-index-ducklake
```

### 4. Run a Sanity Test
```bash
python dynamic_index.py --mode sanity
```

---

## 📖 Project Overview

The system consists of modular Python components for indexing, querying, and managing document data. 
It leverages DuckLake as the metadata catalog and DuckDB’s analytics engine for storage and retrieval.

### Core Components

| File | Description |
|------|--------------|
| `dynamic_index.py` | CLI entry point — handles import, indexing, querying, deletion, cleanup. |
| `helper_functions.py` | Utilities for tokenization, connecting to DuckLake, and I/O management. |
| `index_tools.py` | Implements index building (dictionary, documents, postings) and reindexing logic. |
| `fts_tools.py` | BM25-based full-text search functions (AND/OR modes). |
| `test.py` | Functional test routines for validating setup and indexing. |

---

## 🧠 Key Concepts

### DuckLake Catalog

The metadata catalog (`metadata_catalog.ducklake`) stores schema definitions and pointers to data files.  
If it doesn’t exist, the system automatically creates one:

```sql
CREATE DUCKLAKE 'metadata_catalog.ducklake' (DATA_PATH 'data/');
```

### Index Structure

| Table | Description |
|--------|-------------|
| `dict` | Unique terms (`termid`, `term`, `df`) |
| `docs` | Documents (`docid`, `len`) |
| `postings` | Postings (`termid`, `docid`, `tf`) |

---

## ⚙️ Functionality

### 1. Initialise
```bash
python dynamic_index.py --mode initialise --parquet extra/metadata_0.parquet
```

Imports data, tokenizes content, builds Parquets, and registers them in DuckLake.

### 2. Import (Incremental)
```bash
python dynamic_index.py --mode import --parquet extra/metadata_1.parquet
```

### 3. Reindex
```bash
python dynamic_index.py --mode reindex
```

### 4. Query
**Disjunctive (OR):**
```bash
python dynamic_index.py --mode query --q "machine learning"
```
**Conjunctive (AND):**
```bash
python dynamic_index.py --mode query --q "deep learning" --qtype conjunctive
```

### 5. Delete
```bash
python dynamic_index.py --mode delete --docid 17998
```

### 6. Cleanup
```bash
python dynamic_index.py --mode cleanup --older-than 7 --dry-run
```

### 7. Test
```bash
python dynamic_index.py --mode test
```

---

## 🔍 BM25 Querying

BM25 formula:
```
score(D, Q) = Σ_t∈Q idf(t) * ((k1 + 1) * f(t, D)) / (f(t, D) + k1 * (1 - b + b * (len(D) / avgdl)))
```
with:
- `idf(t) = ln((N - df + 0.5) / (df + 0.5))`
- `k1 = 1.2`, `b = 0.75`

Modes:
- **Conjunctive** → documents with all query terms
- **Disjunctive** → documents with any query term

---

## 🏗 Architecture

```
Parquet → my_ducklake.main.data → build_index_to_parquet → dict/docs/postings → import_index_parquets_into_ducklake
```

| Step | Action | Source | Destination |
|------|--------|---------|-------------|
| 1 | Stream data | `my_ducklake.main.data` | Python memory |
| 2 | Tokenize + compute TF/DF | Python | DataFrame |
| 3 | Save Parquets | pandas → Parquet | Disk |
| 4 | Import to DuckLake | DuckDB SQL | DuckLake tables |

---

## 🧩 Dependencies

- Python ≥ 3.10  
- DuckDB ≥ 1.4  
- DuckLake extension  
- pandas  
- spaCy  
- pyarrow / fastparquet  

---

## 📂 Folder Structure

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
│   ├── webcrawl_data/
```

---

## ⚖️ License

MIT License — free to use, modify, and distribute with attribution.

---

**Author:** Sam Struthers  
**Repository:** [https://github.com/s4mstruthers/dynamic-index-ducklake](https://github.com/s4mstruthers/dynamic-index-ducklake)  
**Purpose:** BSc Computer Science Thesis — *Dynamic Full-Text Search with DuckLake*
