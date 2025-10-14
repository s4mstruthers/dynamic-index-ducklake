# Bachelor Thesis DuckDB–DuckLake Indexing Framework

# Overview

This project integrates DuckDB and DuckLake to manage and index text documents efficiently using a combination of:
	•	Virtual DuckLake tables for document storage
	•	Python-based indexing tools to generate inverted indexes
	•	BM25 ranking for query retrieval and search testing

All indexing data and search structures are stored as Parquet files to allow efficient re-importing into DuckLake.

# Core Components

### DuckLake Virtual Database

	All DuckLake tables are attached via:
	
	`connect_ducklake(con)`
	
	This attaches the metadata catalog and exposes:
		•	`my_ducklake.data `– the main dataset containing (docid, content)
		•	`my_ducklake.dict`, `my_ducklake.docs`, and `my_ducklake.postings` – created/updated by indexing.
	
	`my_ducklake.data` is virtual — it points to metadata_0.parquet.

### Index Tables

	Table	Columns	Purpose
	dict	termid, term, df	Term dictionary. df = number of documents containing the term.
	docs	docid, len	Document statistics (number of tokens per doc).
	postings	termid, docid, tf	Term–document frequency mapping.

### Indexing Flow

**The typical process is:**
1.	Reset & Import Raw Data
	The `metadata_0.parquet` is imported as `my_ducklake.data`.
	
	```
	from helper import reset_and_reindex
	reset_and_reindex(con)
	```
	
	This:
		•	Drops all DuckLake tables (dict, docs, postings, data)
		•	Imports the metadata_0.parquet
		•	Builds new index Parquet files (dict.parquet, docs.parquet, postings.parquet)
		•	Imports them back into DuckLake

2.	Run Tests

	`python test.py --mode tests`
	
	This executes a non-destructive validation:
		•	Inserts sample docs
		•	Modifies them
		•	Deletes them
		•	Confirms all counts (df, tf, len) are restored correctly
		•	Prints a truth table summarizing each subtest

3.	Query the Index
	Use BM25 ranking implemented in fts_tools.py:
	
	`python test.py --mode query --q "your search terms" --top 10 --show-content`
	
	Example output:
	
	Top 5 for query: 'artificial intelligence'
	 1. docid=42  score=4.832  |  'Artificial intelligence is a branch of computer science...'
	 2. docid=317 score=3.229  |  'AI applications are found in...'


4.	Inspect Sanity State

	`python test.py --mode sanity`
	
	Prints table schemas and top 2 rows of each index table for debugging.

# Tools Overview

`helper.py`
	•	Connects DuckLake and installs extensions.
	•	Contains tokenization (via spaCy).
	•	Exposes metric helpers (get_df, get_freq, get_dl, etc.).
	•	Implements reset_and_reindex() for a full rebuild cycle.

`indexing_tools.py`
	•	Builds inverted index (dict/docs/postings) in memory using Pandas.
	•	Writes 3 Parquet files: dict.parquet, docs.parquet, postings.parquet.
	•	Imports those files as DuckLake tables.

`update_tools.py`
	•	Handles dynamic updates (without relying on PK/FK or cascades).
	•	Functions:
	•	insert(con, doc, docid=None)
	•	modify(con, docid, new_content)
	•	delete(con, docid)
	•	Ensures df, tf, and document lengths stay consistent.

`fts_tools.py`
	•	Implements BM25 retrieval:
	•	idf() – inverse document frequency
	•	tf() – term weighting
	•	bm25_score() – computes document–query score
	•	match_bm25() – returns ranked top-n matches

`test.py`
	•	CLI driver for all operations:
	•	--mode tests → runs structured correctness tests (with truth table)
	•	--mode reindex → rebuilds index from Parquet
	•	--mode query → run BM25 query
	•	--mode sanity → inspect DuckLake structure

# Command Reference

Command	Description
`python test.py --mode tests`	Run insert/modify/delete integrity tests
`python test.py --mode reindex --parquet metadata_0.parquet --limit 5000`	Reset + rebuild index (limit optional)
`python test.py --mode query --q "machine learning" --top 5 --show-content`	Run BM25 ranking query
`python test.py --mode sanity	Print schema + sample rows for each table`

# Important Notes
	•	No primary/foreign keys — all consistency is handled manually.
	•	DuckLake tables are virtual. They map onto Parquet data files.
	•	All index manipulations are transactionally safe (BEGIN/COMMIT managed inside each tool).
	•	test.py tests are non-destructive — they insert, modify, and delete test docs, leaving the original state intact.

# Example Usage Flow

1. Full rebuild from base metadata
`python test.py --mode reindex --parquet metadata_0.parquet --limit 1000`

2. Verify structure
`python test.py --mode sanity`

3. Run query
`python test.py --mode query --q "artificial intelligence" --top 5 --show-content`

4. Run correctness tests
`python test.py --mode tests`
