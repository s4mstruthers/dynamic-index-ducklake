# Implementing DuckDB FTS in DuckLake
## Working with duckLake
`https://medium.com/@pratushmaheshwari/getting-started-with-ducklake-a-lightweight-table-format-using-duckdb-b7a43b9bff1b`
DuckDB can interact with a PostgreSQL database using the postgres extension. -> aka we are using DuckDB to query but we are translating the DuckDB commands to work in postgres


Current idea is to connect to the duckDB locally then extract the data from ducklake using a dataframe then creating index in the local duckDB then searching the database then copying the duckDB to duckLake. Currently the connection isn't through ducklake but can be added after.

Next steps, make it do the fts with persistance

## How pragma builds a schema we can then use in query.
This PRAGMA builds the index under a newly created schema. The schema will be named after the input table: if an index is created on table 'main.table_name', then the schema will be named 'fts_main_table_name'.

# Using postgres for FTS
**tsv**:
A postgres type that stores a tokenized, normalized and weighted representation of text for full-text search.

**GIN**: generalized inverted index
This is a postgres index type which is optimized for set-membership lookups (inverted index, ie term1 -> doc1, doc3). This makes it great for tsvectore.
Slower writes compared to a binary tree but faster reads for FTS and supports concurrency

**plpgsql**:
Built-in language for writing server-side functions, procedures, and trigger bodies with variables (needed to create the trigger)