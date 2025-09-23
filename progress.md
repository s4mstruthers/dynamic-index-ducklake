# Implementing DuckDB FTS in DuckLake
## Working with duckLake
`https://medium.com/@pratushmaheshwari/getting-started-with-ducklake-a-lightweight-table-format-using-duckdb-b7a43b9bff1b`
DuckDB can interact with a PostgreSQL database using the postgres extension. -> aka we are using DuckDB to query but we are translating the DuckDB commands to work in postgres


Current idea is to connect to the duckDB locally then extract the data from ducklake using a dataframe then creating index in the local duckDB then searching the database then copying the duckDB to duckLake. Currently the connection isn't through ducklake but can be added after.

Next steps, make it do the fts with persistance
