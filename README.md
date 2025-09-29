# BachelorThesis
Using DuckLake's multi-table cataloging database architecture to perform selective search queries locally on server nodes.

# Problem formulation and motivation
For my bachelor thesis I plan to research the performance of selective searching on distributed data using DuckLake's new cataloging architecture which allows for a centralised method of cataloging metadata in a single multi-table database compared to current architectures which have a single-table catalog database which points to metadata files which then recursively link to numerous files such as a manifest list and manifest file before finally pointing to the data files. These meta-data files across the whole meta-layer often have various file types making it far more complex to query. The centralised approach of using a multi-table catalog database (DuckLake) means that all of the meta-can be stored across relational tables internally in the catalog database, meaning we only need to query a single database (using SQL statements) in order to access data files.

Using SQL based catalog transactions provides a simple and structured way of querying a data lake. We can make the catalog database with PostgreSQL or something like DuckDB meaning that we can have data warehouse features such as ACID transactions and Multi-Version Concurrency Control (MVCC) on a data lake using DuckLake.For many years we have optimised multi-table querying and very efficient and optimised solutions have already been implemented and used for years across many systems.

The centralised structure of DuckLake opens many opportunities to explore in the Information Retrieval area, and I wish to research the potential of server-side selective search where the client sends a SQL query to the data node and then that node locally performs the selective search instead of the client having to download index files and searching it themselves. I aim to research how this new architecture can be used to for Full Text Search (FTS) by using selective search locally on shards across the distributed nodes. This has the potential to improve on today's infrastructure and optimise Information retrieval by bringing the query to the data instead of bringing the data to the client who then searches limited results locally. I aim to analyse the performance metrics of this new method and compare this approach to standard methods that are being widely used currently.


# Proposed Research Questions
- How does a SQL-based transaction structure impact the performance of conducting a selective search with data stored on a single node?
-  How do SQL-based transaction structure impact the performance of conducting a selective search with data distributed across multiple nodes?
-  Does having a centralised multi-table metadata structure have an impact on the accuracy of selective search with data stored on a single node?
- Does having a centralised multi-table metadata structure have an impact on the accuracy of selective searches with data distributed across multiple nodes?
- Does using SQL statements to query data lakes open new opportunities for optimisation?

