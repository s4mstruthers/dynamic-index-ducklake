# Development so far
- first I researched into object storage / blob storage, and how S3 and other object storages work.
- I then looked into alternative approaches to working with a data lake and implementing a lakehouse such as Apache Iceberg so that I better understood the reasoning behind the choice to go to ducklake
- I looked into duckDB and how it works, and the pros and cons and then also looking into the ducklake release notes, podcasts and demos
- I have then experimented with the duckDB fts extension locally to see how it worked and see if there was any potential to adapt this to work on ducklake.
- Then setup ducklake with the metadata.parquet files and importing the parquet files into ducklake to test how to work with ducklake.
- I experimented with using a postgres catalog for the metadata but found that this became overly complex and difficult to work with so then restructured the development setup to use a duckDB database to hold the metadata catalog as this proved to be a lot simpler to workwith and cleaner.

# Next steps
- I plan to run the duckDB fts extension separately in a local instance to create an index for metadata_0.parquet file which I then plan to use for developing a custom python implementation of BM25 which will run on ducklake where the docs, postings and dict are all stored in object storage parquet files.