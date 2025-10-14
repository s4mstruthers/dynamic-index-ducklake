# Development so far
- first I researched into object storage / blob storage, and how S3 and other object storages work.
- I then looked into alternative approaches to working with a data lake and implementing a lakehouse such as Apache Iceberg so that I better understood the reasoning behind the choice to go to ducklake
- I looked into duckDB and how it works, and the pros and cons and then also looking into the ducklake release notes, podcasts and demos
- I have then experimented with the duckDB fts extension locally to see how it worked and see if there was any potential to adapt this to work on ducklake.
- Then setup ducklake with the metadata.parquet files and importing the parquet files into ducklake to test how to work with ducklake.
- I experimented with using a postgres catalog for the metadata but found that this became overly complex and difficult to work with so then restructured the development setup to use a duckDB database to hold the metadata catalog as this proved to be a lot simpler to workwith and cleaner.
- I plan to run the duckDB fts extension separately in a local instance to create an index for metadata_0.parquet file which I then plan to use for developing a custom python implementation of BM25 which will run on ducklake where the docs, postings and dict are all stored in object storage parquet files.

# Dynamic Update ideas
- Annotative indexing isn't really ideal because it is essentially creating its own indexing structure that better allows for dynamic updates and ACID transactions etc
- I believe that alot of the limitations of doing dynamic index like buttcher outlined will be bypassed with the lakehouse format of ducklake.
- Ducklake allows you to interact with the index as if its a normal DB and interact with the documents as if they are tables, and then provides a centralised way to do updates and push changes etc.
- I believe that implementing features of dynamic updates on ducklake will allow for multiple transactions due to the conflict management and concurrent user issue already being result by ducklake
- By finding a way to reverse engineer the indexing so that we can remove content it will be very useful or potentially developing an indexing algorithm that we can run dynamically on a single document then cascade the changes into the overall index.
- I should implement the functionality to INSERT, DELETE, MODIFY
- INSERT: I could run indexing on a single document and then push changes into the index and update the statistics
- DELETE: I could delete all references containing that docid and then index the document needing deleted to then decrease statistics by that amount in the index, meaning that any reference to the document will be removed
- MODIFY: I could  run a DELETE and then INSERT essentially deleting the old version and updating the index with new one, as delete will adjust the statistics this should work fine.
- REMERGE approach: treats document modifications like a DELETE followed by an INSERT
- non-incremental dynamic text collections index policy: allows full range of update operations (insertstions, deletions and modifications). Lazy deletion procedure does not remove obselete postings from the index immediately but instead merely marks them as deleted.
	- I believe that this is similar to how ducklake works where changes aren't pushed immediately but instead stored as transactions in metadata waiting for a push.

- Create an indexing algorithm that will create the same index as what currently have
- Create the DELETE, INSERT, and MODIFY tools 

# Next steps
- cleaning up code removing unnecessary / overcomplicated stuff and make sure I understand everything by adding comments
- Create a pseudocode for the fts_tools to ensure mathematically I know what is going on.
- make the 3 speachmark descriptions explaining the logic for every function