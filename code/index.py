import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DUCKLAKE_FOLDER = BASE_DIR.parent / "ducklake"
DUCKLAKE_DATA = DUCKLAKE_FOLDER / "data_files"
DUCKLAKE_METADATA = DUCKLAKE_FOLDER / "metadata_catalog.ducklake"
parquet_file_data = BASE_DIR.parent / "parquet/metadata_0.parquet"
parquet_file_dict = BASE_DIR.parent / "parquet/dict.parquet"
parquet_file_docs = BASE_DIR.parent / "parquet/docs.parquet"
parquet_file_postings = BASE_DIR.parent / "parquet/postings.parquet"

con = duckdb.connect()

def connect_ducklake():
    sql = f"""
        INSTALL ducklake;
        LOAD ducklake;
        ATTACH 'ducklake:{DUCKLAKE_METADATA}' AS my_ducklake (DATA_PATH '{DUCKLAKE_DATA}');
        INSTALL fts;
        LOAD fts;
        -- Not using USE my_ducklake here as I want to work mostly locally first so dont want to change default schema
    """
    con.execute(sql)

def import_data(limit=5000):
    sql = f"""
        CREATE OR REPLACE TEMP TABLE data AS
        SELECT docid, main_content
        FROM my_ducklake.main.data
        LIMIT {limit};
    """
    con.execute(sql)

def create_index():
    # Build FTS over the TEMP table `data`
    # Creates a new local schema called fts_main_data 
    # main is default for temp tables

    con.execute("""
        PRAGMA create_fts_index('data', 'docid', 'main_content', overwrite=1);
    """)

def test():
    # Show that objects exist; DESCRIBE the correct table (`data`)
    tables = con.execute("SHOW ALL TABLES;").fetch_df()
    print("Tables:\n", tables, "\n")
    describe = con.execute("DESCRIBE data;").fetch_df()
    print("Describe data:\n", describe)

def search(query):
    # Use the correct FTS schema (fts_main_data) and base table name (data)
    sql = f"""
        SELECT docid, main_content, score
        FROM (
            SELECT d.*, fts_main_data.match_bm25(d.docid, '{query}') AS score
            FROM data d
        ) s
        WHERE score IS NOT NULL
        ORDER BY score DESC
        LIMIT 10;
    """
    print(con.execute(sql).fetch_df())

def create_parquet():
    # ensure output folder exists
    (parquet_file_dict.parent).mkdir(parents=True, exist_ok=True)

    parquet_sql = f"""
        COPY (
            SELECT termid, docid, COUNT(*) AS tf
            FROM fts_main_data.terms
            GROUP BY termid, docid
            ORDER BY termid, docid
        ) TO '{parquet_file_postings}' (FORMAT PARQUET);

        COPY (SELECT * FROM fts_main_data.docs ORDER BY docid)
        TO '{parquet_file_docs}' (FORMAT PARQUET);

        COPY (SELECT * FROM fts_main_data.dict ORDER BY termid)
        TO '{parquet_file_dict}' (FORMAT PARQUET);
    """
    con.execute(parquet_sql)

def import_data_ducklake():
    sql = f"""
        USE my_ducklake;

        CREATE OR REPLACE TABLE main.dict AS
        SELECT * FROM '{parquet_file_dict}';

        CREATE OR REPLACE TABLE main.docs AS
        SELECT * FROM '{parquet_file_docs}';

        CREATE OR REPLACE TABLE main.postings AS
        SELECT * FROM '{parquet_file_postings}';
    """
    con.execute(sql)

def main():
    connect_ducklake()
    import_data()
    create_index()
    test()
    # search("birthday party")
    create_parquet()
    import_data_ducklake()
    # import_parquet_back_into_ducklake()
    print("Complete")

if __name__ == "__main__":
    main()