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
    connect_ducklake_sql = f"""
        INSTALL ducklake;
        ATTACH 'ducklake:{DUCKLAKE_METADATA}' AS my_ducklake (DATA_PATH '{DUCKLAKE_DATA}');
        USE my_ducklake;
        """
    con.execute(connect_ducklake_sql)

def import_data():
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS my_ducklake.data AS
        SELECT * FROM '{parquet_file_data}';
        CREATE TABLE IF NOT EXISTS my_ducklake.dict AS
        SELECT * FROM '{parquet_file_dict}';
        CREATE TABLE IF NOT EXISTS my_ducklake.docs AS
        SELECT * FROM '{parquet_file_docs}';
        CREATE TABLE IF NOT EXISTS my_ducklake.postings AS
        SELECT * FROM '{parquet_file_postings}';
        """
        con.execute(create_sql)
    
def test_import():
    result1 = con.execute(f"""
                         SHOW ALL TABLES; 
                         """).fetch_df()
    print("\nTables in ducklake: \n", result1)
    result2 = con.execute(f"""
                         SELECT * FROM my_ducklake.data LIMIT 2; 
                         """).fetch_df()
    print("\ndata sample: \n", result2)

    result3 = con.execute(f"""
                         SELECT * FROM my_ducklake.dict LIMIT 100; 
                         """).fetch_df()
    print("\ndict sample: \n", result3)
    result4 = con.execute(f"""
                         SELECT * FROM my_ducklake.docs LIMIT 2; 
                         """).fetch_df()
    print("\ndocs sample: \n", result4)
    result5 = con.execute(f"""
                         SELECT * FROM my_ducklake.postings LIMIT 2; 
                         """).fetch_df()
    print("\npostings sample: \n", result5)


def cleanup():
    con.execute("""
                USE memory;
                DETACH my_ducklake;
            """)
    
def main():
    connect_ducklake()
    #import_data()
    test_import()
    cleanup()

if __name__ == "__main__":
    main()

