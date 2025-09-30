import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DUCKLAKE_FOLDER = BASE_DIR.parent / "ducklake"

DUCKLAKE_DATA = DUCKLAKE_FOLDER / "data_files"
DUCKLAKE_METADATA = DUCKLAKE_FOLDER / "metadata_catalog.ducklake"
parquet_file = BASE_DIR.parent / "parquet/metadata_0.parquet"

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
        SELECT * FROM '{parquet_file}';
        """
        con.execute(create_sql)
     
def cleanup():
    con.execute("""
                USE memory;
                DETACH my_ducklake;
            """)
    
def test_import():
    result = con.execute(f"""
                         SELECT * FROM my_ducklake.data LIMIT 2; 
                         """).fetch_df()
    print(result)

def main():
    connect_ducklake()
    cleanup()

if __name__ == "__main__":
    main()

