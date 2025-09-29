import duckdb
import pandas
con = duckdb.connect()

#Initialising ducklake
con.execute("""
INSTALL ducklake;
INSTALL postgres;
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost user=samstruthers password=jofkud-caTha5-jezmud' AS my_ducklake (DATA_PATH 'data_files/');
USE my_ducklake;
""")

#Importing the data from parquet into duckLake
con.execute("""
    CREATE TABLE IF NOT EXISTS my_ducklake.data AS
    FROM 'data_files/metadata_0.parquet';
            """)

result = con.execute("""
    SELECT * FROM my_ducklake.data LIMIT 0;
""").fetch_df()
print("import successful\n")
print(result)


con.execute("""
    USE memory;
    DETACH my_ducklake;
            """)
