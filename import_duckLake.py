import duckdb
import pandas
import re
con = duckdb.connect()

def connect_ducklake():
    #Initialising ducklake connection
    dbname = "ducklake_catalog"
    host = "localhost"
    user = "samstruthers"
    password = "jofkud-caTha5-jezmud"

    connect_ducklake_sql = f"""
    INSTALL ducklake;
    INSTALL postgres;
    ATTACH 'ducklake:postgres:dbname={dbname} host={host} user={user} password={password} AS my_ducklake (DATA_PATH 'data_files/');
    USE my_ducklake;
    """

    con.execute(connect_ducklake_sql)

def import_data(path,n):
    
        #SQL query to create a virtual table for the imported parquet in ducklake catalog
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS my_ducklake.data{n} AS
        SELECT * FROM '{path}';
        """
        #Will only execute if the path is in the expected format
        con.execute(create_sql)

def cleanup():
    #This will clean up the connection to ducklake making sure it has been detached properly
    con.execute("""
                USE memory;
                DETACH my_ducklake;
                """)
    
def test_import(n):
    #This will test the table was created successfuly by fetching the header names and printing them
    result = con.execute(f"""
                         SELECT * FROM my_ducklake.data{n} LIMIT 0; 
                         """).fetch_df()
    return result

def main():
    connect_ducklake()

    path = input("Enter path to parquet: ")

    #This will extract the number associated to the parquet so that we can make a new table for it
    match = re.search(r"metadata_(\d+)\.parquet", path)
    if match:
        n = int(match.group(1))
        import_data(path,n)
        print(test_import(n))
    
    cleanup()

if __name__ == "__main__":
    main()






