import math
import spacy
import duckdb
from pathlib import Path
from index_helper import test, create_temp_tables
nlp = spacy.load("en_core_web_sm")

# ------------- DuckLake initialisation -----------------------------
BASE_DIR = Path(__file__).resolve().parent
DUCKLAKE_FOLDER = BASE_DIR.parent / "ducklake"

DUCKLAKE_DATA = DUCKLAKE_FOLDER / "data_files"
DUCKLAKE_METADATA = DUCKLAKE_FOLDER / "metadata_catalog.ducklake"

con = duckdb.connect()

def connect_ducklake(con):
    connect_ducklake_sql = f"""
        INSTALL ducklake;
        ATTACH 'ducklake:{DUCKLAKE_METADATA}' AS my_ducklake (DATA_PATH '{DUCKLAKE_DATA}');
        USE my_ducklake;
        """
    con.execute(connect_ducklake_sql)

def cleanup():
    con.execute("""
                USE memory;
                DETACH my_ducklake;
            """)

# ----- Index implementation ---------------------
def get_data(con):
    data_sql = """
        SELECT docid, main_content FROM my_ducklake.data LIMIT 5000;
        """
    return con.execute(data_sql).fetch_df()

def tokenize(content):
    tokens = [token.text.lower() for token in nlp(content) if token.is_alpha]

def create_index():
    create_temp_tables(con)
    
# ------------- RUNTIME -------------------------------------------
def main():
    connect_ducklake(con)
    test(con)
    print("Complete")

if __name__ == "__main__":
    main()
