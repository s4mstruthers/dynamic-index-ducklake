import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DUCKLAKE_FOLDER = BASE_DIR.parent / "ducklake"

DUCKLAKE_DATA = DUCKLAKE_FOLDER / "data_files"
parquet_file = BASE_DIR.parent / "parquet/metadata_0.parquet"
INDEX_DB = BASE_DIR.parent / "local_index/duck_fts.duckdb"
con = duckdb.connect(INDEX_DB)

def connect_duckdb():
    connect_duckdb_sql = f"""
        INSTALL fts;
        LOAD fts;
        """
    con.execute(connect_duckdb_sql)

def import_data():
        # Take only top 5000
        import_sql = f"""
                    CREATE TABLE IF NOT EXISTS documents AS
                    SELECT docid, main_content
                    FROM '{parquet_file}'
                    LIMIT 5000;
                    """
        con.execute(import_sql)

def test():
     tables = con.execute("""
                         SHOW ALL TABLES;
                         """).fetch_df()
     print("Tables in database: \n",tables)
     print("\n")
     describe = con.execute(""" 
                        DESCRIBE documents;
                        """).fetch_df()
     print("Describe documents: \n",describe)

def create_index():
    index_sql = """
        PRAGMA create_fts_index(
            'main.documents', 'docid', 'main_content',overwrite = 1
        );
        """
    con.execute(index_sql)

def alter():
    alter_sql = """
        PRAGMA drop_fts_index(documents);
        DROP TABLE documents;
        """
    count = """
        SELECT COUNT(*) FROM documents;
        """
    #con.execute(alter_sql)
    print(con.execute(count).fetch_df())

def create_parquet():
    parquet_sql = """
        COPY (SELECT termid, docid, COUNT(*) AS tf FROM fts_main_documents.terms GROUP BY ALL ORDER BY ALL) TO 'postings.parquet';
        COPY (FROM fts_main_documents.docs ORDER BY docid) TO 'docs.parquet';
        COPY (FROM fts_main_documents.dict ORDER BY termid) TO 'dict.parquet';
    """
    con.execute(parquet_sql)

def search(query):
     search_sql = f"""
            SELECT docid, main_content, score
            FROM (
                SELECT *, fts_main_documents.match_bm25(
                    docid,
                    '{query}'
                ) AS score
                FROM documents
            ) sq
            WHERE score IS NOT NULL
            ORDER BY score DESC
            LIMIT 10;
        """
     print(con.execute(search_sql).fetch_df())


def main():
    connect_duckdb()
    #import_data()
    #create_index()
    test()
    #alter()
    #search("birthday party")
    #create_parquet()
    print("Complete")


if __name__ == "__main__":
    main()
