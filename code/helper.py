from pathlib import Path
import spacy

#--------------------------------
#          PATHS
#--------------------------------
BASE_DIR = Path(__file__).resolve().parent
DUCKLAKE_FOLDER = BASE_DIR.parent / "ducklake"
DUCKLAKE_DATA = DUCKLAKE_FOLDER / "data_files"
DUCKLAKE_METADATA = DUCKLAKE_FOLDER / "metadata_catalog.ducklake"
TEST_FOLDER = BASE_DIR.parent / "test"
PARQUET_FOLDER = BASE_DIR.parent / "parquet"

#--------------------------------
#          LOAD NLP
#--------------------------------
nlp = spacy.load("en_core_web_sm")

#--------------------------------
#          DUCKLAKE
#--------------------------------
def connect_ducklake(con):
    sql = f"""
        INSTALL ducklake;
        LOAD ducklake;
        ATTACH 'ducklake:{DUCKLAKE_METADATA}' AS my_ducklake (DATA_PATH '{DUCKLAKE_DATA}');
        INSTALL fts;
        LOAD fts;
        -- Not using USE my_ducklake here as I want to work mostly locally first so dont want to change default schema
    """
    con.execute(sql)

#--------------------------------
#         SANITY CHECKS
#--------------------------------
def test_ducklake(con):
    # List all tables
    # tables = con.execute("SHOW ALL TABLES;").fetch_df()
    # print("Tables in database:\n", tables, "\n")

    # Dict
    describe = con.execute("DESCRIBE my_ducklake.dict;").fetch_df()
    print("Describe dict:\n", describe, "\n")
    top_dict = con.execute("SELECT * FROM my_ducklake.dict LIMIT 2;").fetch_df()
    print("Top 2 rows in dict:\n", top_dict, "\n")

    # Docs
    describe = con.execute("DESCRIBE my_ducklake.docs;").fetch_df()
    print("Describe docs:\n", describe, "\n")
    top_docs = con.execute("SELECT * FROM my_ducklake.docs LIMIT 2;").fetch_df()
    print("Top 2 rows in docs:\n", top_docs, "\n")

    # Postings
    describe = con.execute("DESCRIBE my_ducklake.postings;").fetch_df()
    print("Describe postings:\n", describe, "\n")
    top_post = con.execute("SELECT * FROM my_ducklake.postings LIMIT 2;").fetch_df()
    print("Top 2 rows in postings:\n", top_post, "\n")

#--------------------------------
#         READ DATA
#--------------------------------

# Lookup term t in dict and return the termid
def get_termid(con, t):
    termid_sql = f"""
    SELECT termid FROM my_ducklake.dict
    WHERE term = '{t}';
    """
    result = con.execute(termid_sql).fetchone()
    if result:
        termid = result[0]
        return termid
    else:
        return None

# Get the frequency of termid in docid in postings table
def get_freq(con, termid,docid):
    freq_sql = f"""
    SELECT tf FROM my_ducklake.postings
    WHERE termid = {termid} AND docid = {docid};
    """
    result = con.execute(freq_sql).fetchone()
    if result:
        freq = result[0]
        return freq
    else:
        return 0

# Get length of document
def get_dl(con, docid):
    freq_sql = f"""
    SELECT len FROM my_ducklake.docs
    WHERE docid = {docid};
    """
    result = con.execute(freq_sql).fetchone()
    if result:
        freq = result[0]
        return freq
    else:
        raise Exception(f"Error getting dl for docid = '{docid}'")

# Get the average length of document in corpus
def get_avgdl(con):
    avgdl_sql = """
    SELECT AVG(len) FROM my_ducklake.docs;
    """
    result = con.execute(avgdl_sql).fetchone()
    if result:
        avgdl = result[0]
        return avgdl
    else:
        raise Exception("Error getting avgdl")

# Get the number of documents in the corpus
def get_ndocs(con):
    ndocs_sql = """
    SELECT COUNT(*) FROM my_ducklake.docs;
    """
    result = con.execute(ndocs_sql).fetchone()
    if result:
        ndocs = result[0]
        return ndocs
    else:
        raise Exception("Error getting number of documents")

# Get the number of documents containing termid
def get_ndocs_t(con,termid):
    ndt_sql = f"""
    SELECT df FROM my_ducklake.dict WHERE termid = {termid};
    """
    result = con.execute(ndt_sql).fetchone()
    if result:
        ndt = result[0]
        return ndt
    else:
        raise Exception(f"Error getting number of documents containing termid = {termid}")
    
def get_docids(con):
    docids_sql = """
    SELECT docid FROM my_ducklake.docs;
    """
    result = con.execute(docids_sql).fetchall()
    if result:
        docids = [row[0] for row in result]
        return docids
    else:
        raise Exception("Error getting docids")

def get_content(con, docid):
    content_sql = f"""
    SELECT content FROM my_ducklake.data WHERE docid = {docid};
    """
    content = con.execute(content_sql).fetchone()
    if content:
        return content
    
#--------------------------------
#            TOOLS
#--------------------------------



# This will tokenise a string content
def tokenize(content):
    # Had to increase this to allow for large document to be tokenized
    nlp.max_length = 2_000_000
    # Tokenize content and lowercase alphabetic tokens
    tokens = [token.text.lower() for token in nlp(content) if token.is_alpha]
    return tokens

def tokenize_query(con, query):
    tokens = tokenize(query)

    # Get termids, exclude None
    termids = [tid for term in tokens if (tid := get_termid(con, term)) is not None]

    # Return just the list of valid termids
    return termids

# --------------------------------
#   RESET + REINDEX
# --------------------------------

def import_parquet(con, parquet="metadata_0.parquet"):
    parquet_full_path = BASE_DIR.parent / f"parquet/{parquet}"

    sql = f"""
        USE my_ducklake;

        DROP TABLE IF EXISTS main.data;

        CREATE TABLE main.data AS
        SELECT 
            CAST(docid AS BIGINT) AS docid,
            CAST(main_content AS TEXT) AS content
        FROM '{parquet_full_path}';
    """
    con.execute(sql)

def reset_and_reindex(con, parquet: str = "metadata_0.parquet", limit: int | None = None):
    """
    2) Drop my_ducklake.main.{postings, docs, dict, data}.
    3) Import the given parquet into my_ducklake.main.data as (docid, content).
    4) Build dict/docs/postings to Parquet from my_ducklake.data.
    5) Import those Parquets back into my_ducklake (no constraints).
    """

    # 2) clean slate
    con.execute("USE my_ducklake")
    con.execute("""
        DROP TABLE IF EXISTS main.postings;
        DROP TABLE IF EXISTS main.docs;
        DROP TABLE IF EXISTS main.dict;
        DROP TABLE IF EXISTS main.data;
    """)

    # 3) import raw data parquet -> my_ducklake.main.data (docid, content)
    import_parquet(con, parquet=parquet)

    # 4) build index to parquet from current data
    from indexing_tools import build_index_to_parquet_from_ducklake, import_index_parquets_into_ducklake
    build_index_to_parquet_from_ducklake(con, limit=limit)

    # 5) import dict/docs/postings parquets back into my_ducklake
    import_index_parquets_into_ducklake(con)