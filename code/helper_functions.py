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
    """
    Attach the DuckLake catalog as `my_ducklake` and load required extensions.

    - INSTALL/LOAD ducklake & fts.
    - ATTACH the DuckLake catalog at DUCKLAKE_METADATA with DATA_PATH DUCKLAKE_DATA.
    - Does not switch default schema globally (no `USE` here on purpose).
    """
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
    """
    Print schema (DESCRIBE) and top-2 rows for each index table in `my_ducklake`.

    Tables shown:
      - my_ducklake.dict
      - my_ducklake.docs
      - my_ducklake.postings
    """
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
def get_termid(con, t):
    """
    Return `termid` for a given term string `t` from `my_ducklake.dict`.
    If the term does not exist, return None.
    """
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

def get_freq(con, termid, docid):
    """
    Return term frequency (tf) for (termid, docid) from `my_ducklake.postings`.
    If no row exists, return 0.
    """
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

def get_dl(con, docid):
    """
    Return document length (`len`) for `docid` from `my_ducklake.docs`.
    Raises if the docid is missing.
    """
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

def get_avgdl(con):
    """
    Return the average document length across all rows in `my_ducklake.docs`.
    Raises if query unexpectedly returns no value.
    """
    avgdl_sql = """
    SELECT AVG(len) FROM my_ducklake.docs;
    """
    result = con.execute(avgdl_sql).fetchone()
    if result:
        avgdl = result[0]
        return avgdl
    else:
        raise Exception("Error getting avgdl")

def get_ndocs(con):
    """
    Return the number of documents (row count) from `my_ducklake.docs`.
    """
    ndocs_sql = """
    SELECT COUNT(*) FROM my_ducklake.docs;
    """
    result = con.execute(ndocs_sql).fetchone()
    if result:
        ndocs = result[0]
        return ndocs
    else:
        raise Exception("Error getting number of documents")

def get_ndocs_t(con, termid):
    """
    Return document frequency `df` for a given `termid` from `my_ducklake.dict`.
    This is the number of distinct documents containing that term.
    """
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
    """
    Return a Python list of all `docid` values from `my_ducklake.docs`.
    Raises on unexpected empty fetch.
    """
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
    """
    Return the content string for `docid` from `my_ducklake.data`.
    Returns None if the row is missing.
    """
    content_sql = f"""
    SELECT content FROM my_ducklake.data WHERE docid = {docid};
    """
    content = con.execute(content_sql).fetchone()
    if content:
        return content

#--------------------------------
#            TOOLS
#--------------------------------
def tokenize(content):
    """
    Tokenize a text `content` using spaCy:
      - Increases `nlp.max_length` to allow large documents.
      - Returns a list of lowercase alphabetic tokens (no digits/punct).
    """
    # Had to increase this to allow for large document to be tokenized
    nlp.max_length = 2_000_000
    # Tokenize content and lowercase alphabetic tokens
    tokens = [token.text.lower() for token in nlp(content) if token.is_alpha]
    return tokens

def tokenize_query(con, query):
    """
    Tokenize a query string and map tokens to existing termids:
      - Uses `tokenize(query)` to produce tokens.
      - Looks up each token in `my_ducklake.dict` via `get_termid`.
      - Returns a list of termids (None filtered out).
    """
    tokens = tokenize(query)

    # Get termids, exclude None
    termids = [tid for term in tokens if (tid := get_termid(con, term)) is not None]

    # Return just the list of valid termids
    return termids

# --------------------------------
#   RESET + REINDEX
# --------------------------------
def import_parquet(con, parquet="metadata_0.parquet"):
    """
    Load a source parquet into `my_ducklake.main.data` as a managed table:
      - Drops `main.data` if it exists.
      - Creates `main.data` with columns (docid BIGINT, content TEXT).
      - Reads from `<repo_root>/parquet/{parquet}` selecting docid/main_content.
    Note: this creates a table (not a view) and will write fragments under DuckLake.
    """
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
    Full rebuild:
      1) Drop `my_ducklake.main.{postings, docs, dict, data}` if present.
      2) Import the given parquet into `my_ducklake.main.data` as (docid, content).
      3) Build dict/docs/postings to Parquet from `my_ducklake.data` (Python indexer).
      4) Import those Parquets back into DuckLake (no constraints).
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
    from index_tools import build_index_to_parquet_from_ducklake, import_index_parquets_into_ducklake

    # 4) build index to parquet from current data
    build_index_to_parquet_from_ducklake(con, limit=limit)

    # 5) import dict/docs/postings parquets back into my_ducklake
    import_index_parquets_into_ducklake(con)