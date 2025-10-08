import math
import spacy
import heapq
import duckdb
from pathlib import Path
from bm25_helper import get_termid, get_freq, get_dl, get_avgdl, get_docids, get_ndocs, get_ndocs_t, docid_to_content

nlp = spacy.load("en_core_web_sm")

# ------------- DuckLake initialisation -----------------------------
BASE_DIR = Path(__file__).resolve().parent
DUCKLAKE_FOLDER = BASE_DIR.parent / "ducklake"

DUCKLAKE_DATA = DUCKLAKE_FOLDER / "data_files"
DUCKLAKE_METADATA = DUCKLAKE_FOLDER / "metadata_catalog.ducklake"

con = duckdb.connect()

def connect_ducklake():
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

#----------------- BM25 Implementation ----------------------------

# Term frequency factor -> measures how often a query term appears in a document
def tf(termid,docid):
    freq = get_freq(con,termid,docid)
    dl = get_dl(con, docid)
    avgdl = get_avgdl(con)
    K_1 = 1.2
    B = 0.75
    return freq / (freq + K_1*(1-B+B*(dl/avgdl) ) )

# Inverse Document Frequency -> measuring importance of term across entire corpus
def idf(termid):
    N = get_ndocs(con)
    N_t = get_ndocs_t(con,termid)
    return math.log((N - N_t + 0.5)/(N_t +0.5))

def tokenize_query(query):
    # Iterates over every token in the processed document and converts to lowercase
    # Change to token.lemma_.lower() to convert tokens to base form (lemmas)
    tokens = [token.text.lower() for token in nlp(query) if token.is_alpha]

    #tid uses (:=) which lets you assign a value from inside an expression, which means that I can assign only not None value to the array
    terms = [tid for t in tokens if (tid := get_termid(con, t)) is not None]
    return terms

# Given an array of terms in query and docid -> sum up the contributions of all query terms in document -> this is the bm25 scoring formula
def bm25_score(terms,docid):
    score = 0
    for termid in terms:
        score += idf(termid)*tf(termid,docid)
    return score

# This will run bm25 algorithm for a query and return the top n results
def match_bm25(query,top_n):
    terms = tokenize_query(query)
    docids = get_docids(con)
    scores = []
    for docid in docids:
        scores.append((docid,bm25_score(terms,docid)))

    # sort scores in descending order by bm25_score and grab top_n, using heapq.nlargest as it will find top n efficiently without sorting the full array
    # uses min-heap, where it will initialise a min-heap and then for each element compare it to smallest element in the heap and then remove smallest and insert the new one
    top_n = heapq.nlargest(10, scores, key=lambda x: x[1])

    return top_n
#------------------------------------------------------------------

# ------------- RUNTIME -------------------------------------------
def main():
    connect_ducklake()
    query = "legacies of faith"
    top_5 = match_bm25(query,5)
    for i in top_5:
        print(f"""
              Docid:
              {i[0]}

              Score:
              {i[1]}

              Content:
              {docid_to_content(con,i[0])}

              """)

    print("Complete")

if __name__ == "__main__":
    main()