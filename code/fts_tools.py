# fts_tools.py
import math
import heapq
from helper import get_freq, get_dl, get_avgdl, get_docids, get_ndocs, get_ndocs_t, tokenize_query

def tf(con, termid, docid):
    freq = get_freq(con, termid, docid)
    dl = get_dl(con, docid)
    avgdl = get_avgdl(con)
    K_1, B = 1.2, 0.75
    return freq / (freq + K_1 * (1 - B + B * (dl / avgdl)))

def idf(con, termid):
    N = get_ndocs(con)
    N_t = get_ndocs_t(con, termid)
    return math.log((N - N_t + 0.5) / (N_t + 0.5))

def bm25_score(con, terms, docid):
    return sum(idf(con, t) * tf(con, t, docid) for t in terms)

def match_bm25(con, query, top_n):
    terms = tokenize_query(con, query)
    docids = get_docids(con)
    scores = [(docid, bm25_score(con, terms, docid)) for docid in docids]
    return heapq.nlargest(top_n, scores, key=lambda x: x[1])