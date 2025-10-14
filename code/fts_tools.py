# fts_tools.py
from __future__ import annotations

import math
import heapq

# Only import light helpers; these do not import fts_tools.
from helper_functions import (
    get_freq, get_dl, get_avgdl, get_docids, get_ndocs, get_ndocs_t, tokenize_query
)

# ----------------------- BM25 core --------------------------------

def tf(con, termid: int, docid: int) -> float:
    """
    BM25 term-frequency component for (termid, docid).
    Uses standard parameters K1=1.2, B=0.75 and:
        tf = f / (f + K1 * (1 - B + B * (dl / avgdl)))
    where:
        f    = raw term frequency (postings.tf)
        dl   = document length (docs.len)
        avgdl= average document length over corpus
    """
    freq = get_freq(con, termid, docid)
    dl = get_dl(con, docid)
    avgdl = get_avgdl(con)
    K1, B = 1.2, 0.75
    return 0.0 if dl == 0 or avgdl is None or avgdl == 0 else freq / (freq + K1 * (1 - B + B * (dl / avgdl)))


def idf(con, termid: int) -> float:
    """
    BM25 inverse document frequency:
        idf = ln( (N - N_t + 0.5) / (N_t + 0.5) )
    where:
        N   = total number of documents
        N_t = number of documents containing termid (dict.df)
    """
    N = get_ndocs(con)
    N_t = get_ndocs_t(con, termid)
    # guard: avoid log of non-positive (can happen with tiny corpora)
    numerator = (N - N_t + 0.5)
    denominator = (N_t + 0.5)
    if denominator <= 0 or numerator <= 0:
        return 0.0
    return math.log(numerator / denominator)


def bm25_score(con, terms: list[int], docid: int) -> float:
    """
    BM25 score for a document given a list of query termids.
    """
    return sum(idf(con, t) * tf(con, t, docid) for t in terms)


def match_bm25(con, query: str, top_n: int):
    """
    Rank documents by BM25 for a raw query string.

    Steps:
      1) tokenize_query(con, query) -> list of termids (unknown terms dropped)
      2) iterate all docids; compute BM25 score per doc
      3) return top_n as [(docid, score)] in descending score

    NOTE: This is a simple full-scan ranker for testing/validation.
    """
    terms = tokenize_query(con, query)
    if not terms:
        return []

    docids = get_docids(con)
    scores = [(docid, bm25_score(con, terms, docid)) for docid in docids]
    return heapq.nlargest(top_n, scores, key=lambda x: x[1])