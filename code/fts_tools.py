# fts_tools.py

from helper_functions import tokenize_query


def conjunctive_bm25(con, query, top_n):
    """
    BM25 (AND semantics): only documents that contain ALL query terms are scored/returned.
    Returns: list[(docid, score)] ordered by score desc, length <= top_n.
    """
    termids = tokenize_query(con, query)
    if not termids:
        return []

    # Temporary query term table
    con.execute("DROP TABLE IF EXISTS query_terms")
    con.execute("CREATE TEMP TABLE query_terms(termid BIGINT)")
    con.executemany("INSERT INTO query_terms VALUES (?)", [(t,) for t in termids])

    K1 = 1.2
    B = 0.75

    rows = con.execute(
        """
        /* ----------------------------------------------------------------------
           Compute BM25 scores for documents that contain ALL query terms.
           BM25(D,Q) = SUM_over_t_in_Q [ idf(t) * tf_norm(t,D) ]
        ---------------------------------------------------------------------- */

        WITH corpus_stats AS (
          SELECT
            COUNT(*)::DOUBLE AS N,
            AVG(docs.len)::DOUBLE AS avgdl
          FROM my_ducklake.docs AS docs
        ),

        term_hits AS (
          SELECT
            postings.docid AS docid,
            postings.termid AS termid,
            postings.tf::DOUBLE AS tf,
            docs.len::DOUBLE AS len
          FROM my_ducklake.postings AS postings
          JOIN my_ducklake.docs AS docs ON docs.docid = postings.docid
          WHERE postings.termid IN (SELECT termid FROM query_terms)
        ),

        conjunctive_documents AS (
          SELECT docid
          FROM term_hits
          GROUP BY docid
          HAVING COUNT(DISTINCT termid) = (SELECT COUNT(*) FROM query_terms)
        ),

        idf_table AS (
          SELECT
            dict.termid AS termid,
            ln((stats.N - dict.df + 0.5) / (dict.df + 0.5)) AS idf
          FROM my_ducklake.dict AS dict, corpus_stats AS stats
          WHERE dict.termid IN (SELECT termid FROM query_terms)
        ),

        scored AS (
          SELECT
            th.docid AS docid,
            SUM(
              idf_table.idf *
              ((? + 1) * th.tf) /
              (th.tf + ? * (1 - ? + ? * (th.len / stats.avgdl)))
            ) AS score
          FROM term_hits AS th
          JOIN conjunctive_documents AS cd ON cd.docid = th.docid
          JOIN idf_table ON idf_table.termid = th.termid
          CROSS JOIN corpus_stats AS stats
          GROUP BY th.docid
        )

        SELECT docid, score
        FROM scored
        ORDER BY score DESC
        LIMIT ?
        """,
        [K1, K1, B, B, top_n],
    ).fetchall()

    con.execute("DROP TABLE IF EXISTS query_terms")
    return [(row[0], float(row[1])) for row in rows]


def disjunctive_bm25(con, query, top_n):
    """
    BM25 (OR semantics): documents that contain ANY query term are scored/returned.
    Returns: list[(docid, score)] ordered by score desc, length <= top_n.
    """
    termids = tokenize_query(con, query)
    if not termids:
        return []

    # Temporary query term table
    con.execute("DROP TABLE IF EXISTS query_terms")
    con.execute("CREATE TEMP TABLE query_terms(termid BIGINT)")
    con.executemany("INSERT INTO query_terms VALUES (?)", [(t,) for t in termids])

    K1 = 1.2
    B = 0.75

    rows = con.execute(
        """
        /* ----------------------------------------------------------------------
           Compute BM25 scores for documents that contain ANY query term.
           BM25(D,Q) = SUM_over_t_in_Q [ idf(t) * tf_norm(t,D) ]
        ---------------------------------------------------------------------- */

        WITH corpus_stats AS (
          SELECT
            COUNT(*)::DOUBLE AS N,
            AVG(docs.len)::DOUBLE AS avgdl
          FROM my_ducklake.docs AS docs
        ),

        term_hits AS (
          SELECT
            postings.docid AS docid,
            postings.termid AS termid,
            postings.tf::DOUBLE AS tf,
            docs.len::DOUBLE AS len
          FROM my_ducklake.postings AS postings
          JOIN my_ducklake.docs AS docs ON docs.docid = postings.docid
          WHERE postings.termid IN (SELECT termid FROM query_terms)
        ),

        idf_table AS (
          SELECT
            dict.termid AS termid,
            ln((stats.N - dict.df + 0.5) / (dict.df + 0.5)) AS idf
          FROM my_ducklake.dict AS dict, corpus_stats AS stats
          WHERE dict.termid IN (SELECT termid FROM query_terms)
        ),

        scored AS (
          SELECT
            th.docid AS docid,
            SUM(
              idf_table.idf *
              ((? + 1) * th.tf) /
              (th.tf + ? * (1 - ? + ? * (th.len / stats.avgdl)))
            ) AS score
          FROM term_hits AS th
          JOIN idf_table ON idf_table.termid = th.termid
          CROSS JOIN corpus_stats AS stats
          GROUP BY th.docid
        )

        SELECT docid, score
        FROM scored
        ORDER BY score DESC
        LIMIT ?
        """,
        [K1, K1, B, B, top_n],
    ).fetchall()

    con.execute("DROP TABLE IF EXISTS query_terms")
    return [(row[0], float(row[1])) for row in rows]


# -----------------------
# Query helpers (BM25)
# -----------------------
def run_bm25_query(con, query, top_n=10, show_content=False, qtype="disjunctive"):
    """
    Execute a BM25 query (conjunctive/disjunctive) and print a ranked list.
    Always shows raw BM25 scores.
    """
    if qtype == "conjunctive":
        from fts_tools import conjunctive_bm25 as bm25_runner
    else:
        from fts_tools import disjunctive_bm25 as bm25_runner

    results = bm25_runner(con, query, top_n)
    if not results:
        print("No results.")
        return

    print(f"Top {len(results)} for {qtype} BM25 query: {query!r} (raw BM25 scores)")
    for rank, (docid, score) in enumerate(results, 1):
        line = f"{rank:2d}. docid={docid}  score={score:.6f}"
        if show_content:
            row = con.execute(
                "SELECT content FROM my_ducklake.data WHERE docid = ?", (docid,)
            ).fetchone()
            if row and row[0] is not None:
                snippet = str(row[0])[:160].replace("\n", " ")
                line += f"  |  {snippet!r}"
        print(line)