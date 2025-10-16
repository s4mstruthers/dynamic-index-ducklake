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
    B  = 0.75

    rows = con.execute(
        """
        /* ----------------------------------------------------------------------
           Compute BM25 scores for documents that contain ALL query terms.
           BM25(D,Q) = SUM_over_t_in_Q [ idf(t) * tf_norm(t,D) ]
        ---------------------------------------------------------------------- */

        WITH corpus_stats AS (
          -- Compute N (number of documents) and avgdl (average document length)
          SELECT
            COUNT(*)::DOUBLE  AS N,
            AVG(docs.len)::DOUBLE AS avgdl
          FROM my_ducklake.docs AS docs
        ),

        term_hits AS (
          -- Candidate term occurrences restricted to query terms
          SELECT
            postings.docid         AS docid,
            postings.termid        AS termid,
            postings.tf::DOUBLE    AS tf,     -- f_(t,D)
            docs.len::DOUBLE       AS len     -- dl_D
          FROM my_ducklake.postings AS postings
          JOIN my_ducklake.docs AS docs ON docs.docid = postings.docid
          WHERE postings.termid IN (SELECT termid FROM query_terms)
        ),

        conjunctive_documents AS (
          -- Keep only documents that contain ALL query terms
          SELECT docid
          FROM term_hits
          GROUP BY docid
          HAVING COUNT(DISTINCT termid) = (SELECT COUNT(*) FROM query_terms)
        ),

        idf_table AS (
          -- Compute idf(t) = ln( (N - df_t + 0.5) / (df_t + 0.5) )
          SELECT
            dict.termid AS termid,
            ln( (stats.N - dict.df + 0.5) / (dict.df + 0.5) ) AS idf
          FROM my_ducklake.dict AS dict, corpus_stats AS stats
          WHERE dict.termid IN (SELECT termid FROM query_terms)
        ),

        scored AS (
          -- For each (doc, term) pair that appears in the doc,
          -- compute subscore = idf(t) * ((K1 + 1) * f_t,D) / (f_t,D + K1 * (1 - B + B * (dl_D / avgdl)))
          SELECT
            th.docid AS docid,
            SUM(
              idf_table.idf *
              ( (? + 1) * th.tf ) /
              ( th.tf + ? * (1 - ? + ? * (th.len / stats.avgdl)) )
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
    B  = 0.75

    rows = con.execute(
        """
        /* ----------------------------------------------------------------------
           Compute BM25 scores for documents that contain ANY query term.
           BM25(D,Q) = SUM_over_t_in_Q [ idf(t) * tf_norm(t,D) ]
        ---------------------------------------------------------------------- */

        WITH corpus_stats AS (
          -- Compute N (number of documents) and avgdl (average document length)
          SELECT
            COUNT(*)::DOUBLE  AS N,
            AVG(docs.len)::DOUBLE AS avgdl
          FROM my_ducklake.docs AS docs
        ),

        term_hits AS (
          -- Candidate term occurrences restricted to query terms
          SELECT
            postings.docid         AS docid,
            postings.termid        AS termid,
            postings.tf::DOUBLE    AS tf,     -- (f_t,D)
            docs.len::DOUBLE       AS len     -- (dl_D)
          FROM my_ducklake.postings AS postings
          JOIN my_ducklake.docs AS docs ON docs.docid = postings.docid
          WHERE postings.termid IN (SELECT termid FROM query_terms)
        ),

        idf_table AS (
          -- Compute idf(t) = ln( (N - df_t + 0.5) / (df_t + 0.5) )
          SELECT
            dict.termid AS termid,
            ln( (stats.N - dict.df + 0.5) / (dict.df + 0.5) ) AS idf
          FROM my_ducklake.dict AS dict, corpus_stats AS stats
          WHERE dict.termid IN (SELECT termid FROM query_terms)
        ),

        scored AS (
          -- For each (doc, term) that matches, compute and sum BM25 subscores
          SELECT
            th.docid AS docid,
            SUM(
              idf_table.idf *
              ( (? + 1) * th.tf ) /
              ( th.tf + ? * (1 - ? + ? * (th.len / stats.avgdl)) )
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