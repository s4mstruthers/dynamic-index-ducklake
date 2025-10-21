# fts_tools.py
# BM25 query runners over DuckLake-backed index (dict/docs/postings).
# Provides conjunctive (AND) and disjunctive (OR) semantics.

from helper_functions import tokenize_query

# ---------------------------------------------------------------------
# BM25 (AND semantics)
# ---------------------------------------------------------------------
def conjunctive_bm25(con, query, top_n):
    """
    Rank only documents that contain ALL query terms (conjunctive/AND).

    Notes:
      - Uses a TEMP table for the current query's termids to keep SQL legible.
      - BM25 parameters (k1, b) are constants here to ensure reproducibility.
      - Drops the TEMP table on exit to avoid leaking state across queries.

    Returns:
      list[(docid:int, score:float)] ordered by descending score (<= top_n).
    """
    termids = tokenize_query(con, query)
    if not termids:
        return []

    # Maintain isolation across queries
    con.execute("DROP TABLE IF EXISTS query_terms")
    con.execute("CREATE TEMP TABLE query_terms(termid BIGINT)")
    con.executemany("INSERT INTO query_terms VALUES (?)", [(t,) for t in termids])

    K1 = 1.2
    B = 0.75

    rows = con.execute(
        """
        /* ---------------------------------------------------------------
           BM25(D,Q) with AND semantics:
           - Restrict to docs that contain ALL query terms (conjunctive).
           - Compute corpus stats once and reuse across scoring.
        ---------------------------------------------------------------- */

        WITH corpus_stats AS (
          SELECT
            COUNT(*)::DOUBLE AS N,
            AVG(d.len)::DOUBLE AS avgdl
          FROM my_ducklake.docs AS d
        ),

        term_hits AS (
          SELECT
            p.docid     AS docid,
            p.termid    AS termid,
            p.tf::DOUBLE AS tf,
            d.len::DOUBLE AS len
          FROM my_ducklake.postings AS p
          JOIN my_ducklake.docs AS d ON d.docid = p.docid
          WHERE p.termid IN (SELECT termid FROM query_terms)
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

# ---------------------------------------------------------------------
# BM25 (OR semantics)
# ---------------------------------------------------------------------
def disjunctive_bm25(con, query, top_n):
    """
    Rank documents that contain ANY query term (disjunctive/OR).

    Notes:
      - TEMP table holds current query termids for clarity and safety.
      - BM25 parameters (k1, b) fixed to avoid config drift across runs.

    Returns:
      list[(docid:int, score:float)] ordered by descending score (<= top_n).
    """
    termids = tokenize_query(con, query)
    if not termids:
        return []

    con.execute("DROP TABLE IF EXISTS query_terms")
    con.execute("CREATE TEMP TABLE query_terms(termid BIGINT)")
    con.executemany("INSERT INTO query_terms VALUES (?)", [(t,) for t in termids])

    K1 = 1.2
    B = 0.75

    rows = con.execute(
        """
        /* ---------------------------------------------------------------
           BM25(D,Q) with OR semantics:
           - Consider any doc containing at least one query term.
        ---------------------------------------------------------------- */

        WITH corpus_stats AS (
          SELECT
            COUNT(*)::DOUBLE AS N,
            AVG(d.len)::DOUBLE AS avgdl
          FROM my_ducklake.docs AS d
        ),

        term_hits AS (
          SELECT
            p.docid     AS docid,
            p.termid    AS termid,
            p.tf::DOUBLE AS tf,
            d.len::DOUBLE AS len
          FROM my_ducklake.postings AS p
          JOIN my_ducklake.docs AS d ON d.docid = p.docid
          WHERE p.termid IN (SELECT termid FROM query_terms)
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

# ---------------------------------------------------------------------
# Query Orchestration
# ---------------------------------------------------------------------
def run_bm25_query(con, query, top_n=10, show_content=False, qtype="disjunctive"):
    """
    Execute a BM25 query (conjunctive/disjunctive) and pretty-print results.

    Parameters:
      - con: DuckDB connection (already attached to `my_ducklake`).
      - query: raw string query (tokenized and mapped to termids).
      - top_n: max results to return.
      - show_content: include a short content snippet for each doc.
      - qtype: 'conjunctive' (AND) or 'disjunctive' (OR).

    Prints:
      - Ranking header
      - Per-result line: rank, docid, BM25 score, optional snippet
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

    # 1) gather ids
    docids = [docid for docid, _ in results]

    content_by_id = {}
    if show_content and docids:
        # 2) single batched fetch
        placeholders = ",".join(["?"] * len(docids))
        rows = con.execute(
            f"SELECT docid, content FROM my_ducklake.data WHERE docid IN ({placeholders})",
            docids,
        ).fetchall()
        # 3) map
        content_by_id = {docid: content for docid, content in rows}

    # 4) print with O(1) lookups
    for rank, (docid, score) in enumerate(results, 1):
        line = f"{rank:2d}. docid={docid}  score={score:.6f}"
        if show_content:
            content = content_by_id.get(docid)
            if content is not None:
                snippet = str(content)[:160].replace("\n", " ")
                line += f"  |  {snippet!r}"
        print(line)