from helper import tokenize, get_termid
from collections import Counter

def _next_id(con, table: str, col: str) -> int:
    return int(con.execute(f"SELECT COALESCE(MAX({col}) + 1, 1) FROM my_ducklake.{table}").fetchone()[0])

def _exists(con, table: str, col: str, val) -> bool:
    return bool(con.execute(f"SELECT 1 FROM my_ducklake.{table} WHERE {col} = ? LIMIT 1", (val,)).fetchone())

def delete(con, docid: int):
    con.execute("BEGIN")
    try:
        # 1) decrement df once per distinct term in this doc
        con.execute("""
            UPDATE my_ducklake.dict
            SET df = CASE WHEN df > 0 THEN df - 1 ELSE 0 END
            WHERE termid IN (
                SELECT DISTINCT termid
                FROM my_ducklake.postings
                WHERE docid = ?
            )
        """, (docid,))

        # 2) remove dict rows that hit 0 (only the ones touched)
        con.execute("""
            DELETE FROM my_ducklake.dict
            WHERE df = 0
              AND termid IN (
                  SELECT DISTINCT termid
                  FROM my_ducklake.postings
                  WHERE docid = ?
              )
        """, (docid,))

        # 3) explicit cleanup (no cascades)
        con.execute("DELETE FROM my_ducklake.postings WHERE docid = ?", (docid,))
        con.execute("DELETE FROM my_ducklake.docs      WHERE docid = ?", (docid,))
        con.execute("DELETE FROM my_ducklake.data      WHERE docid = ?", (docid,))

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

def insert(con, doc: str, docid: int | None = None):
    tokens = tokenize(doc)
    if not tokens:
        return None

    tf = Counter(tokens)
    terms = list(tf.keys())

    con.execute("BEGIN")
    try:
        # 1) dict "upsert" per distinct term (manual)
        term_to_id = {}
        for t in terms:
            tid = get_termid(con, t)  # may be None
            if tid is None:
                tid = _next_id(con, "dict", "termid")
                con.execute(
                    "INSERT INTO my_ducklake.dict (termid, term, df) VALUES (?, ?, 1)",
                    (tid, t)
                )
            else:
                con.execute("UPDATE my_ducklake.dict SET df = df + 1 WHERE termid = ?", (tid,))
            term_to_id[t] = tid

        # 2) docs row
        doc_len = len(tokens)
        if docid is None:
            docid = _next_id(con, "docs", "docid")
            con.execute("INSERT INTO my_ducklake.docs (docid, len) VALUES (?, ?)", (docid, doc_len))
        else:
            if _exists(con, "docs", "docid", docid):
                con.execute("UPDATE my_ducklake.docs SET len = ? WHERE docid = ?", (doc_len, docid))
            else:
                con.execute("INSERT INTO my_ducklake.docs (docid, len) VALUES (?, ?)", (docid, doc_len))

        # 3) postings rows
        rows = [(term_to_id[t], docid, tf[t]) for t in terms]
        con.executemany("INSERT INTO my_ducklake.postings (termid, docid, tf) VALUES (?, ?, ?)", rows)

        # 4) data row
        if _exists(con, "data", "docid", docid):
            con.execute("UPDATE my_ducklake.data SET content = ? WHERE docid = ?", (doc, docid))
        else:
            con.execute("INSERT INTO my_ducklake.data (docid, content) VALUES (?, ?)", (docid, doc))

        con.execute("COMMIT")
        return docid
    except Exception:
        con.execute("ROLLBACK")
        raise

def modify(con, docid: int, content: str):
    delete(con, docid)
    return insert(con, content, docid=docid)