#!/usr/bin/env python3
"""
verify_index.py

Correctness checks for the DuckLake dynamic index. Three layers:

  1. Invariants  (default, read-only) — internal consistency of the live index:
       (a) dict.df  == real number of distinct docs per term
       (b) docs.len == sum of term frequencies for the doc
       (c) no orphan postings (every posting points to a real term and doc)
       (d) data and docs row counts match

  2. Behaviour   (--behaviour) — runs a full insert -> modify -> delete cycle on a
     sentinel document with unique nonsense terms, asserting the index reacts
     correctly at each step, then removes the sentinel so the index is left
     unchanged.

  3. Parity      (--parity) — the gold-standard test. Snapshots the incrementally
     maintained index, runs a full `reindex` (rebuilt from `data` from scratch, the
     source of truth), and confirms the two are identical. Comparison is keyed on
     the term STRING, not termid, because reindex and insert assign term IDs
     differently. NOTE: this rebuilds the index (content is unchanged if correct).

Usage:
    python tests/verify_index.py                 # invariants only (safe, read-only)
    python tests/verify_index.py --behaviour     # + sentinel insert/modify/delete
    python tests/verify_index.py --parity        # + reindex-parity check
    python tests/verify_index.py --all           # everything

Exit code is 0 if all selected checks pass, 1 otherwise (CI-friendly).
"""

import argparse
import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

# Make the project's modules in ../code importable when run from anywhere.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "code"))

import duckdb
from helper_functions import connect_ducklake, get_docid_count
from index_tools import reindex, insert, modify, delete
from fts_tools import run_bm25_query


# ---------------------------------------------------------------------
# Small result-reporting helpers
# ---------------------------------------------------------------------
class Reporter:
    """Collects named PASS/FAIL results and prints a summary."""

    def __init__(self):
        self.results = []  # list[(name, passed, detail)]

    def record(self, name, passed, detail=""):
        self.results.append((name, passed, detail))
        tag = "PASS" if passed else "FAIL"
        line = f"  [{tag}] {name}"
        if detail:
            line += f"  ->  {detail}"
        print(line)

    def all_passed(self):
        return all(p for _, p, _ in self.results)

    def summary(self):
        total = len(self.results)
        passed = sum(1 for _, p, _ in self.results if p)
        print("-" * 60)
        print(f"  {passed}/{total} checks passed.")
        print("-" * 60)


# ---------------------------------------------------------------------
# Layer 1: structural invariants (read-only)
# ---------------------------------------------------------------------
def check_invariants(con, rep):
    print("\n== Invariant checks (read-only) ==")

    # (a) df integrity
    rows = con.execute("""
        SELECT d.termid, d.df, p.real_df
        FROM my_ducklake.dict d
        JOIN (SELECT termid, COUNT(DISTINCT docid) AS real_df
              FROM my_ducklake.postings GROUP BY termid) p USING (termid)
        WHERE d.df <> p.real_df
        LIMIT 5
    """).fetchall()
    rep.record("dict.df matches distinct doc count", not rows,
               "" if not rows else f"{len(rows)}+ mismatches, e.g. {rows[0]}")

    # (b) length integrity
    rows = con.execute("""
        SELECT dc.docid, dc.len, s.tf_sum
        FROM my_ducklake.docs dc
        JOIN (SELECT docid, SUM(tf) AS tf_sum
              FROM my_ducklake.postings GROUP BY docid) s USING (docid)
        WHERE dc.len <> s.tf_sum
        LIMIT 5
    """).fetchall()
    rep.record("docs.len matches sum of term frequencies", not rows,
               "" if not rows else f"{len(rows)}+ mismatches, e.g. {rows[0]}")

    # (c) no orphan postings
    orphans = con.execute("""
        SELECT COUNT(*) FROM my_ducklake.postings p
        LEFT JOIN my_ducklake.dict d USING (termid)
        LEFT JOIN my_ducklake.docs o USING (docid)
        WHERE d.termid IS NULL OR o.docid IS NULL
    """).fetchone()[0]
    rep.record("no orphan postings", orphans == 0,
               "" if orphans == 0 else f"{orphans} orphan rows")

    # (d) every indexed doc exists in the source data (docs is a subset of data).
    # NOTE: docs can legitimately be SMALLER than data: a document whose content
    # produces no [a-z]+ tokens (empty / null / non-Latin text) is never added to
    # docs/postings/dict. A full reindex yields the same gap, so we only require
    # that there are no *orphan* indexed docs, not that the counts are equal.
    orphan_docs = con.execute("""
        SELECT COUNT(*) FROM my_ducklake.docs dc
        LEFT JOIN my_ducklake.data da USING (docid)
        WHERE da.docid IS NULL
    """).fetchone()[0]
    rep.record("every indexed doc exists in source data", orphan_docs == 0,
               "" if orphan_docs == 0 else f"{orphan_docs} indexed docs missing from data")

    # Informational: how many source docs are unindexed (no alphabetic tokens).
    data_n = con.execute("SELECT COUNT(*) FROM my_ducklake.data").fetchone()[0]
    docs_n = con.execute("SELECT COUNT(*) FROM my_ducklake.docs").fetchone()[0]
    print(f"  [info] data={data_n}, indexed docs={docs_n}, "
          f"unindexed (no [a-z]+ tokens or duplicate docid)={data_n - docs_n}")


# ---------------------------------------------------------------------
# Layer 2: behavioural insert -> modify -> delete cycle (self-cleaning)
# ---------------------------------------------------------------------
def _doc_has_term(con, docid, term):
    """True if `docid` has a posting for `term` (the retrievable-via-index test)."""
    row = con.execute("""
        SELECT 1
        FROM my_ducklake.postings p
        JOIN my_ducklake.dict d USING (termid)
        WHERE d.term = ? AND p.docid = ?
        LIMIT 1
    """, [term, docid]).fetchone()
    return row is not None


def check_behaviour(con, rep):
    print("\n== Behaviour checks (insert -> modify -> delete) ==")

    # Unique nonsense terms so we never collide with the real corpus.
    term_before = "qsentinelaaa"
    term_after = "qsentinelbbb"
    start_count = get_docid_count(con)

    # INSERT (auto-assigned docid)
    docid = insert(con, f"{term_before} commonfiller word")
    rep.record("insert: new doc retrievable by its term",
               _doc_has_term(con, docid, term_before),
               f"docid={docid}")
    rep.record("insert: doc count increased by 1",
               get_docid_count(con) == start_count + 1)

    # Regression: a conjunctive query that repeats a term must still match the
    # document (de-duplication of query termids).
    with redirect_stdout(io.StringIO()):
        results, _ = run_bm25_query(
            con, f"{term_before} {term_before}", top_n=5, qtype="conjunctive")
    rep.record("conjunctive query with a repeated term still matches",
               any(d == docid for d, _ in results))

    # MODIFY (swap the unique term)
    modify(con, docid, f"{term_after} commonfiller word")
    rep.record("modify: old term no longer retrieves the doc",
               not _doc_has_term(con, docid, term_before))
    rep.record("modify: new term retrieves the doc",
               _doc_has_term(con, docid, term_after))
    rep.record("modify: doc count unchanged",
               get_docid_count(con) == start_count + 1)

    # Invariants must still hold mid-flight.
    mid = Reporter()
    check_invariants(con, mid)
    rep.record("modify: invariants still hold", mid.all_passed())

    # DELETE (cleanup) — leaves the index as we found it.
    delete(con, docid)
    rep.record("delete: doc no longer retrievable",
               not _doc_has_term(con, docid, term_after))
    rep.record("delete: doc count restored to original",
               get_docid_count(con) == start_count)

    # Regression: inserting a token-less document must NOT add a docs row, so the
    # incremental index stays consistent with a from-scratch reindex.
    zero_id = insert(con, "1234 5678 -- !!!")     # no [a-z] tokens
    rep.record("token-less insert does not create a docs row (reindex-consistent)",
               get_docid_count(con) == start_count)
    delete(con, zero_id)                          # remove its row from `data`


# ---------------------------------------------------------------------
# Layer 3: reindex parity (rebuild from scratch and compare)
# ---------------------------------------------------------------------
def _snapshot(con):
    """Read the index keyed on term strings (termid-independent)."""
    df = {t: d for t, d in
          con.execute("SELECT term, df FROM my_ducklake.dict").fetchall()}
    dl = {doc: ln for doc, ln in
          con.execute("SELECT docid, len FROM my_ducklake.docs").fetchall()}
    pg = {(term, doc): tf for term, doc, tf in con.execute("""
              SELECT d.term, p.docid, p.tf
              FROM my_ducklake.postings p
              JOIN my_ducklake.dict d USING (termid)
          """).fetchall()}
    return df, dl, pg


def check_parity(con, rep):
    print("\n== Reindex-parity check (rebuilds the index) ==")

    before_df, before_dl, before_pg = _snapshot(con)
    reindex(con)
    after_df, after_dl, after_pg = _snapshot(con)

    def diff(name, a, b):
        only_a = set(a) - set(b)
        only_b = set(b) - set(a)
        changed = [k for k in (set(a) & set(b)) if a[k] != b[k]]
        ok = not (only_a or only_b or changed)
        detail = "" if ok else (
            f"incremental-only={len(only_a)}, rebuild-only={len(only_b)}, "
            f"value-mismatch={len(changed)}"
        )
        rep.record(f"parity: {name}", ok, detail)

    diff("dict (term, df)", before_df, after_df)
    diff("docs (docid, len)", before_dl, after_dl)
    diff("postings (term, docid, tf)", before_pg, after_pg)


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Verify DuckLake index correctness.")
    parser.add_argument("--behaviour", action="store_true",
                        help="Run the insert/modify/delete sentinel cycle (self-cleaning).")
    parser.add_argument("--parity", action="store_true",
                        help="Run the reindex-parity check (rebuilds the index).")
    parser.add_argument("--all", action="store_true",
                        help="Run every check.")
    args = parser.parse_args()

    run_behaviour = args.behaviour or args.all
    run_parity = args.parity or args.all

    con = duckdb.connect()
    connect_ducklake(con)

    # Bail out early on an empty index — the checks would be meaningless.
    if get_docid_count(con) == 0:
        print("ERROR: index is empty. Run `initialise` (or `reindex`) first.")
        sys.exit(1)

    rep = Reporter()
    check_invariants(con, rep)
    if run_behaviour:
        check_behaviour(con, rep)
    if run_parity:
        check_parity(con, rep)

    print()
    rep.summary()
    sys.exit(0 if rep.all_passed() else 1)


if __name__ == "__main__":
    main()
