
# --------------------Helper functions ----------------------------

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

def docid_to_content(con, docid):
    content_sql = f"""
    SELECT main_content FROM my_ducklake.data WHERE docid = {docid};
    """
    content = con.execute(content_sql).fetchone()
    if content:
        return content