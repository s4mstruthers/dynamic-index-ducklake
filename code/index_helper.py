def create_temp_tables(con):
    create_table_sql = """
        -- Creating TEMP table for dict
        CREATE TEMP TABLE dict (
            termid BIGINT,
            term VARCHAR,
            df BIGINT
        );

        -- Creating TEMP table for docs
        CREATE TEMP TABLE docs (
            docid BIGINT,
            name INTEGER,
            len BIGINT
        );

        -- Creating TEMP table for postings
        CREATE TEMP TABLE postings (
            termid BIGINT,
            docid BIGINT,
            tf BIGINT
        );
    """
    con.execute(create_table_sql)
    
def test(con):
     tables = con.execute("""
                         SHOW ALL TABLES;
                         """).fetch_df()
     print("Tables in database: \n",tables)
     print("\n")
     describe = con.execute(""" 
                        DESCRIBE my_ducklake.dict;
                        """).fetch_df()
     print("Describe dict: \n",describe)
     describe = con.execute(""" 
                        DESCRIBE my_ducklake.docs;
                        """).fetch_df()
     print("Describe docs: \n",describe)
     describe = con.execute(""" 
                        DESCRIBE my_ducklake.postings;
                        """).fetch_df()
     print("Describe postings: \n",describe)