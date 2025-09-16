import io
import pandas as pd
import psycopg2

PARQUET_PATH = "./owi-dedup/metadata_0.parquet"
TABLE_NAME = "metadata_test"  # change if you want a different name

def quote_ident(ident: str) -> str:
    # Double-quote identifier and escape embedded quotes
    return '"' + ident.replace('"', '""') + '"'

def main():
    # 1) Read parquet WITHOUT altering columns
    df = pd.read_parquet(PARQUET_PATH)

    # Fail fast on duplicate column names (COPY requires unique headers)
    cols = list(df.columns)
    if len(cols) != len(set(cols)):
        raise ValueError("Duplicate column names in Parquet. Fix the source or rename before load.")

    # 2) Build CREATE TABLE IF NOT EXISTS: all TEXT columns with quoted names
    cols_ddl = ",\n  ".join(f"{quote_ident(c)} TEXT" for c in cols)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {quote_ident(TABLE_NAME)} (
      {cols_ddl}
    );
    """

    # 3) Connect
    conn = psycopg2.connect(
        dbname="postgres",
        user="duck",
        password="duckpass",
        host="localhost",
        port=5432,
    )
    conn.autocommit = False
    cur = conn.cursor()

    # 4) Ensure table exists (no truncate: we append)
    cur.execute(create_sql)
    conn.commit()

    # 5) COPY data (append). Use explicit quoted column list to preserve names exactly.
    col_list_sql = ", ".join(quote_ident(c) for c in cols)
    copy_sql = f"COPY {quote_ident(TABLE_NAME)} ({col_list_sql}) FROM STDIN WITH (FORMAT csv, HEADER false)"

    # Convert DataFrame to CSV in-memory; HEADER false aligns with COPY column list
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)  # exact values as strings
    buf.seek(0)

    cur.copy_expert(copy_sql, buf)
    conn.commit()

    cur.close()
    conn.close()
    print(f"Loaded {len(df)} rows into {TABLE_NAME}.")

if __name__ == "__main__":
    main()