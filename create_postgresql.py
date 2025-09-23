import psycopg2

# Connect to default postgres DB
conn = psycopg2.connect(dbname="postgres", user="samstruthers", password="jofkud-caTha5-jezmud", host="localhost")
conn.autocommit = True  # Required for CREATE DATABASE as it must execute immediately, outside a transaction block

cur = conn.cursor()
cur.execute("CREATE DATABASE ducklake_catalog;")

cur.close()
conn.close()