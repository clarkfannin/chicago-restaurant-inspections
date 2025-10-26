import os
import psycopg2
from urllib.parse import urlparse

LOCAL_DB_URL = "postgresql://clarkfannin@localhost:5432/chicago_inspections"
SUPABASE_DB_URL = "postgresql://postgres.arfutvgrtgogpyaeoftv:FenderJaguar2003!@aws-1-us-east-2.pooler.supabase.com:6543/postgres"

parsed_supabase = urlparse(SUPABASE_DB_URL)

def get_local_conn():
    return psycopg2.connect(LOCAL_DB_URL)

def get_supabase_conn():
    return psycopg2.connect(
        dbname=parsed_supabase.path[1:],
        user=parsed_supabase.username,
        password=parsed_supabase.password,
        host=parsed_supabase.hostname,
        port=parsed_supabase.port
    )

def migrate_table(table_name, key_column, batch_size=1000):
    print(f"Starting migration for {table_name}...")

    local_conn = get_local_conn()
    supabase_conn = get_supabase_conn()
    local_cur = local_conn.cursor()
    supabase_cur = supabase_conn.cursor()

    local_cur.execute(f"SELECT * FROM {table_name};")
    rows = local_cur.fetchall()
    columns = [desc[0] for desc in local_cur.description]

    print(f"Fetched {len(rows)} rows from local {table_name}")

    col_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    update_set = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col != key_column])

    insert_sql = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({key_column}) DO UPDATE SET {update_set};
    """

    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        try:
            supabase_cur.executemany(insert_sql, batch)
            supabase_conn.commit()
            print(f"Migrated rows {start}–{start + len(batch) - 1} to Supabase {table_name}")
        except Exception as e:
            print(f"Error inserting batch starting at row {start}: {e}")
            supabase_conn.rollback()

    local_cur.close()
    supabase_cur.close()
    local_conn.close()
    supabase_conn.close()
    print(f"Finished migration for {table_name}")


if __name__ == "__main__":
    migrate_table("google_ratings", "restaurant_id")
    print("Full migration complete!")
