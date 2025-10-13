import requests
import pandas as pd
from io import StringIO
import psycopg2
import os
from datetime import datetime
from urllib.parse import urlparse

CHICAGO_API_TOKEN = os.environ.get("CHICAGO_API_TOKEN")
if not CHICAGO_API_TOKEN:
    raise ValueError("CHICAGO_API_TOKEN environment variable not set")

DB_URL = os.environ.get("SUPABASE_DB_URL")
if not DB_URL:
    raise ValueError("SUPABASE_DB_URL environment variable not set")

parsed_url = urlparse(DB_URL)

def get_connection():
    return psycopg2.connect(
        dbname=parsed_url.path[1:],
        user=parsed_url.username,
        password=parsed_url.password,
        host=parsed_url.hostname,
        port=parsed_url.port
    )

def fetch_inspection_data(conn):
    """Fetch only new inspections since last date in DB."""
    cur = conn.cursor()
    cur.execute("SELECT MAX(inspection_date) FROM inspections;")
    last_date = cur.fetchone()[0]
    cur.close()

    if last_date:
        filter_str = f"?$where=inspection_date>'{last_date.strftime('%Y-%m-%d')}'"
    else:
        filter_str = ""

    url = f'https://data.cityofchicago.org/api/views/4ijn-s7e5/rows.csv{filter_str}'
    headers = {'X-App-Token': CHICAGO_API_TOKEN}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    df = pd.read_csv(StringIO(response.text))
    print(f'Fetched {len(df)} new records.')
    return df

def main():
    start_time = datetime.now()
    print(f"Starting data load at {start_time}")

    try:
        conn = get_connection()
        df = fetch_inspection_data(conn)
        df = clean_data(df)

        insert_restaurants(df, conn)
        insert_inspections(df, conn)

        conn.close()
        duration = (datetime.now() - start_time).total_seconds()
        print(f"\nData load completed successfully in {duration:.2f} seconds")

    except Exception as e:
        print(f"\nError during data load: {e}")
        raise


if __name__ == "__main__":
    main()
