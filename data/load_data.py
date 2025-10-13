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
    """Get a connection to the Supabase database."""
    print("Connecting to Supabase...", flush=True)
    conn = psycopg2.connect(
        dbname=parsed_url.path[1:],
        user=parsed_url.username,
        password=parsed_url.password,
        host=parsed_url.hostname,
        port=parsed_url.port
    )
    print("Connected!", flush=True)
    return conn

def fetch_inspection_data(conn):
    """Fetch only new inspections since last date in DB."""
    print("Checking last inspection date in DB...", flush=True)
    cur = conn.cursor()
    cur.execute("SELECT MAX(inspection_date) FROM inspections;")
    last_date = cur.fetchone()[0]
    cur.close()

    if last_date:
        print(f"Last inspection date: {last_date}", flush=True)
        filter_str = f"?$where=inspection_date>'{last_date.strftime('%Y-%m-%d')}'"
    else:
        print("No previous inspections found, fetching all data...", flush=True)
        filter_str = ""

    url = f'https://data.cityofchicago.org/api/views/4ijn-s7e5/rows.csv{filter_str}'
    print(f"Fetching new inspections from Chicago API...\nURL: {url}", flush=True)
    headers = {'X-App-Token': CHICAGO_API_TOKEN}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    df = pd.read_csv(StringIO(response.text))
    print(f"Fetched {len(df)} new records.", flush=True)
    return df

def clean_data(df):
    print("Cleaning data...", flush=True)
    df['Inspection Date'] = pd.to_datetime(df['Inspection Date'], errors='coerce')
    df['License #'] = pd.to_numeric(df['License #'], errors='coerce').astype('Int64')
    df['Zip'] = pd.to_numeric(df['Zip'], errors='coerce').astype('Int64')
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

    text_columns = ['DBA Name', 'AKA Name', 'Facility Type', 'Address', 'City', 'State', 'Results', 'Risk']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df = df.dropna(subset=['License #', 'Inspection Date'])
    print(f"Cleaned. {len(df)} valid records.", flush=True)
    return df

# insert_restaurants and insert_inspections remain the same, with print statements

def main():
    start_time = datetime.now()
    print(f"Starting data load at {start_time}", flush=True)

    try:
        conn = get_connection()
        df = fetch_inspection_data(conn)

        if len(df) == 0:
            print("No new inspections to load.", flush=True)
            conn.close()
            return

        df = clean_data(df)
        insert_restaurants(df, conn)
        insert_inspections(df, conn)

        conn.close()
        duration = (datetime.now() - start_time).total_seconds()
        print(f"\nData load completed successfully in {duration:.2f} seconds", flush=True)

    except Exception as e:
        print(f"\nError during data load: {e}", flush=True)
        raise

if __name__ == "__main__":
    main()
