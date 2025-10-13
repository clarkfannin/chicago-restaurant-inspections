import requests
import pandas as pd
from io import StringIO
import psycopg2
import os
from datetime import datetime, date
from urllib.parse import urlparse

CHICAGO_API_TOKEN = os.environ.get("CHICAGO_API_TOKEN")
if not CHICAGO_API_TOKEN:
    raise ValueError("CHICAGO_API_TOKEN environment variable not set")

DB_URL = os.environ.get("SUPABASE_DB_URL")
if not DB_URL:
    raise ValueError("SUPABASE_DB_URL environment variable not set")

parsed_url = urlparse(DB_URL)

def get_connection():
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
    """Fetch only new inspections since last date in DB (safe URL encoding)."""
    print("Checking last inspection date in DB...", flush=True)
    cur = conn.cursor()
    cur.execute("SELECT MAX(inspection_date) FROM inspections;")
    last_date = cur.fetchone()[0]
    cur.close()
    print(f"Last inspection date in DB: {last_date}", flush=True)

    if last_date:
        if isinstance(last_date, (datetime, date)):
            date_str = last_date.strftime('%m/%d/%Y')
        else:
            try:
                parsed = datetime.fromisoformat(str(last_date))
                date_str = parsed.strftime('%m/%d/%Y')
            except Exception:
                date_str = str(last_date)
        where_clause = f"inspection_date>'{date_str}'"
        print(f"Using WHERE clause: {where_clause}", flush=True)
    else:
        where_clause = None
        print("No last_date found; will fetch all rows (first run).", flush=True)

    base_url = 'https://data.cityofchicago.org/api/views/4ijn-s7e5/rows.csv'
    headers = {'X-App-Token': CHICAGO_API_TOKEN}

    params = {'$where': where_clause} if where_clause else None
    print(f"Requesting Chicago API (base URL): {base_url}", flush=True)
    response = requests.get(base_url, headers=headers, params=params, timeout=180)
    print(f"Final request URL: {response.request.url}", flush=True)

    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text))
    print(f"Fetched {len(df)} records from Chicago API.", flush=True)
    return df

def clean_data(df):
    print('Cleaning data...', flush=True)
    df['Inspection Date'] = pd.to_datetime(df['Inspection Date'], errors='coerce', format='%m/%d/%Y')
    df['License #'] = pd.to_numeric(df['License #'], errors='coerce').astype('Int64')
    df['Zip'] = pd.to_numeric(df['Zip'], errors='coerce').astype('Int64')
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

    text_columns = ['DBA Name', 'AKA Name', 'Facility Type', 'Address', 'City', 'State', 'Results', 'Risk', 'Violations']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df = df.dropna(subset=['License #', 'Inspection Date'])
    print(f"Cleaned. {len(df)} valid records.", flush=True)
    return df

def insert_restaurants(df, conn):
    print("Inserting/updating restaurants...", flush=True)
    cur = conn.cursor()
    restaurants = df.groupby('License #').first().reset_index()
    inserted = 0

    for _, row in restaurants.iterrows():
        try:
            cur.execute("""
                INSERT INTO restaurants 
                (license_number, dba_name, aka_name, facility_type, address, city, state, zip, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (license_number) DO UPDATE SET
                    dba_name = EXCLUDED.dba_name,
                    aka_name = EXCLUDED.aka_name,
                    address = EXCLUDED.address,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip = EXCLUDED.zip,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude;
            """, (
                int(row['License #']) if pd.notna(row['License #']) else None,
                row['DBA Name'],
                row['AKA Name'],
                row['Facility Type'],
                row['Address'],
                row['City'],
                row['State'],
                int(row['Zip']) if pd.notna(row['Zip']) else None,
                float(row['Latitude']) if pd.notna(row['Latitude']) else None,
                float(row['Longitude']) if pd.notna(row['Longitude']) else None
            ))
            inserted += 1
        except Exception as e:
            print(f"Error inserting restaurant {row['License #']}: {e}", flush=True)

    conn.commit()
    cur.close()
    print(f"Inserted/updated {inserted} restaurants", flush=True)

def insert_inspections(df, conn):
    print("Inserting new inspections...", flush=True)
    cur = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO inspections 
                (inspection_id, restaurant_license, inspection_date, inspection_type, result, risk, violations)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (inspection_id) DO NOTHING;
            """, (
                int(row['Inspection ID']) if pd.notna(row['Inspection ID']) else None,
                int(row['License #']) if pd.notna(row['License #']) else None,
                row['Inspection Date'].date() if pd.notna(row['Inspection Date']) else None,
                row['Inspection Type'],
                row['Results'],
                row['Risk'],
                row['Violations'] if pd.notna(row['Violations']) else None
            ))
            inserted += 1
        except Exception as e:
            print(f"Error inserting inspection {row.get('Inspection ID', 'unknown')}: {e}", flush=True)

    conn.commit()
    cur.close()
    print(f"Inserted {inserted} new inspections", flush=True)

def main():
    start_time = datetime.now()
    print(f"Starting data load at {start_time}", flush=True)

    try:
        conn = get_connection()
        df = fetch_inspection_data(conn)
        if df.empty:
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
