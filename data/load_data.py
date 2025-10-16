import requests
import pandas as pd
import psycopg2
import os
from datetime import datetime
from urllib.parse import urlparse

CHICAGO_API_TOKEN = os.environ.get("CHICAGO_API_TOKEN")
if not CHICAGO_API_TOKEN:
    raise ValueError("CHICAGO_API_TOKEN environment variable not set")

SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise ValueError("SUPABASE_DB_URL environment variable not set")

parsed_url = urlparse(SUPABASE_DB_URL)

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
    
    print("Checking last inspection date in DB...", flush=True)
    cur = conn.cursor()
    cur.execute("SELECT MAX(inspection_date) FROM inspections;")
    last_date = cur.fetchone()[0]
    cur.close()
    print(f"Last inspection date in DB: {last_date}", flush=True)

    if last_date:
        date_str = last_date.strftime("%Y-%m-%d")
        filter_str = f"?$where=inspection_date>'{date_str}'"
        print(f"Filtering from date: {date_str}", flush=True)
    else:
        filter_str = ""
        print("No last_date found; fetching all data.", flush=True)

    url = f"https://data.cityofchicago.org/resource/4ijn-s7e5.json{filter_str}"
    print(f"Query URL: {url}", flush=True)

    headers = {"X-App-Token": CHICAGO_API_TOKEN}
    response = requests.get(url, headers=headers, timeout=180)
    response.raise_for_status()

    data = response.json()
    print(f"Fetched {len(data)} records from JSON API.", flush=True)

    df = pd.DataFrame(data)
    df.columns = [col.strip().replace("_", " ").title() for col in df.columns]
    rename_map = {
        "License": "License #",
        "License ": "License #",
        "Inspection Id": "Inspection ID",
        "Inspection Type": "Inspection Type",
        "Dba Name": "DBA Name",
        "Aka Name": "AKA Name",
}
    df.rename(columns=rename_map, inplace=True)

    return df




def clean_data(df):
    if "Inspection Date" not in df.columns:
        if "Inspectiondate" in df.columns:
            df.rename(columns={"Inspectiondate": "Inspection Date"}, inplace=True)
        else:
            raise KeyError("Expected column 'inspection_date' or 'Inspection Date' not found.")

    df["Inspection Date"] = pd.to_datetime(df["Inspection Date"], errors="coerce")

    df = df.dropna(subset=["Inspection Date"])

    for col in ["Facility Type", "Risk", "Results", "Violations"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

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
                INSERT INTO inspections (
                    id, inspection_id, restaurant_license,
                    inspection_date, inspection_type, result, risk, violations, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO NOTHING;
            """, (
                int(row['Inspection ID']) if pd.notna(row['Inspection ID']) else None,  # id
                int(row['Inspection ID']) if pd.notna(row['Inspection ID']) else None,  # inspection_id (duplicate of id)
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
