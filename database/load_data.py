import requests
import pandas as pd
from io import StringIO
import psycopg2
from datetime import datetime


def fetch_inspection_data():
    print('Starting...')
    url = 'https://data.cityofchicago.org/api/views/4ijn-s7e5/rows.csv'
    headers = {
        'X-App-Token': 'hYbEzmjNHCSUUGU4vdQFJmPvk'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text))
    print(f'Fetched {len(df)} records.')
    return df


def clean_data(df):
    print('Cleaning...')
    df['Inspection Date'] = pd.to_datetime(
        df['Inspection Date'], errors='coerce')
    df['License #'] = pd.to_numeric(
        df['License #'], errors='coerce').astype('Int64')
    df['Zip'] = pd.to_numeric(df['Zip'], errors='coerce').astype('Int64')
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

    text_columns = ['DBA Name', 'AKA Name', 'Facility Type',
                    'Address', 'City', 'State', 'Results', 'Risk']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df = df.dropna(subset=['License #', 'Inspection Date'])
    print(f'Cleaned. {len(df)} valid records.')
    return df


def insert_restaurants(df, conn):
    print("Inserting restaurants...")

    cur = conn.cursor()
    restaurants = df.groupby('License #').first().reset_index()

    inserted = 0
    for idx, row in restaurants.iterrows():
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
            print(f"Error inserting restaurant {row['License #']}: {e}")

    conn.commit()
    cur.close()
    print(f"Inserted/updated {inserted} restaurants")


def insert_inspections(df, conn):
    """Insert inspections into database"""
    print("Inserting inspections...")

    cur = conn.cursor()

    inserted = 0
    for idx, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO inspections 
                (inspection_id, restaurant_license, inspection_date, inspection_type, result, risk, violations)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (inspection_id) DO NOTHING;
            """, (
                int(row['Inspection ID']) if pd.notna(
                    row['Inspection ID']) else None,
                int(row['License #']) if pd.notna(row['License #']) else None,
                row['Inspection Date'].date() if pd.notna(
                    row['Inspection Date']) else None,
                row['Inspection Type'],
                row['Results'],
                row['Risk'],
                row['Violations'] if pd.notna(row['Violations']) else None
            ))
            inserted += 1
        except Exception as e:
            print(
                f"Error inserting inspection {row.get('Inspection ID', 'unknown')}: {e}")

    conn.commit()
    cur.close()
    print(f"Inserted {inserted} new inspections")

def main():
    start_time = datetime.now()
    print(f"Starting data load at {start_time}")

    try:
        df = fetch_inspection_data()

        df = clean_data(df)

        conn = psycopg2.connect(
            dbname="chicago_inspections",
            user="clarkfannin",
            host="localhost"
        )

        insert_restaurants(df, conn)
        insert_inspections(df, conn)

        conn.close()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(
            f"\nData load completed successfully in {duration:.2f} seconds")

    except Exception as e:
        print(f"\nError during data load: {e}")
        raise


if __name__ == "__main__":
    main()
