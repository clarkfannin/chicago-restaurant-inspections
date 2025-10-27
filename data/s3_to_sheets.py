import os
import io
import boto3
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import hashlib

BUCKET_NAME = "inspection-data-dump"
CSV_FILES = ["restaurants.csv", "inspections.csv", "google_ratings.csv", "inspection_categories.csv"]
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
NUMERIC_COLUMNS = {
    'restaurants': ['id', 'license_number', 'zip', 'latitude', 'longitude'],
    'inspections': ['id', 'restaurant_license', 'violation_count'],
    'google_ratings': ['id', 'restaurant_id', 'rating', 'user_ratings_total'],
    'inspection_categories': ['id', 'restaurant_license', 'zip', 'category_violation_count']
}
DATE_COLUMNS = {
    'inspections': ['inspection_date'],
    'inspection_categories': ['inspection_date']
}

creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT"]
creds = Credentials.from_service_account_info(eval(creds_json), scopes=SCOPES)
gc = gspread.authorize(creds)
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
sh = gc.open_by_key(SHEET_ID)
s3 = boto3.client("s3")

def hash_df(df):
    return hashlib.md5(df.to_csv(index=False).encode('utf-8')).hexdigest()

def sanitize_df(df, table_name):
    df = df.replace([float('inf'), float('-inf')], pd.NA)
    numeric_cols = NUMERIC_COLUMNS.get(table_name, [])
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    date_cols = DATE_COLUMNS.get(table_name, [])
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            df[col] = df[col].astype(str)
            df[col] = df[col].replace('NaT', '')
    for col in df.columns:
        if col not in numeric_cols + date_cols:
            df[col] = df[col].fillna('')
    df = df.fillna('')
    return df

for csv_name in CSV_FILES:
    print(f"Syncing {csv_name}...", flush=True)
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=csv_name)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    table_name = os.path.splitext(csv_name)[0]
    df = sanitize_df(df, table_name)
    new_hash = hash_df(df)
    try:
        ws = sh.worksheet(table_name)
        existing = ws.get_all_values()
        existing_df = pd.DataFrame(existing[1:], columns=existing[0]) if existing else pd.DataFrame()
        existing_df = sanitize_df(existing_df, table_name)
        existing_hash = hash_df(existing_df)
        if existing_hash == new_hash:
            print(f"Skipped {csv_name} (no changes)", flush=True)
            continue
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=table_name, rows=str(len(df)+10), cols=str(len(df.columns)))
    data = [df.columns.tolist()] + df.astype(str).values.tolist()
    ws.update(data)
    numeric_cols = NUMERIC_COLUMNS.get(table_name, [])
    for col in numeric_cols:
        if col in df.columns:
            col_idx = df.columns.get_loc(col) + 1
            col_letter = chr(64 + col_idx)
            pattern = "0" if col in ['id', 'restaurant_id', 'license_number', 'user_ratings_total', 'zip', 'violation_count', 'category_violation_count'] else "0.0##"
            ws.format(f'{col_letter}2:{col_letter}', {"numberFormat": {"type": "NUMBER", "pattern": pattern}})
    print(f"Synced {len(df)} rows", flush=True)

print("All files synced to Google Sheets!", flush=True)
