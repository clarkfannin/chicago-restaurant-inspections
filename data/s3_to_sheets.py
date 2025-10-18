import os
import io
import boto3
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

BUCKET_NAME = "inspection-data-dump"
CSV_FILES = ["restaurants.csv", "inspections.csv", "google_ratings.csv"]
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT"]
creds = Credentials.from_service_account_info(eval(creds_json), scopes=SCOPES)
gc = gspread.authorize(creds)

SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
sh = gc.open_by_key(SHEET_ID)

s3 = boto3.client("s3")

NUMERIC_COLUMNS = {
    'restaurants': ['id', 'license_number', 'zip', 'latitude', 'longitude'],
    'inspections': ['id', 'inspection_id', 'restaurant_license', 'violation_count'],
    'google_ratings': ['id', 'restaurant_id', 'rating', 'user_ratings_total']
}

for csv_name in CSV_FILES:
    print(f"Syncing {csv_name}...", flush=True)
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=csv_name)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    
    df = df.replace([float('inf'), float('-inf')], float('nan'))
    
    table_name = os.path.splitext(csv_name)[0]
    numeric_cols = NUMERIC_COLUMNS.get(table_name, [])
    
    for col in df.columns:
        if col in numeric_cols:
            pass
        else:
            df[col] = df[col].fillna('')
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    sheet_tab = table_name

    try:
        ws = sh.worksheet(sheet_tab)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_tab, rows=str(len(df)+10), cols=str(len(df.columns)))

    data = [df.columns.values.tolist()]
    
    for _, row in df.iterrows():
        row_data = []
        for col in df.columns:
            val = row[col]
            if pd.isna(val) and col in numeric_cols:
                row_data.append('')
            elif pd.isna(val):
                row_data.append('')
            else:
                row_data.append(val)
        data.append(row_data)
    
    ws.update(data)
    
    for col in numeric_cols:
        if col in df.columns:
            col_idx = df.columns.get_loc(col) + 1
            col_letter = chr(64 + col_idx)

            ws.format(f'{col_letter}2:{col_letter}', {
                "numberFormat": {
                    "type": "NUMBER",
                    "pattern": "0"
                }
            })
    
    print(f"  ✓ Synced {len(df)} rows", flush=True)

print("All files synced to Google Sheets!", flush=True)