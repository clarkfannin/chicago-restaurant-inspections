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

for csv_name in CSV_FILES:
    print(f"Syncing {csv_name}...")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=csv_name)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    
    df = df.replace([float('inf'), float('-inf')], float('nan'))
    df = df.fillna('')

    sheet_tab = os.path.splitext(csv_name)[0]

    try:
        ws = sh.worksheet(sheet_tab)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_tab, rows=str(len(df)+10), cols=str(len(df.columns)))

    ws.update([df.columns.values.tolist()] + df.values.tolist())

print("All files synced to Google Sheets!")
