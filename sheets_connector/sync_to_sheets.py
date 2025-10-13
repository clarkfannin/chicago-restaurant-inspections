import pandas as pd
from sqlalchemy import create_engine
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# --- sql connection ---
engine = create_engine("postgresql://clarkfannin@localhost:5432/chicago_inspections")

tables = ["inspections", "google_ratings"]  # replace with your 3 tables

# --- google sheets setup ---
creds = Credentials.from_service_account_file(
    "sheets_connector/credentials.json",
    scopes=["https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"]
)
client = gspread.authorize(creds)

spreadsheet = client.open("my_sql_clone")

def safe_set_with_dataframe(worksheet, df, chunk_size=5000):
    worksheet.clear()
    from gspread_dataframe import set_with_dataframe
    for start in range(0, len(df), chunk_size):
        end = start + chunk_size
        temp_df = df.iloc[start:end]
        if start == 0:
            set_with_dataframe(worksheet, temp_df)
        else:
            worksheet.append_rows(temp_df.values.tolist(), value_input_option="USER_ENTERED")

for t in tables:
    df = pd.read_sql(f"SELECT * FROM public.{t}", engine)
    df = df.astype(str)
    
    try:
        sheet = spreadsheet.worksheet(t)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=t, rows="100", cols="20")
    safe_set_with_dataframe(sheet, df)
    print(f"✅ synced {t}")
