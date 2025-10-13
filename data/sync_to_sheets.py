import pandas as pd
from sqlalchemy import create_engine
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# --- sql connection ---
engine = create_engine("postgresql://clarkfannin@localhost:5432/chicago_inspections")

tables = ["inspections", "google_ratings", "restaurants"]

# --- google sheets setup ---
creds = Credentials.from_service_account_file(
    "credentials.json",
    scopes=["https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"]
)
client = gspread.authorize(creds)
spreadsheet = client.open("my_sql_clone")


def append_dataframe(worksheet, df, chunk_size=5000):
    try:
        existing_rows = len(worksheet.get_all_values())
    except:
        existing_rows = 0

    if existing_rows == 0:
        worksheet.append_row(df.columns.tolist(), value_input_option="USER_ENTERED")
        existing_rows = 1

    df_to_add = df.iloc[existing_rows-1:]
    if df_to_add.empty:
        print(f"Worksheet {worksheet.title} is already up to date")
        return

    for start in range(0, len(df_to_add), chunk_size):
        end = start + chunk_size
        chunk = df_to_add.iloc[start:end]
        worksheet.append_rows(chunk.values.tolist(), value_input_option="USER_ENTERED")

    print(f"Appended {len(df_to_add)} rows to {worksheet.title}")



for t in tables:
    df = pd.read_sql(f"SELECT * FROM public.{t}", engine)
    df = df.astype(str)
    
    try:
        sheet = spreadsheet.worksheet(t)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=t, rows=str(len(df)+1000), cols=str(df.shape[1]+5))

    append_dataframe(sheet, df)
