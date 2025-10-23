from sqlalchemy import create_engine, text
import pandas as pd
import re
import os

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
if not SUPABASE_DB_URL:
    raise ValueError("SUPABASE_DB_URL environment variable not set")

engine = create_engine(SUPABASE_DB_URL)

VIOLATION_CATEGORIES = {
    **dict.fromkeys([18, 19, 20, 21, 22, 23, 24, 25, 30, 33, 34, 36], "Food Safety & Temperature"),
    **dict.fromkeys([1, 2, 3, 4, 5, 6, 7, 8, 9, 57, 58], "Personnel & Training"),
    **dict.fromkeys([16, 39, 40, 41, 42, 43, 44], "Sanitation & Cleanliness"),
    **dict.fromkeys([10, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56], "Facility & Equipment"),
    **dict.fromkeys([11, 12, 13, 14, 15, 26, 27, 31, 35, 37], "Source & Labeling"),
    **dict.fromkeys([17, 28, 38], "Pest Control & Contamination"),
    **dict.fromkeys([29, 32, 59, 60, 61, 62, 63], "Administrative/Compliance")
}

INCLUDED_FACILITY_KEYWORDS = ['RESTAURANT']


def extract_codes(text):
    if not text or pd.isna(text):
        return None
    codes = re.findall(r'(?:^|\| )(\d+)\.', text)
    return ','.join(codes) if codes else None


def map_categories(codes):
    if not codes:
        return None
    categories = {VIOLATION_CATEGORIES.get(int(c.strip())) for c in codes.split(',')
                  if VIOLATION_CATEGORIES.get(int(c.strip()))}
    return ', '.join(sorted(categories)) if categories else None


def build_facility_filter():
    conditions = [f"UPPER(r.facility_type) LIKE '%{kw}%'" for kw in INCLUDED_FACILITY_KEYWORDS]
    return " OR ".join(conditions)


def export_inspections(output_dir='dumps'):
    os.makedirs(output_dir, exist_ok=True)
    facility_filter = build_facility_filter()

    query = f"""
    SELECT i.id, i.inspection_id, i.restaurant_license, i.inspection_date, i.inspection_type,
           i.result, i.risk, i.violations, r.dba_name, r.address, r.zip
    FROM inspections i
    JOIN restaurants r ON i.restaurant_license = r.license_number
    WHERE i.inspection_date > CURRENT_DATE - INTERVAL '5 years'
      AND ({facility_filter})
      AND i.result != 'Out of Business'
    ORDER BY i.inspection_date DESC
    """

    df = pd.read_sql(text(query), engine)
    print(f"Queried inspections — rows fetched: {len(df)}", flush=True)

    # --- extract codes and categories ---
    df['violation_codes'] = df['violations'].apply(extract_codes)
    df['violation_categories'] = df['violation_codes'].apply(map_categories)
    df['violation_count'] = df['violation_codes'].apply(lambda x: len(x.split(',')) if x else 0)

    df_expanded = df.assign(
        violation_category=df['violation_categories'].str.split(', ')
    ).explode('violation_category')

    df_expanded['violation_count'] = df_expanded.duplicated('id').map({True: 0, False: None})
    df_expanded['violation_count'] = df_expanded['violation_count'].fillna(
        df.groupby('id')['violation_count'].transform('first')
    )

    df_expanded['category_violation_count'] = df_expanded.groupby(
        ['id', 'violation_category']
    )['violation_category'].transform('count')

    df_expanded = df_expanded.drop(['violations', 'violation_categories'], axis=1)
    df_expanded = df_expanded.replace([float('inf'), float('-inf')], float('nan')).fillna('')

    output = os.path.join(output_dir, 'inspections.csv')
    df_expanded.to_csv(output, index=False)
    print(f"Inspections: {len(df_expanded):,} rows, {os.path.getsize(output)/(1024*1024):.2f} MB", flush=True)


def export_restaurants(output_dir='dumps'):
    os.makedirs(output_dir, exist_ok=True)
    facility_filter = build_facility_filter()

    query = f"""
    SELECT DISTINCT r.*
    FROM restaurants r
    WHERE EXISTS (
        SELECT 1 FROM inspections i 
        WHERE i.restaurant_license = r.license_number
        AND i.inspection_date > CURRENT_DATE - INTERVAL '5 years'
    )
    AND ({facility_filter})
    """

    df = pd.read_sql(text(query), engine)
    df = df.replace([float('inf'), float('-inf')], float('nan')).fillna('')

    output = os.path.join(output_dir, 'restaurants.csv')
    df.to_csv(output, index=False)
    print(f"Restaurants: {len(df):,} rows, {os.path.getsize(output)/(1024*1024):.2f} MB", flush=True)


def export_google_ratings(output_dir='dumps'):
    os.makedirs(output_dir, exist_ok=True)
    facility_filter = build_facility_filter()

    query = f"""
    SELECT gr.*
    FROM google_ratings gr
    WHERE gr.restaurant_id IN (
        SELECT DISTINCT r.id
        FROM restaurants r
        WHERE EXISTS (
            SELECT 1 FROM inspections i 
            WHERE i.restaurant_license = r.license_number
            AND i.inspection_date > CURRENT_DATE - INTERVAL '5 years'
        )
        AND ({facility_filter})
    )
    """

    df = pd.read_sql(text(query), engine)
    df = df.replace([float('inf'), float('-inf')], float('nan')).fillna('')

    output = os.path.join(output_dir, 'google_ratings.csv')
    df.to_csv(output, index=False)
    print(f"Google Ratings: {len(df):,} rows, {os.path.getsize(output)/(1024*1024):.2f} MB", flush=True)


if __name__ == "__main__":
    print("Exporting food service establishments only...", flush=True)
    print(f"Including facilities matching: {', '.join(INCLUDED_FACILITY_KEYWORDS)}", flush=True)
    export_inspections()
    export_restaurants()
    export_google_ratings()
    print("Export complete!", flush=True)
