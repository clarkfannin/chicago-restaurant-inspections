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
OUTPUT_DIR = 'dumps'
os.makedirs(OUTPUT_DIR, exist_ok=True)


def build_facility_filter():
    return " OR ".join(f"UPPER(r.facility_type) LIKE '%{kw}%'" for kw in INCLUDED_FACILITY_KEYWORDS)


def read_sql_clean(query):
    df = pd.read_sql(text(query), engine)
    return df.replace([float('inf'), float('-inf')], pd.NA).fillna('')


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


def export_inspections(facility_filter):
    query = f"""
    SELECT i.id, i.restaurant_license, i.inspection_date, i.result,
           r.dba_name, r.address, r.zip, i.violations
    FROM inspections i
    JOIN restaurants r ON i.restaurant_license = r.license_number
    WHERE i.inspection_date > CURRENT_DATE - INTERVAL '5 years'
      AND ({facility_filter})
      AND i.result != 'Out of Business'
    ORDER BY i.inspection_date DESC
    """
    df = read_sql_clean(query)
    df['violation_codes'] = df['violations'].apply(extract_codes)

    df['violation_count'] = df['violation_codes'].apply(
        lambda x: len(x.split(',')) if x else 0
    )

    df['inspection_date'] = pd.to_datetime(
        df['inspection_date']).dt.strftime('%Y-%m-%d')
    df = df.drop(columns=['violations'])
    df.to_csv(os.path.join(OUTPUT_DIR, 'inspections.csv'), index=False)
    print(f"Inspections: {len(df):,} rows")


def export_inspection_categories(facility_filter):
    query = f"""
    SELECT i.id, i.restaurant_license, i.inspection_date, i.result, i.violations,
           r.dba_name, r.address, r.zip
    FROM inspections i
    JOIN restaurants r ON i.restaurant_license = r.license_number
    WHERE i.inspection_date > CURRENT_DATE - INTERVAL '5 years'
      AND ({facility_filter})
      AND i.result != 'Out of Business'
    """
    df = read_sql_clean(query)
    df = df[df['violations'].notna() & df['violations'].str.strip().ne('')]
    df['violation_codes'] = df['violations'].apply(extract_codes)
    df['violation_category'] = df['violation_codes'].apply(map_categories)
    df = df[df['violation_category'].notna() & df['violation_category'].ne('')]
    df_expanded = (
        df[['id', 'restaurant_license', 'inspection_date', 'result',
            'dba_name', 'address', 'zip', 'violation_category']]
        .assign(violation_category=lambda d: d['violation_category'].str.split(', '))
        .explode('violation_category')
        .dropna(subset=['violation_category'])
    )
    df_expanded['category_violation_count'] = (
        df_expanded.groupby(['id', 'violation_category'])[
            'violation_category'].transform('count')
    )
    df_expanded['inspection_date'] = pd.to_datetime(
        df_expanded['inspection_date']).dt.strftime('%Y-%m-%d')
    df_expanded.to_csv(os.path.join(
        OUTPUT_DIR, 'inspection_categories.csv'), index=False)
    print(f"Inspection categories: {len(df_expanded):,} rows")


def export_restaurants(facility_filter):
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
    df = read_sql_clean(query)
    df.to_csv(os.path.join(OUTPUT_DIR, 'restaurants.csv'), index=False)
    print(f"Restaurants: {len(df):,} rows")


def export_google_ratings(facility_filter):
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
    df = read_sql_clean(query)
    df.to_csv(os.path.join(OUTPUT_DIR, 'google_ratings.csv'), index=False)
    print(f"Google Ratings: {len(df):,} rows")


if __name__ == "__main__":
    facility_filter = build_facility_filter()
    export_inspections(facility_filter)
    export_inspection_categories(facility_filter)
    export_restaurants(facility_filter)
    export_google_ratings(facility_filter)
    print("Export complete!")
