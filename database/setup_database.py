import psycopg2

conn = psycopg2.connect(
    dbname="chicago_inspections",
    user="clarkfannin",
    host="localhost"
)
cur = conn.cursor()

cur.execute("""
            CREATE TABLE IF NOT EXISTS restaurants (
        id SERIAL PRIMARY KEY,
        license_number BIGINT UNIQUE,
        dba_name VARCHAR(255),
        aka_name VARCHAR(255),
        facility_type VARCHAR(100),
        address VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(10),
        zip INTEGER,
        latitude FLOAT,
        longitude FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
            """)

cur.execute("""
    CREATE TABLE IF NOT EXISTS inspections (
        id SERIAL PRIMARY KEY,
        inspection_id BIGINT UNIQUE,
        restaurant_license BIGINT REFERENCES restaurants(license_number),
        inspection_date DATE,
        inspection_type VARCHAR(100),
        result VARCHAR(50),
        risk VARCHAR(50),
        violations TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")

conn.commit()
cur.close()
conn.close()
print("Database setup complete!")