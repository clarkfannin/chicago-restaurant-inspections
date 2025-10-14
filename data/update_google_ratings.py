import psycopg2
import requests
import time
import os
from urllib.parse import urlparse

# Read Google API key from environment
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise RuntimeError("GOOGLE_API_KEY environment variable is not set")

# Read DB URL from environment
db_url = os.environ.get("SUPABASE_DB_URL")
if not db_url:
    raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

parsed_url = urlparse(db_url)

def get_connection():
    return psycopg2.connect(
        dbname=parsed_url.path[1:],
        user=parsed_url.username,
        password=parsed_url.password,
        host=parsed_url.hostname,
        port=parsed_url.port
    )

def search_place_new_api(name, address, city):
    url = "https://places.googleapis.com/v1/places:searchText"

    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': GOOGLE_API_KEY,
        'X-Goog-FieldMask': 'places.id,places.displayName,places.rating,places.userRatingCount'
    }

    data = {'textQuery': f"{name} {address} {city}"}

    response = requests.post(url, json=data, headers=headers)

    if response.status_code == 200:
        result = response.json()
        if result.get('places'):
            place = result['places'][0]
            return {
                'place_id': place.get('id'),
                'name': place.get('displayName', {}).get('text'),
                'rating': place.get('rating'),
                'user_ratings_total': place.get('userRatingCount')
            }
    else:
        print(f"Error: {response.status_code} - {response.text}")

    return None

def fetch_all_google_ratings():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, dba_name, address, city FROM restaurants")
    restaurants = cur.fetchall()
    print(f"Fetching Google ratings for {len(restaurants)} restaurants...")

    fetched = 0
    failed = 0

    for rest_id, name, address, city in restaurants:
        place_data = search_place_new_api(name, address, city)

        if place_data and place_data.get('rating') is not None:
            cur.execute("""
                INSERT INTO google_ratings (restaurant_id, place_id, rating, user_ratings_total)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (restaurant_id) DO UPDATE SET
                    place_id = EXCLUDED.place_id,
                    rating = EXCLUDED.rating,
                    user_ratings_total = EXCLUDED.user_ratings_total;
            """, (
                rest_id,
                place_data.get('place_id'),
                place_data.get('rating'),
                place_data.get('user_ratings_total')
            ))

            fetched += 1
            print(f"✓ {name}: {place_data.get('rating')} stars ({place_data.get('user_ratings_total')} reviews)")
        else:
            failed += 1
            print(f"✗ {name}: Not found on Google")

        conn.commit()
        time.sleep(0.2)  # rate limiting

    cur.close()
    conn.close()

    print(f"Fetched/Updated {fetched} Google ratings")
    print(f"Failed to find {failed} restaurants")

if __name__ == "__main__":
    fetch_all_google_ratings()
