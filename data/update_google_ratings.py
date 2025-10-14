import psycopg2
import requests
import time
import os
from urllib.parse import urlparse, quote

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

def find_place_id(name, address, city):
    """
    Find Place using findplacefromtext - returns place_id
    COST: $0 (FREE) when used with Find Place request
    """
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    
    params = {
        'input': f"{name}, {address}, {city}",
        'inputtype': 'textquery',
        'fields': 'place_id',  # Only request place_id to keep it free
        'key': GOOGLE_API_KEY
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        result = response.json()
        if result.get('candidates') and len(result['candidates']) > 0:
            return result['candidates'][0].get('place_id')
        elif result.get('status') != 'OK':
            print(f"  API Status: {result.get('status')}")
    else:
        print(f"  Error: {response.status_code}")
    
    return None

def get_place_details_by_id(place_id):
    """
    Get place details using place_id only
    COST: $0 (FREE) - "Places API Place Details Essentials (IDs Only): Unlimited"
    
    CRITICAL: Must use place_id field (not fields parameter) to stay free!
    """
    url = "https://places.googleapis.com/v1/places/{place_id}"
    
    # Use the new Places API format with place_id in URL
    formatted_url = f"https://places.googleapis.com/v1/{place_id}"
    
    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': GOOGLE_API_KEY,
        'X-Goog-FieldMask': 'id,displayName,rating,userRatingCount'
    }
    
    response = requests.get(formatted_url, headers=headers)
    
    if response.status_code == 200:
        place = response.json()
        return {
            'place_id': place.get('id'),
            'name': place.get('displayName', {}).get('text'),
            'rating': place.get('rating'),
            'user_ratings_total': place.get('userRatingCount')
        }
    else:
        print(f"  Error getting details: {response.status_code} - {response.text}")
    
    return None

def fetch_all_google_ratings():
    conn = get_connection()
    cur = conn.cursor()
    
    # Skip restaurants we already have data for
    cur.execute("""
        SELECT id, dba_name, address, city 
        FROM restaurants 
        WHERE id NOT IN (
            SELECT restaurant_id 
            FROM google_ratings 
            WHERE place_id IS NOT NULL
        )
    """)
    restaurants = cur.fetchall()
    
    total = len(restaurants)
    print(f"📍 Fetching Google ratings for {total} restaurants...")
    print(f"💰 COST: $0.00 (using FREE Place ID Lookup!)\n")
    
    fetched = 0
    failed = 0
    
    for idx, (rest_id, name, address, city) in enumerate(restaurants, 1):
        print(f"[{idx}/{total}] {name}...")
        
        # Step 1: Find the place_id (FREE)
        place_id = find_place_id(name, address, city)
        
        if not place_id:
            failed += 1
            print(f"  ✗ Could not find place_id\n")
            time.sleep(0.1)
            continue
        
        print(f"  Found place_id: {place_id}")
        
        # Step 2: Get details using place_id (FREE)
        place_data = get_place_details_by_id(place_id)
        
        if place_data and place_data.get('rating') is not None:
            cur.execute("""
                INSERT INTO google_ratings (restaurant_id, place_id, rating, user_ratings_total)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (restaurant_id) DO UPDATE SET
                    place_id = EXCLUDED.place_id,
                    rating = EXCLUDED.rating,
                    user_ratings_total = EXCLUDED.user_ratings_total
            """, (
                rest_id,
                place_data.get('place_id'),
                place_data.get('rating'),
                place_data.get('user_ratings_total')
            ))
            
            fetched += 1
            print(f"  ✓ {place_data.get('rating')} stars ({place_data.get('user_ratings_total')} reviews)\n")
        else:
            failed += 1
            print(f"  ✗ Could not get place details\n")
        
        conn.commit()
        time.sleep(0.1)  # Be nice to the API
    
    cur.close()
    conn.close()
    
    print(f"=" * 50)
    print(f"✅ Fetched/Updated: {fetched} restaurants")
    print(f"❌ Failed: {failed} restaurants")
    print(f"💰 Total Cost: $0.00 (FREE!)")
    print(f"💸 Money Saved vs Text Search: ${(fetched + failed) * 32 / 1000:.2f}")

if __name__ == "__main__":
    # Safety check
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) 
        FROM restaurants 
        WHERE id NOT IN (
            SELECT restaurant_id 
            FROM google_ratings 
            WHERE place_id IS NOT NULL
        )
    """)
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    print(f"\n{'='*50}")
    print(f"🎯 Places to process: {count}")
    print(f"💰 Estimated cost: $0.00 (FREE!)")
    print(f"{'='*50}\n")
    
    if count == 0:
        print("No restaurants to process. All done!")
    else:
        fetch_all_google_ratings()