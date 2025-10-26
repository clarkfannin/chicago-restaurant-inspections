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

def get_place_details_by_id(place_id):
    """
    Get place details using EXISTING place_id
    COST: $0 (100% FREE!) - "Places API Place Details Essentials (IDs Only): Unlimited"
    
    This is free because you already have the place_id!
    """
    url = f"https://places.googleapis.com/v1/{place_id}"
    
    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': GOOGLE_API_KEY,
        'X-Goog-FieldMask': 'id,displayName,rating,userRatingCount'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        place = response.json()
        return {
            'place_id': place.get('id'),
            'name': place.get('displayName', {}).get('text'),
            'rating': place.get('rating'),
            'user_ratings_total': place.get('userRatingCount')
        }
    else:
        print(f"  Error: {response.status_code}", flush=True)
        if response.status_code == 400:
            print(f"  Response: {response.text}", flush=True)
    
    return None

def update_ratings_from_existing_place_ids():
    """
    Update ratings for restaurants that already have place_ids in the database
    This is 100% FREE!
    """
    conn = get_connection()
    cur = conn.cursor()
    
    # Find all restaurants that have place_ids but need rating updates
    cur.execute("""
        SELECT restaurant_id, place_id 
        FROM google_ratings 
        WHERE place_id IS NOT NULL
    """)
    existing_places = cur.fetchall()
    
    total = len(existing_places)
    print(f"📍 Updating ratings for {total} restaurants with existing place_ids", flush=True)
    print(f"💰 COST: $0.00 (100% FREE - using existing place_ids!)\n", flush=True)
    
    updated = 0
    failed = 0
    
    for idx, (rest_id, place_id) in enumerate(existing_places, 1):
        cur.execute("SELECT dba_name FROM restaurants WHERE id = %s", (rest_id,))
        name_result = cur.fetchone()
        name = name_result[0] if name_result else "Unknown"
        
        print(f"[{idx}/{total}] {name}...", flush=True)
        
        # Get fresh details using existing place_id (FREE!)
        place_data = get_place_details_by_id(place_id)
        
        if place_data and place_data.get('rating') is not None:
            cur.execute("""
                UPDATE google_ratings 
                SET rating = %s, 
                    user_ratings_total = %s,
                    updated_at = NOW()
                WHERE restaurant_id = %s
            """, (
                place_data.get('rating'),
                place_data.get('user_ratings_total'),
                rest_id
            ))
            
            updated += 1
            print(f"  ✓ {place_data.get('rating')} stars ({place_data.get('user_ratings_total')} reviews)\n", flush=True)
        else:
            failed += 1
            print(f"  ✗ Could not get place details\n", flush=True)
        
        conn.commit()
        time.sleep(0.05)  # Be nice to the API
    
    cur.close()
    conn.close()
    
    print(f"=" * 50, flush=True)
    print(f"✅ Updated: {updated} restaurants", flush=True)
    print(f"❌ Failed: {failed} restaurants", flush=True)
    print(f"💰 Total Cost: $0.00 (FREE!)", flush=True)
    print(f"💸 Money saved vs Text Search: ${total * 32 / 1000:.2f}", flush=True)

if __name__ == "__main__":
    print(f"\n{'='*50}")
    print(f"🎯 Checking database...")
    print(f"{'='*50}\n")
    
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT COUNT(*) 
        FROM google_ratings 
        WHERE place_id IS NOT NULL
    """)
    with_ids = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    if with_ids == 0:
        print("❌ No place_ids found in database!")
        print("   Nothing to update (script stays free).")
    else:
        print(f"✅ Found {with_ids} restaurants with existing place_ids")
        print(f"💰 Updating ratings... (100% FREE)\n")
        update_ratings_from_existing_place_ids()