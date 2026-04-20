import json
import requests
from pathlib import Path

# Supabase Config
SUPABASE_URL = "https://omowdfzyudedrtcuhnvy.supabase.co"
SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tb3dkZnp5dWRlZHJ0Y3VobnZ5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjQzNjI3OCwiZXhwIjoyMDkyMDEyMjc4fQ.kgQTvZRIrgFXTwL5wDM5oYLmDS9GtRjltE53wcpDQes"

def wipe_and_upload():
    json_path = Path('output/ksa_merchants_supabase.json')
    if not json_path.exists():
        print(f"File not found: {json_path}")
        return

    print(f"Loading data from {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        merchants_list = json.load(f)

    # Group by city
    city_data = {}
    for m in merchants_list:
        city = m.get('city', 'Other').strip()
        if city not in city_data:
            city_data[city] = []
        city_data[city].append(m)

    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json"
    }

    tables = {
        "Riyadh": "merchants_riyadh",
        "Jeddah": "merchants_jeddah",
        "Dammam": "merchants_dammam",
        "Khobar": "merchants_khobar",
        "Mecca": "merchants_mecca",
        "Medina": "merchants_medina"
    }

    print("\nStarting Wipe and Upload process...")

    for city, merchants in city_data.items():
        table = tables.get(city)
        if not table:
            print(f"Skipping city '{city}': No matching table found.")
            continue

        print(f"\nProcessing {city} ({len(merchants)} records)...")
        
        # 1. WIPE
        print(f"   Wiping table {table}...")
        del_resp = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?id=not.is.null", headers=headers)
        if del_resp.status_code not in [200, 204]:
            print(f"   Failed to wipe {table}: {del_resp.status_code} {del_resp.text}")
            continue
        
        # 2. MAP & UPLOAD
        recs = []
        for m in merchants:
            recs.append({
                "merchant_name": m.get("merchant_name_ar") or m.get("merchant_name_en") or m.get("merchant_name_raw"),
                "mall": m.get("mall_name"),
                "city": m.get("city"),
                "priority": "Medium", # Default since not in file
                "category": m.get("category"),
                "sub_category": m.get("sub_category"),
                "rating": m.get("rating"),
                "reviews_count": m.get("total_reviews"),
                "avg_price": m.get("avg_price"),
                "branches_ksa": 1, # Default
                "phone": m.get("phone"),
                "website": m.get("website"),
                "opening_hours": m.get("opening_hours"),
                "lat": m.get("lat"),
                "lng": m.get("lng"),
                "top_reviews": m.get("reviews_text")
            })

        print(f"   Uploading {len(recs)} records in batches...")
        for i in range(0, len(recs), 200):
            batch = recs[i:i+200]
            post_resp = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=headers, json=batch)
            if post_resp.status_code not in [200, 201]:
                print(f"   Batch upload failed: {post_resp.status_code} {post_resp.text}")
                break
        else:
            print(f"   {city} upload complete.")

    print("\nALL DONE! Supabase has been wiped and re-synced with ksa_merchants_supabase.json")

if __name__ == "__main__":
    wipe_and_upload()
