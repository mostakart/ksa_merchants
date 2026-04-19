import json
import pandas as pd
from pathlib import Path
import os, sys, io, requests

# Supabase Config
SUPABASE_URL = "https://omowdfzyudedrtcuhnvy.supabase.co"
SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tb3dkZnp5dWRlZHJ0Y3VobnZ5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjQzNjI3OCwiZXhwIjoyMDkyMDEyMjc4fQ.kgQTvZRIrgFXTwL5wDM5oYLmDS9GtRjltE53wcpDQes"

def main():
    if sys.platform == "win32":
        os.system('chcp 65001 > nul')
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

    json_path = 'output/master_checkpoint.json'
    if not os.path.exists(json_path):
        print("Checkpoint missing!")
        return

    print(f"Loading {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    enriched = data.get('enriched', {})
    
    # 1. Group by city
    city_data = {}
    for mall_key, merchants in enriched.items():
        city = mall_key.split('::')[0] if '::' in mall_key else 'Riyadh'
        if city not in city_data: city_data[city] = []
        city_data[city].extend(merchants)

    # 2. Export each city to Excel
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    for city, merchants in city_data.items():
        if not merchants: continue
        
        df = pd.DataFrame(merchants)
        # Map columns to match Supabase/Dashboard expected format
        col_map = {
            "merchant": "Merchant",
            "mall": "Mall",
            "city": "City",
            "category": "Category",
            "rating": "Rating",
            "reviews": "Reviews",
            "avg_price": "Avg Price",
            "branches": "Branches (KSA)",
            "phone": "Phone",
            "website": "Website",
            "opening_hours": "Opening Hours",
            "lat": "Lat",
            "lng": "Lng",
            "top_reviews": "Top Reviews",
            "priority": "Priority"
        }
        
        # Ensure all columns exist
        for k in col_map.keys():
            if k not in df.columns:
                df[k] = ""

        df = df.rename(columns=col_map)
        
        # Keep only relevant columns
        cols_to_keep = list(col_map.values())
        df = df[cols_to_keep]

        out_path = output_dir / f"KSA_Merchants_{city}_Updated.xlsx"
        print(f"Saving {out_path} ({len(df)} rows)...")
        
        # Split into sheets by Mall for better UX
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            malls = df['Mall'].unique()
            if len(malls) == 0:
                df.to_excel(writer, sheet_name="Sheet1", index=False)
            else:
                for mall in malls:
                    mall_name = str(mall) if mall else "Unknown"
                    mall_df = df[df['Mall'] == mall]
                    sheet_name = mall_name[:31].replace(':', '').replace('/', '').replace('[', '').replace(']', '')
                    if not sheet_name.strip(): sheet_name = "Mall"
                    mall_df.to_excel(writer, sheet_name=sheet_name, index=False)

    # 3. Supabase Upload (Using the logic from your uploader)
    print("\nStarting Supabase Upload...")
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    
    supabase_map = {
        "Riyadh": "merchants_riyadh",
        "Jeddah": "merchants_jeddah",
        "Dammam": "merchants_dammam",
        "Khobar": "merchants_khobar",
        "Mecca": "merchants_mecca",
        "Medina": "merchants_medina"
    }

    for city, merchants in city_data.items():
        table = supabase_map.get(city)
        if not table: continue
        
        print(f"  Uploading {city} ({len(merchants)} rows) to {table}...")
        
        # Truncate
        requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?id=not.is.null", headers=headers)
        
        # Prepare records
        records = []
        for m in merchants:
            rec = {
                "merchant_name": m.get("merchant"),
                "mall": m.get("mall"),
                "city": m.get("city"),
                "priority": m.get("priority"),
                "category": m.get("category"),
                "rating": m.get("rating"),
                "reviews_count": m.get("reviews"),
                "avg_price": m.get("avg_price"),
                "branches_ksa": m.get("branches"),
                "phone": m.get("phone"),
                "website": m.get("website"),
                "opening_hours": m.get("opening_hours"),
                "lat": float(m.get("lat")) if m.get("lat") else None,
                "lng": float(m.get("lng")) if m.get("lng") else None,
                "top_reviews": m.get("top_reviews")
            }
            records.append(rec)
        
        # Upload in batches
        batch_size = 200
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=headers, json=batch)
            print(f"    Uploaded {i + len(batch)}/{len(records)}...", end="\r")
        print(f"\n  ✅ {city} Done.")

    print("\n🚀 ALL DONE! Check the dashboard.")

if __name__ == "__main__":
    main()
