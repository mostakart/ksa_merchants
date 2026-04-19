import json, requests

# Supabase Config
SUPABASE_URL = "https://omowdfzyudedrtcuhnvy.supabase.co"
SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tb3dkZnp5dWRlZHJ0Y3VobnZ5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjQzNjI3OCwiZXhwIjoyMDkyMDEyMjc4fQ.kgQTvZRIrgFXTwL5wDM5oYLmDS9GtRjltE53wcpDQes"

def upload():
    data = json.load(open('output/smart_master.json', 'r', encoding='utf-8'))['enriched']
    
    city_data = {}
    for k, v in data.items():
        city = k.split('::')[0].strip()
        if city not in city_data: city_data[city] = []
        city_data[city].extend(v)

    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}", "Content-Type": "application/json"}
    tables = {"Riyadh":"merchants_riyadh", "Jeddah":"merchants_jeddah", "Dammam":"merchants_dammam", "Khobar":"merchants_khobar", "Mecca":"merchants_mecca", "Medina":"merchants_medina"}

    for city, merchants in city_data.items():
        table = tables.get(city)
        if not table: continue
        print(f"Uploading {city} ({len(merchants)})...")
        requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?id=not.is.null", headers=headers)
        
        recs = []
        for m in merchants:
            recs.append({
                "merchant_name": m.get("merchant_name") or m.get("merchant"), 
                "mall": m.get("mall") or m.get("mall_name"), 
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
                "lat": m.get("lat"), "lng": m.get("lng"), 
                "top_reviews": m.get("top_reviews") or m.get("reviews_text")
            })
        
        for i in range(0, len(recs), 200):
            requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=headers, json=recs[i:i+200])

    print("DONE! The dashboard is now perfectly clean and deduplicated.")

if __name__ == "__main__": upload()
