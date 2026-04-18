import json, requests, time, sys, os

def load_env(path=".env"):
    if not os.path.exists(path): return
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

load_env()

SUPABASE_URL = "https://omowdfzyudedrtcuhnvy.supabase.co"
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY", "")

if not SERVICE_KEY:
    print("❌  Add SUPABASE_SERVICE_KEY to .env"); sys.exit(1)

print(f"✅  Key: {SERVICE_KEY[:20]}…")

HEADERS = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}", "Content-Type": "application/json", "Prefer": "return=minimal"}
CITY_TABLE = {"Riyadh":"merchants_riyadh","Jeddah":"merchants_jeddah","Dammam":"merchants_dammam","Khobar":"merchants_khobar","Mecca":"merchants_mecca","Medina":"merchants_medina"}

def clear(table):
    r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}?id=not.is.null", headers=HEADERS)
    print(f"  🗑️  {table} ({r.status_code})")

def upload(records, table, batch=300):
    ok = fail = 0
    for i in range(0, len(records), batch):
        b = records[i:i+batch]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=b, timeout=60)
        if r.status_code in (200,201): ok += len(b)
        else: fail += len(b); print(f"\n  ❌ {r.text[:200]}")
        print(f"  ✅ {ok}/{len(records)}", end="\r")
        time.sleep(0.1)
    print(f"\n  {ok} ok | {fail} failed")

def norm(m, city, mall):
    def safe_str(v):
        if v is None: return None
        s = str(v).strip()
        return None if s in ("None","nan","null","") else s

    def safe_int(v):
        try:
            s = str(v).strip()
            if s in ("","None","nan","null"): return None
            return int(float(s.replace(",","")))
        except: return None

    def safe_num(v):
        try:
            s = str(v).strip()
            if s in ("","None","nan","null"): return None
            return float(s)
        except: return None

    return {
        "merchant_name": safe_str(m.get("merchant")),
        "mall":          str(mall).split("::")[-1].strip(),
        "city":          city,
        "category":      safe_str(m.get("category")),
        "priority":      safe_str(m.get("priority")),
        "rating":        safe_str(m.get("rating")),
        "reviews_count": safe_int(m.get("total_reviews")),
        "avg_price":     safe_str(m.get("avg_price")),
        "branches_ksa":  safe_int(m.get("branches")),
        "phone":         safe_str(m.get("phone")),
        "website":       safe_str(m.get("website")),
        "opening_hours": safe_str(m.get("opening_hours")),
        "lat":           safe_num(m.get("lat")),
        "lng":           safe_num(m.get("lng")),
        "top_reviews":   safe_str(m.get("reviews_text")),
    }

print("Loading…")
with open("output/master_checkpoint.json") as f:
    data = json.load(f)
enriched = data["enriched"]

city_records = {c:[] for c in CITY_TABLE}
for key, merchants in enriched.items():
    city = key.split("::")[0]
    if city in city_records:
        for m in merchants:
            city_records[city].append(norm(m, city, key))

print("\n🗑️  Clearing…")
for t in CITY_TABLE.values(): clear(t)

total = 0
for city, table in CITY_TABLE.items():
    recs = city_records[city]
    print(f"\n📂 {city} ({len(recs):,})")
    upload(recs, table)
    total += len(recs)

print(f"\n✅ Total: {total:,}")