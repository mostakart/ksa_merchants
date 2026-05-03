import os
import json
import httpx
import asyncio
from dotenv import load_dotenv

# Load configuration
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

async def import_merchants():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] Missing Supabase credentials in .env")
        return

    json_path = "scraper/discovered_merchants.json"
    if not os.path.exists(json_path):
        print(f"[!] No discovery file found at {json_path}. Run waffarha_discovery.py first.")
        return

    with open(json_path, "r", encoding="utf-8") as f:
        discovered = json.load(f)

    if not discovered:
        print("[-] No new merchants to import.")
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    async with httpx.AsyncClient() as client:
        # 1. Get existing merchants to avoid duplicates
        existing_names = set()
        try:
            r = await client.get(f"{SUPABASE_URL}/rest/v1/tracked_merchants?select=name", headers=headers)
            if r.status_code == 200:
                existing_names = {m["name"].lower() for m in r.json()}
                print(f"[*] Found {len(existing_names)} existing merchants in database.")
        except Exception as e:
            print(f"[!] Warning: Could not fetch existing merchants: {e}")

        # 2. Filter discovered list
        rows = []
        for name, category in discovered.items():
            if name.lower() in existing_names:
                continue
            rows.append({
                "name": name,
                "category": category,
                "is_active": True,
                "last_updated": None
            })

        if not rows:
            print("[-] No new merchants to import (all already exist).")
            return

        print(f"[*] Importing {len(rows)} new merchants...")

        # 3. Chunking inserts
        chunk_size = 50
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i+chunk_size]
            try:
                r = await client.post(f"{SUPABASE_URL}/rest/v1/tracked_merchants", headers=headers, json=chunk)
                if r.status_code in (200, 201, 204):
                    print(f"[+] Imported chunk {i//chunk_size + 1} ({len(chunk)} merchants).")
                else:
                    print(f"[!] Failed chunk {i//chunk_size + 1}: {r.status_code} - {r.text}")
            except Exception as e:
                print(f"[!] Error in chunk {i//chunk_size + 1}: {e}")

    print("\n" + "="*50)
    print(f"DONE: {len(rows)} new merchants added.")
    print("="*50)

if __name__ == "__main__":
    asyncio.run(import_merchants())
