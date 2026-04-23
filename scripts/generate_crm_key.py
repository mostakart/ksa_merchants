#!/usr/bin/env python3
import secrets
import requests
import sys

SUPABASE_URL = "https://omowdfzyudedrtcuhnvy.supabase.co"
SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tb3dkZnp5dWRlZHJ0Y3VobnZ5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjQzNjI3OCwiZXhwIjoyMDkyMDEyMjc4fQ.kgQTvZRIrgFXTwL5wDM5oYLmDS9GtRjltE53wcpDQes"

def generate_key(name, email=None):
    # Generate a secure random key
    api_key = f"wfr_crm_{secrets.token_urlsafe(32)}"
    
    # Register in Supabase
    url = f"{SUPABASE_URL}/rest/v1/crm_api_keys"
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    
    payload = {
        "key_name": name,
        "key_hash": api_key, # Storing directly for simplicity, in production use hashing
        "owner_email": email,
        "is_active": True
    }
    
    r = requests.post(url, headers=headers, json=payload)
    
    if r.status_code in (200, 201):
        print(f"✅ Successfully generated key for: {name}")
        print(f"🔑 API KEY: {api_key}")
        print("⚠️  Save this key! It will not be shown again.")
    else:
        print(f"❌ Failed to register key: {r.status_code}")
        print(r.text)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 generate_crm_key.py <key_name> [owner_email]")
        sys.exit(1)
    
    name = sys.argv[1]
    email = sys.argv[2] if len(sys.argv) > 2 else None
    generate_key(name, email)
