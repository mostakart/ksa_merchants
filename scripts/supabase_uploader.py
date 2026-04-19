#!/usr/bin/env python3
"""
KSA Merchants → Supabase Uploader
===================================
Reads 6 Excel files and uploads to corresponding Supabase tables.

Usage:
  1. pip install pandas requests openpyxl
  2. Set SERVICE_KEY below (Settings → API → service_role)
  3. python supabase_uploader.py
"""

import pandas as pd
import requests
import json
import time
import sys
from pathlib import Path

# ═══════════════════════════════════════════════════
# CONFIG — Edit before running
# ═══════════════════════════════════════════════════
SUPABASE_URL = "https://omowdfzyudedrtcuhnvy.supabase.co"

# Use SERVICE ROLE key — NOT anon key
# Location: Supabase Dashboard → Settings → API → service_role (secret)
SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tb3dkZnp5dWRlZHJ0Y3VobnZ5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjQzNjI3OCwiZXhwIjoyMDkyMDEyMjc4fQ.kgQTvZRIrgFXTwL5wDM5oYLmDS9GtRjltE53wcpDQes"

BATCH_SIZE = 200  # Records per HTTP request

# Excel filename → Supabase table
FILES = {
    "KSA_Merchants_Riyadh.xlsx": "merchants_riyadh",
    "KSA_Merchants_Jeddah.xlsx": "merchants_jeddah",
    "KSA_Merchants_Dammam.xlsx": "merchants_dammam",
    "KSA_Merchants_Khobar.xlsx": "merchants_khobar",
    "KSA_Merchants_Mecca.xlsx":  "merchants_mecca",
    "KSA_Merchants_Medina.xlsx": "merchants_medina",
}

# Excel column → Supabase column
COL_MAP = {
    "Merchant":        "merchant_name",
    "Mall":            "mall",
    "City":            "city",
    "Priority":        "priority",
    "Category":        "category",
    "Rating":          "rating",
    "Reviews":         "reviews_count",
    "Avg Price":       "avg_price",
    "Branches (KSA)": "branches_ksa",
    "Phone":           "phone",
    "Website":         "website",
    "Opening Hours":   "opening_hours",
    "Lat":             "lat",
    "Lng":             "lng",
    "Top Reviews":     "top_reviews",
}

# ═══════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════

def make_headers():
    return {
        "apikey":        SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }


def clean(val, col):
    """Convert a cell value to a JSON-safe Python type."""
    # Handle pandas NaN / None / empty string
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass

    if val is None or val == "":
        return None

    # Float geo columns
    if col in ("lat", "lng"):
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    # Integer columns
    if col in ("reviews_count", "branches_ksa"):
        try:
            return int(float(str(val).replace(",", "").strip()))
        except (ValueError, TypeError):
            return None

    # Everything else → clean string
    return str(val).strip()


def df_to_records(df):
    """Convert DataFrame to list of clean dicts for Supabase."""
    records = []
    supabase_cols = list(COL_MAP.values())

    for _, row in df.iterrows():
        record = {}
        for col in supabase_cols:
            if col in df.columns:
                record[col] = clean(row[col], col)
        records.append(record)
    return records


def truncate_table(table):
    """Delete all rows to allow idempotent re-uploads."""
    # Delete rows where id is not null (i.e. all rows)
    url = f"{SUPABASE_URL}/rest/v1/{table}?id=not.is.null"
    r = requests.delete(url, headers=make_headers(), timeout=30)
    if r.status_code not in (200, 204):
        print(f"  ⚠️  Truncate warning {r.status_code}: {r.text[:150]}")
    else:
        print(f"  🗑️  Table cleared.")


def upload_batch(records, table):
    """POST a batch of records to Supabase. Returns (ok: bool, response)."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(
        url,
        headers=make_headers(),
        data=json.dumps(records, ensure_ascii=False).encode("utf-8"),
        timeout=60
    )
    return r.status_code in (200, 201), r


def process(filepath, table):
    """Read Excel and upload to Supabase in batches."""
    print(f"\n{'─' * 55}")
    print(f"  📂  {filepath}  →  {table}")

    # Read all columns as string to avoid type coercion issues
    df = pd.read_excel(filepath, dtype=str)
    print(f"  Rows loaded : {len(df)}")
    print(f"  Columns     : {list(df.columns)}")

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Rename to Supabase column names
    df = df.rename(columns=COL_MAP)

    # Keep only mapped columns that exist
    keep = [c for c in COL_MAP.values() if c in df.columns]
    skip = [c for c in COL_MAP.values() if c not in df.columns]
    if skip:
        print(f"  ⚠️  Skipped (not in Excel): {skip}")

    df = df[keep].copy()
    records = df_to_records(df)

    # Clear existing data first
    truncate_table(table)

    success = failed = 0
    total = len(records)

    for i in range(0, total, BATCH_SIZE):
        batch = records[i: i + BATCH_SIZE]
        ok, resp = upload_batch(batch, table)

        if ok:
            success += len(batch)
            pct = int(success / total * 100)
            print(f"  ✅  [{pct:3d}%] {success}/{total} rows uploaded…", end="\r")
        else:
            failed += len(batch)
            print(f"\n  ❌  Batch {i // BATCH_SIZE + 1} failed ({resp.status_code}):")
            print(f"      {resp.text[:400]}")

        time.sleep(0.15)  # Avoid rate limiting

    print(f"\n  📊  Done: ✅ {success} uploaded  |  ❌ {failed} failed")
    return success, failed


# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════

def main():
    print("╔══════════════════════════════════════════════════╗")
    print("║   KSA Merchants — Supabase Uploader              ║")
    print(f"║   Project : {SUPABASE_URL[8:45]:37s}║")
    print("╚══════════════════════════════════════════════════╝\n")

    # Guard: make sure user set the key
    if "YOUR_SERVICE_ROLE_KEY_HERE" in SERVICE_KEY:
        print("❌  SERVICE_KEY is not set!")
        print("    Edit this file and paste your service_role key.")
        print("    Location: Supabase → Settings → API → service_role (secret)")
        sys.exit(1)

    # Find available Excel files (check root and output/ dir)
    output_dir = Path("output")
    found = []
    missing = []
    
    for f, t in FILES.items():
        if Path(f).exists():
            found.append((f, t))
        elif (output_dir / f).exists():
            found.append((str(output_dir / f), t))
        else:
            missing.append(f)

    if missing:
        print(f"⚠️  Files not found (will skip):")
        for f in missing:
            print(f"    • {f}")

    if not found:
        print("\n❌  No Excel files found!")
        print("    Make sure the .xlsx files are in the same folder as this script.")
        sys.exit(1)

    print(f"🚀  Uploading {len(found)} file(s)…")

    total_success = total_failed = 0
    for filepath, table in found:
        s, f = process(filepath, table)
        total_success += s
        total_failed  += f

    print(f"\n{'═' * 55}")
    print(f"✅  Upload complete!")
    print(f"   Total uploaded : {total_success:,}")
    print(f"   Total failed   : {total_failed:,}")
    print("═" * 55)


if __name__ == "__main__":
    main()
