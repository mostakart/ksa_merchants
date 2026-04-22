#!/usr/bin/env python3
"""
Waffarha CRM Portal — Zoho Ticket Uploader
===========================================
Reads Cases__1.csv and uploads 50k tickets to Supabase `zoho_tickets` table.

Prerequisites:
  1. Run create_tickets_table.sql in Supabase SQL Editor first
  2. pip install requests
  3. Set SUPABASE_SERVICE_KEY in .env (or edit below)

Usage:
  python3 upload_tickets.py --csv /path/to/Cases__1.csv
  python3 upload_tickets.py --csv /path/to/Cases__1.csv --resume   # skip already-uploaded IDs
"""

import csv
import json
import os
import sys
import time
import argparse
import math
from pathlib import Path
from datetime import datetime

# ─── Load .env ────────────────────────────────────────────────
def load_env(path=".env"):
    if not os.path.exists(path):
        return
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

load_env()
load_env(str(Path(__file__).parent.parent / ".env"))

# ─── Config ───────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://omowdfzyudedrtcuhnvy.supabase.co")
SERVICE_KEY  = os.environ.get("SUPABASE_KEY", "")
TABLE        = "zoho_tickets"
BATCH_SIZE   = 500

if not SERVICE_KEY:
    print("❌  SUPABASE_KEY not set. Add it to .env or export it.")
    sys.exit(1)

HEADERS = {
    "apikey":        SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

# ─── Safe type helpers ────────────────────────────────────────
def safe_str(v):
    if v is None:
        return None
    s = str(v).strip()
    return None if s.lower() in ("none", "nan", "null", "") else s

def safe_int(v):
    try:
        s = str(v).strip()
        if s.lower() in ("", "none", "nan", "null"):
            return None
        return int(float(s.replace(",", "")))
    except Exception:
        return None

def safe_bigint(v):
    try:
        s = str(v).strip()
        if s.lower() in ("", "none", "nan", "null"):
            return None
        val = int(float(s))
        return val if val > 0 else None
    except Exception:
        return None

def safe_bool(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None

def safe_ts(v):
    if not v or str(v).strip().lower() in ("none", "nan", "null", ""):
        return None
    s = str(v).strip()
    # Zoho format: "2025-12-03 20:35:48"
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.isoformat() + "Z"
    except ValueError:
        return None

def clean_subject(v):
    if not v:
        return None
    import re
    s = re.sub(r"<[^>]+>", " ", str(v)).strip()
    s = re.sub(r"\s+", " ", s)
    return s[:500] if s else None

# ─── Row mapper ───────────────────────────────────────────────
def map_row(row):
    return {
        "ticket_id":               safe_str(row.get("ID")),
        "subject":                 clean_subject(row.get("Subject")),
        "status":                  safe_str(row.get("Status")),
        "channel":                 safe_str(row.get("Channel")),
        "priority":                safe_str(row.get("Priority")),
        "reason":                  safe_str(row.get("Reason")),
        "sub_reason":              safe_str(row.get("Sub Reason")),
        "ticket_owner":            safe_str(row.get("Ticket Owner")),
        "created_time":            safe_ts(row.get("Created Time")),
        "closed_time":             safe_ts(row.get("Ticket Closed Time")),
        "happiness_rating":        safe_str(row.get("Happiness Rating")),
        "resolution_time_ms":      safe_bigint(row.get("Resolution Time in Business Hours")),
        "first_response_time_ms":  safe_bigint(row.get("First Response Time in Business Hours")),
        "total_response_time_ms":  safe_bigint(row.get("Total Response Time in Business Hours")),
        "num_threads":             safe_int(row.get("Number of Threads")),
        "num_responses":           safe_int(row.get("Number of Responses")),
        "num_reassign":            safe_int(row.get("Number of Reassign")),
        "num_reopen":              safe_int(row.get("Number of Reopen")),
        "is_overdue":              safe_bool(row.get("Is Overdue")),
        "is_escalated":            safe_bool(row.get("Is Escalated")),
        "escalation_validity":     safe_str(row.get("Escalation Validity")),
        "sla_violation_type":      safe_str(row.get("SLA Violation Type")),
        "sla_name":                safe_str(row.get("SLA Name")),
        "team_id":                 safe_str(row.get("Team Id")),
        "tags":                    safe_str(row.get("Tags")),
        "total_time_spent":        safe_int(row.get("Total Time Spent")),
        "merchant_name":           safe_str(row.get("Merchant Name")),
        "branch_name":             safe_str(row.get("Branch Name")),
        "country":                 safe_str(row.get("Country")),
        "language":                safe_str(row.get("Language")),
        "user_id":                 safe_str(row.get("User_ID")),
        "order_id":                safe_str(row.get("Order_ID")),
        "transaction_id":          safe_str(row.get("Transaction_ID")),
    }

# ─── Supabase helpers ─────────────────────────────────────────
import requests

def upload_batch(records):
    """Upsert a batch. Returns (ok, success_count, error_text)."""
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    h = {**HEADERS, "Prefer": "resolution=ignore-duplicates,return=minimal"}
    r = requests.post(url, headers=h, data=json.dumps(records, ensure_ascii=False, default=str).encode("utf-8"), timeout=60)
    if r.status_code in (200, 201):
        return True, len(records), ""
    return False, 0, r.text[:300]

def get_existing_ids():
    """Fetch all ticket_ids already in Supabase (for resume mode)."""
    print("  Fetching existing ticket IDs from Supabase…")
    ids = set()
    offset = 0
    limit = 1000
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{TABLE}?select=ticket_id&limit={limit}&offset={offset}",
            headers=HEADERS, timeout=30
        )
        if not r.ok:
            print(f"  ⚠️  Could not fetch existing IDs: {r.text[:100]}")
            break
        batch = r.json()
        if not batch:
            break
        ids.update(row["ticket_id"] for row in batch if row.get("ticket_id"))
        offset += limit
        if len(batch) < limit:
            break
    return ids

# ─── Main ─────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Upload Zoho tickets to Supabase")
    parser.add_argument("--csv", required=True, help="Path to Cases__1.csv")
    parser.add_argument("--resume", action="store_true", help="Skip already-uploaded ticket IDs")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"❌  File not found: {csv_path}")
        sys.exit(1)

    print("╔══════════════════════════════════════════════════╗")
    print("║   Waffarha — Zoho Ticket Uploader               ║")
    print(f"║   File: {csv_path.name:41s}║")
    print("╚══════════════════════════════════════════════════╝\n")
    print(f"  Supabase: {SUPABASE_URL}")
    print(f"  Table:    {TABLE}")
    print(f"  Batch:    {BATCH_SIZE} rows\n")

    existing_ids = set()
    if args.resume:
        existing_ids = get_existing_ids()
        print(f"  Resume mode: {len(existing_ids):,} tickets already uploaded\n")

    # ── Read CSV ──
    print("📂  Reading CSV…")
    all_records = []
    skipped = 0
    with open(csv_path, encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapped = map_row(row)
            tid = mapped.get("ticket_id")
            if not tid:
                skipped += 1
                continue
            if args.resume and tid in existing_ids:
                skipped += 1
                continue
            all_records.append(mapped)

    print(f"  Rows to upload: {len(all_records):,}  (skipped: {skipped:,})\n")

    if not all_records:
        print("✅  Nothing to upload.")
        return

    # ── Upload in batches ──
    total = len(all_records)
    success = 0
    failed = 0
    n_batches = math.ceil(total / BATCH_SIZE)

    print(f"🚀  Uploading {total:,} rows in {n_batches} batches…\n")
    t0 = time.time()

    for i in range(0, total, BATCH_SIZE):
        batch = all_records[i: i + BATCH_SIZE]
        ok, cnt, err = upload_batch(batch)
        if ok:
            success += cnt
        else:
            failed += len(batch)
            print(f"\n  ❌  Batch {i // BATCH_SIZE + 1} failed: {err}")

        pct = int(success / total * 100)
        elapsed = time.time() - t0
        rate = success / elapsed if elapsed > 0 else 0
        eta = (total - success) / rate if rate > 0 else 0
        print(f"  [{pct:3d}%] {success:,}/{total:,} rows  |  {rate:.0f} rows/s  |  ETA {eta:.0f}s   ", end="\r")
        time.sleep(0.1)

    elapsed = time.time() - t0
    print(f"\n\n{'═' * 52}")
    print(f"✅  Done in {elapsed:.1f}s")
    print(f"   Uploaded : {success:,}")
    print(f"   Failed   : {failed:,}")
    print("═" * 52)

if __name__ == "__main__":
    main()
