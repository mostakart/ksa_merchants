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

def safe_float(v):
    try:
        s = str(v).strip()
        if s.lower() in ("", "none", "nan", "null"):
            return None
        return float(s.replace(",", ""))
    except Exception:
        return None

def safe_bool(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y", "t"):
        return True
    if s in ("false", "0", "no", "n", "f"):
        return False
    return None

def safe_ts(v):
    if not v or str(v).strip().lower() in ("none", "nan", "null", ""):
        return None
    s = str(v).strip()
    # Handle multiple formats if needed, but standard ISO or Zoho format usually works
    try:
        if " " in s and ":" in s:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            return dt.isoformat() + "Z"
        return s # Fallback to raw if it looks like ISO
    except ValueError:
        return s

# ─── Row mappers ──────────────────────────────────────────────
def map_ticket_row(row):
    return {
        "ticket_number":     safe_str(row.get("ticket_number")),
        "channel":           safe_str(row.get("channel")),
        "status":            safe_str(row.get("status")),
        "department_id":     safe_str(row.get("department_id")),
        "assignee":          safe_str(row.get("assignee")),
        "customer_email":    safe_str(row.get("customer_email")),
        "customer_phone":    safe_str(row.get("customer_phone")),
        "tags":              safe_str(row.get("tags")),
        "message_direction": safe_str(row.get("message_direction")),
        "sender_name":       safe_str(row.get("sender_name")),
        "message":           safe_str(row.get("message")),
        "has_attachments":   safe_bool(row.get("has_attachments")),
        "ticket_time":       safe_ts(row.get("ticket_time")),
    }

def map_analysis_row(row):
    return {
        "ticket_number":               safe_str(row.get("ticket_number")),
        "p_issue_type":                safe_str(row.get("p_issue_type")),
        "p_merchant_name":             safe_str(row.get("p_merchant_name")),
        "p_merchant_issue_type":       safe_str(row.get("p_merchant_issue_type")),
        "p_payment_blocker":           safe_bool(row.get("p_payment_blocker")),
        "p_refund_requested":          safe_bool(row.get("p_refund_requested")),
        "p_ux_friction_point":         safe_str(row.get("p_ux_friction_point")),
        "p_missing_feature":           safe_str(row.get("p_missing_feature")),
        "p_root_cause_owner":          safe_str(row.get("p_root_cause_owner")),
        "p_smart_tags":                safe_str(row.get("p_smart_tags")),
        "mer_branch_name":             safe_str(row.get("mer_branch_name")),
        "m_promo_code_used":           safe_str(row.get("m_promo_code_used")),
        "fin_ticket_monetary_value":   safe_float(row.get("fin_ticket_monetary_value")),
        "c_misleading_wording_exact":  safe_str(row.get("c_misleading_wording_exact")),
        "f_fraud_suspicion":           safe_bool(row.get("f_fraud_suspicion")),
        "cs_escalation_department":    safe_str(row.get("cs_escalation_department")),
        "s_initial_sentiment":         safe_str(row.get("s_initial_sentiment")),
        "s_final_sentiment":           safe_str(row.get("s_final_sentiment")),
        "s_sentiment_shift":           safe_str(row.get("s_sentiment_shift")),
        "s_churn_intent":              safe_bool(row.get("s_churn_intent")),
        "s_customer_effort_score":     safe_int(row.get("s_customer_effort_score")),
        "s_profanity_detected":        safe_bool(row.get("s_profanity_detected")),
        "s_gratitude_detected":        safe_bool(row.get("s_gratitude_detected")),
        "s_sentiment_summary":         safe_str(row.get("s_sentiment_summary")),
        "a_empathy_score":             safe_int(row.get("a_empathy_score")),
        "a_policy_compliance":         safe_bool(row.get("a_policy_compliance")),
        "a_grammar_professionalism":   safe_int(row.get("a_grammar_professionalism")),
        "a_knowledge_accuracy":        safe_int(row.get("a_knowledge_accuracy")),
        "a_is_template_heavy":         safe_bool(row.get("a_is_template_heavy")),
        "a_one_touch_resolution":      safe_bool(row.get("a_one_touch_resolution")),
        "a_escalated":                 safe_bool(row.get("a_escalated")),
        "a_overall_score":             safe_float(row.get("a_overall_score")),
        "a_evaluation_notes":          safe_str(row.get("a_evaluation_notes")),
        "ai_status":                   safe_str(row.get("ai_status")),
        "ai_model_used":               safe_str(row.get("ai_model_used")),
        "analyzed_at":                 safe_ts(row.get("analyzed_at")),
    }

# ─── Supabase helpers ─────────────────────────────────────────
import requests

def upload_batch(records, table):
    """Upsert a batch. Returns (ok, success_count, error_text)."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    # Use ticket_number as the resolution key for both tables
    h = {**HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"}
    r = requests.post(url, headers=h, data=json.dumps(records, ensure_ascii=False, default=str).encode("utf-8"), timeout=60)
    if r.status_code in (200, 201):
        return True, len(records), ""
    return False, 0, r.text[:300]

def get_existing_ids(table):
    """Fetch all ticket_numbers already in Supabase."""
    print(f"  Fetching existing ticket numbers from {table}…")
    ids = set()
    offset = 0
    limit = 2000
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{table}?select=ticket_number&limit={limit}&offset={offset}",
            headers=HEADERS, timeout=30
        )
        if not r.ok:
            print(f"  ⚠️  Could not fetch existing IDs: {r.text[:100]}")
            break
        batch = r.json()
        if not batch:
            break
        ids.update(row["ticket_number"] for row in batch if row.get("ticket_number"))
        offset += limit
        if len(batch) < limit:
            break
    return ids

# ─── Main Logic ───────────────────────────────────────────────
def process_upload(path, table, mapper, resume):
    print(f"\n🚀 Processing {table} from {path.name}…")
    
    existing_ids = set()
    if resume:
        existing_ids = get_existing_ids(table)
        print(f"  Resume mode: {len(existing_ids):,} records already in DB\n")

    all_records = []
    skipped = 0
    
    # Handle both CSV and TXT (if TXT is comma-separated)
    encoding = "utf-8-sig" if path.suffix == ".csv" else "utf-8"
    
    with open(path, encoding=encoding, errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapped = mapper(row)
            tnum = mapped.get("ticket_number")
            if not tnum:
                skipped += 1
                continue
            if resume and tnum in existing_ids:
                skipped += 1
                continue
            all_records.append(mapped)

    total = len(all_records)
    print(f"  Rows to upload: {total:,}  (skipped: {skipped:,})")

    if not all_records:
        return

    success = 0
    failed = 0
    n_batches = math.ceil(total / BATCH_SIZE)

    t0 = time.time()
    for i in range(0, total, BATCH_SIZE):
        batch = all_records[i: i + BATCH_SIZE]
        ok, cnt, err = upload_batch(batch, table)
        if ok:
            success += cnt
        else:
            failed += len(batch)
            print(f"\n  ❌  Batch {i // BATCH_SIZE + 1} failed: {err}")

        pct = int(success / total * 100)
        elapsed = time.time() - t0
        rate = success / elapsed if elapsed > 0 else 0
        print(f"  [{pct:3d}%] {success:,}/{total:,} rows  |  {rate:.0f} rows/s   ", end="\r")

    print(f"\n  ✅ {table} upload complete: {success:,} success, {failed:,} failed")

def main():
    parser = argparse.ArgumentParser(description="Upload Zoho tickets and Analysis to Supabase")
    parser.add_argument("--tickets", help="Path to tickets_rows.txt or .csv")
    parser.add_argument("--analysis", help="Path to ticket_analysis_rows.csv")
    parser.add_argument("--resume", action="store_true", help="Skip existing records")
    args = parser.parse_args()

    if not args.tickets and not args.analysis:
        parser.print_help()
        sys.exit(1)

    print("╔══════════════════════════════════════════════════╗")
    print("║   Waffarha — CRM Data Ingestion Tool            ║")
    print("╚══════════════════════════════════════════════════╝\n")
    print(f"  Supabase: {SUPABASE_URL}")

    if args.tickets:
        p = Path(args.tickets)
        if p.exists():
            process_upload(p, "zoho_tickets", map_ticket_row, args.resume)
        else:
            print(f"❌  Tickets file not found: {args.tickets}")

    if args.analysis:
        p = Path(args.analysis)
        if p.exists():
            process_upload(p, "ticket_analysis", map_analysis_row, args.resume)
        else:
            print(f"❌  Analysis file not found: {args.analysis}")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
