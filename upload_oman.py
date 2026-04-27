import os
import math
import pandas as pd
from supabase import create_client, Client

# Use environment variables if available, otherwise hardcode for now
SUPABASE_URL = "https://omowdfzyudedrtcuhnvy.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tb3dkZnp5dWRlZHJ0Y3VobnZ5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjQzNjI3OCwiZXhwIjoyMDkyMDEyMjc4fQ.kgQTvZRIrgFXTwL5wDM5oYLmDS9GtRjltE53wcpDQes"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def upload_oman_data(excel_path: str):
    print(f"Reading {excel_path}...")
    xl = pd.ExcelFile(excel_path)
    
    print("Clearing existing data in merchants_muscat...")
    try:
        supabase.table("merchants_muscat").delete().neq("id", 0).execute()
        print("Data cleared.")
    except Exception as e:
        print(f"Warning: Could not clear table: {e}")

    records = []
    for sheet in xl.sheet_names:
        df = pd.read_excel(xl, sheet)
        df = df.where(pd.notnull(df), None)
        mall_name = sheet.strip()
        print(f"Processing sheet: {mall_name} ({len(df)} rows)")
        
        for _, row in df.iterrows():
            def safe_float(val):
                try: return float(val)
                except: return None
                
            def safe_int(val):
                try: return int(float(val))
                except: return None

            def sanitize(val):
                if val is None: return None
                if isinstance(val, float) and math.isnan(val): return None
                if pd.isna(val): return None
                return val

            def safe_float(val):
                val = sanitize(val)
                if val is None: return None
                try: return float(val)
                except: return None
                
            def safe_int(val):
                val = sanitize(val)
                if val is None: return None
                try: return int(float(val))
                except: return None
                
            def safe_str(val):
                val = sanitize(val)
                if val is None: return None
                return str(val)

            # Handle column variations
            priority_val = row.get("Priority")
            if (priority_val is None or (isinstance(priority_val, float) and math.isnan(priority_val))) and "Column 1" in row:
                priority_val = row.get("Column 1")
                
            priority_str = safe_str(priority_val)
            # Map A -> High, B -> Medium, C -> Low
            if priority_str:
                p_upper = priority_str.strip().upper()
                if p_upper == "A": priority_str = "High"
                elif p_upper == "B": priority_str = "Medium"
                elif p_upper == "C": priority_str = "Low"

            record = {
                "merchant_name": safe_str(row.get("Merchant")),
                "category": safe_str(row.get("Category")),
                "priority": priority_str,
                "rating": safe_float(row.get("Rating")),
                "reviews_count": safe_int(row.get("No.Reviews")),
                "branches_ksa": safe_int(row.get("Branches (Oman)")),
                "avg_price": safe_str(row.get("Average Price")),
                "top_reviews": safe_str(row.get("Top Reviews")),
                "instagram": safe_str(row.get("Instagram")),
                "city": "Muscat",
                "mall": safe_str(mall_name)
            }
            
            # Final sanity check to avoid any stray NaNs
            for k, v in record.items():
                if isinstance(v, float) and math.isnan(v):
                    record[k] = None

            if record["merchant_name"]: # Ensure we don't upload empty rows
                records.append(record)

    print(f"Uploading {len(records)} records to merchants_muscat...")
    
    # Insert in batches
    batch_size = 50
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table("merchants_muscat").insert(batch).execute()
            print(f"✅ Inserted batch {i//batch_size + 1}")
        except Exception as e:
            print(f"❌ Error inserting batch {i//batch_size + 1}: {e}")

if __name__ == "__main__":
    current_dir = os.getcwd()
    excel_file = os.path.join(current_dir, "Oman Merchants Research (1).xlsx")
    if not os.path.exists(excel_file):
        print("Please run this script from the directory containing the excel file.")
    else:
        upload_oman_data(excel_file)
