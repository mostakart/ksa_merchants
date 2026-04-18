# merge_checkpoints.py
# يبني master_checkpoint.json من الـ Excel files مباشرة
#
# Run:
#   python pipeline/merge_checkpoints.py
#   python pipeline/merge_checkpoints.py --excel file1.xlsx file2.xlsx

import sys, json
import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

CITY_KEYWORDS = {
    "riyadh":"Riyadh","riyad":"Riyadh","jeddah":"Jeddah","jeda":"Jeddah",
    "dammam":"Dammam","khobar":"Khobar","mecca":"Mecca","medina":"Medina",
}

def detect_city(filename):
    name = filename.lower()
    for kw, city in CITY_KEYWORDS.items():
        if kw in name:
            return city
    return "Unknown"

def excel_to_merchants(path, city):
    xl = pd.ExcelFile(path)
    enriched = {}
    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        mall_key = f"{city}::{sheet}"
        merchants = []
        for _, row in df.iterrows():
            name = str(row.get("Merchant","") or "").strip()
            if not name or name == "nan":
                continue
            def clean(v):
                s = str(v or "")
                return "" if s.lower() == "nan" else s
            m = {
                "merchant":      name,
                "mall_name":     clean(row.get("Mall", sheet)),
                "city":          city,
                "category":      clean(row.get("Category","")),
                "rating":        float(row["Rating"])        if pd.notna(row.get("Rating"))         else None,
                "total_reviews": int(row["Reviews"])         if pd.notna(row.get("Reviews"))        else None,
                "avg_price":     clean(row.get("Avg Price","")),
                "branches":      int(row["Branches (KSA)"]) if pd.notna(row.get("Branches (KSA)")) else 1,
                "phone":         clean(row.get("Phone","")),
                "website":       clean(row.get("Website","")),
                "opening_hours": clean(row.get("Opening Hours","")),
                "lat":           float(row["Lat"]) if pd.notna(row.get("Lat")) else None,
                "lng":           float(row["Lng"]) if pd.notna(row.get("Lng")) else None,
                "reviews_text":  clean(row.get("Top Reviews","")),
                "priority":      clean(row.get("Priority","")),
                "place_id":      "",
                "p1_status":     "OK",
                "p2_status":     "OK",
            }
            merchants.append(m)
        enriched[mall_key] = merchants
    return enriched

def main():
    args = sys.argv[1:]

    if "--excel" in args:
        idx = args.index("--excel")
        files = [Path(f) for f in args[idx+1:] if not f.startswith("--")]
    else:
        files = sorted(list(Path("output/excel").glob("*.xlsx"))
                     + list(Path("output").glob("KSA_Merchants_*.xlsx")))

    if not files:
        print("❌ No Excel files found!")
        print("   Put them in output/excel/ OR run:")
        print("   python pipeline/merge_checkpoints.py --excel Riyadh.xlsx Jeddah.xlsx")
        sys.exit(1)

    print("\n" + "="*60)
    print("   CHECKPOINT BUILDER — from Excel")
    print("="*60)

    result = {"cities_done":[], "mall_data":{}, "enriched":{}, "contact_done":[], "costs_snapshot":{}}

    for fpath in files:
        if not fpath.exists():
            print(f"\n⚠️  Not found: {fpath}")
            continue
        city = detect_city(fpath.name)
        print(f"\n📂 {fpath.name}  →  {city}")
        enriched = excel_to_merchants(fpath, city)
        added_malls, added_merchants = 0, 0
        for k, v in enriched.items():
            if k not in result["enriched"]:
                result["enriched"][k] = v
                added_malls += 1
                added_merchants += len(v)
        if city not in result["cities_done"]:
            result["cities_done"].append(city)
        print(f"   ✅ {added_malls} malls | {added_merchants} merchants")

    total = sum(len(v) for v in result["enriched"].values())
    print(f"\n{'='*60}")
    print(f"✅ DONE — {total} merchants | {len(result['enriched'])} malls | cities: {result['cities_done']}")
    print(f"\n📋 Breakdown:")

    by_city = {}
    for k, v in result["enriched"].items():
        city = k.split("::",1)[0]
        by_city.setdefault(city, []).append((k.split("::",1)[1], len(v)))

    for city, malls in sorted(by_city.items()):
        city_total = sum(n for _,n in malls)
        print(f"\n   🏙️  {city} — {len(malls)} malls | {city_total} merchants")
        for mall_name, n in sorted(malls, key=lambda x: -x[1]):
            print(f"      {mall_name:<40} {n}")

    out = OUTPUT_DIR / "master_checkpoint.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n💾 Saved: {out} ({out.stat().st_size//1024} KB)")
    print(f"\n   Next steps:")
    print(f"   python pipeline/ksa_pipeline_master.py --topup Riyadh")
    print(f"   python pipeline/ksa_pipeline_master.py --topup Jeddah")
    print(f"   python pipeline/ksa_pipeline_master.py --city Dammam")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()