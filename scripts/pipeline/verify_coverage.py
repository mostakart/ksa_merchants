
import os, sys, json
from pathlib import Path

# Fix Windows Unicode encoding
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

def verify_coverage():
    checkpoint_path = Path("output/master_checkpoint.json")
    if not checkpoint_path.exists():
        print("❌ master_checkpoint.json not found!")
        return

    with open(checkpoint_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    mall_data = data.get("mall_data", {})
    enriched = data.get("enriched", {})

    print(f"\n{'='*70}")
    print(f"📊 COVERAGE VERIFICATION REPORT")
    print(f"{'='*70}")

    for city, malls in mall_data.items():
        print(f"\n🏙️  CITY: {city} ({len(malls)} malls discovered)")
        
        ent_count = 0
        retail_count = 0
        total_malls_with_any = 0
        
        malls_with_details = []
        for mall in malls:
            mall_name = mall["mall_name"]
            mall_key = f"{city}::{mall_name}"
            merchants = enriched.get(mall_key, [])
            
            ent = [m for m in merchants if m.get("category") == "Entertainment"]
            retail = [m for m in merchants if m.get("category") == "Retail - Perfumes"]
            
            malls_with_details.append({
                "name": mall_name,
                "ent": len(ent),
                "retail": len(retail)
            })
            
            if len(ent) > 0: ent_count += 1
            if len(retail) > 0: retail_count += 1
            if merchants: total_malls_with_any += 1

        print(f"   ✅ Total Malls: {len(malls)}")
        print(f"   🎬 Malls with Entertainment: {ent_count}")
        print(f"   🧴 Malls with Retail - Perfumes: {retail_count}")
        
        print(f"\n   📋 Detailed Breakdown (First 10 malls):")
        for m in malls_with_details[:10]:
            status = "✅" if m["ent"] > 0 and m["retail"] > 0 else "⚠️"
            print(f"      {status} {m['name']:<35} | Ent: {m['ent']:<3} | Retail: {m['retail']:<3}")
        
        if len(malls_with_details) > 10:
            print(f"      ... and {len(malls_with_details)-10} more malls.")

    print(f"\n{'='*70}")
    print("💡 Note: If a mall truly has no cinema/perfume shop, it will show as missing.")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    verify_coverage()
