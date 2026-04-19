import json
import os

def merge():
    old_path = 'data/checkpoints/master_checkpoint.json'
    new_path = 'output/master_checkpoint.json'
    
    if not os.path.exists(old_path) or not os.path.exists(new_path):
        print("Files missing")
        return

    with open(old_path, 'r', encoding='utf-8') as f:
        old_data = json.load(f)
    with open(new_path, 'r', encoding='utf-8') as f:
        new_data = json.load(f)

    merged = old_data.get('enriched', {}).copy()
    
    for k, v in new_data.get('enriched', {}).items():
        if k in merged:
            # Deduplicate by place_id
            existing_pids = {m['place_id'] for m in merged[k]}
            for m in v:
                if m['place_id'] not in existing_pids:
                    merged[k].append(m)
                    existing_pids.add(m['place_id'])
        else:
            merged[k] = v

    # Stats
    stats = {}
    for k, v in merged.items():
        city = k.split('::')[0] if '::' in k else 'Other'
        stats[city] = stats.get(city, 0) + len(v)
    
    print("Merged Stats:")
    for city, count in stats.items():
        print(f"  {city}: {count}")
    print(f"Total: {sum(stats.values())}")

    # Save
    final_data = old_data.copy()
    final_data['enriched'] = merged
    with open('output/master_checkpoint.json', 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    print("\nSaved to output/master_checkpoint.json")

if __name__ == "__main__":
    merge()
