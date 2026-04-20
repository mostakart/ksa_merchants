import json
from pathlib import Path

def fix_checkpoint():
    path = Path('output/master_checkpoint.json')
    if not path.exists():
        print("Checkpoint not found")
        return
    
    data = json.loads(path.read_text(encoding='utf-8'))
    
    # We want to rerun these cities to pick up the new categories
    to_rerun = ['Dammam', 'Khobar', 'Mecca', 'Medina']
    
    current_done = data.get('cities_done', [])
    new_done = [c for c in current_done if c not in to_rerun]
    
    data['cities_done'] = new_done
    
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"Fixed checkpoint. Removed {to_rerun} from cities_done.")
    print(f"Remaining done: {new_done}")

if __name__ == "__main__":
    fix_checkpoint()
