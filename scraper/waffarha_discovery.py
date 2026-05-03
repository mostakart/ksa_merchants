import os
import asyncio
import re
from playwright.async_api import async_playwright
from dotenv import load_dotenv
import httpx

# Load configuration
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CATEGORIES = {
    "1":   "Hot Deals",
    "6":   "Food & Beverage",
    "7":   "Health & Beauty",
    "8":   "Activities & Entertainment",
    "9":   "Hotels & Resorts",
    "10":  "Retail & Services",
    "111": "Activities & Entertainment", # Kids
    "156": "Food & Beverage", # Easter eats
}

async def get_existing_merchants():
    """Get already tracked merchants to avoid duplicates."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return set()
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"{SUPABASE_URL}/rest/v1/tracked_merchants?select=name", headers=headers)
            if r.status_code == 200:
                return {m["name"].lower() for m in r.json()}
        except Exception as e:
            print(f"[!] Could not fetch existing merchants: {e}")
    return set()

async def scrape_category(browser, cat_id, cat_name):
    """Scrape a single category page for merchant names."""
    url = f"https://waffarha.com/en/category-c-{cat_id}"
    print(f"[*] Crawling {cat_name} ({url})...")
    
    page = await browser.new_page()
    merchants = set()
    
    try:
        await page.goto(url, wait_until="networkidle", timeout=60000)
        
        # Scroll multiple times to trigger infinite load if present
        for _ in range(5):
            await page.mouse.wheel(0, 3000)
            await asyncio.sleep(1)
        
        # Extract all deal titles
        titles = await page.evaluate('''() => {
            return Array.from(document.querySelectorAll('h3.slid_title, h2, .deal-title, a[title]')).map(el => el.innerText || el.title);
        }''')
        
        for title in titles:
            if not title: continue
            # Find patterns like @MerchantName or @ Merchant Name
            match = re.search(r'@\s*([A-Za-z0-9\s\'\-\.&]+)', title)
            if match:
                m_name = match.group(1).strip()
                # Clean up (sometimes it grabs too much)
                m_name = re.split(r'\s{2,}|–|-|\n', m_name)[0].strip()
                if m_name and len(m_name) > 2:
                    merchants.add(m_name)
                    
    except Exception as e:
        print(f"[!] Error scraping category {cat_id}: {e}")
    finally:
        await page.close()
        
    return merchants

async def main():
    existing = await get_existing_merchants()
    print(f"[*] Found {len(existing)} existing merchants in database.")
    
    discovered = {}
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        for cat_id, cat_name in CATEGORIES.items():
            found = await scrape_category(browser, cat_id, cat_name)
            for m in found:
                if m.lower() not in existing:
                    discovered[m] = cat_name
            print(f"[+] Found {len(found)} merchants in {cat_name} ({len(discovered)} total new).")
            
        await browser.close()
    
    # Final Report
    print("\n" + "="*50)
    print(f"DISCOVERY COMPLETE: {len(discovered)} NEW MERCHANTS FOUND")
    print("="*50)
    
    # Sort by category for better display
    for m_name, cat in sorted(discovered.items(), key=lambda x: x[1]):
        print(f"[{cat: <25}] {m_name}")
        
    # Option to save to CSV or JSON
    import json
    with open("scraper/discovered_merchants.json", "w", encoding="utf-8") as f:
        json.dump(discovered, f, indent=2, ensure_ascii=False)
    print(f"\n[!] Saved {len(discovered)} new merchants to scraper/discovered_merchants.json")

if __name__ == "__main__":
    asyncio.run(main())
