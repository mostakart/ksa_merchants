import os
import asyncio
import random
import base64
import time
from datetime import datetime
import httpx
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
from dotenv import load_dotenv

# Load configuration
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # Service role key
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

# --- USER AGENTS & VIEWPORTS ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
]

# --- HELPERS ---
async def fetch_active_merchants():
    """Get active merchants from Supabase."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    url = f"{SUPABASE_URL}/rest/v1/tracked_merchants?is_active=eq.true"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()

async def scrape_merchant(context, merchant):
    """Scrape a single merchant's social media or website."""
    name = merchant["name"]
    urls = {
        "facebook": merchant.get("facebook_url"),
        "instagram": merchant.get("instagram_url"),
        "website": merchant.get("website_url")
    }
    
    # Prioritize URLs (FB > IG > Web)
    platforms = [p for p in ["facebook", "instagram", "website"] if urls[p]]
    if not platforms:
        print(f"[-] No URLs for {name}")
        return

    # Randomly pick a platform to scrape this time
    platform = random.choice(platforms)
    url = urls[platform]
    
    print(f"[*] Scraping {name} on {platform}...")
    page = await context.new_page()
    
    try:
        # Apply stealth
        await stealth_async(page)
        
        # Navigate
        await page.goto(url, wait_until="networkidle", timeout=60000)
        
        # Random scroll to look human
        await page.mouse.wheel(0, random.randint(300, 700))
        await asyncio.sleep(random.uniform(2, 5))
        
        # Extract Text (Targeting common post classes)
        # Note: Selectors are tricky for FB/IG, using a broad approach
        text_content = ""
        if "facebook" in platform:
            elements = await page.query_selector_all('div[data-ad-preview="message"], div[dir="auto"]')
            text_content = "\n".join([await el.inner_text() for el in elements[:5]])
        elif "instagram" in platform:
            elements = await page.query_selector_all('h1, span._ap30')
            text_content = "\n".join([await el.inner_text() for el in elements[:5]])
        else:
            text_content = await page.inner_text("body")
            text_content = text_content[:2000] # Limit website text

        # Capture Screenshot
        screenshot_bytes = await page.screenshot(full_page=False)
        screenshot_b64 = base64.b64encode(screenshot_bytes).decode('utf-8')
        
        # Payload for n8n
        payload = {
            "merchantId": merchant["id"],
            "merchantName": name,
            "platform": platform,
            "rawText": text_content.strip(),
            "screenshot": screenshot_b64,
            "scrapedAt": datetime.utcnow().isoformat()
        }
        
        # Send to n8n
        if N8N_WEBHOOK_URL:
            async with httpx.AsyncClient() as client:
                resp = await client.post(N8N_WEBHOOK_URL, json=payload, timeout=30)
                print(f"[+] Sent to n8n for {name}: {resp.status_code}")
        else:
            print(f"[!] No N8N_WEBHOOK_URL set. Skipping upload.")

    except Exception as e:
        print(f"[!] Failed {name} ({platform}): {str(e)}")
    finally:
        await page.close()

async def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] Missing Supabase credentials in .env")
        return

    merchants = await fetch_active_merchants()
    if not merchants:
        print("[-] No active merchants found.")
        return

    print(f"[*] Found {len(merchants)} active merchants. Shuffling order...")
    random.shuffle(merchants)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        for i, merchant in enumerate(merchants):
            # Rotate User Agent
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={'width': random.randint(1280, 1920), 'height': random.randint(800, 1080)}
            )
            
            await scrape_merchant(context, merchant)
            await context.close()
            
            # Anti-bot delay
            if i < len(merchants) - 1:
                delay = random.uniform(12, 28)
                print(f"[*] Sleeping for {delay:.1f}s...")
                await asyncio.sleep(delay)
                
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
