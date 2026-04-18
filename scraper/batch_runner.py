"""
Resumable async batch scraper.

Usage:
    python -m scraper.batch_runner
    python -m scraper.batch_runner --concurrency 20 --rps 5

Checkpoint file: scraped_urls.json   (same directory as this script's parent)
Output files:    scraped_results.json / scraped_results.csv
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

# Allow running as `python scraper/batch_runner.py` from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.extractor import MerchantWebIntel, scrape
from scraper.utils import normalize_url

# ── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CHECKPOINT_FILE = DATA_DIR / "checkpoints" / "scraped_urls.json"
RESULTS_JSON = DATA_DIR / "output" / "scraped_results.json"
RESULTS_CSV = DATA_DIR / "output" / "scraped_results.csv"

EXCEL_FILES = [
    DATA_DIR / "input" / "KSA_Merchants_Riyadh.xlsx",
    DATA_DIR / "input" / "KSA_Merchants_Jeddah.xlsx",
    DATA_DIR / "input" / "KSA_Merchants_Dammam.xlsx",
    DATA_DIR / "input" / "KSA_Merchants_Khobar.xlsx",
    DATA_DIR / "input" / "KSA_Merchants_Mecca.xlsx",
    DATA_DIR / "input" / "KSA_Merchants_Medina.xlsx",
]


# ── Excel reader ─────────────────────────────────────────────────────────────

def load_merchant_urls() -> dict[str, list[dict[str, Any]]]:
    """
    Returns {normalized_url: [merchant_meta, ...]}
    where merchant_meta = {merchant, city, mall, ...}
    """
    url_map: dict[str, list[dict[str, Any]]] = {}

    for fpath in EXCEL_FILES:
        if not fpath.exists():
            print(f"[WARN] Missing file: {fpath.name}")
            continue

        xl = pd.ExcelFile(fpath)
        for sheet in xl.sheet_names:
            df = pd.read_excel(fpath, sheet_name=sheet, dtype=str)

            web_col = next(
                (c for c in df.columns if "website" in str(c).lower()),
                None,
            )
            if not web_col:
                continue

            for _, row in df.iterrows():
                raw_url = str(row.get(web_col, "")).strip()
                if not raw_url or raw_url.lower() in ("nan", "none", ""):
                    continue

                try:
                    url = normalize_url(raw_url)
                except Exception:
                    continue

                merchant_name = str(row.get("Merchant", "")).strip()
                meta = {
                    "merchant": merchant_name,
                    "city": str(row.get("City", "")).strip(),
                    "mall": str(row.get("Mall", sheet)).strip(),
                    "source_file": fpath.name,
                    "original_url": raw_url,
                }
                url_map.setdefault(url, []).append(meta)

    return url_map


# ── Checkpoint I/O ────────────────────────────────────────────────────────────

def load_checkpoint() -> dict[str, Any]:
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(checkpoint: dict[str, Any]) -> None:
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    tmp.replace(CHECKPOINT_FILE)


# ── Rate limiter ─────────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, rps: float):
        self._interval = 1.0 / rps
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._interval - (now - self._last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()


# ── Batch processor ───────────────────────────────────────────────────────────

async def process_batch(
    batch: list[tuple[str, str]],  # [(url, merchant_name), ...]
    checkpoint: dict[str, Any],
    rate_limiter: RateLimiter,
    pbar: tqdm,
    counters: dict[str, int],
) -> None:
    async def worker(url: str, merchant_name: str) -> None:
        await rate_limiter.acquire()
        try:
            intel = await asyncio.wait_for(scrape(url, merchant_name), timeout=30.0)
        except Exception as exc:
            from scraper.extractor import MerchantWebIntel
            intel = MerchantWebIntel(
                merchant_name=merchant_name,
                website_url=url,
                scraped_at=datetime.now(timezone.utc).isoformat(),
                status_code=0,
                is_alive=False,
                error=f"Timeout or fatal error: {exc}"
            )
        entry = {
            "status": "done" if intel.is_alive else ("error" if intel.error else "done"),
            "result": intel.model_dump(),
            "scraped_at": intel.scraped_at,
        }
        checkpoint[url] = entry
        if intel.error:
            counters["errors"] += 1
        else:
            counters["done"] += 1
        pbar.update(1)
        pbar.set_postfix(done=counters["done"], errors=counters["errors"], skipped=counters["skipped"])

    tasks = [worker(url, name) for url, name in batch]
    await asyncio.gather(*tasks)


# ── Export helpers ────────────────────────────────────────────────────────────

def export_results(checkpoint: dict[str, Any], url_map: dict[str, list[dict[str, Any]]]) -> None:
    rows = []
    for url, entry in checkpoint.items():
        if entry.get("status") == "skip" or "result" not in entry:
            continue
        result = entry["result"]
        merchants = url_map.get(url, [])
        if merchants:
            for m in merchants:
                row = {**result, **m}
                rows.append(row)
        else:
            rows.append(result)

    with open(RESULTS_JSON, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(RESULTS_CSV, index=False, encoding="utf-8-sig")

    print(f"\n✓ Saved {len(rows):,} records → {RESULTS_JSON.name} + {RESULTS_CSV.name}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(concurrency: int = 20, rps: float = 5.0, batch_size: int = 20) -> None:
    print("Loading merchant URLs from Excel files…")
    url_map = load_merchant_urls()
    all_urls = list(url_map.keys())
    print(f"  Total unique URLs: {len(all_urls):,}")

    checkpoint = load_checkpoint()
    already_done = {u for u, v in checkpoint.items() if v.get("status") in ("done", "error", "skip")}
    pending_urls = [u for u in all_urls if u not in already_done]

    print(f"  Already scraped:   {len(already_done):,}")
    print(f"  Remaining:         {len(pending_urls):,}")

    if not pending_urls:
        print("Nothing left to scrape. Exporting results…")
        export_results(checkpoint, url_map)
        return

    rate_limiter = RateLimiter(rps)
    counters = {"done": len(already_done), "errors": 0, "skipped": 0}
    sem = asyncio.Semaphore(concurrency)

    # Wrap each url with its primary merchant name for the scraper
    def primary_name(url: str) -> str:
        metas = url_map.get(url, [])
        return metas[0]["merchant"] if metas else ""

    pending_with_names = [(u, primary_name(u)) for u in pending_urls]

    with tqdm(total=len(all_urls), initial=len(already_done), unit="url",
              desc="Scraping", ncols=90) as pbar:

        for i in range(0, len(pending_with_names), batch_size):
            batch = pending_with_names[i: i + batch_size]

            await process_batch(batch, checkpoint, rate_limiter, pbar, counters)

            # Checkpoint after every batch
            save_checkpoint(checkpoint)

    print(f"\nDone. Scraped: {counters['done']:,}  Errors: {counters['errors']:,}")
    export_results(checkpoint, url_map)


def run() -> None:
    parser = argparse.ArgumentParser(description="KSA Merchant Web Scraper")
    parser.add_argument("--concurrency", type=int, default=20, help="Max concurrent requests")
    parser.add_argument("--rps", type=float, default=5.0, help="Max requests per second")
    parser.add_argument("--batch-size", type=int, default=20, help="Checkpoint every N URLs")
    args = parser.parse_args()

    asyncio.run(main(concurrency=args.concurrency, rps=args.rps, batch_size=args.batch_size))


if __name__ == "__main__":
    run()
