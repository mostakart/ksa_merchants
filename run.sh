#!/usr/bin/env bash
# KSA Merchant Web Intelligence Pipeline
# Run from the repo root: bash run.sh

set -e
cd "$(dirname "$0")"

echo "============================================"
echo "  KSA Merchant Web Intelligence Scraper"
echo "============================================"
echo ""

# Step 1: Scrape
echo "[1/3] Running batch scraper…"
python3 -m scraper.batch_runner "$@"

echo ""
# Step 2: Merge back into Excel
echo "[2/3] Merging results into Excel files…"
python3 -m scraper.merger

echo ""
# Step 3: Generate intelligence report
echo "[3/3] Generating intelligence report…"
python3 -m scraper.intel_report

echo ""
echo "============================================"
echo "  All done!"
echo "  Output files:"
echo "    scraped_urls.json       (checkpoint)"
echo "    scraped_results.json    (raw results)"
echo "    scraped_results.csv     (flat CSV)"
echo "    *_ENRICHED.xlsx         (enriched Excel)"
echo "    intel_report.md         (summary report)"
echo "============================================"
