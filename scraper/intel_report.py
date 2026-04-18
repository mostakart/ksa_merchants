"""
Generate KSA Merchant Web Intelligence Report.

Reads scraped_results.json → prints + saves intel_report.md

Usage:
    python -m scraper.intel_report
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RESULTS_JSON = DATA_DIR / "output" / "scraped_results.json"
REPORT_FILE = DATA_DIR / "output" / "intel_report.md"

DELIVERY_FLAGS = {
    "has_hungerstation": "HungerStation",
    "has_jahez": "Jahez",
    "has_talabat": "Talabat",
    "has_marsool": "Marsool",
    "has_toyo": "Toyo",
    "has_noon_food": "NoonFood",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def pct(n: int, total: int) -> str:
    if total == 0:
        return "0.0%"
    return f"{n / total * 100:.1f}%"


def bd_score(r: dict[str, Any]) -> int:
    score = 0
    if r.get("is_alive"):
        score += 20
    if any(r.get(f) for f in ["has_hungerstation", "has_jahez", "has_talabat"]):
        score += 15
    if r.get("instagram_url"):
        score += 15
    if r.get("whatsapp_number"):
        score += 10
    if r.get("email"):
        score += 10
    if r.get("has_online_menu"):
        score += 10
    if r.get("founding_year"):
        score += 10
    if r.get("branch_count_on_site"):
        score += 10
    return score


def table_row(*cells: str) -> str:
    return "| " + " | ".join(str(c) for c in cells) + " |"


def table_sep(*widths: int) -> str:
    return "| " + " | ".join("-" * w for w in widths) + " |"


# ── Report builder ────────────────────────────────────────────────────────────

def build_report(records: list[dict[str, Any]]) -> str:
    total = len(records)
    alive = [r for r in records if r.get("is_alive")]
    dead = [r for r in records if not r.get("is_alive")]

    lines: list[str] = []
    a = lines.append

    a(f"# KSA Merchant Web Intelligence Report")
    a(f"*Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*")
    a("")

    # ── Coverage ────────────────────────────────────────────────────────────
    a("## Coverage")
    a("")
    a(f"| Metric | Count |")
    a(f"|--------|-------|")
    a(f"| Total unique websites scraped | {total:,} |")
    a(f"| Successfully reachable (2xx) | {len(alive):,} ({pct(len(alive), total)}) |")
    a(f"| Dead / unreachable | {len(dead):,} ({pct(len(dead), total)}) |")
    a(f"| Had scrape errors | {sum(1 for r in records if r.get('error')):,} |")
    a("")

    # ── Delivery platforms ──────────────────────────────────────────────────
    a("## Delivery Platform Presence")
    a("")
    a(table_row("Platform", "Merchants", "% of alive sites"))
    a(table_sep(20, 12, 18))
    for flag, label in DELIVERY_FLAGS.items():
        n = sum(1 for r in alive if r.get(flag))
        a(table_row(label, f"{n:,}", pct(n, len(alive))))
    a("")

    multi_platform = [r for r in alive if sum(1 for f in DELIVERY_FLAGS if r.get(f)) >= 2]
    zero_platform  = [r for r in alive if sum(1 for f in DELIVERY_FLAGS if r.get(f)) == 0]
    a(f"- **On 2+ platforms** (most competitive): {len(multi_platform):,} ({pct(len(multi_platform), len(alive))})")
    a(f"- **On 0 platforms** (First Mover opportunity): {len(zero_platform):,} ({pct(len(zero_platform), len(alive))})")
    a("")

    # ── Social media ─────────────────────────────────────────────────────────
    a("## Social Media & Contact Presence")
    a("")
    social_fields = {
        "instagram_url": "Instagram",
        "tiktok_url": "TikTok",
        "snapchat_url": "Snapchat",
        "twitter_url": "Twitter / X",
        "youtube_url": "YouTube",
        "facebook_url": "Facebook",
        "whatsapp_number": "WhatsApp contact",
        "email": "Email address",
        "reservation_link": "Reservation link",
    }
    a(table_row("Channel", "Merchants", "% of alive sites"))
    a(table_sep(25, 12, 18))
    for field, label in social_fields.items():
        n = sum(1 for r in alive if r.get(field))
        a(table_row(label, f"{n:,}", pct(n, len(alive))))
    a("")

    # ── Brand maturity ────────────────────────────────────────────────────────
    a("## Brand Maturity")
    a("")
    loyalty_n    = sum(1 for r in alive if r.get("has_loyalty_program"))
    app_n        = sum(1 for r in alive if r.get("has_mobile_app"))
    catering_n   = sum(1 for r in alive if r.get("has_catering"))
    franchise_n  = sum(1 for r in alive if r.get("has_franchise_info"))
    menu_n       = sum(1 for r in alive if r.get("has_online_menu"))
    ar_n         = sum(1 for r in alive if r.get("has_arabic_content"))

    a(table_row("Signal", "Merchants", "% of alive sites"))
    a(table_sep(30, 12, 18))
    a(table_row("Have loyalty/rewards program", f"{loyalty_n:,}", pct(loyalty_n, len(alive))))
    a(table_row("Have mobile app", f"{app_n:,}", pct(app_n, len(alive))))
    a(table_row("Have catering/events offering", f"{catering_n:,}", pct(catering_n, len(alive))))
    a(table_row("Have franchise info", f"{franchise_n:,}", pct(franchise_n, len(alive))))
    a(table_row("Have online menu", f"{menu_n:,}", pct(menu_n, len(alive))))
    a(table_row("Arabic content on site", f"{ar_n:,}", pct(ar_n, len(alive))))
    a("")

    years = [r["founding_year"] for r in alive if r.get("founding_year")]
    if years:
        avg_year = int(sum(years) / len(years))
        oldest = min(records, key=lambda r: r.get("founding_year") or 9999)
        a(f"- **Average founding year:** {avg_year}")
        a(f"- **Oldest brand:** {oldest.get('merchant_name') or oldest.get('merchant', 'Unknown')} (est. {oldest['founding_year']})")
        a(f"- **Brands with known founding year:** {len(years):,} ({pct(len(years), len(alive))})")
    a("")

    # ── Page language breakdown ───────────────────────────────────────────────
    a("## Page Language Breakdown")
    a("")
    lang_counts = Counter(r.get("page_language", "unknown") for r in alive)
    a(table_row("Language", "Count", "%"))
    a(table_sep(15, 10, 10))
    for lang, cnt in lang_counts.most_common():
        a(table_row(lang or "unknown", f"{cnt:,}", pct(cnt, len(alive))))
    a("")

    # ── BD Priority: Top 20 ───────────────────────────────────────────────────
    a("## BD Priority Signals")
    a("")
    a("### Top 20 Merchants by BD Enrichment Score")
    a("")
    scored = [(bd_score(r), r) for r in alive]
    scored.sort(key=lambda x: x[0], reverse=True)
    top20 = scored[:20]

    a(table_row("Rank", "Merchant", "City", "Score", "Platforms", "Instagram", "WhatsApp"))
    a(table_sep(4, 30, 12, 7, 30, 10, 12))
    for rank, (score, r) in enumerate(top20, 1):
        platforms = ",".join(lbl for f, lbl in DELIVERY_FLAGS.items() if r.get(f)) or "—"
        ig = r.get("instagram_handle") or ("✓" if r.get("instagram_url") else "—")
        wa = "✓" if r.get("whatsapp_number") else "—"
        name = (r.get("merchant_name") or r.get("merchant", ""))[:30]
        city = str(r.get("city", ""))[:12]
        a(table_row(rank, name, city, score, platforms, ig, wa))
    a("")

    # ── First Mover targets ────────────────────────────────────────────────────
    a("### First Mover Targets (0 delivery platforms, alive site)")
    a("")
    first_movers = [r for r in zero_platform if r.get("is_alive")]
    first_movers.sort(key=lambda r: bd_score(r), reverse=True)
    shown = first_movers[:30]
    a(table_row("Merchant", "City", "Website", "Score"))
    a(table_sep(30, 12, 40, 7))
    for r in shown:
        name = (r.get("merchant_name") or r.get("merchant", ""))[:30]
        city = str(r.get("city", ""))[:12]
        url  = (r.get("website_url", ""))[:40]
        a(table_row(name, city, url, bd_score(r)))
    if len(first_movers) > 30:
        a(f"*… and {len(first_movers) - 30:,} more — see scraped_results.csv*")
    a("")

    # ── Loyalty program merchants ─────────────────────────────────────────────
    a("### Merchants with Loyalty Programs (Competitive pitch needed)")
    a("")
    loyalty_brands = [r for r in alive if r.get("has_loyalty_program")]
    loyalty_brands.sort(key=lambda r: bd_score(r), reverse=True)
    a(table_row("Merchant", "City", "Platforms", "Score"))
    a(table_sep(30, 12, 30, 7))
    for r in loyalty_brands[:25]:
        name = (r.get("merchant_name") or r.get("merchant", ""))[:30]
        city = str(r.get("city", ""))[:12]
        platforms = ",".join(lbl for f, lbl in DELIVERY_FLAGS.items() if r.get(f)) or "none"
        a(table_row(name, city, platforms, bd_score(r)))
    if len(loyalty_brands) > 25:
        a(f"*… and {len(loyalty_brands) - 25:,} more*")
    a("")

    a("---")
    a("*Report generated by KSA Merchant Web Intelligence Scraper (WaffarhaBot/1.0)*")

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not RESULTS_JSON.exists():
        print(f"[ERROR] {RESULTS_JSON} not found. Run batch_runner and merger first.")
        sys.exit(1)

    with open(RESULTS_JSON, encoding="utf-8") as f:
        records = json.load(f)

    # Deduplicate by website_url (take first occurrence)
    seen: set[str] = set()
    unique_records: list[dict[str, Any]] = []
    for r in records:
        url = r.get("website_url", "")
        if url not in seen:
            seen.add(url)
            unique_records.append(r)

    print(f"Loaded {len(records):,} records ({len(unique_records):,} unique URLs)")

    report = build_report(unique_records)

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report)

    print(report)
    print(f"\n✓ Saved → {REPORT_FILE.name}")


if __name__ == "__main__":
    main()
