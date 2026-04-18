"""
Shared helpers: Arabic-Indic numeral conversion, regex patterns, URL normalization.
"""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlparse, urljoin

# ── Arabic-Indic → ASCII ────────────────────────────────────────────────────
_AR_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

def arabic_to_ascii(text: str) -> str:
    return text.translate(_AR_DIGITS)


# ── Founding year ────────────────────────────────────────────────────────────
_YEAR_PATTERN = re.compile(
    r"(?:since|founded|est\.?|established|تأسس(?:ت)?|منذ)\s*"
    r"([\d١-٩][٠-٩\d]{3})",
    re.IGNORECASE | re.UNICODE,
)

def extract_founding_year(text: str) -> Optional[int]:
    text_norm = arabic_to_ascii(text)
    for m in _YEAR_PATTERN.finditer(text_norm):
        try:
            year = int(arabic_to_ascii(m.group(1)))
            if 1950 <= year <= 2025:
                return year
        except ValueError:
            continue
    return None


# ── Branch count ─────────────────────────────────────────────────────────────
_BRANCH_PATTERN = re.compile(
    r"([\d١-٩][٠-٩\d]*)\s*\+?\s*(?:فرع|فروع|branches?|locations?|outlets?)",
    re.IGNORECASE | re.UNICODE,
)

def extract_branch_count(text: str) -> Optional[int]:
    text_norm = arabic_to_ascii(text)
    m = _BRANCH_PATTERN.search(text_norm)
    if m:
        try:
            return int(arabic_to_ascii(m.group(1)))
        except ValueError:
            return None
    return None


# ── Email ────────────────────────────────────────────────────────────────────
_EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

def extract_emails(text: str) -> list:
    return list(dict.fromkeys(_EMAIL_PATTERN.findall(text)))


# ── Phone numbers ────────────────────────────────────────────────────────────
_PHONE_PATTERN = re.compile(
    r"(?:\+?\d(?:[\s\-().]*\d){7,15})",
    re.UNICODE,
)

def extract_phones(text: str) -> list:
    raw = _PHONE_PATTERN.findall(text)
    cleaned = []
    seen: set = set()
    for p in raw:
        p = re.sub(r"[\s\-().]", "", p)
        if len(p) >= 8 and p not in seen:
            seen.add(p)
            cleaned.append(p)
    return cleaned


# ── WhatsApp number ──────────────────────────────────────────────────────────
_WA_PATTERN = re.compile(
    r"(?:wa\.me|whatsapp\.com/send[?&]phone=)[/=]?(\d+)",
    re.IGNORECASE,
)

def extract_whatsapp(urls: list) -> Optional[str]:
    for url in urls:
        m = _WA_PATTERN.search(url)
        if m:
            number = m.group(1)
            if not number.startswith("+"):
                number = "+" + number
            return number
    return None


# ── Price detection ──────────────────────────────────────────────────────────
_PRICE_PATTERN = re.compile(
    r"(\d[\d,]*(?:\.\d+)?)\s*(?:–|-|to)?\s*(\d[\d,]*(?:\.\d+)?)?\s*(SAR|ريال|SR|ر\.س)",
    re.IGNORECASE | re.UNICODE,
)

def extract_price_range(text: str) -> tuple:
    """Returns (price_range_str, currency_str)."""
    m = _PRICE_PATTERN.search(text)
    if not m:
        return None, None
    currency = m.group(3)
    lo = m.group(1).replace(",", "")
    hi = m.group(2)
    if hi:
        price_range = f"{lo}-{hi.replace(',', '')} {currency}"
    else:
        price_range = f"{lo} {currency}"
    return price_range, currency


# ── Language detection ───────────────────────────────────────────────────────
_AR_UNICODE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]")

def detect_language(html_lang: Optional[str], text: str) -> tuple:
    """Returns (lang_string, has_arabic)."""
    arabic_chars = len(_AR_UNICODE.findall(text))
    total_chars = len(re.sub(r"\s", "", text)) or 1
    arabic_ratio = arabic_chars / total_chars
    has_arabic = arabic_ratio > 0.05

    if html_lang:
        lang = html_lang.lower()
        if "ar" in lang and arabic_ratio < 0.05:
            return "en", has_arabic
        if "ar" in lang and "en" in lang:
            return "ar,en", has_arabic
        if "ar" in lang:
            return "ar,en" if arabic_ratio < 0.7 else "ar", has_arabic
        if arabic_ratio > 0.3:
            return "ar,en", has_arabic
        return "en", has_arabic

    if arabic_ratio > 0.5:
        return "ar", has_arabic
    if arabic_ratio > 0.05:
        return "ar,en", has_arabic
    return "en", has_arabic


# ── Social handle extraction ─────────────────────────────────────────────────
def instagram_handle(url: str) -> Optional[str]:
    try:
        path = urlparse(url).path.strip("/").split("?")[0].split("/")[0]
        if path and path not in ("p", "reel", "stories", "explore", "accounts"):
            return "@" + path
    except Exception:
        pass
    return None


# ── URL normalization ────────────────────────────────────────────────────────
def normalize_url(url: str) -> str:
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def same_domain(url1: str, url2: str) -> bool:
    try:
        return urlparse(url1).netloc == urlparse(url2).netloc
    except Exception:
        return False


def build_menu_url(base_url: str, href: str) -> str:
    return urljoin(base_url, href)
