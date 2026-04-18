"""
Core extraction logic: fetch a URL, parse HTML, return MerchantWebIntel.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, urljoin

import json
import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel

from scraper.utils import (
    build_menu_url,
    detect_language,
    extract_branch_count,
    extract_emails,
    extract_founding_year,
    extract_phones,
    extract_price_range,
    extract_whatsapp,
    instagram_handle,
    normalize_url,
)

# ── Constants ────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WaffarhaBot/1.0)",
    "Accept-Language": "ar,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

TIMEOUT = 15.0

DELIVERY_DOMAINS = {
    "has_hungerstation": "hungerstation.com",
    "has_jahez": "jahez.net",
    "has_talabat": "talabat.com",
    "has_marsool": "marsool.com",
    "has_toyo": "toyou.sa",
    "has_noon_food": "noon",
}

SOCIAL_DOMAINS: dict = {
    "instagram_url": "instagram.com",
    "tiktok_url": "tiktok.com",
    "snapchat_url": "snapchat.com",
    "twitter_url": ("twitter.com", "x.com"),
    "youtube_url": "youtube.com",
    "facebook_url": "facebook.com",
}

RESERVATION_DOMAINS = ("sevenrooms.com", "resy.com", "opentable.com", "tablecheck.com", "eat.app")

LOYALTY_KEYWORDS = re.compile(
    r"loyalty|نقاط|مكافآت|rewards?\b|program|points\b|بطاقة\s*الولاء",
    re.IGNORECASE | re.UNICODE,
)
CATERING_KEYWORDS = re.compile(
    r"catering|كيترينج|مناسبات|corporate\s*events?|events?\s*catering|ضيافة",
    re.IGNORECASE | re.UNICODE,
)
FRANCHISE_KEYWORDS = re.compile(
    r"franchise|امتياز|فرانشايز|franchising",
    re.IGNORECASE | re.UNICODE,
)
MENU_KEYWORDS = re.compile(
    r"/menu|قائمة\s*الطعام|menu\.html|our.?menu|food.?menu",
    re.IGNORECASE | re.UNICODE,
)

CAREERS_KEYWORDS = re.compile(r"/careers|/jobs|/vacancies|وظائف|توظيف", re.IGNORECASE | re.UNICODE)
PIXEL_FB = re.compile(r"fbevents\.js|fbq\(", re.IGNORECASE)
PIXEL_TIKTOK = re.compile(r"tiktok\.com/api/v1/pixel|ttq\.load", re.IGNORECASE)
PIXEL_SNAP = re.compile(r"snap\.tr", re.IGNORECASE)
PIXEL_GADS = re.compile(r"googletagmanager\.com/gtag|googleadservices\.com", re.IGNORECASE)
COPYRIGHT_YEAR = re.compile(r"(?:©|copyright).*?(20\d\d)", re.IGNORECASE)
CRM_KEYWORDS = re.compile(r"mailchimp|klaviyo", re.IGNORECASE)
ECOMMERCE_SALLA = re.compile(r"salla\.sa|cdn\.salla", re.IGNORECASE)
ECOMMERCE_ZID = re.compile(r"zid\.sa|cdn\.zid", re.IGNORECASE)
ECOMMERCE_WOO = re.compile(r"woocommerce", re.IGNORECASE)
ECOMMERCE_SHOPIFY = re.compile(r"cdn\.shopify\.com", re.IGNORECASE)
PAYMENT_APPLE = re.compile(r"apple\s*pay", re.IGNORECASE)
PAYMENT_STC = re.compile(r"stc\s*pay", re.IGNORECASE)
PAYMENT_MADA = re.compile(r"mada(?:\s*pay)?", re.IGNORECASE)
PAYMENT_TABBY = re.compile(r"tabby", re.IGNORECASE)
PAYMENT_TAMARA = re.compile(r"tamara", re.IGNORECASE)
ONLINE_ORDERING = re.compile(r"add to cart|checkout|أضف إلى السلة|اطلب الآن|order online", re.IGNORECASE | re.UNICODE)


# ── Model ────────────────────────────────────────────────────────────────────

class MerchantWebIntel(BaseModel):
    merchant_name: str
    website_url: str
    scraped_at: str

    # Delivery platforms
    has_hungerstation: bool = False
    has_jahez: bool = False
    has_talabat: bool = False
    has_marsool: bool = False
    has_toyo: bool = False
    has_noon_food: bool = False
    delivery_links: list = []

    # Social media
    instagram_url: Optional[str] = None
    tiktok_url: Optional[str] = None
    snapchat_url: Optional[str] = None
    twitter_url: Optional[str] = None
    youtube_url: Optional[str] = None
    facebook_url: Optional[str] = None
    instagram_handle: Optional[str] = None

    # Contact & booking
    email: Optional[str] = None
    whatsapp_number: Optional[str] = None
    reservation_link: Optional[str] = None
    app_store_link: Optional[str] = None
    play_store_link: Optional[str] = None

    # Brand intelligence
    founding_year: Optional[int] = None
    branch_count_on_site: Optional[int] = None
    has_loyalty_program: bool = False
    has_mobile_app: bool = False
    has_online_menu: bool = False
    has_catering: bool = False
    has_franchise_info: bool = False

    # Logo
    logo_url: Optional[str] = None
    logo_source: Optional[str] = None

    # Growth
    has_careers_page: bool = False
    
    # Marketing Pixels
    has_facebook_pixel: bool = False
    has_tiktok_pixel: bool = False
    has_snapchat_pixel: bool = False
    has_google_ads: bool = False
    active_pixels: list = []
    
    # Business Ops
    footer_copyright_year: Optional[int] = None
    site_is_maintained: bool = False
    schema_price_range: Optional[str] = None
    schema_cuisine_type: Optional[str] = None
    schema_aggregate_rating: Optional[float] = None
    
    # CRM
    has_email_signup: bool = False
    crm_platform: Optional[str] = None
    
    # Tech Stack
    cms_platform: Optional[str] = None
    uses_cloudflare: bool = False
    has_ecommerce_engine: bool = False
    payment_methods_found: list = []
    has_online_ordering: bool = False

    # Pricing signals
    price_range_detected: Optional[str] = None
    currency_found: Optional[str] = None

    # Site health
    status_code: int = 0
    is_alive: bool = False
    redirect_url: Optional[str] = None
    page_language: Optional[str] = None
    has_arabic_content: bool = False
    load_time_ms: int = 0

    # Raw signals
    phone_numbers_found: list = []
    emails_found: list = []
    meta_description: Optional[str] = None
    page_title: Optional[str] = None
    error: Optional[str] = None


# ── HTTP client factory ──────────────────────────────────────────────────────

def _make_client(verify: bool = True) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=HEADERS,
        timeout=TIMEOUT,
        follow_redirects=True,
        verify=verify,
        limits=httpx.Limits(max_connections=30, max_keepalive_connections=10),
    )


# ── Core fetch ───────────────────────────────────────────────────────────────

async def _fetch(client: httpx.AsyncClient, url: str) -> tuple:
    """Returns (response, load_time_ms)."""
    t0 = time.monotonic()
    resp = await client.get(url)
    elapsed = int((time.monotonic() - t0) * 1000)
    return resp, elapsed


# ── Link harvesting ──────────────────────────────────────────────────────────

def _harvest_links(soup: BeautifulSoup, base_url: str) -> list:
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if href.startswith(("http://", "https://")):
            links.append(href)
        elif href.startswith("//"):
            links.append("https:" + href)
        elif href.startswith("/"):
            links.append(build_menu_url(base_url, href))
    return links


def _find_social(links: list) -> dict:
    result: dict = {k: None for k in SOCIAL_DOMAINS}
    for url in links:
        url_lower = url.lower()
        for field, domain in SOCIAL_DOMAINS.items():
            if result[field] is not None:
                continue
            if isinstance(domain, tuple):
                if any(d in url_lower for d in domain):
                    result[field] = url
            else:
                if domain in url_lower:
                    result[field] = url
    return result


def _find_delivery(html: str, links: list) -> dict:
    html_lower = html.lower()
    found_links: list = []
    flags: dict = {}
    for field, domain in DELIVERY_DOMAINS.items():
        present = domain.lower() in html_lower
        flags[field] = present
        if present:
            for lnk in links:
                if domain.lower() in lnk.lower():
                    found_links.append(lnk)
    return {**flags, "delivery_links": list(dict.fromkeys(found_links))}


def _find_reservations(links: list) -> Optional[str]:
    for url in links:
        if any(d in url.lower() for d in RESERVATION_DOMAINS):
            return url
    return None


def _find_store_links(links: list) -> tuple:
    app_store = next((u for u in links if "apps.apple.com" in u.lower()), None)
    play_store = next((u for u in links if "play.google.com" in u.lower()), None)
    return app_store, play_store


def _find_menu_link(links: list, base_url: str) -> Optional[str]:
    base_domain = urlparse(base_url).netloc
    for url in links:
        parsed = urlparse(url)
        if parsed.netloc == base_domain and "/menu" in parsed.path.lower():
            return url
    return None


# ── HTML parser ──────────────────────────────────────────────────────────────

def _parse(html: str, base_url: str, status_code: int, load_ms: int,
           final_url: str, merchant_name: str, original_url: str,
           extra_html: str = "", headers: dict = None) -> MerchantWebIntel:
    headers = headers or {}

    combined_text = html + extra_html
    soup = BeautifulSoup(html, "lxml")

    # Title & meta
    page_title = soup.title.string.strip() if soup.title and soup.title.string else None
    meta_tag = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
    meta_desc = meta_tag.get("content", "").strip() if meta_tag else None  # type: ignore

    # Language
    html_tag = soup.find("html")
    html_lang = html_tag.get("lang") if html_tag else None  # type: ignore
    page_text = soup.get_text(" ", strip=True)
    lang, has_arabic = detect_language(html_lang, page_text)

    # Links
    links = _harvest_links(soup, base_url)

    # Delivery
    delivery = _find_delivery(combined_text, links)

    # Social
    social = _find_social(links)
    ig_handle = instagram_handle(social["instagram_url"]) if social["instagram_url"] else None

    # Contact
    emails = extract_emails(combined_text)
    phones = extract_phones(page_text)
    wa_urls = [u for u in links if "wa.me" in u or "whatsapp" in u.lower()]
    wa_number = extract_whatsapp(wa_urls)

    # Booking & store links
    reservation = _find_reservations(links)
    app_store, play_store = _find_store_links(links)

    # Brand signals
    founding_year = extract_founding_year(combined_text)
    branch_count = extract_branch_count(combined_text)
    has_loyalty = bool(LOYALTY_KEYWORDS.search(combined_text))
    has_catering = bool(CATERING_KEYWORDS.search(combined_text))
    has_franchise = bool(FRANCHISE_KEYWORDS.search(combined_text))
    has_mobile_app = bool(app_store or play_store)

    # Menu detection
    menu_link = _find_menu_link(links, base_url)
    has_online_menu = bool(menu_link) or bool(MENU_KEYWORDS.search(combined_text))

    # Pricing
    price_range, currency = extract_price_range(combined_text)

    # Logo
    logo_url = None
    logo_source = None
    
    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content"):
        logo_url = urljoin(base_url, og_img["content"].strip())
        logo_source = "og:image"
    
    if not logo_url:
        apple_icon = soup.find("link", rel=re.compile(r"apple-touch-icon", re.I))
        if apple_icon and apple_icon.get("href"):
            logo_url = urljoin(base_url, apple_icon["href"].strip())
            logo_source = "apple_touch_icon"
            
    if not logo_url:
        icon = soup.find("link", rel=re.compile(r"icon", re.I))
        if icon and icon.get("href"):
            logo_url = urljoin(base_url, icon["href"].strip())
            logo_source = "link_icon"
            
    if not logo_url:
        for img in soup.find_all("img"):
            cls = " ".join(img.get("class", [])).lower()
            idx = (img.get("id") or "").lower()
            if "logo" in cls or "logo" in idx:
                if img.get("src"):
                    logo_url = urljoin(base_url, img["src"].strip())
                    logo_source = "img_class_logo"
                    break

    # Advanced Extraction
    uses_cloudflare = "cloudflare" in headers.get("server", "").lower()
    
    cms_platform = None
    if "wp-content" in combined_text or "WordPress" in combined_text:
        cms_platform = "wordpress"
    elif ECOMMERCE_SHOPIFY.search(combined_text):
        cms_platform = "shopify"
    elif "wix.com" in combined_text:
        cms_platform = "wix"
    elif "squarespace.com" in combined_text:
        cms_platform = "squarespace"
        
    has_ecommerce_engine = bool(
        ECOMMERCE_SALLA.search(combined_text) or
        ECOMMERCE_ZID.search(combined_text) or
        ECOMMERCE_WOO.search(combined_text) or
        cms_platform == "shopify"
    )
    
    has_careers_page = any(CAREERS_KEYWORDS.search(lnk) for lnk in links)
    
    has_facebook_pixel = bool(PIXEL_FB.search(combined_text))
    has_tiktok_pixel = bool(PIXEL_TIKTOK.search(combined_text))
    has_snapchat_pixel = bool(PIXEL_SNAP.search(combined_text))
    has_google_ads = bool(PIXEL_GADS.search(combined_text))
    active_pixels = []
    if has_facebook_pixel: active_pixels.append("facebook")
    if has_tiktok_pixel: active_pixels.append("tiktok")
    if has_snapchat_pixel: active_pixels.append("snapchat")
    if has_google_ads: active_pixels.append("google_ads")
    
    footer_copyright_year = None
    copy_match = COPYRIGHT_YEAR.search(combined_text[-50000:])
    if copy_match:
        try:
            footer_copyright_year = int(copy_match.group(1))
        except: pass
    site_is_maintained = (footer_copyright_year is not None and footer_copyright_year >= 2023)
    
    payments = []
    if PAYMENT_APPLE.search(combined_text): payments.append("Apple Pay")
    if PAYMENT_STC.search(combined_text): payments.append("STC Pay")
    if PAYMENT_MADA.search(combined_text): payments.append("Mada")
    if PAYMENT_TABBY.search(combined_text): payments.append("Tabby")
    if PAYMENT_TAMARA.search(combined_text): payments.append("Tamara")
    has_online_ordering = bool(ONLINE_ORDERING.search(combined_text))
    
    has_email_signup = bool(re.search(r'type=["\']email["\']', combined_text, re.I) and 
                            re.search(r'subscribe|newsletter|اشترك|نشرة', combined_text, re.I))
    crm_match = CRM_KEYWORDS.search(combined_text)
    crm_platform = crm_match.group(0).lower() if crm_match else None
    
    schema_price_range = None
    schema_cuisine_type = None
    schema_aggregate_rating = None
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") in ("Restaurant", "FoodEstablishment"):
                schema_price_range = data.get("priceRange")
                schema_cuisine_type = data.get("servesCuisine")
                if isinstance(data.get("aggregateRating"), dict):
                    try:
                        schema_aggregate_rating = float(data["aggregateRating"].get("ratingValue"))
                    except: pass
        except Exception:
            pass

    # Redirect
    redirect_url = final_url if final_url != original_url else None

    return MerchantWebIntel(
        merchant_name=merchant_name,
        website_url=original_url,
        scraped_at=datetime.now(timezone.utc).isoformat(),
        # delivery
        has_hungerstation=delivery["has_hungerstation"],
        has_jahez=delivery["has_jahez"],
        has_talabat=delivery["has_talabat"],
        has_marsool=delivery["has_marsool"],
        has_toyo=delivery["has_toyo"],
        has_noon_food=delivery["has_noon_food"],
        delivery_links=delivery["delivery_links"],
        # social
        instagram_url=social["instagram_url"],
        tiktok_url=social["tiktok_url"],
        snapchat_url=social["snapchat_url"],
        twitter_url=social["twitter_url"],
        youtube_url=social["youtube_url"],
        facebook_url=social["facebook_url"],
        instagram_handle=ig_handle,
        # contact
        email=emails[0] if emails else None,
        whatsapp_number=wa_number,
        reservation_link=reservation,
        app_store_link=app_store,
        play_store_link=play_store,
        # brand
        founding_year=founding_year,
        branch_count_on_site=branch_count,
        has_loyalty_program=has_loyalty,
        has_mobile_app=has_mobile_app,
        has_online_menu=has_online_menu,
        has_catering=has_catering,
        has_franchise_info=has_franchise,
        # pricing
        price_range_detected=price_range,
        currency_found=currency,
        # logo
        logo_url=logo_url,
        logo_source=logo_source,
        # growth
        has_careers_page=has_careers_page,
        # pixels
        has_facebook_pixel=has_facebook_pixel,
        has_tiktok_pixel=has_tiktok_pixel,
        has_snapchat_pixel=has_snapchat_pixel,
        has_google_ads=has_google_ads,
        active_pixels=active_pixels,
        # ops
        footer_copyright_year=footer_copyright_year,
        site_is_maintained=site_is_maintained,
        schema_price_range=schema_price_range,
        schema_cuisine_type=schema_cuisine_type,
        schema_aggregate_rating=schema_aggregate_rating,
        # crm
        has_email_signup=has_email_signup,
        crm_platform=crm_platform,
        # tech
        cms_platform=cms_platform,
        uses_cloudflare=uses_cloudflare,
        has_ecommerce_engine=has_ecommerce_engine,
        payment_methods_found=payments,
        has_online_ordering=has_online_ordering,
        # health
        status_code=status_code,
        is_alive=200 <= status_code < 300,
        redirect_url=redirect_url,
        page_language=lang,
        has_arabic_content=has_arabic,
        load_time_ms=load_ms,
        # raw
        phone_numbers_found=phones,
        emails_found=emails,
        meta_description=meta_desc,
        page_title=page_title,
        error=None,
    )


# ── Public API ───────────────────────────────────────────────────────────────

async def scrape(url: str, merchant_name: str = "") -> MerchantWebIntel:
    """
    Fetch URL, extract intel. Returns MerchantWebIntel with error set on failure.
    Never raises — all exceptions are caught.
    """
    original_url = normalize_url(url)

    async def _attempt(verify: bool) -> MerchantWebIntel:
        async with _make_client(verify=verify) as client:
            resp, load_ms = await _fetch(client, original_url)
            final_url = str(resp.url)
            html = resp.text[:500000]

            extra_html = ""
            soup_tmp = BeautifulSoup(html, "lxml")
            links_tmp = _harvest_links(soup_tmp, final_url)
            menu_link = _find_menu_link(links_tmp, final_url)
            if menu_link and menu_link != final_url:
                try:
                    menu_resp, _ = await _fetch(client, menu_link)
                    if 200 <= menu_resp.status_code < 300:
                        extra_html = menu_resp.text[:500000]
                except Exception:
                    pass

            return _parse(
                html=html,
                base_url=final_url,
                status_code=resp.status_code,
                load_ms=load_ms,
                final_url=final_url,
                merchant_name=merchant_name,
                original_url=original_url,
                extra_html=extra_html,
                headers=dict(resp.headers)
            )

    try:
        return await _attempt(verify=True)
    except Exception as exc:
        exc_str = str(exc).lower()
        # Retry without SSL verification for cert errors
        if any(k in exc_str for k in ("ssl", "certificate", "cert", "[ssl]")):
            try:
                return await _attempt(verify=False)
            except Exception as exc2:
                return _error_result(original_url, merchant_name, str(exc2))
        if "timeout" in exc_str or isinstance(exc, httpx.TimeoutException):
            return _error_result(original_url, merchant_name, "timeout")
        return _error_result(original_url, merchant_name, str(exc))


def _error_result(url: str, merchant_name: str, error: str) -> MerchantWebIntel:
    return MerchantWebIntel(
        merchant_name=merchant_name,
        website_url=url,
        scraped_at=datetime.now(timezone.utc).isoformat(),
        status_code=0,
        is_alive=False,
        error=error,
    )
