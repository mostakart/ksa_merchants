# analyze_ksa_merchants_v2.py
# KSA Merchant Market Analysis — Enhanced Version
# Fixes: price imputation, skewed reviews, opening hours, contact score, geo clusters
#
# Run: python analyze_ksa_merchants_v2.py KSA_Merchants_Phase2.xlsx
# Optional: GEMINI_API_KEY=your_key python analyze_ksa_merchants_v2.py ...

import sys, os, re, json, math, time, warnings
import numpy as np
import pandas as pd
from pathlib import Path
from collections import Counter

warnings.filterwarnings("ignore")

# ── Gemini Setup ───────────────────────────────────────────────────────────────
try:
    import google.generativeai as genai
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        GEMINI_MODEL = genai.GenerativeModel("gemini-1.5-flash")
        GEMINI_AVAILABLE = True
        print("✅ Gemini connected")
    else:
        GEMINI_AVAILABLE = False
        print("⚠️  No GEMINI_API_KEY — LLM analysis skipped")
except ImportError:
    GEMINI_AVAILABLE = False


# ══════════════════════════════════════════════════════════════════════════════
# 1. LOAD & CLEAN
# ══════════════════════════════════════════════════════════════════════════════
def load_and_clean(filepath: str) -> pd.DataFrame:
    print(f"\n📂 Loading: {filepath}")
    xl = pd.ExcelFile(filepath)
    frames = [xl.parse(s) for s in xl.sheet_names]
    df = pd.concat(frames, ignore_index=True)
    df.columns = [c.strip() for c in df.columns]

    # Rename columns to standard names
    rename = {
        "Merchant": "merchant", "Mall": "mall", "City": "city",
        "Category": "category", "Rating": "rating", "Reviews": "reviews",
        "Avg Price": "avg_price", "Branches (KSA)": "branches",
        "Phone": "phone", "Website": "website",
        "Opening Hours": "opening_hours", "Lat": "lat", "Lng": "lng",
        "Priority": "priority", "Top Reviews": "reviews_text",
        "OSM Cuisine": "cuisine",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Types
    df["rating"]   = pd.to_numeric(df.get("rating"),   errors="coerce")
    df["reviews"]  = pd.to_numeric(df.get("reviews"),  errors="coerce").fillna(0).astype(int)
    df["branches"] = pd.to_numeric(df.get("branches"), errors="coerce").fillna(1).astype(int)
    df["lat"]      = pd.to_numeric(df.get("lat"),      errors="coerce")
    df["lng"]      = pd.to_numeric(df.get("lng"),      errors="coerce")

    # Clean strings
    df["merchant"] = df["merchant"].astype(str).str.strip()
    df["mall"]     = df["mall"].astype(str).str.strip()
    df["city"]     = df.get("city", pd.Series(["Riyadh"]*len(df))).fillna("Riyadh")

    # Priority → clean
    def clean_priority(p):
        p = str(p)
        if "High" in p or "🔴" in p: return "High"
        if "Low"  in p or "🟢" in p: return "Low"
        return "Medium"
    df["priority"] = df["priority"].apply(clean_priority)

    print(f"   ✅ {len(df)} merchants | {df['mall'].nunique()} malls | {df['category'].nunique()} categories")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 2. PRICE — SMART IMPUTATION
# ══════════════════════════════════════════════════════════════════════════════
# Category-level price medians based on Saudi market data
CATEGORY_PRICE_DEFAULTS = {
    "Casual Dining":     65.0,   # typical sit-down restaurant
    "Fast Food":         35.0,   # quick service
    "Cafes":             45.0,   # coffee + light bites
    "Desserts & Sweets": 30.0,   # sweets shops
    "Other":             50.0,
}

def parse_price(p):
    if pd.isna(p) or str(p).strip() in ["", "nan", "None"]: return None
    nums = re.findall(r"\d+", str(p))
    if len(nums) >= 2: return (int(nums[0]) + int(nums[1])) / 2
    if len(nums) == 1: return int(nums[0])
    return None

def price_segment(mid):
    if pd.isna(mid): return "Unknown"
    if mid < 40:    return "Budget (<40 SAR)"
    if mid < 80:    return "Mid (40–80 SAR)"
    if mid < 150:   return "Premium (80–150 SAR)"
    return "Luxury (150+ SAR)"

def enrich_price(df: pd.DataFrame) -> pd.DataFrame:
    df["price_mid_raw"]    = df["avg_price"].apply(parse_price)
    df["price_has_data"]   = df["price_mid_raw"].notna()

    # Impute missing: use category median from actual data, fallback to defaults
    cat_medians = df.groupby("category")["price_mid_raw"].median().to_dict()
    def impute(row):
        if pd.notna(row["price_mid_raw"]): return row["price_mid_raw"]
        return cat_medians.get(row["category"],
               CATEGORY_PRICE_DEFAULTS.get(row["category"], 50.0))

    df["price_mid"]      = df.apply(impute, axis=1)
    df["price_segment"]  = df["price_mid"].apply(price_segment)
    df["price_imputed"]  = ~df["price_has_data"]  # True = was imputed
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 3. OPENING HOURS ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
def analyze_opening_hours(hours_str: str) -> dict:
    if pd.isna(hours_str) or not str(hours_str).strip():
        return {"opens_late_night": False, "opens_early": False,
                "days_open": 0, "avg_hours_per_day": 0, "is_24h": False}

    h = str(hours_str)
    is_24h = "24" in h or "مفتوح على مدار" in h

    # Count days mentioned
    days = ["السبت","الأحد","الاثنين","الثلاثاء","الأربعاء","الخميس","الجمعة"]
    days_open = sum(1 for d in days if d in h)

    # Late night: closes after midnight (12:00 ص = midnight, or 1:00 ص etc.)
    opens_late = bool(re.search(r"1[12]:\d+ ص|[1-4]:\d+ ص", h))

    # Early morning: opens before 8am
    opens_early = bool(re.search(r"[5-7]:\d+ ص|8:00 ص", h))

    return {
        "opens_late_night": opens_late,
        "opens_early":      opens_early,
        "days_open":        days_open if days_open > 0 else 7,
        "is_24h":           is_24h,
    }

def enrich_hours(df: pd.DataFrame) -> pd.DataFrame:
    hours_data = df["opening_hours"].apply(analyze_opening_hours)
    df["opens_late_night"] = hours_data.apply(lambda x: x["opens_late_night"])
    df["opens_early"]      = hours_data.apply(lambda x: x["opens_early"])
    df["days_open"]        = hours_data.apply(lambda x: x["days_open"])
    df["is_24h"]           = hours_data.apply(lambda x: x["is_24h"])
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 4. CONTACT & PROFILE COMPLETENESS SCORE
# ══════════════════════════════════════════════════════════════════════════════
def contact_completeness(row) -> dict:
    """Score 0–100 how complete a merchant's profile is."""
    score = 0
    breakdown = {}

    # Core data (50 pts)
    if pd.notna(row.get("rating")):        score += 15; breakdown["rating"] = 15
    if int(row.get("reviews", 0)) > 0:     score += 15; breakdown["reviews"] = 15
    if row.get("price_has_data", False):   score += 10; breakdown["price"] = 10
    if pd.notna(row.get("lat")):           score += 10; breakdown["location"] = 10

    # Contact (30 pts)
    phone   = str(row.get("phone", ""))
    website = str(row.get("website", ""))
    if phone and phone not in ["nan", "", "None"]:   score += 15; breakdown["phone"] = 15
    if website and website not in ["nan", "", "None"]: score += 15; breakdown["website"] = 15

    # Reviews content (20 pts)
    reviews_text = str(row.get("reviews_text", ""))
    if len(reviews_text) > 50:             score += 10; breakdown["reviews_text"] = 10
    if pd.notna(row.get("opening_hours")): score += 10; breakdown["hours"] = 10

    return {"profile_score": score, "profile_breakdown": str(breakdown)}

def enrich_contact(df: pd.DataFrame) -> pd.DataFrame:
    contact_data = df.apply(contact_completeness, axis=1)
    df["profile_score"]     = contact_data.apply(lambda x: x["profile_score"])
    df["profile_breakdown"] = contact_data.apply(lambda x: x["profile_breakdown"])

    def contact_tier(score):
        if score >= 80: return "Complete"
        if score >= 50: return "Partial"
        return "Sparse"
    df["profile_tier"] = df["profile_score"].apply(contact_tier)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 5. ARABIC REVIEW NLP
# ══════════════════════════════════════════════════════════════════════════════
POSITIVE_KEYWORDS = [
    "لذيذ","لذيذة","رائع","رائعة","ممتاز","ممتازة","جميل","جميلة","نظيف","نظيفة",
    "سريع","سريعة","مذهل","مذهلة","إبداع","طازج","طازجة","هادئ","هادئة","مريح",
    "أنصح","ننصح","أفضل","مفضل","أحسن","جودة","يستاهل","يستحق","احترافي","متميز",
    "خدمة ممتازة","أجواء رائعة","تجربة رائعة","تنوع","اهتمام","دقة","كرم","اللذاذة",
    "هيبة","راقي","راقية","مبدع","مبدعة","خلاقية","شهي","شهية","ناجح","مميز",
]
NEGATIVE_KEYWORDS = [
    "بطيء","بطيئة","انتظار","انتظرنا","غالي","غالية","سيء","سيئة","مزعج","مزعجة",
    "بارد","باردة","ضوضاء","صغير","ضيق","مشكلة","شكوى","خيبة","متأخر","قديم",
    "قذر","قذرة","مكلف","مخيب","مخيبة","لا أنصح","لا ننصح","مؤسف","محبط","طويل",
    "أسعار مرتفعة","جودة رديئة","لن أعود","ما أرجع","مشكلة","خدمة سيئة","ضعيف",
    "بدون طعم","طعم سيء","رديء","مخيبة للآمال","بالة","سوء","فوضى","تأخر",
]

def analyze_reviews_nlp(text: str) -> dict:
    if pd.isna(text) or len(str(text).strip()) < 10:
        return {"pos_count":0,"neg_count":0,"sentiment_ratio":None,
                "top_pos_kw":"","top_neg_kw":"","sentiment_label":"Unknown",
                "review_length":0,"mentions_delivery":False,"mentions_price":False}

    t = str(text)
    pos_found = [kw for kw in POSITIVE_KEYWORDS if kw in t]
    neg_found = [kw for kw in NEGATIVE_KEYWORDS if kw in t]
    total = len(pos_found) + len(neg_found)
    ratio = len(pos_found) / total if total > 0 else 0.5

    # Extra signals
    mentions_delivery = any(w in t for w in ["توصيل","ديليفري","delivery","أوردر","طلب"])
    mentions_price    = any(w in t for w in ["سعر","أسعار","غالي","رخيص","قيمة","ثمن"])
    mentions_wait     = any(w in t for w in ["انتظار","وقت","بطيء","دقيقة","ساعة"])

    label = "Unknown"
    if ratio >= 0.7:  label = "Positive"
    elif ratio >= 0.4: label = "Mixed"
    else:              label = "Negative"

    return {
        "pos_count":          len(pos_found),
        "neg_count":          len(neg_found),
        "sentiment_ratio":    round(ratio, 2),
        "top_pos_kw":         ", ".join(pos_found[:3]),
        "top_neg_kw":         ", ".join(neg_found[:3]),
        "sentiment_label":    label,
        "review_length":      len(t),
        "mentions_delivery":  mentions_delivery,
        "mentions_price":     mentions_price,
        "mentions_wait":      mentions_wait,
    }

def enrich_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    nlp_data = df["reviews_text"].apply(analyze_reviews_nlp)
    for key in ["pos_count","neg_count","sentiment_ratio","top_pos_kw",
                "top_neg_kw","sentiment_label","review_length",
                "mentions_delivery","mentions_price"]:
        df[key] = nlp_data.apply(lambda x: x[key])
    df["mentions_wait"] = nlp_data.apply(lambda x: x.get("mentions_wait", False))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 6. GEO CLUSTERING (using coordinates)
# ══════════════════════════════════════════════════════════════════════════════
def haversine(lat1, lng1, lat2, lng2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlng/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def assign_geo_zone(lat: float, lng: float) -> str:
    """Classify Riyadh geographic zones."""
    if pd.isna(lat) or pd.isna(lng): return "Unknown"
    # North Riyadh
    if lat > 24.75: return "North Riyadh"
    # South Riyadh
    if lat < 24.60: return "South Riyadh"
    # East Riyadh
    if lng > 46.75: return "East Riyadh"
    # West Riyadh
    if lng < 46.55: return "West Riyadh"
    return "Central Riyadh"

def enrich_geo(df: pd.DataFrame) -> pd.DataFrame:
    df["geo_zone"] = df.apply(lambda r: assign_geo_zone(r["lat"], r["lng"]), axis=1)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 7. BD PRIORITY SCORE v2 — Enhanced formula
# ══════════════════════════════════════════════════════════════════════════════
def compute_bd_score(df: pd.DataFrame) -> pd.DataFrame:
    max_reviews = df["reviews"].max() or 1

    def bd_score(row):
        r   = float(row.get("rating") or 0)
        v   = int(row.get("reviews") or 0)
        sr  = float(row.get("sentiment_ratio") or 0.5)
        b   = min(int(row.get("branches") or 1), 13)
        p   = {"High":1.0,"Medium":0.5,"Low":0.0}.get(row.get("priority","Medium"), 0.5)
        ps  = float(row.get("profile_score") or 0) / 100
        del_ = 1.1 if row.get("mentions_delivery") else 1.0  # delivery bonus

        rating_score    = (r / 5) * 25
        review_score    = (math.log10(v+1) / math.log10(max_reviews+1)) * 20
        sentiment_score = sr * 15
        branch_score    = (b / 13) * 15
        priority_score  = p * 10
        profile_score   = ps * 10
        online_bonus    = 5 if row.get("website") and str(row.get("website")) not in ["nan",""] else 0

        raw = (rating_score + review_score + sentiment_score +
               branch_score + priority_score + profile_score + online_bonus) * del_
        return round(min(raw, 100), 2)

    df["bd_priority_score"] = df.apply(bd_score, axis=1)

    def tier(s):
        if s >= 75: return "Tier 1 — Top Target"
        if s >= 50: return "Tier 2 — Mid Target"
        return "Tier 3 — Low Priority"
    df["tier"] = df["bd_priority_score"].apply(tier)

    def quadrant(row):
        med_r = df["rating"].median()
        med_v = df["reviews"].median()
        r, v = row["rating"], row["reviews"]
        if pd.isna(r): return "Unknown"
        if r >= med_r and v >= med_v: return "⭐ Star"
        if r >= med_r and v <  med_v: return "💎 Hidden Gem"
        if r <  med_r and v >= med_v: return "⚠️ Risky"
        return "❌ Weak"
    df["quadrant"] = df.apply(quadrant, axis=1)

    def rating_bucket(r):
        if pd.isna(r): return "Unknown"
        if r < 3.5:  return "Poor (<3.5)"
        if r < 4.0:  return "Average (3.5–4.0)"
        if r < 4.3:  return "Good (4.0–4.3)"
        if r < 4.6:  return "Great (4.3–4.6)"
        return "Excellent (4.6–5.0)"
    df["rating_bucket"] = df["rating"].apply(rating_bucket)

    def review_bucket(v):
        if v < 100:   return "Micro (<100)"
        if v < 500:   return "Small (100–500)"
        if v < 2000:  return "Medium (500–2K)"
        if v < 5000:  return "Large (2K–5K)"
        return "Viral (>5K)"
    df["review_bucket"] = df["reviews"].apply(review_bucket)

    return df


# ══════════════════════════════════════════════════════════════════════════════
# 8. GEMINI — PER MERCHANT (top 200 by BD score)
# ══════════════════════════════════════════════════════════════════════════════
def gemini_analyze_merchant(row: dict) -> dict:
    if not GEMINI_AVAILABLE: return {}
    reviews = str(row.get("reviews_text",""))[:600]
    if len(reviews) < 20: return {}

    prompt = f"""You are a merchant acquisition analyst for a food delivery platform in Saudi Arabia.
Analyze this F&B merchant. Return ONLY valid JSON, no markdown:

Merchant: {row.get('merchant','')}
Category: {row.get('category','')}
Rating: {row.get('rating','')}/5 ({row.get('reviews','')} reviews)
Price range: {row.get('avg_price','')}
Has website: {bool(row.get('website',''))}
Customer reviews (Arabic): {reviews}

Return:
{{
  "pain_points": ["max 3 specific pain points from reviews"],
  "strengths": ["max 3 specific strengths from reviews"],
  "acquisition_pitch": "one sentence why platform should/shouldn't onboard them",
  "online_readiness": "low|medium|high",
  "delivery_potential": "low|medium|high",
  "competitor_threat": "low|medium|high",
  "gemini_tier": "tier1|tier2|tier3"
}}"""

    try:
        resp = GEMINI_MODEL.generate_content(prompt)
        text = re.sub(r"```json|```","",resp.text).strip()
        return json.loads(text)
    except:
        return {}

def run_gemini_batch(df: pd.DataFrame, top_n: int = 200) -> pd.DataFrame:
    cols = ["gemini_pain_points","gemini_strengths","gemini_pitch",
            "gemini_online_readiness","gemini_delivery_potential",
            "gemini_competitor_threat","gemini_tier"]
    for c in cols: df[c] = ""

    if not GEMINI_AVAILABLE:
        print("⏭️  Skipping Gemini (no API key)")
        return df

    top_idx = df.nlargest(top_n, "bd_priority_score").index.tolist()
    print(f"\n🤖 Gemini analyzing top {top_n} merchants...")

    for i, idx in enumerate(top_idx):
        result = gemini_analyze_merchant(df.loc[idx].to_dict())
        df.at[idx, "gemini_pain_points"]          = "; ".join(result.get("pain_points", []))
        df.at[idx, "gemini_strengths"]            = "; ".join(result.get("strengths", []))
        df.at[idx, "gemini_pitch"]                = result.get("acquisition_pitch", "")
        df.at[idx, "gemini_online_readiness"]     = result.get("online_readiness", "")
        df.at[idx, "gemini_delivery_potential"]   = result.get("delivery_potential", "")
        df.at[idx, "gemini_competitor_threat"]    = result.get("competitor_threat", "")
        df.at[idx, "gemini_tier"]                 = result.get("gemini_tier", "")
        if (i+1) % 20 == 0:
            print(f"   [{i+1}/{top_n}] ✅  cost ~${(i+1)*0.001:.3f}")
        time.sleep(0.4)

    print("   ✅ Gemini done")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 9. AGGREGATED OUTPUTS
# ══════════════════════════════════════════════════════════════════════════════
def build_mall_metrics(df: pd.DataFrame) -> pd.DataFrame:
    agg = df.groupby("mall").agg(
        merchant_count        =("merchant","count"),
        avg_rating            =("rating","mean"),
        avg_reviews           =("reviews","mean"),
        total_reviews         =("reviews","sum"),
        avg_bd_score          =("bd_priority_score","mean"),
        max_bd_score          =("bd_priority_score","max"),
        pct_high_priority     =("priority", lambda x:(x=="High").mean()*100),
        pct_tier1             =("tier",     lambda x:(x.str.startswith("Tier 1")).mean()*100),
        pct_positive_sentiment=("sentiment_label",lambda x:(x=="Positive").mean()*100),
        pct_with_price        =("price_has_data","mean"),
        pct_with_phone        =("phone",    lambda x:x.notna().mean()*100),
        pct_with_website      =("website",  lambda x:x.notna().mean()*100),
        unique_categories     =("category","nunique"),
        avg_branches          =("branches","mean"),
        tier1_count           =("tier",     lambda x:(x.str.startswith("Tier 1")).sum()),
        star_merchants        =("quadrant", lambda x:(x=="⭐ Star").sum()),
        hidden_gems           =("quadrant", lambda x:(x=="💎 Hidden Gem").sum()),
        avg_profile_score     =("profile_score","mean"),
        pct_late_night        =("opens_late_night","mean"),
        delivery_mentions     =("mentions_delivery","sum"),
    ).round(2)

    # Top 3 merchants by reviews
    def top3(g):
        return " | ".join(g.nlargest(3,"reviews")["merchant"].tolist())
    agg["top3_merchants"] = df.groupby("mall").apply(top3)

    # Category breakdown
    cat_pivot = df.groupby(["mall","category"]).size().unstack(fill_value=0)
    cat_pivot.columns = [f"n_{c.lower().replace(' ','_').replace('&','')}" for c in cat_pivot.columns]
    agg = agg.join(cat_pivot)

    # Whitespace opportunity
    n_cats = df["category"].nunique()
    agg["whitespace_gaps"] = n_cats - agg["unique_categories"]
    agg["whitespace_score"] = (agg["whitespace_gaps"] * agg["avg_bd_score"]).round(2)

    return agg.reset_index().sort_values("avg_bd_score", ascending=False)


def build_category_metrics(df: pd.DataFrame) -> pd.DataFrame:
    agg = df.groupby("category").agg(
        merchant_count        =("merchant","count"),
        avg_rating            =("rating","mean"),
        avg_reviews           =("reviews","mean"),
        total_reviews         =("reviews","sum"),
        avg_bd_score          =("bd_priority_score","mean"),
        avg_branches          =("branches","mean"),
        pct_high_priority     =("priority",  lambda x:(x=="High").mean()*100),
        pct_tier1             =("tier",      lambda x:(x.str.startswith("Tier 1")).mean()*100),
        pct_positive          =("sentiment_label",lambda x:(x=="Positive").mean()*100),
        avg_price             =("price_mid","mean"),
        pct_with_delivery_mention=("mentions_delivery","mean"),
        avg_profile_score     =("profile_score","mean"),
    ).round(2)

    agg["dominant_price_segment"] = df.groupby("category")["price_segment"].agg(
        lambda x: x.value_counts().index[0] if len(x)>0 else "Unknown"
    )
    def top_pain(g):
        all_kw = " ".join(g.dropna().tolist())
        words = [w.strip() for w in all_kw.split(",") if len(w.strip())>2]
        return " | ".join([w for w,_ in Counter(words).most_common(3)])
    agg["top_pain_points"] = df.groupby("category")["top_neg_kw"].apply(top_pain)
    agg["top_strengths"]   = df.groupby("category")["top_pos_kw"].apply(top_pain)

    return agg.reset_index().sort_values("avg_bd_score", ascending=False)


def build_whitespace_gaps(df: pd.DataFrame) -> pd.DataFrame:
    all_malls = df["mall"].unique()
    all_cats  = df["category"].unique()
    existing  = set(zip(df["mall"], df["category"]))
    mall_scores = df.groupby("mall")["bd_priority_score"].mean().to_dict()
    mall_review = df.groupby("mall")["reviews"].sum().to_dict()

    gaps = []
    for mall in all_malls:
        for cat in all_cats:
            if (mall, cat) not in existing:
                score = mall_scores.get(mall, 0)
                revs  = mall_review.get(mall, 0)
                gaps.append({
                    "mall":                mall,
                    "missing_category":    cat,
                    "mall_avg_bd_score":   round(score, 2),
                    "mall_total_reviews":  revs,
                    "opportunity_score":   round(score * math.log10(revs+1), 2),
                    "recommendation":      f"No {cat} in {mall} — high-traffic gap",
                })

    return pd.DataFrame(gaps).sort_values("opportunity_score", ascending=False)


def build_keyword_corpus(df: pd.DataFrame) -> pd.DataFrame:
    pos_all, neg_all = [], []
    for _, row in df.iterrows():
        pos_all.extend([w.strip() for w in str(row.get("top_pos_kw","")).split(",") if w.strip()])
        neg_all.extend([w.strip() for w in str(row.get("top_neg_kw","")).split(",") if w.strip()])

    rows = []
    for kw, freq in Counter(pos_all).most_common(30):
        rows.append({"keyword":kw,"frequency":freq,"sentiment":"positive"})
    for kw, freq in Counter(neg_all).most_common(30):
        rows.append({"keyword":kw,"frequency":freq,"sentiment":"negative"})
    return pd.DataFrame(rows)


def build_geo_metrics(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("geo_zone").agg(
        merchant_count=("merchant","count"),
        avg_rating    =("rating","mean"),
        avg_bd_score  =("bd_priority_score","mean"),
        total_reviews =("reviews","sum"),
        unique_malls  =("mall","nunique"),
        tier1_count   =("tier",lambda x:(x.str.startswith("Tier 1")).sum()),
    ).round(2).reset_index().sort_values("avg_bd_score", ascending=False)


def build_summary(df: pd.DataFrame, mall_metrics: pd.DataFrame) -> dict:
    return {
        "total_merchants":          int(len(df)),
        "total_malls":              int(df["mall"].nunique()),
        "total_categories":         int(df["category"].nunique()),
        "avg_rating":               round(float(df["rating"].mean()),2),
        "median_rating":            round(float(df["rating"].median()),2),
        "avg_reviews":              round(float(df["reviews"].mean()),0),
        "median_reviews":           round(float(df["reviews"].median()),0),
        "avg_bd_score":             round(float(df["bd_priority_score"].mean()),2),
        "pct_tier1":                round(float(df["tier"].str.startswith("Tier 1").mean()*100),1),
        "pct_high_priority":        round(float((df["priority"]=="High").mean()*100),1),
        "pct_positive_sentiment":   round(float((df["sentiment_label"]=="Positive").mean()*100),1),
        "pct_with_phone":           round(float(df["phone"].notna().mean()*100),1),
        "pct_with_website":         round(float(df["website"].notna().mean()*100),1),
        "pct_with_price_data":      round(float(df["price_has_data"].mean()*100),1),
        "pct_coords_available":     100.0,
        "merchants_multi_branch":   int((df["branches"]>1).sum()),
        "merchants_delivery_mention":int(df["mentions_delivery"].sum()),
        "whitespace_gaps":          int(len(df["mall"].unique())*len(df["category"].unique()) - len(df.groupby(["mall","category"]).size())),
        "priority_dist":            df["priority"].value_counts().to_dict(),
        "tier_dist":                df["tier"].value_counts().to_dict(),
        "category_dist":            df["category"].value_counts().to_dict(),
        "quadrant_dist":            df["quadrant"].value_counts().to_dict(),
        "price_segment_dist":       df["price_segment"].value_counts().to_dict(),
        "rating_bucket_dist":       df["rating_bucket"].value_counts().to_dict(),
        "review_bucket_dist":       df["review_bucket"].value_counts().to_dict(),
        "geo_zone_dist":            df["geo_zone"].value_counts().to_dict(),
        "top10_bd_merchants":       df.nlargest(10,"bd_priority_score")[["merchant","mall","category","rating","reviews","bd_priority_score","tier"]].to_dict("records"),
        "top5_malls":               mall_metrics.nlargest(5,"avg_bd_score")[["mall","merchant_count","avg_rating","avg_bd_score"]].to_dict("records"),
        "top5_hidden_gems":         df[(df["quadrant"]=="💎 Hidden Gem")].nlargest(5,"bd_priority_score")[["merchant","mall","rating","reviews","bd_priority_score"]].to_dict("records"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# 10. GEMINI EXECUTIVE SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
def gemini_market_summary(summary: dict) -> str:
    if not GEMINI_AVAILABLE: return ""
    prompt = f"""You are a senior BD analyst for a food delivery platform entering Saudi Arabia.

Market data from {summary['total_merchants']} F&B merchants in {summary['total_malls']} malls, Riyadh:
- Avg rating: {summary['avg_rating']}/5 (median: {summary['median_rating']})
- Avg reviews: {summary['avg_reviews']} (median: {summary['median_reviews']})
- % Tier 1 targets: {summary['pct_tier1']}%
- % Positive sentiment: {summary['pct_positive_sentiment']}%
- Category breakdown: {summary['category_dist']}
- % with delivery mentions in reviews: {round(summary['merchants_delivery_mention']/summary['total_merchants']*100,1)}%
- Whitespace acquisition gaps: {summary['whitespace_gaps']}
- Top 10 targets: {[m['merchant'] for m in summary['top10_bd_merchants']]}
- Hidden gems: {[m['merchant'] for m in summary['top5_hidden_gems']]}

Write a 6-bullet Arabic executive summary covering:
1. حجم الفرصة في السوق
2. أفضل الفئات والمولات للاستهداف
3. أبرز المخاطر التنافسية
4. التسلسل المقترح للاستهداف (أي فئات/مولات أولاً)
5. الفجوات في السوق (whitespace)
6. توصية نهائية بعدد المرشحين الأوائل

اكتب باللغة العربية. كن محدداً بالأرقام."""

    try:
        resp = GEMINI_MODEL.generate_content(prompt)
        return resp.text
    except Exception as e:
        return f"Gemini failed: {e}"


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_ksa_merchants_v2.py <file.xlsx>")
        sys.exit(1)

    filepath   = sys.argv[1]
    output_dir = Path("analysis_output")
    output_dir.mkdir(exist_ok=True)

    print("\n" + "="*70)
    print("   KSA MERCHANT MARKET ANALYSIS v2")
    print("   BD + Acquisition Intelligence")
    print("="*70)

    df = load_and_clean(filepath)
    print("\n⚙️  Enriching data...")
    df = enrich_price(df)
    df = enrich_hours(df)
    df = enrich_contact(df)
    df = enrich_sentiment(df)
    df = enrich_geo(df)
    df = compute_bd_score(df)
    df = run_gemini_batch(df, top_n=1385)

    print("\n📊 Building aggregated metrics...")
    mall_metrics     = build_mall_metrics(df)
    category_metrics = build_category_metrics(df)
    whitespace_gaps  = build_whitespace_gaps(df)
    keyword_corpus   = build_keyword_corpus(df)
    geo_metrics      = build_geo_metrics(df)
    summary          = build_summary(df, mall_metrics)

    print("\n🤖 Generating Gemini executive summary...")
    exec_summary = gemini_market_summary(summary)
    if exec_summary:
        summary["gemini_executive_summary_ar"] = exec_summary

    # Save outputs
    print("\n💾 Saving files...")
    df.to_csv(output_dir/"merchants_enriched.csv",    index=False, encoding="utf-8-sig")
    mall_metrics.to_csv(output_dir/"metrics_by_mall.csv",         index=False, encoding="utf-8-sig")
    category_metrics.to_csv(output_dir/"metrics_by_category.csv", index=False, encoding="utf-8-sig")
    whitespace_gaps.to_csv(output_dir/"whitespace_gaps.csv",       index=False, encoding="utf-8-sig")
    keyword_corpus.to_csv(output_dir/"top_keywords_corpus.csv",    index=False, encoding="utf-8-sig")
    geo_metrics.to_csv(output_dir/"metrics_by_geo_zone.csv",       index=False, encoding="utf-8-sig")
    with open(output_dir/"summary_metrics.json","w",encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # ── Human Summary ──────────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("✅ ANALYSIS COMPLETE")
    print("="*70)
    print(f"\n🏪 MARKET: {summary['total_merchants']} merchants | {summary['total_malls']} malls")
    print(f"⭐ QUALITY: avg {summary['avg_rating']}/5 | median reviews {summary['median_reviews']}")
    print(f"🎯 PIPELINE: {summary['pct_tier1']}% Tier 1 | {summary['pct_high_priority']}% High Priority")
    print(f"😊 SENTIMENT: {summary['pct_positive_sentiment']}% positive")
    print(f"📦 DELIVERY READY: {summary['merchants_delivery_mention']} mention delivery")
    print(f"🕳️  GAPS: {summary['whitespace_gaps']} whitespace slots")

    print(f"\n🏆 TOP 10 BD TARGETS:")
    for m in summary["top10_bd_merchants"]:
        print(f"   {m['merchant'][:35]:<35} | {m['mall'][:18]:<18} | Score: {m['bd_priority_score']}")

    print(f"\n💎 TOP HIDDEN GEMS:")
    for m in summary["top5_hidden_gems"]:
        print(f"   {m['merchant'][:35]:<35} | Rating: {m['rating']} | Reviews: {m['reviews']}")

    print(f"\n📂 OUTPUT FILES:")
    for f in sorted(output_dir.iterdir()):
        print(f"   {f.name:<45} {f.stat().st_size//1024} KB")

    if exec_summary:
        print(f"\n🤖 GEMINI EXECUTIVE SUMMARY (Arabic):")
        print(exec_summary)

    print("\n" + "="*70)

if __name__ == "__main__":
    main()
