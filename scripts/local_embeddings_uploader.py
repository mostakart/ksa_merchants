import os
import pandas as pd
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from supabase import create_client, Client

# ==========================================
# 0. إجبار البايثون يقرأ الفايل من نفس المسار
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '.env')
load_dotenv(dotenv_path=env_path)

# ==========================================
# 1. إعدادات Supabase 
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print(f"❌ خطأ: مش قادر أقرأ المفاتيح. اتأكد إنك حاططهم صح جوه الفايل ده بالظبط:\n{env_path}")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# 2. تحميل موديل الذكاء الاصطناعي على جهازك
# ==========================================
print("⏳ جاري تحميل الموديل الذكي الداعم للعربية (Local)...")
model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
print("✅ تم تحميل الموديل بنجاح!")

# مسارات ملفات الإكسيل للـ 6 مدن (حسب الصورة عندك)
excel_files = [
    "KSA_Merchants_Riyadh.xlsx",
    "KSA_Merchants_Jeddah.xlsx",
    "KSA_Merchants_Dammam.xlsx",
    "KSA_Merchants_Khobar.xlsx",
    "KSA_Merchants_Mecca.xlsx",
    "KSA_Merchants_Medina.xlsx"
]

def process_and_upload():
    total_processed = 0
    
    for file in excel_files:
        file_path = os.path.join(current_dir, file)
        if not os.path.exists(file_path):
            print(f"⚠️ تحذير: الملف {file} غير موجود، سيتم تخطيه.")
            continue

        print(f"\n📂 جاري قراءة ملف: {file}")
        xls = pd.ExcelFile(file_path)
        
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            
            for index, row in df.iterrows():
                # تنظيف البيانات
                merchant = str(row.get('Merchant', '')).strip()
                city = str(row.get('City', '')).strip()
                category = str(row.get('Category', '')).strip()
                
                if not merchant or merchant == 'nan' or merchant == '':
                    continue
                
                # صناعة النص الدلالي
                rich_text = f"تاجر {merchant} في مدينة {city} داخل {sheet_name}. "
                rich_text += f"الفئة: {category}. "
                rich_text += f"أبرز التقييمات: {str(row.get('Top Reviews', ''))}"

                # 🚀 تحويل النص لـ Vector مجاناً على جهازك
                try:
                    embedding = model.encode(rich_text).tolist()
                    
                    # تجهيز البيانات للرفع
                    data = {
                        "merchant_name": merchant,
                        "category": category,
                        "mall": sheet_name,
                        "city": city,
                        "priority": str(row.get('Priority', '')),
                        "rating": float(row.get('Rating', 0)) if pd.notna(row.get('Rating')) else 0.0,
                        "reviews": int(row.get('Reviews', 0)) if pd.notna(row.get('Reviews')) else 0,
                        "avg_price": str(row.get('Avg Price', '')).strip(),
                        "branches": int(row.get('Branches (KSA)', 1)) if pd.notna(row.get('Branches (KSA)')) else 1,
                        "phone": str(row.get('Phone', '')),
                        "top_reviews": str(row.get('Top Reviews', '')),
                        "embedding": embedding
                    }
                    
                    # رفع الداتا للجدول الموحد في Supabase
                    supabase.table("ksa_merchants_master").insert(data).execute()
                    total_processed += 1
                    print(f"✅ تم الرفع بنجاح: {merchant}")
                except Exception as e:
                    print(f"❌ فشل رفع {merchant}: {e}")

    print(f"\n🎉 مبروك! اكتملت العملية بالكامل مجاناً.")
    print(f"📊 إجمالي التجار اللي بقوا جاهزين للبحث الدلالي: {total_processed}")

if __name__ == "__main__":
    process_and_upload()