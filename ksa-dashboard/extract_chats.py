import os
import json
import time
import pandas as pd
import google.generativeai as genai
from supabase import create_client, Client
from pydantic import BaseModel
from typing import List, Optional

# ==========================================
# 1. حط المفاتيح بتاعتك هنا مباشرة بين علامات التنصيص
# ==========================================
GEMINI_API_KEY="AIzaSyAZvnA3UGs3qhEbBQIuXofNPlJSPbyTH3E"
SUPABASE_URL="https://omowdfzyudedrtcuhnvy.supabase.co"
SUPABASE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tb3dkZnp5dWRlZHJ0Y3VobnZ5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjQzNjI3OCwiZXhwIjoyMDkyMDEyMjc4fQ.kgQTvZRIrgFXTwL5wDM5oYLmDS9GtRjltE53wcpDQes"


# تهيئة الاتصال
genai.configure(api_key=GEMINI_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- PYDANTIC SCHEMAS FOR MINIMAL TOKEN USAGE ---
class ChatMessage(BaseModel):
    sender: str
    time: str
    text: str

class ChatMetadata(BaseModel):
    chat_duration: Optional[str]
    waiting_time: Optional[str]
    operating_system: Optional[str]
    browser: Optional[str]
    city: Optional[str]
    customer_info_json: Optional[str]

class ExtractedTranscript(BaseModel):
    metadata: ChatMetadata
    messages: List[ChatMessage]

# Initialize the Gemini Model with the strict schema
model = genai.GenerativeModel(
    model_name="gemini-2.5-pro",
    generation_config=genai.GenerationConfig(
        response_mime_type="application/json",
        response_schema=ExtractedTranscript,
        temperature=0.1 
    )
)

def process_tickets(csv_path: str):
    # مسار الفايل المباشر
    current_dir = os.getcwd()
    full_csv_path = os.path.join(current_dir, csv_path)
    
    if not os.path.exists(full_csv_path):
        print(f"❌ Error: Cannot find {csv_path} in {current_dir}")
        return

    df = pd.read_csv(full_csv_path)
    
    for index, row in df.iterrows():
        ticket_id = row['id']
        ticket_number = row['ticket_number']
        raw_message = row['message']
        
        if pd.isna(raw_message) or str(raw_message).strip() == "":
            continue

        print(f"Processing Ticket: {ticket_number}...")
        prompt = f"Extract the chat flow and 'Visitor's Info' metadata from the following transcript:\n\n{raw_message}"

        try:
            time.sleep(4.5) # Prevent hitting 15 RPM rate limit
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = model.generate_content(prompt)
                    break
                except Exception as e:
                    if "429" in str(e) and attempt < max_retries - 1:
                        print(f"Rate limited. Waiting 15 seconds... (Attempt {attempt+1}/{max_retries})")
                        time.sleep(15)
                    else:
                        raise e

            extracted_data = json.loads(response.text)
            
            metadata = extracted_data.get('metadata', {})
            messages = extracted_data.get('messages', [])
            
            insert_payload = {
                "ticket_id": ticket_id,
                "ticket_number": ticket_number,
                "chat_duration": metadata.get("chat_duration"),
                "device_os": metadata.get("operating_system"),
                "chat_metadata": metadata,
                "messages": messages
            }

            supabase.table("chat_transcripts").insert(insert_payload).execute()
            print(f"✅ Successfully inserted {ticket_number}")

        except Exception as e:
            print(f"❌ Failed to process {ticket_number}: {e}")

if __name__ == "__main__":
    # اسم ملف الشيت بتاعك
    process_tickets("../scripts/tickets_rows.csv")