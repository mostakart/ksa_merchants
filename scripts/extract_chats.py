import os
import json
import pandas as pd
import google.generativeai as genai
from supabase import create_client, Client
from pydantic import BaseModel
from typing import List, Optional
from dotenv import load_dotenv

# --- CONFIGURATION ---
# Load variables from the .env file
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Safety check to ensure keys were loaded properly
if not all([GEMINI_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
    raise ValueError("🚨 Missing API keys! Make sure your .env file is set up correctly in the same folder.")

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
    model_name="gemini-1.5-flash",
    generation_config=genai.GenerationConfig(
        response_mime_type="application/json",
        response_schema=ExtractedTranscript,
        temperature=0.1 # Low temperature for accurate extraction
    )
)

def process_tickets(csv_path: str):
    df = pd.read_csv(csv_path)
    
    # Process row by row
    for index, row in df.iterrows():
        ticket_id = row['id']
        ticket_number = row['ticket_number']
        raw_message = row['message']
        
        # Skip empty messages
        if pd.isna(raw_message) or str(raw_message).strip() == "":
            continue

        print(f"Processing Ticket: {ticket_number}...")

        # Minimal Prompt - Relying on the schema to guide the model
        prompt = f"Extract the chat flow and 'Visitor's Info' metadata from the following transcript:\n\n{raw_message}"

        try:
            # Call Gemini API
            response = model.generate_content(prompt)
            extracted_data = json.loads(response.text)
            
            # Prepare data for Supabase
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

            # Insert into Supabase
            supabase.table("chat_transcripts").insert(insert_payload).execute()
            print(f"✅ Successfully inserted {ticket_number}")

        except Exception as e:
            print(f"❌ Failed to process {ticket_number}: {e}")

if __name__ == "__main__":
    # Ensure tickets_rows.csv is in the same directory
    process_tickets("tickets_rows.csv")