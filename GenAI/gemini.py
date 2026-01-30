from openai import OpenAI
import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import List, Literal
import json

load_dotenv()

# --- CONFIGURATION ---
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

gemini = OpenAI(base_url=GEMINI_BASE_URL, api_key=GEMINI_API_KEY)

# --- SCHEMA DEFINITION ---
class CryptoNewsItem(BaseModel):
    # 1. Reasoning field helps the AI "think" without polluting other fields
    reasoning: str = Field(..., description="Briefly explain why you chose the prediction trend.") 
    
    symbol: str = Field(..., description="The ticker symbol (e.g., BTC). Use 'N/A' if not found.")
    name: str = Field(..., description="The full name of the cryptocurrency.")
    title: str = Field(..., description="The EXACT news headline from the input. Do not alter.") 
    description: str = Field(..., description="A brief summary of the specific news event.")
    prediction: Literal["Positive", "Negative", "Neutral"] = Field(..., description="The inferred market trend.")
    published_at: str = Field(..., description="The exact timestamp string provided in the input.")

class CryptoAnalysisResult(BaseModel):
    news_analysis: List[CryptoNewsItem]

# --- MAIN FUNCTION ---
def get_gemini_response(news_inputs: List[List[str]]) -> str:
    """
    Sends a list of [Title, Description, Timestamp] to Gemini.
    Returns a JSON string matching the CryptoAnalysisResult schema.
    """
    try:
        response = gemini.chat.completions.create(
            model="gemini-2.5-flash", 
            messages=[
                {
                    "role": "system",
                    "content": """You are a crypto market analyst. 
                    The user will provide a list of news items: `[Title, Description, Timestamp]`.
                    
                    Your Task:
                    1. **Reasoning:** First, fill the 'reasoning' field with your analysis of the sentiment.
                    2. **Extraction:** Extract Name, Symbol, and Trend (Positive, Negative, Neutral).
                    3. **Copying:** You must COPY the 'Title' and 'Timestamp' EXACTLY as they appear in the input.
                    4. Ignore non-crypto news.
                    """
                },
                {
                    "role": "user",
                    "content": json.dumps(news_inputs)
                }
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "crypto_analysis",
                    "schema": CryptoAnalysisResult.model_json_schema()
                }
            }
        )
        return response.choices[0].message.content

    except Exception as e:
        print(f"Error communicating with Gemini API: {e}")
        return ""