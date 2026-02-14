from openai import OpenAI
import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import List, Literal
import json

load_dotenv()

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

gemini = OpenAI(base_url=GEMINI_BASE_URL, api_key=GEMINI_API_KEY)

class CryptoNewsItem(BaseModel):
    symbol: str = Field(..., description="The ticker symbol (e.g., BTC, ETH). Use 'N/A' if not found.")
    name: str = Field(..., description="The full name of the cryptocurrency.")
    description: str = Field(..., description="A brief summary of the specific news event with maximum 10 words.")
    prediction: Literal["Positive", "Negative", "Neutral"] = Field(..., description="The inferred market trend.")
    published_at: str = Field(..., description="The exact timestamp string provided in the input for this specific news item.")

class CryptoAnalysisResult(BaseModel):
    news_analysis: List[CryptoNewsItem]

def get_gemini_response(news_descriptions: List[List[str]]) -> str:

    try:
        # 4. The "Prompt" is now a combination of System Instruction + Schema
        response = gemini.chat.completions.create(
        model="gemini-2.5-flash", 
        messages=[
            {
                "role": "system",
                "content": """You are a cryptocurrency market analyst. 
                The user will provide a list of news items. Each item is formatted as a list: `[News Description, Timestamp]`.
                
                Your Task:
                1. Analyze the 'News Description' to extract crypto Name, Symbol, and Prediction.
                2. **CRITICAL:** You must copy the 'Timestamp' value from the input directly into the `published_at` field for that item.
                3. If a single description mentions multiple coins (e.g. Bitcoin AND Ethereum), create separate output entries for each, but assign the **SAME timestamp** to both.
                4. Ignore news about non-crypto assets (e.g. General Motors).
                """
            },
            {
                "role": "user",
                "content": json.dumps(news_descriptions)
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
    except Exception as e:
        print(f"Error communicating with Gemini API: {e}")
        return ""

    gemini_output = json.loads(response.choices[0].message.content)

    return gemini_output.get("news_analysis", [])

