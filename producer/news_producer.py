import uuid
import requests
import json
import os
import sys

# Safety Import Block
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(current_dir)
sys.path.append(parent_dir)

from dotenv import load_dotenv
from GenAI.gemini import get_gemini_response
from producer.schema import clean_news_data # Optional if you use it
from azure.eventhub import EventHubProducerClient, EventData

load_dotenv()


CMC_API_KEY = os.getenv('COINMARKETCAP_API_KEY')
CRYPTOPANIC_TOKEN = os.getenv('CRYPTOPANIC_TOKEN')
EVENTHUB_CONN_STR = os.getenv('EVENTHUB_CONNECTION_STRING')
EVENTHUB_NAME = os.getenv('EVENTHUB_NAME')

def get_crypto_price(): 
    print("💰 Fetching crypto price...")
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
    headers = { "X-CMC_PRO_API_KEY": CMC_API_KEY }
    params = { "symbol": "BTC,ETH,XRP" }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json().get("data", {})
        
        clean_records = []
        for symbol, info in data.items():
            record = {
                "record_id": str(uuid.uuid4()),
                "data_type": "PRICE",
                "symbol": info["symbol"],
                "name": info["name"],
                "price": info["quote"]["USD"]["price"],
                "title": None,          
                "description": None,    
                "trend": "Neutral", 
                "published_at": info["quote"]["USD"]["last_updated"]
            }
            clean_records.append(record)
        return clean_records

    except Exception as e:
        print(f" Error fetching crypto price: {e}")
        return []

def get_enriched_news():
    print("📰 Fetching raw news from CryptoPanic...")
    url = f"https://cryptopanic.com/api/developer/v2/posts/?auth_token={CRYPTOPANIC_TOKEN}&public=true"

    try:
        response = requests.get(url, timeout=10)
        raw_data = response.json().get('results', [])

        if not raw_data: return []

        # Prepare Input for Gemini: [Title, Description, Timestamp]
        inputs_for_ai = []
        for item in raw_data:
            title = item['title']
            desc = f"Source: {item.get('domain', 'Unknown source')}" 
            timestamp = item["created_at"]
            inputs_for_ai.append([title, desc, timestamp])

        print(f"🤖 Asking Gemini to analyze {len(inputs_for_ai)} articles...")
        
        ai_response_json = get_gemini_response(inputs_for_ai)

        news_records = []
        if ai_response_json:
            parsed_data = json.loads(ai_response_json)
            analyzed_items = parsed_data.get("news_analysis", [])
            
            print(f" Gemini returned {len(analyzed_items)} analyzed events.")

            for item in analyzed_items:
                record = {
                    "record_id": str(uuid.uuid4()),
                    "data_type": "NEWS",
                    "symbol": item['symbol'],
                    "name": item['name'],
                    "price": None,
                    "title": item['title'], 
                    "description": item['description'],
                    "trend": item['prediction'], 
                    "published_at": item['published_at']
                }
                news_records.append(record)
        
        return news_records

    except Exception as e:
        print(f" Error in News/AI pipeline: {e}")
        return []

if __name__ == "__main__":
    print("🚀 Starting Single-Batch Run...")
    

    price_records = get_crypto_price()

    news_records = get_enriched_news()


    final_dataset = price_records + news_records

    if final_dataset:

        print("\n" + "="*50)
        print("🔍 PREVIEW DATA")
        print("="*50)
        print(json.dumps(final_dataset, indent=2))
        print("="*50 + "\n")


        with open("local_data_history.json", "a") as f:
            for row in final_dataset:
                f.write(json.dumps(row) + "\n")
        print("💾 Saved to 'local_data_history.json'")

        try:
            producer = EventHubProducerClient.from_connection_string(
                conn_str=EVENTHUB_CONN_STR, 
                eventhub_name=EVENTHUB_NAME
            )
            batch = producer.create_batch()
            
            print(f"📡 Sending {len(final_dataset)} events to Azure...")
            for row in final_dataset:
                batch.add(EventData(json.dumps(row)))
            
            producer.send_batch(batch)
            producer.close()
            print("✅ Data successfully sent to Databricks!")
            
        except Exception as e:
            print(f" Azure Send Failed: {e}")
    else:
        print(" No data collected.")

    print(" Process Complete. Exiting.")