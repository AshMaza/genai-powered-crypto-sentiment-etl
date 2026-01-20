import uuid
import requests
import json
import time
import os
from dotenv import load_dotenv
from schema import clean_news_data
from eventhub_client import get_eventhub_producer
from azure.eventhub import EventData
import json

load_dotenv()

def get_crypto_news():
    print("Fetching crypto news from CryptoPanic...")
    
    CRYPTOPANIC_TOKEN = os.getenv('CRYPTOPANIC_TOKEN')
    url = f"https://cryptopanic.com/api/developer/v2/posts/?auth_token={CRYPTOPANIC_TOKEN}"

    MAX_RETRIES = 3
    BACKOFF_SECONDS = 5

    for attempt in range(MAX_RETRIES):
        print(f"Attempt {attempt + 1}/{MAX_RETRIES}")
        try:
            response = requests.get(url,timeout=10)

            if response.status_code == 429 or response.status_code == 503: # Rate limit or service unavailable
                time.sleep(BACKOFF_SECONDS)
                continue
        
            response.raise_for_status() # Check if the request was successful
            
            data = response.json()
            raw_news = data.get('results', [])

            if not raw_news:
                print("No news returned from CryptoPanic")
                return []
            
            cleaned_news = clean_news_data(raw_news)

            event_envelopes = []
            for item in cleaned_news:
                event = {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "crypto_news",
                    "source": "cryptopanic",
                    "ingested_at": item["ingested_at"],
                    "payload": {
                        "title": item["title"],
                        "description": item["description"],
                        "published_at": item["published_at"]
                    }
                }

                event_envelopes.append(event)

            return event_envelopes

        except Exception as e:
            print(f"Error fetching data: {e}")
            
    return []

if __name__ == "__main__":
    event_envelopes = get_crypto_news()

    producer = get_eventhub_producer()
    batch = producer.create_batch()

    for event in event_envelopes:
        event_data = EventData(json.dumps(event))
        try:
            batch.add(event_data)
        except ValueError:
            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(event_data)

    if len(batch) > 0:
        producer.send_batch(batch)

    producer.close()

    # Print the first item clearly to verify
    if event_envelopes:
        print(f"\nSuccess! Fetched {len(event_envelopes)} articles.")
    else:
        print("No news found.")

