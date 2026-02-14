import time
import os
import uuid
import requests
from producer.schema import clean_news_data

def get_crypto_news():
    print("Fetching crypto news from CryptoPanic...")
    
    CRYPTOPANIC_TOKEN = os.getenv('CRYPTOPANIC_TOKEN')
    url = f"https://cryptopanic.com/api/developer/v2/posts/?auth_token={CRYPTOPANIC_TOKEN}"

    MAX_RETRIES = 3
    BACKOFF_SECONDS = 5

    for attempt in range(MAX_RETRIES):
        print(f"Attempt to fetch the NEWS data: {attempt + 1}/{MAX_RETRIES}")
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

            # final_news_data = []
            # for item in cleaned_news:
            #     news = {
            #         "title": item["title"],
            #         "description": item["description"],
            #         "published_at": item["published_at"]
            #     }

                # final_news_data.append(news)

            print(f"Fetched and cleaned {len(cleaned_news)} news articles.")
            return cleaned_news

        except Exception as e:
            print(f"Error fetching data: {e}")
            
    return []