import os
import time
import requests
from producer.schema import clean_price_data

def get_crypto_price(list): 
    print("\nFetching crypto price from CoinMarketCap...")
    COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY')
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY
    }
    params = {
        "symbol": f"BTC,ETH,CMC20,USDT,BNB,XRP,USDC,ADA,SOL,DOGE,TRON,SOLANA,LINK,DOT,AVAX,{list}",
        # "convert": "USD"
    }

    MAX_RETRIES = 3
    BACKOFF_SECONDS = 5

    for attempt in range(MAX_RETRIES):
        print(f"Attempt to fetch the PRICE data: {attempt + 1}/{MAX_RETRIES}")
        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 429 or response.status_code == 503: # Rate limit or service unavailable
                time.sleep(BACKOFF_SECONDS)
                continue

            response.raise_for_status()
            data = response.json()

            final_data = clean_price_data(data)
            
            print(f"Fetched and cleaned price data for {len(final_data)} cryptocurrencies.")
            return final_data
        except Exception as e:
            print(f"Error fetching crypto price: {e}")
            return []