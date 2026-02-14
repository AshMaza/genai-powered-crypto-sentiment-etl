import time

REQUIRED_FIELDS = ["description", "published_at"]

def validate_news_item(item):
    for field in REQUIRED_FIELDS:
        if field not in item or item[field] is None:
            return False
    return True 

def clean_news_data(raw_news):
    cleaned_news = []
    for item in raw_news:
        news_item = {
            "description": item.get('description'),
            "published_at": item.get('published_at'),
        }

        if validate_news_item(news_item):
            cleaned_news.append(news_item)

    return cleaned_news

def clean_price_data(raw_price_data):
    cleaned_prices = []

    for symbol, data in raw_price_data.get('data', {}).items():
        price_info = data.get('quote', {}).get('USD', {})
        cleaned_prices.append({
            "symbol": symbol,
            "price": price_info.get('price'),
            "volume_24h": price_info.get('volume_24h'),
            "percent_change_1h": price_info.get('percent_change_1h'),
            "percent_change_24h": price_info.get('percent_change_24h'),
            "percent_change_7d": price_info.get('percent_change_7d'),
            "cmc_rank": data.get('cmc_rank'),
            "last_updated": price_info.get('last_updated'),
            "ingested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        })
        

    return cleaned_prices

def get_crypto_list(news_data): 
    default_list = ["BTC","ETH","CMC20","USDT","BNB","XRP","USDC","ADA","SOL","DOGE","TRON","SOLANA","LINK","DOT","AVAX"]
    list = ""
    for item in news_data: 
        if item["symbol"] not in default_list and item["symbol"] != "N/A":
            list += item["symbol"] + ","
    return list[:-1]  
        