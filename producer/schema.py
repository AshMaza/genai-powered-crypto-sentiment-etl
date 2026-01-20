import time

REQUIRED_FIELDS = ["title", "description", "published_at"]

def validate_news_item(item):
    for field in REQUIRED_FIELDS:
        if field not in item or item[field] is None:
            return False
    return True 

def clean_news_data(raw_news):
    cleaned_news = []
    for item in raw_news:
        news_item = {
            "title": item.get('title'),
            "description": item.get('description'),
            "author": "CryptoPanic",
            "published_at": item.get('published_at'),
            "ingested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime())
        }

        if validate_news_item(news_item):
            cleaned_news.append(news_item)

    return cleaned_news
