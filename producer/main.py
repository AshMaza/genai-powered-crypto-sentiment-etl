from dotenv import load_dotenv
from GenAI.gemini import get_gemini_response
from data.sample_data import get_sample_news_data, get_sample_gemini_response, get_sample_price_data
from producer.crypto_news import get_crypto_news
from producer.crypto_price import get_crypto_price
from producer.schema import get_crypto_list
from producer.eventhub_client import send_to_eventhub
import os

load_dotenv()

if __name__ == "__main__":
    crypto_api_data = get_crypto_news()
    # crypto_api_data = get_sample_news_data()

    # print("Raw Crypto News Data:")
    # print(json.dumps(crypto_api_data, indent=2))

    news_data = get_gemini_response(crypto_api_data)

    crypto_list = get_crypto_list(news_data)

    # news_data = get_sample_gemini_response()
    # print("Gemini Response:")
    # print(json.dumps(news_data, indent=2))

    price_data = get_crypto_price(crypto_list)

    # price_data = get_sample_price_data()
    # print("Crypto Price Data:")
    # print(json.dumps(price_data, indent=2))

    send_to_eventhub(
        eventhub_con=os.getenv("EVENTHUB_CONNECTION_STRING"),
        eventhub_name=os.getenv("NEWS_EVENTHUB_NAME"),
        event_envelopes=news_data
    )

    send_to_eventhub(
        eventhub_con=os.getenv("EVENTHUB_CONNECTION_STRING"),
        eventhub_name=os.getenv("PRICE_EVENTHUB_NAME"),
        event_envelopes=price_data
    )




    