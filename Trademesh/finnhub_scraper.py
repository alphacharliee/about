import os
import requests
from dotenv import load_dotenv

load_dotenv()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

def get_general_news(limit=5):
    try:
        url = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_API_KEY}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        articles = response.json()[:limit]
        return [{
            'title': article.get('headline'),
            'description': article.get('summary'),
            'link': article.get('url'),
            'published': article.get('datetime'),
            'source': article.get('source')
        } for article in articles]
    except Exception as e:
        print(f"[ERROR] Failed to fetch general news from Finnhub: {e}")
        return []

def get_company_news(ticker, limit=5):
    import datetime
    try:
        to_date = datetime.datetime.now().date()
        from_date = to_date - datetime.timedelta(days=7)
        url = f"https://finnhub.io/api/v1/company-news?symbol={ticker}&from={from_date}&to={to_date}&token={FINNHUB_API_KEY}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        articles = response.json()[:limit]
        return [{
            'title': article.get('headline'),
            'description': article.get('summary'),
            'link': article.get('url'),
            'published': article.get('datetime'),
            'source': article.get('source')
        } for article in articles]
    except Exception as e:
        print(f"[ERROR] Failed to fetch news for {ticker}: {e}")
        return []

if __name__ == "__main__":
    print(f"[DEBUG] FINNHUB_API_KEY loaded? {'✅ Yes' if FINNHUB_API_KEY else '❌ No'}")

    from time import sleep
    news = get_general_news(limit=3)
    if news:
        print("\n🔹 Top 3 General Headlines from Finnhub:")
        for i, item in enumerate(news, 1):
            print(f"\n[{i}] {item['title']}\n{item['description']}\n{item['link']}")
            sleep(0.5)
    else:
        print("❌ No news returned. Check your API key or request limits.")