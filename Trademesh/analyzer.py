import spacy
from textblob import TextBlob

nlp = spacy.load("en_core_web_sm")

# Expanded company-to-ticker map from S&P 500, NASDAQ-100, and Dow Jones (no duplicates)
COMPANY_TICKER_MAP = {
    'apple': 'AAPL',
    'microsoft': 'MSFT',
    'amazon': 'AMZN',
    'alphabet': 'GOOGL',
    'meta': 'META',
    'tesla': 'TSLA',
    'nvidia': 'NVDA',
    'berkshire hathaway': 'BRK.B',
    'unitedhealth': 'UNH',
    'johnson & johnson': 'JNJ',
    'visa': 'V',
    'procter & gamble': 'PG',
    'jpmorgan': 'JPM',
    'exxonmobil': 'XOM',
    'home depot': 'HD',
    'mastercard': 'MA',
    'chevron': 'CVX',
    'eli lilly': 'LLY',
    'pfizer': 'PFE',
    'coca cola': 'KO',
    'pepsico': 'PEP',
    'salesforce': 'CRM',
    'walt disney': 'DIS',
    'intel': 'INTC',
    'caterpillar': 'CAT',
    '3m': 'MMM',
    'goldman sachs': 'GS',
    'starbucks': 'SBUX',
    'amgen': 'AMGN',
    'abbvie': 'ABBV',
    'broadcom': 'AVGO',
    'qualcomm': 'QCOM',
    'texas instruments': 'TXN',
    'lockheed martin': 'LMT',
    'boeing': 'BA',
    'at&t': 'T',
    'verizon': 'VZ',
    'nextEra energy': 'NEE',
    'costco': 'COST',
    'target': 'TGT',
    'walmart': 'WMT',
    'home depot': 'HD',
    'american express': 'AXP',
}

def analyze_article(title, description):
    full_text = f"{title}. {description}"
    blob = TextBlob(full_text)

    sentiment = blob.sentiment.polarity
    subjectivity = blob.sentiment.subjectivity

    sentiment_label = (
        'Positive' if sentiment > 0.1 else
        'Negative' if sentiment < -0.1 else
        'Neutral'
    )

    # Named Entity Recognition
    doc = nlp(full_text)
    org_entities = [ent.text for ent in doc.ents if ent.label_ == "ORG"]

    # Combine with existing company map
    mentioned_companies = []
    seen = set()

    for org in org_entities:
        org_lower = org.lower()
        for known_name, ticker in COMPANY_TICKER_MAP.items():
            if known_name in org_lower and ticker not in seen:
                mentioned_companies.append({'name': known_name, 'ticker': ticker})
                seen.add(ticker)

    return {
        'sentiment_score': sentiment,
        'sentiment_label': sentiment_label,
        'subjectivity': subjectivity,
        'companies': mentioned_companies
    }