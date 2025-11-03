import spacy

nlp = spacy.load("en_core_web_sm")

event_patterns = {
    "M&A": ["acquire", "merger", "buyout", "takeover", "merge with", "acquisition", "deal to acquire"],
    "Earnings": ["beats expectations", "missed revenue", "earnings per share", "guidance", "q2 results", "q1 results", "profit warning", "quarterly earnings"],
    "Geopolitics": ["sanctions", "opec", "tensions", "embargo", "tariff", "conflict", "UN", "NATO", "regime", "election", "diplomatic", "military"],
    "Regulation": ["SEC", "FTC", "antitrust", "investigation", "probe", "fine", "lawsuit", "settlement", "regulator"],
    "Layoffs": ["lay off", "job cuts", "staff reduction", "reducing workforce", "cutting jobs"],
    "Leadership Change": ["ceo resigns", "chief executive", "steps down", "leadership shakeup", "new ceo", "appoints ceo"],
    "Partnerships": ["partnership", "collaboration", "alliance", "partnered with", "strategic partner"],
    "Product Launch": ["launches", "releases", "introduces", "unveils", "new product", "product reveal"]
}

def classify_event(text):
    doc = nlp(text.lower())
    event_type = None
    confidence = 0.0
    for category, keywords in event_patterns.items():
        match_count = sum(1 for token in doc if token.text in keywords)
        if match_count > 0:
            confidence_score = match_count / len(keywords)
            if confidence_score > confidence:
                confidence = confidence_score
                event_type = category
    return {
        "event_type": event_type,
        "confidence": round(confidence, 2)
    }