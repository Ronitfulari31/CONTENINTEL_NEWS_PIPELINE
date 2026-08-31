import os
import re
import ftfy
import nltk
import geonamescache
from typing import Dict, Any, List

# --- Task 1: Preprocessing Setup ---
def clean_text(text: str) -> str:
    """Removes HTML remnants, fixes broken unicode, and normalizes whitespace."""
    if not text:
        return ""
    text = ftfy.fix_text(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# --- Task 2: Language Detection ---
from fast_langdetect import detect

def detect_language(text: str) -> str:
    """Detects ISO language code using compressed FastText model on CPU."""
    try:
        res = detect(text, model="lite")
        return res["lang"].lower()
    except Exception:
        return "en"

# --- Task 3: Translation (Non-English -> English) ---
import argostranslate.package
import argostranslate.translate

def translate_if_needed(text: str, source_lang: str) -> str:
    """Translates text to English locally on CPU using ArgosTranslate if non-English."""
    if source_lang == "en" or not source_lang:
        return text
    try:
        installed_languages = argostranslate.translate.get_installed_languages()
        src_lang = next((l for l in installed_languages if l.code == source_lang), None)
        tgt_lang = next((l for l in installed_languages if l.code == "en"), None)
        if src_lang and tgt_lang:
            translation = src_lang.get_translation(tgt_lang)
            return translation.translate(text)
    except Exception as e:
        print(f"Translation warning: {e}. Falling back to original text.")
    return text

# --- Task 4, 5, 6, 7: spaCy NER, Location & Rule-Based Classifier Setup ---
import spacy

nlp = spacy.load("en_core_web_sm")
gnc = geonamescache.GeonamesCache()
cities = set(c['name'].lower() for c in gnc.get_cities().values())
countries = set(c['name'].lower() for c in gnc.get_countries().values())

KEYWORDS_CATEGORY = {
    "Technology": ["ai", "software", "tech", "data", "cyber", "apple", "google", "microsoft", "app", "chip"],
    "Business": ["market", "stock", "economy", "bank", "revenue", "profit", "trade", "investment"],
    "Politics": ["election", "government", "president", "law", "minister", "parliament", "policy", "vote"],
    "Sports": ["match", "league", "player", "tournament", "stadium", "score", "champion", "football"]
}


def extract_keywords(text: str, category: str = "General", top_n: int = 8) -> List[str]:
    if not text:
        return []
    text_lower = text.lower()
    words = re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{2,}", text_lower)
    if not words:
        return []

    if category in KEYWORDS_CATEGORY:
        candidates = [kw for kw in KEYWORDS_CATEGORY[category] if kw in text_lower]
        if candidates:
            return candidates[:top_n]

    freq = {}
    for word in words:
        if word in {"the", "and", "for", "with", "into", "from", "that", "this", "have", "will", "about"}:
            continue
        freq[word] = freq.get(word, 0) + 1

    return [word for word, _ in sorted(freq.items(), key=lambda x: (-x[1], x[0]))[:top_n]]


def process_spacy_tasks(text: str) -> Dict[str, Any]:
    doc = nlp(text)
    ner_entities = []
    locations = []

    for ent in doc.ents:
        ner_entities.append({"text": ent.text, "label": ent.label_})
        if ent.label_ in ["GPE", "LOC"]:
            ent_lower = ent.text.lower()
            is_valid_geo = ent_lower in cities or ent_lower in countries
            locations.append({"text": ent.text, "verified_location": is_valid_geo})

    text_lower = text.lower()
    scores = {cat: sum(1 for kw in kws if kw in text_lower) for cat, kws in KEYWORDS_CATEGORY.items()}
    best_cat = max(scores, key=scores.get)
    category = best_cat if scores[best_cat] > 0 else "General"

    return {
        "ner_entities": ner_entities,
        "locations": locations,
        "category": category,
        "keywords": extract_keywords(text, category=category)
    }

# --- Task 8: Lexicon Sentiment Analysis ---
from nltk.sentiment.vader import SentimentIntensityAnalyzer

try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

vader_analyzer = SentimentIntensityAnalyzer()


def get_sentiment(text: str) -> Dict[str, Any]:
    scores = vader_analyzer.polarity_scores(text)
    compound = scores['compound']
    label = "Positive" if compound >= 0.05 else ("Negative" if compound <= -0.05 else "Neutral")
    return {"polarity_score": compound, "label": label}

# --- Task 9: Extractive Summarization ---
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer

try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab', quiet=True)


def generate_summary(text: str, sentence_count: int = 2) -> str:
    if len(text.split()) < 40:
        return text
    try:
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        summarizer = LexRankSummarizer()
        summary_sentences = summarizer(parser.document, sentence_count)
        return " ".join([str(s) for s in summary_sentences])
    except Exception:
        return text[:200] + "..."


def run_nlp_pipeline(raw_payload: str) -> Dict[str, Any]:
    clean_payload = clean_text(raw_payload)
    lang = detect_language(clean_payload)
    english_payload = translate_if_needed(clean_payload, source_lang=lang)
    translation_applied = bool(lang != "en" and english_payload != clean_payload)
    spacy_results = process_spacy_tasks(english_payload)
    sentiment = get_sentiment(english_payload)
    summary = generate_summary(english_payload, sentence_count=2)

    return {
        "preprocessing": {
            "cleaned_text": clean_payload,
            "word_count": len(clean_payload.split())
        },
        "language_detection": {
            "detected_language": lang,
            "original_language": lang
        },
        "translation": {
            "source_language": lang,
            "translated_text": english_payload,
            "applied": translation_applied,
            "translated_from_original": clean_payload
        },
        "ner": {"entities": spacy_results["ner_entities"]},
        "location_extraction": {"locations": spacy_results["locations"]},
        "category_classification": {
            "category": spacy_results["category"],
            "category_scores": {
                cat: sum(1 for kw in kws if kw in english_payload.lower())
                for cat, kws in KEYWORDS_CATEGORY.items()
            }
        },
        "keyword_extraction": {"keywords": spacy_results["keywords"]},
        "summary": {"summary_text": summary},
        "sentiment": sentiment,
        "processed_text": english_payload,
        "translated_text": english_payload,
        "detected_language": lang,
        "original_language": lang,
        "translation_applied": translation_applied,
        "category": spacy_results["category"],
        "keywords": spacy_results["keywords"],
        "ner_entities": spacy_results["ner_entities"],
        "extracted_locations": spacy_results["locations"],
        "summary_text": summary
    }
