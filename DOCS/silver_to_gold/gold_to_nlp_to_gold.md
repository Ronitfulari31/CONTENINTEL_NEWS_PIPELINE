# Gold to NLP Enrichment to Gold

## Purpose

The NLP enrichment layer transforms Gold analytics-ready articles into deeply analyzed, **AI-enriched** articles by applying 9 independent CPU-optimized text analysis tasks:

1. **Preprocessing** — Clean text, fix unicode, normalize whitespace
2. **Language Detection** — Detect article language (FastText model)
3. **Translation** — Translate non-English articles to English (ArgosTranslate)
4. **Named Entity Recognition (NER)** — Extract people, places, organizations (spaCy)
5. **Location Extraction** — Verify geographic locations against geonames database
6. **Category Classification** — Rule-based topic classification (Technology, Business, Politics, Sports, General)
7. **Keyword Extraction** — TF-based keyword ranking
8. **Summary Generation** — Extractive summarization using LexRank algorithm
9. **Sentiment Analysis** — Sentiment polarity score and label (VADER lexicon)

The final enriched results are persisted back to the Gold layer in ADLS Gen2 for consumption by analytics, dashboards, and recommendation engines.

---

## Why Local CPU Processing?

**Problem:** Spark UDFs cannot reliably serialize large Python NLP libraries on Windows.

**Solution:** 
1. Read Gold articles with Spark (distributed, scalable)
2. Collect rows into local Python memory
3. Process sequentially with CPU-safe NLP functions
4. Write enriched JSONL locally (fast, no serialization issues)
5. Persist JSONL back to ADLS Gold (structured, queryable)

This hybrid approach combines Spark's I/O scalability with Python's NLP power.

---

## Architecture Overview

```
GOLD DELTA LAYER (Input)
    ↓ [Spark reads]
LOCAL PYTHON MEMORY
    ↓ [Sequential NLP processing]
    ├─→ Task 1: Preprocessing
    ├─→ Task 2: Language Detection
    ├─→ Task 3: Translation
    ├─→ Task 4: NER
    ├─→ Task 5: Location Extraction
    ├─→ Task 6: Category Classification
    ├─→ Task 7: Keyword Extraction
    ├─→ Task 8: Summarization
    └─→ Task 9: Sentiment Analysis
    ↓ [Enrich JSON structure]
NLP ENRICHED JSONL (Local File)
    ↓ [Spark writes]
GOLD DELTA LAYER (Output)
    ↓ [Ready for consumption]
Analytics & Dashboards
```

---

## Directory Structure

```
CONTENINTEL_NEWS_PIPELINE/
├── nlp_pipeline/                      ← CANONICAL NLP PACKAGE
│   ├── __init__.py
│   ├── pipeline.py                    9-task NLP processor ⭐
│   ├── nlp_enrichment_standalone.py   Gold → JSONL enrichment ⭐
│   ├── save_nlp_to_gold.py            JSONL → Gold persistence ⭐
│   ├── gold_nlp_processor.py          Alternative Spark-based processor
│   └── enrichment.py                  Helper utilities
│
├── data/
│   └── nlp_enriched/
│       └── enriched_articles_*.jsonl  Intermediate NLP output
│
├── config.py                          Central configuration
└── DOCS/
    └── silver_to_gold_nlp/
        └── gold_to_nlp_to_gold.md     (this file)
```

---

## Complete NLP Pipeline

### Task 1: Preprocessing

**Purpose:** Clean text, fix unicode, normalize whitespace for downstream tasks.

**Implementation:**

```python
import ftfy
import re

def clean_text(text: str) -> str:
    """
    1. Fix broken unicode with ftfy library
    2. Remove HTML tags with regex
    3. Normalize excessive whitespace
    """
    if not text:
        return ""
    
    # Fix unicode encoding issues
    text = ftfy.fix_text(text)
    
    # Remove HTML tags (greedy match)
    text = re.sub(r'<[^>]+>', '', text)
    
    # Normalize whitespace (multiple spaces → single space)
    text = re.sub(r'\s+', ' ', text)
    
    # Trim leading/trailing whitespace
    return text.strip()
```

**Example:**
```
Input:  "<p>The <strong>AI</strong> revolution   is here.</p>"
Step 1: "<p>The <strong>AI</strong> revolution   is here.</p>"
Step 2: "The AI revolution   is here."                           (HTML removed)
Step 3: "The AI revolution is here."                             (spaces normalized)
Step 4: "The AI revolution is here."                             (trimmed)

Output:
{
  "preprocessing": {
    "cleaned_text": "The AI revolution is here.",
    "word_count": 5
  }
}
```

---

### Task 2: Language Detection

**Purpose:** Automatically detect article language using lightweight FastText model.

**Implementation:**

```python
from fast_langdetect import detect

def detect_language(text: str) -> str:
    """
    Detect ISO language code using compressed FastText model.
    Runs on CPU without heavy dependencies.
    """
    try:
        result = detect(text, model="lite")
        return result["lang"].lower()
    except Exception:
        # Default to English if detection fails
        return "en"
```

**Supported Languages:** 160+ languages (ISO 639-1 codes)

**Examples:**
```
Text: "Ceci est un article en français..."
Output: "fr" (French)

Text: "Dies ist ein deutscher Artikel..."
Output: "de" (German)

Text: "This is an English article..."
Output: "en" (English)

Text: "यह एक हिंदी लेख है..."
Output: "hi" (Hindi)
```

**Output JSON:**
```json
{
  "language_detection": {
    "detected_language": "en",
    "original_language": "en"
  }
}
```

---

### Task 3: Translation

**Purpose:** Translate non-English articles to English for consistent NLP processing.

**Implementation:**

```python
import argostranslate.translate as argo_translate

def translate_if_needed(text: str, source_lang: str) -> str:
    """
    If source language is not English:
    - Load source language → English translation model
    - Translate entire text to English
    - Return translated text
    
    If already English:
    - Return original text unchanged
    """
    if source_lang == "en" or not source_lang:
        return text
    
    try:
        # Get installed languages
        installed_languages = argo_translate.get_installed_languages()
        
        # Find source language model
        src_lang = next(
            (l for l in installed_languages if l.code == source_lang),
            None
        )
        
        # Find English target model
        tgt_lang = next(
            (l for l in installed_languages if l.code == "en"),
            None
        )
        
        # Perform translation if both models available
        if src_lang and tgt_lang:
            translation = src_lang.get_translation(tgt_lang)
            return translation.translate(text)
    
    except Exception as e:
        print(f"⚠ Translation failed: {e}. Using original text.")
    
    return text
```

**Setup (First Run):**
```powershell
.\.venv\Scripts\python.exe -m argostranslate.scripts.install_all_packages
```

**Example:**
```
Input:  source_lang="fr", text="La révolution de l'IA est ici."
Output: "The AI revolution is here."

Input:  source_lang="en", text="The AI revolution is here."
Output: "The AI revolution is here."  (unchanged)
```

**Output JSON:**
```json
{
  "translation": {
    "source_language": "fr",
    "translated_text": "The AI revolution is here.",
    "applied": true,
    "translated_from_original": "La révolution de l'IA est ici."
  }
}
```

---

### Task 4: Named Entity Recognition (NER)

**Purpose:** Extract people, organizations, locations, and other named entities.

**Implementation:**

```python
import spacy

# Load spaCy English model (small, CPU-optimized)
nlp = spacy.load("en_core_web_sm")

def extract_named_entities(text: str) -> list:
    """
    Process text with spaCy NLP pipeline.
    Extract all named entities with labels.
    """
    doc = nlp(text)
    
    ner_entities = [
        {
            "text": ent.text,
            "label": ent.label_
        }
        for ent in doc.ents
    ]
    
    return ner_entities
```

**Entity Types (spaCy en_core_web_sm):**

| Label | Description | Examples |
|-------|-------------|----------|
| PERSON | People | Barack Obama, Elon Musk, Melinda Gates |
| ORG | Organizations | Microsoft, Google, United Nations |
| GPE | Geopolitical entities | United States, United Kingdom, India |
| LOC | Non-GPE locations | Silicon Valley, Amazon rainforest |
| DATE | Dates/times | August 31, 2026 |
| MONEY | Monetary values | $100 million, €50,000 |
| PERCENT | Percentages | 25%, 50% |
| EVENT | Named events | World War II, Olympics |

**Example:**
```
Input: "On August 31, Elon Musk announced a new Tesla factory in Austin, Texas."

Processing with spaCy:
- "August 31" → DATE
- "Elon Musk" → PERSON
- "Tesla" → ORG
- "Austin" → LOC
- "Texas" → GPE

Output:
[
  {"text": "August 31", "label": "DATE"},
  {"text": "Elon Musk", "label": "PERSON"},
  {"text": "Tesla", "label": "ORG"},
  {"text": "Austin", "label": "LOC"},
  {"text": "Texas", "label": "GPE"}
]
```

**Output JSON:**
```json
{
  "ner": {
    "entities": [
      {"text": "Barack Obama", "label": "PERSON"},
      {"text": "United States", "label": "GPE"},
      {"text": "Microsoft", "label": "ORG"}
    ]
  }
}
```

---

### Task 5: Location Extraction & Verification

**Purpose:** Extract geographic locations and verify them against authoritative geonames database.

**Implementation:**

```python
import geonamescache

gnc = geonamescache.GeonamesCache()

# Pre-load city and country names
cities = set(c['name'].lower() for c in gnc.get_cities().values())
countries = set(c['name'].lower() for c in gnc.get_countries().values())

def extract_and_verify_locations(ner_entities: list) -> list:
    """
    Filter NER entities for geographic locations (GPE, LOC).
    Cross-reference against geonames database.
    Mark as verified or unverified.
    """
    locations = []
    
    for ent in ner_entities:
        if ent['label'] in ["GPE", "LOC"]:
            entity_name_lower = ent['text'].lower()
            
            # Check if name exists in cities or countries
            is_verified = (
                entity_name_lower in cities or
                entity_name_lower in countries
            )
            
            locations.append({
                "text": ent['text'],
                "verified_location": is_verified
            })
    
    return locations
```

**Geonames Database:** 11.5M+ cities, 248 countries

**Example:**
```
Input NER entities:
[
  {"text": "New York", "label": "GPE"},
  {"text": "Silicon Valley", "label": "LOC"},
  {"text": "Canada", "label": "GPE"}
]

Processing:
- "New York" → Found in cities → verified: true
- "Silicon Valley" → Not in database (region, not city) → verified: false
- "Canada" → Found in countries → verified: true

Output:
[
  {"text": "New York", "verified_location": true},
  {"text": "Silicon Valley", "verified_location": false},
  {"text": "Canada", "verified_location": true}
]
```

**Output JSON:**
```json
{
  "location_extraction": {
    "locations": [
      {"text": "New York", "verified_location": true},
      {"text": "Silicon Valley", "verified_location": false},
      {"text": "Canada", "verified_location": true}
    ]
  }
}
```

---

### Task 6: Category Classification

**Purpose:** Classify articles into broad categories using keyword-based rules.

**Implementation:**

```python
KEYWORDS_CATEGORY = {
    "Technology": [
        "ai", "software", "tech", "data", "cyber", "apple", "google",
        "microsoft", "app", "chip", "cloud", "algorithm", "neural"
    ],
    "Business": [
        "market", "stock", "economy", "bank", "revenue", "profit",
        "trade", "investment", "ceo", "earnings", "nasdaq", "fortune"
    ],
    "Politics": [
        "election", "government", "president", "law", "minister",
        "parliament", "policy", "vote", "senate", "congress", "bill"
    ],
    "Sports": [
        "match", "league", "player", "tournament", "stadium", "score",
        "champion", "football", "basketball", "soccer", "nfl", "nba"
    ]
}

def classify_category(text: str) -> str:
    """
    Simple keyword-matching classifier:
    1. Count category keywords found in text
    2. Return category with highest count
    3. Default to "General" if no matches
    """
    text_lower = text.lower()
    
    # Score each category
    scores = {}
    for category, keywords in KEYWORDS_CATEGORY.items():
        count = sum(1 for kw in keywords if kw in text_lower)
        scores[category] = count
    
    # Find category with highest score
    if max(scores.values()) > 0:
        return max(scores, key=scores.get)
    else:
        return "General"
```

**Example:**
```
Input: "Apple released new AI chip for iPhone"

Scoring:
- Technology: 3 matches (apple, ai, chip)
- Business: 0 matches
- Politics: 0 matches
- Sports: 0 matches

Output: "Technology" (highest score)

---

Input: "Market drops 2% on new policy"

Scoring:
- Technology: 0 matches
- Business: 1 match (market)
- Politics: 1 match (policy)
- Sports: 0 matches

Output: "Business" or "Politics" (tie, first wins)
```

**Output JSON:**
```json
{
  "category_classification": {
    "category": "Technology",
    "category_scores": {
      "Technology": 5,
      "Business": 1,
      "Politics": 0,
      "Sports": 0
    }
  }
}
```

---

### Task 7: Keyword Extraction

**Purpose:** Extract and rank keywords to summarize article topics.

**Implementation:**

```python
from collections import Counter
import re

def extract_keywords(text: str, category: str, top_n: int = 8) -> list:
    """
    1. Try to extract category-specific keywords first
    2. If found, return top N category keywords
    3. Otherwise, use TF (term frequency) scoring
    4. Filter common stopwords
    5. Return top N keywords
    """
    text_lower = text.lower()
    
    # Extract words (3+ characters, alphanumeric/hyphen)
    words = re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{2,}", text_lower)
    
    if not words:
        return []
    
    # Try category keywords first
    category_kws = KEYWORDS_CATEGORY.get(category, [])
    candidates = [kw for kw in category_kws if kw in text_lower]
    
    if candidates:
        return candidates[:top_n]
    
    # Fall back to TF scoring
    common_stopwords = {
        "the", "and", "for", "with", "into", "from", "that", "this",
        "have", "will", "about", "are", "been", "was", "is", "not"
    }
    
    # Count word frequencies, excluding stopwords
    freq = Counter()
    for word in words:
        if word not in common_stopwords:
            freq[word] += 1
    
    # Return top N by frequency
    return [word for word, _ in freq.most_common(top_n)]
```

**Example:**
```
Input: text="Artificial intelligence and machine learning algorithms", category="Technology"

Step 1: Extract words
["artificial", "intelligence", "and", "machine", "learning", "algorithms"]

Step 2: Try category keywords
Found: ["ai", "machine", "learning"] in KEYWORDS_CATEGORY["Technology"]
(Note: "ai" matches substring in "artificial intelligence")

Output:
["artificial", "intelligence", "machine"]  (top 3)
```

**Output JSON:**
```json
{
  "keyword_extraction": {
    "keywords": [
      "artificial intelligence",
      "machine learning",
      "neural networks",
      "deep learning",
      "data science",
      "algorithms",
      "models",
      "training"
    ]
  }
}
```

---

### Task 8: Summary Generation

**Purpose:** Generate extractive summary highlighting key sentences.

**Implementation:**

```python
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer
import nltk

# Download NLTK punkt tokenizer (first run)
try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab', quiet=True)

def generate_summary(text: str, sentence_count: int = 2) -> str:
    """
    LexRank extractive summarization:
    1. Parse text into sentences
    2. Build TF-IDF weighted term graph
    3. Rank sentences by graph importance
    4. Return top N sentences as summary
    """
    if len(text.split()) < 40:
        return text  # Too short to summarize
    
    try:
        # Parse text into sentence tokens
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        
        # Initialize LexRank summarizer
        summarizer = LexRankSummarizer()
        
        # Extract top N sentences
        summary_sentences = summarizer(parser.document, sentence_count)
        
        # Join sentences into single string
        return " ".join([str(s) for s in summary_sentences])
    
    except Exception as e:
        print(f"⚠ Summarization failed: {e}")
        # Fallback: return first 200 chars
        return text[:200] + "..."
```

**Algorithm:** LexRank (graph-based)
- Builds TF-IDF weighted sentence graph
- Ranks sentences by eigenvector centrality
- Returns most "central" (representative) sentences

**Example:**
```
Input:
"Apple released a new AI-powered iPhone chip today. The chip features 
neural engine capabilities. The new processor improves battery life. 
Customers can expect 20% faster performance. The device launches in Q4."

LexRank ranks sentences by importance:
1. "The chip features neural engine capabilities." (score: 0.85)
2. "The new processor improves battery life." (score: 0.78)
3. "Customers can expect 20% faster performance." (score: 0.72)

Output (2 sentences):
"The chip features neural engine capabilities. The new processor improves battery life."
```

**Output JSON:**
```json
{
  "summary": {
    "summary_text": "The key findings were confirmed by independent researchers. The study has significant implications for the industry."
  }
}
```

---

### Task 9: Sentiment Analysis

**Purpose:** Determine article sentiment polarity (positive, negative, neutral) with confidence score.

**Implementation:**

```python
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# Download VADER lexicon (first run)
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

# Initialize VADER analyzer
vader = SentimentIntensityAnalyzer()

def get_sentiment(text: str) -> dict:
    """
    VADER (Valence Aware Dictionary and sEntiment Reasoner):
    
    Scores text on 4 dimensions:
    - negative: proportion of text expressing negativity
    - neutral: proportion of text that's neutral
    - positive: proportion of text expressing positivity
    - compound: normalized sentiment score (-1.0 to +1.0)
    
    Labels:
    - Positive: compound >= 0.05
    - Neutral: -0.05 < compound < 0.05
    - Negative: compound <= -0.05
    """
    scores = vader.polarity_scores(text)
    
    compound = scores['compound']
    
    # Classify based on compound score
    if compound >= 0.05:
        label = "Positive"
    elif compound <= -0.05:
        label = "Negative"
    else:
        label = "Neutral"
    
    return {
        "polarity_score": compound,
        "label": label,
        "positive": scores['pos'],
        "neutral": scores['neu'],
        "negative": scores['neg']
    }
```

**VADER Strengths:**
- Works on social media, news, informal text
- Handles negations ("not good" = negative)
- Handles intensifiers ("very good" = more positive)
- Fast, no neural network required

**Example:**
```
Input: "The AI breakthrough is amazing and will transform industries!"
Scores: positive=0.67, neutral=0.33, negative=0, compound=0.85
Output: label="Positive", polarity_score=0.85

Input: "The market crashed today."
Scores: positive=0, neutral=0.65, negative=0.35, compound=-0.39
Output: label="Negative", polarity_score=-0.39

Input: "The article discusses various viewpoints."
Scores: positive=0.18, neutral=0.82, negative=0, compound=0.18
Output: label="Neutral", polarity_score=0.18
```

**Output JSON:**
```json
{
  "sentiment": {
    "polarity_score": 0.65,
    "label": "Positive",
    "positive": 0.42,
    "neutral": 0.58,
    "negative": 0.0
  }
}
```

---

## Complete NLP Output Schema

Each article produces this enriched JSON structure:

```json
{
  "article_id": "abc123def456",
  "title": "Breaking News: AI Breakthrough",
  "url_domain": "techcrunch.com",
  "source_country": "US",
  "published_date": "2026-08-31",
  "full_text_payload": "original article text...",
  "processed_text": "cleaned and translated text...",
  "nlp": {
    "preprocessing": {
      "cleaned_text": "cleaned article text",
      "word_count": 420
    },
    "language_detection": {
      "detected_language": "en",
      "original_language": "en"
    },
    "translation": {
      "source_language": "en",
      "translated_text": "translated text if needed",
      "applied": false,
      "translated_from_original": "original text before translation"
    },
    "ner": {
      "entities": [
        {"text": "Barack Obama", "label": "PERSON"},
        {"text": "United States", "label": "GPE"},
        {"text": "Microsoft", "label": "ORG"}
      ]
    },
    "location_extraction": {
      "locations": [
        {"text": "New York", "verified_location": true},
        {"text": "Silicon Valley", "verified_location": false}
      ]
    },
    "category_classification": {
      "category": "Technology",
      "category_scores": {
        "Technology": 8,
        "Business": 2,
        "Politics": 0,
        "Sports": 0
      }
    },
    "keyword_extraction": {
      "keywords": [
        "artificial intelligence",
        "neural networks",
        "machine learning",
        "deep learning",
        "algorithms",
        "models",
        "training",
        "data"
      ]
    },
    "summary": {
      "summary_text": "Summary of key points from the article in 2-3 sentences."
    },
    "sentiment": {
      "polarity_score": 0.65,
      "label": "Positive",
      "positive": 0.42,
      "neutral": 0.58,
      "negative": 0.0
    }
  },
  "processed_at": "2026-08-31T10:53:26.123456"
}
```

---

## Complete Execution Pipeline

### Phase 1: Gold → Local NLP Enrichment

**File:** `nlp_pipeline/nlp_enrichment_standalone.py`

**Process:**
1. Read Gold articles with Spark
2. Collect rows to Python memory
3. Apply 9 NLP tasks sequentially
4. Write enriched JSONL locally

**Run:**
```powershell
cd E:\DATA_ENGG_PROJECTS\CONTENINTEL_NEWS_PIPELINE
python -m nlp_pipeline.nlp_enrichment_standalone
```

**Output:**
```
════════════════════════════════════════════════════════════════════
        NLP ENRICHMENT - Standalone Processor
════════════════════════════════════════════════════════════════════

📖 Reading articles from: abfss://bronze/gold/news_articles
✓ Loaded 1,190 articles for processing

Processing articles with NLP pipeline...
──────────────────────────────────────────────────────────────────
  [238/1190] Processing article ID: article-12345
  [476/1190] Processing article ID: article-67890
  [714/1190] Processing article ID: article-54321
  [952/1190] Processing article ID: article-98765
──────────────────────────────────────────────────────────────────

✓ Successfully enriched 1,190/1,190 articles
✓ Enriched articles saved to: data/nlp_enriched/enriched_articles_20260831_105326.jsonl
  Size: 45,678,901 bytes

📊 Sample Enrichment Result (Article 1):
{
  "article_id": "...",
  "title": "...",
  "nlp": {
    "preprocessing": {...},
    "language_detection": {...},
    ...
  }
}

✅ NLP Enrichment complete!
════════════════════════════════════════════════════════════════════
```

**Timing:** ~1-2 minutes per 100 articles on typical CPU

### Phase 2: NLP JSONL → Gold Persistence

**File:** `nlp_pipeline/save_nlp_to_gold.py`

**Process:**
1. Read latest JSONL from `data/nlp_enriched/`
2. Load into Spark DataFrame
3. Flatten nested NLP structure
4. Write to ADLS Gold with partitioning

**Run:**
```powershell
cd E:\DATA_ENGG_PROJECTS\CONTENINTEL_NEWS_PIPELINE
python -m nlp_pipeline.save_nlp_to_gold
```

**Output:**
```
════════════════════════════════════════════════════════════════════════════
                SAVE NLP ENRICHED DATA TO GOLD LAYER
════════════════════════════════════════════════════════════════════════════

📖 Reading enriched local dataset:
   File: data/nlp_enriched/enriched_articles_20260831_105326.jsonl
   Size: 43.58 MB

🔧 Initializing Spark Session...

📥 Reading JSONL into DataFrame...
   ✓ Loaded 1,190 records

🔄 Flattening nested NLP structure...

📋 Schema Validation:
 |-- article_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- domain: string (nullable = true)
 |-- detected_language: string (nullable = true)
 |-- predicted_category: string (nullable = true)
 |-- sentiment_label: string (nullable = true)
 |-- sentiment_polarity: double (nullable = true)
 |-- summary: string (nullable = true)
 |-- ner_entities: array (nullable = true)
 |-- extracted_locations: array (nullable = true)

📊 Statistics:
   Total Records: 1,190
   Columns: 14

💾 Persisting to ADLS Gen2 Gold Layer...
   Path: abfss://bronze/gold/nlp_enriched_articles

✅ Successfully persisted NLP Enriched Data into ADLS Gen2 Gold Layer!
   Records Written: 1,190
   Timestamp: 2026-08-31T10:53:26.123456

════════════════════════════════════════════════════════════════════════════
```

**Timing:** ~2-5 minutes for 1,000+ articles (depends on JSONL size)

---

## Final Gold NLP Schema

| Field | Type | Source |
|-------|------|--------|
| `article_id` | string | Original Gold |
| `title` | string | Original Gold |
| `domain` | string | Original Gold |
| `source_country` | string | Original Gold (partition) |
| `published_date` | date | Original Gold (partition) |
| `detected_language` | string | NLP Task 2 |
| `predicted_category` | string | NLP Task 6 |
| `sentiment_label` | string | NLP Task 9 |
| `sentiment_polarity` | double | NLP Task 9 |
| `summary` | string | NLP Task 8 |
| `ner_entities` | array<struct> | NLP Task 4 |
| `extracted_locations` | array<struct> | NLP Task 5 |
| `nlp_processed_at` | timestamp | Enrichment pipeline |
| `ingested_to_gold_at` | timestamp | Spark write |

---

## Complete Python Code

### File: `nlp_pipeline/pipeline.py`

```python
"""
CPU-Optimized NLP Pipeline with 9 Sequential Tasks
Processes article text without Spark UDF serialization issues.
"""

import os
import re
import ftfy
import nltk
import geonamescache
from typing import Dict, Any, List
from collections import Counter

# ─────────────────────────────────────────────────────────────
# TASK 1: PREPROCESSING
# ─────────────────────────────────────────────────────────────

def clean_text(text: str) -> str:
    """Remove HTML remnants, fix broken unicode, normalize whitespace."""
    if not text:
        return ""
    text = ftfy.fix_text(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


# ─────────────────────────────────────────────────────────────
# TASK 2: LANGUAGE DETECTION
# ─────────────────────────────────────────────────────────────

from fast_langdetect import detect

def detect_language(text: str) -> str:
    """Detects ISO language code using FastText model on CPU."""
    try:
        res = detect(text, model="lite")
        return res["lang"].lower()
    except Exception:
        return "en"


# ─────────────────────────────────────────────────────────────
# TASK 3: TRANSLATION
# ─────────────────────────────────────────────────────────────

import argostranslate.package
import argostranslate.translate

def translate_if_needed(text: str, source_lang: str) -> str:
    """Translates non-English text to English locally on CPU."""
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


# ─────────────────────────────────────────────────────────────
# TASK 4, 5, 6, 7: spaCy + Rules-Based Setup
# ─────────────────────────────────────────────────────────────

import spacy

nlp = spacy.load("en_core_web_sm")
gnc = geonamescache.GeonamesCache()
cities = set(c['name'].lower() for c in gnc.get_cities().values())
countries = set(c['name'].lower() for c in gnc.get_countries().values())

KEYWORDS_CATEGORY = {
    "Technology": ["ai", "software", "tech", "data", "cyber", "apple", "google", "microsoft"],
    "Business": ["market", "stock", "economy", "bank", "revenue", "profit", "trade"],
    "Politics": ["election", "government", "president", "law", "minister", "parliament"],
    "Sports": ["match", "league", "player", "tournament", "stadium", "score", "champion"]
}


def extract_keywords(text: str, category: str = "General", top_n: int = 8) -> List[str]:
    """Extract keywords with category bias."""
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

    stopwords = {"the", "and", "for", "with", "into", "from", "that", "this"}
    freq = Counter(w for w in words if w not in stopwords)
    return [word for word, _ in freq.most_common(top_n)]


def process_spacy_tasks(text: str) -> Dict[str, Any]:
    """NER + Location + Category extraction."""
    doc = nlp(text)
    ner_entities = []
    locations = []

    for ent in doc.ents:
        ner_entities.append({"text": ent.text, "label": ent.label_})
        if ent.label_ in ["GPE", "LOC"]:
            ent_lower = ent.text.lower()
            is_verified = ent_lower in cities or ent_lower in countries
            locations.append({"text": ent.text, "verified_location": is_verified})

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


# ─────────────────────────────────────────────────────────────
# TASK 8: SENTIMENT ANALYSIS
# ─────────────────────────────────────────────────────────────

from nltk.sentiment.vader import SentimentIntensityAnalyzer

try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

vader_analyzer = SentimentIntensityAnalyzer()


def get_sentiment(text: str) -> Dict[str, Any]:
    """VADER sentiment analysis."""
    scores = vader_analyzer.polarity_scores(text)
    compound = scores['compound']
    label = "Positive" if compound >= 0.05 else ("Negative" if compound <= -0.05 else "Neutral")
    return {"polarity_score": compound, "label": label}


# ─────────────────────────────────────────────────────────────
# TASK 9: SUMMARIZATION
# ─────────────────────────────────────────────────────────────

from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer

try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab', quiet=True)


def generate_summary(text: str, sentence_count: int = 2) -> str:
    """LexRank extractive summarization."""
    if len(text.split()) < 40:
        return text
    try:
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        summarizer = LexRankSummarizer()
        summary_sentences = summarizer(parser.document, sentence_count)
        return " ".join([str(s) for s in summary_sentences])
    except Exception:
        return text[:200] + "..."


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE: ORCHESTRATE ALL 9 TASKS
# ─────────────────────────────────────────────────────────────

def run_nlp_pipeline(raw_payload: str) -> Dict[str, Any]:
    """
    Execute all 9 NLP tasks sequentially in order:
    1. Preprocessing
    2. Language Detection
    3. Translation
    4. NER
    5. Location Extraction
    6. Category Classification
    7. Keyword Extraction
    8. Summarization
    9. Sentiment Analysis
    """
    
    # Task 1: Preprocessing
    clean_payload = clean_text(raw_payload)
    
    # Task 2: Language Detection
    lang = detect_language(clean_payload)
    
    # Task 3: Translation
    english_payload = translate_if_needed(clean_payload, source_lang=lang)
    translation_applied = bool(lang != "en" and english_payload != clean_payload)
    
    # Tasks 4-7: spaCy-based tasks
    spacy_results = process_spacy_tasks(english_payload)
    
    # Task 8: Sentiment
    sentiment = get_sentiment(english_payload)
    
    # Task 9: Summary
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
    }
```

---

## Running the Complete Gold → NLP → Gold Flow

### Step 1: Process Articles Locally

```powershell
python -m nlp_pipeline.nlp_enrichment_standalone
```

Creates: `data/nlp_enriched/enriched_articles_20260831_105326.jsonl`

### Step 2: Persist to Gold

```powershell
python -m nlp_pipeline.save_nlp_to_gold
```

Updates: `abfss://bronze/gold/nlp_enriched_articles`

### Total Time
- 1,000 articles: ~45-60 minutes
- 500 articles: ~25-30 minutes
- 100 articles: ~5-10 minutes

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: spacy` | `.\.venv\Scripts\pip install spacy && python -m spacy download en_core_web_sm` |
| `Translation model not found` | `.\.venv\Scripts\python -m argostranslate.scripts.install_all_packages` |
| `No JSONL files found` | Re-run `nlp_enrichment_standalone.py` |
| `Azure auth error` | Verify `config.py` AZURE_STORAGE_KEY |
| "Processing is very slow" | Increase `sentence_count` or run on faster CPU |

---

## Next Steps

After NLP enrichment is complete:
- ✅ Analytics dashboard ready (`streamlit run app.py`)
- ✅ NLP results queryable in Gold layer
- ✅ Machine learning features available
- ✅ Content recommendation system can be built
