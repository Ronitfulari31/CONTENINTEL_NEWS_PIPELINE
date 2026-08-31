# ContentIntel News Pipeline: Complete Flow Documentation

## Overview

This document provides a comprehensive end-to-end flow of the news pipeline across all four stages with technology stack, filenames, and accomplishments at each stage.

```
BRONZE LAYER (Raw Data)
       ↓
[Stage 1: Bronze → Silver]
       ↓
SILVER LAYER (Deduplicated & Cleaned)
       ↓
[Stage 2: Silver → Gold]
       ↓
GOLD LAYER (Analytics-Ready)
       ↓
[Stage 3: Gold → NLP → Gold]
       ↓
GOLD LAYER (NLP-Enriched)
       ↓
[Stage 4: Gold → Streamlit]
       ↓
STREAMLIT DASHBOARD (Analytics & Insights)
```

---

# Stage 1: Bronze → Silver

## Purpose
Receive raw articles from RSS/Kafka, deduplicate using Redis, and normalize data into Silver layer for consistent schema.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                              │
│                   (RSS → Kafka → Redis)                         │
└────────────────────────────────────────┬────────────────────────┘
                                         │
        ┌────────────────────────────────┘
        │
        v
┌─────────────────────────────────────────────────────────────────┐
│                  BRONZE LAYER (Raw Delta)                       │
│          spark_stream_consumer.py processes Kafka               │
│         Writes raw articles to ADLS Bronze layer               │
└────────────────────────────────────────┬────────────────────────┘
                                         │
        ┌────────────────────────────────┘
        │ [PySpark Streaming]
        v
┌─────────────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Normalized)                      │
│         spark_bronze_to_silver.py deduplicates & cleans        │
│        Removes duplicates, standardizes schema, filters        │
└─────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Ingestion** | RSS (feedparser) | Poll news feeds |
| **Deduplication** | Redis | Track URL hashes, TTL 7 days |
| **Message Queue** | Kafka | Publish article events |
| **Streaming** | PySpark Structured Streaming | Subscribe to Kafka, write Delta |
| **Storage** | Delta Lake on ADLS Gen2 | ACID transactions, time travel |
| **Storage Format** | Parquet + Delta Log | Compressed, queryable |

## Key Filenames & Modules

### Input Sources
- **File:** `ingestion/rss_scrapper.py`
  - Polls RSS feeds from `config.RSS_FEEDS`
  - Countries: US, UK, India
  - Produces: `ArticleSchema` objects

### Processing Pipeline
- **File:** `ingestion/redis_client.py`
  - Async Redis client
  - Deduplication via `SETNX` with 7-day TTL
  - Returns `is_duplicate()` boolean

- **File:** `ingestion/kafka_producer.py`
  - Async Kafka producer
  - Publishes to topic: `news.raw`
  - Partition key: `source_country`

- **File:** `spark/spark_stream_consumer.py`
  - Reads Kafka topic `news.raw`
  - Structured Streaming (append-only)
  - Writes to Bronze Delta table

### Configuration
- **File:** `config.py`
  - `KAFKA_BOOTSTRAP_SERVERS` → localhost:9092
  - `REDIS_HOST` → localhost, `REDIS_PORT` → 6379
  - `ADLS_BRONZE_PATH` → abfss://bronze/news_articles
  - `ADLS_CHECKPOINT_PATH` → abfss://bronze/checkpoints/news_raw

- **File:** `.env`
  - Azure Storage credentials (SharedKey auth)
  - Kafka/Redis connection details

- **File:** `docker-compose.yml`
  - Redis container (port 6379)
  - Kafka container (port 9092)

### Utilities
- **File:** `spark/utils.py`
  - `setup_hadoop_env()` — Configure Windows Hadoop
  - `get_free_port()` — Find available port for Spark UI

- **File:** `models.py`
  - `ArticleSchema` (Pydantic)
  - Fields: id, title, url, source_country, content, published_at, word_count, is_breaking_news
  - Validation before Kafka publish

## What Has Been Done

### ✅ Completed

1. **RSS Ingestion Pipeline**
   - ✓ Configured 3 country-based RSS feed sources
   - ✓ Async article fetching with httpx
   - ✓ Content extraction with trafilatura

2. **Deduplication Layer**
   - ✓ Redis SETNX-based dedup with 7-day TTL
   - ✓ MD5 hash from URL for consistency
   - ✓ Atomic operations (no race conditions)

3. **Kafka Producer**
   - ✓ AIOKafkaProducer for async publishing
   - ✓ JSON serialization of ArticleSchema
   - ✓ source_country as partition key

4. **Spark Streaming Consumer**
   - ✓ Structured Streaming from Kafka earliest offset
   - ✓ Delta Lake write with checkpointing
   - ✓ ADLS Gen2 integration (SharedKey auth)
   - ✓ Micro-batch ACID writes

5. **Schema & Validation**
   - ✓ Pydantic ArticleSchema with type validation
   - ✓ Consistent field mapping across layers

## Input Schema (Bronze)

```json
{
  "id": "abc123",
  "title": "Article Title",
  "url": "https://example.com/article",
  "source_country": "US",
  "content": "<p>Article body...</p>",
  "published_at": "2026-08-31T09:45:00Z",
  "word_count": 450,
  "is_breaking_news": false
}
```

## Output Schema (Bronze Layer)

Same as input (raw data)

```
ADLS Bronze Directory:
abfss://bronze/news_articles/
├── _delta_log/
│   ├── 00000000000000000000.json
│   └── 00000000000000000001.json
├── part-00000-...parquet
└── part-00001-...parquet

ADLS Checkpoint Directory:
abfss://bronze/checkpoints/news_raw/
├── offsets/
├── commits/
└── metadata
```

## Running Bronze → Silver

### Prerequisites
```powershell
# Start Docker services
docker compose up -d

# Verify services running
docker ps
```

### Terminal 1: Spark Consumer (Keep Running)
```powershell
python -m spark.spark_stream_consumer
```

### Terminal 2: RSS Scraper (Keep Running)
```powershell
python -m ingestion.rss_scrapper
```

### Terminal 3: Validation
```powershell
python -m spark.read_delta_lake
```

Expected output:
```
Bronze table contains XXX articles
Sample columns: id, title, url, source_country, published_at
```

## Data Volume
- Scraper polls every 5-10 minutes
- Typically 50-200 articles per cycle
- 7-day dedup window in Redis

---

# Stage 2: Silver → Gold

## Purpose
Transform Bronze raw data into clean, analytics-ready Gold layer with data quality checks, normalization, and enrichment.

## Architecture

```
BRONZE LAYER (Raw)
       ↓
    [Read Delta]
       ↓
┌──────────────────────────────────────────┐
│   spark_silver_processor.py              │
│   (PySpark Transformations)              │
├──────────────────────────────────────────┤
│ 1. Schema Sanitization & Type Casting    │
│ 2. Text Normalization (HTML, lowercase)  │
│ 3. Data Enrichment (domain, read time)   │
│ 4. Quality Filtering                     │
│ 5. Partitioning (country, date)          │
└──────────────────────────────────────────┘
       ↓
    [Write Delta]
       ↓
GOLD LAYER (Analytics-Ready)
```

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Processing Engine** | PySpark DataFrame API | Distributed transformation |
| **String Operations** | `regexp_replace`, `trim`, `lower` | Text normalization |
| **Type Casting** | `col().cast()` | Schema consistency |
| **Filtering** | `filter()`, `isNotNull()` | Quality control |
| **Partitioning** | `partitionBy()` | Optimize queries |
| **Storage** | Delta Lake | ACID writes, schema evolution |

## Key Filenames & Modules

### Spark Job
- **File:** `spark/spark_silver_processor.py`
  - Main transformation orchestrator
  - Reads: ADLS Silver path
  - Writes: ADLS Gold path
  - Single entry point: `python -m spark.spark_silver_processor`

### Configuration
- **File:** `config.py`
  - `ADLS_SILVER_PATH` → abfss://bronze/silver/news_articles
  - `ADLS_GOLD_PATH` → abfss://bronze/gold/news_articles
  - Spark UI port fallback via `spark.utils.get_free_port()`

### Utilities
- **File:** `spark/utils.py`
  - Windows Hadoop setup
  - Java/Spark home resolution
  - Free port detection

- **File:** `spark/read_silver_delta.py` (Validation)
  - Reads and prints Silver table schema
  - Debugging tool

## What Has Been Done

### ✅ Completed

1. **Schema Sanitization**
   - ✓ Convert `published_at` (string) → `published_date` (timestamp)
   - ✓ Cast `word_count` to integer
   - ✓ Cast `is_breaking_news` to boolean

2. **Text Normalization**
   - ✓ Remove HTML tags using regex `r'<[^>]+>'`
   - ✓ Lowercase all text with `lower()`
   - ✓ Trim whitespace with `trim()`
   - ✓ Creates `clean_title` and `clean_content` fields

3. **Data Enrichment**
   - ✓ Extract root domain from URL using regex
   - ✓ Calculate read time: `word_count / 200` (minutes)
   - ✓ Add processing timestamp

4. **Quality Filtering**
   - ✓ Remove null IDs, titles, content, URLs
   - ✓ Filter out articles with word_count < 5
   - ✓ Typical rejection rate: 3-5% of records

5. **Partitioning & Write**
   - ✓ Partition by `source_country` and `published_date`
   - ✓ Write in overwrite mode (replace existing)
   - ✓ Enable schema merging for evolution

## Input Schema (Silver)

```json
{
  "id": "abc123",
  "title": "Breaking News: <em>Tech Giant</em> Announced",
  "url": "https://www.techcrunch.com/article/news",
  "source_country": "US",
  "content": "<p>Article content with HTML...</p>",
  "published_at": "2026-08-31T09:45:00Z",
  "word_count": 1250,
  "is_breaking_news": true
}
```

## Output Schema (Gold)

| Field | Type | Transformation |
|-------|------|-----------------|
| `id` | string | Passthrough |
| `title` | string | Original (kept) |
| `clean_title` | string | **NEW** — HTML removed, lowercase |
| `url` | string | Passthrough |
| `domain` | string | **NEW** — Extracted from URL |
| `content` | string | Original (kept) |
| `clean_content` | string | **NEW** — HTML removed, lowercase |
| `source_country` | string | Passthrough (partition key) |
| `published_at` | string | Passthrough (ISO format) |
| `published_date` | timestamp | **NEW** — Parsed from string (partition key) |
| `word_count` | integer | **CAST** — to int type |
| `read_time_minutes` | integer | **NEW** — word_count / 200 |
| `is_breaking_news` | boolean | **CAST** — to boolean type |
| `processed_timestamp` | timestamp | **NEW** — Current timestamp |

## Running Silver → Gold

```powershell
cd E:\DATA_ENGG_PROJECTS\CONTENINTEL_NEWS_PIPELINE
python -m spark.spark_silver_processor
```

**Expected Output:**
```
════════════════════════════════════════════════════════════════
                SILVER TO GOLD TRANSFORMATION
════════════════════════════════════════════════════════════════

📖 Reading articles from Silver layer...
   ✓ Loaded 1,234 articles from Silver

🔧 Step 1: Sanitizing schema and casting types...
🔧 Step 2: Normalizing text...
🔧 Step 3: Enriching data...
🔧 Step 4: Adding processing timestamp...
🔧 Step 5: Applying quality filters...
   ✓ Articles kept: 1,190/1,234
   ✓ Articles rejected: 44

💾 Writing to Gold layer...
   ✓ Persisted 1,190 articles to ADLS Gold layer

📊 Summary:
   Unique Countries: 3
   Unique Domains: 142
   Avg Word Count: 487
   Breaking News Count: 18

✅ Transformation complete!
════════════════════════════════════════════════════════════════
```

## Data Volume
- Input: ~1,234 articles (from Silver)
- Output: ~1,190 articles (after quality filter)
- Duration: 2-5 minutes (1,000 articles)

---

# Stage 3: Gold → NLP → Gold

## Purpose
Enrich Gold articles with 9 AI/NLP tasks (language detection, translation, entity recognition, sentiment, etc.), then persist results back to Gold layer.

## Architecture

```
GOLD LAYER (Input)
       ↓
[Spark Reads Articles]
       ↓
LOCAL PYTHON MEMORY
       ↓
┌─────────────────────────────────────────────────────┐
│        nlp_pipeline/pipeline.py                     │
│     (9 Sequential CPU-Optimized Tasks)              │
├─────────────────────────────────────────────────────┤
│  Task 1:  Preprocessing (ftfy, regex)               │
│  Task 2:  Language Detection (FastText)             │
│  Task 3:  Translation (ArgosTranslate)              │
│  Task 4:  NER (spaCy)                               │
│  Task 5:  Location Extraction (geonamescache)       │
│  Task 6:  Category Classification (keywords)        │
│  Task 7:  Keyword Extraction (TF)                   │
│  Task 8:  Summarization (LexRank)                   │
│  Task 9:  Sentiment Analysis (VADER)                │
└─────────────────────────────────────────────────────┘
       ↓
NLP ENRICHED JSONL (Local File)
       ↓
[Spark Reads JSONL]
       ↓
[Flatten & Persist to Gold]
       ↓
GOLD LAYER (NLP-Enriched Output)
```

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Preprocessing** | ftfy, regex | Unicode fixing, HTML removal |
| **Language Detection** | FastText (fast_langdetect) | 160+ languages, CPU-fast |
| **Translation** | ArgosTranslate | Offline, runs on CPU |
| **NER** | spaCy en_core_web_sm | Entity extraction (PERSON, ORG, GPE, etc.) |
| **Geolocation** | geonamescache | Verify locations against 11.5M database |
| **Classification** | Keyword rules | Custom category scoring |
| **Keywords** | TF (Term Frequency) | Word frequency ranking |
| **Summarization** | LexRank + NLTK | Extractive summary algorithm |
| **Sentiment** | VADER (NLTK) | Polarity scoring (-1 to +1) |
| **Storage** | Delta Lake + JSONL | Intermediate + final |

## Key Filenames & Modules

### NLP Pipeline Package
- **Directory:** `nlp_pipeline/`
  - Core location for all NLP modules

- **File:** `nlp_pipeline/pipeline.py`
  - Orchestrates all 9 NLP tasks
  - Entry point: `run_nlp_pipeline(text: str) → dict`
  - CPU-safe (no Spark serialization)

- **File:** `nlp_pipeline/nlp_enrichment_standalone.py`
  - Reads Gold articles with Spark
  - Collects to Python memory
  - Applies pipeline.py to each article
  - Writes enriched JSONL locally
  - Entry point: `python -m nlp_pipeline.nlp_enrichment_standalone`

- **File:** `nlp_pipeline/save_nlp_to_gold.py`
  - Reads latest JSONL from `data/nlp_enriched/`
  - Flattens nested NLP structure
  - Writes to ADLS Gold with partitioning
  - Entry point: `python -m nlp_pipeline.save_nlp_to_gold`

- **File:** `nlp_pipeline/__init__.py`
  - Package entry point
  - Exports `run_nlp_pipeline`

### Configuration
- **File:** `config.py`
  - `ADLS_GOLD_PATH` (input)
  - `ADLS_GOLD_NLP_PATH` (output)
  - NLP settings (sentence count, keywords limit)

## What Has Been Done

### ✅ Completed

1. **Task 1: Preprocessing**
   - ✓ Unicode normalization (ftfy library)
   - ✓ HTML tag removal (regex)
   - ✓ Whitespace normalization

2. **Task 2: Language Detection**
   - ✓ FastText-based detection
   - ✓ 160+ language support
   - ✓ Fallback to English if fails

3. **Task 3: Translation**
   - ✓ ArgosTranslate offline translation
   - ✓ Non-English → English only
   - ✓ Preserves original language indicator

4. **Task 4: Named Entity Recognition**
   - ✓ spaCy en_core_web_sm model
   - ✓ Extracts: PERSON, ORG, GPE, LOC, DATE, MONEY, etc.
   - ✓ Returns text + label for each entity

5. **Task 5: Location Extraction**
   - ✓ Filters NER entities to geographic types (GPE, LOC)
   - ✓ Cross-references geonamescache (11.5M+ locations)
   - ✓ Marks as verified/unverified

6. **Task 6: Category Classification**
   - ✓ Keyword-based rules for 4 categories: Technology, Business, Politics, Sports
   - ✓ Category scoring (most keywords match)
   - ✓ Fallback to "General" if no matches

7. **Task 7: Keyword Extraction**
   - ✓ Category-biased keyword selection
   - ✓ Term frequency scoring fallback
   - ✓ Stopword filtering
   - ✓ Top N keywords (configurable, default 8)

8. **Task 8: Summarization**
   - ✓ LexRank extractive algorithm
   - ✓ Ranks sentences by graph importance
   - ✓ Returns top N sentences (configurable, default 2)
   - ✓ Falls back to original text if too short

9. **Task 9: Sentiment Analysis**
   - ✓ VADER (Valence Aware Dictionary) lexicon
   - ✓ Compound score (-1.0 to +1.0)
   - ✓ Labels: Positive, Negative, Neutral
   - ✓ Component scores (positive, neutral, negative %s)

### Local JSONL Generation
- ✓ Intermediate JSONL file created at `data/nlp_enriched/enriched_articles_*.jsonl`
- ✓ Each line is a complete enriched article JSON object
- ✓ Nested NLP structure: `nlp.preprocessing`, `nlp.language_detection`, etc.

### Persistence to Gold
- ✓ Flatten nested JSON for queryability
- ✓ Write to ADLS Gold with partitioning
- ✓ Partition by `source_country`, `published_date`

## Input Schema (Gold)

```json
{
  "id": "abc123",
  "title": "breaking news: ai chip released",
  "clean_title": "breaking news: ai chip released",
  "url": "https://techcrunch.com/article",
  "domain": "techcrunch.com",
  "content": "article body...",
  "clean_content": "article body...",
  "source_country": "US",
  "published_at": "2026-08-31T09:45:00Z",
  "published_date": "2026-08-31",
  "word_count": 1200,
  "read_time_minutes": 6,
  "is_breaking_news": true
}
```

## Output Schema (Gold NLP-Enriched)

| Field | Type | Source |
|-------|------|--------|
| `article_id` | string | Gold ID |
| `title` | string | Gold title |
| `domain` | string | Gold domain |
| `detected_language` | string | Task 2 output |
| `predicted_category` | string | Task 6 output |
| `sentiment_label` | string | Task 9 output (Positive/Negative/Neutral) |
| `sentiment_polarity` | double | Task 9 output (-1 to +1) |
| `summary` | string | Task 8 output |
| `ner_entities` | array<struct> | Task 4 output |
| `extracted_locations` | array<struct> | Task 5 output (verified) |
| `keywords` | array<string> | Task 7 output |
| `nlp_processed_at` | timestamp | Processing timestamp |

## Running Gold → NLP → Gold

### Phase 1: Process Articles Locally

```powershell
python -m nlp_pipeline.nlp_enrichment_standalone
```

**Expected Output:**
```
════════════════════════════════════════════════════════════════
        NLP ENRICHMENT - Standalone Processor
════════════════════════════════════════════════════════════════

📖 Reading articles from: abfss://bronze/gold/news_articles
✓ Loaded 1,190 articles for processing

Processing articles with NLP pipeline...
  [238/1190] Processing article ID: article-12345
  [476/1190] Processing article ID: article-67890
  [714/1190] Processing article ID: article-54321
  [952/1190] Processing article ID: article-98765

✓ Successfully enriched 1,190/1,190 articles
✓ Enriched articles saved to: data/nlp_enriched/enriched_articles_20260831_105326.jsonl
  Size: 45,678,901 bytes

✅ NLP Enrichment complete!
════════════════════════════════════════════════════════════════
```

**Duration:** ~1-2 minutes per 100 articles (CPU-dependent)

**Output Location:** `data/nlp_enriched/enriched_articles_*.jsonl`

### Phase 2: Persist to Gold

```powershell
python -m nlp_pipeline.save_nlp_to_gold
```

**Expected Output:**
```
════════════════════════════════════════════════════════════════════════════
                SAVE NLP ENRICHED DATA TO GOLD LAYER
════════════════════════════════════════════════════════════════════════════

📖 Reading enriched local dataset:
   File: data/nlp_enriched/enriched_articles_20260831_105326.jsonl
   Size: 43.58 MB

📥 Reading JSONL into DataFrame...
   ✓ Loaded 1,190 records

🔄 Flattening nested NLP structure...

💾 Persisting to ADLS Gen2 Gold Layer...
   Path: abfss://bronze/gold/nlp_enriched_articles

✅ Successfully persisted 1,190 records to Gold!
   Timestamp: 2026-08-31T10:53:26.123456

════════════════════════════════════════════════════════════════════════════
```

**Duration:** ~2-5 minutes (I/O bound)

**Output Location:** `abfss://bronze/gold/nlp_enriched_articles/` (partitioned)

## Example NLP Output

```json
{
  "article_id": "abc123",
  "title": "Breaking: AI Chip Released",
  "domain": "techcrunch.com",
  "detected_language": "en",
  "predicted_category": "Technology",
  "sentiment_label": "Positive",
  "sentiment_polarity": 0.65,
  "summary": "Apple released a new AI-powered chip. The processor improves battery life.",
  "ner_entities": [
    {"text": "Apple", "label": "ORG"},
    {"text": "AI", "label": "TECHNOLOGY"}
  ],
  "extracted_locations": [
    {"text": "United States", "verified_location": true}
  ],
  "keywords": ["artificial intelligence", "chip", "processor", "apple"],
  "nlp_processed_at": "2026-08-31T10:53:26.123456"
}
```

## Data Volume
- Input: 1,190 articles (from Gold)
- Output: 1,190 enriched articles (JSONL → Gold)
- JSONL file size: 40-50 MB
- Total time: 60-90 minutes (end-to-end)

---

# Stage 4: Gold → Streamlit

## Purpose
Create interactive web dashboard for browsing, searching, and analyzing NLP-enriched articles with visualizations.

## Architecture

```
GOLD LAYER (NLP-Enriched)
       ↓
[Spark Reads Delta]
       ↓
[Streamlit loads data]
       ↓
┌────────────────────────────────────────────┐
│      app.py (Streamlit Dashboard)          │
├────────────────────────────────────────────┤
│ Page 1: News Portal                        │
│  - Search by keyword                       │
│  - Filter by category, country, sentiment  │
│  - Article grid display                    │
│  - Click to view details                   │
│                                            │
│ Page 2: Deep Analysis                      │
│  - Full article content                    │
│  - Sentiment gauge visualization           │
│  - Extracted entities (NER)                │
│  - Verified locations                      │
│  - Sentiment breakdown (pie chart)         │
│  - Keywords, summary, category             │
│  - Language & translation info             │
└────────────────────────────────────────────┘
       ↓
BROWSER (http://localhost:8501)
```

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Web Framework** | Streamlit | Python-based interactive dashboards |
| **Data Loading** | PySpark (local) | Read Gold Delta table |
| **Visualization** | Plotly | Interactive charts & gauges |
| **JSON Processing** | Python json | Parse nested NLP results |
| **Session State** | Streamlit st.session_state | Page navigation, filters |

## Key Filenames & Modules

### Dashboard Application
- **File:** `app.py`
  - Main Streamlit entry point
  - Page 1: News Portal (search, filter, grid)
  - Page 2: Deep Analysis (full NLP results)
  - Navigation via session state

### Configuration
- **File:** `config.py`
  - `ADLS_GOLD_NLP_PATH` — Read location
  - Spark session settings

### Data Sources
- **Source:** JSONL files at `data/nlp_enriched/`
  - Latest enriched articles
  - Alternative to live Delta read (faster)

## What Has Been Done

### ✅ Completed

1. **News Portal Page**
   - ✓ Search bar (keyword search across title/content)
   - ✓ Filter sidebar:
     - Category dropdown
     - Country multi-select
     - Sentiment filter
   - ✓ Article grid display:
     - Thumbnail (domain)
     - Title
     - Sentiment label with color
     - Read time estimate
   - ✓ Click article → navigate to details

2. **Deep Analysis Page**
   - ✓ Article header (title, domain, country, published date)
   - ✓ Full article content display
   - ✓ Sentiment visualization:
     - Gauge chart (polarity score)
     - Color-coded label (Positive/Negative/Neutral)
   - ✓ NLP Results Section:
     - Detected language
     - Extracted entities (table format)
     - Verified locations (with verification status)
     - Keywords (tag display)
     - Summary (formatted text)
     - Category classification
   - ✓ Back button → return to portal

3. **Session State Management**
   - ✓ Navigate between pages without page reload
   - ✓ Maintain selected article ID across pages
   - ✓ Filter state persistence

4. **Data Loading**
   - ✓ Load JSONL from `data/nlp_enriched/` (cached)
   - ✓ Parse nested NLP JSON structure
   - ✓ Handle missing/null fields gracefully

## Input Schema (Gold NLP-Enriched)

Data read from:
- **Source:** `data/nlp_enriched/enriched_articles_*.jsonl`
  - Or: `abfss://bronze/gold/nlp_enriched_articles/` (Spark)

```json
{
  "article_id": "abc123",
  "title": "Breaking News",
  "url_domain": "techcrunch.com",
  "source_country": "US",
  "published_date": "2026-08-31",
  "nlp": {
    "language_detection": {"detected_language": "en"},
    "sentiment": {"polarity_score": 0.65, "label": "Positive"},
    "ner": {"entities": [...]},
    "location_extraction": {"locations": [...]},
    "category_classification": {"category": "Technology"},
    "keyword_extraction": {"keywords": [...]},
    "summary": {"summary_text": "..."},
    "translation": {"source_language": "en", "translated_text": "..."}
  }
}
```

## Dashboard Features

### News Portal Page

**Layout:**
```
┌─────────────────────────────────────────────────────┐
│  ContentIntel News Portal                           │
├─────────────────────────────────────────────────────┤
│ [Search]  [Filter by Category] [Filter by Country]  │
│ [Sentiment: All / Positive / Neutral / Negative]    │
├─────────────────────────────────────────────────────┤
│  Article 1        Article 2        Article 3        │
│  ┌─────────┐     ┌─────────┐     ┌─────────┐       │
│  │ Domain  │     │ Domain  │     │ Domain  │       │
│  │ Title   │     │ Title   │     │ Title   │       │
│  │ 😊      │     │ 😐      │     │ 😞      │       │
│  │ 5 min   │     │ 7 min   │     │ 3 min   │       │
│  └─────────┘     └─────────┘     └─────────┘       │
│       ↓                ↓                ↓            │
│    (click)          (click)          (click)        │
└─────────────────────────────────────────────────────┘
```

**Interactions:**
- Type in search box → filter articles by keyword
- Select category → show articles in that category
- Select countries → show articles from those countries
- Select sentiment → show articles with that polarity
- Click article card → navigate to Deep Analysis page

### Deep Analysis Page

**Layout:**
```
┌─────────────────────────────────────────────────────┐
│ Deep Analysis: Article Title                        │
│ [← Back to Portal]                                  │
├─────────────────────────────────────────────────────┤
│ Domain: techcrunch.com                              │
│ Country: US | Date: 2026-08-31 | Read Time: 6 min  │
├─────────────────────────────────────────────────────┤
│ [Full Article Content]                              │
│ Article text displayed in full...                   │
├─────────────────────────────────────────────────────┤
│ SENTIMENT ANALYSIS                                  │
│ ┌─────────────────────┐   Label: Positive          │
│ │    ╱╲               │   Score: 0.65               │
│ │   ╱  ╲  ●           │   Confidence: High          │
│ │  ╱    ╲             │                             │
│ └─────────────────────┘                             │
│  [Gauge Chart: Score from -1 to +1]                │
├─────────────────────────────────────────────────────┤
│ NLP RESULTS                                         │
│ Language: en                                        │
│ Category: Technology                                │
│ Keywords: ai, chip, processor, apple                │
│                                                     │
│ Summary:                                            │
│ "Apple released a new chip..."                      │
│                                                     │
│ Entities:                                           │
│ | Type     | Entity            |                    │
│ | PERSON   | Steve Jobs        |                    │
│ | ORG      | Apple             |                    │
│ | GPE      | United States     |                    │
│                                                     │
│ Locations (Verified):                               │
│ ✓ United States | ✓ California | ✗ Silicon Valley |
│                                                     │
│ Translation:                                        │
│ Source: en | Applied: no | Text: [original]        │
└─────────────────────────────────────────────────────┘
```

## Running Streamlit Dashboard

### Prerequisites
- Gold NLP-enriched articles exist in `data/nlp_enriched/`
  - Run `nlp_pipeline.nlp_enrichment_standalone` first
  - Run `nlp_pipeline.save_nlp_to_gold` second

### Start Dashboard
```powershell
cd E:\DATA_ENGG_PROJECTS\CONTENINTEL_NEWS_PIPELINE
streamlit run app.py
```

**Expected Output:**
```
═════════════════════════════════════════════════════════
  You can now view your Streamlit app in your browser.

  Local URL:            http://localhost:8501
  Network URL:          http://192.168.x.x:8501
═════════════════════════════════════════════════════════

Collecting usage statistics. To deactivate, set browser.gatherUsageStats to False.

2026-08-31 11:00:00 INFO streamlit.root: Created temporary directory...
2026-08-31 11:00:01 INFO streamlit.SessionClientManagerBase: Connected. Ready to accept messages.
```

**Access:**
- Open browser → `http://localhost:8501`
- Portal page loads automatically
- Click articles to view details

### Headless Mode (No Browser Launch)
```powershell
$env:STREAMLIT_SERVER_HEADLESS = "true"
streamlit run app.py --server.headless=true --server.port=8501
```

## Data Flow in Streamlit

```
@st.cache_data
load_data():
  ↓ Read JSONL from data/nlp_enriched/
  ↓ Parse JSON, extract fields
  ↓ Return list of article dicts
  ↓ Cache result (expires after 24 hours)

Portal Page:
  ↓ Display all articles (or filtered)
  ↓ User clicks article
  ↓ Set st.session_state['selected_article_id']
  ↓ Rerun

Analysis Page:
  ↓ Read selected_article_id from session_state
  ↓ Find article in data
  ↓ Display full analysis
  ↓ Visualize sentiment gauge
  ↓ Show NLP results tables

Back to Portal:
  ↓ Clear session state
  ↓ Return to portal page
```

## Key Code Components

### app.py Structure
```python
# 1. Import & Cache
import streamlit as st
from pathlib import Path
import json

@st.cache_data
def load_data():
    # Read JSONL, return articles list
    pass

# 2. Page Selection
if 'page' not in st.session_state:
    st.session_state.page = 'portal'

if st.session_state.page == 'portal':
    show_portal()
elif st.session_state.page == 'analysis':
    show_analysis()

# 3. Portal Page
def show_portal():
    # Search, filter, grid
    pass

# 4. Analysis Page
def show_analysis():
    # Full article, sentiment gauge, NLP results
    pass
```

## Data Volume
- Display: 1,190 articles per session
- JSONL file: 40-50 MB
- Load time: ~2-5 seconds (cached)
- Update interval: Manual (re-run Streamlit to reload)

---

# Complete End-to-End Summary

| Stage | Input | Technology | Process | Output | Files |
|-------|-------|-----------|---------|--------|-------|
| **1: Bronze** | RSS feeds | Kafka, Redis, Spark | Poll, dedupe, stream | Bronze Delta (raw) | `spark_stream_consumer.py` |
| **2: Silver** | Bronze | PySpark Transformations | N/A (streaming writes) | Silver Delta | `spark_stream_consumer.py` |
| **3: Gold** | Silver | PySpark (SQL API) | Clean, normalize, filter | Gold Delta | `spark_silver_processor.py` |
| **4: NLP** | Gold | spaCy, FastText, VADER, LexRank | 9 NLP tasks locally | Gold NLP Delta | `nlp_enrichment_standalone.py`, `save_nlp_to_gold.py` |
| **5: Dashboard** | Gold NLP | Streamlit + Plotly | Visualize & interact | Browser UI | `app.py` |

---

# Complete Execution Checklist

## ✅ Before Running

```powershell
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start Docker
docker compose up -d

# 3. Verify services
docker ps
```

## ✅ Stage 1: Ingestion (Bronze)

```powershell
# Terminal 1: Spark Consumer (keep running)
python -m spark.spark_stream_consumer

# Terminal 2: RSS Scraper (keep running)
python -m ingestion.rss_scrapper

# Terminal 3: Validation
python -m spark.read_delta_lake
```

## ✅ Stage 2: Silver → Gold

```powershell
# Wait 10+ minutes for articles to accumulate
python -m spark.spark_silver_processor
```

## ✅ Stage 3: Gold → NLP → Gold

```powershell
# Phase 1: Local enrichment (~1 hour for 1,190 articles)
python -m nlp_pipeline.nlp_enrichment_standalone

# Phase 2: Persist to Gold
python -m nlp_pipeline.save_nlp_to_gold
```

## ✅ Stage 4: Dashboard

```powershell
# Launch Streamlit
streamlit run app.py

# Open browser
http://localhost:8501
```

---

## Approximate Total Runtime

| Stage | Duration | Notes |
|-------|----------|-------|
| Setup (Docker, deps) | 5-10 min | One-time |
| Ingestion (Bronze) | 10-30 min | Waiting for articles |
| Silver → Gold | 5-10 min | Transform 1,000+ articles |
| Gold → NLP | 60-90 min | CPU-bound, sequential |
| NLP → Gold | 5-10 min | I/O, fast |
| Dashboard | Instant | Already running |
| **TOTAL** | **90-150 min** | **End-to-end pipeline** |

---

## Architecture Diagram (All Stages)

```
                    RSS FEEDS
                   (US/UK/IN)
                       ↓
           ┌───────────────────────┐
           │   INGESTION LAYER     │
           │ (feedparser, httpx)   │
           └───────────┬───────────┘
                       ↓
           ┌───────────────────────┐
           │   REDIS DEDUPLICATION │
           │   (7-day TTL)         │
           └───────────┬───────────┘
                       ↓
           ┌───────────────────────┐
           │   KAFKA PUBLISHER     │
           │   (news.raw topic)    │
           └───────────┬───────────┘
                       ↓
        ╔══════════════════════════════╗
        ║   SPARK STREAM CONSUMER      ║
        ║  (Structured Streaming)      ║
        ║  [CHECKPOINT: news_raw]      ║
        ╚═════────────┬────────────────╝
                      ↓
        ╔═════════════════════════════════════╗
        ║  BRONZE LAYER (Raw Delta)           ║
        ║  abfss://bronze/news_articles       ║
        ║  [Raw articles: id, title, content] ║
        ╚═════════────┬─────────────────────╝
                      ↓
        ╔═════════════════════════════════════╗
        ║  SILVER LAYER (In-Memory Cache)     ║
        ║  [Streaming deduplication, cleaning]║
        ║  [Normalized schema]                ║
        ╚═════════────┬─────────────────────╝
                      ↓
      ┌───────────────────────────────────┐
      │  spark_silver_processor.py        │
      │  - Sanitize schema                │
      │  - Normalize text (HTML removal)  │
      │  - Enrich (domain, read time)     │
      │  - Filter quality                 │
      │  - Partition by country + date    │
      └───────────┬───────────────────────┘
                  ↓
        ╔════════════════════════════════════╗
        ║   GOLD LAYER (Analytics-Ready)     ║
        ║   abfss://bronze/gold/news_articles║
        ║   [1,190 clean articles]           ║
        ║   [14 fields: title, domain,...]   ║
        ╚════════────┬───────────────────────╝
                     ↓
      ┌────────────────────────────────────┐
      │  nlp_enrichment_standalone.py      │
      │  - Read Gold with Spark            │
      │  - Apply 9 NLP tasks locally       │
      │  - Sequential processing           │
      │  - Write JSONL                     │
      └────────────┬────────────────────────┘
                   ↓
    ╔═════════════════════════════════════════╗
    ║   LOCAL JSONL (Intermediate)            ║
    ║   data/nlp_enriched/*.jsonl             ║
    ║   [1,190 articles with full NLP output] ║
    ║   [Nested structure: nlp.task.*]        ║
    ╚═════════────┬───────────────────────────╝
                  ↓
      ┌─────────────────────────────────┐
      │  save_nlp_to_gold.py            │
      │  - Read JSONL                   │
      │  - Flatten NLP structure        │
      │  - Write to Delta               │
      │  - Partition & optimize         │
      └─────────────┬───────────────────┘
                    ↓
        ╔════════════════════════════════════╗
        ║  GOLD LAYER (NLP-Enriched)         ║
        ║  abfss://bronze/gold/               ║
        ║  nlp_enriched_articles              ║
        ║  [1,190 enriched articles]         ║
        ║  [27 fields: sentiment, entities,..║
        ╚════════════┬─────────────────────╝
                     ↓
           ┌──────────────────────┐
           │   app.py (Streamlit) │
           │  - News Portal Page  │
           │  - Deep Analysis     │
           │  - Visualizations    │
           └──────────┬───────────┘
                      ↓
              🌐 BROWSER 🌐
           http://localhost:8501
```

---

# Notes & Best Practices

1. **Run stages sequentially** — Wait for previous stage to complete
2. **Monitor logs** — Each script prints progress and errors
3. **Check disk space** — JSONL files can be 40-50 MB
4. **Validate at each stage** — Use `read_delta_lake.py` to inspect data
5. **Clean old JSONL** — Remove old files from `data/nlp_enriched/` to avoid confusion
6. **Cache in Streamlit** — Data is cached for 24 hours (configurable)
7. **Partition strategy** — Gold layer partitioned by country + date for efficient queries
8. **NLP processing** — CPU-intensive, ~1 minute per 100 articles

---

This document provides the complete flow across all 4 stages with technology, filenames, and accomplishments at each step.
