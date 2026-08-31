# ContentIntel News Pipeline: Complete Directory Structure

## Clear File Organization & Layer Mapping

This document shows the **exact directory layout** with files organized by **data layer** and **functional purpose**.

---

## Root Directory Overview

```
E:\DATA_ENGG_PROJECTS\CONTENINTEL_NEWS_PIPELINE/
│
├── 📄 Configuration Files (Root Level)
│   ├── config.py                      ← Central config (Azure, Kafka, Redis, paths)
│   ├── models.py                      ← Pydantic schema (ArticleSchema)
│   ├── requirements.txt                ← Python dependencies
│   ├── docker-compose.yml              ← Redis + Kafka services
│   ├── .env                            ← Azure credentials (KEEP SECRET!)
│   └── .gitignore                      ← Exclude secrets, venv, cache
│
├── 📁 ingestion/                       ← STAGE 1: RSS → Kafka → Bronze
│   ├── __init__.py
│   ├── rss_scrapper.py                 ← Poll RSS feeds, validate, publish Kafka
│   ├── kafka_producer.py               ← Async Kafka publisher
│   └── redis_client.py                 ← Async Redis deduplication client
│
├── 📁 spark/                           ← STAGE 2-4: Spark jobs
│   ├── __init__.py
│   ├── utils.py                        ← Windows Hadoop/Java setup helper
│   ├── spark_stream_consumer.py        ← Kafka → Bronze Delta (STREAMING)
│   ├── read_delta_lake.py              ← Validation reader (any layer)
│   └── read_silver_delta.py            ← Silver layer validation
│
├── 📁 nlp_pipeline/                    ← CANONICAL NLP PACKAGE
│   ├── __init__.py                     ← Package entry point
│   ├── pipeline.py                     ← 9-task NLP orchestrator ⭐⭐⭐
│   ├── nlp_enrichment_standalone.py    ← Gold → JSONL processor ⭐
│   ├── save_nlp_to_gold.py             ← JSONL → Gold persister ⭐
│   ├── gold_nlp_processor.py           ← Alternative Spark processor
│   └── enrichment.py                   ← Helper utilities
│
├── 📁 data/                            ← LOCAL DATA STORAGE
│   ├── 📁 delta/
│   │   └── 📁 news_articles/           ← Local Delta cache (Bronze/Silver/Gold)
│   │       └── _delta_log/
│   │
│   ├── 📁 nlp_enriched/                ← INTERMEDIATE NLP JSONL
│   │   ├── enriched_articles_20260831_105326.jsonl  ← Latest
│   │   ├── enriched_articles_20260831_095421.jsonl  ← Previous
│   │   └── enriched_articles_20260831_082935.jsonl  ← Older
│   │
│   ├── 📁 checkpoints/
│   │   └── 📁 news_raw/                ← Spark streaming checkpoint
│   │       ├── offsets/
│   │       ├── commits/
│   │       └── metadata
│   │
│   └── 📁 (other folders)
│
├── 📁 DOCS/                            ← DOCUMENTATION
│   ├── bronze_silver/
│   │   └── source_ingestion_bronze.md  ← Ingestion architecture
│   │
│   ├── silver_to_gold_nlp/
│   │   ├── silver_to_gold.md           ← Silver → Gold transformation
│   │   ├── gold_to_nlp_to_gold.md      ← NLP enrichment pipeline
│   │   └── pipeline_architecture.md    ← Combined overview
│   │
│   └── complete_flow.md                ← THIS: End-to-end flow
│
├── 📁 .venv/                           ← Python virtual environment
│   └── (Python packages installed here)
│
├── 📁 .hadoop/                         ← Windows Hadoop binaries
│   └── bin/
│       ├── winutils.exe
│       └── hadoop.dll
│
└── 📁 .git/                            ← Git version control
```

---

# Data Layer Mapping

## Where Does Each Layer Live?

### Layer 1: BRONZE (Raw Articles)

**What:** Raw articles from Kafka, no processing

**Location:**
```
ADLS Gen2:  abfss://bronze/news_articles/
Local:      data/delta/news_articles/  (cache only)
```

**Files Created By:**
- `spark/spark_stream_consumer.py` — Reads Kafka, writes Bronze

**Files Stored As:**
- Parquet files + Delta Log
- Partition: None (streaming append)
- Example path:
  ```
  abfss://bronze/news_articles/
  ├── _delta_log/
  │   ├── 00000000000000000000.json    ← Transaction log
  │   ├── 00000000000000000001.json
  │   └── ...
  └── part-00000-...parquet            ← Actual data
  ```

**Schema:**
```json
{
  "id": "string",
  "title": "string",
  "url": "string",
  "source_country": "string",
  "content": "string",
  "published_at": "string (ISO)",
  "word_count": "int",
  "is_breaking_news": "boolean",
  "processed_timestamp": "timestamp"
}
```

**Record Count:** 1,000+ (after 10+ minutes of scraping)

---

### Layer 2: SILVER (Deduplicated & Cleaned)

**What:** Bronze data but deduplicated (Redis checks), normalized schema

**Location:**
```
ADLS Gen2:  abfss://bronze/silver/news_articles/
Local:      data/delta/news_articles/  (cache only)
```

**Files Created By:**
- NOT YET IMPLEMENTED
- Placeholder: Would use spark_stream_consumer or separate processor
- Currently: Bronze is read directly by Gold processor

**Files Stored As:**
- Parquet files + Delta Log
- Partition: source_country, published_date
- Example path:
  ```
  abfss://bronze/silver/news_articles/
  ├── source_country=US/
  │   ├── published_date=2026-08-31/
  │   │   ├── _delta_log/
  │   │   └── part-00000-...parquet
  │   └── published_date=2026-08-30/
  │       └── part-00000-...parquet
  └── source_country=GB/
      └── published_date=2026-08-31/
  ```

**Schema:**
Same as Bronze (normalized version)

**Record Count:** ~1,000+ (deduped, typically 90-95% of Bronze)

---

### Layer 3: GOLD (Analytics-Ready)

**What:** Clean, enriched, ready for analytics OR NLP processing

**Location:**
```
ADLS Gen2:  abfss://bronze/gold/news_articles/
Local:      data/delta/news_articles/  (cache only)
```

**Files Created By:**
- `spark/spark_silver_processor.py` — Reads Silver, writes Gold

**Files Stored As:**
- Parquet files + Delta Log
- Partition: source_country, published_date
- Example path:
  ```
  abfss://bronze/gold/news_articles/
  ├── source_country=US/
  │   ├── published_date=2026-08-31/
  │   │   ├── _delta_log/
  │   │   │   ├── 00000000000000000000.json
  │   │   │   └── 00000000000000000001.json
  │   │   ├── part-00000-...parquet
  │   │   └── part-00001-...parquet
  │   └── published_date=2026-08-30/
  └── source_country=GB/
      └── published_date=2026-08-31/
  ```

**Schema:**
```json
{
  "id": "string",
  "title": "string",
  "clean_title": "string (NEW)",
  "url": "string",
  "domain": "string (NEW)",
  "content": "string",
  "clean_content": "string (NEW)",
  "source_country": "string",
  "published_at": "string",
  "published_date": "timestamp (NEW)",
  "word_count": "int (CAST)",
  "read_time_minutes": "int (NEW)",
  "is_breaking_news": "boolean (CAST)",
  "processed_timestamp": "timestamp (NEW)"
}
```

**Record Count:** ~1,190 (after quality filtering)

---

### Layer 4: GOLD NLP-ENRICHED

**What:** Gold articles + 9 NLP analysis tasks + all results

**Location:**
```
ADLS Gen2:  abfss://bronze/gold/nlp_enriched_articles/
Local JSONL: data/nlp_enriched/enriched_articles_*.jsonl
```

**Files Created By (2 Steps):**

**Step 1: Local JSONL Generation**
- `nlp_pipeline/nlp_enrichment_standalone.py` 
  - Reads: Gold Delta (ADLS)
  - Processes: Local Python loop (9 NLP tasks)
  - Writes: Local JSONL file

**Step 2: ADLS Persistence**
- `nlp_pipeline/save_nlp_to_gold.py`
  - Reads: Local JSONL
  - Writes: Gold NLP Delta (ADLS)

**Files Stored As:**

**Local JSONL (Intermediate):**
```
data/nlp_enriched/
├── enriched_articles_20260831_105326.jsonl  ← Latest (use this)
├── enriched_articles_20260831_095421.jsonl  ← Previous
└── enriched_articles_20260831_082935.jsonl  ← Older

Each line = 1 complete article JSON object (1,190 lines total)
File size: ~40-50 MB
```

**ADLS Delta (Final):**
```
abfss://bronze/gold/nlp_enriched_articles/
├── source_country=US/
│   ├── published_date=2026-08-31/
│   │   ├── _delta_log/
│   │   ├── part-00000-...parquet
│   │   └── part-00001-...parquet
│   └── published_date=2026-08-30/
├── source_country=GB/
│   └── published_date=2026-08-31/
└── source_country=IN/
    └── published_date=2026-08-31/
```

**Schema (Flattened from nested JSON):**
```json
{
  "article_id": "string",
  "title": "string",
  "domain": "string",
  "source_country": "string",
  "published_date": "date",
  "detected_language": "string (Task 2)",
  "predicted_category": "string (Task 6)",
  "sentiment_label": "string (Task 9)",
  "sentiment_polarity": "double (Task 9)",
  "summary": "string (Task 8)",
  "ner_entities": "array<struct> (Task 4)",
  "extracted_locations": "array<struct> (Task 5)",
  "keywords": "array<string> (Task 7)",
  "nlp_processed_at": "timestamp",
  "ingested_to_gold_at": "timestamp"
}
```

**Full NLP Structure (Local JSONL):**
```json
{
  "article_id": "abc123",
  "title": "Breaking News",
  "nlp": {
    "preprocessing": {"cleaned_text": "...", "word_count": 420},
    "language_detection": {"detected_language": "en"},
    "translation": {"source_language": "en", "translated_text": "..."},
    "ner": {"entities": [...]},
    "location_extraction": {"locations": [...]},
    "category_classification": {"category": "Technology"},
    "keyword_extraction": {"keywords": [...]},
    "summary": {"summary_text": "..."},
    "sentiment": {"polarity_score": 0.65, "label": "Positive"}
  }
}
```

**Record Count:** 1,190 (same as Gold input)

---

# NLP Folder Structure Clarification

## Why Two NLP Folders? (IMPORTANT!)

### ❌ OLD/DEPRECATED: `nlp/` folder
```
nlp/                          ← OLD STRUCTURE (DELETE ME)
├── __init__.py
├── pipeline.py               ← Duplicate (don't use)
├── nlp_enrichment_standalone.py  ← Duplicate (don't use)
├── save_nlp_to_gold.py       ← Duplicate (don't use)
├── gold_nlp_processor.py     ← Duplicate (don't use)
├── enrichment.py             ← Duplicate (don't use)
└── nlp_pipeline.py           ← Duplicate (don't use)
```

**Status:** Legacy structure from earlier iterations. NO LONGER USED.
**Action:** Should be deleted to avoid confusion.

---

### ✅ CANONICAL: `nlp_pipeline/` folder
```
nlp_pipeline/                 ← CURRENT STRUCTURE (USE THIS)
├── __init__.py               ← Entry point: from nlp_pipeline.pipeline import run_nlp_pipeline
├── pipeline.py               ← ⭐ MAIN: 9-task NLP orchestrator
├── nlp_enrichment_standalone.py  ← ⭐ Gold → JSONL converter
├── save_nlp_to_gold.py       ← ⭐ JSONL → Gold persister
├── gold_nlp_processor.py     ← Alternative (not recommended)
├── enrichment.py             ← Helper utilities
└── __pycache__/              ← Python bytecode cache
```

**Status:** Active, production-ready.
**Why this name?** Indicates the full **NLP pipeline package** (not just nlp).
**How to import:**
```python
from nlp_pipeline.pipeline import run_nlp_pipeline
from nlp_pipeline.nlp_enrichment_standalone import enrich_articles_locally
```

---

## File Purposes in `nlp_pipeline/`

| File | Purpose | Input | Output | Entry Point |
|------|---------|-------|--------|------------|
| `pipeline.py` | Orchestrate 9 NLP tasks | Raw text (string) | Dict with all NLP results | `run_nlp_pipeline(text)` |
| `nlp_enrichment_standalone.py` | Process Gold articles locally | Gold Delta table | JSONL file | `python -m nlp_pipeline.nlp_enrichment_standalone` |
| `save_nlp_to_gold.py` | Persist JSONL to ADLS | Local JSONL | ADLS Gold Delta | `python -m nlp_pipeline.save_nlp_to_gold` |
| `enrichment.py` | Utility helpers | Raw articles | Enriched records | Helper functions |
| `gold_nlp_processor.py` | Spark-based alternative | Gold Delta | Gold NLP Delta | `python -m nlp_pipeline.gold_nlp_processor` |

---

# Complete Data Flow with Directories

```
┌──────────────────────────────────────────────────────────────────┐
│                    STAGE 1: INGESTION                            │
│                  (ingestion/ folder)                             │
└──────────────────────┬───────────────────────────────────────────┘
                       │
            RSS Feeds → ingestion/rss_scrapper.py
                       ↓
                   KAFKA (news.raw)
                       ↓
            spark/spark_stream_consumer.py
                       ↓
        ╔═══════════════════════════════════════╗
        ║   BRONZE LAYER                        ║
        ║   abfss://bronze/news_articles/       ║
        ║   [Raw articles, no processing]       ║
        ║   Record count: 1,000+                ║
        ╚═════════────┬─────────────────────────╝
                      │
┌─────────────────────────────────────────────────────────────────┐
│                   STAGE 2: TRANSFORMATION                       │
│                  (spark/ folder)                                │
└──────────────────────┬──────────────────────────────────────────┘
                       │
        spark/spark_silver_processor.py
                       ↓
        ╔═════════════════════════════════════════════╗
        ║   GOLD LAYER (Before NLP)                  ║
        ║   abfss://bronze/gold/news_articles/       ║
        ║   [Cleaned, normalized, quality filtered]  ║
        ║   Record count: 1,190                      ║
        ║   Partitions: country, date                ║
        ╚═════════────┬───────────────────────────────╝
                      │
┌─────────────────────────────────────────────────────────────────┐
│                  STAGE 3: NLP ENRICHMENT                        │
│                 (nlp_pipeline/ folder)                          │
└──────────────────────┬──────────────────────────────────────────┘
                       │
        nlp_pipeline/nlp_enrichment_standalone.py
        - Reads: abfss://bronze/gold/news_articles/
        - Processes: 9 NLP tasks (local Python)
        - Writes: data/nlp_enriched/enriched_articles_*.jsonl
                       │
                       ↓
    ╔════════════════════════════════════════════════════════╗
    ║   LOCAL JSONL (Intermediate)                          ║
    ║   data/nlp_enriched/enriched_articles_*.jsonl         ║
    ║   [1,190 articles with full NLP structure]            ║
    ║   File size: 40-50 MB                                 ║
    ╚════════────┬─────────────────────────────────────────╝
                 │
        nlp_pipeline/save_nlp_to_gold.py
        - Reads: data/nlp_enriched/enriched_articles_*.jsonl
        - Flattens: Nested NLP → Flat schema
        - Writes: ADLS Gold NLP Delta
                 │
                 ↓
    ╔════════════════════════════════════════════════════════╗
    ║   GOLD LAYER (NLP-Enriched)                           ║
    ║   abfss://bronze/gold/nlp_enriched_articles/          ║
    ║   [Cleaned + 9 NLP analysis tasks]                    ║
    ║   Record count: 1,190                                 ║
    ║   Partitions: country, date                           ║
    ║   Fields: sentiment, entities, summary, etc.          ║
    ╚════════────┬─────────────────────────────────────────╝
                 │
┌───────────────────────────────────────────────────────────────┐
│               STAGE 4: DASHBOARD                              │
│                  (app.py)                                     │
└───────────────────┬─────────────────────────────────────────┘
                    │
        app.py
        - Reads: data/nlp_enriched/enriched_articles_*.jsonl
        - OR: abfss://bronze/gold/nlp_enriched_articles/
                    │
                    ↓
    ╔═══════════════════════════════════════════════════════╗
    ║   STREAMLIT DASHBOARD                                ║
    ║   http://localhost:8501                              ║
    ║   Page 1: News Portal (search, filter, grid)         ║
    ║   Page 2: Deep Analysis (sentiment, NER, etc.)       ║
    ╚═══════════════════════════════════════════════════════╝
```

---

# Practical File Reference

## Where to Find What?

### "I need to modify how articles are scraped"
→ File: `ingestion/rss_scrapper.py`

### "I need to configure Kafka/Redis/Azure"
→ File: `config.py`

### "I need to clean and transform articles"
→ File: `spark/spark_silver_processor.py`

### "I need to see Bronze articles"
→ Location: `abfss://bronze/news_articles/` (ADLS)
→ Tool: `spark/read_delta_lake.py`

### "I need to see Gold articles (before NLP)"
→ Location: `abfss://bronze/gold/news_articles/` (ADLS)
→ Tool: `spark/read_delta_lake.py`

### "I need to run NLP on Gold articles"
→ File: `nlp_pipeline/nlp_enrichment_standalone.py`
→ Command: `python -m nlp_pipeline.nlp_enrichment_standalone`

### "I need to save NLP results back to Gold"
→ File: `nlp_pipeline/save_nlp_to_gold.py`
→ Command: `python -m nlp_pipeline.save_nlp_to_gold`

### "I need to use NLP tasks in my code"
→ Import: `from nlp_pipeline.pipeline import run_nlp_pipeline`

### "I need to see enriched articles locally"
→ Location: `data/nlp_enriched/enriched_articles_*.jsonl`
→ (Latest file by timestamp)

### "I need to see NLP results in Gold"
→ Location: `abfss://bronze/gold/nlp_enriched_articles/` (ADLS)
→ Tool: `spark/read_delta_lake.py`

### "I need to launch the dashboard"
→ File: `app.py`
→ Command: `streamlit run app.py`

---

# Storage Locations Summary Table

| Layer | Data | ADLS Location | Local Cache | Format | Partition | Records |
|-------|------|---------------|-------------|--------|-----------|---------|
| **BRONZE** | Raw articles | `abfss://bronze/news_articles/` | `data/delta/news_articles/` | Delta/Parquet | None | 1,000+ |
| **SILVER** | Deduplicated | `abfss://bronze/silver/...` | `data/delta/...` | Delta/Parquet | country, date | ~950 |
| **GOLD** | Cleaned | `abfss://bronze/gold/news_articles/` | `data/delta/...` | Delta/Parquet | country, date | 1,190 |
| **NLP (Local)** | Enriched JSONL | None | `data/nlp_enriched/` | JSONL | None | 1,190 |
| **NLP (ADLS)** | Enriched + flattened | `abfss://bronze/gold/nlp_enriched_articles/` | None | Delta/Parquet | country, date | 1,190 |

---

# File Cleanup Recommendation

To avoid confusion, **delete the deprecated `nlp/` folder:**

```powershell
Remove-Item -Path "nlp" -Force -Recurse
```

After this, only `nlp_pipeline/` exists (canonical).

---

# Directory Decision Tree

```
┌─ Is it Kafka/Redis related?
│  └─ YES → ingestion/
│
├─ Is it Spark job / transformation?
│  └─ YES → spark/
│
├─ Is it NLP processing?
│  └─ YES → nlp_pipeline/    (ONLY THIS ONE)
│
├─ Is it dashboard?
│  └─ YES → app.py (root level)
│
├─ Is it data storage?
│  ├─ Local cache? → data/delta/
│  ├─ Intermediate JSONL? → data/nlp_enriched/
│  └─ ADLS? → abfss://bronze/...
│
├─ Is it documentation?
│  └─ YES → DOCS/
│
└─ Is it configuration?
   └─ YES → config.py or .env (root level)
```

---

# Summary

| Aspect | Clear Answer |
|--------|--------------|
| **Bronze Layer** | `abfss://bronze/news_articles/` (ADLS) + `data/delta/` (local cache) |
| **Silver Layer** | `abfss://bronze/silver/news_articles/` (ADLS) - Not yet implemented |
| **Gold Layer (Before NLP)** | `abfss://bronze/gold/news_articles/` (ADLS) |
| **Gold Layer (After NLP)** | `abfss://bronze/gold/nlp_enriched_articles/` (ADLS) |
| **NLP Folder (OLD)** | DELETE `nlp/` - it's deprecated and confusing |
| **NLP Folder (CURRENT)** | USE `nlp_pipeline/` - this is the canonical package |
| **Intermediate NLP Data** | `data/nlp_enriched/enriched_articles_*.jsonl` (local JSONL files) |
| **All Layer Readers** | `spark/read_delta_lake.py` can read any ADLS path |
| **Configuration** | `config.py` defines all ADLS paths and Spark settings |

This structure is **clear, organized, and follows data engineering best practices!**
