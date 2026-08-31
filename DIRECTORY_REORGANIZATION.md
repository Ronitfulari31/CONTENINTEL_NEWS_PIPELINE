# 📁 Directory Reorganization - Layer-Based Structure

## Overview
The project directory has been reorganized into a **layer-based architecture** for better clarity and maintainability. All data processing follows a clear pipeline: Bronze → Silver → Gold → NLP News enrichment.

---

## 🏗️ New Directory Structure

```
ContentIntel News Pipeline/
│
├── 📁 bronze/                          # Raw ingestion layer
│   ├── __init__.py
│   └── ingestion/
│       ├── __init__.py
│       ├── fetch_rss.py                # RSS feed fetching
│       ├── kafka_producer.py           # Kafka producer for streaming
│       ├── redis_client.py             # Redis client utilities
│       └── rss_scrapper.py             # Web scraping utilities
│
├── 📁 silver/                          # Cleaned & normalized data layer
│   ├── __init__.py
│   └── processors/
│       ├── __init__.py
│       └── spark_silver_processor.py   # Bronze → Silver transformation
│
├── 📁 gold/                            # Business-ready enriched data layer
│   ├── __init__.py
│   └── processors/
│       ├── __init__.py
│       ├── spark_gold_processor.py     # Gold layer processor
│       ├── read_delta_lake.py          # Delta Lake reading utilities
│       └── utils.py                    # Spark utilities
│
├── 📁 nlp_news/                        # NLP enrichment (News-specific)
│   ├── __init__.py
│   ├── pipeline.py                     # Main NLP pipeline (9 tasks)
│   ├── nlp_enrichment_standalone.py    # Standalone NLP processor
│   ├── save_nlp_to_gold.py             # Save enriched results to Gold
│   ├── gold_nlp_processor.py           # Alternative Spark NLP processor
│   └── enrichment.py                   # NLP helper utilities
│
├── 📁 spark/                           # Generic Spark utilities (shared)
│   ├── __init__.py
│   ├── read_delta_lake.py              # Read Delta Lake tables
│   ├── read_silver_delta.py            # Read Silver layer
│   ├── spark_gold_processor.py         # Gold processor
│   ├── spark_silver_processor.py       # Silver processor (original)
│   ├── spark_stream_consumer.py        # Stream consumer
│   └── utils.py                        # Spark setup & utilities
│
├── 📁 data/                            # Local data storage
│   ├── checkpoints/                    # Kafka checkpoints
│   │   └── news_raw/
│   ├── delta/                          # Local Delta Lake tables
│   │   └── news_articles/
│   └── nlp_enriched/                   # NLP enrichment results (JSONL)
│
├── 📁 DOCS/                            # Documentation
│   ├── DIRECTORY_STRUCTURE.md          # Data layer clarifications
│   ├── complete_flow.md                # End-to-end pipeline documentation
│   ├── bronze_silver/
│   ├── silver_to_gold/
│   ├── source_ingestion_bronze/
│   └── [other documentation files]
│
├── 📄 config.py                        # Central configuration
├── 📄 models.py                        # Data models & schemas
├── 📄 app.py                           # Streamlit dashboard
├── 📄 requirements.txt                 # Python dependencies
├── 📄 docker-compose.yml               # Docker services (Kafka, Redis)
└── 📄 PIPELINE_COMMANDS.md             # Pipeline execution commands
```

---

## 🔄 Data Flow Architecture

```
RSS Feeds
   ↓
bronze/ingestion/          ← Data Collection (Raw)
   ↓
silver/processors/         ← Data Cleaning & Normalization
   ↓
gold/processors/           ← Business Logic & Enrichment
   ↓
nlp_news/                  ← NLP Tasks (9 stages)
   ↓
Gold Layer (ADLS)         ← Final enriched storage
   ↓
app.py (Streamlit)        ← Dashboard visualization
```

---

## 📦 Layer Responsibilities

### **Bronze Layer** (`bronze/ingestion/`)
**Purpose**: Data ingestion from external sources

| File | Purpose |
|------|---------|
| `fetch_rss.py` | Fetch and parse RSS feeds |
| `kafka_producer.py` | Stream articles to Kafka |
| `rss_scrapper.py` | Web scraping utilities |
| `redis_client.py` | Redis caching client |

**Output**: Raw articles streamed to Kafka, stored in Bronze Delta

---

### **Silver Layer** (`silver/processors/`)
**Purpose**: Data cleaning, normalization, and standardization

| File | Purpose |
|------|---------|
| `spark_silver_processor.py` | Transform Bronze → Silver (schema sanitization, normalization) |

**Transformations**:
- HTML tag removal
- Data type casting & normalization
- Null value handling
- Quality filtering

**Output**: Cleaned articles in Silver Delta

---

### **Gold Layer** (`gold/processors/`)
**Purpose**: Business-ready enrichment and optimization

| File | Purpose |
|------|---------|
| `spark_gold_processor.py` | Gold layer processing & enrichment |
| `read_delta_lake.py` | Read Delta Lake tables |
| `utils.py` | Spark utilities (shared) |

**Output**: Enriched articles in Gold Delta

---

### **NLP News Layer** (`nlp_news/`)
**Purpose**: NLP enrichment with 9 sequential tasks

| File | Purpose |
|------|---------|
| `pipeline.py` | Main NLP orchestrator (9 tasks) |
| `nlp_enrichment_standalone.py` | Standalone local processor (recommended) |
| `save_nlp_to_gold.py` | Persist enriched results to Gold |
| `gold_nlp_processor.py` | Alternative Spark-based processor |
| `enrichment.py` | NLP helper utilities |

**9 NLP Tasks**:
1. Text preprocessing (ftfy, regex normalization)
2. Language detection (FastText)
3. Translation (ArgosTranslate for non-English)
4. Named Entity Recognition (spaCy)
5. Location extraction & verification
6. Category classification
7. Keyword extraction (TF-based)
8. Summary generation (LexRank)
9. Sentiment analysis (VADER)

**Output**: Enriched articles with nested NLP fields in Gold Delta

---

## 🔄 Import Examples

### Before (Old Structure)
```python
from ingestion.fetch_rss import fetch_all_feeds
from nlp_pipeline.pipeline import run_nlp_pipeline
from spark.utils import setup_hadoop_env
```

### After (New Structure)
```python
from bronze.ingestion.fetch_rss import fetch_all_feeds
from nlp_news.pipeline import run_nlp_pipeline
from spark.utils import setup_hadoop_env  # (unchanged - still in spark/)
```

---

## 📋 Migration Summary

### ✅ Completed Operations

1. **Created layer directories**:
   - ✓ `bronze/` with `ingestion/` subdirectory
   - ✓ `silver/` with `processors/` subdirectory
   - ✓ `gold/` with `processors/` subdirectory
   - ✓ `nlp_news/` directory

2. **Moved files**:
   - ✓ `ingestion/*` → `bronze/ingestion/`
   - ✓ `nlp_pipeline/*` → `nlp_news/`

3. **Copied Spark processors**:
   - ✓ `spark/spark_silver_processor.py` → `silver/processors/`
   - ✓ `spark/spark_gold_processor.py` → `gold/processors/`
   - ✓ `spark/utils.py` → `gold/processors/` (for reference)
   - ✓ `spark/read_delta_lake.py` → `gold/processors/` (for reference)

4. **Updated imports**:
   - ✓ `from nlp_pipeline.` → `from nlp_news.`
   - ✓ `from ingestion.` → `from bronze.ingestion.`
   - ✓ All Python files updated

5. **Cleaned up**:
   - ✓ Removed empty `ingestion/` folder
   - ✓ Removed empty `nlp_pipeline/` folder
   - ✓ Added `__init__.py` to all layer packages

6. **Validated**:
   - ✓ All imports working correctly
   - ✓ Structure organized by data layers

---

## 🚀 Usage Going Forward

### Running Bronze Layer (Ingestion)
```python
from bronze.ingestion.fetch_rss import fetch_all_feeds
feeds = await fetch_all_feeds()
```

### Running Silver Layer Processing
```python
from silver.processors.spark_silver_processor import create_silver_processor
create_silver_processor()
```

### Running Gold Layer Processing
```python
from gold.processors.spark_gold_processor import ensure_gold_container_exists
ensure_gold_container_exists()
```

### Running NLP Enrichment
```python
from nlp_news.nlp_enrichment_standalone import enrich_articles_locally
enrich_articles_locally()
```

---

## 📍 ADLS Data Locations

Each layer also has corresponding storage in Azure ADLS Gen2:

| Layer | ADLS Path | Format |
|-------|-----------|--------|
| Bronze | `abfss://bronze/bronze/news_articles/` | Delta Table |
| Silver | `abfss://bronze/silver/news_articles/` | Delta Table |
| Gold | `abfss://bronze/gold/news_articles/` | Delta Table |
| NLP | `abfss://bronze/gold/nlp_enriched_articles/` | Delta Table (partitioned) |

---

## 🔗 Related Documentation

- [Complete Flow Documentation](DOCS/complete_flow.md) - End-to-end pipeline overview
- [Silver to Gold Transformation](DOCS/silver_to_gold_nlp/silver_to_gold.md)
- [Gold to NLP Enrichment](DOCS/silver_to_gold_nlp/gold_to_nlp_to_gold.md)
- [Directory Structure Reference](DOCS/DIRECTORY_STRUCTURE.md)

---

## ✨ Benefits of New Structure

✅ **Clarity**: Clear separation of data layers (Bronze → Silver → Gold → NLP)  
✅ **Maintainability**: Organized packages by responsibility  
✅ **Scalability**: Easy to add new processors to each layer  
✅ **Documentation**: Self-documenting through folder names  
✅ **Isolation**: Each layer can be tested and deployed independently  

---

*Last Updated: 2026-08-31*
