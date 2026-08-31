# 📰 ContentIntel — End-to-End News Data Engineering & NLP Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.1+-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1+-003366?style=for-the-badge&logo=delta&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Latest-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Qdrant](https://img.shields.io/badge/Qdrant-Vector%20DB-DC2626?style=for-the-badge&logo=qdrant&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)

An **enterprise-grade Data Engineering & NLP Intelligence Platform** built around a **Medallion Lakehouse Architecture (Bronze → Silver → Gold)**, **Apache Airflow Orchestration**, **Qdrant Vector DB Semantic Search**, a **9-Task Local NLP Pipeline**, and an interactive **Streamlit Analytics Dashboard**.

---

## 🌟 Data Engineering & AI Skills Demonstrated

### 1. **Medallion Lakehouse Architecture**
- **Bronze Layer (Raw Storage)**: Real-time RSS ingestion, Redis URL deduplication, Kafka streaming, and PySpark raw append to Azure ADLS Gen2 Delta Lake.
- **Silver Layer (Cleaned & Standardized)**: PySpark batch/stream cleaning, regex HTML removal, ISO timestamp casting, word count computation, and quality filtering (`word_count >= 30`).
- **Gold Layer (Business Analytics & Features)**: Business aggregates, publisher metrics, and `nlp_input_articles` feature tables partitioned by `source_country` and `published_date`.

### 2. **Pipeline Orchestration (Apache Airflow)**
- Automated DAG (`contentintel_news_pipeline`) scheduling hourly runs across 5 sequential stages.
- Guaranteed task execution ordering:
  $$\text{Bronze Ingestion} \longrightarrow \text{Silver Clean} \longrightarrow \text{Gold Feature Store} \longrightarrow \text{9-Task NLP} \longrightarrow \text{Qdrant Vector Indexing}$$
- Postgres metadata backend with Airflow Webserver (Port `8080`) and Scheduler running in Docker.

### 3. **Vector Data Engineering & Semantic Search**
- High-performance vector embeddings generated using **FastEmbed** (`BAAI/bge-small-en-v1.5`, 384-dimensions).
- **Qdrant Vector DB** indexing dense vectors alongside rich analytical metadata payloads (sentiment, entities, category, country).
- **Hybrid Search Engine**: Vector similarity combined with structured metadata filtering (category, country, sentiment) and article-to-article recommendation discovery loops.

### 4. **9-Stage AI / NLP Enrichment Engine**
1. **Preprocessing**: Unicode normalization (`ftfy`) & HTML/whitespace regex cleaning.
2. **Language Detection**: `fast_langdetect` (FastText Lite).
3. **Machine Translation**: Offline CPU-friendly translation (`argostranslate`) to convert non-English articles into English.
4. **Named Entity Recognition (NER)**: spaCy (`en_core_web_sm`) extracting 15+ entity types.
5. **Geographic Verification**: Cross-referencing entities against `geonamescache` (11.5M+ global cities and countries).
6. **Category Classification**: Rule-based keyword matching (Technology, Business, Politics, Sports, General).
7. **Keyword Extraction**: Term frequency ranking with category candidate filtering.
8. **Extractive Summarization**: Graph-based `sumy` LexRankSummarizer (preserves original text without hallucination).
9. **Sentiment Analysis**: `nltk` VADER Sentiment Intensity Analyzer (Polarity score & Label).

### 5. **Cloud Lakehouse & Containerization**
- **Azure ADLS Gen2**: Cloud storage integration with PySpark using `abfss://` protocols.
- **Docker Compose**: Containerized multi-service stack (Redis, Kafka, Qdrant, Postgres, Airflow Init, Webserver, Scheduler).

---

## 🏗️ Architecture & Pipeline Flow

```
                     ┌─────────────────────────┐
                     │   External RSS Feeds    │
                     │  (BBC US, UK, India...) │
                     └────────────┬────────────┘
                                  │
                                  ▼
                     ┌─────────────────────────┐
                     │  Bronze Ingestion Layer │
                     │   - fetch_rss.py        │
                     │   - Redis deduplication │
                     │   - Kafka (news.raw)    │
                     └────────────┬────────────┘
                                  │
                                  ▼
                     ┌─────────────────────────┐
                     │  Silver Processing      │
                     │(spark_silver_processor) │
                     │ Clean, sanitize, filter │
                     └────────────┬────────────┘
                                  │
                                  ▼
                     ┌─────────────────────────┐
                     │   Gold Processing       │
                     │ (spark_gold_processor)  │
                     │ Domain/country analytics│
                     └────────────┬────────────┘
                                  │
                                  ▼
                     ┌─────────────────────────┐
                     │  9-Task NLP Pipeline    │
                     │(nlp_enrichment_stand-   │
                     │  alone / pipeline.py)   │
                     └────────────┬────────────┘
                                  │
                        ┌─────────┴─────────┐
                        ▼                   ▼
             ┌─────────────────────┐   ┌───────────────────────────┐
             │ JSONL Storage       │   │ Qdrant Vector Database    │
             │ data/nlp_enriched/  │   │ (BAAI/bge-small-en-v1.5)  │
             └──────────┬──────────┘   └─────────────┬─────────────┘
                        │                            │
                        └─────────────┬──────────────┘
                                      ▼
                         ┌───────────────────────────┐
                         │ Streamlit Dashboard UI    │
                         │ (app.py)                  │
                         └───────────────────────────┘
```

---

## 📁 Project Directory Structure

```
ContentIntel News Pipeline/
│
├── 📁 bronze/                           # Raw Ingestion Layer
│   └── ingestion/
│       ├── fetch_rss.py                 # Async RSS feed fetcher
│       ├── kafka_producer.py            # Streaming producer to Kafka
│       ├── redis_client.py              # Redis URL deduplication client
│       └── rss_scrapper.py              # Web scraping utilities
│
├── 📁 silver/                           # Cleaned & Normalized Data Layer
│   └── processors/
│       └── spark_silver_processor.py    # PySpark Bronze -> Silver transformation
│
├── 📁 gold/                             # Business-Ready Enriched Layer
│   └── processors/
│       ├── spark_gold_processor.py      # PySpark Silver -> Gold aggregates & NLP features
│       └── read_delta_lake.py           # Delta Lake reading utilities
│
├── 📁 nlp_news/                         # 9-Task NLP Pipeline
│   ├── pipeline.py                      # Core 9-task NLP orchestrator
│   ├── nlp_enrichment_standalone.py     # Standalone local processor (JSONL output)
│   └── save_nlp_to_gold.py              # Persist enriched results to Delta Lake
│
├── 📁 searching/                        # Vector Database Indexer
│   └── indexer.py                       # FastEmbed + Qdrant vector indexer
│
├── 📁 recommendation/                   # Hybrid Search & Recommendation Engine
│   └── search_recommendations.py        # Qdrant hybrid search & related article discovery
│
├── 📁 dags/                             # Airflow Orchestration
│   └── news_pipeline_dag.py             # 5-stage automated DAG definition
│
├── 📁 DOCS/                             # Documentation & Flow Diagrams
│   ├── AIRFLOW_ORCHESTRATION.md         # Airflow setup & monitoring guide
│   └── complete_flow.md                 # End-to-end architecture guide
│
├── 📄 app.py                            # Streamlit Analytics Dashboard
├── 📄 config.py                         # Central configuration
├── 📄 models.py                         # Pydantic data schemas
├── 📄 docker-compose.yml                # Docker stack (Redis, Kafka, Qdrant, Airflow, Postgres)
├── 📄 PIPELINE_COMMANDS.md              # Pipeline execution manual
└── 📄 requirements.txt                  # Python dependencies
```

---

## 🚀 Quick Start Guide

### 1. Prerequisites
- Python 3.9+
- Docker Desktop
- Java 8/11 (for PySpark)

### 2. Environment Setup
```powershell
# Clone the repository
git clone https://github.com/your-username/CONTENINTEL_NEWS_PIPELINE.git
cd CONTENINTEL_NEWS_PIPELINE

# Create virtual environment & activate
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # Windows

# Install Python dependencies
pip install -r requirements.txt
```

### 3. Start Container Services (Docker)
```powershell
docker compose up -d
```
Starts Redis, Kafka, Qdrant Vector DB, Postgres, Airflow Webserver (Port `8080`), and Airflow Scheduler.

---

## 🔄 Running the Pipeline

### Option A: Via Apache Airflow UI (Automated)
1. Open your browser: [http://localhost:8080](http://localhost:8080) (User: `admin` / Password: `admin`)
2. Find `contentintel_news_pipeline`.
3. Toggle to **Unpause**, then click **Trigger DAG**.

### Option B: Manual Execution via Terminal
```powershell
# 1. Fetch RSS Feeds into Bronze
python -m bronze.ingestion.fetch_rss

# 2. Process Bronze to Silver (PySpark)
python -m silver.processors.spark_silver_processor

# 3. Process Silver to Gold (PySpark)
python -m gold.processors.spark_gold_processor

# 4. Execute 9-Task NLP Pipeline
python -m nlp_news.nlp_enrichment_standalone

# 5. Index into Qdrant Vector Store
python -m searching.indexer

# 6. Launch Streamlit Analytics Portal
streamlit run app.py
```

---

## 📊 Streamlit UI Screenshots & Features

### 📰 Page 1: IntelliNews Portal
- **Multi-Filter Bar**: Filter news headlines dynamically by category, country, or sentiment label.
- **Qdrant Semantic Search Bar**: Natural-language semantic queries powered by dense vector similarity.
- **Global Metrics Bar**: Total enriched headlines, active portal display count, topics detected, and average sentiment polarity.

### 🔬 Page 2: Focused 9-Task NLP Report & Recommendation Discovery Loop
- **Full Cleaned Article View**: Expandable text content view.
- **9-Task Visual Breakdown**:
  - NER entity tags (`PERSON`, `ORG`, `GPE`, `DATE`, etc.)
  - Verified geographic badges (`geonamescache`)
  - Extractive 2-sentence LexRank summary
  - Plotly VADER sentiment gauge indicator (-1.0 to +1.0)
- **Discovery Loop (Related Articles)**: Real-time Qdrant cosine vector recommendations with category bias.

---

## 📄 License
This project is licensed under the MIT License - see the `LICENSE` file for details.
