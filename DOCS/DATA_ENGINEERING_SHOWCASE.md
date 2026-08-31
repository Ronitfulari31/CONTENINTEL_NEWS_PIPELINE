# 🎯 Data Engineering Portfolio & Resume Showcase Guide

## 1. 60-Second Technical Elevator Pitch

> *"In this project, I built ContentIntel, an end-to-end news intelligence platform using a Medallion Lakehouse Architecture (Bronze → Silver → Gold) on Apache Spark and Delta Lake. It streams RSS feeds into Kafka with Redis deduplication, cleanses data in PySpark, and runs an offline 9-task NLP enrichment pipeline (spaCy, FastText, ArgosTranslate, VADER, LexRank). Enriched output is indexed into Qdrant Vector Database using 384-dimensional FastEmbed embeddings for hybrid semantic search and recommendation discovery loops, all automated via an hourly Apache Airflow DAG and visualized in a Streamlit dual-view portal."*

---

## 2. Professional Resume Bullet Points

- **Data Lakehouse Architecture**: Designed and deployed a multi-layer Medallion Lakehouse (Bronze, Silver, Gold) using **PySpark** and **Delta Lake** on **Azure ADLS Gen2**, enabling ACID transactions, time travel, and Schema Enforcement for real-time news articles.
- **Pipeline Orchestration**: Built an automated **Apache Airflow DAG** (`contentintel_news_pipeline`) orchestrating 5 sequential pipeline stages with retry logic, scheduling, and containerized Postgres backend.
- **Streaming & Deduplication**: Implemented real-time feed ingestion into **Apache Kafka** with **Redis** in-memory URL deduplication, processing 1,000+ feeds while eliminating duplicate record processing.
- **AI & Vector Engineering**: Integrated **Qdrant Vector Database** and **FastEmbed** (`BAAI/bge-small-en-v1.5`), building a hybrid search engine that combines 384-dimensional dense vector similarity with structured metadata filters (category, country, sentiment).
- **NLP & Feature Engineering**: Developed a CPU-optimized 9-task local NLP pipeline (spaCy NER, FastText language detection, ArgosTranslate, LexRank summarization, VADER sentiment, Geonames location verification) outputting structured JSONL feature stores.
- **Dashboard & BI**: Created an interactive **Streamlit** dual-view portal rendering live Qdrant semantic search results, Plotly sentiment gauge metrics, and vector recommendation discovery loops.

---

## 3. Core Architecture Decisions & Technical Trade-offs

### Q: Why Delta Lake instead of plain Parquet?
- **ACID Transactions**: Prevents corrupt state when continuous streaming jobs append records concurrently.
- **Time Travel & Data Lineage**: Allows querying historical table snapshots to audit data quality over time.
- **Schema Enforcement & Evolution**: Ensures incoming dirty Bronze records do not break Silver/Gold queries.

### Q: Why standalone CPU-based NLP enrichment instead of PySpark UDFs?
- **Spark Serialization Bottlenecks**: Heavy C-extension Python libraries (spaCy, ArgosTranslate, FastText) suffer from worker serialization overhead and memory leaks when wrapped in PySpark UDFs on Windows/CPUs.
- **Modular Isolation**: Running NLP as a batch task reading Gold Delta tables and producing JSONL staging files isolates compute resources, prevents worker OOM errors, and makes debugging straightforward.

### Q: Why Qdrant Vector Database instead of traditional ElasticSearch/Solr?
- **Dense Vector Search**: Standard keyword inverted indexes fail on semantic intent (e.g. searching "tech breakthroughs" matching "AI milestone"). Qdrant provides HNSW indexing for fast cosine vector similarity.
- **Hybrid Metadata Filtering**: Allows filtering by exact categories (`category='Technology'`) AND computing vector distance in a single query pass (`query_points`).

---

## 4. Technical Skills Matrix

| Data Engineering Domain | Technologies Used | Demonstrated Competency |
| :--- | :--- | :--- |
| **Distributed Data Processing** | Apache Spark, PySpark | DataFrame transformations, schema sanitization, window aggregations |
| **Storage & Lakehouse** | Delta Lake, Azure ADLS Gen2 | Partitioning, merge schemas, ACID transactions, cloud protocols (`abfss://`) |
| **Orchestration** | Apache Airflow | DAG creation, BashOperator, task dependencies, Postgres backend |
| **Streaming & Messaging** | Apache Kafka, Redis | Topic publishing, consumer streams, hash-based URL deduplication |
| **Vector Search & AI** | Qdrant, FastEmbed, spaCy | 384-dim dense embeddings, cosine similarity, hybrid search, NER |
| **NLP & Feature Extraction** | VADER, LexRank, FastText, ArgosTranslate | Extractive summaries, sentiment scoring, language routing, translation |
| **Infrastructure & CI/CD** | Docker, Docker Compose | Multi-container setup (Kafka, Redis, Qdrant, Postgres, Airflow) |
| **Visualization & BI** | Streamlit, Plotly | Interactive UI grids, Plotly gauges, session state navigation |
