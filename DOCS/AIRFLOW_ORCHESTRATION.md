# 🚀 Apache Airflow Orchestration Guide

## 1. Overview
This project uses **Apache Airflow** to orchestrate and schedule the end-to-end data engineering and NLP intelligence pipeline.

Airflow automates the sequential execution of the 5 main pipeline stages:
1. **Bronze Ingestion**: Fetch RSS feeds, check Redis deduplication, publish to Kafka.
2. **Silver Processing**: Clean, normalize, and sanitize raw records with PySpark into Silver Delta Lake.
3. **Gold Processing**: Generate business analytics, publisher metrics, and create `nlp_input_articles` Delta tables.
4. **NLP Enrichment**: Execute the 9-task local NLP pipeline (NER, sentiment, summary, translation, categories) and save enriched JSONL files.
5. **Qdrant Vector Indexing**: Generate FastEmbed vector embeddings and index payloads into Qdrant Vector Store for hybrid search & recommendations.

---

## 2. DAG Structure (`contentintel_news_pipeline`)

```
   ┌───────────────────────┐
   │ ingest_rss_to_bronze  │  (Fetch RSS feeds & Bronze ingestion)
   └───────────┬───────────┘
               │
               ▼
   ┌───────────────────────┐
   │process_bronze_to_silver│ (PySpark Silver cleaning & schema sanitization)
   └───────────┬───────────┘
               │
               ▼
   ┌───────────────────────┐
   │ process_silver_to_gold│ (PySpark Gold analytics & nlp_input tables)
   └───────────┬───────────┘
               │
               ▼
   ┌───────────────────────┐
   │  run_nlp_enrichment   │ (9-Task local NLP enrichment -> JSONL output)
   └───────────┬───────────┘
               │
               ▼
   ┌───────────────────────┐
   │    index_to_qdrant    │ (Qdrant vector indexing BAAI/bge-small-en-v1.5)
   └───────────────────────┘
```

**DAG Configuration**:
- **DAG File**: `dags/news_pipeline_dag.py`
- **DAG ID**: `contentintel_news_pipeline`
- **Schedule**: `@hourly` (runs every hour)
- **Catchup**: `False`

---

## 3. How to Run Airflow

### Option A: Using Docker Compose (Recommended)

Start all services (Redis, Kafka, Qdrant, Postgres DB, Airflow Webserver, Airflow Scheduler):

```powershell
docker compose up -d
```

Access the Airflow Web UI in your browser:
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Username**: `admin`
- **Password**: `admin`

In the Airflow UI, find `contentintel_news_pipeline`, unpause the DAG toggle, and click **Trigger DAG** to run the pipeline.

To stop all services:
```powershell
docker compose down
```

---

### Option B: Running Airflow Locally (Virtual Environment)

If running Airflow directly inside your Python environment:

1. **Install Apache Airflow**:
   ```powershell
   pip install "apache-airflow>=2.8.0"
   ```

2. **Initialize Airflow Database & User**:
   ```powershell
   $env:AIRFLOW_HOME = "$PWD\airflow_home"
   airflow db init
   airflow users create --username admin --password admin --firstname Airflow --lastname Admin --role Admin --email admin@example.com
   ```

3. **Start Airflow Standalone**:
   ```powershell
   airflow standalone
   ```

4. **Trigger DAG from CLI**:
   ```powershell
   airflow dags trigger contentintel_news_pipeline
   ```

---

## 4. Monitoring & Logs

- **Task Execution Status**: View active DAG runs, Gantt charts, and task durations in the Airflow UI at `http://localhost:8080`.
- **Logs**: Click on any task node (e.g. `run_nlp_enrichment`) -> **Logs** to view stdout and stderr for PySpark, NLP, or Qdrant execution.
