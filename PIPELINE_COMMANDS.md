# ContentIntel Pipeline Commands

Run these commands from the project root:

```powershell
# Activate the Python environment for this project
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
& .\.venv\Scripts\Activate.ps1
```

## 1) Start supporting services

```powershell
# Start Redis + Kafka containers required by the pipeline
# Use this before running scraper or Spark consumer
docker compose up -d
```

## 2) Run the RSS scraper

```powershell
# Pull RSS articles, check dedupe in Redis, and publish valid items to Kafka topic: news.raw
# Keep this running in one terminal while the pipeline is active
python -m ingestion.rss_scrapper
```

## 3) Run the Spark Kafka consumer

```powershell
# Read messages from Kafka topic news.raw and write them to Delta Lake (bronze layer)
# Keep this running continuously in a separate terminal
python -m spark.spark_stream_consumer
```

## 4) Run the Silver layer processor

```powershell
# Read bronze Delta data, clean and enrich the records, and write them to the silver Delta table
# Run this after the bronze consumer is active
python -m spark.spark_silver_processor
```

## 5) Validate the Delta data

```powershell
# Read the Delta table from ADLS and print total row count + sample rows
# Use this after the consumer has written data
python -m spark.read_delta_lake
```

## 5) Stop the local services

```powershell
# Stop Redis and Kafka containers when you are done with the pipeline
docker compose down
```

## Optional: Reset deduplication cache

```powershell
# Clear Redis keys so previously scraped URLs are treated as new again
# Useful only when you want to re-ingest data from the beginning
docker exec contentintel_redis redis-cli FLUSHDB
```

## Typical startup order

```powershell
# Terminal 1
docker compose up -d

# Terminal 2
python -m spark.spark_stream_consumer

# Terminal 3
python -m ingestion.rss_scrapper

# Terminal 4 (when checking output)
python -m spark.read_delta_lake
```

> Tip: keep the scraper and Spark consumer running in separate terminals. They are the main live parts of the ingestion pipeline.

---

## 6) Run with Apache Airflow Orchestration

```powershell
# Start full container stack including Airflow Webserver & Scheduler
docker compose up -d

# Access Airflow Web UI at http://localhost:8080 (User: admin / Pass: admin)
# Unpause and trigger the DAG 'contentintel_news_pipeline'
```

