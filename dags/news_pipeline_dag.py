"""
Airflow DAG: News Intelligence & Medallion Data Pipeline
Orchestrates the end-to-end flow:
1. Ingest RSS Feeds (Bronze)
2. Process Silver Layer (Clean & Sanitize)
3. Process Gold Layer (Business Aggregations & NLP Input)
4. Run 9-Task NLP Enrichment Standalone
5. Index Enriched Articles into Qdrant Vector Store
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Ensure project root is in sys.path for Airflow task runners
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="contentintel_news_pipeline",
    default_args=default_args,
    description="End-to-end Medallion pipeline orchestration with NLP & Vector Indexing",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["news", "medallion", "spark", "nlp", "qdrant"],
) as dag:

    # Task 1: Ingest RSS Feeds to Bronze Layer
    ingest_bronze = BashOperator(
        task_id="ingest_rss_to_bronze",
        bash_command=f'cd "{PROJECT_ROOT}" && python -m bronze.ingestion.fetch_rss',
    )

    # Task 2: Transform Bronze to Silver Layer (PySpark Data Sanitization)
    process_silver = BashOperator(
        task_id="process_bronze_to_silver",
        bash_command=f'cd "{PROJECT_ROOT}" && python -m silver.processors.spark_silver_processor',
    )

    # Task 3: Transform Silver to Gold Layer (PySpark Business Aggregations & NLP Input)
    process_gold = BashOperator(
        task_id="process_silver_to_gold",
        bash_command=f'cd "{PROJECT_ROOT}" && python -m gold.processors.spark_gold_processor',
    )

    # Task 4: Run Standalone 9-Task NLP Enrichment Pipeline
    run_nlp = BashOperator(
        task_id="run_nlp_enrichment",
        bash_command=f'cd "{PROJECT_ROOT}" && python -m nlp_news.nlp_enrichment_standalone',
    )

    # Task 5: Index Enriched JSONL Outputs into Qdrant Vector Store
    index_qdrant = BashOperator(
        task_id="index_to_qdrant",
        bash_command=f'cd "{PROJECT_ROOT}" && python -m searching.indexer',
    )

    # Define Linear Pipeline Dependency Chain
    ingest_bronze >> process_silver >> process_gold >> run_nlp >> index_qdrant
