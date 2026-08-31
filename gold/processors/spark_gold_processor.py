import os

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    current_timestamp,
    max,
    min,
    round,
    sum,
)

from config import Config
from utils.utils import get_free_port, setup_hadoop_env


def ensure_gold_container_exists() -> None:
    if not Config.AZURE_STORAGE_ACCOUNT or not Config.AZURE_STORAGE_KEY:
        raise ValueError("AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY must be configured.")

    service = BlobServiceClient(
        account_url=f"https://{Config.AZURE_STORAGE_ACCOUNT}.blob.core.windows.net",
        credential=Config.AZURE_STORAGE_KEY,
    )
    container_client = service.get_container_client("gold")
    try:
        container_client.create_container()
        print("Created Azure Blob container: gold")
    except ResourceExistsError:
        print("Azure Blob container already exists: gold")


def create_gold_processor():
    setup_hadoop_env()
    ensure_gold_container_exists()

    spark = (
        SparkSession.builder.appName("ContentIntel-Gold-Processor")
        .config("spark.ui.port", str(get_free_port()))
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4",
        )
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            f"spark.hadoop.fs.azure.account.auth.type.{Config.AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net",
            "SharedKey",
        )
        .config(
            f"spark.hadoop.fs.azure.account.key.{Config.AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net",
            Config.AZURE_STORAGE_KEY,
        )
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.AzureLogStore",
        )
        .master("local[*]")
        .getOrCreate()
    )

    print("--- READING CLEANED SILVER DELTA TABLE ---")
    silver_df = spark.read.format("delta").load(Config.ADLS_SILVER_PATH)

    nlp_input_articles = (
        silver_df.filter(col("word_count") >= 30)
        .select(
            col("id").alias("article_id"),
            col("clean_title"),
            col("clean_content"),
            concat_ws("\n\n", col("clean_title"), col("clean_content")).alias("full_text_payload"),
            col("domain"),
            col("source_country"),
            col("published_at_ts"),
            col("published_date"),
            current_timestamp().alias("created_at"),
        )
    )

    nlp_input_path = f"{Config.ADLS_GOLD_PATH}/nlp_input_articles"
    (
        nlp_input_articles.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("source_country", "published_date")
        .save(nlp_input_path)
    )

    search_recommender_features = (
        silver_df.filter(col("word_count") >= 30)
        .select(
            col("id").alias("doc_id"),
            col("clean_title").alias("title"),
            col("url"),
            col("domain"),
            col("word_count"),
            col("est_read_time_mins"),
            col("published_at_ts"),
            col("published_date"),
            current_timestamp().alias("gold_indexed_at"),
        )
    )

    search_features_path = f"{Config.ADLS_GOLD_PATH}/search_recommender_features"
    (
        search_recommender_features.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("published_date")
        .save(search_features_path)
    )

    daily_publisher_summary = (
        silver_df.groupBy("published_date", "source_country", "domain")
        .agg(
            count("id").alias("total_articles"),
            sum("word_count").alias("total_word_count"),
            round(avg("word_count"), 2).alias("avg_word_count"),
            round(avg("est_read_time_mins"), 2).alias("avg_read_time_mins"),
            max("published_at_ts").alias("latest_article_ts"),
            min("published_at_ts").alias("earliest_article_ts"),
        )
        .withColumn("gold_processed_at", current_timestamp())
    )

    daily_metrics_path = f"{Config.ADLS_GOLD_PATH}/daily_publisher_metrics"
    (
        daily_publisher_summary.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("published_date")
        .save(daily_metrics_path)
    )

    print("--- GOLD LAYER PROCESSING COMPLETED SUCCESSFULLY ---")
    print(f"NLP Input Table: {nlp_input_path}")
    print(f"Search Features Table: {search_features_path}")


if __name__ == "__main__":
    create_gold_processor()