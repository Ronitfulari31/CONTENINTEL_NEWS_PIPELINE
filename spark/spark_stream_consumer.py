import os
import sys

base_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(base_dir, ".."))

from config import Config
from .utils import configure_spark_home, configure_windows_environment, get_free_port

configure_windows_environment(project_root)
configure_spark_home()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp, from_json, lower, size, split
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Schema matching NewsArticle Pydantic model
article_schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("source_country", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("content", StringType(), True),
    StructField("word_count", IntegerType(), True),
    StructField("is_breaking_news", BooleanType(), True),
    StructField("article_id", StringType(), True),
    StructField("extracted_text", StringType(), True),
    StructField("ingest_timestamp", StringType(), True),
])

def create_spark_session():
    storage_account = Config.AZURE_STORAGE_ACCOUNT
    storage_key = Config.AZURE_STORAGE_KEY
    if not storage_account or not storage_key or storage_key.startswith("YOUR_"):
        raise ValueError("AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_KEY is missing in your .env file!")

    return (
        SparkSession.builder
        .appName("ContentIntel-News-Consumer")
        .master("local[*]")
        .config("spark.ui.port", str(get_free_port()))
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-azure:3.3.4"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # Azure Authentication
        .config(f"spark.hadoop.fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SharedKey")
        .config(f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.AzureLogStore")
        
        # FIX FOR WINDOWS ClassCastException & LOCAL BUFFERING
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
        .config("spark.hadoop.fs.azure.data.blocks.buffer", "array")
        .getOrCreate()
    )

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\nStarting PySpark Structured Streaming Consumer...")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "news.raw")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), article_schema).alias("data"))
        .select("data.*")
    )

    transformed_df = parsed_df.withColumn("processed_timestamp", current_timestamp())

    checkpoint_dir = Config.ADLS_CHECKPOINT_PATH
    delta_output_dir = Config.ADLS_BRONZE_PATH

    print(f"Streaming write active. Ingesting into Delta Lake at: {delta_output_dir}\n")

    query = (
        transformed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .start(delta_output_dir)
    )

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()