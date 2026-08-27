import os

base_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(base_dir, ".."))

from .utils import configure_spark_home, configure_windows_environment

configure_windows_environment(project_root)
configure_spark_home()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, length, current_timestamp, lower, when, size, split
from pyspark.sql.types import StructType, StructField, StringType

# Define Schema matching our NewsArticle Pydantic model
article_schema = StructType([
    StructField("article_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("source_country", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("extracted_text", StringType(), True),
    StructField("ingest_timestamp", StringType(), True)
])

def create_spark_session():
    return (
        SparkSession.builder
        .appName("ContentIntel-News-Consumer")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # --- WINDOWS NATIVE IO FIXES ---
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.native.lib", "false")
        .getOrCreate()
    )

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting PySpark Structured Streaming Consumer...")

    # 1. Read Raw Stream from Kafka Topic 'news.raw'
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "news.raw")
        .option("startingOffsets", "earliest")
        .load()
    )

    # 2. Deserialize JSON payload & Apply Data Transformations
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), article_schema).alias("data"))
        .select("data.*")
    )

    # 3. Feature Engineering: Accurate word counts and sentiment flags
    transformed_df = (
        parsed_df
        .withColumn("text_length", length(col("extracted_text")))
        .withColumn("word_count", size(split(col("extracted_text"), r"\s+"))) # Proper word count heuristic
        .withColumn(
            "is_breaking_news", 
            when(lower(col("title")).contains("breaking") | lower(col("title")).contains("live"), True).otherwise(False)
        )
        .withColumn("processed_timestamp", current_timestamp())
    )

    # Define Local Output Locations for Delta Lake
    checkpoint_dir = os.path.join(project_root, "data", "checkpoints", "news_raw")
    delta_output_dir = os.path.join(project_root, "data", "delta", "news_articles")

    # 4. Write Streaming Data continuously to Delta Lake
    query = (
        transformed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .start(delta_output_dir)
    )

    print(f"Streaming write active. Ingesting into Delta Lake at: {delta_output_dir}")
    query.awaitTermination()

if __name__ == "__main__":
    process_stream()