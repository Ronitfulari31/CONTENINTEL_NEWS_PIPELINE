import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    ceil,
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    lit,
    regexp_extract,
    regexp_replace,
    size,
    split,
    to_date,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import IntegerType

from config import Config
from spark.utils import get_free_port, setup_hadoop_env


def create_silver_processor():
    setup_hadoop_env()

    spark = (
        SparkSession.builder.appName("ContentIntel-Silver-Processor")
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
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.LocalFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.file.impl",
            "org.apache.hadoop.fs.local.LocalFs",
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
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .master("local[*]")
        .getOrCreate()
    )

    bronze_df = spark.readStream.format("delta").load(Config.ADLS_BRONZE_PATH)

    bronze_text_col = coalesce(
        col("full_content"),
        col("content"),
        col("summary_snippet"),
        lit(""),
    )

    silver_df = (
        bronze_df.filter(col("id").isNotNull() & col("title").isNotNull())
        .withColumn(
            "published_at_ts",
            to_timestamp(col("published_at"), "EEE, dd MMM yyyy HH:mm:ss z"),
        )
        .withColumn("published_date", to_date(col("published_at_ts")))
        .withColumn(
            "clean_title",
            trim(regexp_replace(col("title"), r"<[^>]+>", " ")),
        )
        .withColumn(
            "clean_content_raw",
            regexp_replace(
                regexp_replace(bronze_text_col, r"<[^>]+>", " "),
                r"\s+",
                " ",
            ),
        )
        .withColumn("clean_content", trim(col("clean_content_raw")))
        .withColumn(
            "word_count",
            when(
                trim(col("clean_content")).isNull() | (trim(col("clean_content")) == ""),
                lit(0),
            ).otherwise(size(split(trim(col("clean_content")), " ")).cast(IntegerType())),
        )
        .withColumn(
            "est_read_time_mins",
            ceil(col("word_count") / lit(200)).cast(IntegerType()),
        )
        .withColumn(
            "domain",
            regexp_extract(col("url"), r"https?://(?:www\.)?([^/]+)", 1),
        )
        .withColumn(
            "full_text_payload",
            concat_ws("\n\n", col("clean_title"), col("clean_content")),
        )
        .withColumn("silver_processed_at", current_timestamp())
        .filter(col("word_count") >= 30)
    )

    query = (
        silver_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", Config.ADLS_SILVER_CHECKPOINT_PATH)
        .partitionBy("source_country", "published_date")
        .start(Config.ADLS_SILVER_PATH)
    )

    print(f"Silver Streaming Job started. Writing to: {Config.ADLS_SILVER_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    create_silver_processor()