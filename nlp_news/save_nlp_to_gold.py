"""
Save NLP Enriched Articles to Gold Layer
Reads local JSONL file and persists to ADLS Gen2 Delta table for analytics.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

from config import Config
from utils.utils import get_free_port, setup_hadoop_env

GOLD_ENRICHED_PATH = f"{Config.ADLS_GOLD_PATH}/nlp_enriched_articles"


def create_spark_session():
    setup_hadoop_env()
    return SparkSession.builder \
        .appName("Gold_NLP_Persist") \
        .config("spark.ui.port", str(get_free_port())) \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config(f"spark.hadoop.fs.azure.account.auth.type.{Config.AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net", "SharedKey") \
        .config(f"spark.hadoop.fs.azure.account.key.{Config.AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net", Config.AZURE_STORAGE_KEY) \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.AzureLogStore") \
        .master("local[1]") \
        .getOrCreate()


def main():
    print("═" * 80)
    print("SAVE NLP ENRICHED DATA TO GOLD LAYER")
    print("═" * 80)

    jsonl_dir = Path("data/nlp_enriched")
    if not jsonl_dir.exists():
        raise FileNotFoundError(f"NLP enrichment directory not found: {jsonl_dir}")

    jsonl_files = sorted(jsonl_dir.glob("enriched_articles_*.jsonl"), reverse=True)
    if not jsonl_files:
        raise FileNotFoundError(f"No enriched JSONL files found in {jsonl_dir}")

    target_file = str(jsonl_files[0])
    file_size_mb = Path(target_file).stat().st_size / (1024 * 1024)

    print(f"\n📖 Reading enriched local dataset:")
    print(f"   File: {target_file}")
    print(f"   Size: {file_size_mb:.2f} MB")

    print(f"\n🔧 Initializing Spark Session...")
    spark = create_spark_session()

    try:
        print(f"\n📥 Reading JSONL into DataFrame...")
        enriched_df = spark.read.json(target_file)
        total_records = enriched_df.count()
        print(f"   ✓ Loaded {total_records} records")

        print(f"\n🔄 Flattening nested NLP structure...")
        structured_df = enriched_df.select(
            col("article_id"),
            col("title"),
            col("url_domain").alias("domain"),
            col("source_country"),
            col("published_date"),
            col("nlp.language").alias("detected_language"),
            col("nlp.category").alias("predicted_category"),
            col("nlp.sentiment.label").alias("sentiment_label"),
            col("nlp.sentiment.polarity_score").alias("sentiment_polarity"),
            col("nlp.summary").alias("summary"),
            col("nlp.entities").alias("ner_entities"),
            col("nlp.locations").alias("extracted_locations"),
            col("processed_at").alias("nlp_processed_at"),
            current_timestamp().alias("ingested_to_gold_at")
        )

        print(f"\n📋 Schema Validation:")
        structured_df.printSchema()

        print(f"\n📊 Statistics:")
        print(f"   Total Records: {total_records}")
        print(f"   Columns: {len(structured_df.columns)}")

        print(f"\n💾 Persisting to ADLS Gen2 Gold Layer...")
        print(f"   Path: {GOLD_ENRICHED_PATH}")
        structured_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(GOLD_ENRICHED_PATH)

        print(f"\n✅ Successfully persisted NLP Enriched Data into ADLS Gen2 Gold Layer!")
        print(f"   Records Written: {total_records}")
        print(f"   Timestamp: {enriched_df.select(col('processed_at')).first()[0]}")
        print("\n" + "=" * 80)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
