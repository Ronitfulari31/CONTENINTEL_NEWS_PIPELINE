from pyspark.sql import SparkSession
from config import Config
from spark.utils import get_free_port, setup_hadoop_env

def read_silver_table():
    setup_hadoop_env()

    spark = SparkSession.builder \
        .appName("ContentIntel-Silver-Reader") \
        .config("spark.ui.port", str(get_free_port())) \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-azure:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config(f"spark.hadoop.fs.azure.account.key.{Config.AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net", Config.AZURE_STORAGE_KEY) \
        .getOrCreate()

    print(f"Reading Silver Delta table from: {Config.ADLS_SILVER_PATH}")
    
    # Read the transformed Delta Lake table from Azure
    silver_df = spark.read.format("delta").load(Config.ADLS_SILVER_PATH)

    print("\n--- SILVER LAYER SCHEMA ---")
    silver_df.printSchema()

    print(f"\nTotal Cleaned Records: {silver_df.count()}")

    print("\n--- SAMPLE ENRICHED RECORDS ---")
    silver_df.select(
        "id", "clean_title", "domain", "word_count", 
        "est_read_time_mins", "source_country", "published_date"
    ).show(5, truncate=False)

if __name__ == "__main__":
    read_silver_table()