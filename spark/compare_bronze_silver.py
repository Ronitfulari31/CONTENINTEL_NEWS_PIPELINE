from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config import Config
from spark.utils import get_free_port, setup_hadoop_env

def compare_bronze_silver():
    setup_hadoop_env()

    spark = SparkSession.builder \
        .appName("ContentIntel-Bronze-Vs-Silver-Comparison") \
        .config("spark.ui.port", str(get_free_port())) \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-azure:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config(f"spark.hadoop.fs.azure.account.key.{Config.AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net", Config.AZURE_STORAGE_KEY) \
        .getOrCreate()

    print("\n--- LOADING BRONZE AND SILVER TABLES FROM AZURE ---")
    bronze_df = spark.read.format("delta").load(Config.ADLS_BRONZE_PATH)
    silver_df = spark.read.format("delta").load(Config.ADLS_SILVER_PATH)

    print(f"Bronze Total Rows: {bronze_df.count()}")
    print(f"Silver Total Rows: {silver_df.count()}")

    # Alias dataframes to easily compare columns side-by-side
    b = bronze_df.alias("b")
    s = silver_df.alias("s")

    # Join Bronze and Silver on Article ID
    joined_df = b.join(s, col("b.id") == col("s.id"))

    print("\n==========================================================================")
    print(" 1. TIMESTAMP & DATE CASTING COMPARISON")
    print("==========================================================================")
    joined_df.select(
        col("b.published_at").alias("BRONZE_published_at_raw_string"),
        col("s.published_at_ts").alias("SILVER_published_at_timestamp"),
        col("s.published_date").alias("SILVER_published_date_extracted")
    ).show(3, truncate=False)

    print("\n==========================================================================")
    print(" 2. TEXT CLEANING & HTML SANITIZATION COMPARISON")
    print("==========================================================================")
    joined_df.select(
        col("b.title").alias("BRONZE_raw_title"),
        col("s.clean_title").alias("SILVER_clean_title")
    ).show(3, truncate=False)

    print("\n==========================================================================")
    print(" 3. DOMAIN EXTRACTION & FEATURE ENGINEERING COMPARISON")
    print("==========================================================================")
    joined_df.select(
        col("b.url").alias("BRONZE_raw_url"),
        col("s.domain").alias("SILVER_extracted_domain"),
        col("s.word_count").alias("SILVER_word_count"),
        col("s.est_read_time_mins").alias("SILVER_est_read_time_mins")
    ).show(3, truncate=False)

    print("\n==========================================================================")
    print(" 4. FULL CONTENT TRANSFORMATION (RAW TEXT VS CLEANED TEXT)")
    print("==========================================================================")
    sample = joined_df.select("b.content", "s.clean_content").first()
    if sample:
        print("\n--- RAW BRONZE CONTENT SAMPLE (FIRST 200 CHARS) ---")
        print(repr(sample["content"][:200]) if sample["content"] else "None")
        print("\n--- CLEANED SILVER CONTENT SAMPLE (FIRST 200 CHARS) ---")
        print(repr(sample["clean_content"][:200]) if sample["clean_content"] else "None")

if __name__ == "__main__":
    compare_bronze_silver()