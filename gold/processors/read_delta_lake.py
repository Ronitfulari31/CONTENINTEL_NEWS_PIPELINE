import os

from config import Config
from utils.utils import configure_spark_home, configure_windows_environment, get_free_port

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
configure_windows_environment(project_root)
configure_spark_home()

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


def read_delta():
    storage_account = Config.AZURE_STORAGE_ACCOUNT
    storage_key = Config.AZURE_STORAGE_KEY
    if not storage_account or not storage_key or storage_key.startswith("YOUR_"):
        raise ValueError("AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_KEY is missing in your .env file!")

    spark = (
        SparkSession.builder
        .appName("Delta-Reader")
        .master("local[*]")
        .config("spark.ui.port", str(get_free_port()))
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(f"spark.hadoop.fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SharedKey")
        .config(f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.AzureLogStore")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.native.lib", "false")
        .getOrCreate()
    )

    try:
        df = spark.read.format("delta").load(Config.ADLS_BRONZE_PATH)
    except AnalysisException as error:
        spark.stop()
        if "PATH_NOT_FOUND" in str(error):
            raise FileNotFoundError(
                f"ADLS Delta table not found at {Config.ADLS_BRONZE_PATH}. "
                "Run the streaming consumer first and verify the bronze container exists."
            ) from error
        raise

    print("\n" + "=" * 60)
    print(f"Total Articles Persisted in Delta Lake: {df.count()}")
    print("=" * 60 + "\n")
    df.select("source_country", "title", "word_count", "is_breaking_news", "processed_timestamp") \
        .show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    read_delta()
