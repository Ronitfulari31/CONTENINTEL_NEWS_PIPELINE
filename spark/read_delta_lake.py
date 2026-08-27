import os

from .utils import configure_spark_home, configure_windows_environment

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
configure_windows_environment(project_root)
configure_spark_home()

from pyspark.sql import SparkSession


def read_delta():
    spark = (
        SparkSession.builder
        .appName("Delta-Reader")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.native.lib", "false")
        .getOrCreate()
    )

    delta_path = os.path.join(project_root, "data", "delta", "news_articles")
    if not os.path.exists(delta_path):
        print(f"Path not found: {delta_path}\nEnsure spark_stream_consumer has ingested data.")
        spark.stop()
        return

    df = spark.read.format("delta").load(delta_path)

    print("\n" + "=" * 60)
    print(f"Total Articles Persisted in Delta Lake: {df.count()}")
    print("=" * 60 + "\n")
    df.select("source_country", "title", "word_count", "is_breaking_news", "processed_timestamp") \
        .show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    read_delta()
