import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

    AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY", "")
    AZURE_CONTAINER = os.getenv("AZURE_CONTAINER", "bronze")
    ADLS_BRONZE_PATH = f"abfss://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/news_articles"
    ADLS_CHECKPOINT_PATH = f"abfss://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/news_raw"
    ADLS_SILVER_PATH = f"abfss://silver@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/news_articles_cleaned"
    ADLS_SILVER_CHECKPOINT_PATH = f"abfss://silver@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/news_silver"
    ADLS_GOLD_PATH = f"abfss://gold@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/news_analytics"
   
    RSS_FEEDS = {
        "US": ["https://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml"],
        "UK": ["https://feeds.bbci.co.uk/news/uk/rss.xml"],
        "INDIA": ["https://feeds.bbci.co.uk/news/world/asia/india/rss.xml"],
    }
    SCRAPER_INTERVAL_SECONDS = int(os.getenv("SCRAPER_INTERVAL_SECONDS", "300"))
