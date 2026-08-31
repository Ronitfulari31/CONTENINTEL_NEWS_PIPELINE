import asyncio
import hashlib
import logging
import httpx
import feedparser
import trafilatura
from pydantic import ValidationError

from config import Config
from .redis_client import RedisClient
from .kafka_producer import KafkaProducerWrapper
from models import ArticleSchema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def generate_url_hash(url: str) -> str:
    """Generates an MD5 hash for URL deduplication and article_id."""
    return hashlib.md5(url.encode("utf-8")).hexdigest()

async def fetch_article_text(client: httpx.AsyncClient, url: str) -> str:
    """Fetches full article HTML and extracts clean text using trafilatura."""
    try:
        response = await client.get(url, timeout=10.0, follow_redirects=True)
        if response.status_code == 200:
            extracted = trafilatura.extract(response.text)
            return extracted if extracted else ""
    except Exception as e:
        logger.warning(f"Failed extracting text from {url}: {e}")
    return ""

async def process_feed_entry(entry, country_code: str, http_client: httpx.AsyncClient, redis_client: RedisClient, kafka_producer: KafkaProducerWrapper):
    """Processes a single RSS feed entry, validates it, and publishes to Kafka."""
    url = getattr(entry, "link", None)
    if not url:
        return

    url_hash = generate_url_hash(url)

    # 1. Deduplication Check via Redis
    if await redis_client.is_duplicate(url_hash):
        return

    # 2. Extract Full Article Body Text
    extracted_text = await fetch_article_text(http_client, url)
    if not extracted_text:
        return  # Skip articles with empty body text or blocked requests (e.g. paywalls)

    title = getattr(entry, "title", "No Title")
    published_at = getattr(entry, "published", None)

    # 3. Pydantic Model Validation
    try:
        article = ArticleSchema(
            id=url_hash,
            title=title,
            url=url,
            source_country=country_code,
            content=extracted_text,
            published_at=published_at or "",
            word_count=len(extracted_text.split()),
            is_breaking_news=("breaking" in title.lower()),
            full_content=extracted_text,
            summary_snippet=(extracted_text[:500] if extracted_text else ""),
            article_id=url_hash,
            extracted_text=extracted_text,
        )
    except ValidationError as ve:
        logger.error(f"Validation failed for article {url}: {ve}")
        return

    # 4. Publish to Kafka Topic 'news.raw'
    await kafka_producer.send_article(topic="news.raw", article=article)
    logger.info(f"Published article '{title[:30]}...' [{country_code}] to Kafka 'news.raw'")

async def poll_feeds():
    """Main loop to continuously fetch feeds and push to pipeline."""
    redis_client = RedisClient()
    kafka_producer = KafkaProducerWrapper()
    try:
        await kafka_producer.start()

        async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as http_client:
            while True:
                logger.info("Starting feed polling cycle...")
                for country, feeds in Config.RSS_FEEDS.items():
                    for feed_url in feeds:
                        try:
                            parsed_feed = feedparser.parse(feed_url)
                            for entry in parsed_feed.entries:
                                await process_feed_entry(entry, country, http_client, redis_client, kafka_producer)
                        except Exception as e:
                            logger.error(f"Error reading feed {feed_url}: {e}")

                logger.info(f"Cycle complete. Waiting {Config.SCRAPER_INTERVAL_SECONDS} seconds...")
                await asyncio.sleep(Config.SCRAPER_INTERVAL_SECONDS)
    finally:
        await kafka_producer.stop()
        await redis_client.close()

if __name__ == "__main__":
    try:
        asyncio.run(poll_feeds())
    except KeyboardInterrupt:
        logger.info("Scraper manually stopped.")