import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

import feedparser
import httpx
import trafilatura
from pydantic import ValidationError

from config import Config
from models import ArticleSchema

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def generate_url_hash(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def prefer_summary_or_entry(entry: Any) -> str:
    summary = getattr(entry, "summary", None) or getattr(entry, "description", None)
    if isinstance(summary, str):
        return summary
    return ""


async def fetch_full_article_text(client: httpx.AsyncClient, url: str) -> str:
    try:
        response = await client.get(url, timeout=15.0, follow_redirects=True)
        response.raise_for_status()
        downloaded = trafilatura.extract(response.text)
        if downloaded and len(downloaded.split()) >= 30:
            return downloaded.strip()
    except Exception as exc:
        logger.warning("Failed to scrape full article %s: %s", url, exc)

    return ""


async def process_feed_entry(entry: Any, country_code: str, http_client: httpx.AsyncClient) -> dict | None:
    url = getattr(entry, "link", None)
    if not url:
        return None

    title = getattr(entry, "title", "No Title")
    published_at = getattr(entry, "published", None) or ""
    summary = prefer_summary_or_entry(entry)

    full_content = await fetch_full_article_text(http_client, url)
    if not full_content:
        fallback_text = summary.strip()
        if len(fallback_text.split()) < 30:
            logger.info("Skipping article with insufficient text: %s", url)
            return None
        full_content = fallback_text

    article_id = generate_url_hash(url)

    try:
        article = ArticleSchema(
            id=article_id,
            title=title,
            url=url,
            source_country=country_code,
            content=full_content,
            published_at=published_at,
            word_count=len(full_content.split()),
            is_breaking_news=("breaking" in title.lower()),
        )
    except ValidationError as exc:
        logger.error("Validation failed for %s: %s", url, exc)
        return None

    record = {
        "id": article.id,
        "title": article.title,
        "summary_snippet": summary[:500] if summary else "",
        "full_content": article.content,
        "url": article.url,
        "published": article.published_at,
        "source_country": article.source_country,
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    return record


async def fetch_all_feeds() -> list[dict]:
    all_records: list[dict] = []
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as http_client:
        for country, feeds in Config.RSS_FEEDS.items():
            for feed_url in feeds:
                try:
                    parsed_feed = feedparser.parse(feed_url)
                    for entry in parsed_feed.entries:
                        record = await process_feed_entry(entry, country, http_client)
                        if record:
                            all_records.append(record)
                except Exception as exc:
                    logger.error("Error processing feed %s: %s", feed_url, exc)
    return all_records


async def main() -> None:
    records = await fetch_all_feeds()
    for rec in records:
        print(json.dumps(rec, ensure_ascii=False))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("RSS fetch manually stopped.")
