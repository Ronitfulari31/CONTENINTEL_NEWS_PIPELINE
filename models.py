from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field, HttpUrl

class NewsArticle(BaseModel):
    """
    Data model representing a scraped news article payload passed from
    the scraper to Kafka and consumed by PySpark.
    """
    article_id: str = Field(..., description="MD5 hash or unique ID of the article URL")
    title: str = Field(..., description="Article headline")
    url: str = Field(..., description="Canonical source URL")
    source_country: str = Field(default="GLOBAL", description="Country code (e.g., US, UK, IN)")
    published_at: Optional[str] = Field(default=None, description="Original publication date/time string")
    extracted_text: str = Field(..., description="Cleaned article body text")
    ingest_timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="UTC timestamp when ingested by the scraper"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "article_id": "e4d909c290d0fb1ca068ffaddf22cbd0",
                "title": "Global Tech Summit Announced",
                "url": "https://example.com/tech-summit",
                "source_country": "US",
                "published_at": "2026-08-27T10:00:00Z",
                "extracted_text": "The annual tech summit will feature keynotes...",
                "ingest_timestamp": "2026-08-27T16:12:42Z"
            }
        }