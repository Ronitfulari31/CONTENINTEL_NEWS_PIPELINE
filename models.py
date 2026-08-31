from pydantic import BaseModel


class ArticleSchema(BaseModel):
    id: str
    title: str
    url: str
    source_country: str
    content: str
    published_at: str
    word_count: int
    is_breaking_news: bool = False
    summary_snippet: str = ""
    full_content: str = ""
    article_id: str | None = None
    extracted_text: str | None = None
    ingest_timestamp: str | None = None