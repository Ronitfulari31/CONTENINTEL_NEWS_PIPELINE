import json
from datetime import datetime
from pathlib import Path


def enrich_articles_from_gold(gold_articles):
    """Apply local NLP enrichment and return JSON-ready records."""
    from nlp_news.pipeline import run_nlp_pipeline

    processing_ts = datetime.utcnow().isoformat()
    enriched = []

    for row in gold_articles:
        nlp_output = run_nlp_pipeline(row.full_text_payload)
        task_results = nlp_output.get("tasks", nlp_output)

        enriched_record = {
            "article_id": row.article_id,
            "title": row.clean_title,
            "url_domain": row.domain,
            "source_country": row.source_country,
            "published_date": str(row.published_date),
            "full_text_payload": row.full_text_payload,
            "processed_text": nlp_output.get("processed_text", row.full_text_payload),
            "nlp": {
                "preprocessing": task_results.get("preprocessing", {}),
                "language_detection": task_results.get("language_detection", {"detected_language": nlp_output.get("detected_language", "en")}),
                "translation": task_results.get("translation", {
                    "source_language": nlp_output.get("detected_language", "en"),
                    "translated_text": nlp_output.get("translated_text", nlp_output.get("processed_text", row.full_text_payload)),
                    "applied": nlp_output.get("translation_applied", False),
                }),
                "ner": task_results.get("ner", {"entities": nlp_output.get("ner_entities", [])}),
                "location_extraction": task_results.get("location_extraction", {"locations": nlp_output.get("extracted_locations", [])}),
                "category_classification": task_results.get("category_classification", {"category": nlp_output.get("category", "General")}),
                "keyword_extraction": task_results.get("keyword_extraction", {"keywords": nlp_output.get("keywords", [])}),
                "summary": task_results.get("summary", {"summary_text": nlp_output.get("summary_text", "")}),
                "sentiment": task_results.get("sentiment", nlp_output.get("sentiment", {})),
            },
            "processed_at": processing_ts,
        }
        enriched.append(enriched_record)

    return enriched


def save_jsonl(records, out_dir="data/nlp_enriched"):
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    file_name = f"enriched_articles_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
    full_path = out_path / file_name
    with open(full_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return full_path
