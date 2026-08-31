"""
Standalone NLP Enrichment Script
Processes articles from Gold Delta layer and writes enriched results without Spark serialization issues.
"""

import json
from datetime import datetime
from pathlib import Path

from config import Config
from nlp_news.pipeline import run_nlp_pipeline


def enrich_articles_locally():
    """Read gold articles, apply NLP enrichment, save results as JSONL locally."""
    try:
        from pyspark.sql import SparkSession
        from utils.utils import setup_hadoop_env, get_free_port
    except ImportError as e:
        print(f"Missing Spark dependencies: {e}")
        return

    print("═" * 70)
    print("NLP ENRICHMENT - Standalone Processor")
    print("═" * 70)

    setup_hadoop_env()

    spark = (
        SparkSession.builder
        .appName("NLP_Enrichment_Standalone")
        .config("spark.ui.port", str(get_free_port()))
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(f"spark.hadoop.fs.azure.account.auth.type.{Config.AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net", "SharedKey")
        .config(f"spark.hadoop.fs.azure.account.key.{Config.AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net", Config.AZURE_STORAGE_KEY)
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.AzureLogStore")
        .master("local[1]")
        .getOrCreate()
    )

    try:
        gold_path = f"{Config.ADLS_GOLD_PATH}/nlp_input_articles"
        print(f"\n📖 Reading articles from: {gold_path}")
        df = spark.read.format("delta").load(gold_path)
        articles = df.collect()
        total = len(articles)

        print(f"✓ Loaded {total} articles for processing\n")
        if total == 0:
            print("No articles to process.")
            return

        enriched_data = []
        processing_ts = datetime.utcnow().isoformat()

        print("Processing articles with NLP pipeline...")
        print("-" * 70)

        for idx, row in enumerate(articles, 1):
            try:
                if idx % max(1, total // 5) == 0 or idx == total:
                    print(f"  [{idx:3d}/{total}] Processing article ID: {row.article_id}")

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
                        "language_detection": task_results.get("language_detection", {
                            "detected_language": nlp_output.get("detected_language", nlp_output.get("original_language", "en"))
                        }),
                        "translation": task_results.get("translation", {
                            "source_language": nlp_output.get("detected_language", nlp_output.get("original_language", "en")),
                            "translated_text": nlp_output.get("translated_text", nlp_output.get("processed_text", row.full_text_payload)),
                            "applied": nlp_output.get("translation_applied", False),
                        }),
                        "ner": task_results.get("ner", {"entities": nlp_output.get("ner_entities", [])}),
                        "location_extraction": task_results.get("location_extraction", {"locations": nlp_output.get("extracted_locations", [])}),
                        "category_classification": task_results.get("category_classification", {
                            "category": nlp_output.get("category", "General"),
                            "category_scores": nlp_output.get("category_scores", {})
                        }),
                        "keyword_extraction": task_results.get("keyword_extraction", {"keywords": nlp_output.get("keywords", [])}),
                        "summary": task_results.get("summary", {"summary_text": nlp_output.get("summary_text", nlp_output.get("summary", ""))}),
                        "sentiment": task_results.get("sentiment", nlp_output.get("sentiment", {})),
                    },
                    "processed_at": processing_ts,
                }
                enriched_data.append(enriched_record)
            except Exception as e:
                print(f"  ⚠ Error processing article {row.article_id}: {str(e)[:80]}")
                continue

        print("-" * 70)
        success_count = len(enriched_data)
        print(f"\n✓ Successfully enriched {success_count}/{total} articles")

        if success_count == 0:
            print("No articles were successfully enriched.")
            return

        output_dir = Path("data/nlp_enriched")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"enriched_articles_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"

        with open(output_file, 'w', encoding='utf-8') as f:
            for record in enriched_data:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')

        print(f"\n✓ Enriched articles saved to: {output_file}")
        print(f"  Size: {output_file.stat().st_size:,} bytes")

        if enriched_data:
            print(f"\n📊 Sample Enrichment Result (Article 1):")
            print(json.dumps(enriched_data[0], indent=2, ensure_ascii=False))

        print(f"\n✅ NLP Enrichment complete!")
        print("=" * 70)

    except FileNotFoundError as fnf:
        print(f"❌ Input path not found: {fnf}")
        print("Please ensure gold_input_articles table exists by running spark_gold_processor.py first.")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    enrich_articles_locally()
