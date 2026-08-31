📰 ContentIntel News Pipeline - AI-Powered News Enrichment

A comprehensive end-to-end data engineering pipeline that ingests RSS feeds, processes them through a multi-layer data lakehouse (Bronze → Silver → Gold), applies advanced NLP enrichment (9 tasks), and visualizes insights through an interactive Streamlit dashboard.

🎯 Project Overview

ContentIntel is a production-ready news intelligence platform that:

✅ Ingests news from multiple RSS feeds in real-time

✅ Cleans and normalizes raw data through layered processing

✅ Enriches articles with 9 AI/NLP tasks (language detection, NER, sentiment, etc.)

✅ Visualizes comprehensive analysis through an interactive dashboard

Technology Stack: Apache Spark, Delta Lake, spaCy, FastText, VADER, LexRank, Azure ADLS Gen2, Docker, Streamlit

🏗️ Architecture Overview

End-to-End Data Flow


RSS Feeds

   ↓

┌─────────────────────────────────────────────────────────────┐

│ BRONZE LAYER (Raw Ingestion)                                │

│ • fetch_rss.py - Parse RSS feeds                            │

│ • kafka_producer.py - Stream to Kafka                       │

│ • redis_client.py - Cache management                        │

└─────────────────────────────────────────────────────────────┘

   ↓

[ADLS Gen2: abfss://bronze/bronze/news_articles/]

   ↓

┌─────────────────────────────────────────────────────────────┐

│ SILVER LAYER (Clean & Normalize)                            │

│ • spark_silver_processor.py                                 │

│ • HTML tag removal, type casting, null handling             │

│ • Quality filtering, text normalization                     │

└─────────────────────────────────────────────────────────────┘

   ↓

[ADLS Gen2: abfss://bronze/silver/news_articles/]

   ↓

┌─────────────────────────────────────────────────────────────┐

│ GOLD LAYER (Business Ready)                                 │

│ • spark_gold_processor.py                                   │

│ • Domain extraction, read time calculation                  │

│ • Partitioning by source_country + published_date           │

└─────────────────────────────────────────────────────────────┘

   ↓

[ADLS Gen2: abfss://bronze/gold/news_articles/]

   ↓

┌─────────────────────────────────────────────────────────────┐

│ NLP ENRICHMENT LAYER (9 Sequential Tasks)                   │

│ • nlp_enrichment_standalone.py                              │

│ • Local CPU processing (avoids Spark serialization issues)  │

│ • Outputs JSONL locally before persisting                   │

└─────────────────────────────────────────────────────────────┘

   ↓

[Local Cache: data/nlp_enriched/enriched_articles_*.jsonl]

   ↓

┌─────────────────────────────────────────────────────────────┐

│ NLP GOLD LAYER (Enriched & Queryable)                       │

│ • save_nlp_to_gold.py                                       │

│ • Flatten nested NLP structure for queries                  │

│ • Partition by source_country + published_date              │

└─────────────────────────────────────────────────────────────┘

   ↓

[ADLS Gen2: abfss://bronze/gold/nlp_enriched_articles/]

   ↓

┌─────────────────────────────────────────────────────────────┐

│ STREAMLIT DASHBOARD (Interactive Visualization)             │

│ • Page 1: News Portal with search, filters, grid            │

│ • Page 2: Deep Analysis with NLP results                    │

└─────────────────────────────────────────────────────────────┘


High-Level Architecture Diagrams

RSS → Bronze Pipeline:



Bronze → Silver Transformation:



Silver → Gold Enrichment:



Gold → NLP Processing:



Gold → Streamlit Dashboard:



Complete Pipeline:



🧠 9-Task NLP Enrichment Pipeline

The core innovation of ContentIntel is a comprehensive 9-task NLP pipeline that processes each article sequentially:

Task 1: Text Preprocessing 🧹

Removes HTML tags and special characters

Normalizes whitespace

Applies ftfy for text encoding fixes

Uses regex for comprehensive text cleanup

Input: Raw article text  

Output: Clean, normalized text

Task 2: Language Detection 🌍

Uses FastText (fast_langdetect) model

Detects language from 160+ languages

Essential for translation routing

Example: "Detected language: EN"

Task 3: Translation 🔄

Uses ArgosTranslate for offline translation

Automatically translates non-English articles to English

Enables unified NLP processing across multiple languages

Example: If German → Translates to English

Task 4: Named Entity Recognition (NER) 👤

Powered by spaCy en_core_web_sm model

Identifies and classifies entities:

  - PERSON: People names

  - ORG: Organizations

  - GPE: Geopolitical entities

  - DATE: Time references

  - CARDINAL: Numerical expressions

  - MONEY: Financial amounts

  - etc.

Entities Detected: 15+ entity types  

Framework: spaCy v3.0+ (production-grade NLP)

Task 5: Location Extraction & Verification 📍

Extracts GPE (Geopolitical Entity) tags from NER

Verifies against geonamescache (11.5M+ locations)

Returns verified location names with country codes

Example Output:


- UK (country: GB)

- London (country: GB)

- Nottingham (country: GB)


Task 6: Category Classification 🏷️

Rule-based classification using keywords

Categories: Technology, Business, Politics, Sports, Health, Science, Entertainment, etc.

Biased scoring towards category-relevant keywords

Example: "Predicted category: Technology"

Task 7: Keyword Extraction 🔑

TF-based keyword ranking with category bias

Filters out stopwords

Returns top keywords by relevance

Example Keywords:

people

data

education

employment

neets (youth without work/education)

Task 8: Extractive Summarization 📝

Uses LexRank algorithm (graph-based, no ML training needed)

Extracts 2-3 key sentences

Provides condensed article overview without hallucination

Input: Full article content  

Output: 2-3 key sentences preserving original text

Task 9: Sentiment Analysis 💭

VADER (Valence Aware Dictionary and sEntiment Reasoner)

Scores sentiment as float between -1 (negative) and +1 (positive)

Includes compound score for overall sentiment

Sentiment Gauge Example: 0.995 (Very Positive)  

Categories: Negative | Neutral | Positive

📊 Interactive Streamlit Dashboard

Page 1: News Portal

Provides the interactive news discovery experience:

Semantic search powered by Qdrant

Article cards with category, source, country, and sentiment

Search and filtering of enriched news

Full cleaned article content (collapsible)

Related-article recommendation / discovery loop

Page 2: Comprehensive 9-Task Analysis

Visual output of all 9 NLP tasks:


2. Comprehensive 9-Task NLP Analysis Report

Task 1: Preprocessing

├─ Cleaned text preview: "One in eight young people still out of work..."

Task 2: Language Detection

├─ Detected language: EN

Task 3: Translation

├─ No translation was required, the article was already in English.

Task 4: Named Entity Recognition (NER)

├─ PERSON (CARDINAL), ORG (CARDINAL), DATE, MONEY, etc.

├─ [20+ entities extracted]

Task 5: Location Extraction

├─ UK, London, Nottingham, Willshire, News, Bristol, Cornwall, Leeds, King, Leeds

Task 6: Category Classification

├─ Predicted category: Technology

Task 7: Keyword Extraction

├─ ai, data, rss, translation

Task 8: Extractive Summary

├─ "The new data suggests the number of young people who are Neet has fallen..."

├─ "The report warned that one in six young people would be out of work..."

Task 9: Sentiment Score & Label

├─ Sentiment Score: 0.995 (gauge visualization)

├─ Label: Positive


Dashboard Screenshots

The Streamlit application provides the final user-facing layer of the pipeline. The screenshots below are from the running project and demonstrate the major outputs delivered by the platform.

Full Article Text + NLP Preview

The article analysis view presents the selected news story, metadata, and the beginning of the comprehensive NLP analysis. Users can expand the cleaned article content before reviewing the individual NLP tasks.

![Full Article Text + NLP Preview](DOCS/streamlit_ui/01_article_full_text_and_nlp_preview.png)



Comprehensive 9-Task NLP Analysis


![9-Task NLP Analysis](DOCS/streamlit_ui/02_nlp_9_task_analysis.png)


This view shows the actual NLP output generated for an article, including:

Text preprocessing

Language detection

Translation

Named Entity Recognition (NER)

Location extraction

Category classification

Keyword extraction

Extractive summarization

Sentiment analysis

The interface also visualizes the sentiment score and exposes extracted entities, locations, keywords, category, and summary.



Semantic Search Results

The News Portal provides semantic search through Qdrant. A natural-language query is entered in the search interface, and the application returns the most relevant enriched news articles.


![Semantic Search Results](DOCS/streamlit_ui/03_semantic_search_results.png)


The result view demonstrates:

Natural-language search

Qdrant semantic retrieval

Article filtering

Similarity-based relevance

Category and sentiment information

Article cards with deep-analysis navigation



Recommendation / Related Articles

After an article is analyzed, the application provides a Discovery Loop: Recommended Similar Articles section. These results are generated using Qdrant vector similarity with category-aware discovery.


![Recommendation Results](DOCS/streamlit_ui/04_recommendation_results.png)


The recommendation cards expose:

Related article title

Vector match / similarity score

Sentiment

Source country

Navigation to analyze the recommended article



Streamlit UI flow:
News Portal → Search → Article Selection → Full Text → 9-Task NLP Analysis → Related Article Recommendations

These screenshots showcase the actual end-user experience of the search, NLP enrichment, article analysis, and recommendation components.

� Vector Search & Recommendation Engine

The discovery loop leverages Qdrant vector database for semantic search and content recommendation, enabling users to find semantically similar articles beyond keyword matching.

Architecture: Gold → Vector Indexing → Search Engine


Gold Layer (Enriched Articles)

         ↓

    [JSONL Staging]

         ↓

┌─────────────────────────────────┐

│  Vector Indexing (indexer.py)   │

│  • TextEmbedding (FastEmbed)     │

│  • 384-dim dense vectors         │

│  • Metadata extraction (NLP)     │

└─────────┬───────────────────────┘

          ↓

┌─────────────────────────────────┐

│   Qdrant Vector Database        │

│  • Vector collection            │

│  • Payload with metadata        │

│  • COSINE distance metric       │

└─────────┬───────────────────────┘

          ↓

┌─────────────────────────────────┐

│ Search Engine                   │

│ (search_recommendations.py)     │

│  • Hybrid search (semantic +    │

│    metadata filters)            │

│  • Related recommendations      │

│  • Trending analysis            │

└─────────┬───────────────────────┘

          ↓

    Streamlit Dashboard


Features

Hybrid Semantic + Metadata Search

Semantic vector similarity (384-dim embeddings)

Structured metadata filtering (category, country, sentiment)

Combined query execution for precision results

Recommendation Loop

Given an article, find semantically similar articles

Optional category bias for related recommendations

Vector neighborhood discovery in embedding space

Advanced Query Patterns

Category + Sentiment filtering

Geographic (country) filtering

Complex filter combinations

Trending topic extraction

Usage Examples

1. Hybrid Semantic Search:


from nlp_news.search_recommendations import NewsSearchEngine

engine = NewsSearchEngine()

results = engine.hybrid_search(

    query_text="artificial intelligence breakthrough",

    category_filter="Technology",

    limit=5

)


2. Get Related Articles:


recommendations = engine.get_related_recommendations(

    article_id="art_12345",

    limit=3

)


3. Advanced Filtering:


results = engine.advanced_search(

    query_text="economic impact",

    filters={

        "category": "Business",

        "country": "GB",

        "sentiment": "Negative"

    }

)


4. Trending Topics:


trending = engine.get_trending_topics(limit=10)


�📁 Project Directory Structure


ContentIntel News Pipeline/

│

├── bronze/

│   └── ingestion/              ← RSS collection & Kafka streaming

│       ├── fetch_rss.py

│       ├── kafka_producer.py

│       ├── redis_client.py

│       └── rss_scrapper.py

│

├── silver/

│   └── processors/             ← Data cleaning layer

│       └── spark_silver_processor.py

│

├── gold/

│   └── processors/             ← Data enrichment layer

│       ├── spark_gold_processor.py

│       ├── read_delta_lake.py

│       └── utils.py

│

├── nlp_news/                   ← NLP enrichment & vector search

│   ├── pipeline.py             (9-task orchestrator)

│   ├── nlp_enrichment_standalone.py

│   ├── save_nlp_to_gold.py

│   ├── enrichment.py

│   ├── indexer.py              (Qdrant vector indexing)

│   └── search_recommendations.py (Hybrid search & recommendations)

│

├── utils/

│   └── utils.py                ← Shared Spark utilities

│

├── data/

│   ├── checkpoints/            ← Kafka checkpoints

│   ├── delta/                  ← Local Delta cache

│   └── nlp_enriched/           ← JSONL output

│

├── DOCS/                       ← Documentation & diagrams

│

├── app.py                      ← Streamlit dashboard

├── config.py                   ← Central configuration

├── models.py                   ← Data schemas

├── requirements.txt            ← Dependencies

└── docker-compose.yml          ← Docker services


🚀 Quick Start

Prerequisites

Python 3.9+

Java 8+

Docker & Docker Compose

Azure Storage Account (ADLS Gen2) or local Spark setup

Installation

Clone the repository


git clone <repo-url>

cd CONTENINTEL_NEWS_PIPELINE


Create virtual environment


python -m venv .venv

.venv\Scripts\activate  # Windows

source .venv/bin/activate  # Linux/Mac


Install dependencies


pip install -r requirements.txt


Configure environment


# Update .env file with:

AZURE_STORAGE_ACCOUNT=<your-account>

AZURE_STORAGE_KEY=<your-key>


Start Docker services (Kafka, Redis)


docker-compose up -d


📋 Pipeline Execution

1. Ingest RSS Feeds (Bronze Layer)


python -c "

from bronze.ingestion.fetch_rss import fetch_all_feeds

import asyncio

feeds = asyncio.run(fetch_all_feeds())

"


2. Process Bronze → Silver


python -c "

from silver.processors.spark_silver_processor import create_silver_processor

create_silver_processor()

"


3. Enrich Silver → Gold


python -c "

from gold.processors.spark_gold_processor import ensure_gold_container_exists

ensure_gold_container_exists()

"


4. Apply NLP Enrichment


python -c "

from nlp_news.nlp_enrichment_standalone import enrich_articles_locally

enrich_articles_locally()

"


5. Persist NLP Results to Gold


python -c "

from nlp_news.save_nlp_to_gold import main

main()

"


6. Index Enriched Articles to Qdrant


python -c "

from nlp_news.indexer import QdrantIndexer

indexer = QdrantIndexer(host='localhost', port=6333)

indexed_count = indexer.index_latest_jsonl('data/nlp_enriched')

print(f'Indexed {indexed_count} articles')

"


7. Launch Streamlit Dashboard


streamlit run app.py --server.port 8501


Open browser: http://localhost:8501

📊 Data Volumes & Performance

Input: 1,190+ articles from multiple RSS feeds

Processing: ~40-50 MB JSONL intermediate output

NLP Processing Time: Sequential (CPU-optimized)

Output Partitions: By source_country + published_date

🔧 Configuration

Edit config.py to customize:

RSS feed sources

Azure ADLS paths

Kafka bootstrap servers

Redis cache settings

Spark configuration

NLP model selections

🧪 Validation & Testing

All imports validated:


✓ nlp_news.pipeline.run_nlp_pipeline

✓ nlp_news.nlp_enrichment_standalone.enrich_articles_locally

✓ bronze.ingestion.fetch_rss.fetch_all_feeds

✓ silver.processors.spark_silver_processor.create_silver_processor

✓ gold.processors.spark_gold_processor.ensure_gold_container_exists

✓ utils.utils.get_free_port


📚 Documentation

Comprehensive documentation available in /DOCS:

complete_flow.md - End-to-end pipeline walkthrough

DIRECTORY_STRUCTURE.md - Data layer locations

DIRECTORY_REORGANIZATION.md - Architecture details

silver_to_gold.md - Silver layer transformation

gold_to_nlp_to_gold.md - NLP enrichment details

🏛️ Architecture Decisions

Why Local NLP Processing?

Spark UDFs struggle with large Python NLP libraries on Windows

Local CPU batch processing avoids serialization timeouts

JSONL intermediate format enables debugging & validation

Scales to thousands of articles efficiently

Why Delta Lake?

ACID transactions for reliability

Time travel capability for data lineage

Partitioned storage for query performance

Seamless integration with Spark

Why Streamlit?

Rapid dashboard development

No JavaScript/frontend expertise needed

Real-time data refresh

Interactive widgets for filtering & search

📈 Future Enhancements

Real-time Kafka stream processing

Redis caching for frequently accessed articles

Graph-based recommendation engine

Advanced sentiment analysis (transformers)

Multi-language NLP pipelines

API layer for external integrations

Scheduled pipeline orchestration (Airflow)

🤝 Contributing

Fork the repository

Create feature branch (git checkout -b feature/amazing-feature)

Commit changes (git commit -m 'Add amazing feature')

Push to branch (git push origin feature/amazing-feature)

Open Pull Request

📄 License

This project is licensed under the MIT License - see LICENSE file for details.

👥 Contact & Support

For questions, issues, or suggestions:

Create an issue on GitHub

Check existing documentation in /DOCS

Review architecture diagrams for context

🎓 Learning Resources

Technologies Used

Apache Spark: Distributed processing

Delta Lake: Data lake format

spaCy: NLP & NER

VADER: Sentiment analysis

FastText: Language detection

LexRank: Summarization

Azure ADLS Gen2: Cloud storage

Streamlit: Dashboard

Concepts Demonstrated

Data lakehouse architecture (Bronze-Silver-Gold)

ETL/ELT pipeline design

NLP task orchestration

Real-time data ingestion

Cloud data processing

Interactive data visualization
