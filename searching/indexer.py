"""
Qdrant Vector Index Engine
Reads enriched JSONL files from NLP pipeline and indexes them into Qdrant vector database.
Handles both vector embeddings (semantic search) and metadata (analytical filtering).
"""

import json
import glob
import os
from datetime import datetime
from typing import List, Optional

from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct, Filter, FieldCondition, MatchValue
from fastembed import TextEmbedding


class QdrantIndexer:
    """
    Vector database indexer for enriched news articles.
    
    Converts NLP-enriched JSONL articles into vector embeddings and metadata payloads
    stored in Qdrant for semantic search and filtering.
    """
    
    def __init__(self, host: str = "localhost", port: int = 6333, collection_name: str = "news_articles"):
        """
        Initialize the Qdrant indexer.
        
        Args:
            host: Qdrant server host
            port: Qdrant server port
            collection_name: Name of the vector collection
        """
        self.client = QdrantClient(host=host, port=port)
        self.collection_name = collection_name
        self.embedding_model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
        self._ensure_collection()
        print(f"✓ Connected to Qdrant at {host}:{port}")
        print(f"✓ Using collection: {collection_name}")

    def _ensure_collection(self) -> None:
        """
        Create collection if it doesn't exist.
        Vector dimension: 384 (BAAI/bge-small-en-v1.5)
        Distance metric: COSINE (for semantic similarity)
        """
        try:
            collections = [c.name for c in self.client.get_collections().collections]
            if self.collection_name not in collections:
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=384, distance=Distance.COSINE)
                )
                print(f"✓ Created collection: {self.collection_name}")
            else:
                print(f"✓ Collection exists: {self.collection_name}")
        except Exception as e:
            print(f"Error ensuring collection: {e}")
            raise

    def index_latest_jsonl(self, jsonl_dir: str = "data/nlp_enriched") -> int:
        """
        Index the latest enriched JSONL file into Qdrant.
        
        Args:
            jsonl_dir: Directory containing enriched JSONL files
            
        Returns:
            Number of articles indexed
        """
        jsonl_files = sorted(glob.glob(os.path.join(jsonl_dir, "*.jsonl")))
        if not jsonl_files:
            print(f"⚠ No enriched JSONL files found in {jsonl_dir}")
            return 0
        
        latest_file = jsonl_files[-1]
        print(f"\n📄 Indexing: {latest_file}")
        
        points = []
        article_count = 0
        
        try:
            with open(latest_file, "r", encoding="utf-8") as f:
                for idx, line in enumerate(f):
                    if not line.strip():
                        continue
                    
                    article = json.loads(line)
                    
                    # Combine title and body for comprehensive semantic coverage
                    title = article.get("title", "")
                    content = article.get("full_text_payload", "")
                    text_to_embed = f"{title} {content}"[:2000]  # Limit to 2000 chars for embedding
                    
                    try:
                        # Generate embedding using FastEmbed
                        embeddings = list(self.embedding_model.embed([text_to_embed]))
                        if not embeddings:
                            continue
                        embedding = embeddings[0].tolist()
                    except Exception as e:
                        print(f"Warning: Failed to embed article {idx}: {e}")
                        continue
                    
                    # Extract NLP metadata for payload
                    nlp_data = article.get("nlp", {})
                    category_classification = nlp_data.get("category_classification", {})
                    sentiment_data = nlp_data.get("sentiment", {})
                    entities = nlp_data.get("entities", [])
                    locations = nlp_data.get("locations", [])
                    language_detection = nlp_data.get("language_detection", {})
                    keywords = nlp_data.get("keyword_extraction", {}).get("keywords", nlp_data.get("keywords", []))
                    
                    category_value = (
                        nlp_data.get("category")
                        or category_classification.get("category")
                        or article.get("predicted_category")
                        or article.get("category")
                        or ""
                    )
                    
                    # Build payload with analytical metadata
                    payload = {
                        "article_id": article.get("article_id", f"art_{idx}"),
                        "title": title,
                        "domain": article.get("url_domain", ""),
                        "source_country": article.get("source_country", ""),
                        "published_date": article.get("published_at", ""),
                        "category": category_value,
                        "sentiment_score": sentiment_data.get("compound", sentiment_data.get("polarity_score", 0.0)),
                        "sentiment_label": sentiment_data.get("label", ""),
                        "language": language_detection.get("detected_language", language_detection) or nlp_data.get("language", ""),
                        "entities": [e.get("text", "") for e in entities][:10],  # Top 10 entities
                        "locations": [l.get("text", "") for l in locations][:5],  # Top 5 locations
                        "keywords": [str(k) for k in keywords][:10]  # Top 10 keywords
                    }
                    
                    points.append(
                        PointStruct(
                            id=article_count,
                            vector=embedding,
                            payload=payload
                        )
                    )
                    article_count += 1
            
            # Upsert all points to Qdrant
            if points:
                self.client.upsert(
                    collection_name=self.collection_name,
                    points=points
                )
                print(f"\n✅ Successfully indexed {len(points)} articles into Qdrant")
                print(f"   Collection: {self.collection_name}")
                print(f"   Vector dimension: 384 (BAAI/bge-small-en-v1.5)")
                print(f"   Distance metric: COSINE")
                return len(points)
            else:
                print("⚠ No valid articles to index")
                return 0
                
        except Exception as e:
            print(f"Error indexing JSONL file: {e}")
            raise

    def clear_collection(self) -> None:
        """Delete and recreate the collection (use with caution)."""
        try:
            self.client.delete_collection(collection_name=self.collection_name)
            print(f"✓ Deleted collection: {self.collection_name}")
            self._ensure_collection()
            print(f"✓ Recreated collection: {self.collection_name}")
        except Exception as e:
            print(f"Error clearing collection: {e}")
            raise

    def get_collection_stats(self) -> dict:
        """Get statistics about the indexed collection."""
        try:
            collection_info = self.client.get_collection(collection_name=self.collection_name)
            return {
                "collection_name": self.collection_name,
                "vectors_count": collection_info.points_count,
                "vector_size": 384,
                "distance_metric": "COSINE"
            }
        except Exception as e:
            print(f"Error getting collection stats: {e}")
            return {}


if __name__ == "__main__":
    """
    Example usage:
    python -c "
    from searching.indexer import QdrantIndexer
    indexer = QdrantIndexer()
    indexed_count = indexer.index_latest_jsonl()
    stats = indexer.get_collection_stats()
    print(f'Stats: {stats}')
    "
    """
    indexer = QdrantIndexer()
    indexed_count = indexer.index_latest_jsonl()
    stats = indexer.get_collection_stats()
    print(f"\n📊 Collection Stats: {stats}")
