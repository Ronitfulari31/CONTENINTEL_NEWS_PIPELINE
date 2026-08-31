"""
Search and Recommendation Engine
Hybrid search combining semantic vector similarity with structured metadata filtering.
Implements recommendation loops for discovering related articles.
"""

from typing import List, Dict, Optional
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, Range
from fastembed import TextEmbedding


class NewsSearchEngine:
    """
    Hybrid search and recommendation engine powered by Qdrant vector database.
    
    Features:
    - Semantic search using dense vector embeddings (384-dim)
    - Metadata filtering by category, country, sentiment, etc.
    - Related article recommendations via vector similarity
    - Full-text query understanding through embeddings
    """
    
    def __init__(self, host: str = "localhost", port: int = 6333, collection_name: str = "news_articles"):
        """
        Initialize the search engine.
        
        Args:
            host: Qdrant server host
            port: Qdrant server port
            collection_name: Name of the indexed collection
        """
        self.client = QdrantClient(host=host, port=port)
        self.collection_name = collection_name
        self.embedding_model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
        print(f"✓ Initialized NewsSearchEngine")
        print(f"✓ Using collection: {collection_name}")

    def hybrid_search(
        self,
        query_text: str,
        category_filter: Optional[str] = None,
        country_filter: Optional[str] = None,
        sentiment_filter: Optional[str] = None,
        limit: int = 5
    ) -> List[Dict]:
        """
        Hybrid semantic + metadata search.
        
        Combines vector similarity (semantic meaning) with structured metadata filtering
        (category, country, sentiment label).
        
        Args:
            query_text: Natural language query (e.g., "artificial intelligence breakthrough")
            category_filter: Optional category filter (e.g., "Technology")
            country_filter: Optional country code filter (e.g., "GB")
            sentiment_filter: Optional sentiment label filter (e.g., "Positive")
            limit: Maximum number of results
            
        Returns:
            List of matching articles with metadata and similarity scores
            
        Example:
            engine = NewsSearchEngine()
            results = engine.hybrid_search(
                "AI breakthrough",
                category_filter="Technology",
                limit=3
            )
        """
        # Generate query embedding
        query_vectors = list(self.embedding_model.embed([query_text]))
        if not query_vectors:
            print("⚠ Failed to generate query embedding")
            return []
        
        query_vector = query_vectors[0].tolist()
        
        # Build filter conditions for metadata
        filter_conditions = []
        
        if category_filter:
            filter_conditions.append(
                FieldCondition(
                    key="category",
                    match=MatchValue(value=category_filter)
                )
            )
        
        if country_filter:
            filter_conditions.append(
                FieldCondition(
                    key="source_country",
                    match=MatchValue(value=country_filter)
                )
            )
        
        if sentiment_filter:
            filter_conditions.append(
                FieldCondition(
                    key="sentiment_label",
                    match=MatchValue(value=sentiment_filter)
                )
            )
        
        # Combine all filter conditions with AND logic
        query_filter = None
        if filter_conditions:
            query_filter = Filter(must=filter_conditions)
        
        try:
            # Qdrant v1 API uses query_points() instead of search()
            search_results = self.client.query_points(
                collection_name=self.collection_name,
                query=query_vector,
                query_filter=query_filter,
                limit=limit,
                with_payload=True,
                with_vectors=False,
            ).points

            # Format results
            results = []
            for hit in search_results:
                result = {
                    "similarity_score": hit.score,
                    **hit.payload
                }
                results.append(result)

            return results

        except Exception as e:
            print(f"Error during hybrid search: {e}")
            return []

    def get_related_recommendations(
        self,
        article_id: str,
        limit: int = 3,
        category_bias: Optional[str] = None
    ) -> List[Dict]:
        """
        Find similar articles using vector similarity (recommendation loop).
        
        Given an article ID, finds the nearest vector neighbors in the embedding space,
        enabling "Related Articles" discovery.
        
        Args:
            article_id: ID of the source article
            limit: Number of recommendations to return
            category_bias: Optional category to bias recommendations
            
        Returns:
            List of related articles with similarity scores
            
        Example:
            engine = NewsSearchEngine()
            recs = engine.get_related_recommendations(
                article_id="art_12345",
                limit=5
            )
        """
        try:
            # Retrieve the source article's vector
            records, _ = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=Filter(
                    must=[FieldCondition(
                        key="article_id",
                        match=MatchValue(value=article_id)
                    )]
                ),
                with_vectors=True,
                limit=1
            )
            
            if not records:
                print(f"⚠ Article {article_id} not found in collection")
                return []
            
            target_vector = records[0].vector
            target_payload = records[0].payload
            
            # Build filter to bias towards same category (optional)
            rec_filter = None
            if category_bias:
                rec_filter = Filter(
                    must=[FieldCondition(
                        key="category",
                        match=MatchValue(value=category_bias)
                    )]
                )

            similar_results = self.client.query_points(
                collection_name=self.collection_name,
                query=target_vector,
                query_filter=rec_filter,
                limit=limit + 1,
                with_payload=True,
                with_vectors=False,
            ).points

            recommendations = []
            for hit in similar_results:
                if hit.payload.get("article_id") != article_id:
                    result = {
                        "similarity_score": hit.score,
                        **hit.payload
                    }
                    recommendations.append(result)

            if not recommendations and category_bias:
                fallback_results = self.client.query_points(
                    collection_name=self.collection_name,
                    query=target_vector,
                    limit=limit + 1,
                    with_payload=True,
                    with_vectors=False,
                ).points
                for hit in fallback_results:
                    if hit.payload.get("article_id") != article_id:
                        recommendations.append({
                            "similarity_score": hit.score,
                            **hit.payload
                        })

            return recommendations[:limit]
            
        except Exception as e:
            print(f"Error fetching recommendations: {e}")
            return []

    def advanced_search(
        self,
        query_text: str,
        filters: Optional[Dict] = None,
        limit: int = 10
    ) -> List[Dict]:
        """
        Advanced search with complex filter combinations.
        
        Args:
            query_text: Search query
            filters: Dictionary of filter conditions:
                {
                    "category": "Technology",
                    "countries": ["GB", "US"],
                    "sentiment": "Positive",
                    "min_score": 0.7
                }
            limit: Maximum results
            
        Returns:
            List of matching articles
        """
        if filters is None:
            filters = {}
        
        # Use hybrid search with filters
        return self.hybrid_search(
            query_text=query_text,
            category_filter=filters.get("category"),
            country_filter=filters.get("country"),
            sentiment_filter=filters.get("sentiment"),
            limit=limit
        )

    def get_trending_topics(self, limit: int = 10) -> List[Dict]:
        """
        Get most common topics/entities from indexed articles.
        
        Returns top entities by frequency.
        """
        try:
            # Retrieve all articles
            records, _ = self.client.scroll(
                collection_name=self.collection_name,
                with_payload=True,
                limit=limit * 5
            )
            
            entity_counts = {}
            for record in records:
                entities = record.payload.get("entities", [])
                for entity in entities:
                    entity_counts[entity] = entity_counts.get(entity, 0) + 1
            
            # Sort by frequency
            trending = sorted(entity_counts.items(), key=lambda x: x[1], reverse=True)
            return [{"entity": name, "count": count} for name, count in trending[:limit]]
            
        except Exception as e:
            print(f"Error fetching trending topics: {e}")
            return []

    def search_by_metadata(
        self,
        category: Optional[str] = None,
        country: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict]:
        """
        Pure metadata-based search (no semantic similarity).
        
        Useful for categorical browsing without semantic understanding.
        """
        filter_conditions = []
        
        if category:
            filter_conditions.append(
                FieldCondition(key="category", match=MatchValue(value=category))
            )
        
        if country:
            filter_conditions.append(
                FieldCondition(key="source_country", match=MatchValue(value=country))
            )
        
        query_filter = None
        if filter_conditions:
            query_filter = Filter(must=filter_conditions)
        
        try:
            # Use a neutral query vector to get all matching records
            results, _ = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=query_filter,
                with_payload=True,
                limit=limit
            )
            
            return [{"payload": r.payload} for r in results]
            
        except Exception as e:
            print(f"Error in metadata search: {e}")
            return []


if __name__ == "__main__":
    """
    Example usage and testing:
    
    python -c "
    from nlp_news.search_recommendations import NewsSearchEngine
    
    engine = NewsSearchEngine()
    
    # Hybrid semantic search
    results = engine.hybrid_search(
        'artificial intelligence breakthrough',
        category_filter='Technology',
        limit=3
    )
    
    # Recommendations
    recs = engine.get_related_recommendations(
        article_id='art_0',
        limit=5
    )
    
    # Trending
    trending = engine.get_trending_topics(limit=5)
    "
    """
    engine = NewsSearchEngine()
    
    # Test hybrid search
    print("\n🔍 Testing Hybrid Search:")
    print("-" * 60)
    results = engine.hybrid_search("technology innovation", limit=3)
    for i, result in enumerate(results, 1):
        print(f"{i}. {result.get('title', 'N/A')} (Score: {result.get('similarity_score', 0):.3f})")
    
    # Test recommendations
    if results:
        print("\n🎯 Testing Recommendations:")
        print("-" * 60)
        article_id = results[0].get("article_id", "art_0")
        recs = engine.get_related_recommendations(article_id, limit=3)
        for i, rec in enumerate(recs, 1):
            print(f"{i}. {rec.get('title', 'N/A')} (Score: {rec.get('similarity_score', 0):.3f})")
    
    # Test trending
    print("\n📈 Testing Trending Topics:")
    print("-" * 60)
    trending = engine.get_trending_topics(limit=5)
    for topic in trending:
        print(f"- {topic.get('entity', 'N/A')}: {topic.get('count', 0)} occurrences")
