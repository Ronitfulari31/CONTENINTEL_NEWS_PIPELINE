# Recommendation System Theory in This Project

## 1. Purpose of the recommendation system

The recommendation system in this project is designed to help users discover related news stories based on meaning, not just exact words. Instead of suggesting articles only by matching keywords, it finds articles that are semantically close to the current article or the user query.

This is useful in a news platform because:

- a reader may want more coverage on the same event
- a topic may be described with different wording across outlets
- similar stories may be spread across different publishers or countries
- the user may want a “read next” experience while browsing the portal

In this project, recommendations are generated from the vector database Qdrant using semantic similarity.

---

## 2. Core idea: recommendation as nearest-neighbor search

Each news article is converted into a vector embedding using a sentence-transformer model such as BAAI/bge-small-en-v1.5.

After embedding:

- every article becomes a point in a high-dimensional semantic space
- similar articles are located close to each other
- distant articles are less related

When the system needs recommendations for an article, it:

1. fetches the article’s vector from Qdrant
2. asks Qdrant for the nearest vectors around it
3. removes the original article itself
4. returns the closest matches as recommendations

This is a classic nearest-neighbor recommendation strategy.

---

## 3. Why vector similarity is used

Traditional recommendation based on keyword overlap is weak because different articles can discuss the same theme using different words.

Example:

- “AI breakthrough in chip design”
- “New machine learning hardware innovation”
- “GPU acceleration changes semiconductor industry”

These may all be semantically related, even if they do not share the same exact terms.

Vector embeddings capture semantic relationships, so the system can group these articles together even when the language differs.

---

## 4. Similarity metric

This project uses cosine similarity in Qdrant.

Cosine similarity measures the angle between two vectors:

- vectors pointing in the same direction are more similar
- vectors with larger angular separation are less similar

In practice, Qdrant returns a score that indicates how close the vectors are; this score is exposed in the application as similarity_score.

The project stores vectors with:

- dimension: 384
- distance metric: COSINE

This is configured in the Qdrant indexer.

---

## 5. The recommendation method used here

The engine method is called get_related_recommendations in the recommendation module.

Its logic is:

1. find the source article by article_id
2. read its stored vector
3. query Qdrant for similar vectors
4. optionally apply a same-category filter
5. remove self-match
6. return top results

Pseudo-flow:

```python
records = client.scroll(... article_id ... with_vectors=True)
target_vector = records[0].vector
similar_results = client.query_points(
    collection_name="news_articles",
    query=target_vector,
    query_filter=category_filter,
    limit=limit + 1,
)
```

This means the recommendation engine is content-based and semantic, not collaborative filtering.

---

## 6. Content-based recommendation theory

This system is a content-based recommender.

In content-based systems, the model recommends items that are similar to the item the user is currently viewing.

The features are derived from:

- article title
- article content
- metadata such as category, country, sentiment
- extracted keywords and entities

The article is represented as a vector, and similarity is computed against other articles.

This is especially useful for news because the content changes constantly and users often want to continue reading on the same topic.

---

## 7. Why category bias is added

The project supports a category_bias parameter.

This means if the current article is classified as Technology, the recommendation engine may prefer other Technology articles before suggesting unrelated content.

This helps keep recommendations more coherent.

Example:

- If an article is categorized as Business, recommendations should usually prefer business-related stories.
- If no same-category matches exist, it falls back to broader semantic similarity.

This is a practical way to tune recommendation quality in a real-world news portal.

---

## 8. Recommendation quality considerations

A good recommendation engine should balance:

- relevance: recommended articles should be closely related to the current article
- diversity: not all recommended articles should be identical or repetitive
- freshness: newer articles should also be considered
- category consistency: stories should preserve topical coherence

This project does a simple but powerful version of this by combining:

- semantic vector similarity
- category filtering
- score-based ranking

---

## 9. Role of metadata in recommendation

Even though the core recommender is vector-based, metadata still matters.

Each indexed article includes fields such as:

- category
- source_country
- sentiment_label
- language
- keywords
- entities

Metadata allows filtering and improves explainability. For example, a UI can show:

- category
- sentiment
- source country
- vector match score

This helps the user understand why a story was recommended.

---

## 10. Difference between recommendation and search

A simple way to understand the difference:

- Search answers: “What articles match my query?”
- Recommendation answers: “What else is relevant to this article I am reading?”

In this project:

- search is query-based and user-driven
- recommendation is item-based and context-driven

Both are powered by the same vector database and the same semantic embedding philosophy.

---

## 11. Recommendation in the actual app

In the Streamlit application, after a user opens a news article, the app calls the recommendation engine and displays a small set of related stories.

The recommendation panel shows:

- article title
- category
- sentiment
- source country
- similarity score

This creates a discovery loop of reading deeper into the same topic area.

---

## 12. Summary

The recommendation system in this project is based on semantic vector similarity over news content.

It works because:

- each article is converted into a meaningful vector
- similar articles are placed close to each other in vector space
- nearest-neighbor search finds related content
- metadata adds context and filtering
- category bias improves topical consistency

This makes the recommendation engine suitable for a news portal where meaning and context matter more than raw keyword overlap.
