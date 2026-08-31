# Search System Theory in This Project

## 1. Purpose of the search system

The search system in this project is designed to let users find news articles by meaning, not just by literal keyword matching.

A normal keyword search can easily miss relevant articles when the wording differs. In news, the same topic can appear under many different expressions.

For example, a person may search for:

- “AI breakthroughs”
- “machine learning progress”
- “new innovation in tech”

These terms may refer to similar stories, even if the exact words are different.

This is why the project uses semantic search built on embeddings and a vector database.

---

## 2. Search problem in news data

News articles are highly variable in language style. The same topic can be expressed in many ways:

- different headlines
- different prose styles
- different publishers
- different countries
- different sentiment framing

A keyword-only search would be limited to the exact words used in the article text. That creates poor recall and weak relevance.

Semantic search fixes this by comparing meaning instead of literal text.

---

## 3. Core idea: search by embedding similarity

The system converts both the user query and each article into vector embeddings.

These embeddings are generated using a language model, specifically FastEmbed with BAAI/bge-small-en-v1.5.

Once these vectors are created:

- the query is a vector
- every article is a vector
- Qdrant compares them using similarity distance

Articles whose vectors are closest to the query vector are considered the best matches.

This is the foundation of semantic search.

---

## 4. Why semantic search is better than keyword search

Keyword search asks:

- does this article contain the same words as my query?

Semantic search asks:

- does this article mean the same thing as my query?

This matters because meaning often survives across:

- synonyms
- paraphrases
- different headline styles
- changing terminology

Example:

- query: “climate policy pressure on energy markets”
- relevant article: “government regulation changes fossil fuel investment outlook”

These may not share many exact words, but they are conceptually similar.

---

## 5. Hybrid search: semantic + metadata filters

The project does not rely only on semantic similarity. It also combines metadata filtering.

The hybrid_search method supports filters such as:

- category
- source_country
- sentiment_label

This means the query can answer questions like:

- “Find Technology news about AI breakthroughs”
- “Show positive Business articles from the US”
- “Find negative political stories from the UK”

This is important because real search in news platforms often needs both meaning and structured context.

---

## 6. The role of Qdrant

The project uses Qdrant as the vector database.

Qdrant stores:

- article embeddings
- article metadata payload
- similarity search index

It can perform:

- nearest-neighbor vector search
- metadata filters
- score-based result ranking

This makes it suitable for both semantic search and recommendation.

In this project, the collection is named news_articles and uses cosine distance.

---

## 7. Similarity metric used

The vector search uses cosine similarity.

It measures how close two embedding vectors are in direction.

Mathematically, cosine similarity is:

$$
\text{cosine similarity}(a, b) = \frac{a \cdot b}{\|a\|\|b\|}
$$

A higher score means the two texts are more semantically similar.

Qdrant uses this metric behind the scenes, and the results include a similarity score for each match.

---

## 8. Search flow in the project

The search flow is implemented in the search engine module.

The flow is:

1. User enters a query string
2. The system converts the query into an embedding
3. It builds optional metadata filters
4. It sends both the query vector and filters to Qdrant
5. Qdrant returns matching articles ranked by similarity
6. The app displays the most relevant results

This is why the search is called hybrid: it combines semantic meaning with structured filtering.

---

## 9. Metadata filtering theory

Metadata filters improve precision.

Even if a query is semantically relevant, the user may want a narrow subset of results.

For example:

- category = Technology
- country = US
- sentiment = Positive

Filtering ensures the returned results match both the topic and the user’s context.

This is especially valuable for enterprise dashboards, journalism tools, and news analytics systems.

---

## 10. Query understanding in the app

The UI also adds a small local query interpretation step.

It looks at the user query and tries to infer a likely category such as:

- Technology
- Business
- Politics
- Sports

This is not true AI understanding by itself, but it helps the app refine results using intuitive category clues.

This acts as an extra layer before or alongside semantic search.

---

## 11. Search quality factors

A good semantic search system should optimize for:

- relevance: the top results should match the user intent
- precision: fewer irrelevant results
- recall: relevant items should not be missed
- speed: results should be returned quickly
- explainability: users should know why an article was returned

This project achieves these partly through:

- vector similarity
- metadata filters
- ranking by similarity score
- rich article payloads

---

## 12. Search versus recommendation

The difference is important:

- Search is driven by a user’s question or intent.
- Recommendation is driven by the current article or user interest.

In this project:

- Search helps locate relevant article content from a natural-language query
- Recommendation helps continue reading similar content after an article is chosen

Both use the same vector space and indexing strategy, but their goals are different.

---

## 13. Search in the final user experience

In the Streamlit portal, users can type a question or topic into the search box.

The app then runs the hybrid search and reorders the article list according to Qdrant similarity.

This creates a better browsing experience than plain filtering because the user can describe what they want in natural language.

---

## 14. Summary

The search system in this project is a semantic search engine based on vector embeddings and metadata filtering.

It works because:

- articles are converted into embeddings
- queries are converted into the same embedding space
- Qdrant finds the closest article vectors
- filters improve precision
- ranking by similarity returns the best matches

This makes the platform much more capable than simple keyword search and is essential for a modern intelligent news discovery system.
