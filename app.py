import glob
import json
import re
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from recommendation.search_recommendations import NewsSearchEngine

# ---------------------------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="IntelliNews Portal & NLP Analytics",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for UI styling
st.markdown(
    """
<style>
    .stCard {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 16px;
        background-color: #ffffff;
        margin-bottom: 16px;
    }
    .metric-card {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        padding: 12px;
        border-radius: 8px;
        text-align: center;
    }
</style>
""",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Data Loader
# ---------------------------------------------------------------------------
@st.cache_data
def load_data():
    jsonl_dir = "data/nlp_enriched"
    jsonl_files = sorted(glob.glob(f"{jsonl_dir}/enriched_articles_*.jsonl"), reverse=True)
    if not jsonl_files:
        st.error(
            "No enriched JSONL file found! Please run your NLP pipeline first."
        )
        return pd.DataFrame()

    file_path = jsonl_files[0]
    records = []

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            data = json.loads(line)
            nlp = data.get("nlp", {})
            sentiment = nlp.get("sentiment", {})
            translation = nlp.get("translation", {})
            language_detection = nlp.get("language_detection", {})
            category_classification = nlp.get("category_classification", {})
            keyword_extraction = nlp.get("keyword_extraction", {})
            summary = nlp.get("summary", {})
            ner = nlp.get("ner", {})
            locations = nlp.get("location_extraction", {})

            records.append({
                "article_id": data.get("article_id", "N/A"),
                "title": data.get("title", "Untitled Article"),
                "domain": data.get("url_domain", "unknown.com"),
                "source_country": data.get("source_country", "Global"),
                "full_news_text": data.get(
                    "processed_text",
                    data.get("full_text_payload", "No full text payload available."),
                ),
                "preprocessed_text": nlp.get("preprocessing", {}).get("cleaned_text", data.get("processed_text", "")),
                "detected_language": language_detection.get("detected_language", nlp.get("language", "en")),
                "translated_text": translation.get("translated_text", data.get("processed_text", "")),
                "translation_applied": translation.get("applied", nlp.get("translation_applied", False)),
                "predicted_category": category_classification.get("category", nlp.get("category", "General")),
                "keywords_extracted": keyword_extraction.get("keywords", nlp.get("keywords", [])),
                "sentiment_label": sentiment.get("label", "Neutral"),
                "sentiment_polarity": sentiment.get("polarity_score", 0.0),
                "extractive_summary": summary.get("summary_text", nlp.get("summary", "No summary generated for this article.")),
                "ner_entities": ner.get("entities", nlp.get("entities", [])),
                "extracted_locations": locations.get("locations", nlp.get("locations", [])),
            })
    return pd.DataFrame(records)


@st.cache_resource
def get_search_engine():
    """Create one Qdrant search client for the Streamlit session."""
    return NewsSearchEngine(host="localhost", port=6333)


def infer_search_category(query):
    """Use explicit category words in a query as a local result constraint."""
    query_lower = query.lower()
    category_terms = {
        "Technology": ("technology", "tech", "artificial intelligence", "ai"),
        "Politics": ("politics", "political", "election", "government"),
        "Sports": ("sports", "sport", "football", "soccer", "cricket", "tennis"),
        "Business": ("business", "economy", "economic", "market", "finance"),
    }
    for category, terms in category_terms.items():
        if any(
            re.search(rf"\b{re.escape(term)}\b", query_lower)
            for term in terms
        ):
            return category
    return None

df = load_data()

if df.empty:
    st.stop()

# Initialize session state for single-article navigation
if "selected_article_id" not in st.session_state:
    st.session_state["selected_article_id"] = None

# Helper function to switch to Deep Analysis view
def view_article_analysis(article_id):
    st.session_state["selected_article_id"] = article_id

# Helper function to return back to Portal view
def return_to_portal():
    st.session_state["selected_article_id"] = None


def load_related_articles(article_id, category_bias=None, limit=3):
    """Fetch real related articles from the Qdrant vector store."""
    try:
        engine = NewsSearchEngine(host="localhost", port=6333)
        return engine.get_related_recommendations(
            article_id=article_id,
            category_bias=category_bias,
            limit=limit,
        )
    except Exception as exc:
        st.warning(f"Recommendation service unavailable: {exc}")
        return []

# ---------------------------------------------------------------------------
# VIEW 1: NEWS PORTAL (Main List View)
# ---------------------------------------------------------------------------
if st.session_state["selected_article_id"] is None:

    # Sidebar Filters
    st.sidebar.title("📰 Browse & Filter")

    selected_categories = st.sidebar.multiselect(
        "Filter by Category",
        options=sorted(list(df["predicted_category"].unique())),
        default=sorted(list(df["predicted_category"].unique())),
    )

    selected_sentiments = st.sidebar.multiselect(
        "Filter by Sentiment",
        options=sorted(list(df["sentiment_label"].unique())),
        default=sorted(list(df["sentiment_label"].unique())),
    )

    selected_countries = st.sidebar.multiselect(
        "Filter by Source Country",
        options=sorted(list(df["source_country"].unique())),
        default=sorted(list(df["source_country"].unique())),
    )

    # Filter Dataset
    filtered_df = df[
        (df["predicted_category"].isin(selected_categories))
        & (df["sentiment_label"].isin(selected_sentiments))
        & (df["source_country"].isin(selected_countries))
    ]

    # Top Bar & Search Input
    st.title("📰 IntelliNews Portal")

    search_query = st.text_input(
        "🔍 Semantic search with Qdrant",
        placeholder="Describe what you want to find, e.g. AI breakthroughs...",
    )

    if search_query:
        try:
            search_results = get_search_engine().hybrid_search(
                query_text=search_query,
                limit=len(df),
            )
            relevance_order = {
                result.get("article_id"): index
                for index, result in enumerate(search_results)
                if result.get("article_id")
            }
            inferred_category = infer_search_category(search_query)
            if inferred_category:
                filtered_df = filtered_df[
                    filtered_df["predicted_category"].eq(inferred_category)
                ].copy()
            filtered_df = filtered_df[
                filtered_df["article_id"].isin(relevance_order)
            ].copy()
            filtered_df["_relevance_order"] = filtered_df["article_id"].map(
                relevance_order
            )
            filtered_df = filtered_df.sort_values("_relevance_order").drop(
                columns="_relevance_order"
            )
        except Exception as exc:
            st.warning(f"Qdrant semantic search is unavailable: {exc}")
            filtered_df = filtered_df.iloc[0:0]

    # Global Intelligence Metrics Bar
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Articles Enriched", len(df))
    m2.metric("Filtered Portal Display", len(filtered_df))
    m3.metric("Topics Detected", df["predicted_category"].nunique())
    m4.metric(
        "Avg Sentiment Score", f"{filtered_df['sentiment_polarity'].mean():.2f}"
    )

    st.markdown("---")

    # Article Grid Display (3 Cards Per Row)
    st.subheader(f"Latest Enriched Headlines ({len(filtered_df)} articles)")

    if filtered_df.empty:
        st.warning("No articles matched your active filters or search term.")
    else:
        # Display articles in a grid
        cols_per_row = 3
        rows = [
            filtered_df.iloc[i : i + cols_per_row]
            for i in range(0, len(filtered_df), cols_per_row)
        ]

        for row_idx, row in enumerate(rows):
            cols = st.columns(cols_per_row)
            for col_idx, (idx, article) in enumerate(row.iterrows()):
                with cols[col_idx]:
                    with st.container():
                        # Category badge
                        category_color = {
                            "Technology": "blue",
                            "Business": "red",
                            "Politics": "orange",
                            "Sports": "green",
                            "General": "gray"
                        }.get(article['predicted_category'], "gray")
                        
                        st.markdown(f"**:{category_color}[[{article['predicted_category']}]]**")
                        st.markdown(f"### {article['title']}")
                        st.caption(f"Source: {article['domain']} | {article['source_country']}")

                        # Sentiment badge
                        sentiment_emoji = {
                            "Positive": "😊",
                            "Negative": "😞",
                            "Neutral": "😐"
                        }.get(article['sentiment_label'], "")
                        
                        st.caption(
                            f"{sentiment_emoji} {article['sentiment_label']} "
                            f"({article['sentiment_polarity']:.2f})"
                        )

                        # Snippet of extractive summary
                        snippet = (
                            article["extractive_summary"][:120] + "..."
                            if len(article["extractive_summary"]) > 120
                            else article["extractive_summary"]
                        )
                        st.write(snippet)

                        # Button to navigate to Deep Analysis View
                        # Use row_idx and col_idx to ensure unique keys across reruns
                        unique_key = f"btn_r{row_idx}_c{col_idx}_{article['article_id'][:8]}"
                        st.button(
                            "Read Deep Analysis ➔",
                            key=unique_key,
                            on_click=view_article_analysis,
                            args=(article["article_id"],),
                            use_container_width=True,
                        )

# ---------------------------------------------------------------------------
# VIEW 2: FOCUSED DEEP NLP ANALYTICS REPORT
# ---------------------------------------------------------------------------
else:
    # Locate selected article record
    target_id = st.session_state["selected_article_id"]
    selected_row = df[df["article_id"] == target_id]

    if selected_row.empty:
        st.error("Selected article could not be loaded.")
        st.button("Back to Portal", on_click=return_to_portal)
        st.stop()

    article = selected_row.iloc[0]

    # Navigation Back Button
    st.button("⬅️ Back to Portal", on_click=return_to_portal)

    st.markdown(
        f"# [{article['predicted_category']}] {article['title']}"
    )
    st.caption(
        f"Domain: **{article['domain']}** | Country: **{article['source_country']}** | Language: **{article['detected_language'].upper()}**"
    )

    st.markdown("---")

    # Section 1: Full Article Payload
    st.subheader("1. Full Cleaned News Content")
    with st.expander("Click to Collapse/Expand Complete Article", expanded=True):
        st.write(article["full_news_text"])

    st.markdown("---")

    # Section 2: Comprehensive 9-Task NLP Results
    st.subheader("2. Comprehensive 9-Task NLP Analysis Report")

    nlp_col1, nlp_col2 = st.columns([1, 1])

    with nlp_col1:
        st.markdown("#### Task 1: Preprocessing")
        st.info(f"Cleaned text preview: {article['preprocessed_text'][:300]}..." if len(article['preprocessed_text']) > 300 else article['preprocessed_text'])

        st.markdown("#### Task 2: Language Detection")
        st.success(f"Detected language: **{article['detected_language'].upper()}**")

        st.markdown("#### Task 3: Translation")
        if article['translation_applied']:
            st.success("Translation was applied to convert the article to English.")
        else:
            st.info("No translation was required; the article was already in English.")

        st.markdown("#### Task 4: Named Entity Recognition (NER)")
        if article["ner_entities"]:
            entities_formatted = "  \n".join([
                f"- `{ent['text']}` **({ent['label']})**"
                for ent in article["ner_entities"][:15]
            ])
            st.markdown(entities_formatted)
        else:
            st.info("No major named entities extracted.")

        st.markdown("#### Task 5: Location Extraction")
        if article["extracted_locations"]:
            locations_formatted = "  \n".join([
                f"- 📍 `{loc['text']}`" for loc in article["extracted_locations"]
            ])
            st.markdown(locations_formatted)
        else:
            st.info("No geographic locations cross-referenced.")

    with nlp_col2:
        st.markdown("#### Task 6: Category Classification")
        st.success(f"Predicted category: **{article['predicted_category']}**")

        st.markdown("#### Task 7: Keyword Extraction")
        if article['keywords_extracted']:
            st.markdown(
                "  \n".join([f"- `{kw}`" for kw in article['keywords_extracted']])
            )
        else:
            st.info("No decisive keywords extracted.")

        st.markdown("#### Task 8: Extractive Summary")
        st.success(article["extractive_summary"])

        st.markdown("#### Task 9: Sentiment Score & Label")
        polarity = article["sentiment_polarity"]
        fig_gauge = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=polarity,
                domain={"x": [0, 1], "y": [0, 1]},
                title={"text": f"Label: {article['sentiment_label']}"},
                gauge={
                    "axis": {"range": [-1.0, 1.0]},
                    "bar": {
                        "color": (
                            "#2ecc71"
                            if polarity > 0.05
                            else ("#e74c3c" if polarity < -0.05 else "#95a5a6")
                        )
                    },
                    "steps": [
                        {"range": [-1.0, -0.05], "color": "#fadbd8"},
                        {"range": [-0.05, 0.05], "color": "#ebedef"},
                        {"range": [0.05, 1.0], "color": "#d4efdf"},
                    ],
                },
            )
        )
        fig_gauge.update_layout(height=220, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig_gauge, use_container_width=True)

    st.markdown("---")

    # Section 3: Recommendation Discovery Loop (real Qdrant-based items)
    st.subheader("🔄 Discovery Loop: Recommended Similar Articles")
    st.caption("Qdrant vector similarity with category-aware related article discovery")

    related_articles = load_related_articles(
        article_id=article["article_id"],
        category_bias=article.get("predicted_category"),
        limit=3,
    )

    if not related_articles:
        st.info("No related articles were returned for this story. Try a different article or ensure Qdrant is running.")
    else:
        rec_cols = st.columns(min(len(related_articles), 3))
        for idx, rec in enumerate(related_articles):
            with rec_cols[idx % len(rec_cols)]:
                with st.container():
                    category = rec.get("category") or article.get("predicted_category", "General")
                    sentiment = rec.get("sentiment_label") or "Neutral"
                    score = rec.get("similarity_score", 0.0)
                    color = {
                        "Technology": "blue",
                        "Business": "red",
                        "Politics": "orange",
                        "Sports": "green",
                        "General": "gray"
                    }.get(category, "gray")

                    st.markdown(f"**:{color}[[{category}]]**")
                    st.markdown(f"#### {rec.get('title', 'Untitled article')}")
                    st.caption(
                        f"Vector Match: **{score:.2%}** | Sentiment: {sentiment} | Country: {rec.get('source_country', 'N/A')}"
                    )
                    st.button(
                        "Analyze This Next ➔",
                        key=f"rec_btn_{idx}_{rec.get('article_id', idx)}",
                        on_click=view_article_analysis,
                        args=(rec.get("article_id"),),
                        use_container_width=True,
                    )
