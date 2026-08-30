# Silver Layer Transformation Specs

# Schema Sanitization & Type Casting:

Function: to_timestamp(), col().cast()

Implementation: Converts published_at string (ISO format) into TimestampType() and explicitly casts word_count to IntegerType().

# Text Normalization:

Function: regexp_replace(), trim(), lower()

Implementation: Uses regex r'<[^>]+>' to strip leftover HTML tags from content and title, then trims trailing whitespace.

# Data Enrichment:

Function: regexp_extract(), PySpark SQL functions

Implementation:

Domain Extraction: regexp_extract(col("url"), r'https?://(?:www\.)?([^/]+)', 1) extracts root domains (e.g., bbc.co.uk).

Reading Time: (col("word_count") / 200).cast("int") calculates estimated read time in minutes (assuming 200 wpm).

# Null Handling & Quality Control:

Function: filter(), col().isNotNull()

Implementation: Filters out bad data records: .filter(col("id").isNotNull() & col("title").isNotNull() & (col("word_count") > 5)).

# Partitioning & Writing:

Function: writeStream.partitionBy(), .format("delta")

Implementation: Writes to the silver ADLS container partitioned by source_country and processed_date for query optimization.