#!/usr/bin/env python3
"""Generate embeddings for the future insight app.

Uses multilingual-e5-small to create float vectors (rounded to 3 decimals)
for ~10K entries. Stored as JSON for browser-side cosine similarity search.
"""

import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
EMBEDDINGS_FILE = os.path.join(DATA_DIR, "embeddings.json")

MODEL_NAME = "intfloat/multilingual-e5-small"
BATCH_SIZE = 64
MAX_PAPERS = 10000


def load_data():
    """Load news and papers data, return list of (id, text) tuples."""
    entries = []

    # Load news
    news_path = os.path.join(DATA_DIR, "latest.json")
    if os.path.exists(news_path):
        with open(news_path, "r", encoding="utf-8") as f:
            news = json.load(f)
        for cat, info in news.get("pestle", {}).items():
            for i, article in enumerate(info.get("articles", [])):
                entry_id = f"news_{cat}_{i}"
                title = article.get("title", "")
                summary = (article.get("summary", "") or "")[:200]
                text = f"{title} {summary}"
                entries.append((entry_id, text))
        print(f"  News: {len(entries)} articles")

    # Load papers
    papers_path = os.path.join(DATA_DIR, "papers_light.json")
    if os.path.exists(papers_path):
        with open(papers_path, "r", encoding="utf-8") as f:
            papers = json.load(f)

        indexed = list(enumerate(papers))
        indexed.sort(key=lambda x: (not bool(x[1].get("title_ja")), x[0]))
        selected = indexed[:MAX_PAPERS]

        paper_count = 0
        for orig_idx, paper in selected:
            entry_id = f"paper_{orig_idx}"
            parts = []
            if paper.get("title_ja"):
                parts.append(paper["title_ja"])
            if paper.get("title"):
                parts.append(paper["title"])
            if paper.get("field"):
                parts.append(paper["field"])
            text = " ".join(parts)
            entries.append((entry_id, text))
            paper_count += 1
        print(f"  Papers: {paper_count} (of {len(papers)} total)")

    return entries


def main():
    print("Loading data...")
    entries = load_data()
    print(f"Total entries: {len(entries)}")

    ids = [e[0] for e in entries]
    docs = [f"passage: {e[1]}" for e in entries]

    from sentence_transformers import SentenceTransformer

    print(f"Loading model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)

    print(f"Generating embeddings for {len(docs)} documents...")
    embeddings = model.encode(
        docs,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
        normalize_embeddings=True,
    )

    print(f"Embeddings shape: {embeddings.shape}")

    # Store as JSON with 3 decimal precision
    vectors = {}
    for entry_id, vec in zip(ids, embeddings):
        vectors[entry_id] = [round(float(v), 3) for v in vec]

    output = {
        "model": MODEL_NAME,
        "dims": int(embeddings.shape[1]),
        "count": len(vectors),
        "vectors": vectors,
    }

    print(f"Writing {EMBEDDINGS_FILE}...")
    with open(EMBEDDINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False)

    file_size_mb = os.path.getsize(EMBEDDINGS_FILE) / (1024 * 1024)
    print(f"Done! {file_size_mb:.1f} MB, {len(vectors)} entries")


if __name__ == "__main__":
    main()
