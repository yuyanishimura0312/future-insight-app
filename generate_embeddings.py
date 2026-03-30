#!/usr/bin/env python3
"""Generate binary-quantized embeddings for the future insight app.

Uses multilingual-e5-small to create compact binary vectors for ~70K entries.
Binary quantization reduces storage from ~270MB to ~6MB while retaining
~90% of cosine similarity accuracy.

Optimized for speed: uses titles only (short text per entry).
"""

import json
import os
import sys
import base64

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
EMBEDDINGS_FILE = os.path.join(DATA_DIR, "embeddings_binary.json")

MODEL_NAME = "intfloat/multilingual-e5-small"
BATCH_SIZE = 64
# Limit papers to keep generation time reasonable (~10 min on M2 Mac)
MAX_PAPERS = 10000


def load_data():
    """Load news and papers data, return list of (id, text) tuples.
    Uses short text (title + field only) for fast embedding generation."""
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

    # Load papers (prioritize those with Japanese titles)
    papers_path = os.path.join(DATA_DIR, "papers_light.json")
    if os.path.exists(papers_path):
        with open(papers_path, "r", encoding="utf-8") as f:
            papers = json.load(f)

        # Sort: papers with title_ja first, then by recency
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
        print(f"  Papers: {paper_count} (of {len(papers)} total, limited to {MAX_PAPERS})")

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

    print(f"Generating embeddings for {len(docs)} documents (batch_size={BATCH_SIZE})...")
    embeddings = model.encode(
        docs,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    embeddings = np.array(embeddings, dtype=np.float32)
    print(f"Embeddings shape: {embeddings.shape}")

    # Binary quantization
    print("Applying binary quantization...")
    medians = np.median(embeddings, axis=0)
    binary = (embeddings > medians).astype(np.uint8)
    packed = np.packbits(binary, axis=1)

    vectors = {}
    for entry_id, row in zip(ids, packed):
        vectors[entry_id] = base64.b64encode(row.tobytes()).decode("ascii")

    output = {
        "model": MODEL_NAME,
        "dims": int(embeddings.shape[1]),
        "count": len(vectors),
        "medians": [round(float(m), 6) for m in medians],
        "vectors": vectors,
    }

    print(f"Writing {EMBEDDINGS_FILE}...")
    with open(EMBEDDINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False)

    file_size_mb = os.path.getsize(EMBEDDINGS_FILE) / (1024 * 1024)
    print(f"Done! {file_size_mb:.1f} MB, {len(vectors)} entries")


if __name__ == "__main__":
    main()
