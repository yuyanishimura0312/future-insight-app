#!/usr/bin/env python3
"""
Academic Paper Collector for Future Insight App
OpenAlex APIから5分野の最新学術論文を収集し、SQLiteに格納する。
日次約500件（各分野100件）を目標に取得。

Data source: OpenAlex (https://openalex.org/) - free, no API key required
"""

import sqlite3
import json
import re
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter

from init_db import init_db, DB_PATH

# === 5分野とOpenAlexのコンセプトIDの対応 ===
# OpenAlexではLevel 0のコンセプト（大分野）でフィルタリングできる
FIELDS = {
    "人文学": {
        "openalex_topics": [
            "https://openalex.org/topics/T10243",  # Philosophy
            "https://openalex.org/topics/T10614",  # History
        ],
        # OpenAlex concept IDs for humanities-related fields
        "concept_ids": [
            "C138885662",   # Philosophy
            "C95457728",    # History
            "C162324750",   # Classics
            "C17744445",    # Political Science (partly)
        ],
        # Use broader type filter with keywords
        "openalex_filter": "concept.id:C138885662|C95457728|C162324750|C142362112",
    },
    "社会科学": {
        "concept_ids": [
            "C144133560",   # Economics
            "C15744967",    # Psychology
            "C162324750",   # Sociology
            "C17744445",    # Political Science
            "C142362112",   # Anthropology (relevant for user)
        ],
        "openalex_filter": "concept.id:C144133560|C15744967|C17744445|C142362112",
    },
    "自然科学": {
        "concept_ids": [
            "C121332964",   # Physics
            "C185592680",   # Chemistry
            "C86803240",    # Biology
            "C127313418",   # Geology
            "C33923547",    # Mathematics
        ],
        "openalex_filter": "concept.id:C121332964|C185592680|C86803240|C127313418",
    },
    "工学": {
        "concept_ids": [
            "C41008148",    # Computer Science
            "C127413603",   # Engineering
            "C119857082",   # Materials Science
            "C192562407",   # Environmental Science
        ],
        "openalex_filter": "concept.id:C41008148|C127413603|C119857082",
    },
    "芸術": {
        "concept_ids": [
            "C142362112",   # Art
            "C136764020",   # Art History
            "C33923547",    # Design-related
        ],
        "openalex_filter": "concept.id:C142362112|C136764020|C195592381",
    },
}

# OpenAlex API base URL
OPENALEX_API = "https://api.openalex.org/works"

# Polite pool: OpenAlex asks for an email in the User-Agent for faster access
USER_AGENT = "FutureInsightApp/1.0 (mailto:yuyanishimura0312@users.noreply.github.com)"


def fetch_papers_for_field(field_name: str, field_config: dict,
                           from_date: str, per_page: int = 100) -> list[dict]:
    """Fetch recent papers for a single field from OpenAlex API."""
    params = {
        "filter": f"{field_config['openalex_filter']},from_publication_date:{from_date}",
        "sort": "publication_date:desc",
        "per_page": per_page,
        "page": 1,
    }
    headers = {"User-Agent": USER_AGENT}

    try:
        resp = requests.get(OPENALEX_API, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"  [ERROR] {field_name}: API request failed - {e}")
        return []

    results = data.get("results", [])
    papers = []

    for work in results:
        # Extract essential fields
        title = work.get("title", "")
        if not title:
            continue

        # Build source URL - prefer DOI, fallback to OpenAlex URL
        doi = work.get("doi", "")
        openalex_id = work.get("id", "")
        source_url = doi if doi else openalex_id

        if not source_url:
            continue

        # Extract abstract (OpenAlex provides inverted index format)
        abstract = _reconstruct_abstract(work.get("abstract_inverted_index"))

        # Authors list
        authors = ", ".join(
            authorship.get("author", {}).get("display_name", "")
            for authorship in work.get("authorships", [])[:10]  # Max 10 authors
        )

        # Published date
        published_date = work.get("publication_date", "")

        # Extract concepts as insight tags
        concepts = [
            c.get("display_name", "")
            for c in work.get("concepts", [])[:8]
            if c.get("score", 0) > 0.3
        ]

        # Subfield from primary topic
        primary_topic = work.get("primary_topic", {})
        subfield = ""
        if primary_topic:
            subfield = primary_topic.get("subfield", {}).get("display_name", "")

        # Relevance score from the concept match
        top_score = 0.0
        for c in work.get("concepts", []):
            if any(cid in c.get("id", "") for cid in field_config.get("concept_ids", [])):
                top_score = max(top_score, c.get("score", 0))

        # Language detection from OpenAlex
        language = work.get("language", "en")

        # Source journal/venue
        source_info = work.get("primary_location", {})
        source_name = ""
        if source_info and source_info.get("source"):
            source_name = source_info["source"].get("display_name", "")

        papers.append({
            "title": title,
            "authors": authors,
            "source_url": source_url,
            "doi": doi,
            "summary": abstract if abstract else f"[{title}]",
            "field": field_name,
            "subfield": subfield,
            "source_name": source_name or "OpenAlex",
            "published_date": published_date,
            "language": language,
            "insight_tags": json.dumps(concepts, ensure_ascii=False),
            "novelty_score": None,  # To be calculated later with AI
            "relevance_score": round(top_score, 3),
            "openalex_id": openalex_id,
        })

    return papers


def _reconstruct_abstract(inverted_index: dict | None) -> str:
    """Reconstruct abstract text from OpenAlex's inverted index format."""
    if not inverted_index:
        return ""

    # OpenAlex stores abstracts as {word: [position1, position2, ...]}
    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))

    word_positions.sort(key=lambda x: x[0])
    abstract = " ".join(word for _, word in word_positions)

    # Strip any HTML tags
    abstract = re.sub(r"<[^>]+>", "", abstract).strip()

    # Truncate if too long
    if len(abstract) > 1000:
        abstract = abstract[:1000] + "..."

    return abstract


def store_papers(papers: list[dict]) -> int:
    """Insert papers into SQLite, skipping duplicates. Returns count of new papers."""
    conn = sqlite3.connect(DB_PATH)
    inserted = 0

    for paper in papers:
        try:
            conn.execute("""
                INSERT INTO papers (
                    title, authors, source_url, doi, summary,
                    field, subfield, source_name, published_date,
                    language, insight_tags, novelty_score,
                    relevance_score, openalex_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                paper["title"], paper["authors"], paper["source_url"],
                paper["doi"], paper["summary"], paper["field"],
                paper["subfield"], paper["source_name"],
                paper["published_date"], paper["language"],
                paper["insight_tags"], paper["novelty_score"],
                paper["relevance_score"], paper["openalex_id"],
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            # Duplicate openalex_id — skip
            pass

    conn.commit()
    conn.close()
    return inserted


def create_daily_digest(field_counts: dict, total: int, date_str: str):
    """Create a daily digest record summarizing the day's collection."""
    conn = sqlite3.connect(DB_PATH)

    # Extract top keywords from today's papers
    cursor = conn.execute(
        "SELECT insight_tags FROM papers WHERE date(detected_at) = ?",
        (date_str,)
    )
    all_tags = []
    for row in cursor.fetchall():
        if row[0]:
            try:
                tags = json.loads(row[0])
                all_tags.extend(tags)
            except json.JSONDecodeError:
                pass

    top_keywords = [kw for kw, _ in Counter(all_tags).most_common(20)]

    try:
        conn.execute("""
            INSERT OR REPLACE INTO daily_digests
                (digest_date, total_count, field_counts, trend_keywords)
            VALUES (?, ?, ?, ?)
        """, (
            date_str,
            total,
            json.dumps(field_counts, ensure_ascii=False),
            json.dumps(top_keywords, ensure_ascii=False),
        ))
        conn.commit()
    except sqlite3.Error as e:
        print(f"  [WARN] Failed to create digest: {e}")
    finally:
        conn.close()


def update_trends(date_str: str):
    """Update trend keywords based on today's papers."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(
        "SELECT insight_tags, field FROM papers WHERE date(detected_at) = ?",
        (date_str,)
    )

    keyword_fields = {}  # {(keyword, field): count}
    for row in cursor.fetchall():
        tags_json, field = row
        if not tags_json:
            continue
        try:
            tags = json.loads(tags_json)
        except json.JSONDecodeError:
            continue
        for tag in tags:
            key = (tag, field)
            keyword_fields[key] = keyword_fields.get(key, 0) + 1

    for (keyword, field), count in keyword_fields.items():
        # Upsert: update existing or insert new
        existing = conn.execute(
            "SELECT id, mention_count FROM trends WHERE keyword = ? AND field = ?",
            (keyword, field)
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE trends SET last_seen = ?, mention_count = mention_count + ? WHERE id = ?",
                (date_str, count, existing[0])
            )
        else:
            conn.execute(
                "INSERT INTO trends (keyword, field, first_seen, last_seen, mention_count) VALUES (?, ?, ?, ?, ?)",
                (keyword, field, date_str, date_str, count)
            )

    conn.commit()
    conn.close()


def main():
    today = datetime.now(timezone.utc)
    date_str = today.strftime("%Y-%m-%d")

    # Look back 3 days to catch recent publications
    from_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")

    print(f"=== Academic Paper Collector ({date_str}) ===\n")

    # 1. Initialize DB
    print("1. Initializing database...")
    init_db()
    print()

    # 2. Collect papers from each field
    print("2. Collecting papers from OpenAlex API...")
    all_papers = []
    field_counts = {}

    for field_name, field_config in FIELDS.items():
        print(f"   [{field_name}] Fetching up to 100 papers...")
        papers = fetch_papers_for_field(field_name, field_config, from_date, per_page=100)
        all_papers.extend(papers)
        field_counts[field_name] = len(papers)
        print(f"   [{field_name}] {len(papers)} papers fetched")

        # Be polite to the API
        time.sleep(0.5)

    print(f"\n   Total fetched: {len(all_papers)} papers")

    # 3. Store in database
    print("\n3. Storing papers in database...")
    new_count = store_papers(all_papers)
    print(f"   {new_count} new papers stored ({len(all_papers) - new_count} duplicates skipped)")

    # 4. Create daily digest
    print("\n4. Creating daily digest...")
    create_daily_digest(field_counts, new_count, date_str)
    print("   Daily digest created")

    # 5. Update trends
    print("\n5. Updating trends...")
    update_trends(date_str)
    print("   Trends updated")

    # 6. Summary
    print(f"\n=== Summary ===")
    print(f"Date: {date_str}")
    for field, count in field_counts.items():
        print(f"  {field}: {count} papers")
    print(f"  Total new: {new_count}")
    print(f"  Database: {DB_PATH}")

    # Show DB stats
    conn = sqlite3.connect(DB_PATH)
    total_in_db = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    conn.close()
    print(f"  Total in DB: {total_in_db}")
    print("\n✓ Done!")


if __name__ == "__main__":
    main()
