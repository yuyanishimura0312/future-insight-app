#!/usr/bin/env python3
"""
Academic Paper Collector for Future Insight App
OpenAlex APIから5分野の最新学術論文を収集し、統合DBに格納する。
日次約500件（各分野100件）を目標に取得。

Data source: OpenAlex (https://openalex.org/) - free, no API key required
"""

import json
import re
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

from db import init_db, save_papers, save_daily_digest, update_trends, export_papers_json, DB_PATH

# === 5分野とOpenAlexのコンセプトIDの対応 ===
FIELDS = {
    "人文学": {
        "concept_ids": [
            "C138885662",   # Philosophy
            "C95457728",    # History
            "C162324750",   # Classics
        ],
        "openalex_filter": "concept.id:C138885662|C95457728|C162324750",
    },
    "社会科学": {
        "concept_ids": [
            "C144133560",   # Economics
            "C15744967",    # Psychology
            "C17744445",    # Political Science
            "C142362112",   # Anthropology
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
            "C52119013",    # Art history (level 1, 6.9M works)
            "C153349607",   # Visual arts (level 1, 5M works)
            "C107038049",   # Aesthetics (level 1, 3.7M works)
            "C163286209",   # Performing arts (level 2, 72K works)
            "C554144382",   # Performance art (level 2, 450K works)
            "C119657128",   # Photography (level 2, 964K works)
        ],
        "openalex_filter": "concept.id:C52119013|C153349607|C107038049|C163286209|C554144382|C119657128",
    },
}

OPENALEX_API = "https://api.openalex.org/works"
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
        title = work.get("title", "")
        if not title:
            continue

        doi = work.get("doi", "")
        openalex_id = work.get("id", "")
        source_url = doi if doi else openalex_id
        if not source_url:
            continue

        abstract = _reconstruct_abstract(work.get("abstract_inverted_index"))

        authors = ", ".join(
            authorship.get("author", {}).get("display_name", "")
            for authorship in work.get("authorships", [])[:10]
        )

        published_date = work.get("publication_date", "")

        concepts = [
            c.get("display_name", "")
            for c in work.get("concepts", [])[:8]
            if c.get("score", 0) > 0.3
        ]

        primary_topic = work.get("primary_topic", {})
        subfield = ""
        if primary_topic:
            subfield = primary_topic.get("subfield", {}).get("display_name", "")

        top_score = 0.0
        for c in work.get("concepts", []):
            if any(cid in c.get("id", "") for cid in field_config.get("concept_ids", [])):
                top_score = max(top_score, c.get("score", 0))

        language = work.get("language", "en")

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
            "novelty_score": None,
            "relevance_score": round(top_score, 3),
            "openalex_id": openalex_id,
        })

    return papers


def _reconstruct_abstract(inverted_index: dict | None) -> str:
    """Reconstruct abstract text from OpenAlex's inverted index format."""
    if not inverted_index:
        return ""

    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))

    word_positions.sort(key=lambda x: x[0])
    abstract = " ".join(word for _, word in word_positions)

    # Strip any HTML tags
    abstract = re.sub(r"<[^>]+>", "", abstract).strip()

    if len(abstract) > 1000:
        abstract = abstract[:1000] + "..."

    return abstract


def main():
    today = datetime.now(timezone.utc)
    date_str = today.strftime("%Y-%m-%d")
    from_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
    data_dir = Path(__file__).parent.parent / "data"

    print(f"=== Academic Paper Collector ({date_str}) ===\n")

    # 1. Initialize unified DB
    print("1. Initializing database...")
    init_db()

    # 2. Collect papers from each field
    print("\n2. Collecting papers from OpenAlex API...")
    all_papers = []
    field_counts = {}

    for field_name, field_config in FIELDS.items():
        print(f"   [{field_name}] Fetching up to 100 papers...")
        papers = fetch_papers_for_field(field_name, field_config, from_date, per_page=100)
        all_papers.extend(papers)
        field_counts[field_name] = len(papers)
        print(f"   [{field_name}] {len(papers)} papers fetched")
        time.sleep(0.5)

    print(f"\n   Total fetched: {len(all_papers)} papers")

    # 3. Store in unified database
    print("\n3. Storing papers in database...")
    new_count = save_papers(all_papers)
    print(f"   {new_count} new papers stored ({len(all_papers) - new_count} duplicates skipped)")

    # 4. Create daily digest + update trends
    print("\n4. Creating daily digest & updating trends...")
    save_daily_digest(field_counts, new_count, date_str)
    update_trends(date_str)
    print("   Done")

    # 5. Export papers.json for web dashboard
    print("\n5. Exporting papers.json for dashboard...")
    papers_json = data_dir / "papers.json"
    total_exported = export_papers_json(papers_json)
    print(f"   {total_exported} papers exported to {papers_json}")

    # 6. Summary
    print(f"\n=== Summary ===")
    print(f"Date: {date_str}")
    for field, count in field_counts.items():
        print(f"  {field}: {count} papers")
    print(f"  New: {new_count} / Total in DB: {total_exported}")
    print(f"  Database: {DB_PATH}")
    print("\n✓ Done!")


if __name__ == "__main__":
    main()
