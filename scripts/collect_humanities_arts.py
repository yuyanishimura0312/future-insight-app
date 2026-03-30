#!/usr/bin/env python3
"""
Collect more humanities and arts papers from OpenAlex using topic-based filtering.
Uses primary_topic.subfield.id to get properly classified papers.
"""

import json
import re
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

from db import init_db, save_papers, export_papers_json, DB_PATH

# Subfield IDs from OpenAlex for humanities and arts
# These map to our field classification in clean_papers.py
COLLECTION_TARGETS = {
    "人文学": {
        "subfields": [
            ("subfields/1202", "History"),
            ("subfields/1208", "Literature and Literary Theory"),
            ("subfields/1211", "Philosophy"),
            ("subfields/1204", "Archeology"),
            ("subfields/1203", "Language and Linguistics"),
            ("subfields/1207", "History and Philosophy of Science"),
            ("subfields/1212", "Religious studies"),
            ("subfields/1205", "Classics"),
            ("subfields/1200", "General Arts and Humanities"),
        ],
    },
    "芸術": {
        "subfields": [
            ("subfields/1213", "Visual Arts and Performing Arts"),
            ("subfields/1209", "Museology"),
            ("subfields/1210", "Music"),
            ("subfields/2216", "Architecture"),
        ],
    },
}

OPENALEX_API = "https://api.openalex.org/works"
USER_AGENT = "FutureInsightApp/1.0 (mailto:yuyanishimura0312@users.noreply.github.com)"
DATA_DIR = Path(__file__).parent.parent / "data"


def _reconstruct_abstract(inverted_index):
    if not inverted_index:
        return ""
    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort(key=lambda x: x[0])
    abstract = " ".join(word for _, word in word_positions)
    abstract = re.sub(r"<[^>]+>", "", abstract).strip()
    if len(abstract) > 1000:
        abstract = abstract[:1000] + "..."
    return abstract


def is_junk(title, authors):
    """Filter out junk entries during collection."""
    if not title or not title.strip():
        return True
    # Journal/proceedings names
    if not authors and ("Journal of" in title or "Transactions on" in title or "Proceedings of" in title):
        return True
    # GitHub repo patterns
    if re.match(r'^[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+:', title):
        return True
    # Very short non-informative titles
    if len(title) < 5:
        return True
    return False


def fetch_papers(field_name, subfield_id, subfield_name, from_date, to_date, per_page=200, max_pages=5):
    """Fetch papers for a specific subfield, sorted by citation count."""
    all_papers = []

    for page in range(1, max_pages + 1):
        params = {
            "filter": f"primary_topic.subfield.id:{subfield_id},from_publication_date:{from_date},to_publication_date:{to_date}",
            "sort": "cited_by_count:desc",
            "per_page": per_page,
            "page": page,
        }
        headers = {"User-Agent": USER_AGENT}

        try:
            resp = requests.get(OPENALEX_API, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"    [ERROR] {subfield_name} page {page}: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        total = data.get("meta", {}).get("count", 0)

        for work in results:
            title = work.get("title", "")
            authors = ", ".join(
                a.get("author", {}).get("display_name", "")
                for a in work.get("authorships", [])[:10]
            )

            if is_junk(title, authors):
                continue

            doi = work.get("doi", "")
            openalex_id = work.get("id", "")
            source_url = doi if doi else openalex_id
            if not source_url:
                continue

            abstract = _reconstruct_abstract(work.get("abstract_inverted_index"))

            primary_topic = work.get("primary_topic", {})
            actual_subfield = ""
            if primary_topic:
                actual_subfield = primary_topic.get("subfield", {}).get("display_name", "")

            concepts = [
                c.get("display_name", "")
                for c in work.get("concepts", [])[:8]
                if c.get("score", 0) > 0.3
            ]

            language = work.get("language", "en")
            source_info = work.get("primary_location", {})
            source_name = ""
            if source_info and source_info.get("source"):
                source_name = source_info["source"].get("display_name", "")

            all_papers.append({
                "title": title,
                "authors": authors,
                "source_url": source_url,
                "doi": doi,
                "summary": abstract if abstract else f"[{title}]",
                "field": field_name,
                "subfield": actual_subfield or subfield_name,
                "source_name": source_name or "OpenAlex",
                "published_date": work.get("publication_date", ""),
                "language": language,
                "insight_tags": json.dumps(concepts, ensure_ascii=False),
                "novelty_score": None,
                "relevance_score": round(work.get("cited_by_count", 0) / 1000, 3),
                "openalex_id": openalex_id,
            })

        print(f"    page {page}/{max_pages} ({len(results)} results, total available: {total:,})")
        time.sleep(0.3)

        # Stop if we've gotten all results
        if page * per_page >= total:
            break

    return all_papers


def main():
    init_db()

    print("=== Humanities & Arts Paper Collector ===\n")

    # Collect papers in 6-month periods over the past 2 years
    now = datetime.now(timezone.utc)
    periods = []
    for months_back in range(0, 24, 6):
        end = now - timedelta(days=months_back * 30)
        start = end - timedelta(days=180)
        periods.append((
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
        ))

    total_new = 0
    total_fetched = 0

    for field_name, config in COLLECTION_TARGETS.items():
        print(f"\n--- {field_name} ---")

        for sf_id, sf_name in config["subfields"]:
            print(f"\n  [{sf_name}]")

            for from_date, to_date in periods:
                print(f"    Period: {from_date} ~ {to_date}")
                papers = fetch_papers(
                    field_name, sf_id, sf_name,
                    from_date, to_date,
                    per_page=200,
                    max_pages=3,  # Up to 600 papers per subfield per period
                )
                total_fetched += len(papers)

                if papers:
                    new_count = save_papers(papers)
                    total_new += new_count
                    print(f"    -> {len(papers)} fetched, {new_count} new")
                else:
                    print(f"    -> 0 papers")

                time.sleep(0.5)

    # Export updated papers.json
    print(f"\n--- Exporting ---")
    papers_json = DATA_DIR / "papers.json"
    total_exported = export_papers_json(papers_json)
    print(f"Total in DB: {total_exported}")
    print(f"Fetched: {total_fetched}, New: {total_new}")
    print("\nDone!")


if __name__ == "__main__":
    main()
