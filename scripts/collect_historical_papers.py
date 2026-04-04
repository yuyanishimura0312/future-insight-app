#!/usr/bin/env python3
"""
Historical Paper Collector for Future Insight App
Collect 100 highly-cited papers per field (500 total) from 1990-2025
using the Semantic Scholar Academic Graph API.

Output: data/historical_papers.json
"""

import json
import time
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

# === 5 fields with search queries ===
FIELDS = {
    "自然科学": "physics OR chemistry OR biology OR mathematics",
    "工学": "computer science OR engineering OR materials science",
    "社会科学": "economics OR psychology OR political science OR anthropology",
    "人文学": "philosophy OR history OR linguistics OR literature",
    "芸術": "art OR aesthetics OR visual arts OR performing arts",
}

BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
REQUESTED_FIELDS = "title,authors,year,abstract,citationCount,fieldsOfStudy,externalIds,url"

# Rate limit: 100 requests per 5 minutes without API key
REQUEST_DELAY = 3.5  # seconds between requests (safe margin)
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds to wait on 429 or 5xx errors


def fetch_papers(query: str, limit: int = 100) -> list[dict]:
    """Fetch top-cited papers for a query from Semantic Scholar API."""
    params = {
        "query": query,
        "year": "1990-2025",
        "fields": REQUESTED_FIELDS,
        "limit": limit,
        "sort": "citationCount:desc",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)

            if resp.status_code == 429:
                # Rate limited — wait and retry
                wait = RETRY_DELAY * attempt
                print(f"    Rate limited (429). Waiting {wait}s before retry {attempt}/{MAX_RETRIES}...")
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                # Server error — retry
                wait = RETRY_DELAY * attempt
                print(f"    Server error ({resp.status_code}). Waiting {wait}s before retry {attempt}/{MAX_RETRIES}...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()
            return data.get("data", [])

        except requests.RequestException as e:
            print(f"    Request failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                print(f"    All {MAX_RETRIES} attempts failed. Returning empty list.")
                return []

    return []


def normalize_paper(paper: dict, field_name: str) -> dict:
    """Convert Semantic Scholar API response to our output format."""
    authors = [
        a.get("name", "")
        for a in (paper.get("authors") or [])
        if a.get("name")
    ]

    return {
        "paperId": paper.get("paperId", ""),
        "title": paper.get("title", ""),
        "title_ja": None,  # to be translated later
        "authors": authors,
        "year": paper.get("year"),
        "abstract": paper.get("abstract", ""),
        "citationCount": paper.get("citationCount", 0),
        "fieldsOfStudy": paper.get("fieldsOfStudy") or [],
        "url": paper.get("url", ""),
        "field": field_name,
    }


def main():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    output_path = data_dir / "historical_papers.json"

    print(f"=== Historical Paper Collector ({today}) ===\n")
    print(f"Source: Semantic Scholar Academic Graph API")
    print(f"Range: 1990-2025, sorted by citation count (desc)\n")

    result = {
        "collected_at": today,
        "fields": {},
        "total": 0,
    }

    # Track seen paperIds for deduplication across fields
    seen_ids = set()

    for field_name, query in FIELDS.items():
        print(f"[{field_name}] Searching: {query[:60]}...")

        raw_papers = fetch_papers(query, limit=100)
        print(f"  Received {len(raw_papers)} papers from API")

        papers = []
        for p in raw_papers:
            pid = p.get("paperId", "")
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)
            papers.append(normalize_paper(p, field_name))

        result["fields"][field_name] = papers
        result["total"] += len(papers)
        print(f"  Stored {len(papers)} papers (after dedup)\n")

        # Respect rate limits between field queries
        time.sleep(REQUEST_DELAY)

    # Save output
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"=== Summary ===")
    for field_name, papers in result["fields"].items():
        if papers:
            top = papers[0]
            print(f"  {field_name}: {len(papers)} papers (top: {top['citationCount']} citations)")
        else:
            print(f"  {field_name}: 0 papers")
    print(f"  Total: {result['total']} papers")
    print(f"  Output: {output_path}")
    print(f"\nDone!")


if __name__ == "__main__":
    main()
