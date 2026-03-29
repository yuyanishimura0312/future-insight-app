#!/usr/bin/env python3
"""
Historical Academic Paper Collector - 過去2年分の学術論文を網羅的に収集
OpenAlex APIから5分野×24ヶ月分の論文を取得してDBに格納する。
"""
from __future__ import annotations

import json
import re
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

from db import init_db, save_papers, export_papers_json, DB_PATH

# collect_papers.py と同じフィールド定義
FIELDS = {
    "人文学": {
        "concept_ids": ["C138885662", "C95457728", "C162324750", "C17744445"],
        "openalex_filter": "concept.id:C138885662|C95457728|C162324750|C142362112",
    },
    "社会科学": {
        "concept_ids": ["C144133560", "C15744967", "C17744445", "C142362112"],
        "openalex_filter": "concept.id:C144133560|C15744967|C17744445|C142362112",
    },
    "自然科学": {
        "concept_ids": ["C121332964", "C185592680", "C86803240", "C127313418", "C33923547"],
        "openalex_filter": "concept.id:C121332964|C185592680|C86803240|C127313418",
    },
    "工学": {
        "concept_ids": ["C41008148", "C127413603", "C119857082", "C192562407"],
        "openalex_filter": "concept.id:C41008148|C127413603|C119857082",
    },
    "芸術": {
        "concept_ids": ["C142362112", "C136764020", "C33923547"],
        "openalex_filter": "concept.id:C142362112|C136764020|C195592381",
    },
}

OPENALEX_API = "https://api.openalex.org/works"
USER_AGENT = "FutureInsightApp/1.0 (mailto:yuyanishimura0312@users.noreply.github.com)"


def _reconstruct_abstract(inverted_index: dict | None) -> str:
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


def fetch_papers_page(field_name: str, field_config: dict,
                      from_date: str, to_date: str,
                      per_page: int = 200, page: int = 1) -> tuple[list[dict], int]:
    """1ページ分の論文を取得。(papers, total_count)を返す。"""
    params = {
        "filter": f"{field_config['openalex_filter']},from_publication_date:{from_date},to_publication_date:{to_date}",
        "sort": "cited_by_count:desc",  # 被引用数順で重要な論文を優先
        "per_page": per_page,
        "page": page,
    }
    headers = {"User-Agent": USER_AGENT}

    try:
        resp = requests.get(OPENALEX_API, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"    [ERROR] {field_name} page {page}: {e}")
        return [], 0

    total_count = data.get("meta", {}).get("count", 0)
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

        cited_by = work.get("cited_by_count", 0)

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

    return papers, total_count


def collect_field_historical(field_name: str, field_config: dict,
                              start_date: str, end_date: str,
                              max_per_month: int = 200) -> list[dict]:
    """指定フィールドの過去データを月ごとに収集。"""
    all_papers = []
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start
    while current < end:
        # 月の最初と最後
        month_start = current.strftime("%Y-%m-%d")
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        month_end = (next_month - timedelta(days=1)).strftime("%Y-%m-%d")

        if next_month > end:
            month_end = end.strftime("%Y-%m-%d")

        print(f"    {month_start} ~ {month_end} ...", end=" ", flush=True)

        papers, total = fetch_papers_page(
            field_name, field_config,
            month_start, month_end,
            per_page=max_per_month, page=1
        )
        all_papers.extend(papers)
        print(f"{len(papers)} papers (total available: {total:,})")

        time.sleep(0.3)  # API rate limit respect
        current = next_month

    return all_papers


def main():
    today = datetime.now(timezone.utc)
    end_date = today.strftime("%Y-%m-%d")
    start_date = (today - timedelta(days=730)).strftime("%Y-%m-%d")  # 2年前
    data_dir = Path(__file__).parent.parent / "data"

    print(f"{'='*60}")
    print(f"  Historical Paper Collection: {start_date} ~ {end_date}")
    print(f"  Fields: {', '.join(FIELDS.keys())}")
    print(f"  Target: up to 200 papers/field/month (被引用数順)")
    print(f"{'='*60}\n")

    # 1. Initialize DB
    print("1. Initializing database...")
    init_db()

    # 2. Collect by field
    print("\n2. Collecting papers from OpenAlex API...\n")
    grand_total = 0
    field_totals = {}

    for field_name, field_config in FIELDS.items():
        print(f"  [{field_name}]")
        papers = collect_field_historical(
            field_name, field_config,
            start_date, end_date,
            max_per_month=200
        )
        field_totals[field_name] = len(papers)
        grand_total += len(papers)

        # Save in batches
        if papers:
            new_count = save_papers(papers)
            print(f"    => {new_count} new / {len(papers)} fetched\n")
        else:
            print(f"    => 0 papers\n")

        time.sleep(0.5)

    # 3. Export
    print("3. Exporting papers.json...")
    papers_json = data_dir / "papers.json"
    total_exported = export_papers_json(papers_json)
    print(f"   {total_exported} papers exported\n")

    # 4. Summary
    print(f"{'='*60}")
    print(f"  Summary")
    print(f"{'='*60}")
    print(f"  Period: {start_date} ~ {end_date}")
    for field, count in field_totals.items():
        print(f"    {field}: {count} papers fetched")
    print(f"  Total fetched: {grand_total}")
    print(f"  Total in DB: {total_exported}")
    print(f"  Database: {DB_PATH}")
    print(f"\n  Done!")


if __name__ == "__main__":
    main()
