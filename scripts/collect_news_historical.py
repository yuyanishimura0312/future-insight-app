#!/usr/bin/env python3
"""
Historical News Collector for Future Insight App
GDELT DOC APIを使って過去2年分のPESTLEニュースを収集する。

GDELT API: free, no API key, rate limit = 1 request per 5 seconds
Strategy: collect per month × per PESTLE category = 24 months × 6 = 144 requests
"""

import sqlite3
import json
import hashlib
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

from db import get_connection, init_db, DB_PATH

GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"

# PESTLE search queries — keywords tuned for GDELT global news search
# Each category gets English + Japanese keywords combined with OR
PESTLE_QUERIES = {
    "Political": {
        "label_ja": "政治",
        "query": '(election OR government OR diplomacy OR geopolitics OR "foreign policy" OR sanctions OR parliament OR summit OR NATO OR "united nations")',
    },
    "Economic": {
        "label_ja": "経済",
        "query": '(economy OR GDP OR inflation OR "stock market" OR trade OR tariff OR "central bank" OR recession OR startup OR "supply chain" OR investment)',
    },
    "Social": {
        "label_ja": "社会",
        "query": '(education OR "public health" OR immigration OR inequality OR "mental health" OR aging OR demographic OR urbanization OR diversity OR "social justice")',
    },
    "Technological": {
        "label_ja": "技術",
        "query": '("artificial intelligence" OR AI OR quantum OR robotics OR cybersecurity OR semiconductor OR biotech OR "space exploration" OR blockchain OR "machine learning")',
    },
    "Legal": {
        "label_ja": "法律",
        "query": '(regulation OR "data privacy" OR antitrust OR "intellectual property" OR GDPR OR compliance OR "human rights" OR "supreme court" OR legislation OR patent)',
    },
    "Environmental": {
        "label_ja": "環境",
        "query": '("climate change" OR "renewable energy" OR carbon OR biodiversity OR "net zero" OR sustainability OR deforestation OR "electric vehicle" OR pollution OR wildfire)',
    },
}

# 24 months: April 2024 → March 2026
MONTHS = []
for year in [2024, 2025, 2026]:
    for month in range(1, 13):
        if year == 2024 and month < 4:
            continue  # Start from April 2024
        if year == 2026 and month > 3:
            continue  # End at March 2026
        MONTHS.append((year, month))


def fetch_gdelt(query: str, start_dt: str, end_dt: str, max_records: int = 250) -> list[dict]:
    """Fetch articles from GDELT DOC API for a date range."""
    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": max_records,
        "startdatetime": start_dt,
        "enddatetime": end_dt,
        "format": "json",
        "sort": "datedesc",
    }

    try:
        resp = requests.get(GDELT_API, params=params, timeout=30)
        if resp.status_code == 429:
            # Rate limited — wait and retry once
            time.sleep(10)
            resp = requests.get(GDELT_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("articles", [])
    except (requests.RequestException, ValueError) as e:
        print(f"    [WARN] GDELT fetch failed: {e}")
        return []


def parse_gdelt_date(seendate: str) -> str | None:
    """Parse GDELT seendate format '20260322T191500Z' to 'YYYY-MM-DD'."""
    if not seendate:
        return None
    try:
        return datetime.strptime(seendate, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d")
    except ValueError:
        return None


def store_historical_articles(articles: list[dict], category: str,
                              label_ja: str, year: int, month: int) -> int:
    """Store GDELT articles into the unified DB. Returns insert count."""
    conn = get_connection()

    # Create or get collection for this month
    date_key = f"{year:04d}-{month:02d}-01"
    collected_at = datetime.now(timezone.utc).isoformat()

    cur = conn.execute(
        """INSERT OR IGNORE INTO collections
           (date, collected_at, total_fetched, feeds_count, total_selected)
           VALUES (?, ?, ?, ?, ?)""",
        (date_key, collected_at, len(articles), 1, 0)
    )
    # Get the collection_id (may already exist)
    row = conn.execute(
        "SELECT id FROM collections WHERE date = ?", (date_key,)
    ).fetchone()
    collection_id = row[0]

    inserted = 0
    for article in articles:
        url = article.get("url", "")
        title = article.get("title", "").strip()
        if not url or not title:
            continue

        url_hash = hashlib.sha256(url.encode()).hexdigest()
        pub_date = parse_gdelt_date(article.get("seendate", ""))
        lang = article.get("language", "English")
        # Normalize language code
        lang_code = "ja" if lang.lower() in ("japanese",) else "en"
        domain = article.get("domain", "")

        try:
            conn.execute(
                """INSERT OR IGNORE INTO articles
                   (collection_id, url_hash, title, summary, url, source, lang,
                    published, published_date, pestle_category, relevance_score)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (collection_id, url_hash, title, "",  # No summary from GDELT
                 url, domain, lang_code,
                 article.get("seendate", ""), pub_date,
                 category, 1.0)
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    # Update total_selected count
    conn.execute(
        "UPDATE collections SET total_selected = total_selected + ? WHERE id = ?",
        (inserted, collection_id)
    )

    conn.commit()
    conn.close()
    return inserted


def main():
    print(f"{'#' * 60}")
    print(f"  Historical News Collector (GDELT)")
    print(f"  Period: April 2024 — March 2026 ({len(MONTHS)} months)")
    print(f"  Categories: {len(PESTLE_QUERIES)}")
    print(f"  Estimated time: ~12 minutes")
    print(f"{'#' * 60}\n")

    init_db()

    total_inserted = 0
    total_fetched = 0
    request_count = 0
    total_requests = len(MONTHS) * len(PESTLE_QUERIES)

    for year, month in MONTHS:
        # Build date range for this month
        start_dt = f"{year:04d}{month:02d}01000000"
        if month == 12:
            end_dt = f"{year + 1:04d}0101000000"
        else:
            end_dt = f"{year:04d}{month + 1:02d}01000000"

        month_label = f"{year:04d}-{month:02d}"
        month_inserted = 0

        for category, config in PESTLE_QUERIES.items():
            request_count += 1
            progress = f"[{request_count}/{total_requests}]"
            print(f"  {progress} {month_label} / {config['label_ja']} ({category})...", end=" ", flush=True)

            articles = fetch_gdelt(config["query"], start_dt, end_dt, max_records=250)
            fetched = len(articles)
            total_fetched += fetched

            inserted = store_historical_articles(
                articles, category, config["label_ja"], year, month
            )
            total_inserted += inserted
            month_inserted += inserted
            print(f"{fetched} fetched, {inserted} new")

            # Rate limit: 1 request per 5 seconds
            time.sleep(5)

        print(f"  --- {month_label}: {month_inserted} articles stored ---\n")

    # Export latest.json with all data for the dashboard
    export_latest_json()

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  COMPLETE")
    print(f"  Total fetched: {total_fetched:,}")
    print(f"  Total stored:  {total_inserted:,}")
    print(f"  Database: {DB_PATH}")
    print(f"{'=' * 60}\n")

    # Show DB stats
    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    print(f"  Total articles in DB: {total:,}")

    rows = conn.execute(
        "SELECT pestle_category, COUNT(*) FROM articles GROUP BY pestle_category ORDER BY COUNT(*) DESC"
    ).fetchall()
    for cat, count in rows:
        print(f"    {cat}: {count:,}")

    collections = conn.execute("SELECT COUNT(*) FROM collections").fetchone()[0]
    print(f"\n  Total collections (months): {collections}")
    conn.close()


def export_latest_json():
    """Export the most recent month's data as latest.json for the dashboard."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Get all articles grouped by PESTLE category (most recent first)
    # For the dashboard, export the latest month's worth of data
    rows = conn.execute("""
        SELECT a.title, a.summary, a.url, a.source, a.lang,
               a.published, a.pestle_category, a.relevance_score,
               a.published_date
        FROM articles a
        ORDER BY a.published_date DESC, a.created_at DESC
    """).fetchall()

    # Group by category, take top 20 per category for latest.json
    from collections import defaultdict
    by_category = defaultdict(list)
    for r in rows:
        cat = r["pestle_category"]
        if len(by_category[cat]) < 20:
            by_category[cat].append({
                "title": r["title"],
                "summary": r["summary"] or "",
                "url": r["url"],
                "source": r["source"],
                "lang": r["lang"],
                "published": r["published"] or "",
                "relevance_score": r["relevance_score"],
            })

    pestle = {}
    label_map = {k: v["label_ja"] for k, v in PESTLE_QUERIES.items()}
    for cat in ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]:
        arts = by_category.get(cat, [])
        pestle[cat] = {
            "label_ja": label_map.get(cat, cat),
            "count": len(arts),
            "articles": arts,
        }

    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    conn.close()

    output = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "total_fetched": total,
        "feeds_count": 1,
        "pestle": pestle,
    }

    output_path = Path(__file__).parent.parent / "data" / "latest.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n  Exported latest.json ({sum(len(v['articles']) for v in pestle.values())} articles)")


if __name__ == "__main__":
    main()
