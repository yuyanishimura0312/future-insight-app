#!/usr/bin/env python3
"""
Retry collector: fill gaps left by rate-limited GDELT requests.

Checks the DB for years with < 1000 articles in 2017-2023,
then fetches missing quarter/category combos with longer wait times.

Rate limit strategy: 10 seconds between requests, exponential backoff on 429.
"""
from __future__ import annotations

import sqlite3
import json
import hashlib
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

from db import get_connection, init_db, DB_PATH

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"

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

CATEGORIES = list(PESTLE_QUERIES.keys())


def fetch_gdelt_with_backoff(query: str, start_dt: str, end_dt: str,
                              max_records: int = 250, max_retries: int = 4) -> list[dict]:
    """Fetch with exponential backoff on rate limits."""
    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": max_records,
        "startdatetime": start_dt,
        "enddatetime": end_dt,
        "format": "json",
        "sort": "datedesc",
    }
    for attempt in range(max_retries):
        try:
            resp = requests.get(GDELT_DOC_API, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 15 * (2 ** attempt)  # 15, 30, 60, 120 seconds
                print(f"[429 retry in {wait}s]", end=" ", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data.get("articles", [])
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 10 * (2 ** attempt)
                print(f"[ERR retry in {wait}s]", end=" ", flush=True)
                time.sleep(wait)
            else:
                print(f"[FAIL: {e}]", end=" ", flush=True)
    return []


def parse_gdelt_date(seendate: str) -> str | None:
    if not seendate:
        return None
    try:
        return datetime.strptime(seendate, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d")
    except ValueError:
        return None


def store_articles(articles: list[dict], category: str, year: int, quarter: int) -> int:
    conn = get_connection()
    month = (quarter - 1) * 3 + 1
    date_key = f"{year:04d}-{month:02d}-01"
    collected_at = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """INSERT OR IGNORE INTO collections
           (date, collected_at, total_fetched, feeds_count, total_selected)
           VALUES (?, ?, ?, ?, ?)""",
        (date_key, collected_at, len(articles), 1, 0)
    )
    row = conn.execute("SELECT id FROM collections WHERE date = ?", (date_key,)).fetchone()
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
        lang_code = "ja" if lang.lower() in ("japanese",) else "en"
        domain = article.get("domain", "")

        try:
            conn.execute(
                """INSERT OR IGNORE INTO articles
                   (collection_id, url_hash, title, summary, url, source, lang,
                    published, published_date, pestle_category, relevance_score)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (collection_id, url_hash, title, "", url, domain, lang_code,
                 article.get("seendate", ""), pub_date, category, 1.0)
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.execute(
        "UPDATE collections SET total_selected = total_selected + ? WHERE id = ?",
        (inserted, collection_id)
    )
    conn.commit()
    conn.close()
    return inserted


def quarter_to_dates(year: int, quarter: int):
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 3
    end_year = year
    if end_month > 12:
        end_month = 1
        end_year = year + 1
    return (f"{year:04d}{start_month:02d}01000000",
            f"{end_year:04d}{end_month:02d}01000000")


def find_gaps() -> list[tuple[int, int, str]]:
    """Find quarter/category combos with few or no articles."""
    conn = get_connection()

    gaps = []
    for year in range(2017, 2024):
        for quarter in range(1, 5):
            # Determine date range for this quarter
            start_month = (quarter - 1) * 3 + 1
            end_month = start_month + 2
            start_date = f"{year:04d}-{start_month:02d}-01"
            end_date = f"{year:04d}-{end_month:02d}-31"

            for cat in CATEGORIES:
                count = conn.execute("""
                    SELECT COUNT(*) FROM articles
                    WHERE pestle_category = ?
                      AND published_date >= ? AND published_date <= ?
                """, (cat, start_date, end_date)).fetchone()[0]

                # If fewer than 50 articles for this quarter/category, it's a gap
                if count < 50:
                    gaps.append((year, quarter, cat))

    conn.close()
    return gaps


def regenerate_pestle_files():
    """Regenerate pestle_history.json, pestle_index.json, and latest.json."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT title, summary, url, source, lang, published,
               pestle_category, relevance_score, published_date
        FROM articles
        WHERE pestle_category IS NOT NULL
        ORDER BY published_date DESC
    """).fetchall()

    by_quarter = defaultdict(lambda: defaultdict(list))
    for r in rows:
        pd = r["published_date"] or ""
        if len(pd) >= 7:
            y = int(pd[:4])
            m = int(pd[5:7])
            q = (m - 1) // 3 + 1
            qkey = f"{y}Q{q}"
        else:
            qkey = "unknown"
        cat = r["pestle_category"]
        by_quarter[qkey][cat].append({
            "title": r["title"],
            "summary": r["summary"] or "",
            "url": r["url"],
            "source": r["source"],
            "lang": r["lang"],
            "published": r["published"] or "",
            "relevance_score": r["relevance_score"],
            "published_date": r["published_date"] or "",
        })

    cats = CATEGORIES
    data_dir = Path(__file__).parent.parent / "data"

    # pestle_history.json
    history = {}
    for qkey in sorted(by_quarter.keys()):
        if qkey == "unknown":
            continue
        history[qkey] = {}
        for cat in cats:
            arts = by_quarter[qkey].get(cat, [])
            history[qkey][cat] = {"count": len(arts), "articles": arts[:20]}

    with open(data_dir / "pestle_history.json", "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"  pestle_history.json: {len(history)} quarters")

    # pestle_index.json
    index = {}
    for qkey, cats_data in history.items():
        index[qkey] = {cat: cats_data[cat]["count"] for cat in cats}
    with open(data_dir / "pestle_index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"  pestle_index.json: {len(index)} quarters")

    # latest.json
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    pestle = {}
    label_map = {k: v["label_ja"] for k, v in PESTLE_QUERIES.items()}
    for cat in cats:
        cat_rows = conn.execute("""
            SELECT title, summary, url, source, lang, published, relevance_score, published_date
            FROM articles WHERE pestle_category = ?
            ORDER BY published_date DESC LIMIT 20
        """, (cat,)).fetchall()
        cat_total = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE pestle_category = ?", (cat,)
        ).fetchone()[0]
        pestle[cat] = {
            "label_ja": label_map.get(cat, cat),
            "count": cat_total,
            "articles": [{
                "title": r["title"], "summary": r["summary"] or "", "url": r["url"],
                "source": r["source"], "lang": r["lang"], "published": r["published"] or "",
                "relevance_score": r["relevance_score"], "published_date": r["published_date"] or "",
            } for r in cat_rows]
        }
    latest = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "total_fetched": total,
        "feeds_count": 1,
        "pestle": pestle,
    }
    with open(data_dir / "latest.json", "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)
    print(f"  latest.json: total {total:,} articles")
    conn.close()


def main():
    init_db()

    print("Scanning for gaps in 2017-2023 data...\n")
    gaps = find_gaps()

    if not gaps:
        print("No gaps found! All quarter/category combos have >= 50 articles.")
        return

    print(f"Found {len(gaps)} gaps to fill")
    est_minutes = len(gaps) * 12 / 60  # ~12 seconds per request (10s wait + fetch)
    print(f"Estimated time: ~{est_minutes:.0f} minutes\n")

    total_inserted = 0
    for i, (year, quarter, category) in enumerate(gaps, 1):
        config = PESTLE_QUERIES[category]
        start_dt, end_dt = quarter_to_dates(year, quarter)
        q_label = f"{year}Q{quarter}"

        print(f"  [{i}/{len(gaps)}] {q_label} / {config['label_ja']}...", end=" ", flush=True)

        articles = fetch_gdelt_with_backoff(config["query"], start_dt, end_dt)
        fetched = len(articles)
        inserted = store_articles(articles, category, year, quarter) if articles else 0
        total_inserted += inserted
        print(f"{fetched} fetched, {inserted} new")

        # Generous wait between requests
        time.sleep(10)

    print(f"\nTotal new articles: {total_inserted:,}")

    # Regenerate JSON files
    print("\nRegenerating data files...")
    regenerate_pestle_files()

    # Final stats
    conn = get_connection()
    print(f"\n=== Final DB stats ===")
    rows = conn.execute("""
        SELECT substr(published_date, 1, 4) as year, COUNT(*) as cnt
        FROM articles WHERE published_date IS NOT NULL
        GROUP BY year ORDER BY year
    """).fetchall()
    for y, c in rows:
        print(f"  {y}: {c:,}")
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    print(f"  TOTAL: {total:,}")
    conn.close()


if __name__ == "__main__":
    main()
