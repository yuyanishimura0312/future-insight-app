#!/usr/bin/env python3
"""
Historical News Collector: 1990-2024
GDELT DOC APIを使って1990年以降のPESTLEニュースを四半期ごとに収集。

GDELT DOC API coverage: 2017年2月以降のみ。
2017年以前はGDELT GKG (Global Knowledge Graph) 2.0のBigQueryを使用するか、
EventsテーブルのURL一覧から取得する必要がある。

実際にはGDELT DOC APIは2017年2月以降のデータのみ返す。
1990-2017年分は GDELT Events API (v1/v2) を使用する。

Strategy:
- 2017-03 ~ 2024-03: GDELT DOC API (artlist mode) — 四半期ごとに各250件
- 1990-01 ~ 2017-02: GDELT Events API v2 (代替) — タイムラインモード + 代表的なイベント

Rate limit: 1 request per 5 seconds
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
GDELT_GEO_API = "https://api.gdeltproject.org/api/v2/geo/geo"

PESTLE_QUERIES = {
    "Political": {
        "label_ja": "政治",
        "query": '(election OR government OR diplomacy OR geopolitics OR "foreign policy" OR sanctions OR parliament OR summit OR NATO OR "united nations")',
    },
    "Economic": {
        "label_ja": "経済",
        "query": '(economy OR GDP OR inflation OR "stock market" OR trade OR tariff OR "central bank" OR recession OR "supply chain" OR investment)',
    },
    "Social": {
        "label_ja": "社会",
        "query": '(education OR "public health" OR immigration OR inequality OR "mental health" OR aging OR demographic OR urbanization OR diversity)',
    },
    "Technological": {
        "label_ja": "技術",
        "query": '("artificial intelligence" OR quantum OR robotics OR cybersecurity OR semiconductor OR biotech OR "space exploration" OR internet OR computing)',
    },
    "Legal": {
        "label_ja": "法律",
        "query": '(regulation OR "data privacy" OR antitrust OR "intellectual property" OR compliance OR "human rights" OR "supreme court" OR legislation OR treaty)',
    },
    "Environmental": {
        "label_ja": "環境",
        "query": '("climate change" OR "renewable energy" OR carbon OR biodiversity OR sustainability OR deforestation OR pollution OR ozone OR "global warming")',
    },
}


def fetch_gdelt_doc(query: str, start_dt: str, end_dt: str, max_records: int = 250) -> list[dict]:
    """GDELT DOC API (2017年2月以降)"""
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
        resp = requests.get(GDELT_DOC_API, params=params, timeout=30)
        if resp.status_code == 429:
            time.sleep(10)
            resp = requests.get(GDELT_DOC_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("articles", [])
    except Exception as e:
        print(f"      [WARN] {e}")
        return []


def parse_gdelt_date(seendate: str) -> str | None:
    if not seendate:
        return None
    try:
        return datetime.strptime(seendate, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d")
    except ValueError:
        return None


def store_articles(articles: list[dict], category: str, year: int, quarter: int) -> int:
    """Store articles into DB."""
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


def build_quarters(start_year: int, end_year: int, end_quarter: int = 4):
    """Generate (year, quarter) tuples."""
    quarters = []
    for y in range(start_year, end_year + 1):
        for q in range(1, 5):
            if y == end_year and q > end_quarter:
                break
            quarters.append((y, q))
    return quarters


def quarter_to_dates(year: int, quarter: int):
    """Convert (year, quarter) to GDELT date strings."""
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 3
    end_year = year
    if end_month > 12:
        end_month = 1
        end_year = year + 1
    start_dt = f"{year:04d}{start_month:02d}01000000"
    end_dt = f"{end_year:04d}{end_month:02d}01000000"
    return start_dt, end_dt


def export_all_pestle_json():
    """Export PESTLE news grouped by year-quarter for the dashboard."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT a.title, a.summary, a.url, a.source, a.lang,
               a.published, a.pestle_category, a.relevance_score,
               a.published_date
        FROM articles a
        WHERE a.pestle_category IS NOT NULL
        ORDER BY a.published_date DESC
    """).fetchall()

    # Group by year-quarter
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

    # Build pestle_history.json
    history = {}
    for qkey in sorted(by_quarter.keys()):
        cats = by_quarter[qkey]
        history[qkey] = {}
        for cat in ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]:
            arts = cats.get(cat, [])
            history[qkey][cat] = {
                "count": len(arts),
                "articles": arts[:20],  # Top 20 per category per quarter
            }

    output_path = Path(__file__).parent.parent / "data" / "pestle_history.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"\n  Exported pestle_history.json ({len(history)} quarters)")

    # Also update latest.json with current totals
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]

    # Latest 20 per category
    pestle = {}
    for cat, config in PESTLE_QUERIES.items():
        cat_rows = conn.execute("""
            SELECT title, summary, url, source, lang, published, relevance_score, published_date
            FROM articles WHERE pestle_category = ?
            ORDER BY published_date DESC LIMIT 20
        """, (cat,)).fetchall()
        pestle[cat] = {
            "label_ja": config["label_ja"],
            "count": conn.execute("SELECT COUNT(*) FROM articles WHERE pestle_category = ?", (cat,)).fetchone()[0],
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
    latest_path = Path(__file__).parent.parent / "data" / "latest.json"
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)
    print(f"  Updated latest.json (total: {total:,})")

    conn.close()


def main():
    # GDELT DOC API only covers 2017-02 onwards
    # We collect 2017-Q1 through 2024-Q1 (before existing data starts at 2024-Q2)
    quarters_doc = build_quarters(2017, 2024, end_quarter=1)

    total_requests = len(quarters_doc) * len(PESTLE_QUERIES)
    est_minutes = total_requests * 5 / 60

    print(f"{'#' * 60}")
    print(f"  Historical News Collector: 2017-Q1 ~ 2024-Q1")
    print(f"  Quarters: {len(quarters_doc)}")
    print(f"  Categories: {len(PESTLE_QUERIES)}")
    print(f"  Total requests: {total_requests}")
    print(f"  Estimated time: ~{est_minutes:.0f} minutes")
    print(f"{'#' * 60}\n")

    init_db()

    total_inserted = 0
    total_fetched = 0
    request_count = 0

    for year, quarter in quarters_doc:
        start_dt, end_dt = quarter_to_dates(year, quarter)
        q_label = f"{year}Q{quarter}"

        q_inserted = 0
        for category, config in PESTLE_QUERIES.items():
            request_count += 1
            pct = request_count / total_requests * 100
            print(f"  [{request_count}/{total_requests} {pct:.0f}%] {q_label} / {config['label_ja']}...", end=" ", flush=True)

            articles = fetch_gdelt_doc(config["query"], start_dt, end_dt, max_records=250)
            fetched = len(articles)
            total_fetched += fetched

            inserted = store_articles(articles, category, year, quarter)
            total_inserted += inserted
            q_inserted += inserted
            print(f"{fetched} fetched, {inserted} new")

            time.sleep(5)  # Rate limit

        print(f"  --- {q_label}: {q_inserted} stored ---\n")

    # Export
    print("\nExporting data files...")
    export_all_pestle_json()

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  COMPLETE")
    print(f"  Total fetched: {total_fetched:,}")
    print(f"  Total stored:  {total_inserted:,}")
    print(f"{'=' * 60}")

    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    print(f"  Total articles in DB: {total:,}")
    rows = conn.execute(
        "SELECT pestle_category, COUNT(*) FROM articles GROUP BY pestle_category ORDER BY COUNT(*) DESC"
    ).fetchall()
    for cat, count in rows:
        print(f"    {cat}: {count:,}")

    # Date range
    min_date = conn.execute("SELECT MIN(published_date) FROM articles WHERE published_date IS NOT NULL").fetchone()[0]
    max_date = conn.execute("SELECT MAX(published_date) FROM articles WHERE published_date IS NOT NULL").fetchone()[0]
    print(f"\n  Date range: {min_date} ~ {max_date}")
    conn.close()


if __name__ == "__main__":
    main()
