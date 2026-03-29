#!/usr/bin/env python3
"""
Multi-source historical news collector for PESTLE analysis.

Sources:
  1. The Guardian API (primary) — 1999+, 5000 req/day, 200 results/req
  2. GDELT DOC API (secondary) — 2017+, rate-limited

Target: ~1000 articles per year for 2017-2023
Guardian API handles the bulk; GDELT fills remaining gaps.

Usage:
  python3 collect_news_multisource.py [--guardian-key KEY] [--years 2017-2023]
"""
from __future__ import annotations

import sqlite3
import json
import hashlib
import time
import argparse
import requests
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

from db import get_connection, init_db, DB_PATH

# === Guardian API Config ===
GUARDIAN_API = "https://content.guardianapis.com/search"

# PESTLE → Guardian section mapping
GUARDIAN_PESTLE_MAP = {
    "Political": {
        "sections": ["politics", "world"],
        "query": "government OR election OR diplomacy OR sanctions OR summit",
    },
    "Economic": {
        "sections": ["business"],
        "query": "economy OR inflation OR trade OR investment OR GDP OR recession",
    },
    "Social": {
        "sections": ["society", "education"],
        "query": "health OR immigration OR inequality OR education OR demographic",
    },
    "Technological": {
        "sections": ["technology", "science"],
        "query": "AI OR robotics OR cybersecurity OR biotech OR quantum OR semiconductor",
    },
    "Legal": {
        "sections": ["law"],
        "query": "regulation OR privacy OR antitrust OR legislation OR human rights",
    },
    "Environmental": {
        "sections": ["environment"],
        "query": "climate OR renewable OR carbon OR biodiversity OR pollution OR sustainability",
    },
}

# === GDELT Config ===
GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_PESTLE_QUERIES = {
    "Political": '(election OR government OR diplomacy OR geopolitics OR "foreign policy" OR sanctions OR parliament)',
    "Economic": '(economy OR GDP OR inflation OR trade OR tariff OR "central bank" OR recession OR investment)',
    "Social": '(education OR "public health" OR immigration OR inequality OR aging OR demographic)',
    "Technological": '("artificial intelligence" OR quantum OR robotics OR cybersecurity OR semiconductor OR biotech)',
    "Legal": '(regulation OR "data privacy" OR antitrust OR compliance OR "human rights" OR legislation)',
    "Environmental": '("climate change" OR "renewable energy" OR carbon OR biodiversity OR sustainability OR pollution)',
}

CATEGORIES = ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]
LABEL_JA = {
    "Political": "政治", "Economic": "経済", "Social": "社会",
    "Technological": "技術", "Legal": "法律", "Environmental": "環境",
}


def fetch_guardian(api_key: str, section: str, query: str,
                   from_date: str, to_date: str, page: int = 1,
                   page_size: int = 200) -> dict:
    """Fetch from Guardian API. Returns response dict."""
    params = {
        "section": section,
        "q": query,
        "from-date": from_date,
        "to-date": to_date,
        "page": page,
        "page-size": page_size,
        "order-by": "relevance",
        "show-fields": "trailText",
        "api-key": api_key,
    }
    try:
        resp = requests.get(GUARDIAN_API, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json().get("response", {})
    except Exception as e:
        print(f"    [Guardian ERR] {e}")
        return {}


def fetch_gdelt_with_backoff(query: str, start_dt: str, end_dt: str,
                              max_records: int = 250) -> list[dict]:
    """Fetch from GDELT with exponential backoff."""
    params = {
        "query": query, "mode": "artlist", "maxrecords": max_records,
        "startdatetime": start_dt, "enddatetime": end_dt,
        "format": "json", "sort": "datedesc",
    }
    for attempt in range(3):
        try:
            resp = requests.get(GDELT_DOC_API, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 20 * (2 ** attempt)
                print(f"[429, wait {wait}s]", end=" ", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json().get("articles", [])
        except Exception as e:
            if attempt < 2:
                time.sleep(15)
            else:
                print(f"[GDELT FAIL: {e}]", end=" ", flush=True)
    return []


def store_guardian_articles(articles: list[dict], category: str, year: int) -> int:
    """Store Guardian articles into the DB."""
    conn = get_connection()
    date_key = f"{year}-01-01"
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
        url = article.get("webUrl", "")
        title = article.get("webTitle", "").strip()
        if not url or not title:
            continue

        url_hash = hashlib.sha256(url.encode()).hexdigest()
        pub_date = article.get("webPublicationDate", "")[:10]  # YYYY-MM-DD
        summary = ""
        fields = article.get("fields", {})
        if fields:
            summary = fields.get("trailText", "")

        try:
            conn.execute(
                """INSERT OR IGNORE INTO articles
                   (collection_id, url_hash, title, summary, url, source, lang,
                    published, published_date, pestle_category, relevance_score)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (collection_id, url_hash, title, summary, url,
                 "theguardian.com", "en", pub_date, pub_date, category, 1.0)
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


def store_gdelt_articles(articles: list[dict], category: str, year: int, quarter: int) -> int:
    """Store GDELT articles into the DB."""
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
        pub_date = None
        sd = article.get("seendate", "")
        if sd:
            try:
                pub_date = datetime.strptime(sd, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d")
            except ValueError:
                pass
        lang = article.get("language", "English")
        lang_code = "ja" if lang.lower() in ("japanese",) else "en"

        try:
            conn.execute(
                """INSERT OR IGNORE INTO articles
                   (collection_id, url_hash, title, summary, url, source, lang,
                    published, published_date, pestle_category, relevance_score)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (collection_id, url_hash, title, "", url,
                 article.get("domain", ""), lang_code,
                 sd, pub_date, category, 1.0)
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


def get_year_category_counts() -> dict[int, dict[str, int]]:
    """Get current article counts per year per category."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT substr(published_date, 1, 4) as year, pestle_category, COUNT(*) as cnt
        FROM articles
        WHERE published_date IS NOT NULL
        GROUP BY year, pestle_category
    """).fetchall()
    conn.close()

    counts = defaultdict(lambda: defaultdict(int))
    for y, cat, cnt in rows:
        try:
            counts[int(y)][cat] = cnt
        except ValueError:
            pass
    return counts


def collect_from_guardian(api_key: str, year: int, category: str,
                          target_per_category: int = 170) -> int:
    """Collect articles for one year/category from Guardian API."""
    config = GUARDIAN_PESTLE_MAP[category]
    from_date = f"{year}-01-01"
    to_date = f"{year}-12-31"
    total_inserted = 0

    for section in config["sections"]:
        if total_inserted >= target_per_category:
            break

        remaining = target_per_category - total_inserted
        # Fetch up to 2 pages per section
        for page in range(1, 3):
            if remaining <= 0:
                break
            page_size = min(200, remaining)

            data = fetch_guardian(api_key, section, config["query"],
                                 from_date, to_date, page, page_size)
            results = data.get("results", [])
            if not results:
                break

            inserted = store_guardian_articles(results, category, year)
            total_inserted += inserted
            remaining -= inserted

            # Small delay to stay within rate limits (12 req/sec is generous)
            time.sleep(0.2)

    return total_inserted


def collect_from_gdelt(year: int, category: str, target: int = 170) -> int:
    """Collect articles for one year/category from GDELT."""
    query = GDELT_PESTLE_QUERIES[category]
    total_inserted = 0

    for quarter in range(1, 5):
        if total_inserted >= target:
            break

        start_month = (quarter - 1) * 3 + 1
        end_month = start_month + 3
        end_year = year
        if end_month > 12:
            end_month = 1
            end_year = year + 1
        start_dt = f"{year:04d}{start_month:02d}01000000"
        end_dt = f"{end_year:04d}{end_month:02d}01000000"

        articles = fetch_gdelt_with_backoff(query, start_dt, end_dt)
        inserted = store_gdelt_articles(articles, category, year, quarter)
        total_inserted += inserted

        time.sleep(12)  # Generous rate limiting for GDELT

    return total_inserted


def regenerate_pestle_files():
    """Regenerate all PESTLE JSON files from DB."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT title, summary, url, source, lang, published,
               pestle_category, relevance_score, published_date
        FROM articles WHERE pestle_category IS NOT NULL
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
            continue
        by_quarter[qkey][r["pestle_category"]].append({
            "title": r["title"], "summary": r["summary"] or "",
            "url": r["url"], "source": r["source"], "lang": r["lang"],
            "published": r["published"] or "",
            "relevance_score": r["relevance_score"],
            "published_date": r["published_date"] or "",
        })

    data_dir = Path(__file__).parent.parent / "data"

    # pestle_history.json
    history = {}
    for qkey in sorted(by_quarter.keys()):
        history[qkey] = {}
        for cat in CATEGORIES:
            arts = by_quarter[qkey].get(cat, [])
            history[qkey][cat] = {"count": len(arts), "articles": arts[:20]}

    with open(data_dir / "pestle_history.json", "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    # pestle_index.json
    index = {}
    for qkey, cats_data in history.items():
        index[qkey] = {cat: cats_data[cat]["count"] for cat in CATEGORIES}
    with open(data_dir / "pestle_index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    # latest.json
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    pestle = {}
    for cat in CATEGORIES:
        cat_rows = conn.execute("""
            SELECT title, summary, url, source, lang, published, relevance_score, published_date
            FROM articles WHERE pestle_category = ?
            ORDER BY published_date DESC LIMIT 20
        """, (cat,)).fetchall()
        cat_total = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE pestle_category = ?", (cat,)
        ).fetchone()[0]
        pestle[cat] = {
            "label_ja": LABEL_JA[cat], "count": cat_total,
            "articles": [{
                "title": r["title"], "summary": r["summary"] or "", "url": r["url"],
                "source": r["source"], "lang": r["lang"], "published": r["published"] or "",
                "relevance_score": r["relevance_score"],
                "published_date": r["published_date"] or "",
            } for r in cat_rows]
        }
    latest = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "total_fetched": total, "feeds_count": 1, "pestle": pestle,
    }
    with open(data_dir / "latest.json", "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)

    conn.close()
    print(f"  Regenerated: pestle_history.json ({len(history)} quarters), "
          f"pestle_index.json, latest.json (total: {total:,})")


def main():
    parser = argparse.ArgumentParser(description="Multi-source PESTLE news collector")
    parser.add_argument("--guardian-key", default="test",
                        help="Guardian API key (default: test)")
    parser.add_argument("--years", default="2017-2023",
                        help="Year range (default: 2017-2023)")
    parser.add_argument("--target", type=int, default=1000,
                        help="Target articles per year (default: 1000)")
    parser.add_argument("--skip-gdelt", action="store_true",
                        help="Skip GDELT and use Guardian only")
    args = parser.parse_args()

    start_year, end_year = map(int, args.years.split("-"))
    years = list(range(start_year, end_year + 1))
    target_per_year = args.target
    target_per_cat = target_per_year // len(CATEGORIES)  # ~167 per category

    print(f"{'#' * 60}")
    print(f"  Multi-Source PESTLE News Collector")
    print(f"  Years: {start_year}-{end_year} ({len(years)} years)")
    print(f"  Target: {target_per_year} articles/year ({target_per_cat}/category)")
    print(f"  Sources: Guardian API" + ("" if args.skip_gdelt else " + GDELT"))
    print(f"{'#' * 60}\n")

    init_db()

    # Check current state
    counts = get_year_category_counts()
    print("Current article counts per year:")
    for y in years:
        total = sum(counts[y].values())
        detail = ", ".join(f"{c[:3]}:{counts[y].get(c,0)}" for c in CATEGORIES)
        print(f"  {y}: {total:,} ({detail})")
    print()

    grand_total = 0

    for year in years:
        year_total = sum(counts[year].values())
        if year_total >= target_per_year:
            print(f"[{year}] Already has {year_total:,} articles, skipping.\n")
            continue

        print(f"[{year}] Current: {year_total:,}, need ~{target_per_year - year_total} more")
        year_inserted = 0

        for category in CATEGORIES:
            current = counts[year].get(category, 0)
            need = max(0, target_per_cat - current)
            if need == 0:
                print(f"  {LABEL_JA[category]}: already {current}, skip")
                continue

            # Phase 1: Guardian API
            print(f"  {LABEL_JA[category]}: need {need}...", end=" ", flush=True)
            guardian_inserted = collect_from_guardian(
                args.guardian_key, year, category, target_per_category=need
            )
            print(f"Guardian:{guardian_inserted}", end="", flush=True)

            # Phase 2: GDELT for remaining (if enabled and still need more)
            gdelt_inserted = 0
            remaining = need - guardian_inserted
            if remaining > 50 and not args.skip_gdelt and year >= 2017:
                print(f", GDELT...", end=" ", flush=True)
                gdelt_inserted = collect_from_gdelt(year, category, target=remaining)
                print(f"{gdelt_inserted}", end="", flush=True)

            total_cat = guardian_inserted + gdelt_inserted
            year_inserted += total_cat
            print(f" = {total_cat} new")

        grand_total += year_inserted
        print(f"  --- {year}: +{year_inserted} articles ---\n")

    # Regenerate JSON files
    print("Regenerating data files...")
    regenerate_pestle_files()

    # Final summary
    print(f"\n{'=' * 60}")
    print(f"  COMPLETE — {grand_total:,} new articles added")
    conn = get_connection()
    rows = conn.execute("""
        SELECT substr(published_date, 1, 4) as year, COUNT(*) as cnt
        FROM articles WHERE published_date IS NOT NULL
        GROUP BY year ORDER BY year
    """).fetchall()
    for y, c in rows:
        marker = " <<<" if start_year <= int(y) <= end_year else ""
        print(f"  {y}: {c:,}{marker}")
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    print(f"  TOTAL: {total:,}")
    conn.close()


if __name__ == "__main__":
    main()
