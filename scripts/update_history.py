#!/usr/bin/env python3
"""
Update pestle_index.json and pestle_history.json from the SQLite database.
Ensures the web app's historical views reflect all accumulated data.

Run after each daily collection to keep history files in sync with the DB.

Handles ~56,000+ articles spanning 1990-2026. pestle_history.json is capped
at 50 articles per category per quarter (sorted by relevance_score DESC)
to stay under ~10MB for GitHub Pages frontend loading.
"""

import json
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

from db import get_connection, init_db

DATA_DIR = Path(__file__).parent.parent / "data"
PESTLE_CATEGORIES = ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]

# Cap articles per category per quarter to keep pestle_history.json under ~10MB
MAX_ARTICLES_PER_CATEGORY_PER_QUARTER = 50


def quarter_key(date_str: str) -> str | None:
    """Convert YYYY-MM-DD to YYYYQn format."""
    if not date_str or len(date_str) < 7:
        return None
    try:
        year = date_str[:4]
        month = int(date_str[5:7])
        q = (month - 1) // 3 + 1
        return f"{year}Q{q}"
    except (ValueError, IndexError):
        return None


def progress(current: int, total: int, label: str = "") -> None:
    """Print a simple progress indicator to stderr."""
    pct = current * 100 // total if total > 0 else 100
    bar_len = 30
    filled = bar_len * current // total if total > 0 else bar_len
    bar = "#" * filled + "-" * (bar_len - filled)
    sys.stderr.write(f"\r  [{bar}] {pct:3d}% ({current}/{total}) {label}")
    if current >= total:
        sys.stderr.write("\n")
    sys.stderr.flush()


def load_existing_json(filename: str) -> dict:
    """Load existing JSON file, or return empty dict."""
    path = DATA_DIR / filename
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def export_pestle_index() -> dict:
    """Build pestle_index.json: quarterly counts per PESTLE dimension from DB."""
    print("  Building pestle_index.json ...")
    conn = get_connection()
    rows = conn.execute("""
        SELECT a.pestle_category, c.date, COUNT(*) as cnt
        FROM articles a
        JOIN collections c ON a.collection_id = c.id
        WHERE c.date >= '1990-01-01' AND c.date <= '2026-12-31'
        GROUP BY a.pestle_category, c.date
        ORDER BY c.date
    """).fetchall()
    conn.close()

    # Aggregate by quarter
    quarterly = defaultdict(lambda: defaultdict(int))
    for category, date, cnt in rows:
        qk = quarter_key(date)
        if qk:
            quarterly[qk][category] += cnt

    # Merge with existing index (keep historical data from before DB existed)
    existing = load_existing_json("pestle_index.json")

    for qk, cats in quarterly.items():
        if qk not in existing:
            existing[qk] = {}
        for cat in PESTLE_CATEGORIES:
            # DB data takes priority for quarters where we have it
            if cat in cats:
                existing[qk][cat] = cats[cat]
            elif cat not in existing[qk]:
                existing[qk][cat] = 0

    # Sort by quarter key
    sorted_index = dict(sorted(existing.items()))

    path = DATA_DIR / "pestle_index.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted_index, f, ensure_ascii=False, indent=2)

    print(f"  pestle_index.json: {len(sorted_index)} quarters")
    return sorted_index


def export_pestle_history() -> None:
    """Build pestle_history.json: quarterly articles per PESTLE dimension from DB.

    Processes data quarter-by-quarter to avoid loading all ~56K articles
    into memory at once. Limits to top 50 articles per category per quarter
    (by relevance_score) to keep file size under ~10MB.
    """
    print("  Building pestle_history.json ...")
    conn = get_connection()

    # Get all distinct quarters from the DB (1990-2026)
    quarter_dates = conn.execute("""
        SELECT DISTINCT c.date
        FROM collections c
        WHERE c.date >= '1990-01-01' AND c.date <= '2026-12-31'
        ORDER BY c.date
    """).fetchall()

    # Build list of unique quarters
    quarters_set = {}
    for (d,) in quarter_dates:
        qk = quarter_key(d)
        if qk and qk not in quarters_set:
            quarters_set[qk] = []
        if qk:
            quarters_set[qk].append(d)

    quarters_list = sorted(quarters_set.keys())
    total_quarters = len(quarters_list)
    print(f"  Found {total_quarters} quarters in DB")

    # Load existing history for merging pre-DB data
    existing = load_existing_json("pestle_history.json")

    total_articles_included = 0
    total_articles_in_db = 0

    # Process quarter by quarter to limit memory usage
    for i, qk in enumerate(quarters_list):
        progress(i + 1, total_quarters, qk)

        # Fetch articles for this quarter's dates, already sorted by relevance
        dates = quarters_set[qk]
        placeholders = ",".join("?" for _ in dates)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(f"""
            SELECT a.title, a.summary, a.url, a.source, a.lang,
                   a.published, a.published_date, a.pestle_category,
                   a.relevance_score
            FROM articles a
            JOIN collections c ON a.collection_id = c.id
            WHERE c.date IN ({placeholders})
            ORDER BY a.pestle_category, a.relevance_score DESC
        """, dates).fetchall()

        # Group by category
        by_category = defaultdict(list)
        for r in rows:
            by_category[r["pestle_category"]].append({
                "title": r["title"],
                "summary": r["summary"],
                "url": r["url"],
                "source": r["source"],
                "lang": r["lang"],
                "published": r["published"],
                "published_date": r["published_date"],
                "relevance_score": r["relevance_score"],
            })

        if qk not in existing:
            existing[qk] = {}

        for cat in PESTLE_CATEGORIES:
            new_articles = by_category.get(cat, [])
            old_articles = existing[qk].get(cat, {}).get("articles", [])

            # Merge and deduplicate by URL
            seen_urls = set()
            merged = []
            # New articles are already sorted by relevance_score DESC
            for a in new_articles + old_articles:
                if a["url"] not in seen_urls:
                    seen_urls.add(a["url"])
                    merged.append(a)

            # Sort merged list by relevance_score descending
            merged.sort(key=lambda x: x.get("relevance_score", 0) or 0, reverse=True)

            total_count = len(merged)
            total_articles_in_db += total_count

            # Cap at MAX_ARTICLES to control file size
            capped = merged[:MAX_ARTICLES_PER_CATEGORY_PER_QUARTER]
            total_articles_included += len(capped)

            existing[qk][cat] = {
                "count": total_count,  # true count (not capped)
                "articles": capped,    # top N articles only
            }

    conn.close()

    # Sort by quarter
    sorted_history = dict(sorted(existing.items(), key=lambda x: x[0] if x[0][0].isdigit() else "0000"))

    path = DATA_DIR / "pestle_history.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted_history, f, ensure_ascii=False, indent=2)

    # Report file size
    file_size_mb = path.stat().st_size / (1024 * 1024)

    quarter_count = len([k for k in sorted_history if k[0].isdigit()])
    print(f"  pestle_history.json: {quarter_count} quarters, "
          f"{total_articles_included} articles included "
          f"(of {total_articles_in_db} total, capped at {MAX_ARTICLES_PER_CATEGORY_PER_QUARTER}/cat/quarter)")
    print(f"  File size: {file_size_mb:.1f} MB")


def export_papers_stats() -> None:
    """Update papers_stats.json from DB."""
    conn = get_connection()

    total = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    rows = conn.execute(
        "SELECT field, COUNT(*) as cnt FROM papers GROUP BY field ORDER BY cnt DESC"
    ).fetchall()
    by_field = {r[0]: r[1] for r in rows}

    conn.close()

    stats = {"total": total, "by_field": by_field}
    path = DATA_DIR / "papers_stats.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"  papers_stats.json: {total} papers across {len(by_field)} fields")


def main():
    print("=== Updating history from database ===")
    init_db()
    export_pestle_index()
    export_pestle_history()
    export_papers_stats()
    print("=== History update complete ===")


if __name__ == "__main__":
    main()
