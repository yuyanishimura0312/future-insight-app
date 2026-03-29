#!/usr/bin/env python3
"""
Update pestle_index.json and pestle_history.json from the SQLite database.
Ensures the web app's historical views reflect all accumulated data.

Run after each daily collection to keep history files in sync with the DB.
"""

import json
import sqlite3
from pathlib import Path
from collections import defaultdict

from db import get_connection, init_db

DATA_DIR = Path(__file__).parent.parent / "data"
PESTLE_CATEGORIES = ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]


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


def load_existing_json(filename: str) -> dict:
    """Load existing JSON file, or return empty dict."""
    path = DATA_DIR / filename
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def export_pestle_index() -> dict:
    """Build pestle_index.json: quarterly counts per PESTLE dimension from DB."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT a.pestle_category, c.date, COUNT(*) as cnt
        FROM articles a
        JOIN collections c ON a.collection_id = c.id
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
    """Build pestle_history.json: quarterly articles per PESTLE dimension from DB."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT a.title, a.summary, a.url, a.source, a.lang,
               a.published, a.published_date, a.pestle_category,
               a.relevance_score, c.date as collection_date
        FROM articles a
        JOIN collections c ON a.collection_id = c.id
        ORDER BY c.date DESC, a.relevance_score DESC
    """).fetchall()
    conn.close()

    # Group by quarter and category
    quarterly = defaultdict(lambda: defaultdict(list))
    for r in rows:
        qk = quarter_key(r["collection_date"])
        if not qk:
            continue
        quarterly[qk][r["pestle_category"]].append({
            "title": r["title"],
            "summary": r["summary"],
            "url": r["url"],
            "source": r["source"],
            "lang": r["lang"],
            "published": r["published"],
            "published_date": r["published_date"],
            "relevance_score": r["relevance_score"],
        })

    # Merge with existing history (keep pre-DB data)
    existing = load_existing_json("pestle_history.json")

    for qk, cats in quarterly.items():
        if qk not in existing:
            existing[qk] = {}
        for cat in PESTLE_CATEGORIES:
            if cat in cats:
                # Merge articles, deduplicate by URL
                old_articles = existing[qk].get(cat, {}).get("articles", [])
                new_articles = cats[cat]
                seen_urls = set()
                merged = []
                for a in new_articles + old_articles:
                    if a["url"] not in seen_urls:
                        seen_urls.add(a["url"])
                        merged.append(a)
                existing[qk][cat] = {
                    "count": len(merged),
                    "articles": merged,
                }

    # Sort by quarter
    sorted_history = dict(sorted(existing.items(), key=lambda x: x[0] if x[0][0].isdigit() else "0000"))

    path = DATA_DIR / "pestle_history.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted_history, f, ensure_ascii=False, indent=2)

    total_articles = sum(
        info.get("count", 0)
        for qk, cats in sorted_history.items()
        if qk[0].isdigit()
        for info in cats.values()
        if isinstance(info, dict)
    )
    quarter_count = len([k for k in sorted_history if k[0].isdigit()])
    print(f"  pestle_history.json: {quarter_count} quarters, {total_articles} articles total")


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
