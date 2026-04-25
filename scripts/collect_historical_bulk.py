#!/usr/bin/env python3
"""
Bulk Historical PESTLE Collector
1900-1989年の歴史的PESTLE記事を並列で一気に収集する。

- Claude APIで歴史的出来事を生成（カテゴリ別バッチ）
- 5年分を並列処理（Claude APIレート制限考慮）
- 各カテゴリ最大200件（近代）〜50件（19世紀初頭）
- 既に収集済みの年はスキップ
"""

import json
import hashlib
import sqlite3
import time
import sys
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

from db import get_connection, init_db

DATA_DIR = Path(__file__).parent.parent / "data"

PESTLE_CATEGORIES = {
    "Political":      "政治",
    "Economic":       "経済",
    "Social":         "社会",
    "Technological":  "技術",
    "Legal":          "法律",
    "Environmental":  "環境",
}

# How many concurrent year-processes to run
MAX_WORKERS = 10
# Minimum articles to consider a year "done"
MIN_ARTICLES_THRESHOLD = 1000


def get_collected_years() -> dict[int, int]:
    """Get years already collected with their article counts."""
    conn = get_connection()
    init_db()
    rows = conn.execute(
        "SELECT date, total_selected FROM collections WHERE date LIKE '%-01-01'"
    ).fetchall()
    conn.close()
    result = {}
    for date_str, count in rows:
        try:
            year = int(date_str.split("-")[0])
            result[year] = count
        except (ValueError, IndexError):
            pass
    return result


def collect_category(year: int, category: str, label_ja: str, target: int) -> list[dict]:
    """Collect events for one category in one year using Claude API."""
    client = anthropic.Anthropic()
    all_events = []
    remaining = target
    batch_num = 0

    while remaining > 0 and batch_num < 3:
        batch_size = min(remaining, 100)
        batch_num += 1

        exclude_titles = ""
        if all_events:
            existing = "\n".join(f"- {e['title']}" for e in all_events[:30])
            exclude_titles = f"\n\nDo NOT repeat these already-listed events:\n{existing}"

        prompt = f"""List {batch_size} important real events/developments in the category "{category}" ({label_ja}) that occurred in the year {year}.

For each event, provide:
- title: A concise headline (like a news headline at the time)
- summary: 1-2 sentence description of the event and its significance
- source: The primary organization/publication that would have reported it
- published_date: The approximate date (YYYY-MM-DD format)

IMPORTANT:
- Only include REAL, historically documented events from {year}
- Include both major and minor events — international AND domestic (Japan, US, Europe, Asia, Africa, Latin America)
- Cover diverse aspects within {category}
- If fewer than {batch_size} real events exist, include as many as are real
- Be historically accurate — do not fabricate events{exclude_titles}

Respond ONLY with valid JSON array:
[
  {{"title": "...", "summary": "...", "source": "...", "published_date": "YYYY-MM-DD"}},
  ...
]"""

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=16384,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            if "```" in text:
                text = text.split("```json")[-1].split("```")[0] if "```json" in text else text.split("```")[1].split("```")[0]
            text = text.strip()

            # Fix truncated JSON
            if not text.endswith("]"):
                last_brace = text.rfind("}")
                if last_brace > 0:
                    text = text[:last_brace + 1] + "]"

            events = json.loads(text)
            if isinstance(events, list):
                all_events.extend(events)
                remaining -= len(events)
                if len(events) < batch_size * 0.5:
                    break
        except json.JSONDecodeError:
            remaining = min(remaining, 50)
            continue
        except Exception as e:
            print(f"      [{year}] {category} batch {batch_num} error: {e}")
            break
        time.sleep(0.5)

    return all_events


def process_year(year: int) -> dict:
    """Process a single year: collect all 6 PESTLE categories."""
    # Target 200/category for all years = ~1,200/year
    target_per_cat = 200

    results = {}
    for cat, label_ja in PESTLE_CATEGORIES.items():
        events_raw = collect_category(year, cat, label_ja, target=target_per_cat)

        articles = []
        seen_titles = set()
        for ev in events_raw:
            title = ev.get("title", "").strip()
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            articles.append({
                "title": title,
                "summary": ev.get("summary", ""),
                "url": f"https://historical-reference/{year}/{cat}/{hashlib.md5(title.encode()).hexdigest()[:8]}",
                "source": ev.get("source", "Historical Record"),
                "lang": "en",
                "published": "",
                "published_date": ev.get("published_date", f"{year}-06-15"),
                "relevance_score": 1.0,
            })
        results[cat] = articles

    return results


def store_articles(articles_by_cat: dict[str, list[dict]], year: int) -> int:
    """Store collected articles into the SQLite database."""
    conn = get_connection()

    date_key = f"{year}-01-01"
    collected_at = datetime.now(timezone.utc).isoformat()
    total_articles = sum(len(arts) for arts in articles_by_cat.values())

    conn.execute(
        """INSERT OR IGNORE INTO collections
           (date, collected_at, total_fetched, feeds_count, total_selected)
           VALUES (?, ?, ?, ?, ?)""",
        (date_key, collected_at, total_articles, 1, 0)
    )
    row = conn.execute("SELECT id FROM collections WHERE date = ?", (date_key,)).fetchone()
    collection_id = row[0]

    inserted = 0
    for cat, articles in articles_by_cat.items():
        for a in articles:
            url_hash = hashlib.sha256(a["url"].encode()).hexdigest()
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO articles
                       (collection_id, url_hash, title, summary, url, source, lang,
                        published, published_date, pestle_category, relevance_score)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (collection_id, url_hash, a["title"], a["summary"],
                     a["url"], a["source"], a["lang"],
                     a.get("published", ""), a["published_date"],
                     cat, a["relevance_score"])
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


def process_and_store_year(year: int) -> tuple[int, int, float]:
    """Process a year and store results. Returns (year, article_count, duration)."""
    start_time = time.time()
    try:
        articles = process_year(year)
        inserted = store_articles(articles, year)
        total = sum(len(arts) for arts in articles.values())
        duration = time.time() - start_time
        cats = {PESTLE_CATEGORIES[cat]: len(arts) for cat, arts in articles.items()}
        print(f"  [{year}] {total} articles ({inserted} new) in {duration:.0f}s — {cats}")
        return (year, total, duration)
    except Exception as e:
        duration = time.time() - start_time
        print(f"  [{year}] ERROR: {e} ({duration:.0f}s)")
        return (year, 0, duration)


def main():
    init_db()

    # Determine which years need collection
    collected = get_collected_years()
    all_years = list(range(1900, 1990))  # 1900-1989

    # Skip years that already have enough articles
    todo_years = [y for y in all_years if collected.get(y, 0) < MIN_ARTICLES_THRESHOLD]

    print(f"{'=' * 70}")
    print(f"  Bulk Historical PESTLE Collector")
    print(f"  Total years to process: {len(todo_years)} / {len(all_years)}")
    print(f"  Already done: {len(all_years) - len(todo_years)}")
    print(f"  Parallel workers: {MAX_WORKERS}")
    print(f"  Years: {todo_years[:10]}{'...' if len(todo_years) > 10 else ''}")
    print(f"{'=' * 70}\n")

    if not todo_years:
        print("  All years already collected! Nothing to do.")
        return

    total_articles = 0
    total_start = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_and_store_year, y): y for y in todo_years}
        for future in as_completed(futures):
            year, count, duration = future.result()
            total_articles += count

    elapsed = time.time() - total_start
    print(f"\n{'=' * 70}")
    print(f"  COMPLETE")
    print(f"  Years processed: {len(todo_years)}")
    print(f"  Total articles: {total_articles}")
    print(f"  Total time: {elapsed / 60:.1f} minutes")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
