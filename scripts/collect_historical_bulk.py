#!/usr/bin/env python3
"""
Bulk Historical PESTLE Collector (1990-2025)
各年2,000件（6カテゴリ x 334件）の歴史データを一括収集しDBに格納する。

- 1990-2016: Claude APIで歴史的重要イベントを生成（年あたり6回APIコール）
- 2017-2025: GDELT DOC APIから実ニュースを月別取得

Usage:
  python3 collect_historical_bulk.py --start 1990 --end 2025
  python3 collect_historical_bulk.py --year 2005
  python3 collect_historical_bulk.py --start 2017 --end 2025 --source gdelt
"""

import json
import hashlib
import os
import sys
import sqlite3
import time
import argparse
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

# Ensure API key is available
api_key = os.environ.get("ANTHROPIC_API_KEY")
if not api_key:
    import subprocess
    try:
        api_key = subprocess.check_output(
            ["security", "find-generic-password", "-s", "ANTHROPIC_API_KEY", "-a", "anthropic", "-w"],
            text=True
        ).strip()
        os.environ["ANTHROPIC_API_KEY"] = api_key
    except Exception:
        pass

import anthropic

sys.path.insert(0, str(Path(__file__).parent))
from db import get_connection, init_db

DATA_DIR = Path(__file__).parent.parent / "data"
PER_CATEGORY = 334  # 334 x 6 = 2,004
GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"

PESTLE_CATEGORIES = {
    "Political":      "政治",
    "Economic":       "経済",
    "Social":         "社会",
    "Technological":  "技術",
    "Legal":          "法律",
    "Environmental":  "環境",
}

GDELT_QUERIES = {
    "Political":      '(election OR government OR diplomacy OR geopolitics OR sanctions OR parliament OR "foreign policy" OR summit OR war OR treaty)',
    "Economic":       '(economy OR GDP OR inflation OR "stock market" OR trade OR tariff OR "central bank" OR recession OR investment OR startup)',
    "Social":         '(education OR "public health" OR immigration OR inequality OR "mental health" OR aging OR demographic OR poverty OR protest OR culture)',
    "Technological":  '("artificial intelligence" OR quantum OR robotics OR cybersecurity OR semiconductor OR biotech OR "space exploration" OR internet OR software)',
    "Legal":          '(regulation OR "data privacy" OR antitrust OR "intellectual property" OR compliance OR "human rights" OR legislation OR court OR lawsuit OR patent)',
    "Environmental":  '("climate change" OR "renewable energy" OR carbon OR biodiversity OR sustainability OR deforestation OR pollution OR "electric vehicle" OR flood OR wildfire)',
}


# === Claude-based collection (1990-2016) ===

def collect_year_claude(year: int) -> dict[str, list[dict]]:
    """Use Claude to generate historically significant PESTLE events for a year.
    Generates events in batches per category for higher volume."""
    client = anthropic.Anthropic()
    results = {}

    for cat, label_ja in PESTLE_CATEGORIES.items():
        print(f"    {label_ja} ({cat})...", end=" ", flush=True)

        prompt = f"""You are a historian and foresight analyst. List exactly 55 important real events/developments in the "{cat}" category that occurred in the year {year}.

Cover the FULL YEAR across all months. Include events of varying significance — not just the top headlines, but also:
- Regional events (Asia, Europe, Americas, Africa, Middle East)
- Policy changes, institutional developments
- Emerging trends and weak signals
- Cultural and societal shifts

For each event, provide:
- title: A concise headline (like a news headline at the time)
- summary: 1-2 sentence description of the event and its significance
- source: The primary organization/publication that would have reported it
- published_date: The date (YYYY-MM-DD format, be specific about month/day)

IMPORTANT:
- Only include REAL, historically documented events from {year}
- Distribute events across all 12 months
- Include events from multiple world regions
- Be historically accurate — do not fabricate events

Respond ONLY with a valid JSON array:
[
  {{"title": "...", "summary": "...", "source": "...", "published_date": "YYYY-MM-DD"}},
  ...
]"""

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=8192,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            if "```" in text:
                text = text.split("```json")[-1].split("```")[0] if "```json" in text else text.split("```")[1].split("```")[0]
            text = text.strip()
            events = json.loads(text)
        except Exception as e:
            print(f"ERROR: {e}")
            events = []

        results[cat] = []
        for ev in events[:PER_CATEGORY]:
            results[cat].append({
                "title": ev.get("title", ""),
                "summary": ev.get("summary", ""),
                "url": f"https://historical-reference/{year}/{cat}/{hashlib.md5(ev.get('title', '').encode()).hexdigest()[:12]}",
                "source": ev.get("source", "Historical Record"),
                "lang": "en",
                "published": "",
                "published_date": ev.get("published_date", f"{year}-06-15"),
                "relevance_score": 1.0,
            })
        print(f"{len(results[cat])} events")
        time.sleep(1)  # API rate limit

    # Fill up to PER_CATEGORY with additional batches if needed
    for cat in PESTLE_CATEGORIES:
        if len(results[cat]) < PER_CATEGORY:
            remaining = PER_CATEGORY - len(results[cat])
            existing_titles = {a["title"] for a in results[cat]}
            print(f"    {PESTLE_CATEGORIES[cat]}: filling {remaining} more...", end=" ", flush=True)

            # Generate additional events in batches
            batch_sizes = []
            while remaining > 0:
                batch = min(55, remaining)
                batch_sizes.append(batch)
                remaining -= batch

            for batch_size in batch_sizes:
                try:
                    prompt2 = f"""List exactly {batch_size} MORE real events/developments in the "{cat}" category from {year}.

These must be DIFFERENT from: {json.dumps(list(existing_titles)[:20], ensure_ascii=False)}

Include lesser-known but real events: regional developments, policy changes, institutional milestones, cultural shifts.

Respond ONLY with a valid JSON array:
[{{"title": "...", "summary": "...", "source": "...", "published_date": "YYYY-MM-DD"}}, ...]"""

                    response = client.messages.create(
                        model="claude-haiku-4-5-20251001",
                        max_tokens=8192,
                        messages=[{"role": "user", "content": prompt2}],
                    )
                    text = response.content[0].text.strip()
                    if "```" in text:
                        text = text.split("```json")[-1].split("```")[0] if "```json" in text else text.split("```")[1].split("```")[0]
                    text = text.strip()
                    extra = json.loads(text)
                    for ev in extra:
                        if ev.get("title") not in existing_titles and len(results[cat]) < PER_CATEGORY:
                            results[cat].append({
                                "title": ev.get("title", ""),
                                "summary": ev.get("summary", ""),
                                "url": f"https://historical-reference/{year}/{cat}/{hashlib.md5(ev.get('title', '').encode()).hexdigest()[:12]}",
                                "source": ev.get("source", "Historical Record"),
                                "lang": "en",
                                "published": "",
                                "published_date": ev.get("published_date", f"{year}-06-15"),
                                "relevance_score": 1.0,
                            })
                            existing_titles.add(ev.get("title"))
                    time.sleep(1)
                except Exception as e:
                    print(f"batch error: {e}")
                    break

            print(f"→ {len(results[cat])} total")

    return results


# === GDELT collection (2017-2025) ===

def fetch_gdelt_month(query: str, year: int, month: int, max_records: int = 60) -> list[dict]:
    """Fetch articles from GDELT for a specific year-month."""
    start_dt = f"{year}{month:02d}01000000"
    if month == 12:
        end_dt = f"{year}1231235959"
    else:
        end_dt = f"{year}{month + 1:02d}01000000"

    params = urllib.parse.urlencode({
        "query": query,
        "mode": "artlist",
        "maxrecords": str(max_records),
        "startdatetime": start_dt,
        "enddatetime": end_dt,
        "format": "json",
        "sort": "datedesc",
    })
    url = f"{GDELT_API}?{params}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "FutureInsight/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data.get("articles", [])
    except Exception as e:
        print(f"      [WARN] GDELT {year}-{month:02d}: {e}")
        return []


def collect_year_gdelt(year: int) -> dict[str, list[dict]]:
    """Collect PESTLE articles from GDELT for a year, querying month by month."""
    results = {cat: [] for cat in PESTLE_CATEGORIES}
    seen_urls = set()

    for cat, query in GDELT_QUERIES.items():
        print(f"    {PESTLE_CATEGORIES[cat]} ({cat}):", end="", flush=True)
        per_month = max(30, PER_CATEGORY // 12 + 5)

        for month in range(1, 13):
            # Skip future months
            if year == 2026 and month > 3:
                break
            if year > 2026:
                break

            articles = fetch_gdelt_month(query, year, month, per_month)
            for a in articles:
                art_url = a.get("url", "")
                title = a.get("title", "").strip()
                if not art_url or not title or art_url in seen_urls:
                    continue
                # Only English and Japanese
                lang_str = (a.get("language", "") or "").lower()
                if "english" not in lang_str and "japanese" not in lang_str:
                    continue
                seen_urls.add(art_url)

                seendate = a.get("seendate", "")
                pub_date = f"{year}-{month:02d}-15"
                if seendate:
                    try:
                        pub_date = datetime.strptime(seendate, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d")
                    except ValueError:
                        pass

                results[cat].append({
                    "title": title,
                    "summary": "",
                    "url": art_url,
                    "source": a.get("domain", "GDELT"),
                    "lang": "ja" if "japanese" in lang_str else "en",
                    "published": seendate,
                    "published_date": pub_date,
                    "relevance_score": 1.0,
                })

                if len(results[cat]) >= PER_CATEGORY:
                    break
            print(f" {month}", end="", flush=True)
            time.sleep(10)  # GDELT rate limit

            if len(results[cat]) >= PER_CATEGORY:
                break

        results[cat] = results[cat][:PER_CATEGORY]
        print(f" → {len(results[cat])} articles")

    return results


# === Store to DB ===

def store_articles(articles_by_cat: dict[str, list[dict]], year: int) -> int:
    """Store collected articles into the SQLite database."""
    conn = get_connection()
    init_db()

    date_key = f"{year}-01-01"
    collected_at = datetime.now(timezone.utc).isoformat()
    total_articles = sum(len(arts) for arts in articles_by_cat.values())

    # Delete existing collection for this year to allow re-runs
    existing = conn.execute("SELECT id FROM collections WHERE date = ?", (date_key,)).fetchone()
    if existing:
        conn.execute("DELETE FROM articles WHERE collection_id = ?", (existing[0],))
        conn.execute("DELETE FROM collections WHERE id = ?", (existing[0],))

    cur = conn.execute(
        """INSERT INTO collections
           (date, collected_at, total_fetched, feeds_count, total_selected)
           VALUES (?, ?, ?, ?, ?)""",
        (date_key, collected_at, total_articles, 1, total_articles)
    )
    collection_id = cur.lastrowid

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
                    (collection_id, url_hash, a["title"], a.get("summary", ""),
                     a["url"], a["source"], a["lang"],
                     a.get("published", ""), a["published_date"],
                     cat, a["relevance_score"])
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass

    conn.commit()
    conn.close()
    return inserted


# === Main ===

def main():
    parser = argparse.ArgumentParser(description="Bulk Historical PESTLE Collector")
    parser.add_argument("--start", type=int, default=1990, help="Start year")
    parser.add_argument("--end", type=int, default=2025, help="End year")
    parser.add_argument("--year", type=int, help="Collect a single year")
    parser.add_argument("--source", choices=["auto", "claude", "gdelt"], default="auto",
                        help="Force data source (default: auto based on year)")
    args = parser.parse_args()

    if args.year:
        years = [args.year]
    else:
        years = list(range(args.start, args.end + 1))

    init_db()

    print(f"{'=' * 60}")
    print(f"  Bulk Historical PESTLE Collector")
    print(f"  Years: {years[0]}-{years[-1]} ({len(years)} years)")
    print(f"  Target: {PER_CATEGORY} articles/category = ~{PER_CATEGORY * 6} per year")
    print(f"{'=' * 60}\n")

    for i, year in enumerate(years):
        print(f"\n[{i + 1}/{len(years)}] === Year {year} ===")

        # Check existing count
        conn = get_connection()
        row = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE published_date LIKE ?",
            (f"{year}%",)
        ).fetchone()
        existing_count = row[0] if row else 0
        conn.close()

        if existing_count >= PER_CATEGORY * 6 * 0.9:
            print(f"  Already has {existing_count} articles (>90% of target). Skipping.")
            continue

        # Choose collection method
        if args.source == "claude" or (args.source == "auto" and year <= 2016):
            print(f"  [Claude API] Generating historical events...")
            articles = collect_year_claude(year)
        else:
            print(f"  [GDELT] Collecting real news...")
            articles = collect_year_gdelt(year)

        # Store
        inserted = store_articles(articles, year)
        total = sum(len(arts) for arts in articles.values())
        print(f"  → Stored {inserted}/{total} articles for {year}")

    # Final stats
    conn = get_connection()
    rows = conn.execute(
        """SELECT substr(published_date,1,4) as year, COUNT(*)
           FROM articles
           WHERE published_date >= '1990'
           GROUP BY year ORDER BY year"""
    ).fetchall()
    conn.close()

    print(f"\n{'=' * 60}")
    print("  Final Database Stats (1990-2025)")
    print(f"{'=' * 60}")
    grand_total = 0
    for yr, cnt in rows:
        grand_total += cnt
        marker = " ✓" if cnt >= PER_CATEGORY * 6 * 0.9 else f" ({cnt}/{PER_CATEGORY * 6})"
        print(f"  {yr}: {cnt:>6} articles{marker}")
    print(f"  {'─' * 40}")
    print(f"  Total: {grand_total:>6} articles")


if __name__ == "__main__":
    main()
