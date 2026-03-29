#!/usr/bin/env python3
"""
Daily Historical PESTLE Collector
毎日1年分の歴史的PESTLE記事を収集してDBに蓄積する。

- 1900年〜2016年: Claude APIで歴史的に重要な出来事を生成
- 2017年〜2025年: GDELT DOC APIから実際のニュースを取得
- 各カテゴリ10件ずつ（計60件/日）
- 2025年まで行ったら1900年に戻ってサイクル

State file: data/historical_state.json で進捗を追跡
"""

import json
import hashlib
import os
import sqlite3
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

from db import get_connection, init_db

DATA_DIR = Path(__file__).parent.parent / "data"
STATE_FILE = DATA_DIR / "historical_state.json"

START_YEAR = 1900
END_YEAR = 2025
PER_CATEGORY = 10

PESTLE_CATEGORIES = {
    "Political":      "政治",
    "Economic":       "経済",
    "Social":         "社会",
    "Technological":  "技術",
    "Legal":          "法律",
    "Environmental":  "環境",
}

# GDELT queries (for 2017+)
GDELT_QUERIES = {
    "Political":      '(election OR government OR diplomacy OR geopolitics OR "foreign policy" OR sanctions OR parliament OR summit)',
    "Economic":       '(economy OR GDP OR inflation OR "stock market" OR trade OR tariff OR "central bank" OR recession)',
    "Social":         '(education OR "public health" OR immigration OR inequality OR "mental health" OR aging OR demographic)',
    "Technological":  '("artificial intelligence" OR quantum OR robotics OR cybersecurity OR semiconductor OR biotech OR "space exploration")',
    "Legal":          '(regulation OR "data privacy" OR antitrust OR "intellectual property" OR compliance OR "human rights" OR legislation)',
    "Environmental":  '("climate change" OR "renewable energy" OR carbon OR biodiversity OR sustainability OR deforestation OR pollution)',
}

GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"


# === State management ===

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"current_year": START_YEAR, "completed_cycles": 0}


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def advance_year(state: dict) -> dict:
    """Move to next year, cycling back to START_YEAR after END_YEAR."""
    state["current_year"] += 1
    if state["current_year"] > END_YEAR:
        state["current_year"] = START_YEAR
        state["completed_cycles"] += 1
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    return state


# === GDELT collection (2017+) ===

def fetch_gdelt(query: str, year: int, max_records: int = 10) -> list[dict]:
    """Fetch articles from GDELT for a given year."""
    start_dt = f"{year}0101000000"
    end_dt = f"{year}1231235959"

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
            time.sleep(10)
            resp = requests.get(GDELT_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("articles", [])
    except Exception as e:
        print(f"    [WARN] GDELT error: {e}")
        return []


def collect_from_gdelt(year: int) -> dict[str, list[dict]]:
    """Collect PESTLE articles from GDELT for a year."""
    results = {}
    for cat, query in GDELT_QUERIES.items():
        print(f"    {PESTLE_CATEGORIES[cat]} ({cat})...", end=" ", flush=True)
        articles = fetch_gdelt(query, year, max_records=PER_CATEGORY)

        results[cat] = []
        for a in articles:
            url = a.get("url", "")
            title = a.get("title", "").strip()
            if not url or not title:
                continue
            seendate = a.get("seendate", "")
            pub_date = None
            if seendate:
                try:
                    pub_date = datetime.strptime(seendate, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d")
                except ValueError:
                    pass

            results[cat].append({
                "title": title,
                "summary": "",
                "url": url,
                "source": a.get("domain", "GDELT"),
                "lang": "en",
                "published": seendate,
                "published_date": pub_date or f"{year}-01-01",
                "relevance_score": 1.0,
            })
        print(f"{len(results[cat])} articles")
        time.sleep(5)  # GDELT rate limit

    return results


# === Claude-based historical collection (1900-2016) ===

def collect_from_claude(year: int) -> dict[str, list[dict]]:
    """Use Claude to generate historically significant PESTLE events for a year."""
    client = anthropic.Anthropic()

    prompt = f"""List the {PER_CATEGORY} most important real events/developments for EACH of the 6 PESTLE categories that occurred in the year {year}.

For each event, provide:
- title: A concise headline (like a news headline at the time)
- summary: 1-2 sentence description of the event and its significance
- source: The primary organization/publication that would have reported it
- published_date: The approximate date (YYYY-MM-DD format)

Categories: Political, Economic, Social, Technological, Legal, Environmental

IMPORTANT:
- Only include REAL, historically documented events from {year}
- If fewer than {PER_CATEGORY} significant events exist for a category in {year}, include as many as are real
- For early years (pre-1950), some categories may have fewer events — that is fine
- Be historically accurate — do not fabricate events

Respond ONLY with valid JSON in this exact format:
{{
  "Political": [
    {{"title": "...", "summary": "...", "source": "...", "published_date": "YYYY-MM-DD"}},
    ...
  ],
  "Economic": [...],
  "Social": [...],
  "Technological": [...],
  "Legal": [...],
  "Environmental": [...]
}}"""

    print(f"    Generating historical events via Claude API...", flush=True)
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text.strip()
    # Extract JSON from response (handle markdown code blocks)
    if "```" in text:
        text = text.split("```json")[-1].split("```")[0] if "```json" in text else text.split("```")[1].split("```")[0]
    text = text.strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"    [WARN] JSON parse error: {e}")
        return {cat: [] for cat in PESTLE_CATEGORIES}

    results = {}
    for cat in PESTLE_CATEGORIES:
        events = data.get(cat, [])
        results[cat] = []
        for ev in events[:PER_CATEGORY]:
            results[cat].append({
                "title": ev.get("title", ""),
                "summary": ev.get("summary", ""),
                "url": f"https://historical-reference/{year}/{cat}/{hashlib.md5(ev.get('title', '').encode()).hexdigest()[:8]}",
                "source": ev.get("source", "Historical Record"),
                "lang": "en",
                "published": "",
                "published_date": ev.get("published_date", f"{year}-06-15"),
                "relevance_score": 1.0,
            })
        print(f"    {PESTLE_CATEGORIES[cat]}: {len(results[cat])} events")

    return results


# === Store to DB ===

def store_articles(articles_by_cat: dict[str, list[dict]], year: int) -> int:
    """Store collected articles into the SQLite database."""
    conn = get_connection()
    init_db()

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


# === Main ===

def main():
    state = load_state()
    year = state["current_year"]
    cycle = state["completed_cycles"]

    print(f"{'=' * 50}")
    print(f"  Historical PESTLE Collector")
    print(f"  Year: {year}  (cycle #{cycle + 1})")
    print(f"  Range: {START_YEAR}-{END_YEAR}")
    print(f"{'=' * 50}\n")

    init_db()

    # Choose collection method based on year
    if year >= 2017:
        print(f"  [GDELT] Collecting real news from {year}...")
        articles = collect_from_gdelt(year)
    else:
        print(f"  [Claude] Generating historical events for {year}...")
        articles = collect_from_claude(year)

    # Store to DB
    print(f"\n  Storing to database...")
    inserted = store_articles(articles, year)
    total = sum(len(arts) for arts in articles.values())
    print(f"  {inserted} new / {total} total articles for {year}")

    # Advance to next year
    state = advance_year(state)
    save_state(state)
    print(f"\n  Next run will collect: {state['current_year']}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
