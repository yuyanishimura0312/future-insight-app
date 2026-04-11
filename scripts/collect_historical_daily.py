#!/usr/bin/env python3
"""
Daily Historical PESTLE Collector (v2)
毎日1年分の歴史的PESTLE記事を大量収集してDBに蓄積する。

- 1990年から1年ずつ遡って収集（1990→1989→1988→...）
- 各カテゴリ334件、合計約2000件/日を目標
- 2017年〜: GDELT DOC APIから実際のニュースを取得
- 〜2016年: Claude APIで歴史的出来事を生成（カテゴリ別バッチ）
- 全年分完了後は最新年に戻ってサイクル

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

# Phase 1: 1990→1900 (backwards), then Phase 2: 1991→present (forwards)
PHASE1_START = 1990
PHASE1_END = 1900
PHASE2_END = 2025  # will be updated as years pass
PER_CATEGORY = 334  # 334 x 6 = 2004 articles/day target

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
    # Phase 1: go backwards from 1990
    return {"current_year": PHASE1_START, "phase": 1, "completed_cycles": 0}


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def advance_year(state: dict) -> dict:
    """Phase 1: 1990→1900 (backwards). Phase 2: 1991→present (forwards)."""
    phase = state.get("phase", 1)

    if phase == 1:
        # Going backwards: 1990, 1989, 1988, ..., 1900
        state["current_year"] -= 1
        if state["current_year"] < PHASE1_END:
            # Phase 1 complete, switch to Phase 2: 1991 onwards
            state["phase"] = 2
            state["current_year"] = PHASE1_START + 1  # 1991
    else:
        # Going forwards: 1991, 1992, ..., 2025
        state["current_year"] += 1
        if state["current_year"] > PHASE2_END:
            # All years covered, restart from 1900 for deeper collection
            state["current_year"] = PHASE1_END
            state["phase"] = 1
            state["completed_cycles"] += 1

    state["last_run"] = datetime.now(timezone.utc).isoformat()
    return state


# === GDELT collection (2017+) ===

def fetch_gdelt(query: str, year: int, max_records: int = 250) -> list[dict]:
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
            time.sleep(15)
            resp = requests.get(GDELT_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("articles", [])
    except Exception as e:
        print(f"    [WARN] GDELT error: {e}")
        return []


def collect_from_gdelt(year: int) -> dict[str, list[dict]]:
    """Collect PESTLE articles from GDELT for a year. Target: 334 per category."""
    results = {}
    for cat, query in GDELT_QUERIES.items():
        print(f"    {PESTLE_CATEGORIES[cat]} ({cat})...", end=" ", flush=True)
        # GDELT max is 250 per request, so split into multiple queries if needed
        all_articles = []
        keywords = [k.strip() for k in query.replace("(", "").replace(")", "").split(" OR ")]

        # First try full query
        articles = fetch_gdelt(query, year, max_records=250)
        seen_urls = set()
        for a in articles:
            url = a.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_articles.append(a)

        # If we need more, try individual keywords
        if len(all_articles) < PER_CATEGORY:
            for kw in keywords[:4]:
                if len(all_articles) >= PER_CATEGORY:
                    break
                extra = fetch_gdelt(kw, year, max_records=100)
                for a in extra:
                    url = a.get("url", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_articles.append(a)
                time.sleep(8)

        results[cat] = []
        for a in all_articles[:PER_CATEGORY]:
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
        time.sleep(5)

    return results


# === Claude-based historical collection (pre-2017) ===

def collect_from_claude_batch(year: int, category: str, label_ja: str,
                               target: int = 100) -> list[dict]:
    """Use Claude to generate historically significant events for one category in one year.
    Makes multiple calls if needed to reach target count."""
    client = anthropic.Anthropic()
    all_events = []
    # Batch: request up to 100 per call, multiple calls if target > 100
    remaining = target
    batch_num = 0

    while remaining > 0 and batch_num < 4:
        batch_size = min(remaining, 100)
        batch_num += 1

        exclude_titles = ""
        if all_events:
            existing = "\n".join(f"- {e['title']}" for e in all_events[:50])
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
- Cover diverse aspects: diplomatic, military, elections, social movements, cultural events, scientific discoveries, legal cases, economic crises, trade agreements, environmental disasters, etc.
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

            # Fix truncated JSON: if it ends mid-object, try to close it
            if not text.endswith("]"):
                # Find the last complete object
                last_brace = text.rfind("}")
                if last_brace > 0:
                    text = text[:last_brace + 1] + "]"

            events = json.loads(text)
            if isinstance(events, list):
                all_events.extend(events)
                remaining -= len(events)
                # If Claude returned fewer than requested, the year is exhausted
                if len(events) < batch_size * 0.5:
                    break
        except json.JSONDecodeError as e:
            print(f"      [WARN] JSON parse error (batch {batch_num}): {e}")
            # Try smaller batch on retry
            remaining = min(remaining, 50)
            continue
        except Exception as e:
            print(f"      [WARN] Claude batch {batch_num} error: {e}")
            break
        time.sleep(1)

    return all_events


def collect_from_claude(year: int) -> dict[str, list[dict]]:
    """Collect PESTLE events for a year via Claude, category by category.
    Target: up to PER_CATEGORY per category, realistically 50-200 for pre-2017 years."""
    results = {}
    # For older years, Claude can realistically generate fewer events
    # Scale target based on era
    if year >= 1990:
        target_per_cat = min(PER_CATEGORY, 200)  # Modern era: more events available
    elif year >= 1950:
        target_per_cat = min(PER_CATEGORY, 100)  # Post-war: moderate
    elif year >= 1900:
        target_per_cat = min(PER_CATEGORY, 50)   # Early 20th century: fewer records
    else:
        target_per_cat = min(PER_CATEGORY, 30)

    for cat, label_ja in PESTLE_CATEGORIES.items():
        print(f"    {label_ja} ({cat})...", end=" ", flush=True)
        events = collect_from_claude_batch(year, cat, label_ja, target=target_per_cat)

        results[cat] = []
        seen_titles = set()
        for ev in events:
            title = ev.get("title", "").strip()
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            results[cat].append({
                "title": title,
                "summary": ev.get("summary", ""),
                "url": f"https://historical-reference/{year}/{cat}/{hashlib.md5(title.encode()).hexdigest()[:8]}",
                "source": ev.get("source", "Historical Record"),
                "lang": "en",
                "published": "",
                "published_date": ev.get("published_date", f"{year}-06-15"),
                "relevance_score": 1.0,
            })
        print(f"{len(results[cat])} events")

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
    phase = state.get("phase", 1)
    cycle = state["completed_cycles"]

    direction = "← backwards" if phase == 1 else "→ forwards"
    phase_range = f"{PHASE1_START}→{PHASE1_END}" if phase == 1 else f"{PHASE1_START + 1}→{PHASE2_END}"

    print(f"{'=' * 60}")
    print(f"  Historical PESTLE Collector v2")
    print(f"  Year: {year}  (phase {phase}: {direction})")
    print(f"  Range: {phase_range}  (cycle #{cycle + 1})")
    print(f"  Target: {PER_CATEGORY} per category × 6 = {PER_CATEGORY * 6}")
    print(f"{'=' * 60}\n")

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

    # Summary
    print(f"\n  === Year {year} Summary ===")
    for cat in PESTLE_CATEGORIES:
        count = len(articles.get(cat, []))
        print(f"    {PESTLE_CATEGORIES[cat]:4s} ({cat:15s}): {count:4d}")
    print(f"    {'Total':>21s}: {total:4d}")
    print(f"    {'New in DB':>21s}: {inserted:4d}")

    # Advance to previous year
    state = advance_year(state)
    save_state(state)
    print(f"\n  Next run will collect: {state['current_year']}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
