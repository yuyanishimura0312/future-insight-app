#!/usr/bin/env python3
"""
Historical PESTLE DB backfill via Claude Haiku (1900-1989).

Generates historically-grounded PESTLE news articles based on real events.
Supplements the existing GDELT/Guardian data with more diverse coverage.

Usage:
  python3 backfill_historical_claude.py                    # Full 1900-1989
  python3 backfill_historical_claude.py --start 1950 --end 1959
  python3 backfill_historical_claude.py --year 1945
  python3 backfill_historical_claude.py --dry-run
"""

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import time
from pathlib import Path

PESTLE_DB = Path.home() / "projects" / "research" / "pestle-signal-db" / "data" / "pestle.db"
FI_DB = Path.home() / "projects" / "apps" / "future-insight-app" / "data" / "future_insight.db"

PESTLE_CATEGORIES = ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]

ERA_CONTEXT = {
    (1900, 1913): "Industrial revolution peak, colonial empires, immigration waves, women's suffrage movements, Wright brothers, Einstein's relativity",
    (1914, 1918): "World War I, Russian Revolution, Spanish Flu pandemic, total war, propaganda, women in factories",
    (1919, 1928): "Treaty of Versailles, League of Nations, Roaring Twenties, jazz age, radio, Weimar Republic, Chinese civil war",
    (1929, 1938): "Great Depression, rise of Nazism/fascism, New Deal, Dust Bowl, Japanese militarism, Spanish Civil War",
    (1939, 1945): "World War II, Holocaust, atomic bombs, D-Day, UN founding, Bretton Woods, decolonization begins",
    (1946, 1954): "Cold War begins, Marshall Plan, NATO, Korean War, Chinese Revolution, Indian independence, baby boom",
    (1955, 1964): "Space race, Sputnik, civil rights movement, Cuban missile crisis, decolonization of Africa, JFK assassination",
    (1965, 1974): "Vietnam War, Cultural Revolution, moon landing, oil crisis, Watergate, environmental movement, Woodstock",
    (1975, 1984): "Fall of Saigon, Camp David Accords, Iranian Revolution, Thatcher/Reagan, AIDS crisis, Bhopal disaster, personal computers",
    (1985, 1989): "Perestroika, Chernobyl, fall of Berlin Wall, Tiananmen, Japanese bubble, Iran-Contra, Internet precursors",
}


def get_era(year: int) -> str:
    for (s, e), ctx in ERA_CONTEXT.items():
        if s <= year <= e:
            return ctx
    return ""


def get_api_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    try:
        r = subprocess.run(["security", "find-generic-password", "-s", "ANTHROPIC_API_KEY", "-w"],
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            return r.stdout.strip()
    except Exception:
        pass
    raise RuntimeError("ANTHROPIC_API_KEY not found")


def generate_pestle_year(year: int, api_key: str, per_category: int = 42) -> list[dict]:
    """Generate ~250 PESTLE articles for one year (42 per category × 6)."""
    era = get_era(year)
    # Split into 2 batches to keep output manageable
    all_articles = []

    for batch_cats in [PESTLE_CATEGORIES[:3], PESTLE_CATEGORIES[3:]]:
        cat_str = ", ".join(batch_cats)
        prompt = f"""You are a historical news archive researcher. Generate exactly {per_category} news article entries for EACH of these 3 PESTLE categories for the year {year}: {cat_str}

Era context: {era}

RULES:
1. Every article MUST be based on a REAL historical event that occurred in {year}
2. Geographic diversity: include events from Japan (20%), USA (20%), Europe (20%), Asia (15%), Africa (10%), Latin America (10%), Middle East (5%)
3. Language: 30% Japanese (ja), 70% English (en). Japanese articles use Japanese title/summary.
4. Distribute across all 12 months
5. Summaries: 1-2 sentences about the actual event

Return ONLY a JSON array with {per_category * 3} entries:
{{"cat": "Political|Economic|Social|Technological|Legal|Environmental", "title": "...", "summary": "...", "lang": "en|ja", "month": 1-12, "region": "japan|global"}}"""

        data = {
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 16000,
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            result = subprocess.run(
                ["curl", "-s", "--max-time", "90",
                 "-H", f"x-api-key: {api_key}",
                 "-H", "anthropic-version: 2023-06-01",
                 "-H", "content-type: application/json",
                 "-d", json.dumps(data),
                 "https://api.anthropic.com/v1/messages"],
                capture_output=True, text=True, timeout=95,
            )
            resp = json.loads(result.stdout)
            text = resp.get("content", [{}])[0].get("text", "")
            start = text.find("[")
            end = text.rfind("]") + 1
            if start >= 0 and end > start:
                batch = json.loads(text[start:end])
                all_articles.extend(batch)
        except Exception as e:
            print(f"    [ERROR] {e}")

        time.sleep(2)

    return all_articles


def insert_pestle_articles(conn: sqlite3.Connection, articles: list[dict], year: int) -> int:
    """Insert into pestle.db schema."""
    inserted = 0
    for a in articles:
        title = a.get("title", "").strip()
        if not title:
            continue

        month = a.get("month", 6)
        cat = a.get("cat", "")
        if cat not in PESTLE_CATEGORIES:
            continue

        url = f"backfill://claude-historical/{year}/{hashlib.sha256(title.encode()).hexdigest()[:12]}"
        url_hash = hashlib.sha256(url.encode()).hexdigest()

        if conn.execute("SELECT 1 FROM articles WHERE url_hash = ?", (url_hash,)).fetchone():
            continue

        pub_date = f"{year}-{month:02d}-15"
        lang = a.get("lang", "en")
        region = a.get("region", "japan" if lang == "ja" else "global")
        summary = a.get("summary", "")

        try:
            conn.execute(
                """INSERT OR IGNORE INTO articles
                   (url_hash, title, title_ja, summary, url, source, lang,
                    published, published_date, pestle_category, relevance_score, region, collection_date)
                   VALUES (?,?,NULL,?,?,?,?,?,?,?,1.0,?,?)""",
                (url_hash, title, summary, url, "Claude-Historical", lang,
                 f"{pub_date}T00:00:00Z", pub_date, cat, region, pub_date),
            )
            inserted += 1
        except Exception:
            continue

    conn.commit()
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Backfill PESTLE with Claude-generated historical articles")
    parser.add_argument("--start", type=int, default=1900)
    parser.add_argument("--end", type=int, default=1989)
    parser.add_argument("--year", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_key = get_api_key()
    if args.year:
        start, end = args.year, args.year
    else:
        start, end = args.start, args.end

    years = list(range(start, end + 1))
    print(f"=== PESTLE Historical Backfill (Claude) ===")
    print(f"Range: {start}-{end} ({len(years)} years)")
    print(f"Target: ~{len(years) * 252} articles (42/cat × 6 cats × {len(years)} years)")
    print(f"Estimated cost: ~${len(years) * 0.02:.2f}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print()

    conn = None if args.dry_run else sqlite3.connect(str(PESTLE_DB))
    total = {"fetched": 0, "inserted": 0}

    for i, year in enumerate(years, 1):
        print(f"[{i}/{len(years)}] {year}...", end=" ", flush=True)
        try:
            articles = generate_pestle_year(year, api_key, per_category=42)
            total["fetched"] += len(articles)

            if not args.dry_run and articles:
                ins = insert_pestle_articles(conn, articles, year)
                total["inserted"] += ins
                print(f"OK ({len(articles)} generated, {ins} inserted)")
            else:
                print(f"OK ({len(articles)} generated, dry-run)")
        except KeyboardInterrupt:
            print(f"\nInterrupted at {year}.")
            break
        except Exception as e:
            print(f"ERROR: {e}")

    if conn:
        conn.close()

    print(f"\n=== Complete ===")
    print(f"Generated: {total['fetched']}, Inserted: {total['inserted']}")


if __name__ == "__main__":
    main()
