#!/usr/bin/env python3
"""
Historical PESTLE data backfill — multi-source edition.

Sources:
  1. The Guardian Open Platform API (1999-present, EN only, free 5000 req/day)
  2. NYT Archive API (1851-present, EN only, free 2000 req/day)
  3. GDELT DOC API v2 (2023+ for JA, 2017+ for EN)

Strategy by period:
  2000-2014: Guardian + NYT (EN only; JA structurally absent from free APIs)
  2015-2022: Guardian + NYT + GDELT-EN
  2023-2026: Guardian + NYT + GDELT-EN + GDELT-JA

Usage:
  python3 backfill_historical.py                           # Full 2000-2026
  python3 backfill_historical.py --start 2020 --end 2022   # Range
  python3 backfill_historical.py --month 2023-06           # Single month
  python3 backfill_historical.py --dry-run                 # Preview
  python3 backfill_historical.py --source guardian          # Single source
"""

import argparse
import hashlib
import json
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

# --- Configuration ---

DB_PATH = Path(__file__).parent.parent / "data" / "future_insight.db"
RATE_LIMIT_SECONDS = 10
MAX_ARTICLES_PER_QUERY = 250
ARTICLES_PER_CATEGORY_PER_MONTH = 50  # Target per PESTLE category per month

# PESTLE queries - English and Japanese
PESTLE_QUERIES = {
    "Political": {
        "en": ["politics", "election", "government", "diplomacy", "geopolitics", "sanctions"],
        "ja": ["政治", "選挙", "外交", "安全保障", "国会", "防衛"],
    },
    "Economic": {
        "en": ["economy", "GDP", "inflation", "trade", "investment", "market"],
        "ja": ["経済", "景気", "株価", "為替", "金利", "日銀"],
    },
    "Social": {
        "en": ["education", "health", "migration", "inequality", "demographic", "society"],
        "ja": ["教育", "少子化", "高齢化", "介護", "福祉", "医療"],
    },
    "Technological": {
        "en": ["technology", "artificial intelligence", "semiconductor", "quantum", "robotics", "innovation"],
        "ja": ["AI", "人工知能", "半導体", "量子", "ロボット", "技術"],
    },
    "Legal": {
        "en": ["regulation", "law", "court", "privacy", "antitrust", "legislation"],
        "ja": ["法律", "規制", "法改正", "個人情報", "裁判", "訴訟"],
    },
    "Environmental": {
        "en": ["climate", "environment", "renewable energy", "biodiversity", "carbon", "sustainability"],
        "ja": ["環境", "気候変動", "脱炭素", "再生可能エネルギー", "温暖化", "生態系"],
    },
}

# Keywords that evolved over time - use era-appropriate terms
ERA_KEYWORDS = {
    "Technological": {
        "en": {
            (2000, 2005): ["internet", "dotcom", "broadband", "mobile phone", "Y2K"],
            (2006, 2010): ["web 2.0", "social media", "cloud computing", "smartphone", "iPhone"],
            (2011, 2015): ["big data", "app economy", "3D printing", "wearable", "sharing economy"],
            (2016, 2020): ["artificial intelligence", "blockchain", "5G", "autonomous vehicle", "deep learning"],
            (2021, 2026): ["generative AI", "ChatGPT", "quantum computing", "metaverse", "semiconductor"],
        },
        "ja": {
            (2000, 2005): ["インターネット", "ブロードバンド", "携帯電話", "IT革命"],
            (2006, 2010): ["SNS", "クラウド", "スマートフォン", "iPhone"],
            (2011, 2015): ["ビッグデータ", "アプリ", "3Dプリンター", "IoT"],
            (2016, 2020): ["AI", "ブロックチェーン", "5G", "自動運転", "ディープラーニング"],
            (2021, 2026): ["生成AI", "ChatGPT", "量子コンピュータ", "メタバース", "半導体"],
        },
    },
    "Environmental": {
        "en": {
            (2000, 2005): ["Kyoto Protocol", "ozone", "deforestation", "greenhouse gas"],
            (2006, 2010): ["carbon trading", "clean energy", "Copenhagen", "peak oil"],
            (2011, 2015): ["Fukushima", "renewable energy", "fracking", "Paris Agreement"],
            (2016, 2020): ["climate change", "net zero", "Greta Thunberg", "extinction rebellion"],
            (2021, 2026): ["ESG", "carbon neutral", "tipping point", "just transition", "polycrisis"],
        },
        "ja": {
            (2000, 2005): ["京都議定書", "温室効果ガス", "環境問題", "オゾン"],
            (2006, 2010): ["排出権取引", "クリーンエネルギー", "COP"],
            (2011, 2015): ["福島", "再生可能エネルギー", "メガソーラー"],
            (2016, 2020): ["パリ協定", "SDGs", "脱炭素", "気候変動"],
            (2021, 2026): ["カーボンニュートラル", "GX", "ティッピングポイント"],
        },
    },
}


def get_era_keywords(category: str, lang: str, year: int) -> list[str]:
    """Get era-appropriate keywords for a category/language/year."""
    base = PESTLE_QUERIES[category][lang].copy()
    era_data = ERA_KEYWORDS.get(category, {}).get(lang, {})
    for (start, end), keywords in era_data.items():
        if start <= year <= end:
            base.extend(keywords)
            break
    return base


def fetch_gdelt_period(
    query: str, start_dt: str, end_dt: str, max_records: int = 250, lang_filter: str | None = None
) -> list[dict]:
    """Fetch articles from GDELT DOC API for a date range.
    Uses curl subprocess because Python urllib has HTTP/2 issues with GDELT on macOS."""
    import subprocess

    full_query = f"{query} {lang_filter}" if lang_filter else query
    params = urllib.parse.urlencode({
        "query": full_query,
        "mode": "ArtList",
        "maxrecords": str(max_records),
        "startdatetime": start_dt,
        "enddatetime": end_dt,
        "format": "json",
    })
    url = f"https://api.gdeltproject.org/api/v2/doc/doc?{params}"

    retries = 2
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-s", "--max-time", "30", "-H", "User-Agent: FutureInsight-Backfill/1.0", url],
                capture_output=True, text=True, timeout=35,
            )
            raw = result.stdout

            if not raw.strip().startswith("{"):
                if attempt < retries:
                    time.sleep(15)
                    continue
                return []

            data = json.loads(raw)

            articles = []
            for item in data.get("articles", []):
                title = item.get("title", "").strip()
                art_url = item.get("url", "")
                if not title or not art_url:
                    continue

                lang_str = (item.get("language", "") or "").lower()
                if "english" in lang_str:
                    lang = "en"
                elif "japanese" in lang_str:
                    lang = "ja"
                else:
                    if lang_filter and "japanese" in lang_filter:
                        lang = "ja"
                    elif lang_filter and "english" in lang_filter:
                        lang = "en"
                    else:
                        continue

                articles.append({
                    "title": title,
                    "summary": "",
                    "url": art_url,
                    "source": "GDELT-Historical: " + item.get("domain", ""),
                    "lang": lang,
                    "published": item.get("seendate", ""),
                    "region": "japan" if lang == "ja" else "global",
                })

            return articles

        except Exception as e:
            if attempt < retries:
                time.sleep(15)
                continue
            return []


def _api_get_json(url: str, headers: dict | None = None, timeout: int = 30) -> dict | None:
    """Helper: GET a URL and return parsed JSON, or None on error.
    Uses curl subprocess for macOS HTTP/2 compatibility."""
    import subprocess
    cmd = ["curl", "-s", "--max-time", str(timeout), url]
    if headers:
        for k, v in headers.items():
            cmd.extend(["-H", f"{k}: {v}"])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 5)
        raw = result.stdout
        if not raw.strip().startswith(("{", "[")):
            return None
        return json.loads(raw)
    except Exception:
        return None


# --- Guardian API ---

# PESTLE section mapping for Guardian API
GUARDIAN_SECTIONS = {
    "Political": "politics|world|us-news|global-development",
    "Economic": "business|money|global-development",
    "Social": "society|education|inequality|global-development",
    "Technological": "technology|science",
    "Legal": "law|technology",  # Guardian has a dedicated law section
    "Environmental": "environment|science",
}


def fetch_guardian_month(year: int, month: int, category: str, api_key: str, max_articles: int = 50) -> list[dict]:
    """Fetch articles from The Guardian Content API for one month and PESTLE category."""
    ny, nm = next_month(year, month)
    from_date = f"{year:04d}-{month:02d}-01"
    to_date = f"{ny:04d}-{nm:02d}-01"

    sections = GUARDIAN_SECTIONS.get(category, "world")
    keywords = get_era_keywords(category, "en", year)
    q = " OR ".join(keywords[:4])

    params = urllib.parse.urlencode({
        "q": q,
        "section": sections,
        "from-date": from_date,
        "to-date": to_date,
        "order-by": "relevance",
        "page-size": str(min(max_articles, 50)),
        "show-fields": "headline,trailText",
        "api-key": api_key,
    })
    url = f"https://content.guardianapis.com/search?{params}"

    data = _api_get_json(url)
    if not data:
        return []

    articles = []
    for item in data.get("response", {}).get("results", []):
        title = item.get("webTitle", "").strip()
        art_url = item.get("webUrl", "")
        if not title or not art_url:
            continue

        pub_raw = item.get("webPublicationDate", "")  # ISO 8601
        pub_date = pub_raw[:10] if pub_raw else ""

        fields = item.get("fields", {})
        summary = fields.get("trailText", "") or ""

        articles.append({
            "title": title,
            "summary": summary[:300],
            "url": art_url,
            "source": "Guardian",
            "lang": "en",
            "published": pub_raw,
            "published_date": pub_date,
            "region": "global",
        })

    return articles


# --- NYT Archive API ---

NYT_PESTLE_KEYWORDS = {
    "Political": ["politic", "election", "government", "diplomat", "geopolit", "sanction", "congress", "senate"],
    "Economic": ["econom", "market", "trade", "invest", "inflation", "gdp", "recession", "banking", "fiscal"],
    "Social": ["educat", "health", "migrat", "inequalit", "demograph", "welfare", "poverty", "communit"],
    "Technological": ["technolog", "artificial intellig", "robot", "semiconductor", "quantum", "cyber", "innovat"],
    "Legal": ["regulat", "law", "court", "privacy", "antitrust", "legislat", "ruling", "patent"],
    "Environmental": ["climat", "environment", "carbon", "renewable", "biodivers", "sustainab", "pollut"],
}


def fetch_nyt_archive_month(year: int, month: int, api_key: str, max_per_category: int = 30) -> dict[str, list[dict]]:
    """Fetch NYT Archive for one month, classify into PESTLE categories.
    Returns dict of category -> articles list."""
    url = f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={api_key}"

    data = _api_get_json(url, timeout=60)
    if not data:
        return {}

    docs = data.get("response", {}).get("docs", [])
    if not docs:
        return {}

    # Classify each article into best PESTLE category
    categorized: dict[str, list[dict]] = {cat: [] for cat in NYT_PESTLE_KEYWORDS}

    for doc in docs:
        title = doc.get("headline", {}).get("main", "").strip()
        abstract = doc.get("abstract", "") or ""
        art_url = doc.get("web_url", "")
        if not title or not art_url:
            continue

        pub_raw = doc.get("pub_date", "")
        pub_date = pub_raw[:10] if pub_raw else ""

        text = f"{title} {abstract}".lower()
        best_cat = None
        best_score = 0
        for cat, keywords in NYT_PESTLE_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in text)
            if score > best_score:
                best_score = score
                best_cat = cat

        if best_cat and best_score > 0:
            categorized[best_cat].append({
                "title": title,
                "summary": abstract[:300],
                "url": art_url,
                "source": "NYT Archive",
                "lang": "en",
                "published": pub_raw,
                "published_date": pub_date,
                "region": "global",
            })

    # Limit per category
    for cat in categorized:
        categorized[cat] = categorized[cat][:max_per_category]

    return categorized


def month_range(start_year: int, start_month: int, end_year: int, end_month: int):
    """Generate (year, month) tuples for a range."""
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def format_date(year: int, month: int, day: int = 1) -> str:
    """Format date for GDELT API: YYYYMMDDHHMMSS."""
    return f"{year:04d}{month:02d}{day:02d}000000"


def next_month(year: int, month: int) -> tuple[int, int]:
    """Get next month's year and month."""
    if month == 12:
        return year + 1, 1
    return year, month + 1


def classify_pestle_score(title: str, summary: str, category: str, year: int) -> float:
    """Simple keyword-based PESTLE score for a given category."""
    text = f"{title} {summary}".lower()
    score = 0.0
    for lang in ["en", "ja"]:
        keywords = get_era_keywords(category, lang, year)
        for kw in keywords:
            if kw.lower() in text:
                score += 1 + len(kw) / 10
    return round(score, 2)


def ensure_collection(conn: sqlite3.Connection, date_str: str) -> int:
    """Ensure a collection record exists for a given date, return its ID."""
    # Use INSERT OR IGNORE to handle the race condition where another process
    # inserts the same date between our SELECT and INSERT.
    conn.execute(
        "INSERT OR IGNORE INTO collections (date, collected_at, total_fetched, feeds_count, total_selected) VALUES (?, ?, 0, 0, 0)",
        (date_str, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    cur = conn.execute("SELECT id FROM collections WHERE date = ?", (date_str,))
    return cur.fetchone()[0]


def insert_articles(conn: sqlite3.Connection, articles: list[dict], collection_id: int) -> int:
    """Insert articles into the database, skipping duplicates. Returns count inserted."""
    inserted = 0
    for a in articles:
        # Use full SHA256 (64 chars) to be consistent with the rest of the DB
        url_hash = hashlib.sha256(a["url"].encode()).hexdigest()

        # Check for duplicate URL across all collections (url-level dedup)
        cur = conn.execute("SELECT 1 FROM articles WHERE url_hash = ?", (url_hash,))
        if cur.fetchone():
            continue

        # Parse published date - prefer pre-parsed published_date if available
        raw_date = a.get("published", "")
        pub_date = a.get("published_date", "")
        if not pub_date and raw_date and len(raw_date) >= 10:
            if raw_date[4] == "-":
                pub_date = raw_date[:10]  # ISO 8601: 2020-06-01T17:26:00Z
            else:
                pub_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"  # GDELT: 20230608T034500Z

        try:
            result = conn.execute(
                """INSERT OR IGNORE INTO articles
                   (collection_id, url_hash, title, summary, url, source, lang, published, published_date, pestle_category, relevance_score)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    collection_id,
                    url_hash,
                    a["title"],
                    a.get("summary", ""),
                    a["url"],
                    a["source"],
                    a["lang"],
                    raw_date,
                    pub_date,
                    a["pestle_category"],
                    a.get("relevance_score", 0.0),
                ),
            )
            # rowcount=1 means actually inserted; 0 means IGNORE fired (duplicate)
            if result.rowcount == 1:
                inserted += 1
        except Exception as e:
            # Log unexpected errors (not IntegrityError, which is handled by OR IGNORE)
            print(f"    [WARN] Failed to insert article '{a.get('title', '')[:50]}': {e}")
            continue

    conn.commit()
    return inserted


def backfill_month(
    conn: sqlite3.Connection, year: int, month: int,
    dry_run: bool = False,
    guardian_key: str | None = None,
    nyt_key: str | None = None,
    sources: list[str] | None = None,
) -> dict:
    """Backfill one month of PESTLE data from multiple sources.

    Sources used depend on year and available API keys:
      - guardian: 1999+ (needs key)
      - nyt: 1851+ (needs key)
      - gdelt: 2023+ for JA, 2017+ for EN
    """
    date_str = f"{year:04d}-{month:02d}-15"
    start = format_date(year, month)
    ny, nm = next_month(year, month)
    end = format_date(ny, nm)

    # Determine which sources to use
    use_sources = sources or ["guardian", "nyt", "gdelt"]
    use_guardian = "guardian" in use_sources and guardian_key and year >= 1999
    use_nyt = "nyt" in use_sources and nyt_key
    use_gdelt_en = "gdelt" in use_sources and year >= 2017
    use_gdelt_ja = "gdelt" in use_sources and year >= 2023

    stats = {
        "year": year, "month": month, "fetched": 0, "inserted": 0,
        "by_category": {}, "by_lang": {"en": 0, "ja": 0},
        "by_source": {},
    }

    if not dry_run:
        collection_id = ensure_collection(conn, date_str)

    all_articles = []

    # --- Source 1: NYT Archive (one request returns entire month) ---
    nyt_articles_by_cat = {}
    if use_nyt:
        nyt_articles_by_cat = fetch_nyt_archive_month(year, month, nyt_key, max_per_category=30)
        nyt_count = sum(len(v) for v in nyt_articles_by_cat.values())
        stats["by_source"]["nyt"] = nyt_count
        print(f"    NYT: {nyt_count} articles")
        time.sleep(6)  # NYT rate limit: ~2000/day

    # --- Per-category processing ---
    for category in PESTLE_QUERIES:
        cat_articles = []

        # Guardian
        if use_guardian:
            guardian_arts = fetch_guardian_month(year, month, category, guardian_key, max_articles=30)
            for a in guardian_arts:
                a["pestle_category"] = category
            cat_articles.extend(guardian_arts)
            time.sleep(1)  # Guardian: 12 req/sec, generous limit

        # NYT (already fetched per-month, just merge)
        if use_nyt and category in nyt_articles_by_cat:
            for a in nyt_articles_by_cat[category]:
                a["pestle_category"] = category
            cat_articles.extend(nyt_articles_by_cat[category])

        # GDELT English
        if use_gdelt_en:
            en_keywords = get_era_keywords(category, "en", year)
            for kw in en_keywords[:2]:  # 2 keywords (guardian/nyt cover more)
                articles = fetch_gdelt_period(kw, start, end, max_records=30, lang_filter="sourcelang:english")
                for a in articles:
                    a["pestle_category"] = category
                cat_articles.extend(articles)
                time.sleep(RATE_LIMIT_SECONDS)

        # GDELT Japanese
        if use_gdelt_ja:
            ja_keywords = get_era_keywords(category, "ja", year)
            for kw in ja_keywords[:3]:
                articles = fetch_gdelt_period(kw, start, end, max_records=50, lang_filter="sourcelang:japanese")
                for a in articles:
                    a["pestle_category"] = category
                cat_articles.extend(articles)
                time.sleep(RATE_LIMIT_SECONDS)

        # Deduplicate within category
        seen = set()
        unique = []
        for a in cat_articles:
            if a["url"] not in seen:
                seen.add(a["url"])
                if "relevance_score" not in a:
                    a["relevance_score"] = classify_pestle_score(a["title"], a.get("summary", ""), category, year)
                unique.append(a)

        # Take top articles by score
        unique.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
        selected = unique[:ARTICLES_PER_CATEGORY_PER_MONTH]

        stats["by_category"][category] = len(selected)
        for a in selected:
            stats["by_lang"][a["lang"]] += 1

        all_articles.extend(selected)

    stats["fetched"] = len(all_articles)

    # Count by source
    for a in all_articles:
        src = a.get("source", "").split(":")[0].split(" ")[0]
        stats["by_source"][src] = stats["by_source"].get(src, 0) + 1

    if dry_run:
        src_str = ", ".join(f"{k}:{v}" for k, v in stats["by_source"].items())
        print(f"  [DRY RUN] {year}-{month:02d}: {len(all_articles)} articles "
              f"(EN:{stats['by_lang']['en']}, JA:{stats['by_lang']['ja']}) [{src_str}]")
        return stats

    # Insert into database
    inserted = insert_articles(conn, all_articles, collection_id)
    stats["inserted"] = inserted

    conn.execute(
        "UPDATE collections SET total_fetched = ?, total_selected = ? WHERE id = ?",
        (len(all_articles), inserted, collection_id),
    )
    conn.commit()

    return stats


def _get_api_key(name: str) -> str | None:
    """Get API key from environment or macOS keychain."""
    import os
    key = os.environ.get(name)
    if key:
        return key
    # Try macOS keychain
    import subprocess
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", name, "-w"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None


def main():
    parser = argparse.ArgumentParser(description="Backfill historical PESTLE data (multi-source)")
    parser.add_argument("--start", type=int, default=2000, help="Start year (default: 2000)")
    parser.add_argument("--end", type=int, default=2026, help="End year (default: 2026)")
    parser.add_argument("--month", type=str, help="Single month to backfill (YYYY-MM)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    parser.add_argument("--db", type=str, help="Database path override")
    parser.add_argument("--source", type=str, help="Use specific source only: guardian, nyt, gdelt")
    parser.add_argument("--guardian-key", type=str, help="Guardian API key (or set GUARDIAN_API_KEY env)")
    parser.add_argument("--nyt-key", type=str, help="NYT API key (or set NYT_API_KEY env)")
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else DB_PATH

    if args.month:
        parts = args.month.split("-")
        start_year, start_month = int(parts[0]), int(parts[1])
        end_year, end_month = start_year, start_month
    else:
        start_year, start_month = args.start, 1
        end_year, end_month = args.end, 3 if args.end == 2026 else 12

    # Resolve API keys
    guardian_key = args.guardian_key or _get_api_key("GUARDIAN_API_KEY")
    nyt_key = args.nyt_key or _get_api_key("NYT_API_KEY")

    # Determine sources
    sources = [args.source] if args.source else ["guardian", "nyt", "gdelt"]

    months = list(month_range(start_year, start_month, end_year, end_month))
    total_months = len(months)

    print(f"=== PESTLE Historical Backfill (Multi-Source) ===")
    print(f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
    print(f"Months: {total_months}")
    print(f"Sources: {', '.join(sources)}")
    print(f"  Guardian API key: {'OK' if guardian_key else 'NOT SET (register at open-platform.theguardian.com)'}")
    print(f"  NYT API key: {'OK' if nyt_key else 'NOT SET (register at developer.nytimes.com)'}")
    print(f"Database: {db_path}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print()

    if not args.dry_run:
        conn = sqlite3.connect(str(db_path))
        # Ensure schema
        conn.execute("""CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL UNIQUE,
            collected_at TEXT NOT NULL,
            total_fetched INTEGER NOT NULL,
            feeds_count INTEGER NOT NULL DEFAULT 0,
            total_selected INTEGER NOT NULL DEFAULT 0
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection_id INTEGER NOT NULL REFERENCES collections(id),
            url_hash TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT,
            url TEXT NOT NULL,
            source TEXT NOT NULL,
            lang TEXT NOT NULL DEFAULT 'en',
            published TEXT,
            published_date TEXT,
            pestle_category TEXT NOT NULL,
            relevance_score REAL NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_url_hash ON articles(url_hash)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_dedup ON articles(collection_id, url_hash)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_published_date ON articles(published_date)")
        conn.commit()
    else:
        conn = None

    total_stats = {"fetched": 0, "inserted": 0, "en": 0, "ja": 0}

    for i, (year, month) in enumerate(months, 1):
        print(f"\n[{i}/{total_months}] Processing {year}-{month:02d}...")

        try:
            stats = backfill_month(
                conn, year, month, dry_run=args.dry_run,
                guardian_key=guardian_key, nyt_key=nyt_key, sources=sources,
            )
            total_stats["fetched"] += stats["fetched"]
            total_stats["inserted"] += stats["inserted"]
            total_stats["en"] += stats["by_lang"]["en"]
            total_stats["ja"] += stats["by_lang"]["ja"]

            print(f"  Fetched: {stats['fetched']}, Inserted: {stats['inserted']}, "
                  f"EN: {stats['by_lang']['en']}, JA: {stats['by_lang']['ja']}")

        except KeyboardInterrupt:
            print(f"\n\nInterrupted at {year}-{month:02d}. Progress saved.")
            break
        except Exception as e:
            print(f"  [ERROR] {year}-{month:02d}: {e}")
            continue

    if conn:
        conn.close()

    print(f"\n=== Backfill Complete ===")
    print(f"Total fetched: {total_stats['fetched']}")
    print(f"Total inserted: {total_stats['inserted']}")
    print(f"EN: {total_stats['en']}, JA: {total_stats['ja']}")
    ratio = total_stats['ja'] / max(total_stats['en'], 1)
    print(f"JA/EN ratio: {ratio:.2f}")


if __name__ == "__main__":
    main()
