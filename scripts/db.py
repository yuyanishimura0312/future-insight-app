"""
SQLite database module for Future Insight App.
Unified DB for PESTLE news + academic papers + trends.
"""
from __future__ import annotations

import sqlite3
import json
import hashlib
from pathlib import Path
from datetime import datetime
from collections import Counter

DB_PATH = Path(__file__).parent.parent / "data" / "future_insight.db"

SCHEMA_SQL = """
-- ===== PESTLE News Tables =====

-- Daily collection runs
CREATE TABLE IF NOT EXISTS collections (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT    NOT NULL UNIQUE,
    collected_at    TEXT    NOT NULL,
    total_fetched   INTEGER NOT NULL,
    feeds_count     INTEGER NOT NULL,
    total_selected  INTEGER NOT NULL DEFAULT 0
);

-- Individual news articles
CREATE TABLE IF NOT EXISTS articles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id   INTEGER NOT NULL REFERENCES collections(id),
    url_hash        TEXT    NOT NULL,
    title           TEXT    NOT NULL,
    summary         TEXT,
    url             TEXT    NOT NULL,
    source          TEXT    NOT NULL,
    lang            TEXT    NOT NULL DEFAULT 'en',
    published       TEXT,
    published_date  TEXT,
    pestle_category TEXT    NOT NULL,
    relevance_score REAL    NOT NULL DEFAULT 0,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- CLA analyses linked to articles (for future use)
CREATE TABLE IF NOT EXISTS cla_analyses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id      INTEGER REFERENCES articles(id),
    topic           TEXT,
    litany          TEXT,
    systemic_cause  TEXT,
    worldview       TEXT,
    myth_metaphor   TEXT,
    analyzed_at     TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- News indexes
CREATE INDEX IF NOT EXISTS idx_articles_collection
    ON articles(collection_id);
CREATE INDEX IF NOT EXISTS idx_articles_category
    ON articles(pestle_category);
CREATE INDEX IF NOT EXISTS idx_articles_published_date
    ON articles(published_date);
CREATE INDEX IF NOT EXISTS idx_articles_url_hash
    ON articles(url_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_dedup
    ON articles(collection_id, url_hash);

-- ===== Academic Papers Tables =====

CREATE TABLE IF NOT EXISTS papers (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT    NOT NULL,
    authors         TEXT,
    source_url      TEXT    NOT NULL,
    doi             TEXT,
    summary         TEXT    NOT NULL,
    field           TEXT    NOT NULL,
    subfield        TEXT,
    source_name     TEXT,
    published_date  TEXT,
    detected_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    language        TEXT    DEFAULT 'en',
    insight_tags    TEXT,
    novelty_score   REAL,
    relevance_score REAL,
    openalex_id     TEXT    UNIQUE,
    CHECK (field IN ('人文学', '社会科学', '自然科学', '工学', '芸術'))
);

CREATE TABLE IF NOT EXISTS daily_digests (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    digest_date     TEXT    NOT NULL UNIQUE,
    total_count     INTEGER NOT NULL,
    field_counts    TEXT    NOT NULL,
    highlights      TEXT,
    trend_keywords  TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS trends (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword         TEXT    NOT NULL,
    field           TEXT,
    first_seen      TEXT    NOT NULL,
    last_seen       TEXT    NOT NULL,
    mention_count   INTEGER DEFAULT 1,
    description     TEXT,
    UNIQUE (keyword, field)
);

-- ===== Media Sources Table =====

CREATE TABLE IF NOT EXISTS media_sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL,
    name_ja         TEXT,
    url             TEXT,
    feed_url        TEXT    NOT NULL UNIQUE,
    region          TEXT    NOT NULL DEFAULT 'global',
    categories      TEXT,
    language        TEXT    NOT NULL DEFAULT 'en',
    tier            INTEGER NOT NULL DEFAULT 1,
    status          TEXT    NOT NULL DEFAULT 'active',
    articles_count  INTEGER NOT NULL DEFAULT 0,
    last_fetched    TEXT
);

CREATE INDEX IF NOT EXISTS idx_media_sources_region ON media_sources(region);
CREATE INDEX IF NOT EXISTS idx_media_sources_status ON media_sources(status);

-- Paper indexes
CREATE INDEX IF NOT EXISTS idx_papers_field ON papers(field);
CREATE INDEX IF NOT EXISTS idx_papers_detected_at ON papers(detected_at);
CREATE INDEX IF NOT EXISTS idx_papers_field_detected ON papers(field, detected_at);
CREATE INDEX IF NOT EXISTS idx_papers_source_url ON papers(source_url);
CREATE INDEX IF NOT EXISTS idx_papers_openalex_id ON papers(openalex_id);
CREATE INDEX IF NOT EXISTS idx_papers_published_date2 ON papers(published_date);
CREATE INDEX IF NOT EXISTS idx_digests_date ON daily_digests(digest_date);
CREATE INDEX IF NOT EXISTS idx_trends_keyword ON trends(keyword);
CREATE INDEX IF NOT EXISTS idx_trends_last_seen ON trends(last_seen);
"""


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_connection()
    conn.executescript(SCHEMA_SQL)
    conn.close()
    # Run migrations after initial schema creation
    _run_migrations()


def _run_migrations():
    """Apply incremental schema migrations safely (idempotent)."""
    conn = get_connection()

    # Migration: add 'region' column to articles table
    columns = [row[1] for row in conn.execute("PRAGMA table_info(articles)").fetchall()]
    if "region" not in columns:
        conn.execute("ALTER TABLE articles ADD COLUMN region TEXT NOT NULL DEFAULT 'global'")
        conn.commit()

    conn.close()


def save_media_sources(feeds: list[dict]) -> int:
    """Sync RSS_FEEDS list into the media_sources table.
    Updates existing rows, inserts new ones. Returns count of upserted rows."""
    conn = get_connection()
    init_db_schema_only(conn)
    count = 0
    now = datetime.utcnow().isoformat()

    for feed in feeds:
        feed_url = feed["url"]
        name = feed["name"]
        lang = feed.get("lang", "en")
        tier = feed.get("tier", 1)
        region = feed.get("region", "japan" if lang == "ja" else "global")
        focus = feed.get("focus", "")

        # Determine name_ja: use name directly if it contains Japanese characters
        name_ja = name if any('\u3000' <= c <= '\u9fff' for c in name) else None

        # Count articles from this source
        art_count = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE source = ?", (name,)
        ).fetchone()[0]

        # Get last article date for this source
        last = conn.execute(
            "SELECT MAX(created_at) FROM articles WHERE source = ?", (name,)
        ).fetchone()[0]

        try:
            conn.execute("""
                INSERT INTO media_sources
                    (name, name_ja, feed_url, region, categories, language, tier, status, articles_count, last_fetched)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(feed_url) DO UPDATE SET
                    name = excluded.name,
                    name_ja = excluded.name_ja,
                    region = excluded.region,
                    categories = excluded.categories,
                    language = excluded.language,
                    tier = excluded.tier,
                    articles_count = excluded.articles_count,
                    last_fetched = excluded.last_fetched
            """, (name, name_ja, feed_url, region, focus, lang, tier, art_count, last))
            count += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    return count


def init_db_schema_only(conn):
    """Apply schema without migrations (used internally to ensure tables exist)."""
    conn.executescript(SCHEMA_SQL)


def get_media_sources() -> list[dict]:
    """Return all media sources with their article counts."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, name, name_ja, url, feed_url, region, categories, language, "
        "tier, status, articles_count, last_fetched FROM media_sources ORDER BY name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def export_media_sources_json(output_path) -> int:
    """Export media sources to JSON file. Returns count."""
    sources = get_media_sources()
    output = {
        "exported_at": datetime.utcnow().isoformat(),
        "count": len(sources),
        "sources": sources,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    return len(sources)


def save_collection(output: dict) -> None:
    """Save a full PESTLE collection result to the database."""
    conn = get_connection()
    init_db()

    total_selected = sum(info["count"] for info in output["pestle"].values())

    # Check if collection already exists for this date
    existing = conn.execute(
        "SELECT id FROM collections WHERE date = ?", (output["date"],)
    ).fetchone()

    if existing:
        collection_id = existing[0]
        conn.execute(
            """UPDATE collections
               SET collected_at = ?, total_fetched = ?, feeds_count = ?, total_selected = ?
               WHERE id = ?""",
            (output["collected_at"], output["total_fetched"],
             output["feeds_count"], total_selected, collection_id)
        )
    else:
        cur = conn.execute(
            """INSERT INTO collections
               (date, collected_at, total_fetched, feeds_count, total_selected)
               VALUES (?, ?, ?, ?, ?)""",
            (output["date"], output["collected_at"], output["total_fetched"],
             output["feeds_count"], total_selected)
        )
        collection_id = cur.lastrowid

    # Insert articles
    inserted = 0
    for category, info in output["pestle"].items():
        for article in info["articles"]:
            url_hash = hashlib.sha256(article["url"].encode()).hexdigest()
            pub_date = _normalize_date(article.get("published", ""))
            try:
                region = article.get("region", "global")
                conn.execute(
                    """INSERT OR IGNORE INTO articles
                       (collection_id, url_hash, title, summary, url, source, lang,
                        published, published_date, pestle_category, relevance_score, region)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (collection_id, url_hash, article["title"], article["summary"],
                     article["url"], article["source"], article["lang"],
                     article.get("published", ""), pub_date, category,
                     article.get("relevance_score", 0), region)
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass  # duplicate, skip

    conn.commit()
    conn.close()
    return inserted


def get_stats() -> dict:
    """Get database statistics."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    stats = {}
    stats["total_collections"] = conn.execute("SELECT COUNT(*) FROM collections").fetchone()[0]
    stats["total_articles"] = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]

    # Articles per category
    rows = conn.execute(
        "SELECT pestle_category, COUNT(*) as cnt FROM articles GROUP BY pestle_category ORDER BY cnt DESC"
    ).fetchall()
    stats["by_category"] = {r["pestle_category"]: r["cnt"] for r in rows}

    # Top sources
    rows = conn.execute(
        "SELECT source, COUNT(*) as cnt FROM articles GROUP BY source ORDER BY cnt DESC LIMIT 10"
    ).fetchall()
    stats["top_sources"] = {r["source"]: r["cnt"] for r in rows}

    # Date range
    row = conn.execute("SELECT MIN(date) as first, MAX(date) as last FROM collections").fetchone()
    stats["date_range"] = {"first": row["first"], "last": row["last"]}

    conn.close()
    return stats


def _normalize_date(date_str: str) -> str | None:
    """Best-effort parse to YYYY-MM-DD."""
    if not date_str:
        return None
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ===== Academic Papers Functions =====

def save_papers(papers: list[dict]) -> int:
    """Insert papers into DB, skipping duplicates. Returns count of new papers."""
    conn = get_connection()
    inserted = 0
    for paper in papers:
        try:
            conn.execute("""
                INSERT INTO papers (
                    title, authors, source_url, doi, summary,
                    field, subfield, source_name, published_date,
                    language, insight_tags, novelty_score,
                    relevance_score, openalex_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                paper["title"], paper["authors"], paper["source_url"],
                paper["doi"], paper["summary"], paper["field"],
                paper["subfield"], paper["source_name"],
                paper["published_date"], paper["language"],
                paper["insight_tags"], paper["novelty_score"],
                paper["relevance_score"], paper["openalex_id"],
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return inserted


def save_daily_digest(field_counts: dict, total: int, date_str: str):
    """Create a daily digest record summarizing the day's paper collection."""
    conn = get_connection()

    cursor = conn.execute(
        "SELECT insight_tags FROM papers WHERE date(detected_at) = ?",
        (date_str,)
    )
    all_tags = []
    for row in cursor.fetchall():
        if row[0]:
            try:
                all_tags.extend(json.loads(row[0]))
            except json.JSONDecodeError:
                pass

    top_keywords = [kw for kw, _ in Counter(all_tags).most_common(20)]

    try:
        conn.execute("""
            INSERT OR REPLACE INTO daily_digests
                (digest_date, total_count, field_counts, trend_keywords)
            VALUES (?, ?, ?, ?)
        """, (
            date_str, total,
            json.dumps(field_counts, ensure_ascii=False),
            json.dumps(top_keywords, ensure_ascii=False),
        ))
        conn.commit()
    except sqlite3.Error as e:
        print(f"  [WARN] Failed to create digest: {e}")
    finally:
        conn.close()


def update_trends(date_str: str):
    """Update trend keywords based on today's papers."""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT insight_tags, field FROM papers WHERE date(detected_at) = ?",
        (date_str,)
    )

    keyword_fields = {}
    for row in cursor.fetchall():
        tags_json, field = row
        if not tags_json:
            continue
        try:
            tags = json.loads(tags_json)
        except json.JSONDecodeError:
            continue
        for tag in tags:
            key = (tag, field)
            keyword_fields[key] = keyword_fields.get(key, 0) + 1

    for (keyword, field), count in keyword_fields.items():
        existing = conn.execute(
            "SELECT id, mention_count FROM trends WHERE keyword = ? AND field = ?",
            (keyword, field)
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE trends SET last_seen = ?, mention_count = mention_count + ? WHERE id = ?",
                (date_str, count, existing[0])
            )
        else:
            conn.execute(
                "INSERT INTO trends (keyword, field, first_seen, last_seen, mention_count) VALUES (?, ?, ?, ?, ?)",
                (keyword, field, date_str, date_str, count)
            )

    conn.commit()
    conn.close()


def export_papers_json(output_path: Path) -> int:
    """Export all papers to JSON for the web dashboard. Returns count."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT title, authors, source_url, doi, summary, field, subfield, "
        "source_name, published_date, language, insight_tags, relevance_score "
        "FROM papers ORDER BY detected_at DESC"
    ).fetchall()
    conn.close()

    papers = []
    for r in rows:
        # Parse insight_tags from JSON string to list
        tags = r["insight_tags"]
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except (json.JSONDecodeError, TypeError):
                tags = []

        # Clean summary: remove carriage returns and HTML entities
        summary = (r["summary"] or "").replace("&#13;", "").replace("\r", "").replace("\\n", " ").strip()

        papers.append({
            "title": r["title"],
            "authors": r["authors"],
            "source_url": r["source_url"],
            "doi": r["doi"],
            "summary": summary,
            "field": r["field"],
            "subfield": r["subfield"],
            "source_name": r["source_name"],
            "published_date": r["published_date"],
            "language": r["language"],
            "insight_tags": tags,
            "relevance_score": r["relevance_score"],
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(papers, f, ensure_ascii=False, indent=2)

    return len(papers)


def get_full_stats() -> dict:
    """Get combined stats for news + papers."""
    stats = get_stats()

    conn = get_connection()
    stats["total_papers"] = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]

    rows = conn.execute(
        "SELECT field, COUNT(*) as cnt FROM papers GROUP BY field ORDER BY cnt DESC"
    ).fetchall()
    stats["papers_by_field"] = {r[0]: r[1] for r in rows}

    stats["total_trends"] = conn.execute("SELECT COUNT(*) FROM trends").fetchone()[0]

    conn.close()
    return stats
