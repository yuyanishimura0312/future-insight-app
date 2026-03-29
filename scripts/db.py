"""
SQLite database module for Future Insight App.
Stores PESTLE news collections for trend tracking and CLA analysis.
"""

import sqlite3
import hashlib
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent.parent / "data" / "future_insight.db"

SCHEMA_SQL = """
-- Daily collection runs
CREATE TABLE IF NOT EXISTS collections (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT    NOT NULL UNIQUE,
    collected_at    TEXT    NOT NULL,
    total_fetched   INTEGER NOT NULL,
    feeds_count     INTEGER NOT NULL,
    total_selected  INTEGER NOT NULL DEFAULT 0
);

-- Individual articles
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

-- Indexes
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


def save_collection(output: dict) -> None:
    """Save a full PESTLE collection result to the database."""
    conn = get_connection()
    init_db()

    total_selected = sum(info["count"] for info in output["pestle"].values())

    # Insert or replace collection row
    cur = conn.execute(
        """INSERT OR REPLACE INTO collections
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
                conn.execute(
                    """INSERT OR IGNORE INTO articles
                       (collection_id, url_hash, title, summary, url, source, lang,
                        published, published_date, pestle_category, relevance_score)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (collection_id, url_hash, article["title"], article["summary"],
                     article["url"], article["source"], article["lang"],
                     article.get("published", ""), pub_date, category,
                     article.get("relevance_score", 0))
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
