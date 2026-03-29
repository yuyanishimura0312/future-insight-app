#!/usr/bin/env python3
"""
Database Initializer for Future Insight App
SQLiteデータベースを初期化し、学術論文用のテーブルを作成する。
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "papers.db"

SCHEMA = """
-- 論文テーブル（メイン）
CREATE TABLE IF NOT EXISTS papers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,                    -- 論文タイトル
    authors TEXT,                           -- 著者（カンマ区切り）
    source_url TEXT NOT NULL,               -- 元論文への参照リンク（必須）
    doi TEXT,                               -- DOI（あれば）
    summary TEXT NOT NULL,                  -- サマリー（必須）
    field TEXT NOT NULL,                    -- 大分野（5分野）
    subfield TEXT,                          -- 小分野（例: 認知科学、量子物理 等）
    source_name TEXT,                       -- 取得元（OpenAlex 等）
    published_date TEXT,                    -- 論文の公開日（YYYY-MM-DD）
    detected_at TEXT NOT NULL DEFAULT (datetime('now')),  -- 検出日時
    language TEXT DEFAULT 'en',             -- 論文の言語

    -- 未来洞察に関連するメタデータ
    insight_tags TEXT,                      -- 洞察タグ（JSON配列）
    novelty_score REAL,                     -- 新規性スコア（0.0〜1.0）
    relevance_score REAL,                   -- 未来洞察との関連度（0.0〜1.0）

    -- OpenAlex固有のID（重複チェック用）
    openalex_id TEXT UNIQUE,

    CHECK (field IN ('人文学', '社会科学', '自然科学', '工学', '芸術'))
);

-- 日次ダイジェストテーブル（毎日の要約）
CREATE TABLE IF NOT EXISTS daily_digests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    digest_date TEXT NOT NULL UNIQUE,       -- 対象日（YYYY-MM-DD）
    total_count INTEGER NOT NULL,           -- その日の検出件数
    field_counts TEXT NOT NULL,             -- 分野別件数（JSON）
    highlights TEXT,                        -- その日の注目トピック要約
    trend_keywords TEXT,                    -- トレンドキーワード（JSON配列）
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- トレンドテーブル（分野横断の動向追跡）
CREATE TABLE IF NOT EXISTS trends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword TEXT NOT NULL,                  -- トレンドキーワード
    field TEXT,                             -- 関連分野（NULLなら分野横断）
    first_seen TEXT NOT NULL,               -- 初出日
    last_seen TEXT NOT NULL,                -- 最終検出日
    mention_count INTEGER DEFAULT 1,        -- 累積出現回数
    description TEXT,                       -- トレンドの説明

    UNIQUE (keyword, field)
);

-- インデックス（検索高速化）
CREATE INDEX IF NOT EXISTS idx_papers_field ON papers(field);
CREATE INDEX IF NOT EXISTS idx_papers_detected_at ON papers(detected_at);
CREATE INDEX IF NOT EXISTS idx_papers_field_detected ON papers(field, detected_at);
CREATE INDEX IF NOT EXISTS idx_papers_source_url ON papers(source_url);
CREATE INDEX IF NOT EXISTS idx_papers_openalex_id ON papers(openalex_id);
CREATE INDEX IF NOT EXISTS idx_papers_published_date ON papers(published_date);
CREATE INDEX IF NOT EXISTS idx_digests_date ON daily_digests(digest_date);
CREATE INDEX IF NOT EXISTS idx_trends_keyword ON trends(keyword);
CREATE INDEX IF NOT EXISTS idx_trends_last_seen ON trends(last_seen);
"""


def init_db():
    """Create the database and tables."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    conn.commit()

    # Verify tables were created
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()

    print(f"Database initialized: {DB_PATH}")
    print(f"Tables: {', '.join(tables)}")
    return DB_PATH


if __name__ == "__main__":
    init_db()
