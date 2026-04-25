#!/usr/bin/env python3
"""
Merge backfill worker DBs into the main future_insight.db and pestle.db.

Reads all data/backfill_*.db files, deduplicates against existing articles
by url_hash, and inserts new articles into both databases.

Usage:
  python3 merge_backfill.py           # Merge all backfill DBs
  python3 merge_backfill.py --stats   # Show stats only, don't merge
  python3 merge_backfill.py --cleanup # Delete backfill DBs after merge
"""

import argparse
import hashlib
import sqlite3
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
MAIN_DB = DATA_DIR / "future_insight.db"
PESTLE_DB = Path.home() / "projects" / "research" / "pestle-signal-db" / "data" / "pestle.db"


def get_backfill_dbs() -> list[Path]:
    """Find all backfill database files."""
    return sorted(DATA_DIR.glob("backfill_*.db"))


def show_stats(backfill_dbs: list[Path]):
    """Show statistics for all backfill databases."""
    total = 0
    total_ja = 0
    total_en = 0

    for db_path in backfill_dbs:
        conn = sqlite3.connect(str(db_path))
        try:
            count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
            ja = conn.execute("SELECT COUNT(*) FROM articles WHERE lang='ja'").fetchone()[0]
            en = conn.execute("SELECT COUNT(*) FROM articles WHERE lang='en'").fetchone()[0]

            # Year range
            dates = conn.execute(
                "SELECT MIN(published_date), MAX(published_date) FROM articles WHERE published_date IS NOT NULL AND published_date != ''"
            ).fetchone()

            print(f"  {db_path.name}: {count} articles (EN:{en}, JA:{ja}) [{dates[0]} to {dates[1]}]")
            total += count
            total_ja += ja
            total_en += en
        except Exception as e:
            print(f"  {db_path.name}: ERROR - {e}")
        finally:
            conn.close()

    print(f"\n  Total: {total} articles (EN:{total_en}, JA:{total_ja})")
    return total


def merge_into_db(target_db: Path, backfill_dbs: list[Path], target_name: str) -> int:
    """Merge backfill articles into a target database."""
    if not target_db.exists():
        print(f"  [WARN] {target_name} not found at {target_db}, skipping")
        return 0

    conn = sqlite3.connect(str(target_db))
    inserted = 0
    skipped = 0

    # Get existing url_hashes for deduplication
    print(f"  Loading existing hashes from {target_name}...")
    existing = set()
    try:
        for row in conn.execute("SELECT url_hash FROM articles"):
            existing.add(row[0])
    except sqlite3.OperationalError:
        pass
    print(f"  {len(existing)} existing articles")

    for db_path in backfill_dbs:
        src_conn = sqlite3.connect(str(db_path))
        try:
            # Read all articles from backfill DB
            cursor = src_conn.execute("""
                SELECT url_hash, title, summary, url, source, lang, published,
                       published_date, pestle_category, relevance_score
                FROM articles
            """)

            batch = []
            for row in cursor:
                url_hash = row[0]
                if url_hash in existing:
                    skipped += 1
                    continue

                existing.add(url_hash)
                batch.append(row)

                if len(batch) >= 1000:
                    _insert_batch(conn, batch, target_name)
                    inserted += len(batch)
                    batch = []

            if batch:
                _insert_batch(conn, batch, target_name)
                inserted += len(batch)

        except Exception as e:
            print(f"  [ERROR] Reading {db_path.name}: {e}")
        finally:
            src_conn.close()

    conn.commit()
    conn.close()

    print(f"  {target_name}: +{inserted} inserted, {skipped} duplicates skipped")
    return inserted


def _insert_batch(conn: sqlite3.Connection, batch: list, target_name: str):
    """Insert a batch of articles into the target DB."""
    if "future_insight" in target_name:
        # future_insight.db requires collection_id
        # Use collection_id=0 for backfill data
        for row in batch:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO articles
                       (collection_id, url_hash, title, summary, url, source, lang,
                        published, published_date, pestle_category, relevance_score)
                       VALUES (0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    row,
                )
            except Exception:
                continue
    else:
        # pestle.db schema
        for row in batch:
            url_hash, title, summary, url, source, lang, published, pub_date, category, score = row
            region = "japan" if lang == "ja" else "global"
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO articles
                       (url_hash, title, title_ja, summary, url, source, lang,
                        published, published_date, pestle_category, relevance_score,
                        region, collection_date)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (url_hash, title, None, summary, url, source, lang,
                     published, pub_date, category, score, region, pub_date),
                )
            except Exception:
                continue

    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Merge backfill DBs into main databases")
    parser.add_argument("--stats", action="store_true", help="Show stats only")
    parser.add_argument("--cleanup", action="store_true", help="Delete backfill DBs after merge")
    args = parser.parse_args()

    backfill_dbs = get_backfill_dbs()
    if not backfill_dbs:
        print("No backfill databases found in data/")
        return

    print(f"=== Backfill Merge ===")
    print(f"Found {len(backfill_dbs)} backfill databases:\n")
    total = show_stats(backfill_dbs)

    if args.stats:
        return

    if total == 0:
        print("\nNo articles to merge.")
        return

    print(f"\n--- Merging into future_insight.db ---")
    fi_count = merge_into_db(MAIN_DB, backfill_dbs, "future_insight.db")

    print(f"\n--- Merging into pestle.db ---")
    pe_count = merge_into_db(PESTLE_DB, backfill_dbs, "pestle.db")

    print(f"\n=== Merge Complete ===")
    print(f"future_insight.db: +{fi_count}")
    print(f"pestle.db: +{pe_count}")

    if args.cleanup:
        print("\nCleaning up backfill databases...")
        for db_path in backfill_dbs:
            db_path.unlink()
            print(f"  Deleted: {db_path.name}")


if __name__ == "__main__":
    main()
