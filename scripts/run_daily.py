#!/usr/bin/env python3
"""
Daily Pipeline for Future Insight App
ニュース収集 → 学術論文収集 → JSON出力 を一括実行する。

Usage:
    python3 run_daily.py           # Run both collectors
    python3 run_daily.py --news    # News only
    python3 run_daily.py --papers  # Papers only
"""

import sys
import time
from datetime import datetime, timezone

from db import init_db, get_full_stats


def run_news():
    """Run PESTLE news collection."""
    print("=" * 60)
    print("  STEP 1: PESTLE News Collection")
    print("=" * 60)
    # Import here to avoid circular dependency and allow selective runs
    from collect_news import main as collect_news_main
    collect_news_main()


def run_papers():
    """Run academic paper collection."""
    print("\n" + "=" * 60)
    print("  STEP 2: Academic Paper Collection")
    print("=" * 60)
    from collect_papers import main as collect_papers_main
    collect_papers_main()


def print_summary():
    """Print combined stats from the unified DB."""
    stats = get_full_stats()
    print("\n" + "=" * 60)
    print("  COMBINED SUMMARY")
    print("=" * 60)
    print(f"  News articles:  {stats['total_articles']}")
    print(f"  Academic papers: {stats['total_papers']}")
    print(f"  Trend keywords:  {stats['total_trends']}")
    print(f"  Collections:     {stats['total_collections']}")

    if stats.get("by_category"):
        print("\n  PESTLE categories:")
        for cat, count in stats["by_category"].items():
            print(f"    {cat}: {count}")

    if stats.get("papers_by_field"):
        print("\n  Paper fields:")
        for field, count in stats["papers_by_field"].items():
            print(f"    {field}: {count}")

    print()


def main():
    start = time.time()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"\n{'#' * 60}")
    print(f"  Future Insight App — Daily Pipeline")
    print(f"  Date: {today}")
    print(f"{'#' * 60}\n")

    # Initialize unified database
    init_db()

    args = sys.argv[1:]
    run_all = not args  # No flags = run both

    if run_all or "--news" in args:
        run_news()

    if run_all or "--papers" in args:
        run_papers()

    print_summary()

    elapsed = time.time() - start
    print(f"  Total time: {elapsed:.1f}s")
    print(f"\n{'#' * 60}")
    print(f"  ✓ Daily pipeline complete!")
    print(f"{'#' * 60}\n")


if __name__ == "__main__":
    main()
