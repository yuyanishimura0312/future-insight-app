#!/usr/bin/env python3
"""
Batch translate article titles to Japanese using Claude Haiku.
Sends 50 titles per API call for efficiency.
"""

import json
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic
from db import get_connection

client = anthropic.Anthropic()
MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 50


def get_untranslated(limit=None):
    """Get articles that need translation."""
    conn = get_connection()
    query = """
        SELECT id, title FROM articles
        WHERE lang != 'ja' AND (title_ja IS NULL OR title_ja = '')
        ORDER BY id
    """
    if limit:
        query += f" LIMIT {limit}"
    rows = conn.execute(query).fetchall()
    conn.close()
    return rows


def translate_batch(titles: list[tuple[int, str]]) -> dict[int, str]:
    """Translate a batch of titles. Returns {id: translated_title}."""
    numbered = "\n".join(f"{i+1}. {title}" for i, (_, title) in enumerate(titles))

    prompt = f"""以下の英語ニュース見出しを日本語に翻訳してください。
番号付きで、翻訳のみを返してください。元の番号を維持してください。
説明や注釈は不要です。

{numbered}"""

    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
    except Exception as e:
        print(f"  [ERROR] API call failed: {e}")
        return {}

    # Parse numbered responses
    results = {}
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        match = re.match(r'^(\d+)\.\s*(.+)$', line)
        if match:
            idx = int(match.group(1)) - 1
            translated = match.group(2).strip()
            if 0 <= idx < len(titles):
                article_id = titles[idx][0]
                results[article_id] = translated

    return results


def save_translations(translations: dict[int, str]):
    """Save translated titles to the database."""
    conn = get_connection()
    for article_id, title_ja in translations.items():
        conn.execute(
            "UPDATE articles SET title_ja = ? WHERE id = ?",
            (title_ja, article_id)
        )
    conn.commit()
    conn.close()


def export_latest_json():
    """Re-export latest.json with translated titles."""
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row

    rows = conn.execute("""
        SELECT title, title_ja, summary, url, source, lang,
               published, pestle_category, relevance_score, published_date
        FROM articles
        ORDER BY published_date DESC, created_at DESC
    """).fetchall()

    from collections import defaultdict
    by_category = defaultdict(list)
    for r in rows:
        cat = r["pestle_category"]
        # Use Japanese title if available, otherwise original
        display_title = r["title_ja"] if r["title_ja"] else r["title"]
        by_category[cat].append({
            "title": display_title,
            "title_en": r["title"],
            "summary": r["summary"] or "",
            "url": r["url"],
            "source": r["source"],
            "lang": r["lang"],
            "published": r["published"] or "",
            "relevance_score": r["relevance_score"],
            "published_date": r["published_date"] or "",
        })

    label_map = {
        "Political": "政治", "Economic": "経済", "Social": "社会",
        "Technological": "技術", "Legal": "法律", "Environmental": "環境",
    }

    pestle = {}
    for cat in ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]:
        arts = by_category.get(cat, [])
        pestle[cat] = {
            "label_ja": label_map.get(cat, cat),
            "count": len(arts),
            "articles": arts,
        }

    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    conn.close()

    output = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "total_fetched": total,
        "feeds_count": 31,
        "pestle": pestle,
    }

    output_path = Path(__file__).parent.parent / "data" / "latest.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\nExported latest.json ({total} articles)")


def main():
    untranslated = get_untranslated()
    total = len(untranslated)
    print(f"=== Title Translation ({total} articles) ===\n")

    if total == 0:
        print("All titles already translated!")
        return

    batches = [untranslated[i:i+BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    total_translated = 0
    start = time.time()

    for batch_idx, batch in enumerate(batches):
        progress = f"[{batch_idx+1}/{len(batches)}]"
        print(f"  {progress} Translating {len(batch)} titles...", end=" ", flush=True)

        translations = translate_batch(batch)
        if translations:
            save_translations(translations)
            total_translated += len(translations)
            print(f"{len(translations)} done (total: {total_translated}/{total})")
        else:
            print("failed")

        # Rate limit: ~2 requests per second for Haiku
        time.sleep(0.5)

    elapsed = time.time() - start
    print(f"\n=== Complete ===")
    print(f"  Translated: {total_translated}/{total}")
    print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} min)")

    # Re-export latest.json
    print("\nExporting latest.json with translations...")
    export_latest_json()
    print("\n✓ Done!")


if __name__ == "__main__":
    main()
