#!/usr/bin/env python3
"""
Translate English academic paper titles and summaries to Japanese.
Results saved to data/papers_translations.json
"""

import json
import time
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

client = anthropic.Anthropic()
MODEL = "claude-haiku-4-5-20251001"
DATA_DIR = Path(__file__).parent.parent / "data"


def translate_batch(papers_batch: list[dict], start_idx: int) -> list[dict]:
    """Translate a batch of papers (up to 10)."""
    text = ""
    for i, p in enumerate(papers_batch):
        title = p["title"][:200]
        summary = (p.get("summary") or "")[:300]
        text += f"[{start_idx + i}]\nTitle: {title}\nSummary: {summary}\n\n"

    response = client.messages.create(
        model=MODEL,
        max_tokens=4000,
        messages=[{
            "role": "user",
            "content": f"""以下の英語の学術論文タイトルと要約を日本語に翻訳してください。
学術的な正確さを保ちつつ、読みやすい日本語にしてください。

JSON配列で返してください。各要素: {{"index": 番号, "title_ja": "日本語タイトル", "summary_ja": "日本語要約"}}

{text}

JSONのみ返してください。"""
        }],
    )

    try:
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except (json.JSONDecodeError, IndexError):
        return []


def main():
    with open(DATA_DIR / "papers.json", encoding="utf-8") as f:
        papers = json.load(f)

    # Load existing translations if any
    trans_file = DATA_DIR / "papers_translations.json"
    existing = {}
    if trans_file.exists():
        with open(trans_file, encoding="utf-8") as f:
            for t in json.load(f):
                existing[t["index"]] = t

    # Find English papers needing translation
    to_translate = []
    for i, p in enumerate(papers):
        if p.get("language", "en") == "en" and i not in existing:
            to_translate.append((i, p))

    print(f"総論文数: {len(papers)}")
    print(f"翻訳済み: {len(existing)}")
    print(f"翻訳対象: {len(to_translate)}")

    if not to_translate:
        print("翻訳対象がありません。")
        return

    # Translate in batches of 10
    batch_size = 10
    all_translations = list(existing.values())

    for batch_start in range(0, len(to_translate), batch_size):
        batch = to_translate[batch_start:batch_start + batch_size]
        indices = [idx for idx, _ in batch]
        papers_batch = [p for _, p in batch]

        progress = batch_start + len(batch)
        print(f"  翻訳中... {progress}/{len(to_translate)} ({indices[0]}-{indices[-1]})")

        results = translate_batch(papers_batch, indices[0])

        # Map results back to correct indices
        for r in results:
            r_idx = r.get("index", -1)
            if r_idx >= 0:
                all_translations.append(r)

        time.sleep(0.5)

    # Save
    with open(trans_file, "w", encoding="utf-8") as f:
        json.dump(all_translations, f, ensure_ascii=False, indent=2)

    print(f"\n✓ {len(all_translations)}件の翻訳を保存: {trans_file}")


if __name__ == "__main__":
    main()
