#!/usr/bin/env python3
"""
Parallel CLA regeneration — 5 concurrent API calls for ~5x speedup.
Skips already-completed periods.
"""

import json
import sqlite3
import time
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

MODEL = "claude-sonnet-4-6"
MAX_WORKERS = 5  # concurrent API calls

DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DATA_DIR / "future_insight.db"

PESTLE_CATS = ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]
PESTLE_JA = {
    "Political": "政治", "Economic": "経済", "Social": "社会",
    "Technological": "技術", "Legal": "法律", "Environmental": "環境",
}
PAPER_FIELDS = ["人文学", "社会科学", "自然科学", "工学", "芸術"]

# Each thread gets its own client to avoid connection issues
def make_client():
    return anthropic.Anthropic()


def get_period_data(start_date: str, end_date: str) -> dict:
    """Get news + papers for a period. Uses own connection (thread-safe)."""
    conn = sqlite3.connect(DB_PATH)
    news = {}
    for cat in PESTLE_CATS:
        rows = conn.execute("""
            SELECT title, published_date FROM articles
            WHERE pestle_category = ? AND published_date >= ? AND published_date < ?
            ORDER BY relevance_score DESC, published_date DESC LIMIT 50
        """, (cat, start_date, end_date)).fetchall()
        news[cat] = [(r[0], r[1]) for r in rows]

    papers = {}
    for field in PAPER_FIELDS:
        rows = conn.execute("""
            SELECT title FROM papers
            WHERE field = ? AND published_date >= ? AND published_date < ?
            ORDER BY relevance_score DESC LIMIT 15
        """, (field, start_date, end_date)).fetchall()
        papers[field] = [r[0] for r in rows]

    conn.close()
    return {"news": news, "papers": papers}


def build_context(data: dict) -> str:
    parts = []
    for cat in PESTLE_CATS:
        items = data["news"].get(cat, [])
        if items:
            parts.append(f"[{PESTLE_JA[cat]}({cat})] ({len(items)}件)")
            for title, date in items[:30]:
                parts.append(f"  - [{date}] {title}")
            parts.append("")

    paper_items = [(f, data["papers"][f]) for f in PAPER_FIELDS if data["papers"].get(f)]
    if paper_items:
        parts.append("[学術論文]")
        for field, titles in paper_items:
            parts.append(f"  {field}:")
            for t in titles[:10]:
                parts.append(f"    - {t}")
            parts.append("")
    return "\n".join(parts)


def generate_cla_for_period(key: str, label: str, start: str, end: str) -> tuple[str, dict | None]:
    """Worker function: fetch data + call API. Returns (key, result)."""
    data = get_period_data(start, end)
    total = sum(len(v) for v in data["news"].values()) + sum(len(v) for v in data["papers"].values())

    if total < 5:
        return key, None

    context = build_context(data)
    api_client = make_client()

    prompt = f"""あなたは未来学・社会変動・因果階層分析（CLA）の専門家です。以下は「{label}」期間に収集されたPESTLE分野のニュース見出しと学術論文タイトルです。

{context}

これらのデータを素材に、PESTLE各分野（Political, Economic, Social, Technological, Legal, Environmental）＋全体統合（Overall）の計7カテゴリについて因果階層分析（CLA）を実施してください。

分析の要件:
1. **litany**: データに基づく具体的な事実・トレンドの要約（3-4文）
2. **systemic_causes**: 構造的・制度的な原因の分析（3-4文）
3. **worldview**: 暗黙の前提やイデオロギー、時代精神（2-3文）
4. **myth_metaphor**: 最深層の文化的物語や集合的無意識（2-3文）
5. **key_tension**: この時期の根本的な矛盾・葛藤（1-2文）
6. **emerging_narrative**: 浮かび上がる新しい物語（1-2文）

{label}の時代背景を踏まえ、抽象的な一般論ではなくデータに基づく具体的な分析を行ってください。

以下のJSON形式で、日本語で返してください:
{{
  "Political": {{
    "litany": "...",
    "systemic_causes": "...",
    "worldview": "...",
    "myth_metaphor": "...",
    "key_tension": "...",
    "emerging_narrative": "..."
  }},
  "Economic": {{ ... }},
  "Social": {{ ... }},
  "Technological": {{ ... }},
  "Legal": {{ ... }},
  "Environmental": {{ ... }},
  "Overall": {{ ... }}
}}

JSONのみ返してください。"""

    for attempt in range(3):
        try:
            response = api_client.messages.create(
                model=MODEL,
                max_tokens=8192,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            result = json.loads(text)
            return key, result
        except json.JSONDecodeError:
            if attempt < 2:
                time.sleep(2)
            else:
                return key, None
        except Exception as e:
            if attempt < 2:
                time.sleep(5)
            else:
                print(f"  [{key} ERROR: {e}]")
                return key, None

    return key, None


def build_periods():
    periods = []
    for year in range(1990, 2021):
        periods.append((str(year), f"{year}年", f"{year}-01-01", f"{year + 1}-01-01"))
    for year in range(2021, 2024):
        for q in range(1, 5):
            ms = (q - 1) * 3 + 1
            me = q * 3 + 1
            ey = year
            if me > 12:
                me = 1
                ey = year + 1
            key = f"{year}-{ms:02d}"
            label = f"{year}年Q{q}（{ms}月〜{ms + 2}月）"
            periods.append((key, label, f"{year}-{ms:02d}-01", f"{ey}-{me:02d}-01"))
    return periods


def main():
    all_periods = build_periods()

    # Load existing data to find already-completed new-format entries
    ai_path = DATA_DIR / "ai_analysis.json"
    with open(ai_path, encoding="utf-8") as f:
        ai_data = json.load(f)
    existing_qcla = ai_data.get("quarterly_cla", {})

    # Determine which periods need regeneration
    # Check if already regenerated by looking at content length (new ones are longer)
    to_generate = []
    for key, label, start, end in all_periods:
        existing = existing_qcla.get(key, {})
        overall = existing.get("Overall", {})
        litany = overall.get("litany", "")
        # New Sonnet-generated entries are typically > 80 chars
        if len(litany) < 80:
            to_generate.append((key, label, start, end))

    print(f"{'=' * 60}")
    print(f"  Parallel CLA Regeneration ({MAX_WORKERS} workers)")
    print(f"  Total periods: {len(all_periods)}")
    print(f"  Already done:  {len(all_periods) - len(to_generate)}")
    print(f"  To generate:   {len(to_generate)}")
    print(f"  Model: {MODEL}")
    print(f"{'=' * 60}\n")

    if not to_generate:
        print("All periods already completed!")
        return

    completed = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(generate_cla_for_period, key, label, start, end): (key, label)
            for key, label, start, end in to_generate
        }

        for future in as_completed(futures):
            key, label = futures[future]
            try:
                result_key, cla = future.result()
                if cla:
                    existing_qcla[result_key] = cla
                    completed += 1
                    print(f"  [{completed + failed}/{len(to_generate)}] {label} OK")
                else:
                    failed += 1
                    print(f"  [{completed + failed}/{len(to_generate)}] {label} SKIPPED/FAILED")
            except Exception as e:
                failed += 1
                print(f"  [{completed + failed}/{len(to_generate)}] {label} ERROR: {e}")

            # Save checkpoint every 5 completions
            if (completed + failed) % 5 == 0:
                ai_data["quarterly_cla"] = dict(sorted(existing_qcla.items()))
                with open(ai_path, "w", encoding="utf-8") as f:
                    json.dump(ai_data, f, ensure_ascii=False, indent=2)

    # Final save
    ai_data["quarterly_cla"] = dict(sorted(existing_qcla.items()))
    ai_data["analyzed_at"] = datetime.now(timezone.utc).isoformat()
    with open(ai_path, "w", encoding="utf-8") as f:
        json.dump(ai_data, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 60}")
    print(f"  COMPLETE: {completed} succeeded, {failed} failed")
    print(f"  Total CLA periods: {len(existing_qcla)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
