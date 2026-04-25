#!/usr/bin/env python3
"""
Regenerate CLA analysis using enriched PESTLE data:
  - 1990-2020: yearly analysis (31 periods)
  - 2021-2026Q2: quarterly analysis (22 periods)

Total: 53 periods.
"""

import json
import os
import subprocess
import time
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

# Load API key from keychain
api_key = os.environ.get("ANTHROPIC_API_KEY")
if not api_key:
    try:
        api_key = subprocess.check_output(
            ["security", "find-generic-password", "-s", "ANTHROPIC_API_KEY", "-a", "anthropic", "-w"],
            text=True
        ).strip()
        os.environ["ANTHROPIC_API_KEY"] = api_key
    except Exception:
        pass

import anthropic

client = anthropic.Anthropic()
MODEL = "claude-haiku-4-5-20251001"

DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DATA_DIR / "future_insight.db"

PESTLE_CATS = ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]
PESTLE_JA = {
    "Political": "政治", "Economic": "経済", "Social": "社会",
    "Technological": "技術", "Legal": "法律", "Environmental": "環境",
}
PAPER_FIELDS = ["人文学", "社会科学", "自然科学", "工学", "芸術"]


def get_period_data(conn, start_date: str, end_date: str) -> dict:
    """Get news headlines and paper titles for a date range."""
    news = {}
    for cat in PESTLE_CATS:
        rows = conn.execute("""
            SELECT title, published_date FROM articles
            WHERE pestle_category = ? AND published_date >= ? AND published_date < ?
            ORDER BY relevance_score DESC, published_date DESC
            LIMIT 50
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

    return {"news": news, "papers": papers}


def build_context(data: dict) -> str:
    """Build context string for the CLA prompt."""
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


def generate_cla(period_label: str, context: str) -> dict:
    """Generate CLA analysis using Claude API."""
    prompt = f"""あなたは未来学・社会変動・因果階層分析（CLA）の専門家です。以下は「{period_label}」期間に収集されたPESTLE分野のニュース見出しと学術論文タイトルです。

{context}

これらのデータを素材に、PESTLE各分野（Political, Economic, Social, Technological, Legal, Environmental）＋全体統合（Overall）の計7カテゴリについて因果階層分析（CLA）を実施してください。

分析の要件:
1. **litany**: データに基づく具体的な事実・トレンドの要約（3-4文）
2. **systemic_causes**: 構造的・制度的な原因の分析（3-4文）
3. **worldview**: 暗黙の前提やイデオロギー、時代精神（2-3文）
4. **myth_metaphor**: 最深層の文化的物語や集合的無意識（2-3文）
5. **key_tension**: この時期の根本的な矛盾・葛藤（1-2文）
6. **emerging_narrative**: 浮かび上がる新しい物語（1-2文）

{period_label}の時代背景を踏まえ、抽象的な一般論ではなくデータに基づく具体的な分析を行ってください。

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
            response = client.messages.create(
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
            return json.loads(text)
        except json.JSONDecodeError as e:
            if attempt < 2:
                print(f"retry({attempt+1})...", end=" ", flush=True)
                time.sleep(2)
            else:
                print(f"[JSON ERROR: {e}]", end=" ", flush=True)
                return {}
        except Exception as e:
            print(f"[API ERROR: {e}]", end=" ", flush=True)
            if attempt < 2:
                time.sleep(5)
            return {}


def build_periods():
    """Build all periods to regenerate."""
    periods = []

    # 1990-2020: yearly
    for year in range(1990, 2021):
        periods.append((
            str(year),
            f"{year}年",
            f"{year}-01-01",
            f"{year + 1}-01-01",
        ))

    # 2021-2026: quarterly
    for year in range(2021, 2027):
        for q in range(1, 5):
            month_start = (q - 1) * 3 + 1
            month_end = q * 3 + 1
            end_year = year
            if month_end > 12:
                month_end = 1
                end_year = year + 1
            # Stop at 2026 Q2
            if year == 2026 and q > 2:
                break
            key = f"{year}-{month_start:02d}"
            label = f"{year}年Q{q}（{month_start}月〜{month_start + 2}月）"
            start = f"{year}-{month_start:02d}-01"
            end = f"{end_year}-{month_end:02d}-01"
            periods.append((key, label, start, end))

    return periods


def main():
    periods = build_periods()
    print(f"{'=' * 60}")
    print(f"  CLA Regeneration")
    print(f"  1990-2020: yearly ({sum(1 for k,_,_,_ in periods if len(k)==4)} periods)")
    print(f"  2021-2023: quarterly ({sum(1 for k,_,_,_ in periods if len(k)>4)} periods)")
    print(f"  Total: {len(periods)} periods")
    print(f"  Model: {MODEL}")
    print(f"{'=' * 60}\n")

    conn = sqlite3.connect(DB_PATH)

    # Load existing ai_analysis.json
    ai_path = DATA_DIR / "ai_analysis.json"
    with open(ai_path, encoding="utf-8") as f:
        ai_data = json.load(f)

    existing_qcla = ai_data.get("quarterly_cla", {})

    for i, (key, label, start, end) in enumerate(periods):
        print(f"[{i + 1}/{len(periods)}] {label}...", end=" ", flush=True)

        data = get_period_data(conn, start, end)
        total_items = sum(len(v) for v in data["news"].values()) + sum(len(v) for v in data["papers"].values())

        if total_items < 5:
            print(f"skipped (only {total_items} items)")
            continue

        print(f"({total_items} items)...", end=" ", flush=True)
        context = build_context(data)
        cla = generate_cla(label, context)

        if cla:
            existing_qcla[key] = cla
            print(f"OK ({len(cla)} categories)")
        else:
            print("FAILED")

        # Rate limiting — avoid hitting API limits
        time.sleep(1)

        # Save every 10 periods
        if (i + 1) % 10 == 0:
            ai_data["quarterly_cla"] = dict(sorted(existing_qcla.items()))
            with open(ai_path, "w", encoding="utf-8") as f:
                json.dump(ai_data, f, ensure_ascii=False, indent=2)
            print(f"  [checkpoint saved: {len(existing_qcla)} periods]\n")

    # Final save
    ai_data["quarterly_cla"] = dict(sorted(existing_qcla.items()))
    ai_data["analyzed_at"] = datetime.now(timezone.utc).isoformat()
    with open(ai_path, "w", encoding="utf-8") as f:
        json.dump(ai_data, f, ensure_ascii=False, indent=2)

    conn.close()

    print(f"\n{'=' * 60}")
    print(f"  COMPLETE: {len(existing_qcla)} total CLA periods")
    print(f"  Saved to: {ai_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
