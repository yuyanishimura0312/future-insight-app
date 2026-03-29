#!/usr/bin/env python3
"""
Generate historical CLA (Causal Layered Analysis) from 1990 to present.
- 1990-2019: yearly analysis
- 2020-2026: quarterly analysis
Uses news articles and academic papers from the DB for each period.
"""

import json
import time
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

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
    # News by PESTLE category
    news = {}
    for cat in PESTLE_CATS:
        rows = conn.execute("""
            SELECT title FROM articles
            WHERE pestle_category = ? AND published_date >= ? AND published_date < ?
            ORDER BY relevance_score DESC LIMIT 30
        """, (cat, start_date, end_date)).fetchall()
        news[cat] = [r[0] for r in rows]

    # Papers by field
    papers = {}
    for field in PAPER_FIELDS:
        rows = conn.execute("""
            SELECT title FROM papers
            WHERE field = ? AND published_date >= ? AND published_date < ?
            ORDER BY relevance_score DESC LIMIT 15
        """, (field, start_date, end_date)).fetchall()
        papers[field] = [r[0] for r in rows]

    return {"news": news, "papers": papers}


def generate_cla_for_period(period_label: str, data: dict) -> dict:
    """Generate CLA analysis for all categories in one API call."""
    # Build context from news and papers
    context_parts = []
    for cat in PESTLE_CATS:
        headlines = data["news"].get(cat, [])
        if headlines:
            context_parts.append(f"[{PESTLE_JA[cat]}({cat})]")
            for h in headlines[:20]:
                context_parts.append(f"  - {h}")

    context_parts.append("\n[学術論文]")
    for field, titles in data["papers"].items():
        if titles:
            context_parts.append(f"  {field}:")
            for t in titles[:10]:
                context_parts.append(f"    - {t}")

    context = "\n".join(context_parts)

    if len(context.strip()) < 50:
        return {}

    prompt = f"""あなたは未来学・社会変動の専門家です。以下は「{period_label}」期間に収集されたPESTLE分野のニュース見出しと学術論文タイトルです。

{context}

これらを素材に、PESTLE各分野（Political, Economic, Social, Technological, Legal, Environmental）＋全体（Overall）の計7カテゴリについて因果階層分析（CLA）を実施してください。

以下のJSON形式で返してください:
{{
  "Political": {{
    "litany": "リタニー（表層的事実・トレンドの要約、2-3文）",
    "systemic_causes": "社会的・システム的原因（構造的要因、2-3文）",
    "worldview": "世界観・ディスコース（無意識の前提やイデオロギー、2-3文）",
    "myth_metaphor": "神話・メタファー（最深層の文化的物語、1-2文）",
    "key_tension": "核心的な緊張・矛盾（1文）",
    "emerging_narrative": "浮上しつつある新しいナラティブ（1文）"
  }},
  "Economic": {{ ... }},
  "Social": {{ ... }},
  "Technological": {{ ... }},
  "Legal": {{ ... }},
  "Environmental": {{ ... }},
  "Overall": {{ ... }}
}}

{period_label}の時代背景を踏まえて分析してください。JSONのみ返してください。"""

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
            return json.loads(text)
        except json.JSONDecodeError as e:
            if attempt < 2:
                print(f"retry({attempt+1})...", end=" ", flush=True)
                time.sleep(1)
            else:
                print(f"    [ERROR] {e}")
                return {}
        except Exception as e:
            print(f"    [ERROR] {e}")
            return {}


def build_periods() -> list:
    """Build list of (key, label, start_date, end_date) tuples."""
    periods = []

    # 1990-2019: yearly
    for year in range(1990, 2020):
        periods.append((
            str(year),
            f"{year}年",
            f"{year}-01-01",
            f"{year + 1}-01-01",
        ))

    # 2020-2026: quarterly
    for year in range(2020, 2027):
        for q in range(1, 5):
            month_start = (q - 1) * 3 + 1
            month_end = q * 3 + 1
            end_year = year
            if month_end > 12:
                month_end = 1
                end_year = year + 1
            key = f"{year}-{month_start:02d}"
            label = f"{year}年Q{q}（{month_start}月〜{month_start + 2}月）"
            start = f"{year}-{month_start:02d}-01"
            end = f"{end_year}-{month_end:02d}-01"
            # Don't go past current date
            if start > "2026-03-31":
                break
            periods.append((key, label, start, end))

    return periods


def main():
    print(f"=== Historical CLA Generation ===\n")

    conn = sqlite3.connect(DB_PATH)

    # Load existing ai_analysis.json
    ai_path = DATA_DIR / "ai_analysis.json"
    with open(ai_path, encoding="utf-8") as f:
        ai_data = json.load(f)

    existing_qcla = ai_data.get("quarterly_cla", {})
    print(f"Existing CLA periods: {len(existing_qcla)}")

    periods = build_periods()
    print(f"Target periods: {len(periods)}")

    # Skip periods that already have CLA data
    to_generate = []
    for key, label, start, end in periods:
        if key not in existing_qcla:
            to_generate.append((key, label, start, end))

    print(f"New periods to generate: {len(to_generate)}\n")

    for i, (key, label, start, end) in enumerate(to_generate):
        print(f"[{i + 1}/{len(to_generate)}] {label} ({key})...", end=" ", flush=True)

        data = get_period_data(conn, start, end)
        total_items = sum(len(v) for v in data["news"].values()) + sum(len(v) for v in data["papers"].values())

        if total_items < 5:
            print(f"skipped (only {total_items} items)")
            continue

        cla = generate_cla_for_period(label, data)
        if cla:
            existing_qcla[key] = cla
            print(f"OK ({len(cla)} categories, {total_items} source items)")
        else:
            print("failed")

        time.sleep(0.5)

        # Save periodically
        if (i + 1) % 10 == 0:
            ai_data["quarterly_cla"] = dict(sorted(existing_qcla.items()))
            with open(ai_path, "w", encoding="utf-8") as f:
                json.dump(ai_data, f, ensure_ascii=False, indent=2)
            print(f"  [saved {len(existing_qcla)} periods]")

    # Final save
    ai_data["quarterly_cla"] = dict(sorted(existing_qcla.items()))
    ai_data["analyzed_at"] = datetime.now(timezone.utc).isoformat()
    with open(ai_path, "w", encoding="utf-8") as f:
        json.dump(ai_data, f, ensure_ascii=False, indent=2)

    conn.close()
    print(f"\n=== Done: {len(existing_qcla)} total CLA periods saved ===")


if __name__ == "__main__":
    main()
