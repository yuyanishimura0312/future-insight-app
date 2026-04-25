#!/usr/bin/env python3
"""
Generate CLA Meta-Analysis Report.

Reads cla_historical_yearly.json and cla_historical_quarterly.json,
then produces a comprehensive meta-analysis report (~5,000 chars each)
tracking myth evolution, paradigm shifts, and deep structural changes.

Generates two versions:
  - Japan-focused (日本版)
  - Global (グローバル版)

Output: data/cla_meta_report.json
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

PESTLE_CATS = ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]


def extract_json(text: str) -> dict:
    """Extract JSON from Claude's response."""
    text = text.strip()
    if "```" in text:
        parts = text.split("```")
        for part in parts[1:]:
            candidate = part.strip()
            if candidate.startswith("json"):
                candidate = candidate[4:].strip()
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except json.JSONDecodeError:
            pass
    raise json.JSONDecodeError("Could not extract JSON", text, 0)


def call_claude(prompt: str, max_tokens: int = 8192, retries: int = 3) -> str:
    """Call Claude API with retry logic."""
    for attempt in range(retries):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text
        except Exception as e:
            wait = 2 ** (attempt + 1)
            print(f"  [WARN] API call failed (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(wait)
            else:
                raise


def load_cla_data() -> tuple[list, list]:
    """Load yearly and quarterly CLA data."""
    yearly_path = DATA_DIR / "cla_historical_yearly.json"
    quarterly_path = DATA_DIR / "cla_historical_quarterly.json"

    yearly = []
    if yearly_path.exists():
        with open(yearly_path, encoding="utf-8") as f:
            yearly = json.load(f)
    else:
        print(f"[WARN] {yearly_path} not found")

    quarterly = []
    if quarterly_path.exists():
        with open(quarterly_path, encoding="utf-8") as f:
            quarterly = json.load(f)
    else:
        print(f"[WARN] {quarterly_path} not found")

    return yearly, quarterly


def build_myth_timeline(entries: list) -> str:
    """Extract myth_metaphor layer across all periods for timeline analysis."""
    lines = []
    for entry in entries:
        period = entry["period"]
        cats = entry.get("categories", {})
        myths = []
        for cat in PESTLE_CATS:
            cat_data = cats.get(cat, {})
            myth = cat_data.get("myth_metaphor", "")
            if myth:
                myths.append(f"    {cat}: {myth[:100]}")
        synthesis = entry.get("cross_category_synthesis", "")
        if myths or synthesis:
            lines.append(f"  [{period}]")
            lines.extend(myths)
            if synthesis:
                lines.append(f"    Synthesis: {synthesis[:150]}")
    return "\n".join(lines)


def build_worldview_timeline(entries: list) -> str:
    """Extract worldview layer across all periods."""
    lines = []
    for entry in entries:
        period = entry["period"]
        cats = entry.get("categories", {})
        views = []
        for cat in ["Political", "Economic", "Technological"]:
            cat_data = cats.get(cat, {})
            wv = cat_data.get("worldview", "")
            if wv:
                views.append(f"    {cat}: {wv[:100]}")
        if views:
            lines.append(f"  [{period}]")
            lines.extend(views)
    return "\n".join(lines)


def build_systemic_timeline(entries: list) -> str:
    """Extract systemic_causes layer, sampled at key periods."""
    lines = []
    # Sample every 5th entry for yearly, all for quarterly
    sample_indices = list(range(0, len(entries), max(1, len(entries) // 15)))
    for idx in sample_indices:
        if idx >= len(entries):
            break
        entry = entries[idx]
        period = entry["period"]
        cats = entry.get("categories", {})
        causes = []
        for cat in PESTLE_CATS:
            cat_data = cats.get(cat, {})
            sc = cat_data.get("systemic_causes", "")
            if sc:
                causes.append(f"    {cat}: {sc[:80]}")
        if causes:
            lines.append(f"  [{period}]")
            lines.extend(causes)
    return "\n".join(lines)


def generate_meta_report(yearly: list, quarterly: list, version: str) -> dict:
    """Generate a meta-analysis report for the given version (japan/global).

    Returns a dict with title, report_text, key_shifts, and metadata.
    """
    print(f"\n  Generating {version} meta-report...")

    all_entries = yearly + quarterly

    # Build timeline summaries for different CLA layers
    myth_timeline = build_myth_timeline(all_entries)
    worldview_timeline = build_worldview_timeline(all_entries)
    systemic_timeline = build_systemic_timeline(all_entries)

    # Collect all cross-category syntheses
    syntheses = []
    for entry in all_entries:
        s = entry.get("cross_category_synthesis", "")
        if s:
            syntheses.append(f"  [{entry['period']}] {s[:200]}")
    synthesis_text = "\n".join(syntheses)

    if version == "japan":
        focus = """日本の視点から分析してください。日本社会・経済・政治の変遷を中心に据え、
グローバルな変化が日本にどう影響したかを重点的に論じてください。
バブル崩壊、失われた30年、東日本大震災、コロナ禍、少子高齢化、AI革命などの
日本固有の文脈を踏まえてください。"""
        title_hint = "日本版"
    else:
        focus = """グローバルな視点から分析してください。冷戦終結後の世界秩序の変遷、
グローバリゼーション、テロとの戦い、金融危機、パンデミック、気候変動、
AI革命、地政学的再編などの世界的なメガトレンドを中心に論じてください。"""
        title_hint = "グローバル版"

    prompt = f"""あなたは未来学・社会変動分析の世界的権威です。以下は1990年から2026年にわたるCLA（因果階層分析）の時系列データです。

## 神話・メタファー層の変遷
{myth_timeline[:6000]}

## 世界観層の変遷（政治・経済・技術の主要3分野）
{worldview_timeline[:4000]}

## システム的原因層（サンプル）
{systemic_timeline[:3000]}

## カテゴリ横断統合分析の変遷
{synthesis_text[:4000]}

## 指示

上記のCLAデータを統合し、「{title_hint}」メタ分析レポートを生成してください。

{focus}

以下の構成で、約5,000字の日本語レポートを作成してください:

1. **神話の変遷**: 1990年代から2020年代にかけて、社会の深層にある支配的な神話・メタファーがどう変遷したか
2. **パラダイムシフトの追跡**: 世界観層で起きた主要なパラダイムシフト（3-5回）を特定し、それぞれの転換点を分析
3. **構造的因果連鎖**: システム層で繰り返し現れるパターンと、それが表層の出来事をどう生み出してきたか
4. **現在の深層構造**: 2024-2026年の最新データから見える、今まさに進行中の深層変動
5. **将来への示唆**: これまでの神話変遷パターンから推測される、次の大きな転換

重要: レポートは散文形式（地の文）で記述してください。箇条書きは補助的にのみ使用してください。

以下のJSON形式で返してください:
{{
  "title": "レポートタイトル",
  "report_text": "5,000字程度の散文形式レポート全文",
  "key_paradigm_shifts": [
    {{
      "period": "転換点の時期",
      "name": "パラダイムシフトの名称",
      "description": "説明（2-3文）"
    }}
  ],
  "dominant_myths_timeline": [
    {{
      "era": "時代区分",
      "myth": "支配的神話の要約"
    }}
  ]
}}

JSONのみ返してください。"""

    text = call_claude(prompt, max_tokens=16384)
    result = extract_json(text)

    # Validate
    if "report_text" not in result:
        raise ValueError("Missing report_text in response")

    report_len = len(result["report_text"])
    print(f"  -> {version}: {report_len} chars, "
          f"{len(result.get('key_paradigm_shifts', []))} paradigm shifts, "
          f"{len(result.get('dominant_myths_timeline', []))} myth eras")

    return result


def main():
    print(f"{'=' * 60}")
    print(f"  CLA Meta-Analysis Report Generator")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'=' * 60}")

    yearly, quarterly = load_cla_data()
    print(f"  Yearly entries: {len(yearly)}")
    print(f"  Quarterly entries: {len(quarterly)}")

    if not yearly and not quarterly:
        print("[ERROR] No CLA data found. Run generate_historical_cla.py first.")
        return

    # Generate Japan version
    japan_report = generate_meta_report(yearly, quarterly, "japan")

    time.sleep(1)

    # Generate Global version
    global_report = generate_meta_report(yearly, quarterly, "global")

    # Assemble output
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_coverage": {
            "yearly_periods": len(yearly),
            "quarterly_periods": len(quarterly),
            "total_periods": len(yearly) + len(quarterly),
            "year_range": f"1990-2026",
        },
        "japan": japan_report,
        "global": global_report,
    }

    output_path = DATA_DIR / "cla_meta_report.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 60}")
    print(f"  Complete: {output_path}")
    print(f"  Japan report: {len(japan_report.get('report_text', ''))} chars")
    print(f"  Global report: {len(global_report.get('report_text', ''))} chars")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
