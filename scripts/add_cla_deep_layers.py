#!/usr/bin/env python3
"""
Add key_tension and emerging_narrative fields to existing CLA historical data.

Reads cla_historical_yearly.json and cla_historical_quarterly.json,
and for each period+category that lacks these fields, calls Claude API
to generate them based on the existing 4-layer CLA data.

This is an incremental update — existing fields are preserved.
"""

import json
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

client = anthropic.Anthropic()
MODEL = "claude-haiku-4-5-20251001"

DATA_DIR = Path(__file__).parent.parent / "data"

PESTLE_CATS = ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]


def extract_json(text: str) -> dict:
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
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass
    raise json.JSONDecodeError("Could not extract JSON", text, 0)


def generate_deep_layers(period: str, categories: dict) -> dict:
    """Generate key_tension and emerging_narrative for all categories in a period."""

    # Build context from existing 4-layer CLA
    context_parts = []
    for cat in PESTLE_CATS:
        cat_data = categories.get(cat, {})
        if not cat_data:
            continue
        context_parts.append(f"\n## {cat}")
        for key in ["litany", "systemic_causes", "worldview", "myth_metaphor"]:
            val = cat_data.get(key, "")
            if val:
                context_parts.append(f"- {key}: {val}")

    context = "\n".join(context_parts)

    prompt = f"""以下は「{period}」のCLA（因果階層分析）4層の既存データです。

{context}

各PESTLEカテゴリについて、以下の2つの追加分析を行ってください:

1. **key_tension**: この時期の核心的緊張 — 4層の分析から浮かび上がる、最も根本的な矛盾・葛藤・対立構造（2-3文）
2. **emerging_narrative**: 新たな物語 — この時期に芽生えつつある新しい社会的物語・パラダイム・可能性（2-3文）

以下のJSON形式で返してください:
{{
  "Political": {{
    "key_tension": "...",
    "emerging_narrative": "..."
  }},
  "Economic": {{ "key_tension": "...", "emerging_narrative": "..." }},
  "Social": {{ "key_tension": "...", "emerging_narrative": "..." }},
  "Technological": {{ "key_tension": "...", "emerging_narrative": "..." }},
  "Legal": {{ "key_tension": "...", "emerging_narrative": "..." }},
  "Environmental": {{ "key_tension": "...", "emerging_narrative": "..." }}
}}

{period}の時代背景を踏まえて深い分析を行ってください。JSONのみ返してください。"""

    for attempt in range(3):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            return extract_json(response.content[0].text)
        except Exception as e:
            if attempt < 2:
                print(f" retry({attempt + 1})...", end="", flush=True)
                time.sleep(2)
            else:
                print(f" [ERROR] {e}")
                return {}


def process_entries(entries: list, label: str) -> list:
    """Add deep layers to a list of CLA entries."""
    total = len(entries)
    updated = 0

    for i, entry in enumerate(entries):
        period = entry.get("period", "?")
        categories = entry.get("categories", {})

        # Check if any category already has key_tension
        has_deep = any(
            categories.get(cat, {}).get("key_tension")
            for cat in PESTLE_CATS
        )
        if has_deep:
            print(f"  [{i+1}/{total}] {period} ... skipped (already has deep layers)")
            continue

        print(f"  [{i+1}/{total}] {period} ...", end=" ", flush=True)
        deep = generate_deep_layers(period, categories)

        if deep:
            for cat in PESTLE_CATS:
                cat_deep = deep.get(cat, {})
                if cat_deep and cat in categories:
                    categories[cat]["key_tension"] = cat_deep.get("key_tension", "")
                    categories[cat]["emerging_narrative"] = cat_deep.get("emerging_narrative", "")
            updated += 1
            print(f"done")
        else:
            print(f"failed")

        time.sleep(0.5)

    print(f"  {label}: {updated}/{total} entries updated")
    return entries


def main():
    print(f"{'=' * 60}")
    print(f"  Add Deep Layers (key_tension + emerging_narrative)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'=' * 60}")

    # Process yearly
    yearly_path = DATA_DIR / "cla_historical_yearly.json"
    if yearly_path.exists():
        with open(yearly_path, encoding="utf-8") as f:
            yearly = json.load(f)
        print(f"\n[1/2] Yearly entries: {len(yearly)}")
        yearly = process_entries(yearly, "Yearly")
        with open(yearly_path, "w", encoding="utf-8") as f:
            json.dump(yearly, f, ensure_ascii=False, indent=2)
        print(f"  Saved: {yearly_path}")
    else:
        print("[WARN] cla_historical_yearly.json not found")

    # Process quarterly
    quarterly_path = DATA_DIR / "cla_historical_quarterly.json"
    if quarterly_path.exists():
        with open(quarterly_path, encoding="utf-8") as f:
            quarterly = json.load(f)
        print(f"\n[2/2] Quarterly entries: {len(quarterly)}")
        quarterly = process_entries(quarterly, "Quarterly")
        with open(quarterly_path, "w", encoding="utf-8") as f:
            json.dump(quarterly, f, ensure_ascii=False, indent=2)
        print(f"  Saved: {quarterly_path}")
    else:
        print("[WARN] cla_historical_quarterly.json not found")

    print(f"\n{'=' * 60}")
    print(f"  Complete!")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
