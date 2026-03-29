#!/usr/bin/env python3
"""
Scenario Planning Generator for Future Insight App

CLA meta-analysis → Driving Forces → Impact-Uncertainty Assessment → 4 Scenarios
Uses existing CLA analysis as the primary input (not raw articles).
Based on Schwartz/GBN 8-step methodology, adapted for AI-assisted generation.
"""

import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

client = anthropic.Anthropic()
MODEL = os.getenv("SCENARIO_MODEL", "claude-haiku-4-5-20251001")

DATA_DIR = Path(__file__).parent.parent / "data"

# Minimum data requirements
MIN_SIGNALS = 5
MIN_CLA_CATEGORIES = 4


def load_data():
    """Load all input data files."""
    data = {}

    # CLA analysis + weak signals (primary input)
    ai_path = DATA_DIR / "ai_analysis.json"
    if not ai_path.exists():
        print("[ERROR] ai_analysis.json not found. Run ai_analyze.py first.")
        sys.exit(1)
    with open(ai_path, encoding="utf-8") as f:
        data["ai"] = json.load(f)

    # Alerts (CROSSOVER type for cross-category signals)
    alerts_path = DATA_DIR / "alerts.json"
    if alerts_path.exists():
        with open(alerts_path, encoding="utf-8") as f:
            data["alerts"] = json.load(f)
    else:
        data["alerts"] = {"alerts": []}

    # Latest news (for source article references)
    latest_path = DATA_DIR / "latest.json"
    if latest_path.exists():
        with open(latest_path, encoding="utf-8") as f:
            data["latest"] = json.load(f)
    else:
        data["latest"] = {}

    return data


def validate_data(data: dict) -> bool:
    """Check minimum data requirements."""
    ai = data["ai"]

    # Check CLA exists with enough categories
    cla = ai.get("cla", {})
    valid_categories = [k for k, v in cla.items() if v.get("key_tension")]
    if len(valid_categories) < MIN_CLA_CATEGORIES:
        print(f"[SKIP] CLA data insufficient: {len(valid_categories)} categories (need {MIN_CLA_CATEGORIES})")
        return False

    # Check weak signals exist
    signals = ai.get("weak_signals", [])
    if len(signals) < MIN_SIGNALS:
        print(f"[SKIP] Weak signals insufficient: {len(signals)} (need {MIN_SIGNALS})")
        return False

    return True


def extract_json(text: str) -> dict | list:
    """Extract JSON from Claude's response, handling markdown code blocks and truncation."""
    text = text.strip()

    # Try extracting from code blocks first
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

    # Try parsing the whole text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find JSON array or object boundaries
    for start_char, end_char in [("[", "]"), ("{", "}")]:
        start = text.find(start_char)
        if start == -1:
            continue
        # Find the matching closing bracket
        end = text.rfind(end_char)
        if end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass

    # If truncated JSON array, try to repair by closing brackets
    start = text.find("[")
    if start != -1:
        candidate = text[start:]
        # Try progressively truncating and closing
        for trim in ["}", "},", '"}', '"},' ]:
            last = candidate.rfind(trim)
            if last > 0:
                repaired = candidate[:last + len(trim.rstrip(","))] + "]"
                try:
                    return json.loads(repaired)
                except json.JSONDecodeError:
                    continue

    raise json.JSONDecodeError("Could not extract JSON from response", text, 0)


def call_claude(prompt: str, max_tokens: int = 8000, retries: int = 3) -> str:
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
                print(f"  Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


# ===== Step 1: CLA Meta-Analysis → Driving Forces =====

def step1_driving_forces(data: dict) -> list:
    """Extract driving forces from CLA key_tensions and emerging_narratives."""
    print("\n=== Step 1: CLAメタ解析 → ドライビングフォース抽出 ===")

    ai = data["ai"]
    cla = ai["cla"]
    signals = ai.get("weak_signals", [])
    alerts = data["alerts"].get("alerts", [])

    # Build CLA summary for prompt
    cla_summary = ""
    for cat in ["Political", "Economic", "Social", "Technological", "Legal", "Environmental", "Overall"]:
        if cat not in cla:
            continue
        c = cla[cat]
        cla_summary += f"\n### {cat}\n"
        cla_summary += f"- key_tension: {c.get('key_tension', 'N/A')}\n"
        cla_summary += f"- emerging_narrative: {c.get('emerging_narrative', 'N/A')}\n"
        cla_summary += f"- systemic_causes: {c.get('systemic_causes', 'N/A')}\n"

    # High-impact weak signals
    high_signals = [s for s in signals if s.get("potential_impact") == "High"][:20]
    signals_text = "\n".join(
        f"- [{', '.join(s.get('pestle_categories', []))}] {s['signal']}: {s.get('description', '')[:100]}"
        for s in high_signals
    )

    # CROSSOVER alerts (cross-category)
    crossover = [a for a in alerts if a.get("type") == "CROSSOVER"]
    crossover_text = "\n".join(
        f"- [{a.get('topic', '')}] {a.get('alert_title', '')} ({a.get('n_categories', 0)}カテゴリ横断)"
        for a in crossover
    )

    prompt = f"""あなたはシナリオプランニングの専門家です。以下のCLA（因果階層分析）のメタ解析データから、ドライビングフォース（駆動力）を抽出してください。

## CLA分析データ（7領域のkey_tension + emerging_narrative + systemic_causes）

{cla_summary}

## 高インパクト弱いシグナル

{signals_text}

## カテゴリ横断アラート（CROSSOVER）

{crossover_text if crossover_text else "（なし）"}

## 指示

以下の手順でドライビングフォースを統合してください。

1. CLA各カテゴリの key_tension を出発点とする。各 key_tension は根本的矛盾であり、ドライビングフォースの候補である。
2. 弱いシグナルとCROSSOVERアラートを参照し、key_tension 間の相互関係・重複・統合可能性を評価する。
   - 複数カテゴリにまたがるtensionは統合してクロスカッティングなDFとする
   - 独立性の高いtensionはそのままDFとする
3. emerging_narrative を参照し、各DFの将来の方向性（不確実性の一方の極）を付与する。
4. 最終的に 8-12 のドライビングフォースを出力する。

## 出力形式

以下のJSON配列で返してください。JSONのみ、説明は不要です。

```json
[
  {{
    "id": "df_01",
    "name": "名称（日本語、簡潔に）",
    "name_en": "English name",
    "description": "systemic_causesを参照した構造的説明（80-120字）",
    "description_en": "English description",
    "origin_cla_categories": ["Political"],
    "pestle_categories": ["Political", "Legal"],
    "related_signals": ["弱いシグナル名1"],
    "direction_positive": "emerging_narrativeに基づく楽観的方向",
    "direction_negative": "現状維持・悪化方向"
  }}
]
```"""

    text = call_claude(prompt, max_tokens=6000)
    driving_forces = extract_json(text)

    print(f"  → {len(driving_forces)} のドライビングフォースを抽出")
    for df in driving_forces:
        print(f"    - {df['name']}")

    return driving_forces


# ===== Step 2: Impact-Uncertainty Assessment + Axis Selection =====

def step2_assessment(data: dict, driving_forces: list) -> dict:
    """Assess impact/uncertainty and select scenario axes."""
    print("\n=== Step 2: 重要度-不確実性評価 + 軸選定 ===")

    cla = data["ai"]["cla"]

    # Build worldview/myth context
    deep_context = ""
    for cat in ["Political", "Economic", "Social", "Technological", "Legal", "Environmental", "Overall"]:
        if cat not in cla:
            continue
        c = cla[cat]
        deep_context += f"\n### {cat}\n"
        deep_context += f"- worldview: {c.get('worldview', 'N/A')}\n"
        deep_context += f"- myth_metaphor: {c.get('myth_metaphor', 'N/A')}\n"

    overall_tension = cla.get("Overall", {}).get("key_tension", "")

    df_text = json.dumps(driving_forces, ensure_ascii=False, indent=2)

    prompt = f"""あなたはシナリオプランニングの専門家です。以下のドライビングフォースについて、重要度と不確実性を評価し、シナリオの2軸を選定してください。

## ドライビングフォース一覧

{df_text}

## CLA深層分析（worldview + myth_metaphor）— 評価の参考

{deep_context}

## Overall key_tension（カテゴリ横断の根本的矛盾）

{overall_tension}

## 焦点課題

「2030年に向けて、社会・技術・地政学の構造変化は我々の戦略環境をどう変化させるか」

## 指示

1. 各ドライビングフォースについて以下を評価:
   - impact（重要度 1-10）: 焦点課題への影響度。systemic_causesの構造的深さを考慮
   - uncertainty（不確実性 1-10）: 将来の予測困難度。worldviewレベルでの前提の揺らぎを考慮
   - quadrant: "critical_uncertainty" / "predetermined" / "monitor" / "background"

2. critical_uncertainty の中から、最も独立性の高い2つを選び、シナリオの軸とする。
   - 2つの軸は異なるCLAカテゴリに根ざしていること
   - Overall key_tension が示す二律背反を参照
   - emerging_narrative を一方の極、現行 worldview をもう一方の極として活用

## 出力形式

以下のJSONで返してください。JSONのみ、説明は不要です。

```json
{{
  "assessed_forces": [
    {{
      "id": "df_01",
      "impact": 8.5,
      "uncertainty": 7.0,
      "quadrant": "critical_uncertainty",
      "assessment_rationale": "評価理由（1文）"
    }}
  ],
  "axes": {{
    "x": {{
      "driving_force_id": "df_XX",
      "label": "軸ラベル（短く）",
      "label_en": "English label",
      "pole_positive": "楽観的/変革的な方向",
      "pole_negative": "現状維持的/悪化的な方向",
      "pole_positive_en": "English",
      "pole_negative_en": "English"
    }},
    "y": {{
      "driving_force_id": "df_YY",
      "label": "軸ラベル（短く）",
      "label_en": "English label",
      "pole_positive": "楽観的/変革的な方向",
      "pole_negative": "現状維持的/悪化的な方向",
      "pole_positive_en": "English",
      "pole_negative_en": "English"
    }}
  }},
  "predetermined_elements": [
    "全シナリオ共通の確定トレンド1",
    "全シナリオ共通の確定トレンド2"
  ],
  "selection_rationale": "軸の選定理由（2-3文）"
}}
```"""

    text = call_claude(prompt, max_tokens=6000)
    result = extract_json(text)

    # Merge assessment scores back into driving_forces
    scores = {a["id"]: a for a in result["assessed_forces"]}
    for df in driving_forces:
        if df["id"] in scores:
            df.update(scores[df["id"]])

    print(f"  → 軸X: {result['axes']['x']['label']}")
    print(f"  → 軸Y: {result['axes']['y']['label']}")
    print(f"  → 確定要素: {len(result.get('predetermined_elements', []))} 件")

    return result


# ===== Step 3: Scenario Narrative Generation =====

def step3_scenarios(data: dict, driving_forces: list, assessment: dict) -> list:
    """Generate 4 scenario narratives using full CLA depth."""
    print("\n=== Step 3: シナリオナラティブ生成 ===")

    cla = data["ai"]["cla"]
    signals = data["ai"].get("weak_signals", [])
    alerts = data["alerts"].get("alerts", [])

    # Full CLA for narrative depth
    cla_full = ""
    for cat in ["Political", "Economic", "Social", "Technological", "Legal", "Environmental", "Overall"]:
        if cat not in cla:
            continue
        c = cla[cat]
        cla_full += f"\n### {cat}\n"
        for field in ["litany", "systemic_causes", "worldview", "myth_metaphor", "key_tension", "emerging_narrative"]:
            cla_full += f"- {field}: {c.get(field, 'N/A')}\n"

    # Weak signals grouped by time horizon
    horizon_groups = {}
    for s in signals[:50]:
        h = s.get("time_horizon", "不明")
        horizon_groups.setdefault(h, []).append(s["signal"])
    signals_by_horizon = "\n".join(
        f"- {h}: {', '.join(sigs[:5])}" for h, sigs in horizon_groups.items()
    )

    axes = json.dumps(assessment["axes"], ensure_ascii=False, indent=2)
    predetermined = json.dumps(assessment.get("predetermined_elements", []), ensure_ascii=False)

    prompt = f"""あなたはシナリオプランニングの専門家であり、優れたナラティブライターです。
以下の2軸定義に基づき、4つのシナリオを生成してください。

## シナリオの2軸

{axes}

## 確定要素（全シナリオ共通）

{predetermined}

## CLA分析（全7領域・全層）— シナリオの深層構造

{cla_full}

## 弱いシグナル（時間軸別）

{signals_by_horizon}

## 時間軸

2030年（4年後）

## 指示

4つのシナリオを生成してください。各シナリオは2x2マトリクスの1象限に対応します。

**シナリオの質の基準:**
- ナラティブは500-800字の散文形式（箇条書きではなく文章で記述）
- 「この世界はどう見えるか」（litanyレベルの具体的描写）
- 「どのようにしてこうなったか」（systemic_causesレベルの経路説明）
- 「我々にとって何を意味するか」（worldviewレベルの意味づけ）
- 現在のCLAの myth_metaphor を参照し、各シナリオがどの深層的物語の上に成り立つかを示す
- 4つのシナリオは、現在のCLAの emerging_narrative が異なる形で実現する（あるいは実現しない）世界として構成する

## 出力形式

以下のJSON配列で返してください。JSONのみ、説明は不要です。

```json
[
  {{
    "id": "sc_01",
    "quadrant": "top_right",
    "name": "シナリオ名（印象的かつ本質を突いた日本語名）",
    "name_en": "English scenario name",
    "subtitle": "X軸の極 × Y軸の極（短い説明）",
    "narrative": "500-800字の散文形式ナラティブ。「この世界はどう見えるか」「どうしてこうなったか」「何を意味するか」を含む。",
    "narrative_en": "English narrative (300-500 words)",
    "cla": {{
      "litany": "このシナリオ世界の表層的な出来事・状況（2-3文）",
      "systemic_causes": "この世界を生み出した構造的要因（2-3文）",
      "worldview": "この世界を支える暗黙の前提（2-3文）",
      "myth_metaphor": "この世界の深層にある文化的物語（1-2文）"
    }},
    "key_events": [
      {{"year": 2027, "event": "出来事の説明", "event_en": "English"}},
      {{"year": 2028, "event": "出来事の説明", "event_en": "English"}},
      {{"year": 2030, "event": "出来事の説明", "event_en": "English"}}
    ],
    "implications": [
      "戦略的含意1",
      "戦略的含意2",
      "戦略的含意3"
    ],
    "implications_en": [
      "Strategic implication 1",
      "Strategic implication 2",
      "Strategic implication 3"
    ],
    "signposts": [
      {{
        "indicator": "早期警戒指標の名称",
        "indicator_en": "English indicator name",
        "current_state": "現在の状態",
        "trigger_condition": "このシナリオが展開中であることを示す条件",
        "monitoring_keywords": ["keyword1", "keyword2"]
      }}
    ],
    "wild_cards": [
      "予期せぬ出来事でこのシナリオを加速/破壊するもの"
    ]
  }},
  {{
    "id": "sc_02",
    "quadrant": "top_left",
    "...": "同じ構造"
  }},
  {{
    "id": "sc_03",
    "quadrant": "bottom_right",
    "...": "同じ構造"
  }},
  {{
    "id": "sc_04",
    "quadrant": "bottom_left",
    "...": "同じ構造"
  }}
]
```

**重要:** top_right = X正×Y正, top_left = X負×Y正, bottom_right = X正×Y負, bottom_left = X負×Y負"""

    text = call_claude(prompt, max_tokens=16000)
    scenarios = extract_json(text)

    print(f"  → {len(scenarios)} シナリオを生成")
    for sc in scenarios:
        print(f"    - [{sc['quadrant']}] {sc['name']}")

    return scenarios


# ===== Step 4: Integration =====

def step4_integrate(data: dict, driving_forces: list, assessment: dict, scenarios: list) -> dict:
    """Integrate all components into final scenarios.json."""
    print("\n=== Step 4: 統合・検証・出力 ===")

    # Build source article references for driving forces
    latest = data.get("latest", {})
    pestle = latest.get("pestle", {})
    all_articles = []
    for cat, info in pestle.items():
        for a in info.get("articles", [])[:10]:
            all_articles.append({
                "title": a.get("title", ""),
                "date": a.get("published_date", a.get("published", "")),
                "category": cat,
            })

    # Attach top relevant articles to each driving force
    for df in driving_forces:
        cats = df.get("pestle_categories", [])
        df["source_articles"] = [
            a for a in all_articles if a["category"] in cats
        ][:3]

    # Scenario colors
    color_map = {
        "top_right": "#4a8c5c",
        "top_left": "#2c8acc",
        "bottom_right": "#cd8a32",
        "bottom_left": "#b92a38",
    }
    for sc in scenarios:
        sc["color"] = color_map.get(sc.get("quadrant"), "#783c28")

    # Extract no-regret moves from predetermined elements
    predetermined = assessment.get("predetermined_elements", [])

    # Count input data
    signals_count = len(data["ai"].get("weak_signals", []))
    alerts_count = len(data["alerts"].get("alerts", []))
    articles_count = sum(
        info.get("count", len(info.get("articles", [])))
        for info in pestle.values()
    )

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "version": 1,
        "focal_question": "2030年に向けて、社会・技術・地政学の構造変化は我々の戦略環境をどう変化させるか",
        "focal_question_en": "How will structural changes in society, technology, and geopolitics reshape our strategic environment toward 2030?",
        "time_horizon": "2030",
        "driving_forces": driving_forces,
        "axes": assessment["axes"],
        "predetermined_elements": predetermined,
        "scenarios": scenarios,
        "no_regret_moves": predetermined,  # Will be enhanced in future versions
        "metadata": {
            "input_articles_count": articles_count,
            "input_signals_count": signals_count,
            "input_alerts_count": alerts_count,
            "cla_categories_used": len([
                k for k in data["ai"].get("cla", {})
                if data["ai"]["cla"][k].get("key_tension")
            ]),
            "model": MODEL,
            "generation_method": "cla_meta_analysis + schwartz_gbn + ai_assisted",
        },
    }

    return result


def main():
    print("=" * 60)
    print("  Future Insight App — Scenario Planning Generator")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # Load data
    data = load_data()

    # Validate
    if not validate_data(data):
        print("\n[SKIP] Data requirements not met. Exiting.")
        sys.exit(0)

    # Step 1: CLA meta-analysis → driving forces
    driving_forces = step1_driving_forces(data)

    time.sleep(1)  # Rate limiting between API calls

    # Step 2: Impact-uncertainty assessment + axis selection
    assessment = step2_assessment(data, driving_forces)

    time.sleep(1)

    # Step 3: Scenario narrative generation
    scenarios = step3_scenarios(data, driving_forces, assessment)

    # Step 4: Integration
    result = step4_integrate(data, driving_forces, assessment, scenarios)

    # Write output
    output_path = DATA_DIR / "scenarios.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n=== 完了: {output_path} ===")
    print(f"  ドライビングフォース: {len(driving_forces)} 件")
    print(f"  シナリオ: {len(scenarios)} 件")
    print(f"  軸X: {result['axes']['x']['label']}")
    print(f"  軸Y: {result['axes']['y']['label']}")


if __name__ == "__main__":
    main()
