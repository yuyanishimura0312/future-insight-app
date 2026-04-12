#!/usr/bin/env python3
"""
AI Analysis for Future Insight App
Claude APIを使用して3つの分析を実行:
1. 日英翻訳 — 各記事のタイトル・要約を翻訳
2. CLA分析 — PESTLE分野ごとに因果階層分析（4層）
3. ウィークシグナル抽出 — 変化の兆しを検出
"""

import json
import os
import time
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

client = anthropic.Anthropic()
MODEL = "claude-haiku-4-5-20251001"  # Fast and cost-effective

DATA_DIR = Path(__file__).parent.parent / "data"


def load_latest_news() -> dict:
    with open(DATA_DIR / "latest.json", encoding="utf-8") as f:
        return json.load(f)


def load_papers() -> list:
    """Load papers from daily_papers.json (nested by field) or papers.json (flat list)."""
    # Prefer daily_papers.json which has the latest curated papers
    daily_path = DATA_DIR / "daily_papers.json"
    if daily_path.exists():
        with open(daily_path, encoding="utf-8") as f:
            data = json.load(f)
        papers = []
        for field_data in data.get("fields", {}).values():
            papers.extend(field_data.get("papers", []))
        if papers:
            return papers
    # Fallback to flat papers.json
    path = DATA_DIR / "papers.json"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ===== 1. Translation =====

def translate_articles(news: dict) -> dict:
    """Translate article titles and summaries between Japanese and English."""
    print("\n=== 1. 日英翻訳 ===")
    translations = {}

    for cat, info in news["pestle"].items():
        print(f"  {info['label_ja']} ({cat})...")
        articles_text = ""
        for i, a in enumerate(info["articles"][:20]):
            articles_text += f"[{i}] {a['title']}\n{a['summary'][:150]}\n\n"

        response = client.messages.create(
            model=MODEL,
            max_tokens=4000,
            messages=[{
                "role": "user",
                "content": f"""以下の{cat}分野のニュース記事リストについて、各記事のタイトルと要約を翻訳してください。
- 英語の記事 → 日本語に翻訳
- 日本語の記事 → 英語に翻訳

JSON配列で返してください。各要素は {{"index": 番号, "title_translated": "翻訳タイトル", "summary_translated": "翻訳要約(1-2文)"}} の形式。

記事リスト:
{articles_text}

JSONのみ返してください。説明は不要です。"""
            }],
        )

        try:
            text = response.content[0].text.strip()
            # Extract JSON from response
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            parsed = json.loads(text)
            # Normalize: Claude sometimes returns "summary_transformed" instead of "summary_translated"
            for item in parsed:
                if "summary_transformed" in item and "summary_translated" not in item:
                    item["summary_translated"] = item.pop("summary_transformed")
                if "title_transformed" in item and "title_translated" not in item:
                    item["title_translated"] = item.pop("title_transformed")
            translations[cat] = parsed
        except (json.JSONDecodeError, IndexError):
            translations[cat] = []
            print(f"    [WARN] Failed to parse translation for {cat}")

        time.sleep(0.5)  # Rate limiting

    return translations


# ===== 2. CLA Analysis =====

def cla_analysis(news: dict) -> dict:
    """Perform Causal Layered Analysis on each PESTLE category."""
    print("\n=== 2. CLA分析（因果階層分析） ===")
    cla_results = {}

    for cat, info in news["pestle"].items():
        print(f"  {info['label_ja']} ({cat})...")

        # Compile article headlines for context
        headlines = "\n".join(
            f"- {a['title']}" for a in info["articles"][:20]
        )

        response = client.messages.create(
            model=MODEL,
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": f"""あなたは未来学の専門家です。以下は本日の{info['label_ja']}（{cat}）分野の主要ニュース見出しです。

{headlines}

これらのニュースを素材として、因果階層分析（Causal Layered Analysis: CLA）を実施してください。

以下のJSON形式で返してください:
{{
  "litany": "リタニー（表層）: 今日のニュースから見える表層的な事実・トレンドの要約（2-3文）",
  "systemic_causes": "社会的・システム的原因: これらの事象を生み出している構造的要因（2-3文）",
  "worldview": "世界観・ディスコース: これらを支える無意識の前提やイデオロギー（2-3文）",
  "myth_metaphor": "神話・メタファー: 最深層にある文化的な物語や象徴（1-2文）",
  "key_tension": "この分野で見られる核心的な緊張・矛盾（1文）",
  "emerging_narrative": "浮上しつつある新しいナラティブ（1文）"
}}

JSONのみ返してください。"""
            }],
        )

        try:
            text = response.content[0].text.strip()
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            cla_results[cat] = json.loads(text)
        except (json.JSONDecodeError, IndexError):
            cla_results[cat] = {}
            print(f"    [WARN] Failed to parse CLA for {cat}")

        time.sleep(0.5)

    return cla_results


# ===== 3. Weak Signals =====

# 8 different perspectives for diverse signal generation (~100 total)
SIGNAL_BATCH_CONFIGS = [
    {"focus": "技術・イノベーション",
     "instruction": "特に技術革新、AI、デジタル化、科学技術の進歩に関連するシグナルに注目してください。技術が社会・経済・政治にもたらす予期しない変化の兆しを検出してください。",
     "count": 12},
    {"focus": "地政学・国際関係",
     "instruction": "国際関係、地政学的パワーバランス、戦争と平和、同盟関係の変化に関するシグナルに注目してください。従来の外交・安全保障の枠組みが崩れつつある兆しを検出してください。",
     "count": 12},
    {"focus": "経済・金融・労働",
     "instruction": "経済構造の変化、金融システム、労働市場、貿易、通貨、サプライチェーンに関するシグナルに注目してください。資本主義や経済秩序の転換点となりうる兆しを検出してください。",
     "count": 12},
    {"focus": "社会・文化・価値観",
     "instruction": "社会構造、文化的変化、価値観の転換、世代間ギャップ、アイデンティティに関するシグナルに注目してください。人々の生き方や社会の在り方が根本的に変わりつつある兆しを検出してください。",
     "count": 12},
    {"focus": "環境・気候・資源",
     "instruction": "気候変動、環境政策、エネルギー転換、資源管理、生態系に関するシグナルに注目してください。人類と自然の関係が変わりつつある兆しを検出してください。",
     "count": 12},
    {"focus": "法律・規制・ガバナンス",
     "instruction": "法制度、規制、人権、ガバナンス構造の変化に関するシグナルに注目してください。統治の仕組みや権力の正当性が問い直されている兆しを検出してください。",
     "count": 12},
    {"focus": "分野横断・逆説的動き",
     "instruction": "異なる分野を横断する予期しない接続、既存トレンドに対する逆説的な動き、少数派だが重要な兆しに特に注目してください。一見無関係な出来事の間の隠れた関連性を検出してください。",
     "count": 14},
    {"focus": "日本・アジア固有の動き",
     "instruction": "日本やアジア地域に特有の社会変化、政策転換、文化的シフトに関するシグナルに注目してください。西欧中心の分析では見落とされがちなアジアの兆しを検出してください。",
     "count": 14},
]


def _parse_signal_json(text: str) -> list:
    """Robustly parse JSON array of signals from LLM response."""
    import re
    # Try code block extraction
    if "```" in text:
        for block in re.findall(r"```(?:json)?\s*(.*?)```", text, re.DOTALL):
            try:
                return json.loads(block.strip())
            except json.JSONDecodeError:
                continue
    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try extracting array
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end > start:
        raw = text[start:end + 1]
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # Fix trailing commas
            raw = re.sub(r',\s*}', '}', raw)
            raw = re.sub(r',\s*]', ']', raw)
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                pass
    return []


def _generate_signal_batch(headlines_text: str, config: dict, existing_signals: list) -> list:
    """Generate one batch of signals from a specific perspective."""
    existing_titles = "\n".join(f"- {s['signal']}" for s in existing_signals)

    response = client.messages.create(
        model=MODEL,
        max_tokens=8192,
        messages=[{
            "role": "user",
            "content": f"""あなたは未来学の専門家で、ウィークシグナル（弱い信号）の検出に長けています。

ウィークシグナルとは:
- 現時点ではまだ主流ではないが、将来大きな変化をもたらす可能性のある兆し
- 異なる分野を横断する予期しない接続
- 既存のトレンドに対する反動や逆説的な動き
- 少数派だが重要な動き

【今回の視点: {config['focus']}】
{config['instruction']}

以下は本日収集されたニュースと学術論文の見出しです:

{headlines_text}

【重要: 以下のシグナルはすでに抽出済みなので、重複しないようにしてください】
{existing_titles}

上記と重複しない新しいウィークシグナルを正確に{config['count']}個抽出してください。

JSON配列で返してください。各要素:
{{
  "signal": "シグナルの名称（短く、日本語）",
  "description": "このシグナルの説明と、なぜ重要か（2-3文、日本語）",
  "related_headlines": ["関連する見出し1", "関連する見出し2", "関連する見出し3"],
  "pestle_categories": ["関連するPESTLE分野（政治/経済/社会/技術/法律/環境）"],
  "potential_impact": "high/medium/low",
  "time_horizon": "1-3年/3-5年/5-10年/10年以上",
  "counter_trend": "このシグナルが反する既存トレンド（1文）"
}}

必ず有効なJSON配列のみを返してください。説明文は不要です。"""
        }],
    )

    text = response.content[0].text.strip()
    return _parse_signal_json(text)


def extract_weak_signals(news: dict, papers: list) -> list:
    """Extract ~100 weak signals from news and papers using multiple perspectives."""
    print("\n=== 3. ウィークシグナル抽出（100個目標） ===")

    # Compile headlines (top 50 per category for richer context)
    all_headlines = []
    for cat, info in news["pestle"].items():
        for a in info["articles"][:50]:
            all_headlines.append(f"[{info['label_ja']}] {a['title']}")

    # Add paper titles
    for p in papers[:80]:
        all_headlines.append(f"[学術/{p.get('field','')}] {p['title']}")

    headlines_text = "\n".join(all_headlines)
    all_signals = []

    for i, config in enumerate(SIGNAL_BATCH_CONFIGS):
        print(f"  Batch {i+1}/{len(SIGNAL_BATCH_CONFIGS)}: {config['focus']} (target: {config['count']})")
        try:
            batch = _generate_signal_batch(headlines_text, config, all_signals)
            print(f"    -> {len(batch)} signals")
            all_signals.extend(batch)
        except Exception as e:
            print(f"    [WARN] Batch failed: {e}")
        time.sleep(0.5)

    # If we fell short (e.g. parse errors), run a supplementary batch
    if len(all_signals) < 90:
        shortfall = 100 - len(all_signals)
        print(f"  Supplementary batch: {shortfall} more needed")
        try:
            extra = _generate_signal_batch(headlines_text, {
                "focus": "補完（全分野横断）",
                "instruction": f"全分野を横断して、まだ検出されていないシグナルを{shortfall}個追加してください。",
                "count": shortfall,
            }, all_signals)
            print(f"    -> {len(extra)} signals")
            all_signals.extend(extra)
        except Exception as e:
            print(f"    [WARN] Supplementary batch failed: {e}")

    return all_signals


# ===== Main =====

def main():
    print(f"=== AI Analysis ({datetime.now(timezone.utc).strftime('%Y-%m-%d')}) ===")

    news = load_latest_news()
    papers = load_papers()

    # 1. Translation
    translations = translate_articles(news)

    # 2. CLA
    cla = cla_analysis(news)

    # 3. Weak Signals
    signals = extract_weak_signals(news, papers)
    print(f"\n  {len(signals)} weak signals detected")

    # Save results
    output = {
        "date": news["date"],
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "translations": translations,
        "cla": cla,
        "weak_signals": signals,
    }

    output_file = DATA_DIR / "ai_analysis.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Saved to {output_file}")


if __name__ == "__main__":
    main()
