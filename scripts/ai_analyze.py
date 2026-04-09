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
            translations[cat] = json.loads(text)
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

def extract_weak_signals(news: dict, papers: list) -> list:
    """Extract weak signals from news and papers."""
    print("\n=== 3. ウィークシグナル抽出 ===")

    # Compile headlines (top 30 per category to stay within token limits)
    all_headlines = []
    for cat, info in news["pestle"].items():
        for a in info["articles"][:30]:
            all_headlines.append(f"[{info['label_ja']}] {a['title']}")

    # Add paper titles
    for p in papers[:50]:
        all_headlines.append(f"[学術/{p.get('field','')}] {p['title']}")

    headlines_text = "\n".join(all_headlines)

    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{
            "role": "user",
            "content": f"""あなたは未来学の専門家で、ウィークシグナル（弱い信号）の検出に長けています。

ウィークシグナルとは:
- 現時点ではまだ主流ではないが、将来大きな変化をもたらす可能性のある兆し
- 異なる分野を横断する予期しない接続
- 既存のトレンドに対する反動や逆説的な動き
- 少数派だが重要な動き

以下は本日収集されたニュースと学術論文の見出しです:

{headlines_text}

これらからウィークシグナルを8-10個抽出してください。

JSON配列で返してください。各要素:
{{
  "signal": "シグナルの名称（短く）",
  "description": "このシグナルの説明と、なぜ重要か（2-3文）",
  "related_headlines": ["関連する見出し1", "関連する見出し2"],
  "pestle_categories": ["関連するPESTLE分野"],
  "potential_impact": "high/medium/low",
  "time_horizon": "1-3年/3-5年/5-10年/10年以上",
  "counter_trend": "このシグナルが反する既存トレンド（1文）"
}}

JSONのみ返してください。"""
        }],
    )

    try:
        text = response.content[0].text.strip()
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
        # Try direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        # Try extracting array from text
        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end > start:
            return json.loads(text[start:end + 1])
        print("    [WARN] Failed to parse weak signals")
        return []
    except (json.JSONDecodeError, IndexError) as e:
        print(f"    [WARN] Failed to parse weak signals: {e}")
        return []


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
