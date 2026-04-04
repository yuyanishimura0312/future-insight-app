#!/usr/bin/env python3
"""
Generate Daily CLA Report.

Reads:
  - data/ai_analysis.json (daily CLA results from ai_analyze.py)
  - data/cla_meta_report.json (historical meta-analysis as foundation)
  - data/latest.json (today's PESTLE news)

Generates a ~5,000-char narrative report describing:
  "The myth transitions currently underway and the situation we find ourselves in"

Produces two versions:
  - Japan-focused (日本版)
  - Global (グローバル版)

Output: data/daily_report.json
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


def load_daily_cla() -> dict:
    """Load today's CLA analysis from ai_analysis.json."""
    path = DATA_DIR / "ai_analysis.json"
    if not path.exists():
        print("[ERROR] ai_analysis.json not found. Run ai_analyze.py first.")
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_meta_report() -> dict:
    """Load the historical meta-analysis report."""
    path = DATA_DIR / "cla_meta_report.json"
    if not path.exists():
        print("[WARN] cla_meta_report.json not found. Generating without historical context.")
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_latest_news() -> dict:
    """Load today's PESTLE news."""
    path = DATA_DIR / "latest.json"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def build_daily_cla_summary(ai_data: dict) -> str:
    """Build a summary of today's CLA analysis."""
    cla = ai_data.get("cla", {})
    lines = []
    for cat in PESTLE_CATS + ["Overall"]:
        cat_data = cla.get(cat, {})
        if not cat_data:
            continue
        lines.append(f"\n### {cat}")
        for field in ["litany", "systemic_causes", "worldview", "myth_metaphor",
                       "key_tension", "emerging_narrative"]:
            val = cat_data.get(field, "")
            if val:
                lines.append(f"  {field}: {val}")
    return "\n".join(lines)


def build_news_headlines(news: dict) -> str:
    """Build headline list from today's news."""
    lines = []
    pestle = news.get("pestle", {})
    for cat in PESTLE_CATS:
        info = pestle.get(cat, {})
        articles = info.get("articles", [])
        if articles:
            lines.append(f"\n[{cat}]")
            for a in articles[:10]:
                lines.append(f"  - {a.get('title', '')}")
    return "\n".join(lines)


def build_weak_signals_summary(ai_data: dict) -> str:
    """Build weak signals summary."""
    signals = ai_data.get("weak_signals", [])
    if not signals:
        return "(なし)"
    lines = []
    for s in signals[:8]:
        impact = s.get("potential_impact", "")
        horizon = s.get("time_horizon", "")
        lines.append(f"  - [{impact}/{horizon}] {s.get('signal', '')}: {s.get('description', '')[:100]}")
    return "\n".join(lines)


def generate_daily_report(ai_data: dict, meta_report: dict, news: dict, version: str) -> dict:
    """Generate the daily CLA narrative report.

    Args:
        ai_data: Today's ai_analysis.json
        meta_report: Historical meta-analysis
        news: Today's latest.json
        version: "japan" or "global"

    Returns dict with title, report_text, key_observations.
    """
    print(f"\n  Generating {version} daily report...")

    daily_cla = build_daily_cla_summary(ai_data)
    headlines = build_news_headlines(news)
    weak_signals = build_weak_signals_summary(ai_data)

    # Get meta-report context for the chosen version
    meta_version = meta_report.get(version, {})
    meta_text = meta_version.get("report_text", "")
    paradigm_shifts = meta_version.get("key_paradigm_shifts", [])
    myths_timeline = meta_version.get("dominant_myths_timeline", [])

    # Summarize meta for prompt (avoid token overflow)
    meta_summary = meta_text[:3000] if meta_text else "(歴史的メタ分析なし)"

    paradigm_text = "\n".join(
        f"  - {p.get('period', '')}: {p.get('name', '')} - {p.get('description', '')[:80]}"
        for p in paradigm_shifts
    )

    myths_text = "\n".join(
        f"  - {m.get('era', '')}: {m.get('myth', '')[:80]}"
        for m in myths_timeline
    )

    date_str = ai_data.get("date", datetime.now().strftime("%Y-%m-%d"))

    if version == "japan":
        focus = """日本の視点から記述してください。本日のニュースが日本社会・経済・政治にとって何を意味するか、
日本固有の文脈（少子高齢化、産業構造転換、安全保障環境の変化など）を踏まえて分析してください。"""
        title_hint = "日本版"
    else:
        focus = """グローバルな視点から記述してください。本日のニュースが世界秩序・地政学・
グローバル経済にとって何を意味するか、国際的な文脈を踏まえて分析してください。"""
        title_hint = "グローバル版"

    prompt = f"""あなたは未来学・社会変動分析の世界的権威です。

## 本日（{date_str}）のCLA分析結果
{daily_cla}

## 本日の主要ニュース見出し
{headlines[:3000]}

## 検出されたウィークシグナル
{weak_signals}

## 歴史的CLA メタ分析（1990-2026年の蓄積、{title_hint}）
{meta_summary}

## 過去の主要パラダイムシフト
{paradigm_text if paradigm_text else "(データなし)"}

## 神話の変遷タイムライン
{myths_text if myths_text else "(データなし)"}

## 指示

上記すべてのデータを統合し、「{title_hint}：現在起こりつつある神話の変遷と我々が置かれている状況」というテーマで、{date_str}付の日次レポートを作成してください。

{focus}

以下の視点を含めてください:
1. **今日の出来事の深層読解**: 本日のニュースを表層ではなくCLAの4層で読み解く
2. **歴史的文脈との接続**: 1990年以降の神話変遷の中で、今日の出来事がどこに位置づけられるか
3. **進行中の神話転換**: 現在まさに起こりつつある支配的物語の交代・変容
4. **ウィークシグナルの意味**: 検出された弱い信号が示す将来の可能性
5. **我々が置かれている状況**: 現在の立ち位置の総合的評価

重要: レポートは約5,000字の散文形式（地の文）で記述してください。箇条書きは補助的にのみ使用。
読者は経営者・コンサルタントを想定し、知的かつ実用的な洞察を提供してください。

以下のJSON形式で返してください:
{{
  "title": "レポートタイトル（日付を含む）",
  "report_text": "5,000字程度の散文形式レポート全文",
  "key_observations": [
    {{
      "theme": "観察テーマ",
      "observation": "主要な観察（1-2文）",
      "historical_link": "歴史的文脈との接続（1文）"
    }}
  ],
  "myth_in_transition": {{
    "fading_myth": "衰退しつつある支配的神話",
    "emerging_myth": "浮上しつつある新たな神話",
    "transition_stage": "初期/加速期/転換点/定着期"
  }},
  "action_implications": [
    "経営者・意思決定者への示唆1",
    "経営者・意思決定者への示唆2",
    "経営者・意思決定者への示唆3"
  ]
}}

JSONのみ返してください。"""

    text = call_claude(prompt, max_tokens=16384)
    result = extract_json(text)

    if "report_text" not in result:
        raise ValueError("Missing report_text in response")

    report_len = len(result["report_text"])
    print(f"  -> {version}: {report_len} chars, "
          f"{len(result.get('key_observations', []))} observations, "
          f"{len(result.get('action_implications', []))} implications")

    return result


def main():
    print(f"{'=' * 60}")
    print(f"  Daily CLA Report Generator")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'=' * 60}")

    # Load inputs
    ai_data = load_daily_cla()
    if not ai_data:
        return

    meta_report = load_meta_report()
    news = load_latest_news()

    date_str = ai_data.get("date", datetime.now().strftime("%Y-%m-%d"))
    print(f"  Analysis date: {date_str}")
    print(f"  CLA categories: {len(ai_data.get('cla', {}))}")
    print(f"  Weak signals: {len(ai_data.get('weak_signals', []))}")
    print(f"  Meta report: {'loaded' if meta_report else 'not available'}")

    # Generate Japan version
    japan_report = generate_daily_report(ai_data, meta_report, news, "japan")

    time.sleep(1)

    # Generate Global version
    global_report = generate_daily_report(ai_data, meta_report, news, "global")

    # Assemble output
    output = {
        "date": date_str,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "japan": japan_report,
        "global": global_report,
        "metadata": {
            "model": MODEL,
            "cla_source": "ai_analysis.json",
            "meta_report_available": bool(meta_report),
            "news_articles_count": sum(
                info.get("count", len(info.get("articles", [])))
                for info in news.get("pestle", {}).values()
            ),
            "weak_signals_count": len(ai_data.get("weak_signals", [])),
        },
    }

    output_path = DATA_DIR / "daily_report.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 60}")
    print(f"  Complete: {output_path}")
    print(f"  Japan report: {len(japan_report.get('report_text', ''))} chars")
    print(f"  Global report: {len(global_report.get('report_text', ''))} chars")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
