#!/usr/bin/env python3
"""
Generate Insight Reports.

Produces 10 daily "Insight Reports" that connect individual news articles
to CLA myths — explaining how each article strengthens or changes the
dominant myths identified in our analysis.

Reads:
  - data/latest.json (today's PESTLE news)
  - data/ai_analysis.json (CLA analysis with myths per category)
  - data/cla_meta_report.json (historical myth context)
  - data/daily_report.json (myth_in_transition data)

Output:
  - data/insight_reports.json (today's 10 reports)
  - data/insight_reports_YYYY-MM-DD.json (dated archive copy)
  - data/insight_reports_index.json (cumulative index, no full text)
"""

import json
import time
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

client = anthropic.Anthropic()
# Model can be overridden via environment variable
MODEL = os.environ.get("IR_MODEL", "claude-haiku-4-5-20251001")

DATA_DIR = Path(__file__).parent.parent / "data"

PESTLE_CATS = ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]

PESTLE_JA = {
    "Political": "政治",
    "Economic": "経済",
    "Social": "社会",
    "Technological": "技術",
    "Legal": "法律",
    "Environmental": "環境",
}


def extract_json(text: str):
    """Extract JSON from Claude's response (object or array)."""
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
    # Try to find JSON object or array
    for start_char, end_char in [("{", "}"), ("[", "]")]:
        start = text.find(start_char)
        end = text.rfind(end_char)
        if start != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                continue
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


# ===== Data Loading =====

def load_latest_news() -> dict:
    """Load today's PESTLE news from latest.json."""
    path = DATA_DIR / "latest.json"
    if not path.exists():
        print("[ERROR] latest.json not found.")
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_ai_analysis() -> dict:
    """Load CLA analysis from ai_analysis.json."""
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


def load_daily_report() -> dict:
    """Load daily report for myth_in_transition data."""
    path = DATA_DIR / "daily_report.json"
    if not path.exists():
        print("[WARN] daily_report.json not found. Generating without myth transition data.")
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ===== Step 2: Select 10 myth-relevant articles =====

def build_article_list(news: dict) -> list[dict]:
    """Flatten all articles from all PESTLE categories with index tracking."""
    articles = []
    for cat in PESTLE_CATS:
        info = news.get("pestle", {}).get(cat, {})
        for a in info.get("articles", []):
            articles.append({
                "index": len(articles),
                "category": cat,
                "title": a.get("title", ""),
                "summary": a.get("summary", "")[:200],
                "url": a.get("url", ""),
                "source": a.get("source", ""),
                "published_date": a.get("published_date", ""),
                "lang": a.get("lang", "en"),
                # Keep full article data for later use
                "_full": a,
            })
    return articles


def select_myth_relevant_articles(
    articles: list[dict],
    ai_data: dict,
    daily_report: dict,
) -> list[dict]:
    """Use Claude to select 10 articles most strongly related to myths."""
    print("\n  Selecting myth-relevant articles...")

    # Build CLA myth summary per category
    cla = ai_data.get("cla", {})
    cla_summary_lines = []
    for cat in PESTLE_CATS:
        cat_data = cla.get(cat, {})
        if not cat_data:
            continue
        cla_summary_lines.append(f"\n[{cat} / {PESTLE_JA.get(cat, cat)}]")
        cla_summary_lines.append(f"  myth_metaphor: {cat_data.get('myth_metaphor', 'N/A')}")
        cla_summary_lines.append(f"  emerging_narrative: {cat_data.get('emerging_narrative', 'N/A')}")

    cla_summary = "\n".join(cla_summary_lines)

    # Build myth_in_transition info from daily report
    myth_transition_lines = []
    for version in ["japan", "global"]:
        vdata = daily_report.get(version, {})
        mit = vdata.get("myth_in_transition", {})
        if mit:
            myth_transition_lines.append(f"\n[{version}]")
            myth_transition_lines.append(f"  fading_myth: {mit.get('fading_myth', 'N/A')[:200]}")
            myth_transition_lines.append(f"  emerging_myth: {mit.get('emerging_myth', 'N/A')[:200]}")

    myth_transition_text = "\n".join(myth_transition_lines) if myth_transition_lines else "(データなし)"

    # Build article list text (limit to keep prompt manageable)
    article_lines = []
    for a in articles:
        article_lines.append(
            f"[{a['index']}] ({a['category']}) {a['title']}\n"
            f"    {a['summary'][:150]}"
        )
    # Truncate if too many articles
    articles_text = "\n".join(article_lines[:500])

    prompt = f"""あなたは未来学・CLA（因果階層分析）の専門家です。

## 現在のCLA分析結果（各PESTLE分野の神話・メタファーと浮上するナラティブ）
{cla_summary}

## 神話の変遷（myth_in_transition）
{myth_transition_text}

## 本日の全ニュース記事リスト（{len(articles)}件）
{articles_text}

## 指示

上記の記事リストから、CLAの「神話・メタファー」層に最も強く関連する記事を正確に10件選んでください。

選定基準:
1. 現在の支配的神話を「強化する」記事、または神話の「変化・転換」を示す記事
2. **必ず3つ以上の異なるPESTLE分野から選ぶこと**（1分野から最大4件まで）
3. 表層的なニュースより、深層的な社会変動のシグナルとなる記事を優先
4. インパクトの大きい順にランク付け（最もインパクトの大きいものを最初に）

以下のJSON配列で返してください（正確に10件）:
[
  {{
    "article_index": 記事の番号,
    "category": "PESTLE分野",
    "myth_relation": "strengthens" または "changes",
    "related_myth": "関連する神話（日本語で簡潔に）",
    "reason": "選定理由（日本語で1-2文）"
  }}
]

JSONのみ返してください。"""

    # Retry up to 2 times if fewer than 10 articles are selected
    for attempt in range(3):
        text = call_claude(prompt, max_tokens=4096)
        selections = extract_json(text)

        if not isinstance(selections, list):
            print("  [WARN] Unexpected response format, wrapping in list")
            selections = [selections]

        print(f"  -> Selected {len(selections)} articles (attempt {attempt + 1})")

        if len(selections) >= 10:
            break
        if attempt < 2:
            print(f"  [WARN] Only {len(selections)} articles selected, retrying...")
            time.sleep(2)

    # Validate category diversity — warn if too concentrated
    cat_counts = {}
    for s in selections:
        c = s.get("category", "?")
        cat_counts[c] = cat_counts.get(c, 0) + 1
    max_cat_count = max(cat_counts.values()) if cat_counts else 0
    if max_cat_count > 5:
        top_cat = max(cat_counts, key=cat_counts.get)
        print(f"  [WARN] Category concentration: {top_cat} has {max_cat_count}/{len(selections)} articles")

    return selections


# ===== Step 3: Generate individual reports =====

def generate_single_report(
    article: dict,
    selection: dict,
    ai_data: dict,
    meta_report: dict,
    report_num: int,
    total: int,
) -> dict | None:
    """Generate one insight report for a selected article."""
    print(f"  Generating report {report_num}/{total}...")

    category = selection.get("category", article.get("category", ""))
    myth_relation = selection.get("myth_relation", "strengthens")
    related_myth = selection.get("related_myth", "")
    selection_reason = selection.get("reason", "")

    # Get CLA data for this category
    cla = ai_data.get("cla", {})
    cat_cla = cla.get(category, {})

    cla_context = ""
    if cat_cla:
        cla_context = f"""
  litany: {cat_cla.get('litany', 'N/A')}
  systemic_causes: {cat_cla.get('systemic_causes', 'N/A')}
  worldview: {cat_cla.get('worldview', 'N/A')}
  myth_metaphor: {cat_cla.get('myth_metaphor', 'N/A')}
  key_tension: {cat_cla.get('key_tension', 'N/A')}
  emerging_narrative: {cat_cla.get('emerging_narrative', 'N/A')}"""

    # Get historical context from meta report (use japan version as default)
    meta_japan = meta_report.get("japan", {})
    meta_text = meta_japan.get("report_text", "")
    # Extract a relevant portion (keep it short to avoid token overflow)
    meta_excerpt = meta_text[:2000] if meta_text else "(歴史的メタ分析なし)"

    # Get paradigm shifts relevant context
    paradigm_shifts = meta_japan.get("key_paradigm_shifts", [])
    paradigm_text = "\n".join(
        f"  - {p.get('period', '')}: {p.get('name', '')}"
        for p in paradigm_shifts
    ) if paradigm_shifts else "(データなし)"

    full_article = article.get("_full", article)
    title = full_article.get("title", article.get("title", ""))
    summary = full_article.get("summary", article.get("summary", ""))
    url = full_article.get("url", article.get("url", ""))
    source = full_article.get("source", article.get("source", ""))

    relation_ja = "強化している" if myth_relation == "strengthens" else "変化させている"

    prompt = f"""あなたは未来学・CLA（因果階層分析）の世界的権威であり、ニュースの深層分析を専門としています。

## 分析対象の記事
タイトル: {title}
要約: {summary}
出典: {source}
URL: {url}
PESTLE分野: {category}（{PESTLE_JA.get(category, category)}）

## この記事と神話の関係
関連する神話: {related_myth}
関係性: この記事は上記の神話を{relation_ja}
選定理由: {selection_reason}

## {category}分野のCLA分析結果
{cla_context}

## 歴史的文脈（1990-2026年の主要パラダイムシフト）
{paradigm_text}

## 歴史的メタ分析（抜粋）
{meta_excerpt}

## 指示

上記の記事について、約5,000字のインサイトレポートを作成してください。
このレポートは、1本のニュース記事がCLAの神話層とどのように接続しているかを深く掘り下げるものです。

以下の4つの視点を必ず含めてください:

1. **歴史的経緯**: この記事が扱うテーマの歴史的背景。1990年以降の社会変動の中でどう位置づけられるか。
2. **未来へのシグナル分析**: この記事が示す未来の方向性。どのような変化の兆候が読み取れるか。
3. **今後ウォッチすべき観点**: この記事のテーマに関して、今後注目すべき具体的なポイント。
4. **関連する近年の学術成果**: このテーマに関連する学術的な知見や研究（実在する研究者や理論を参照）。

重要な執筆ルール:
- レポートは約5,000字の散文形式（地の文）で記述してください
- 箇条書きは補助的にのみ使用し、主要な内容は必ず文章で記述
- 読者は経営者・コンサルタントを想定し、知的かつ実用的な洞察を提供
- 日本語で記述してください
- 冒頭要約として、この記事が示す最も重要な洞察を500字程度で記述してください。全文を読まなくても一定の理解が得られる内容にしてください
- 歴史的経緯を5-8項目の時系列データとして構造化してください
- 未来へのシグナル分析と今後ウォッチすべき観点には、それぞれ記事の内容に即した具体的なタイトルをつけてください
- 関連する学術成果は2つ取り上げてください。各学術成果について、まずこの記事のテーマとの関係性に基づいて読者の関心をつなげるコメント（なぜこの学術成果が重要か、どう関連するか）を先に記述し、最後に誰のどのような学術成果かを明記してください。有名な論文だけでなく、あまり知られていない独自の論考や最新の研究も積極的に含めてください

以下のJSON形式で返してください:
{{
  "report_title": "レポートタイトル（日本語、記事の本質を捉えた魅力的なタイトル）",
  "summary": "要約（日本語、500字程度）",
  "report_text": "約5,000字の全文レポート",
  "timeline": [
    {{"year": "1990", "event": "出来事の簡潔な説明（日本語、1-2文）", "significance": "現在のテーマにとっての重要性（日本語、1文）"}},
    {{"year": "2000-2010", "event": "出来事の簡潔な説明", "significance": "重要性の説明"}}
  ],
  "historical_context": "歴史的経緯のセクション要約（2-3文）",
  "future_signals_title": "未来シグナルのタイトル（日本語、記事固有の具体的なタイトル）",
  "future_signals": "未来へのシグナル分析の要約（2-3文）",
  "watch_points_title": "ウォッチポイントのタイトル（日本語、記事固有の具体的なタイトル）",
  "watch_points": "今後ウォッチすべき観点の要約（2-3文）",
  "related_research": [
    {{"comment": "この記事との関係性に基づく読者への導入コメント（2-3文。なぜこの学術成果が重要か、記事テーマとどう関連するか）", "title": "論文/論考のタイトル", "author": "著者名"}},
    {{"comment": "この記事との関係性に基づく読者への導入コメント（2-3文）", "title": "論文/論考のタイトル", "author": "著者名"}}
  ]
}}

JSONのみ返してください。"""

    try:
        text = call_claude(prompt, max_tokens=16384)
        result = extract_json(text)

        if "report_text" not in result:
            print(f"    [WARN] Missing report_text in response for report {report_num}")
            return None

        char_count = len(result.get("report_text", ""))
        print(f"    -> {char_count} chars")
        return result

    except Exception as e:
        print(f"    [ERROR] Failed to generate report {report_num}: {e}")
        return None


# ===== Step 4: Save outputs =====

def save_reports(reports: list[dict], date_str: str, total_articles: int) -> None:
    """Save insight reports to JSON files and update the index."""

    generated_at = datetime.now(timezone.utc).isoformat()

    output = {
        "date": date_str,
        "generated_at": generated_at,
        "reports": reports,
        "metadata": {
            "model": MODEL,
            "total_articles_considered": total_articles,
            "total_reports": len(reports),
        },
    }

    # Save today's reports
    output_path = DATA_DIR / "insight_reports.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  -> Saved {output_path}")

    # Save dated archive copy
    archive_path = DATA_DIR / f"insight_reports_{date_str}.json"
    with open(archive_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  -> Saved {archive_path}")

    # Update cumulative index
    update_index(reports, date_str, generated_at)


def update_index(reports: list[dict], date_str: str, generated_at: str) -> None:
    """Update the cumulative insight_reports_index.json with today's entry."""
    index_path = DATA_DIR / "insight_reports_index.json"

    # Load existing index or create new one
    if index_path.exists():
        with open(index_path, encoding="utf-8") as f:
            index = json.load(f)
    else:
        index = {"updated_at": "", "dates": []}

    index["updated_at"] = generated_at

    # Build today's index entry (metadata only, no full text)
    today_entry = {
        "date": date_str,
        "count": len(reports),
        "reports": [
            {
                "id": r["id"],
                "report_title": r.get("report_title", ""),
                "pestle_category": r.get("article", {}).get("pestle_category", ""),
                "myth_relation": r.get("myth_relation", ""),
            }
            for r in reports
        ],
    }

    # Replace existing entry for today or append
    existing_dates = [d["date"] for d in index["dates"]]
    if date_str in existing_dates:
        idx = existing_dates.index(date_str)
        index["dates"][idx] = today_entry
    else:
        index["dates"].append(today_entry)

    # Sort by date descending
    index["dates"].sort(key=lambda d: d["date"], reverse=True)

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"  -> Updated {index_path}")


# ===== Main =====

def main():
    print(f"{'=' * 60}")
    print(f"  Insight Reports Generator")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'=' * 60}")

    # Step 1: Load data
    news = load_latest_news()
    if not news:
        print("[ERROR] Cannot proceed without latest.json")
        return

    ai_data = load_ai_analysis()
    if not ai_data:
        print("[ERROR] Cannot proceed without ai_analysis.json")
        return

    meta_report = load_meta_report()
    daily_report = load_daily_report()

    date_str = news.get("date", datetime.now().strftime("%Y-%m-%d"))
    print(f"  Date: {date_str}")

    # Build flat article list
    articles = build_article_list(news)
    total_articles = len(articles)
    print(f"  Total articles: {total_articles}")
    print(f"  CLA categories: {len(ai_data.get('cla', {}))}")
    print(f"  Meta report: {'loaded' if meta_report else 'not available'}")
    print(f"  Daily report: {'loaded' if daily_report else 'not available'}")

    # Step 2: Select 10 myth-relevant articles
    selections = select_myth_relevant_articles(articles, ai_data, daily_report)

    if not selections:
        print("[ERROR] No articles selected. Aborting.")
        return

    # Build translation lookup from ai_analysis
    translations = ai_data.get("translations", {})
    translation_map = {}  # (category, index_within_category) -> translated title
    for cat, trans_list in translations.items():
        if isinstance(trans_list, list):
            for t in trans_list:
                idx = t.get("index", -1)
                translation_map[(cat, idx)] = t.get("title_translated", "")

    # Step 3: Generate individual reports
    reports = []
    for i, sel in enumerate(selections):
        article_idx = sel.get("article_index", -1)

        # Validate article index
        if article_idx < 0 or article_idx >= len(articles):
            print(f"  [WARN] Invalid article index {article_idx}, skipping")
            continue

        article = articles[article_idx]
        full_article = article.get("_full", article)

        # Try to find translated title
        cat = article["category"]
        # Calculate index within category
        cat_articles = news.get("pestle", {}).get(cat, {}).get("articles", [])
        within_cat_idx = None
        for ci, ca in enumerate(cat_articles):
            if ca.get("url") == full_article.get("url"):
                within_cat_idx = ci
                break
        title_ja = translation_map.get((cat, within_cat_idx), "") if within_cat_idx is not None else ""

        # Generate report
        result = generate_single_report(
            article, sel, ai_data, meta_report,
            report_num=i + 1, total=len(selections),
        )

        if result:
            report_entry = {
                "id": i,
                "article": {
                    "title": full_article.get("title", ""),
                    "title_ja": title_ja,
                    "url": full_article.get("url", ""),
                    "source": full_article.get("source", ""),
                    "published_date": full_article.get("published_date", ""),
                    "pestle_category": cat,
                    "summary": full_article.get("summary", ""),
                },
                "report_title": result.get("report_title", ""),
                "summary": result.get("summary", ""),
                "report_text": result.get("report_text", ""),
                "timeline": result.get("timeline", []),
                "historical_context": result.get("historical_context", ""),
                "future_signals_title": result.get("future_signals_title", ""),
                "future_signals": result.get("future_signals", ""),
                "watch_points_title": result.get("watch_points_title", ""),
                "watch_points": result.get("watch_points", ""),
                "related_research": result.get("related_research", ""),
                "myth_relation": sel.get("myth_relation", ""),
                "related_myth": sel.get("related_myth", ""),
                "char_count": len(result.get("report_text", "")),
            }
            reports.append(report_entry)

        # Rate limiting between API calls
        if i < len(selections) - 1:
            time.sleep(1)

    # Step 4: Save outputs
    if reports:
        print(f"\n  Saving {len(reports)} reports...")
        save_reports(reports, date_str, total_articles)
    else:
        print("\n  [WARN] No reports were generated.")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  Complete!")
    print(f"  Reports generated: {len(reports)}/{len(selections)}")
    if reports:
        total_chars = sum(r["char_count"] for r in reports)
        avg_chars = total_chars // len(reports) if reports else 0
        print(f"  Total characters: {total_chars:,}")
        print(f"  Average per report: {avg_chars:,}")
        categories_used = set(r["article"]["pestle_category"] for r in reports)
        print(f"  PESTLE categories covered: {', '.join(sorted(categories_used))}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
