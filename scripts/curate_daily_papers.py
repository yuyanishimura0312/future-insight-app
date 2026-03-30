#!/usr/bin/env python3
"""
Daily Paper Curation for Future Insight App
Each day, select the 20 most significant papers per field,
translate them to Japanese, detect novelty, and generate alerts.

Output:
  - data/daily_papers.json    — Curated 20 papers/field with translations
  - data/paper_alerts.json    — Novel research context alerts
"""

import json
import os
import re
import sys
import time
import hashlib
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter, defaultdict
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

from db import get_connection, DB_PATH

MODEL = "claude-haiku-4-5-20251001"
DATA_DIR = Path(__file__).parent.parent / "data"
DAILY_PAPERS_PATH = DATA_DIR / "daily_papers.json"
PAPER_ALERTS_PATH = DATA_DIR / "paper_alerts.json"

FIELDS = ["人文学", "社会科学", "自然科学", "工学", "芸術"]
PAPERS_PER_FIELD = 20
WORKERS = 5

lock = threading.Lock()


def get_recent_papers(days: int = 3) -> dict[str, list[dict]]:
    """Get recently collected papers from the database, grouped by field."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = conn.execute(
        """SELECT id, title, authors, source_url, doi, summary, field, subfield,
                  source_name, published_date, language, insight_tags,
                  relevance_score, openalex_id
           FROM papers
           WHERE detected_at >= ? OR published_date >= ?
           ORDER BY published_date DESC""",
        (cutoff, cutoff),
    ).fetchall()
    conn.close()

    by_field = defaultdict(list)
    for r in rows:
        by_field[r["field"]].append(dict(r))
    return by_field


def get_historical_keywords(days: int = 90) -> set[str]:
    """Get keywords from papers older than recent window for novelty detection."""
    conn = get_connection()
    cutoff_old = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    cutoff_recent = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

    rows = conn.execute(
        """SELECT DISTINCT keyword FROM trends
           WHERE last_seen < ? AND first_seen >= ?""",
        (cutoff_recent, cutoff_old),
    ).fetchall()
    conn.close()

    return {r[0].lower() for r in rows}


def curate_field_papers(field: str, papers: list[dict]) -> dict:
    """Use Claude to select top 20 papers, translate, and score novelty."""
    if len(papers) <= PAPERS_PER_FIELD:
        selected = papers
    else:
        selected = _ai_select_papers(field, papers)

    # Translate and analyze in one call
    result = _ai_translate_and_analyze(field, selected)
    return result


def _ai_select_papers(field: str, papers: list[dict]) -> list[dict]:
    """Use Claude to select the 20 most significant papers from a larger set."""
    client = anthropic.Anthropic()

    # Build compact paper list for selection
    paper_list = []
    for i, p in enumerate(papers[:100]):  # Cap at 100 for API limits
        paper_list.append(
            f"[{i}] {p['title'][:150]} | {p.get('subfield', '')} | "
            f"{p.get('published_date', '')} | score:{p.get('relevance_score', 0)}"
        )

    prompt = f"""あなたは学術論文キュレーターです。以下の「{field}」分野の論文リストから、
最も重要・革新的・影響力のある20件を選んでください。

選定基準:
1. 学術的インパクト（新しい発見、画期的な手法、重要な実証結果）
2. 分野の多様性（サブフィールドが偏らないように）
3. 時事性（最新の課題に関連するもの優先）
4. 新規性（これまでにない視点やアプローチ）

論文リスト:
{chr(10).join(paper_list)}

選んだ20件の番号をJSON配列で返してください。例: [0, 3, 5, ...]
JSON配列のみ返してください。"""

    try:
        resp = client.messages.create(
            model=MODEL, max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        start = text.find("[")
        end = text.rfind("]") + 1
        if start >= 0 and end > start:
            indices = json.loads(text[start:end])
            return [papers[i] for i in indices if i < len(papers)][:PAPERS_PER_FIELD]
    except Exception as e:
        print(f"  [WARN] AI selection failed for {field}: {e}", file=sys.stderr)

    # Fallback: sort by relevance_score
    papers.sort(key=lambda p: p.get("relevance_score", 0), reverse=True)
    return papers[:PAPERS_PER_FIELD]


def _ai_translate_and_analyze(field: str, papers: list[dict]) -> dict:
    """Translate titles+summaries to Japanese and detect novel research contexts."""
    client = anthropic.Anthropic()

    paper_entries = []
    for i, p in enumerate(papers):
        entry = f"[{i}] Title: {p['title'][:200]}"
        if p.get("summary") and p["summary"] != f"[{p['title']}]":
            entry += f"\nSummary: {p['summary'][:300]}"
        if p.get("subfield"):
            entry += f"\nSubfield: {p['subfield']}"
        paper_entries.append(entry)

    prompt = f"""以下の「{field}」分野の学術論文を分析してください。

タスク:
1. 各論文のタイトルと要約を日本語に翻訳
2. 各論文の新規性スコア(0-10)を評価（10=極めて新規、従来にない文脈）
3. 特に新たな研究文脈を開拓している論文があれば指摘

新規性スコアの基準:
- 0-3: 既存研究の延長線上
- 4-6: 新しい視点や手法だが既知の枠組み内
- 7-8: 分野に新たな方向性を示す
- 9-10: 従来にない文脈、パラダイムシフトの可能性

論文:
{chr(10).join(paper_entries)}

以下のJSONフォーマットで返してください:
{{
  "papers": [
    {{
      "n": 0,
      "title_ja": "日本語タイトル",
      "summary_ja": "日本語要約（2-3文）",
      "novelty_score": 5,
      "novelty_reason": "新規性の理由（日本語、1文）"
    }}
  ],
  "alerts": [
    {{
      "paper_index": 0,
      "alert_type": "NEW_CONTEXT",
      "title": "アラートタイトル（日本語）",
      "description": "なぜこれが新たな文脈なのか（日本語、2-3文）",
      "significance": "high"
    }}
  ]
}}

alertsは新規性スコア8以上の論文のみ含めてください。該当なしなら空配列。
JSONのみ返してください。"""

    try:
        resp = client.messages.create(
            model=MODEL, max_tokens=8000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            result = json.loads(text[start:end])

            # Merge translations back into papers
            for item in result.get("papers", []):
                idx = item.get("n", -1)
                if 0 <= idx < len(papers):
                    papers[idx]["title_ja"] = item.get("title_ja", "")
                    papers[idx]["summary_ja"] = item.get("summary_ja", "")
                    papers[idx]["novelty_score"] = item.get("novelty_score", 0)
                    papers[idx]["novelty_reason"] = item.get("novelty_reason", "")

            return {
                "field": field,
                "papers": papers,
                "alerts": result.get("alerts", []),
            }
    except Exception as e:
        print(f"  [WARN] AI translate/analyze failed for {field}: {e}", file=sys.stderr)

    # Fallback: return papers without translations
    return {"field": field, "papers": papers, "alerts": []}


def detect_cross_field_novelty(all_results: list[dict]) -> list[dict]:
    """Detect research themes that appear across multiple fields (cross-pollination)."""
    client = anthropic.Anthropic()

    # Collect high-novelty papers across all fields
    high_novelty = []
    for result in all_results:
        for p in result["papers"]:
            if p.get("novelty_score", 0) >= 6:
                high_novelty.append({
                    "field": result["field"],
                    "title": p["title"][:150],
                    "title_ja": p.get("title_ja", ""),
                    "subfield": p.get("subfield", ""),
                    "novelty_score": p.get("novelty_score", 0),
                })

    if len(high_novelty) < 3:
        return []

    entries = [f"[{h['field']}] {h['title']} (novelty:{h['novelty_score']})" for h in high_novelty]

    prompt = f"""以下は複数分野の高新規性論文リストです。
分野横断的なテーマや、従来にない文脈の組み合わせがあれば指摘してください。

{chr(10).join(entries)}

以下のJSONで返してください:
{{
  "cross_field_alerts": [
    {{
      "alert_type": "CROSS_FIELD",
      "title": "アラートタイトル（日本語）",
      "description": "分野横断的な新文脈の説明（日本語、2-3文）",
      "related_fields": ["分野1", "分野2"],
      "significance": "high"
    }}
  ]
}}

該当なしなら空配列。JSONのみ返してください。"""

    try:
        resp = client.messages.create(
            model=MODEL, max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end]).get("cross_field_alerts", [])
    except Exception as e:
        print(f"  [WARN] Cross-field analysis failed: {e}", file=sys.stderr)

    return []


def main():
    today = datetime.now(timezone.utc)
    date_str = today.strftime("%Y-%m-%d")

    print(f"=== Daily Paper Curation ({date_str}) ===\n")

    # 1. Get recent papers from database
    print("1. Loading recent papers from database...")
    by_field = get_recent_papers(days=3)
    for f in FIELDS:
        print(f"   [{f}] {len(by_field.get(f, []))} candidates")

    if not any(by_field.values()):
        print("  No recent papers found. Skipping curation.")
        return

    # 2. Curate each field (parallel)
    print("\n2. Curating papers with AI (selecting top 20, translating, scoring)...")
    all_results = []

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {}
        for field in FIELDS:
            papers = by_field.get(field, [])
            if not papers:
                print(f"   [{field}] No papers, skipping")
                continue
            futures[executor.submit(curate_field_papers, field, papers)] = field

        for future in as_completed(futures):
            field = futures[future]
            try:
                result = future.result()
                all_results.append(result)
                n_papers = len(result["papers"])
                n_alerts = len(result["alerts"])
                print(f"   [{field}] {n_papers} papers curated, {n_alerts} alerts")
            except Exception as e:
                print(f"   [{field}] ERROR: {e}", file=sys.stderr)

    # 3. Cross-field novelty detection
    print("\n3. Detecting cross-field novelty...")
    cross_alerts = detect_cross_field_novelty(all_results)
    print(f"   {len(cross_alerts)} cross-field alerts detected")

    # 4. Build output JSON
    print("\n4. Writing output files...")

    daily_output = {
        "date": date_str,
        "generated_at": today.isoformat(),
        "fields": {},
        "summary": {
            "total_papers": 0,
            "total_alerts": 0,
            "fields_covered": 0,
        },
    }

    all_alerts = []

    for result in all_results:
        field = result["field"]
        papers_out = []
        for p in result["papers"]:
            papers_out.append({
                "title": p["title"],
                "title_ja": p.get("title_ja", ""),
                "authors": p.get("authors", ""),
                "source_url": p.get("source_url", ""),
                "doi": p.get("doi", ""),
                "summary": p.get("summary", ""),
                "summary_ja": p.get("summary_ja", ""),
                "field": field,
                "subfield": p.get("subfield", ""),
                "published_date": p.get("published_date", ""),
                "language": p.get("language", "en"),
                "novelty_score": p.get("novelty_score", 0),
                "novelty_reason": p.get("novelty_reason", ""),
                "source_name": p.get("source_name", ""),
                "openalex_id": p.get("openalex_id", ""),
            })

        daily_output["fields"][field] = {
            "count": len(papers_out),
            "papers": papers_out,
        }
        daily_output["summary"]["total_papers"] += len(papers_out)
        daily_output["summary"]["fields_covered"] += 1

        # Field-level alerts
        for alert in result.get("alerts", []):
            paper_idx = alert.get("paper_index", -1)
            paper_ref = papers_out[paper_idx] if 0 <= paper_idx < len(papers_out) else None
            all_alerts.append({
                "alert_type": alert.get("alert_type", "NEW_CONTEXT"),
                "title": alert.get("title", ""),
                "description": alert.get("description", ""),
                "significance": alert.get("significance", "medium"),
                "field": field,
                "paper_title": paper_ref["title"] if paper_ref else "",
                "paper_title_ja": paper_ref.get("title_ja", "") if paper_ref else "",
                "paper_url": paper_ref.get("source_url", "") if paper_ref else "",
                "date": date_str,
            })

    # Add cross-field alerts
    for alert in cross_alerts:
        all_alerts.append({
            "alert_type": alert.get("alert_type", "CROSS_FIELD"),
            "title": alert.get("title", ""),
            "description": alert.get("description", ""),
            "significance": alert.get("significance", "medium"),
            "related_fields": alert.get("related_fields", []),
            "field": "横断",
            "date": date_str,
        })

    daily_output["summary"]["total_alerts"] = len(all_alerts)

    # Write daily_papers.json
    with open(DAILY_PAPERS_PATH, "w", encoding="utf-8") as f:
        json.dump(daily_output, f, ensure_ascii=False, indent=2)
    print(f"   daily_papers.json: {daily_output['summary']['total_papers']} papers")

    # Write paper_alerts.json (append to history)
    alerts_history = []
    if PAPER_ALERTS_PATH.exists():
        try:
            alerts_history = json.load(open(PAPER_ALERTS_PATH, encoding="utf-8"))
        except (json.JSONDecodeError, IOError):
            alerts_history = []

    # Keep last 30 days of alerts
    cutoff = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    alerts_history = [a for a in alerts_history if a.get("date", "") >= cutoff]
    alerts_history.extend(all_alerts)

    with open(PAPER_ALERTS_PATH, "w", encoding="utf-8") as f:
        json.dump(alerts_history, f, ensure_ascii=False, indent=2)
    print(f"   paper_alerts.json: {len(all_alerts)} new alerts, {len(alerts_history)} total")

    # 5. Summary
    print(f"\n=== Summary ===")
    print(f"Date: {date_str}")
    for field in FIELDS:
        info = daily_output["fields"].get(field, {})
        print(f"  {field}: {info.get('count', 0)} papers")
    print(f"  Alerts: {len(all_alerts)}")
    print(f"\n  Done!")


if __name__ == "__main__":
    main()
