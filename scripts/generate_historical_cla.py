#!/usr/bin/env python3
"""
Generate historical CLA (Causal Layered Analysis) for two time ranges:
  1. 1990-2020: yearly CLA for each year -> data/cla_historical_yearly.json
  2. 2021-2026: quarterly CLA (Q1-Q4) -> data/cla_historical_quarterly.json

Each entry contains CLA 4-layer analysis for all 6 PESTLE categories,
plus a cross-category synthesis.

Uses Claude API (Haiku) for cost-efficient batch generation.
Leverages existing pestle_history.json and DB articles when available.
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


def extract_json(text: str) -> dict:
    """Extract JSON from Claude's response, handling code blocks."""
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
    # Try finding JSON object boundaries
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except json.JSONDecodeError:
            pass
    raise json.JSONDecodeError("Could not extract JSON", text, 0)


def load_pestle_history() -> dict:
    """Load pestle_history.json for quarterly article data."""
    path = DATA_DIR / "pestle_history.json"
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def load_decade_data(decade: str) -> dict:
    """Load pestle_decades/{decade}.json for historical article data."""
    path = DATA_DIR / "pestle_decades" / f"{decade}.json"
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def get_db_articles(conn, start_date: str, end_date: str) -> dict:
    """Get news headlines from DB for a date range, grouped by PESTLE category."""
    news = {}
    for cat in PESTLE_CATS:
        rows = conn.execute("""
            SELECT title FROM articles
            WHERE pestle_category = ? AND published_date >= ? AND published_date < ?
            ORDER BY relevance_score DESC LIMIT 30
        """, (cat, start_date, end_date)).fetchall()
        news[cat] = [r[0] for r in rows]
    return news


def get_period_context(year: int, conn, pestle_history: dict, decade_data: dict) -> str:
    """Build context string from multiple data sources for a given year."""
    context_parts = []

    # Source 1: DB articles for the year
    db_news = get_db_articles(conn, f"{year}-01-01", f"{year + 1}-01-01")
    has_db = any(len(v) > 0 for v in db_news.values())

    if has_db:
        for cat in PESTLE_CATS:
            headlines = db_news.get(cat, [])
            if headlines:
                context_parts.append(f"[{PESTLE_JA[cat]}({cat})]")
                for h in headlines[:15]:
                    context_parts.append(f"  - {h}")

    # Source 2: pestle_history.json (quarterly data)
    for q in range(1, 5):
        qkey = f"{year}Q{q}"
        qdata = pestle_history.get(qkey, {})
        for cat in PESTLE_CATS:
            cat_data = qdata.get(cat, {})
            articles = cat_data.get("articles", [])
            for a in articles[:5]:
                title = a.get("title", "")
                if title and not has_db:
                    context_parts.append(f"  - [{PESTLE_JA.get(cat, cat)}] {title}")

    # Source 3: decade files
    decade_key = f"{(year // 10) * 10}s"
    for qkey, qdata in decade_data.items():
        # Match keys that start with this year
        if qkey.startswith(str(year)):
            for cat in PESTLE_CATS:
                cat_data = qdata.get(cat, {})
                articles = cat_data.get("articles", [])
                for a in articles[:3]:
                    title = a.get("title", "")
                    if title:
                        context_parts.append(f"  - [{PESTLE_JA.get(cat, cat)}] {title}")

    return "\n".join(context_parts)


def get_quarterly_context(year: int, quarter: int, conn, pestle_history: dict) -> str:
    """Build context string for a specific quarter."""
    context_parts = []
    month_start = (quarter - 1) * 3 + 1
    month_end = quarter * 3 + 1
    end_year = year
    if month_end > 12:
        month_end = 1
        end_year = year + 1

    start_date = f"{year}-{month_start:02d}-01"
    end_date = f"{end_year}-{month_end:02d}-01"

    # DB articles
    db_news = get_db_articles(conn, start_date, end_date)
    for cat in PESTLE_CATS:
        headlines = db_news.get(cat, [])
        if headlines:
            context_parts.append(f"[{PESTLE_JA[cat]}({cat})]")
            for h in headlines[:20]:
                context_parts.append(f"  - {h}")

    # pestle_history.json
    qkey = f"{year}Q{quarter}"
    qdata = pestle_history.get(qkey, {})
    for cat in PESTLE_CATS:
        cat_data = qdata.get(cat, {})
        articles = cat_data.get("articles", [])
        for a in articles[:10]:
            title = a.get("title", "")
            if title:
                context_parts.append(f"  - [{PESTLE_JA.get(cat, cat)}] {title}")

    return "\n".join(context_parts)


def generate_cla(period_label: str, context: str) -> dict | None:
    """Call Claude API to generate CLA for all 6 PESTLE categories + cross-category synthesis.

    Returns dict with 6 PESTLE category CLAs and cross_category_synthesis,
    or None on failure.
    """
    if len(context.strip()) < 30:
        return None

    prompt = f"""あなたは未来学・社会変動分析の世界的専門家です。以下は「{period_label}」の期間に関する主要な出来事・ニュース見出しです。

{context}

この期間について、PESTLE各分野（Political, Economic, Social, Technological, Legal, Environmental）の因果階層分析（CLA: Causal Layered Analysis）を実施してください。

さらに、6カテゴリを横断する統合分析（cross_category_synthesis）も記述してください。

以下のJSON形式で返してください:
{{
  "Political": {{
    "litany": "表層の出来事・トレンドの要約（2-3文）",
    "systemic_causes": "構造的原因・社会システム的要因（2-3文）",
    "worldview": "支配的な世界観・無意識の前提（2-3文）",
    "myth_metaphor": "深層の文化的物語・象徴（1-2文）"
  }},
  "Economic": {{ ... 同構造 ... }},
  "Social": {{ ... 同構造 ... }},
  "Technological": {{ ... 同構造 ... }},
  "Legal": {{ ... 同構造 ... }},
  "Environmental": {{ ... 同構造 ... }},
  "cross_category_synthesis": "6カテゴリを横断して見える時代の構造変動、神話の交差、パラダイム転換の兆候を2-3文で統合分析"
}}

{period_label}の時代背景・歴史的文脈を十分に踏まえて分析してください。
もしニュース見出しが少ない場合でも、その期間の一般的な知識に基づいて分析してください。
JSONのみ返してください。"""

    for attempt in range(3):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=8192,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            result = extract_json(text)

            # Validate structure
            if not isinstance(result, dict):
                raise ValueError("Response is not a dict")
            return result

        except (json.JSONDecodeError, ValueError) as e:
            if attempt < 2:
                print(f"retry({attempt + 1})...", end=" ", flush=True)
                time.sleep(2)
            else:
                print(f"[ERROR] {e}")
                return None
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(3)
            if attempt == 2:
                return None


def save_to_db(conn_db, period: str, categories: dict):
    """Save CLA results to the cla_analyses table in the DB."""
    for cat_name, cla_data in categories.items():
        if cat_name == "cross_category_synthesis":
            continue
        if not isinstance(cla_data, dict):
            continue
        try:
            conn_db.execute("""
                INSERT INTO cla_analyses (topic, litany, systemic_cause, worldview, myth_metaphor, analyzed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                f"{period}_{cat_name}",
                cla_data.get("litany", ""),
                cla_data.get("systemic_causes", ""),
                cla_data.get("worldview", ""),
                cla_data.get("myth_metaphor", ""),
                datetime.now(timezone.utc).isoformat(),
            ))
        except Exception as e:
            print(f"  [DB WARN] {e}")
    conn_db.commit()


def generate_yearly(conn, pestle_history: dict) -> list:
    """Generate yearly CLA for 1990-2020."""
    print("\n=== Yearly CLA: 1990-2020 ===")

    output_path = DATA_DIR / "cla_historical_yearly.json"

    # Load existing results to allow incremental generation
    existing = []
    if output_path.exists():
        with open(output_path, encoding="utf-8") as f:
            existing = json.load(f)
    existing_periods = {e["period"] for e in existing}

    # Load decade data files
    decade_datasets = {}
    for decade in ["1980s", "1990s", "2000s", "2010s", "2020s"]:
        decade_datasets[decade] = load_decade_data(decade)

    # Open DB connection for saving CLA results
    conn_db = sqlite3.connect(DB_PATH)

    results = list(existing)  # Start from existing
    years = list(range(1990, 2021))
    total = len(years)

    for i, year in enumerate(years):
        period = str(year)
        if period in existing_periods:
            print(f"  [{i + 1}/{total}] {year} ... skipped (exists)")
            continue

        print(f"  [{i + 1}/{total}] {year} ...", end=" ", flush=True)

        decade_key = f"{(year // 10) * 10}s"
        decade_data = decade_datasets.get(decade_key, {})
        context = get_period_context(year, conn, pestle_history, decade_data)

        cla = generate_cla(f"{year}年", context)
        if cla:
            # Extract cross_category_synthesis
            synthesis = cla.pop("cross_category_synthesis", "")
            # Build entry in the required format
            entry = {
                "period": period,
                "type": "yearly",
                "categories": {},
                "cross_category_synthesis": synthesis,
            }
            for cat in PESTLE_CATS:
                if cat in cla:
                    entry["categories"][cat] = cla[cat]

            results.append(entry)
            save_to_db(conn_db, period, cla)
            print(f"OK ({len(entry['categories'])} categories)")
        else:
            print("failed/skipped")

        time.sleep(0.5)

        # Save periodically every 5 years
        if (i + 1) % 5 == 0:
            results.sort(key=lambda x: x["period"])
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"  [checkpoint: {len(results)} periods saved]")

    conn_db.close()

    # Final save
    results.sort(key=lambda x: x["period"])
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n  -> Saved {len(results)} yearly entries to {output_path}")
    return results


def generate_quarterly(conn, pestle_history: dict) -> list:
    """Generate quarterly CLA for 2021-2026."""
    print("\n=== Quarterly CLA: 2021-2026 ===")

    output_path = DATA_DIR / "cla_historical_quarterly.json"

    # Load existing results
    existing = []
    if output_path.exists():
        with open(output_path, encoding="utf-8") as f:
            existing = json.load(f)
    existing_periods = {e["period"] for e in existing}

    conn_db = sqlite3.connect(DB_PATH)
    results = list(existing)

    # Build quarter list: 2021-Q1 through 2026-Q1 (current)
    quarters = []
    for year in range(2021, 2027):
        for q in range(1, 5):
            period = f"{year}-Q{q}"
            # Don't generate future quarters
            if year == 2026 and q > 2:
                break
            quarters.append((year, q, period))

    total = len(quarters)

    for i, (year, q, period) in enumerate(quarters):
        if period in existing_periods:
            print(f"  [{i + 1}/{total}] {period} ... skipped (exists)")
            continue

        print(f"  [{i + 1}/{total}] {period} ...", end=" ", flush=True)

        context = get_quarterly_context(year, q, conn, pestle_history)
        label = f"{year}年 第{q}四半期（Q{q}）"

        cla = generate_cla(label, context)
        if cla:
            synthesis = cla.pop("cross_category_synthesis", "")
            entry = {
                "period": period,
                "type": "quarterly",
                "categories": {},
                "cross_category_synthesis": synthesis,
            }
            for cat in PESTLE_CATS:
                if cat in cla:
                    entry["categories"][cat] = cla[cat]

            results.append(entry)
            save_to_db(conn_db, period, cla)
            print(f"OK ({len(entry['categories'])} categories)")
        else:
            print("failed/skipped")

        time.sleep(0.5)

    conn_db.close()

    # Sort by period
    results.sort(key=lambda x: x["period"])
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n  -> Saved {len(results)} quarterly entries to {output_path}")
    return results


def main():
    print(f"{'=' * 60}")
    print(f"  Historical CLA Generation")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'=' * 60}")

    conn = sqlite3.connect(DB_PATH)
    pestle_history = load_pestle_history()
    print(f"pestle_history.json: {len(pestle_history)} quarters loaded")

    # Generate yearly CLA (1990-2020)
    yearly = generate_yearly(conn, pestle_history)

    # Generate quarterly CLA (2021-2026)
    quarterly = generate_quarterly(conn, pestle_history)

    conn.close()

    print(f"\n{'=' * 60}")
    print(f"  Complete: {len(yearly)} yearly + {len(quarterly)} quarterly entries")
    print(f"  Files:")
    print(f"    - {DATA_DIR / 'cla_historical_yearly.json'}")
    print(f"    - {DATA_DIR / 'cla_historical_quarterly.json'}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
