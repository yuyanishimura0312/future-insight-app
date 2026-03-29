#!/usr/bin/env python3
"""
Full data generation for Future Insight App:
1. Generate 100+ weak signals from news data
2. Generate CLA data for 4 quarters (current + 3 past)
3. Batch translate English paper titles to Japanese
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic

client = anthropic.Anthropic()
MODEL = "claude-haiku-4-5-20251001"
DATA_DIR = Path(__file__).parent.parent / "data"


def load_json(name):
    with open(DATA_DIR / name, encoding="utf-8") as f:
        return json.load(f)


def save_json(name, data):
    with open(DATA_DIR / name, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ============================================================
# 1. WEAK SIGNALS — Generate 100+ from news data
# ============================================================
def generate_weak_signals(news_data: dict, target_count: int = 120) -> list:
    print(f"\n{'='*50}")
    print(f"  1. Generating {target_count} weak signals")
    print(f"{'='*50}\n")

    # Collect all articles
    all_articles = []
    for cat, info in news_data.get("pestle", {}).items():
        for a in info.get("articles", []):
            all_articles.append({"cat": cat, **a})

    # Generate in batches of ~30
    all_signals = []
    batch_size = 30
    batches = (target_count + batch_size - 1) // batch_size

    for batch_idx in range(batches):
        remaining = target_count - len(all_signals)
        if remaining <= 0:
            break
        count = min(batch_size, remaining)

        # Select diverse articles for context
        sample_size = min(20, len(all_articles))
        import random
        sample = random.sample(all_articles, sample_size)
        context = "\n".join(
            f"[{a['cat']}] {a['title']} ({a.get('source','')}, {a.get('published_date','')})"
            for a in sample
        )

        # Already generated signals to avoid duplicates
        existing = "\n".join(f"- {s['signal']}" for s in all_signals[-20:])

        prompt = f"""Based on these recent news articles, identify {count} weak signals — early indicators of potentially significant future changes that are not yet mainstream.

NEWS CONTEXT:
{context}

{"ALREADY IDENTIFIED (avoid duplicates):" + chr(10) + existing if existing else ""}

For each signal, provide:
- signal: concise name (in Japanese)
- description: 2-3 sentences explaining the signal (in Japanese)
- detected_date: approximate date when this signal was first visible ({news_data.get('date', '2026-03-29')})
- pestle_categories: array of relevant PESTLE categories (Political/Economic/Social/Technological/Legal/Environmental)
- potential_impact: High/Medium/Low
- time_horizon: Near-term (1-2yr) / Medium-term (3-5yr) / Long-term (5-10yr)
- counter_trend: opposing force or trend (in Japanese)
- related_headlines: 1-2 related headline strings (in Japanese)

Return as JSON array. All text in Japanese."""

        try:
            resp = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}]
            )
            text = resp.content[0].text
            # Extract JSON
            start = text.find("[")
            end = text.rfind("]") + 1
            if start >= 0 and end > start:
                signals = json.loads(text[start:end])
                all_signals.extend(signals)
                print(f"  Batch {batch_idx+1}/{batches}: {len(signals)} signals (total: {len(all_signals)})")
            else:
                print(f"  Batch {batch_idx+1}: failed to parse JSON")
        except Exception as e:
            print(f"  Batch {batch_idx+1}: error - {e}")

        time.sleep(1)

    print(f"  Total weak signals: {len(all_signals)}")
    return all_signals[:target_count]


# ============================================================
# 2. CLA — Generate for 4 quarters
# ============================================================
def generate_quarterly_cla(news_data: dict) -> dict:
    print(f"\n{'='*50}")
    print(f"  2. Generating quarterly CLA data")
    print(f"{'='*50}\n")

    base_date = news_data.get("date", "2026-03-29")
    categories = ["Political", "Economic", "Social", "Technological", "Legal", "Environmental"]
    cat_ja = {"Political":"政治","Economic":"経済","Social":"社会","Technological":"技術","Legal":"法律","Environmental":"環境"}

    quarters = {}
    for q_offset in range(4):
        d = datetime.strptime(base_date, "%Y-%m-%d")
        d = d.replace(month=max(1, d.month - q_offset * 3))
        q_key = d.strftime("%Y-%m")
        quarter_label = f"{d.year}年{d.month}月"

        print(f"  Quarter: {quarter_label}")

        # Collect articles for context
        articles_context = ""
        for cat, info in news_data.get("pestle", {}).items():
            top3 = info.get("articles", [])[:3]
            for a in top3:
                articles_context += f"[{cat}] {a['title']}\n"

        prompt = f"""As a futures studies expert, perform a Causal Layered Analysis (CLA) for each of the 6 PESTLE categories based on global trends as of {quarter_label}.

{"Current news context:" + chr(10) + articles_context if q_offset == 0 else f"Analyze trends that were visible around {quarter_label}, considering the global context at that time."}

For each category ({', '.join(categories)}), provide 4 CLA layers:
- litany: surface-level observable trends and data (2-3 sentences)
- systemic_causes: structural/institutional causes (2-3 sentences)
- worldview: underlying assumptions, paradigms, and discourses (2-3 sentences)
- myth_metaphor: deep cultural narratives and metaphors (1-2 sentences)
- key_tension: the central tension or contradiction (1 sentence)
- emerging_narrative: the new narrative that is emerging (1 sentence)

ALL TEXT MUST BE IN JAPANESE.

Return as JSON object with category names as keys."""

        try:
            resp = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}]
            )
            text = resp.content[0].text
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                cla_data = json.loads(text[start:end])
                quarters[q_key] = cla_data
                print(f"    Generated {len(cla_data)} categories")
            else:
                print(f"    Failed to parse JSON")
        except Exception as e:
            print(f"    Error: {e}")

        time.sleep(2)

    return quarters


# ============================================================
# 3. TRANSLATE PAPERS — Batch translate English titles
# ============================================================
def translate_papers_batch(papers: list, existing_trans: dict, batch_size: int = 50, max_batches: int = 100) -> dict:
    print(f"\n{'='*50}")
    print(f"  3. Translating English paper titles")
    print(f"{'='*50}\n")

    untranslated = []
    for i, p in enumerate(papers):
        if i in existing_trans:
            continue
        if (p.get("language") or "en") != "en":
            continue
        untranslated.append((i, p))

    total_to_translate = min(len(untranslated), batch_size * max_batches)
    print(f"  Untranslated English papers: {len(untranslated)}")
    print(f"  Will translate: {total_to_translate}")

    translated_count = 0
    for batch_start in range(0, total_to_translate, batch_size):
        batch = untranslated[batch_start:batch_start + batch_size]
        if not batch:
            break

        titles = "\n".join(f"{idx}: {p['title']}" for idx, p in batch)

        prompt = f"""Translate these academic paper titles from English to Japanese. Return JSON array with objects having "index" (int) and "title_ja" (string).

{titles}

Return ONLY the JSON array, no other text."""

        try:
            resp = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}]
            )
            text = resp.content[0].text
            start = text.find("[")
            end = text.rfind("]") + 1
            if start >= 0 and end > start:
                results = json.loads(text[start:end])
                for r in results:
                    idx = r.get("index")
                    if idx is not None and r.get("title_ja"):
                        existing_trans[idx] = {"index": idx, "title_ja": r["title_ja"]}
                        translated_count += 1
                print(f"  Batch {batch_start//batch_size + 1}: {len(results)} translated (total: {translated_count})")
        except Exception as e:
            print(f"  Batch {batch_start//batch_size + 1}: error - {e}")

        time.sleep(0.5)

    print(f"  Total newly translated: {translated_count}")
    return existing_trans


def main():
    print(f"\n{'='*60}")
    print(f"  Full Data Generation for Future Insight App")
    print(f"{'='*60}")

    # Load existing data
    news_data = load_json("latest.json")
    ai_data = load_json("ai_analysis.json")
    papers = load_json("papers.json")

    existing_trans = {}
    try:
        trans_list = load_json("papers_translations.json")
        for t in trans_list:
            existing_trans[t["index"]] = t
    except:
        pass

    # 1. Weak Signals
    signals = generate_weak_signals(news_data, target_count=120)
    ai_data["weak_signals"] = signals

    # 2. Quarterly CLA
    quarterly_cla = generate_quarterly_cla(news_data)
    ai_data["quarterly_cla"] = quarterly_cla
    # Keep current CLA as-is for backward compatibility

    # 3. Translate papers (first 5000)
    existing_trans = translate_papers_batch(papers, existing_trans, batch_size=50, max_batches=100)

    # Save all
    print(f"\n{'='*50}")
    print(f"  Saving data...")
    print(f"{'='*50}\n")

    save_json("ai_analysis.json", ai_data)
    print(f"  ai_analysis.json saved (signals: {len(signals)}, quarterly CLA: {len(quarterly_cla)} quarters)")

    trans_list = sorted(existing_trans.values(), key=lambda x: x["index"])
    save_json("papers_translations.json", trans_list)
    print(f"  papers_translations.json saved ({len(trans_list)} translations)")

    print(f"\n  Done!")


if __name__ == "__main__":
    main()
