#!/usr/bin/env python3
"""
PESTLE Alert Detector for Future Insight App
過去データと比較して「新兆候」を検出し、Claude APIで分析的な解説を生成する。

3 types of alerts:
  EMERGENCE (初出) — Previously unseen topic appears with significant frequency
  SURGE (急増)     — Known topic shows unusual spike vs historical baseline
  CROSSOVER (横断) — Topic appears across 3+ PESTLE categories simultaneously
"""

import json
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

from db import get_connection

load_dotenv(Path(__file__).parent.parent / ".env")

# Words to ignore in analysis
STOP_WORDS = frozenset({
    'the', 'a', 'an', 'in', 'on', 'of', 'to', 'for', 'and', 'is', 'are',
    'was', 'were', 'it', 'its', 'at', 'by', 'with', 'from', 'as', 'or',
    'be', 'has', 'had', 'have', 'that', 'this', 'not', 'but', 'will',
    'can', 'do', 'no', 'up', 'out', 'new', 'over', 'after', 'about',
    'into', 'more', 'than', 'how', 'all', 'been', 'says', 'said', 'what',
    'who', 'why', 'when', 'where', 'may', 'two', 'first', 'last', 'one',
    'just', 'also', 'most', 'other', 'some', 'their', 'his', 'her', 'our',
    'they', 'we', 'he', 'she', 'you', 'my', 'your', 'us', 'them', 'me',
    'him', 'get', 'set', 'back', 'very', 'now', 'would', 'could', 'should',
    'news', 'report', 'today', 'week', 'day', 'days', 'year', 'years',
    'month', 'amid', 'per', 'entre', 'para', 'como', 'los', 'las', 'des',
    'une', 'que', 'del', 'por', 'con', 'ser', 'sur', 'est', 'dans', 'une',
    'dit', 'sont', 'aux', 'ces', 'der', 'die', 'und', 'den', 'von',
    'ng', 'nh', 'tr', 'ch', 'th', 'gi', 'ph', 'kh',  # Vietnamese fragments
    'opinion', 'diario', 'march', 'february', 'january', 'december',
    'november', 'october', 'september', 'august', 'july', 'june',
    'short', 'interest', 'across', 'during', 'against', 'under',
    'through', 'between', 'before', 'while', 'every', 'being', 'those',
    'these', 'there', 'here', 'then', 'only', 'still', 'already',
    'much', 'many', 'such', 'each', 'both', 'well', 'even', 'make',
    'made', 'take', 'took', 'come', 'came', 'give', 'gave', 'going',
    'know', 'think', 'want', 'look', 'use', 'used', 'find', 'tell',
    'call', 'called', 'keep', 'let', 'begin', 'seem', 'help', 'show',
    'hear', 'play', 'run', 'move', 'live', 'believe', 'hold', 'bring',
    'happen', 'write', 'provide', 'sit', 'stand', 'lose', 'pay', 'meet',
    'include', 'continue', 'learn', 'change', 'lead', 'understand',
    'watch', 'follow', 'stop', 'create', 'speak', 'read', 'allow',
    'add', 'spend', 'grow', 'open', 'walk', 'win', 'offer', 'remember',
    'consider', 'appear', 'buy', 'wait', 'serve', 'die', 'send', 'build',
    'stay', 'fall', 'cut', 'reach', 'kill', 'remain', 'suggest', 'raise',
    'pass', 'sell', 'require', 'become', 'states', 'state', 'united',
    'people', 'world', 'government', 'country', 'time', 'system',
    'part', 'number', 'group', 'case', 'company', 'work', 'point',
    'fact', 'end', 'water', 'long', 'high', 'small', 'large', 'old',
    'different', 'national', 'right', 'place', 'same', 'another', 'big',
    'even', 'own', 'public', 'good', 'able', 'local', 'possible', 'major',
    'full', 'real', 'early', 'important', 'sure', 'late', 'free',
    'says', 'according', 'among', 'based', 'likely', 'former',
    'could', 'million', 'billion', 'percent', 'half',
    # Noise bigrams fragments
    'tako', 'crisi', 'clima', 'ailing', 'cement',
})

MIN_MENTIONS_EMERGENCE = 5
MIN_MENTIONS_SURGE = 6
MIN_SURGE_RATIO = 4.0
MIN_CATEGORIES_CROSSOVER = 3


def extract_bigrams(title: str) -> list[str]:
    """Extract meaningful 2-word phrases from a title."""
    words = re.findall(r'[a-zA-Z]{3,}', title.lower())
    words = [w for w in words if w not in STOP_WORDS and len(w) > 2]
    bigrams = []
    for i in range(len(words) - 1):
        bg = f'{words[i]} {words[i+1]}'
        # Filter out bigrams where both words are very short
        if len(words[i]) >= 4 or len(words[i+1]) >= 4:
            bigrams.append(bg)
    return bigrams


def extract_keywords(title: str) -> list[str]:
    """Extract single meaningful keywords from a title."""
    words = re.findall(r'[a-zA-Z]{4,}', title.lower())
    return [w for w in words if w not in STOP_WORDS]


def load_articles():
    """Load all articles from DB with date, category, and URL."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT title, pestle_category, published_date, url
        FROM articles
        WHERE published_date IS NOT NULL
        ORDER BY published_date
    """).fetchall()
    conn.close()
    return rows


def find_related_titles(rows, topic: str, recent_months_set: set, limit=10) -> list[str]:
    """Find article titles containing the topic in the recent period."""
    titles = []
    topic_lower = topic.lower()
    for title, cat, pd, url in rows:
        if not pd or len(pd) < 7 or pd[:7] not in recent_months_set:
            continue
        if topic_lower in title.lower():
            titles.append(title)
            if len(titles) >= limit:
                break
    return titles


def detect_emergence_alerts(rows, recent_months=3):
    """Detect topics appearing for the first time in recent period."""
    months = sorted(set(r[2][:7] for r in rows if r[2] and len(r[2]) >= 7))
    if len(months) < recent_months + 1:
        return []

    cutoff_months = set(months[-recent_months:])
    hist_months = set(months[:-recent_months])

    recent_bigrams = Counter()
    recent_by_cat = defaultdict(Counter)
    hist_bigrams = set()

    for title, cat, pd, url in rows:
        if not pd or len(pd) < 7:
            continue
        month = pd[:7]
        bigrams = extract_bigrams(title)
        if month in cutoff_months:
            recent_bigrams.update(bigrams)
            recent_by_cat[cat].update(bigrams)
        elif month in hist_months:
            hist_bigrams.update(bigrams)

    alerts = []
    for bigram, count in recent_bigrams.most_common():
        if count < MIN_MENTIONS_EMERGENCE:
            break
        if bigram not in hist_bigrams:
            cats = [cat for cat, kws in recent_by_cat.items() if bigram in kws]
            related = find_related_titles(rows, bigram.split()[0], cutoff_months, limit=8)
            alerts.append({
                "type": "EMERGENCE",
                "level": "high",
                "topic": bigram,
                "mentions": count,
                "categories": cats,
                "sample_titles": related,
            })

    return alerts[:12]


def detect_surge_alerts(rows, recent_months=3):
    """Detect topics with unusual frequency spikes."""
    months = sorted(set(r[2][:7] for r in rows if r[2] and len(r[2]) >= 7))
    if len(months) < recent_months + 3:
        return []

    cutoff_months = set(months[-recent_months:])
    hist_months = set(months[:-recent_months])
    n_recent = max(len(cutoff_months), 1)
    n_hist = max(len(hist_months), 1)

    recent_counts = Counter()
    hist_counts = Counter()
    recent_by_cat = defaultdict(Counter)

    for title, cat, pd, url in rows:
        if not pd or len(pd) < 7:
            continue
        month = pd[:7]
        terms = extract_keywords(title) + extract_bigrams(title)
        if month in cutoff_months:
            recent_counts.update(terms)
            recent_by_cat[cat].update(terms)
        elif month in hist_months:
            hist_counts.update(terms)

    alerts = []
    seen_topics = set()
    for term, count in recent_counts.most_common(500):
        if count < MIN_MENTIONS_SURGE:
            continue
        hist_count = hist_counts.get(term, 0)
        if hist_count == 0:
            continue

        recent_rate = count / n_recent
        hist_rate = hist_count / n_hist
        ratio = recent_rate / hist_rate

        # Deduplicate: skip if a bigram containing this keyword already added
        if term in seen_topics:
            continue

        if ratio >= MIN_SURGE_RATIO:
            cats = [cat for cat, kws in recent_by_cat.items() if term in kws]
            related = find_related_titles(rows, term, cutoff_months, limit=8)
            seen_topics.add(term)
            alerts.append({
                "type": "SURGE",
                "level": "medium" if ratio < 8.0 else "high",
                "topic": term,
                "mentions": count,
                "ratio": round(ratio, 1),
                "historical_avg": round(hist_rate, 1),
                "categories": cats,
                "sample_titles": related,
            })

    alerts.sort(key=lambda a: (-a["ratio"], -a["mentions"]))
    return alerts[:12]


def detect_crossover_alerts(rows, recent_months=2):
    """Detect topics appearing across multiple PESTLE categories."""
    months = sorted(set(r[2][:7] for r in rows if r[2] and len(r[2]) >= 7))
    if not months:
        return []

    recent = set(months[-recent_months:])
    keyword_cats = defaultdict(set)
    keyword_counts = Counter()

    for title, cat, pd, url in rows:
        if not pd or len(pd) < 7 or pd[:7] not in recent:
            continue
        for kw in extract_keywords(title):
            keyword_cats[kw].add(cat)
            keyword_counts[kw] += 1

    alerts = []
    for kw, cats in keyword_cats.items():
        if len(cats) >= MIN_CATEGORIES_CROSSOVER and keyword_counts[kw] >= 8:
            related = find_related_titles(rows, kw, recent, limit=8)
            alerts.append({
                "type": "CROSSOVER",
                "level": "medium",
                "topic": kw,
                "mentions": keyword_counts[kw],
                "categories": sorted(cats),
                "n_categories": len(cats),
                "sample_titles": related,
            })

    alerts.sort(key=lambda a: (-a["n_categories"], -a["mentions"]))
    return alerts[:8]


def enrich_alerts_with_ai(all_alerts: list[dict]) -> list[dict]:
    """Use Claude API to generate analytical descriptions for each alert."""
    import anthropic
    client = anthropic.Anthropic()

    cat_ja = {
        "Political": "政治", "Economic": "経済", "Social": "社会",
        "Technological": "技術", "Legal": "法律", "Environmental": "環境",
    }

    # Build a single prompt with all alerts for efficiency
    alert_summaries = []
    for i, a in enumerate(all_alerts):
        cats_str = ", ".join(cat_ja.get(c, c) for c in a["categories"])
        titles_str = "\n".join(f"  - {t}" for t in a.get("sample_titles", [])[:6])

        if a["type"] == "EMERGENCE":
            context = f"タイプ: 初出（過去2年間に出現しなかったトピックが新たに{a['mentions']}回出現）"
        elif a["type"] == "SURGE":
            context = f"タイプ: 急増（過去平均の{a.get('ratio', '?')}倍に増加、{a['mentions']}回出現）"
        else:
            context = f"タイプ: 横断出現（{a.get('n_categories', '?')}つのPESTLE分野に同時出現、{a['mentions']}回）"

        alert_summaries.append(
            f"--- アラート{i+1} ---\n"
            f"トピック: {a['topic']}\n"
            f"{context}\n"
            f"関連PESTLE分野: {cats_str}\n"
            f"関連記事タイトル:\n{titles_str}"
        )

    prompt = f"""以下はPESTLE分析（政治・経済・社会・技術・法律・環境）のニュースモニタリングから検出された新兆候アラートです。
過去2年分（2024年4月〜2026年3月）の21,000件以上のグローバルニュースを分析した結果です。

各アラートについて、以下の形式でJSON配列を返してください。各要素は：
- "title": アラートの見出し（日本語、15文字以内、内容を端的に表すもの）
- "analysis": 分析的な解説文（日本語、80〜120文字程度の散文）。何が起きているのか、なぜそれが「新兆候」として重要なのか、未来にどんな影響がありうるかを簡潔に記述してください。
- "title_en": 英語版の見出し（10 words以内）
- "analysis_en": 英語版の解説文（1-2 sentences）

重要:
- 単なるキーワードの羅列や統計値の繰り返しではなく、「読めば状況がわかる」文章にしてください
- 未来洞察の観点から、この兆候が示唆する今後の展開にも触れてください
- ノイズ的なアラート（意味不明なもの、ニュース価値が低いもの）は analysis に「※ノイズの可能性が高い」と明記してください

{chr(10).join(alert_summaries)}

JSONのみを返してください（```json等のマークダウンは不要）。"""

    print("\n  Calling Claude API for alert analysis...")
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=8000,
        messages=[{"role": "user", "content": prompt}],
    )

    try:
        text = resp.content[0].text.strip()
        # Remove markdown code fence if present
        if text.startswith("```"):
            text = re.sub(r'^```\w*\n?', '', text)
            text = re.sub(r'\n?```$', '', text)
        enrichments = json.loads(text)
    except (json.JSONDecodeError, IndexError) as e:
        print(f"  [WARN] Failed to parse AI response: {e}")
        return all_alerts

    # Merge AI analysis into alerts
    for i, a in enumerate(all_alerts):
        if i < len(enrichments):
            e = enrichments[i]
            a["alert_title"] = e.get("title", a["topic"])
            a["description"] = e.get("analysis", a.get("description", ""))
            a["alert_title_en"] = e.get("title_en", a["topic"])
            a["description_en"] = e.get("analysis_en", a.get("description_en", ""))

    print(f"  ✓ {len(enrichments)} alerts enriched with AI analysis")
    return all_alerts


def generate_alerts():
    """Run all alert detectors and return combined results."""
    print("=== PESTLE Alert Detection ===\n")

    rows = load_articles()
    print(f"Analyzing {len(rows)} articles...\n")

    emergence = detect_emergence_alerts(rows)
    print(f"EMERGENCE alerts: {len(emergence)}")

    surge = detect_surge_alerts(rows)
    print(f"SURGE alerts: {len(surge)}")

    crossover = detect_crossover_alerts(rows)
    print(f"CROSSOVER alerts: {len(crossover)}")

    all_alerts = emergence + surge + crossover

    level_order = {"high": 0, "medium": 1, "low": 2}
    all_alerts.sort(key=lambda a: (level_order.get(a["level"], 2), -a["mentions"]))

    # Enrich with AI analysis
    all_alerts = enrich_alerts_with_ai(all_alerts)

    # Remove sample_titles from output (used only for AI context)
    for a in all_alerts:
        a.pop("sample_titles", None)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_articles_analyzed": len(rows),
        "alert_counts": {
            "emergence": len(emergence),
            "surge": len(surge),
            "crossover": len(crossover),
            "total": len(all_alerts),
        },
        "alerts": all_alerts,
    }


def main():
    result = generate_alerts()

    output_path = Path(__file__).parent.parent / "data" / "alerts.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Saved {result['alert_counts']['total']} alerts to {output_path}")


if __name__ == "__main__":
    main()
