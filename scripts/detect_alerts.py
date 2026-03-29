#!/usr/bin/env python3
"""
PESTLE Alert Detector for Future Insight App
過去データと比較して「新兆候」を検出し、アラートを生成する。

3 types of alerts:
  🔴 EMERGENCE (初出) — Previously unseen topic appears with significant frequency
  🟠 SURGE (急増)     — Known topic shows unusual spike vs historical baseline
  🟡 CROSSOVER (横断) — Topic appears across 3+ PESTLE categories simultaneously
"""

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from db import get_connection

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
    # Vietnamese/other noise
    'ng', 'nh', 'tr', 'ch', 'th', 'gi', 'ph', 'kh',
})

# Minimum thresholds
MIN_MENTIONS_EMERGENCE = 4    # Minimum mentions for a "new" topic to be an alert
MIN_MENTIONS_SURGE = 5        # Minimum mentions for a "surge" topic
MIN_SURGE_RATIO = 3.0         # Minimum ratio (recent/historical) to count as surge
MIN_CATEGORIES_CROSSOVER = 3  # Minimum categories for a crossover alert


def extract_bigrams(title: str) -> list[str]:
    """Extract meaningful 2-word phrases from a title."""
    words = re.findall(r'[a-zA-Z]{3,}', title.lower())
    words = [w for w in words if w not in STOP_WORDS and len(w) > 2]
    return [f'{words[i]} {words[i+1]}' for i in range(len(words) - 1)]


def extract_keywords(title: str) -> list[str]:
    """Extract single meaningful keywords from a title."""
    words = re.findall(r'[a-zA-Z]{4,}', title.lower())
    return [w for w in words if w not in STOP_WORDS]


def load_articles():
    """Load all articles from DB with date and category."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT title, pestle_category, published_date
        FROM articles
        WHERE published_date IS NOT NULL
        ORDER BY published_date
    """).fetchall()
    conn.close()
    return rows


def detect_emergence_alerts(rows, recent_months=3):
    """Detect topics appearing for the first time in recent period."""
    months = sorted(set(r[2][:7] for r in rows if r[2] and len(r[2]) >= 7))
    if len(months) < recent_months + 1:
        return []

    cutoff_months = set(months[-recent_months:])
    hist_months = set(months[:-recent_months])

    # Build bigram sets
    recent_bigrams = Counter()
    recent_by_cat = defaultdict(Counter)
    hist_bigrams = set()

    for title, cat, pd in rows:
        if not pd or len(pd) < 7:
            continue
        month = pd[:7]
        bigrams = extract_bigrams(title)
        if month in cutoff_months:
            recent_bigrams.update(bigrams)
            recent_by_cat[cat].update(bigrams)
        elif month in hist_months:
            hist_bigrams.update(bigrams)

    # Find new bigrams with significant frequency
    alerts = []
    for bigram, count in recent_bigrams.most_common():
        if count < MIN_MENTIONS_EMERGENCE:
            break
        if bigram not in hist_bigrams:
            # Find which categories it appears in
            cats = [cat for cat, kws in recent_by_cat.items() if bigram in kws]
            alerts.append({
                "type": "EMERGENCE",
                "level": "high",
                "topic": bigram,
                "mentions": count,
                "categories": cats,
                "description": f"「{bigram}」が過去に例のない頻度で出現（直近{recent_months}ヶ月で{count}回）",
                "description_en": f'"{bigram}" emerged with {count} mentions in last {recent_months} months (no prior history)',
            })

    return alerts[:15]  # Top 15 emergence alerts


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

    for title, cat, pd in rows:
        if not pd or len(pd) < 7:
            continue
        month = pd[:7]
        # Use both keywords and bigrams
        terms = extract_keywords(title) + extract_bigrams(title)
        if month in cutoff_months:
            recent_counts.update(terms)
            recent_by_cat[cat].update(terms)
        elif month in hist_months:
            hist_counts.update(terms)

    alerts = []
    for term, count in recent_counts.most_common(500):
        if count < MIN_MENTIONS_SURGE:
            continue
        hist_count = hist_counts.get(term, 0)
        if hist_count == 0:
            continue  # Handled by emergence alerts

        recent_rate = count / n_recent
        hist_rate = hist_count / n_hist
        ratio = recent_rate / hist_rate

        if ratio >= MIN_SURGE_RATIO:
            cats = [cat for cat, kws in recent_by_cat.items() if term in kws]
            alerts.append({
                "type": "SURGE",
                "level": "medium" if ratio < 5.0 else "high",
                "topic": term,
                "mentions": count,
                "ratio": round(ratio, 1),
                "historical_avg": round(hist_rate, 1),
                "categories": cats,
                "description": f"「{term}」が過去平均の{ratio:.1f}倍に急増（月平均 {hist_rate:.1f}→{recent_rate:.1f}）",
                "description_en": f'"{term}" surged {ratio:.1f}x vs historical avg ({hist_rate:.1f} → {recent_rate:.1f} per month)',
            })

    # Sort by ratio (highest spike first), then by mentions
    alerts.sort(key=lambda a: (-a["ratio"], -a["mentions"]))
    return alerts[:15]  # Top 15 surge alerts


def detect_crossover_alerts(rows, recent_months=2):
    """Detect topics appearing across multiple PESTLE categories."""
    months = sorted(set(r[2][:7] for r in rows if r[2] and len(r[2]) >= 7))
    if not months:
        return []

    recent = set(months[-recent_months:])
    keyword_cats = defaultdict(set)
    keyword_counts = Counter()

    for title, cat, pd in rows:
        if not pd or len(pd) < 7:
            continue
        if pd[:7] not in recent:
            continue
        for kw in extract_keywords(title):
            keyword_cats[kw].add(cat)
            keyword_counts[kw] += 1

    alerts = []
    for kw, cats in keyword_cats.items():
        if len(cats) >= MIN_CATEGORIES_CROSSOVER and keyword_counts[kw] >= 5:
            alerts.append({
                "type": "CROSSOVER",
                "level": "medium",
                "topic": kw,
                "mentions": keyword_counts[kw],
                "categories": sorted(cats),
                "n_categories": len(cats),
                "description": f"「{kw}」が{len(cats)}分野に横断的に出現（{', '.join(sorted(cats))}）",
                "description_en": f'"{kw}" appears across {len(cats)} PESTLE categories ({", ".join(sorted(cats))})',
            })

    alerts.sort(key=lambda a: (-a["n_categories"], -a["mentions"]))
    return alerts[:10]


def generate_alerts():
    """Run all alert detectors and return combined results."""
    print("=== PESTLE Alert Detection ===\n")

    rows = load_articles()
    print(f"Analyzing {len(rows)} articles...\n")

    # Run detectors
    emergence = detect_emergence_alerts(rows)
    print(f"EMERGENCE alerts: {len(emergence)}")
    for a in emergence[:5]:
        print(f"  🔴 {a['topic']} ({a['mentions']} mentions)")

    surge = detect_surge_alerts(rows)
    print(f"\nSURGE alerts: {len(surge)}")
    for a in surge[:5]:
        print(f"  🟠 {a['topic']} ({a['ratio']}x surge)")

    crossover = detect_crossover_alerts(rows)
    print(f"\nCROSSOVER alerts: {len(crossover)}")
    for a in crossover[:5]:
        print(f"  🟡 {a['topic']} ({a['n_categories']} categories)")

    # Combine all alerts
    all_alerts = emergence + surge + crossover

    # Assign severity ordering for sorting
    level_order = {"high": 0, "medium": 1, "low": 2}
    all_alerts.sort(key=lambda a: (level_order.get(a["level"], 2), -a["mentions"]))

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

    # Save to alerts.json
    output_path = Path(__file__).parent.parent / "data" / "alerts.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Saved {result['alert_counts']['total']} alerts to {output_path}")


if __name__ == "__main__":
    main()
