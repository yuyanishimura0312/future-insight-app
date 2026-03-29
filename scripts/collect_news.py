#!/usr/bin/env python3
"""
PESTLE News Collector
世界中のRSSフィードからニュースを取得し、PESTLE 6分野に自動分類する。
各分野20件ずつ、計120件を収集。
"""

import feedparser
import json
import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# === PESTLE Categories ===
PESTLE = {
    "Political": {
        "label_ja": "政治",
        "keywords": [
            # English
            "politics", "election", "government", "policy", "diplomatic", "diplomacy",
            "sanction", "parliament", "congress", "senate", "president", "minister",
            "treaty", "geopolitics", "nato", "united nations", "vote", "campaign",
            "political", "legislation", "democracy", "authoritarian", "coup",
            "bilateral", "summit", "ambassador", "sovereignty", "referendum",
            # Japanese
            "政治", "選挙", "政府", "政策", "外交", "首相", "大統領", "国会",
            "制裁", "条約", "安全保障", "防衛", "与党", "野党", "内閣",
            "サミット", "首脳", "議会", "自民党", "民主", "統治",
        ],
    },
    "Economic": {
        "label_ja": "経済",
        "keywords": [
            "economy", "economic", "gdp", "inflation", "recession", "market",
            "stock", "trade", "tariff", "currency", "interest rate", "central bank",
            "investment", "startup", "venture", "ipo", "merger", "acquisition",
            "supply chain", "manufacturing", "unemployment", "fiscal", "debt",
            "cryptocurrency", "bitcoin", "fintech", "banking",
            "経済", "景気", "株価", "為替", "金利", "インフレ", "GDP", "貿易",
            "投資", "スタートアップ", "起業", "企業", "決算", "売上",
            "日銀", "金融", "円安", "円高", "市場", "雇用", "失業",
        ],
    },
    "Social": {
        "label_ja": "社会",
        "keywords": [
            "social", "society", "culture", "education", "health", "population",
            "demographic", "migration", "immigration", "inequality", "poverty",
            "community", "diversity", "inclusion", "mental health", "aging",
            "urbanization", "lifestyle", "welfare", "gender", "protest",
            "pandemic", "public health", "housing", "labor",
            "社会", "教育", "健康", "人口", "高齢化", "少子化", "移民",
            "格差", "貧困", "福祉", "介護", "医療", "文化", "子育て",
            "多様性", "ジェンダー", "コミュニティ", "地域", "生活",
        ],
    },
    "Technological": {
        "label_ja": "技術",
        "keywords": [
            "technology", "ai", "artificial intelligence", "machine learning",
            "quantum", "robotics", "automation", "blockchain", "5g", "6g",
            "cybersecurity", "cloud", "software", "hardware", "semiconductor",
            "biotech", "space", "satellite", "drone", "iot", "metaverse",
            "virtual reality", "augmented reality", "deeptech", "chip",
            "テクノロジー", "技術", "AI", "人工知能", "ロボット", "量子",
            "半導体", "宇宙", "サイバー", "ブロックチェーン", "自動運転",
            "バイオ", "生成AI", "ChatGPT", "Claude", "ドローン", "DX",
        ],
    },
    "Legal": {
        "label_ja": "法律",
        "keywords": [
            "law", "legal", "regulation", "compliance", "court", "justice",
            "patent", "copyright", "intellectual property", "antitrust",
            "privacy", "gdpr", "data protection", "lawsuit", "ruling",
            "legislation", "criminal", "constitutional", "human rights",
            "labor law", "tax law", "regulatory",
            "法律", "規制", "法案", "裁判", "判決", "訴訟", "特許",
            "知的財産", "個人情報", "コンプライアンス", "憲法", "人権",
            "独占禁止", "著作権", "税制", "改正", "条例", "法改正",
        ],
    },
    "Environmental": {
        "label_ja": "環境",
        "keywords": [
            "environment", "climate", "carbon", "emission", "renewable",
            "solar", "wind energy", "sustainability", "biodiversity",
            "pollution", "deforestation", "ocean", "water", "ecosystem",
            "green", "net zero", "circular economy", "recycling",
            "electric vehicle", "ev", "clean energy", "wildfire", "flood",
            "環境", "気候", "温暖化", "脱炭素", "再生可能", "太陽光",
            "サステナブル", "持続可能", "生態系", "汚染", "リサイクル",
            "EV", "電気自動車", "カーボン", "CO2", "自然災害", "洪水",
        ],
    },
}

# === RSS Feed Sources ===
RSS_FEEDS = [
    # --- Global English ---
    {"url": "https://feeds.bbci.co.uk/news/world/rss.xml", "name": "BBC World", "lang": "en"},
    {"url": "https://feeds.bbci.co.uk/news/technology/rss.xml", "name": "BBC Tech", "lang": "en"},
    {"url": "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml", "name": "BBC Science", "lang": "en"},
    {"url": "https://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC Business", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "name": "NYT World", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml", "name": "NYT Tech", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml", "name": "NYT Climate", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "name": "NYT Business", "lang": "en"},
    {"url": "https://www.theguardian.com/world/rss", "name": "Guardian World", "lang": "en"},
    {"url": "https://www.theguardian.com/environment/rss", "name": "Guardian Environment", "lang": "en"},
    {"url": "https://www.theguardian.com/technology/rss", "name": "Guardian Tech", "lang": "en"},
    {"url": "https://feeds.reuters.com/reuters/topNews", "name": "Reuters Top", "lang": "en"},
    {"url": "https://feeds.reuters.com/reuters/technologyNews", "name": "Reuters Tech", "lang": "en"},
    {"url": "https://feeds.reuters.com/reuters/environment", "name": "Reuters Environment", "lang": "en"},
    {"url": "https://www.aljazeera.com/xml/rss/all.xml", "name": "Al Jazeera", "lang": "en"},
    {"url": "https://techcrunch.com/feed/", "name": "TechCrunch", "lang": "en"},
    {"url": "https://www.wired.com/feed/rss", "name": "Wired", "lang": "en"},
    {"url": "https://www.nature.com/nature.rss", "name": "Nature", "lang": "en"},
    {"url": "https://www.sciencedaily.com/rss/all.xml", "name": "ScienceDaily", "lang": "en"},
    # --- Japanese ---
    {"url": "https://www3.nhk.or.jp/rss/news/cat0.xml", "name": "NHK 主要", "lang": "ja"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat1.xml", "name": "NHK 社会", "lang": "ja"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat3.xml", "name": "NHK 科学", "lang": "ja"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat4.xml", "name": "NHK 政治", "lang": "ja"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat5.xml", "name": "NHK 経済", "lang": "ja"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat6.xml", "name": "NHK 国際", "lang": "ja"},
    {"url": "https://news.yahoo.co.jp/rss/topics/top-picks.xml", "name": "Yahoo Japan", "lang": "ja"},
    {"url": "https://news.yahoo.co.jp/rss/topics/business.xml", "name": "Yahoo ビジネス", "lang": "ja"},
    {"url": "https://news.yahoo.co.jp/rss/topics/science.xml", "name": "Yahoo サイエンス", "lang": "ja"},
    {"url": "https://news.yahoo.co.jp/rss/topics/it.xml", "name": "Yahoo IT", "lang": "ja"},
    {"url": "https://news.yahoo.co.jp/rss/topics/world.xml", "name": "Yahoo 国際", "lang": "ja"},
    {"url": "https://www.nikkei.com/rss/", "name": "日経", "lang": "ja"},
]

# === Scoring function ===
def classify_pestle(title: str, summary: str) -> dict[str, float]:
    """Score an article against each PESTLE category. Returns {category: score}."""
    text = f"{title} {summary}".lower()
    scores = {}
    for category, info in PESTLE.items():
        score = 0
        for kw in info["keywords"]:
            if kw.lower() in text:
                # Longer keywords get higher weight (more specific)
                weight = 1 + len(kw) / 10
                score += weight
        scores[category] = score
    return scores


def fetch_all_feeds() -> list[dict]:
    """Fetch articles from all RSS feeds."""
    articles = []
    seen_urls = set()

    for feed_info in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_info["url"])
            for entry in feed.entries[:30]:  # Max 30 per feed
                url = entry.get("link", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                title = entry.get("title", "").strip()
                summary = entry.get("summary", entry.get("description", "")).strip()
                # Strip HTML tags from summary
                summary = re.sub(r"<[^>]+>", "", summary).strip()
                if len(summary) > 300:
                    summary = summary[:300] + "..."

                published = entry.get("published", entry.get("updated", ""))

                articles.append({
                    "title": title,
                    "summary": summary,
                    "url": url,
                    "source": feed_info["name"],
                    "lang": feed_info["lang"],
                    "published": published,
                })
        except Exception as e:
            print(f"  [WARN] Failed to fetch {feed_info['name']}: {e}")

    return articles


def select_top_articles(articles: list[dict], per_category: int = 20) -> dict:
    """Classify articles and select top N per PESTLE category."""
    # Score all articles
    for article in articles:
        article["scores"] = classify_pestle(article["title"], article["summary"])

    # Select top articles per category, avoiding duplicates
    selected = {}
    used_urls = set()

    for category in PESTLE:
        # Sort by score for this category (descending)
        candidates = sorted(
            [a for a in articles if a["scores"][category] > 0],
            key=lambda a: a["scores"][category],
            reverse=True,
        )

        category_articles = []
        for a in candidates:
            if a["url"] in used_urls:
                continue
            if len(category_articles) >= per_category:
                break
            used_urls.add(a["url"])
            category_articles.append({
                "title": a["title"],
                "summary": a["summary"],
                "url": a["url"],
                "source": a["source"],
                "lang": a["lang"],
                "published": a["published"],
                "relevance_score": round(a["scores"][category], 2),
            })

        selected[category] = {
            "label_ja": PESTLE[category]["label_ja"],
            "count": len(category_articles),
            "articles": category_articles,
        }

    return selected


def main():
    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_file = output_dir / f"pestle_{today}.json"

    print(f"=== PESTLE News Collector ({today}) ===\n")

    # 1. Fetch
    print("1. Fetching RSS feeds...")
    articles = fetch_all_feeds()
    print(f"   {len(articles)} articles collected from {len(RSS_FEEDS)} feeds\n")

    # 2. Classify & Select
    print("2. Classifying into PESTLE categories...")
    result = select_top_articles(articles, per_category=20)

    for cat, info in result.items():
        print(f"   {info['label_ja']} ({cat}): {info['count']} articles")

    # 3. Build output
    output = {
        "date": today,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "total_fetched": len(articles),
        "feeds_count": len(RSS_FEEDS),
        "pestle": result,
    }

    total_selected = sum(info["count"] for info in result.values())
    print(f"\n   Total selected: {total_selected} / 120 target")

    # 4. Save
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n3. Saved to {output_file}")

    # Also save as latest.json for easy access
    latest_file = output_dir / "latest.json"
    with open(latest_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"   Saved to {latest_file}")

    print("\n✓ Done!")


if __name__ == "__main__":
    main()
