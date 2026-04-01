#!/usr/bin/env python3
"""
PESTLE News Collector
世界中のRSSフィードとGDELT APIからニュースを取得し、PESTLE 6分野に自動分類する。
各分野100件ずつ、計600件を目標に収集。
"""

import feedparser
import json
import re
import hashlib
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict
from db import save_collection, get_stats

# === Target per category ===
TARGET_PER_CATEGORY = 100

# === PESTLE Categories ===
PESTLE = {
    "Political": {
        "label_ja": "政治",
        "keywords": [
            "politics", "election", "government", "policy", "diplomatic", "diplomacy",
            "sanction", "parliament", "congress", "senate", "president", "minister",
            "treaty", "geopolitics", "nato", "united nations", "vote", "campaign",
            "political", "legislation", "democracy", "authoritarian", "coup",
            "bilateral", "summit", "ambassador", "sovereignty", "referendum",
            "政治", "選挙", "政府", "政策", "外交", "首相", "大統領", "国会",
            "制裁", "条約", "安全保障", "防衛", "与党", "野党", "内閣",
            "サミット", "首脳", "議会", "自民党", "民主", "統治",
        ],
        "gdelt_query": "politics OR election OR government OR diplomacy OR geopolitics",
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
        "gdelt_query": "economy OR inflation OR market OR trade OR investment OR GDP",
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
        "gdelt_query": "society OR education OR health OR migration OR inequality OR culture",
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
        "gdelt_query": "artificial intelligence OR technology OR semiconductor OR quantum OR cybersecurity",
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
        "gdelt_query": "regulation OR law OR court OR privacy OR antitrust OR legislation",
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
        "gdelt_query": "climate OR environment OR renewable energy OR biodiversity OR pollution",
    },
}

# === RSS Feed Sources (expanded) ===
RSS_FEEDS = [
    # --- Global English: General ---
    {"url": "https://feeds.bbci.co.uk/news/world/rss.xml", "name": "BBC World", "lang": "en"},
    {"url": "https://feeds.bbci.co.uk/news/technology/rss.xml", "name": "BBC Tech", "lang": "en"},
    {"url": "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml", "name": "BBC Science", "lang": "en"},
    {"url": "https://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC Business", "lang": "en"},
    {"url": "https://feeds.bbci.co.uk/news/health/rss.xml", "name": "BBC Health", "lang": "en"},
    {"url": "https://feeds.bbci.co.uk/news/education/rss.xml", "name": "BBC Education", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "name": "NYT World", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml", "name": "NYT Tech", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml", "name": "NYT Climate", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "name": "NYT Business", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Health.xml", "name": "NYT Health", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml", "name": "NYT Politics", "lang": "en"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml", "name": "NYT Science", "lang": "en"},
    {"url": "https://www.theguardian.com/world/rss", "name": "Guardian World", "lang": "en"},
    {"url": "https://www.theguardian.com/environment/rss", "name": "Guardian Environment", "lang": "en"},
    {"url": "https://www.theguardian.com/technology/rss", "name": "Guardian Tech", "lang": "en"},
    {"url": "https://www.theguardian.com/law/rss", "name": "Guardian Law", "lang": "en"},
    {"url": "https://www.theguardian.com/politics/rss", "name": "Guardian Politics", "lang": "en"},
    {"url": "https://www.theguardian.com/society/rss", "name": "Guardian Society", "lang": "en"},
    {"url": "https://feeds.reuters.com/reuters/topNews", "name": "Reuters Top", "lang": "en"},
    {"url": "https://feeds.reuters.com/reuters/technologyNews", "name": "Reuters Tech", "lang": "en"},
    {"url": "https://feeds.reuters.com/reuters/environment", "name": "Reuters Environment", "lang": "en"},
    {"url": "https://www.aljazeera.com/xml/rss/all.xml", "name": "Al Jazeera", "lang": "en"},
    # --- Tech / Science ---
    {"url": "https://techcrunch.com/feed/", "name": "TechCrunch", "lang": "en"},
    {"url": "https://www.wired.com/feed/rss", "name": "Wired", "lang": "en"},
    {"url": "https://www.nature.com/nature.rss", "name": "Nature", "lang": "en"},
    {"url": "https://www.sciencedaily.com/rss/all.xml", "name": "ScienceDaily", "lang": "en"},
    {"url": "https://arstechnica.com/feed/", "name": "Ars Technica", "lang": "en"},
    {"url": "https://www.theverge.com/rss/index.xml", "name": "The Verge", "lang": "en"},
    # --- Policy / Legal ---
    {"url": "https://www.politico.com/rss/politicopicks.xml", "name": "Politico", "lang": "en"},
    {"url": "https://www.lawfaremedia.org/feed", "name": "Lawfare", "lang": "en"},
    # --- Business / Economy ---
    {"url": "https://feeds.bloomberg.com/markets/news.rss", "name": "Bloomberg", "lang": "en"},
    {"url": "https://www.ft.com/?format=rss", "name": "Financial Times", "lang": "en"},
    # --- Environment ---
    {"url": "https://www.carbonbrief.org/feed", "name": "Carbon Brief", "lang": "en"},
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
    {"url": "https://news.yahoo.co.jp/rss/topics/domestic.xml", "name": "Yahoo 国内", "lang": "ja"},
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
            for entry in feed.entries[:50]:  # Increased from 30 to 50 per feed
                url = entry.get("link", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                title = entry.get("title", "").strip()
                summary = entry.get("summary", entry.get("description", "")).strip()
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


def fetch_gdelt_articles(category: str, query: str, max_articles: int = 80) -> list[dict]:
    """Fetch articles from GDELT DOC API for a specific PESTLE category.
    Uses multiple single-keyword queries to avoid OR syntax issues."""
    # Split query into individual keywords and fetch separately
    keywords = [k.strip() for k in query.replace(" OR ", "|").split("|") if k.strip()]
    all_gdelt = []
    seen_urls = set()

    per_keyword = max(10, max_articles // max(len(keywords), 1))
    for kw in keywords[:4]:  # Limit to 4 queries per category to respect rate limits
        try:
            params = urllib.parse.urlencode({
                "query": kw,
                "mode": "ArtList",
                "maxrecords": str(per_keyword),
                "format": "json",
            })
            url = f"https://api.gdeltproject.org/api/v2/doc/doc?{params}"
            req = urllib.request.Request(url, headers={"User-Agent": "FutureInsight/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            for item in data.get("articles", []):
                title = item.get("title", "").strip()
                art_url = item.get("url", "")
                if not title or art_url in seen_urls:
                    continue
                # Only keep English and Japanese articles
                lang_str = (item.get("language", "") or "").lower()
                if "english" not in lang_str and "japanese" not in lang_str:
                    continue
                seen_urls.add(art_url)
                lang = "ja" if "japanese" in lang_str else "en"
                all_gdelt.append({
                    "title": title,
                    "summary": "",
                    "url": art_url,
                    "source": "GDELT: " + item.get("domain", ""),
                    "lang": lang,
                    "published": item.get("seendate", ""),
                })
            time.sleep(5)  # Rate limit between keyword queries
        except Exception as e:
            print(f"    [WARN] GDELT '{kw}' failed: {e}")
            continue

    return all_gdelt[:max_articles]


def select_top_articles(articles: list[dict], per_category: int = TARGET_PER_CATEGORY) -> dict:
    """Classify articles and select top N per PESTLE category."""
    for article in articles:
        article["scores"] = classify_pestle(article["title"], article["summary"])

    # Select top articles per category, allowing shared articles across categories
    selected = {}
    used_urls_per_cat = defaultdict(set)

    for category in PESTLE:
        candidates = sorted(
            [a for a in articles if a["scores"][category] > 0],
            key=lambda a: a["scores"][category],
            reverse=True,
        )

        category_articles = []
        for a in candidates:
            if a["url"] in used_urls_per_cat[category]:
                continue
            if len(category_articles) >= per_category:
                break
            used_urls_per_cat[category].add(a["url"])
            # Parse published date to standard format
            pub_date = ""
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(a["published"])
                pub_date = dt.strftime("%Y-%m-%d")
            except Exception:
                pub_date = a.get("published", "")[:10] if a.get("published") else ""

            category_articles.append({
                "title": a["title"],
                "summary": a["summary"],
                "url": a["url"],
                "source": a["source"],
                "lang": a["lang"],
                "published": a["published"],
                "published_date": pub_date,
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

    JST = timezone(timedelta(hours=9))
    today = datetime.now(JST).strftime("%Y-%m-%d")
    output_file = output_dir / f"pestle_{today}.json"

    print(f"=== PESTLE News Collector ({today}) ===")
    print(f"    Target: {TARGET_PER_CATEGORY} articles per category\n")

    # 1. Fetch RSS feeds
    print("1. Fetching RSS feeds...")
    rss_articles = fetch_all_feeds()
    print(f"   {len(rss_articles)} articles from {len(RSS_FEEDS)} RSS feeds")

    # 2. Fetch GDELT for each category to supplement
    print("\n2. Fetching GDELT API (supplementary)...")
    gdelt_articles = []
    for cat, info in PESTLE.items():
        query = info.get("gdelt_query", "")
        if not query:
            continue
        fetched = fetch_gdelt_articles(cat, query, max_articles=80)
        gdelt_articles.extend(fetched)
        print(f"   {info['label_ja']} ({cat}): +{len(fetched)} from GDELT")

    # 3. Merge and deduplicate
    all_articles = rss_articles + gdelt_articles
    seen = set()
    deduped = []
    for a in all_articles:
        key = hashlib.sha256(a["url"].encode()).hexdigest()[:16]
        if key not in seen:
            seen.add(key)
            deduped.append(a)
    print(f"\n   Total unique articles: {len(deduped)}")

    # 4. Classify & Select
    print("\n3. Classifying into PESTLE categories...")
    result = select_top_articles(deduped, per_category=TARGET_PER_CATEGORY)

    total_selected = 0
    for cat, info in result.items():
        status = "OK" if info["count"] >= TARGET_PER_CATEGORY else f"({info['count']}/{TARGET_PER_CATEGORY})"
        print(f"   {info['label_ja']} ({cat}): {info['count']} articles {status}")
        total_selected += info["count"]

    # 5. Build output
    output = {
        "date": today,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "total_fetched": len(deduped),
        "feeds_count": len(RSS_FEEDS),
        "gdelt_used": len(gdelt_articles) > 0,
        "target_per_category": TARGET_PER_CATEGORY,
        "pestle": result,
    }

    print(f"\n   Total selected: {total_selected} / {TARGET_PER_CATEGORY * 6} target")

    # 6. Save
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n4. Saved to {output_file}")

    latest_file = output_dir / "latest.json"
    with open(latest_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"   Saved to {latest_file}")

    # 7. Save to SQLite database
    print("\n5. Saving to database...")
    inserted = save_collection(output)
    print(f"   {inserted} articles saved to SQLite")

    stats = get_stats()
    print(f"   DB: {stats['total_collections']} collections, {stats['total_articles']} total articles")

    print(f"\nDone! {total_selected} articles collected.")


if __name__ == "__main__":
    main()
